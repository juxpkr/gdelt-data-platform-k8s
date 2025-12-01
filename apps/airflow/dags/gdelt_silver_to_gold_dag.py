from __future__ import annotations
import os
import pendulum
from datetime import timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

SPARK_JOBS_DIR = "/opt/airflow/spark-jobs"
SPARK_ARGS = "--jars /opt/spark/jars/delta-core_2.12-2.4.0.jar,/opt/spark/jars/delta-storage-2.4.0.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar --conf spark.hadoop.fs.s3a.endpoint=http://minio.airflow.svc.cluster.local:9000 --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog pyspark-shell"

def get_dbt_command(cmd: str) -> str:
    return f"""
        # 1. 환경 설정
        export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
        export SPARK_LOC=$(python3 -c "import pyspark; print(pyspark.__path__[0])" | tail -n 1)
        export SPARK_HOME=$SPARK_LOC
        export PATH=$PATH:$SPARK_HOME/bin:/home/airflow/.local/bin
        export PYSPARK_PYTHON=python3
        export PYSPARK_DRIVER_PYTHON=python3
        export LC_ALL=C.UTF-8
        export LANG=C.UTF-8

        # MinIO Credentials
        export AWS_ACCESS_KEY_ID=minioadmin
        export AWS_SECRET_ACCESS_KEY=minioadmin

        # JAR 로딩 + MinIO + Delta 설정
        export PYSPARK_SUBMIT_ARGS="--jars /opt/spark/jars/delta-core_2.12-2.4.0.jar,/opt/spark/jars/delta-storage-2.4.0.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar --conf spark.hadoop.fs.s3a.endpoint=http://minio.airflow.svc.cluster.local:9000 --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog pyspark-shell"

        # 임시 폴더 설정 (Read-only 해결)
        export DBT_TARGET_PATH="/tmp/dbt_target"
        export DBT_LOG_PATH="/tmp/dbt_logs"
        export DBT_PROFILES_DIR="/opt/airflow/dbt"
        mkdir -p $DBT_TARGET_PATH $DBT_LOG_PATH

        # /tmp에서 실행 (Derby/Hive가 현재 디렉토리에 파일 생성 방지)
        cd /tmp && {cmd} --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
    """

with DAG(
    dag_id="gdelt_silver_to_gold",
    start_date=pendulum.now().subtract(days=1),
    schedule=None, 
    catchup=False,
    max_active_runs=1,
    doc_md="""
    Silver Layer → Gold Layer → AI Summary 통합 파이프라인

    실행 순서:
    1. dbt Transformation (Silver → Gold)
       - gold_llm_context 테이블 생성 (LLM용 자연어 컨텍스트)

    2. LLM Summary Generator (Gold → AI Summary)
       - Gemini 2.5 Flash로 배치 데이터 요약
       - gdelt_ai_summaries 테이블 저장
    """,
) as dag:

    # Task 1: dbt Transformation (Silver → Gold)
    dbt_transformation = BashOperator(
        task_id="dbt_transformation",
        bash_command=get_dbt_command("dbt build --target prod"),
    )

    # Task 2: LLM Summary Generator (Gold → AI Summary)
    llm_summary_generator = BashOperator(
        task_id="llm_summary_generator",
        bash_command=f"python {SPARK_JOBS_DIR}/gold/llm_summary_generator.py",
        execution_timeout=timedelta(minutes=10),
        env={
            "PYTHONUNBUFFERED": "1",
            "PYSPARK_SUBMIT_ARGS": SPARK_ARGS,
            "PYTHONPATH": SPARK_JOBS_DIR,
            "GOOGLE_API_KEY": os.getenv("GOOGLE_API_KEY", ""),
        },
        append_env=True,
        doc_md="""
        LLM Summary Generator
        - Gold Layer에서 배치 데이터 추출
        - Gemini 2.5 Flash로 한국어 요약 생성
        - Delta Lake에 저장 (gdelt_ai_summaries)
        """,
    )

    # 작업 순서 정의
    dbt_transformation >> llm_summary_generator
