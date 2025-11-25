"""
GDELT Bronze Layer Pipeline DAG (K8s 16GB)
GDELT 3개 데이터타입 수집 → Kafka → Bronze Layer (Silver는 주석처리)
"""

from __future__ import annotations
from datetime import timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import os
import pendulum

REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")

SPARK_JOBS_DIR = "/opt/airflow/spark-jobs"
SPARK_IMAGE = "juxpkr/geoevent-spark-base:2.2"

# 16GB 리소스 설정
SPARK_CONF_16GB = {
    "spark.driver.memory": "1g",
    "spark.executor.memory": "1g",
    "spark.executor.instances": "1",
    "spark.sql.shuffle.partitions": "10", # 파티션 줄여서 메모리 아낌
    "spark.default.parallelism": "4",
    "spark.sql.adaptive.enabled": "true",
}

with DAG(
    dag_id="gdelt_bronze_to_silver",
    start_date=pendulum.datetime(2025, 11, 20, tz="Asia/Seoul"),
    schedule="0,15,30,45 * * * *",  # 정각 기준 15분 단위 실행
    catchup=False,
    max_active_runs=1,
    doc_md="""
    GDELT End-to-End Pipeline
    - 목적: GDELT 데이터 수집 → Bronze Layer → Silver Layer 파이프라인
    - 데이터: 최신 15분 배치 데이터 (Events, Mentions, GKG)
    - 결과: MinIO Silver Layer에 정제된 분석용 데이터 저장

    실행 순서:
    1. GDELT Producer → Kafka (3개 토픽: events, mentions, gkg)
    2. Bronze Consumer → MinIO Bronze Layer (Delta 형식)
    3. Silver Processor → MinIO Silver Layer (Events, Events Detailed)
    """,
) as dag:

    # Task 1: GDELT Producer → (Bash -> Python Script)
    gdelt_producer = BashOperator(
        task_id="gdelt_producer",
        bash_command=f"python {SPARK_JOBS_DIR}/ingestion/gdelt_producer.py --logical-date '{{{{ data_interval_start }}}}'",
        execution_timeout=timedelta(minutes=30),
        env={"PYTHONPATH": SPARK_JOBS_DIR}, 
        append_env=True, 
        doc_md="""
        GDELT 3-Way Producer
        - Events, Mentions, GKG 데이터를 각각 수집
        - 3개 Kafka 토픽에 분리 저장 (gdelt_events_bronze, gdelt_mentions_bronze, gdelt_gkg_bronze)
        - 최신 15분 배치 데이터 처리
        """,
    )

    # Task 2: Bronze Consumer → MinIO Bronze Layer
    bronze_consumer = SparkSubmitOperator(
        task_id="bronze_consumer",
        conn_id="spark_default",
        application=f"{SPARK_JOBS_DIR}/ingestion/gdelt_bronze_consumer.py",
        conf=SPARK_CONF_16GB,
        jars=f"{SPARK_JOBS_DIR}/jars/delta-core_2.12-2.4.0.jar,{SPARK_JOBS_DIR}/jars/delta-storage-2.4.0.jar,{SPARK_JOBS_DIR}/jars/elasticsearch-spark-30_2.12-7.17.3.jar",
        env_vars={
            "REDIS_HOST": "airflow-redis",
            "REDIS_PORT": "6379",
            "REDIS_PASSWORD": REDIS_PASSWORD,
            "PYTHONPATH": SPARK_JOBS_DIR,
        },
    )

    ## Task 3: Lifecycle Consolidator
    #consolidate_lifecycle = SparkSubmitOperator(
    #    task_id="consolidate_lifecycle",
    #    conn_id="spark_default",
    #    packages="io.delta:delta-core_2.12:2.4.0",
    #    application=f"{SPARK_JOBS_DIR}/audit/lifecycle_consolidator.py",
    #    conf=SPARK_CONF_16GB,
    #    doc_md="""
    #    Lifecycle Consolidator
    #    - Staging 테이블 (lifecycle_staging_event, lifecycle_staging_gkg) 데이터를 Main lifecycle 테이블로 통합
    #    - WAITING 상태 이벤트를 Silver Processor가 읽을 수 있도록 준비
    #    - Staging 테이블 정리
    #    """,
    #)

    ## Task 4: Silver Processing (주석처리 - ES 연동 후 최적화)
    #silver_processor = SparkSubmitOperator(
    #    task_id="silver_processor",
    #    conn_id="spark_default",
    #    application=f"{SPARK_JOBS_DIR}/processing/gdelt_silver_processor.py",
    #    conf=SPARK_CONF_16GB,
    #    packages="io.delta:delta-core_2.12:2.4.0",
    #    env_vars={
    #        "REDIS_HOST": "airflow-redis",
    #        "REDIS_PORT": "6379",
    #        "PYTHONPATH": SPARK_JOBS_DIR,
    #    },
    #    # Airflow의 작업 시간 구간을 Spark 코드의 인자로 전달
    #    application_args=["{{ data_interval_start }}", "{{ data_interval_end }}"],
    #    doc_md="""
    #    Silver Layer Processing
    #    - Bronze Layer → Silver Layer 데이터 변환
    #    - 3-Way 조인 (Events + Mentions + GKG)
    #    - Delta Lake 파티션 저장 (default.gdelt_events, default.gdelt_events_detailed)
    #    """,
    #)

    ## Silver 작업이 성공하면, dbt DAG을 호출 (주석처리)
    #trigger_dbt = TriggerDagRunOperator(
    #    task_id="trigger_dbt_gold_pipeline",
    #    trigger_dag_id="gdelt_silver_to_gold",
    #    wait_for_completion=False,
    #)

    # Task 의존성 정의: Producer → Bronze (Silver/dbt 주석처리)
    (
        gdelt_producer
        >> bronze_consumer
        #>> consolidate_lifecycle
        #>> silver_processor
        #>> trigger_dbt
    )
