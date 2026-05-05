from __future__ import annotations
from datetime import timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
import pendulum

SPARK_JOBS_DIR = "/opt/airflow/spark-jobs"

SPARK_ENV = {
    "PYTHONPATH": SPARK_JOBS_DIR,
    "MINIO_ENDPOINT": "http://minio.gdelt.svc.cluster.local:9000",
    "MINIO_ROOT_USER": "minioadmin",
    "MINIO_ROOT_PASSWORD": "minioadmin",
    "SPARK_DRIVER_MEMORY": "3g",
}

with DAG(
    dag_id="gdelt_lifecycle_audit",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["gdelt", "audit"],
) as dag:

    lifecycle_audit = BashOperator(
        task_id="lifecycle_audit",
        bash_command=f"python {SPARK_JOBS_DIR}/audit/gdelt_lifecycle_audit.py",
        execution_timeout=timedelta(minutes=30),
        env=SPARK_ENV,
        append_env=True,
    )
