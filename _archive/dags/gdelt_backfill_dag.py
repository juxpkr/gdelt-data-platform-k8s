from __future__ import annotations
from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk.bases.operator import chain
import pendulum

SPARK_JOBS_DIR = "/opt/airflow/spark-jobs"

BACKFILL_START_DATE = datetime(2025, 9, 26, 0, 0)
BACKFILL_END_DATE = datetime(2025, 9, 26, 1, 0)

SPARK_ENV = {
    "PYTHONPATH": SPARK_JOBS_DIR,
    "MINIO_ENDPOINT": "http://minio.gdelt.svc.cluster.local:9000",
    "MINIO_ROOT_USER": "minioadmin",
    "MINIO_ROOT_PASSWORD": "minioadmin",
    "KAFKA_BOOTSTRAP_SERVERS": "kafka.gdelt.svc.cluster.local:9092",
    "KAFKA_TOPIC_GDELT_EVENTS": "gdelt_events_bronze",
    "KAFKA_TOPIC_GDELT_MENTIONS": "gdelt_mentions_bronze",
    "KAFKA_TOPIC_GDELT_GKG": "gdelt_gkg_bronze",
    "SPARK_DRIVER_MEMORY": "3g",
}


def _generate_15min_slots() -> list[tuple[datetime, datetime]]:
    slots = []
    current = BACKFILL_START_DATE
    while current < BACKFILL_END_DATE:
        slots.append((current, current + timedelta(minutes=15)))
        current += timedelta(minutes=15)
    return slots


with DAG(
    dag_id="gdelt_backfill_pipeline",
    start_date=pendulum.now(tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    tags=["gdelt", "backfill"],
) as dag:

    producer_tasks = []
    consumer_tasks = []
    processor_tasks = []

    for start_dt, end_dt in _generate_15min_slots():
        batch_id = start_dt.strftime("%Y%m%d_%H%M")

        producer_tasks.append(BashOperator(
            task_id=f"producer_{batch_id}",
            bash_command=(
                f"python {SPARK_JOBS_DIR}/ingestion/gdelt_backfill_producer.py "
                f"--backfill-start '{start_dt.isoformat()}' "
                f"--backfill-end '{end_dt.isoformat()}'"
            ),
            execution_timeout=timedelta(minutes=20),
            env=SPARK_ENV,
            append_env=True,
        ))

        consumer_tasks.append(BashOperator(
            task_id=f"bronze_consumer_{batch_id}",
            bash_command=f"python {SPARK_JOBS_DIR}/ingestion/gdelt_backfill_bronze_consumer.py",
            execution_timeout=timedelta(minutes=20),
            env=SPARK_ENV,
            append_env=True,
        ))

        processor_tasks.append(BashOperator(
            task_id=f"silver_processor_{batch_id}",
            bash_command=(
                f"python {SPARK_JOBS_DIR}/processing/gdelt_backfill_silver_processor.py "
                f"'{start_dt.isoformat()}' '{end_dt.isoformat()}'"
            ),
            execution_timeout=timedelta(minutes=30),
            env=SPARK_ENV,
            append_env=True,
        ))

    chain(producer_tasks, consumer_tasks, processor_tasks)
