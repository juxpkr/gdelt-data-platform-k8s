from __future__ import annotations
from datetime import timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
import pendulum

SPARK_JOBS_DIR = "/opt/airflow/spark-jobs"
SPARK_IMAGE = "juxpkr/gdelt-spark:spark3.5.6-iceberg1.10.1-awsbundle-s3a-java17-arm64"

SPARK_ENV = [
    k8s.V1EnvVar(name="PYTHONPATH",              value=SPARK_JOBS_DIR),
    k8s.V1EnvVar(name="MINIO_ENDPOINT",          value="http://minio.gdelt.svc.cluster.local:9000"),
    k8s.V1EnvVar(name="MINIO_ROOT_USER",         value="minioadmin"),
    k8s.V1EnvVar(name="MINIO_ROOT_PASSWORD",     value="minioadmin"),
    k8s.V1EnvVar(name="KAFKA_BOOTSTRAP_SERVERS", value="kafka.gdelt.svc.cluster.local:9092"),
    k8s.V1EnvVar(name="KAFKA_TOPIC_GDELT_EVENTS",   value="gdelt_events_bronze"),
    k8s.V1EnvVar(name="KAFKA_TOPIC_GDELT_MENTIONS", value="gdelt_mentions_bronze"),
    k8s.V1EnvVar(name="KAFKA_TOPIC_GDELT_GKG",      value="gdelt_gkg_bronze"),
    k8s.V1EnvVar(name="SPARK_DRIVER_MEMORY",     value="2g"),
    k8s.V1EnvVar(name="PROJECT_ROOT",            value=SPARK_JOBS_DIR),
]

spark_jobs_volume = k8s.V1Volume(
    name="spark-jobs",
    host_path=k8s.V1HostPathVolumeSource(
        path="/home/ubuntu/gdelt-data-platform-k8s/apps/spark-jobs",
        type="Directory",
    ),
)

spark_jobs_mount = k8s.V1VolumeMount(
    name="spark-jobs",
    mount_path=SPARK_JOBS_DIR,
    read_only=True,
)

with DAG(
    dag_id="gdelt_bronze_to_silver",
    start_date=pendulum.now().subtract(days=1),
    schedule="0,15,30,45 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["gdelt", "ingestion"],
) as dag:

    gdelt_producer = BashOperator(
        task_id="gdelt_producer",
        bash_command=f"python {SPARK_JOBS_DIR}/ingestion/gdelt_producer.py --logical-date '{{{{ data_interval_start }}}}'",
        execution_timeout=timedelta(minutes=30),
        env={
            "PYTHONPATH": SPARK_JOBS_DIR,
            "KAFKA_BOOTSTRAP_SERVERS": "kafka.gdelt.svc.cluster.local:9092",
        },
        append_env=True,
    )

    bronze_consumer = KubernetesPodOperator(
        task_id="bronze_consumer",
        name="gdelt-bronze-consumer",
        namespace="gdelt",
        image=SPARK_IMAGE,
        image_pull_policy="IfNotPresent",
        env_vars=SPARK_ENV,
        cmds=["python"],
        arguments=[f"{SPARK_JOBS_DIR}/ingestion/gdelt_bronze_consumer.py"],
        volumes=[spark_jobs_volume],
        volume_mounts=[spark_jobs_mount],
        execution_timeout=timedelta(minutes=30),
        get_logs=True,
        is_delete_operator_pod=True,
        on_finish_action="delete_pod",
        in_cluster=True,
    )

    silver_processor = KubernetesPodOperator(
        task_id="silver_processor",
        name="gdelt-silver-processor",
        namespace="gdelt",
        image=SPARK_IMAGE,
        image_pull_policy="IfNotPresent",
        env_vars=SPARK_ENV,
        cmds=["python"],
        arguments=[
            f"{SPARK_JOBS_DIR}/processing/gdelt_silver_processor.py",
            "{{ data_interval_start.strftime('%Y%m%d%H%M%S') }}",
        ],
        volumes=[spark_jobs_volume],
        volume_mounts=[spark_jobs_mount],
        execution_timeout=timedelta(minutes=60),
        get_logs=True,
        is_delete_operator_pod=True,
        on_finish_action="delete_pod",
        in_cluster=True,
    )

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold_pipeline",
        trigger_dag_id="gdelt_silver_to_gold",
        wait_for_completion=False,
    )

    gdelt_producer >> bronze_consumer >> silver_processor >> trigger_gold
