from __future__ import annotations
from datetime import timedelta
from airflow.sdk import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
import pendulum

DBT_IMAGE = "juxpkr/gdelt-dbt:trino1.10.1-core1.11.8-python3.12-arm64"
DBT_PROJECT_HOST_PATH = "/home/ubuntu/gdelt-data-platform-k8s/apps/dbt"

dbt_volume = k8s.V1Volume(
    name="dbt-project",
    host_path=k8s.V1HostPathVolumeSource(
        path=DBT_PROJECT_HOST_PATH,
        type="Directory",
    ),
)

dbt_mount = k8s.V1VolumeMount(
    name="dbt-project",
    mount_path="/app",
    read_only=True,
)

with DAG(
    dag_id="gdelt_silver_to_gold",
    start_date=pendulum.now().subtract(days=1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["gdelt", "gold"],
) as dag:

    dbt_transformation = KubernetesPodOperator(
        task_id="dbt_transformation",
        name="dbt-transformation",
        namespace="gdelt",
        image=DBT_IMAGE,
        image_pull_policy="IfNotPresent",
        env_vars=[
            k8s.V1EnvVar(name="HOME",             value="/tmp"),
            k8s.V1EnvVar(name="DBT_PROFILES_DIR", value="/app"),
            k8s.V1EnvVar(name="DBT_TARGET_PATH",  value="/tmp/dbt-target"),
            k8s.V1EnvVar(name="DBT_LOG_PATH",     value="/tmp/dbt-logs"),
        ],
        cmds=["sh", "-c"],
        arguments=[
            """
            set -eux
            mkdir -p "$DBT_TARGET_PATH" "$DBT_LOG_PATH"
            dbt build \
              --target prod \
              --project-dir /app \
              --profiles-dir /app \
              --log-path "$DBT_LOG_PATH" \
              --target-path "$DBT_TARGET_PATH"
            """
        ],
        volumes=[dbt_volume],
        volume_mounts=[dbt_mount],
        execution_timeout=timedelta(minutes=60),
        get_logs=True,
        is_delete_operator_pod=True,
        on_finish_action="delete_pod",
        in_cluster=True,
    )
