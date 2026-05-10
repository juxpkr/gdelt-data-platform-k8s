from __future__ import annotations
from datetime import timedelta
from airflow.sdk import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.trigger_rule import TriggerRule
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


def _make_audit_pod(task_id: str, name: str, status: str, trigger_rule, err_msg: str | None = None):
    err_py = repr(err_msg)
    code = f"""
import trino, datetime, os, logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

started = datetime.datetime.fromisoformat(os.environ["STARTED_AT"])
now = datetime.datetime.now(datetime.timezone.utc)
duration = (now - started).total_seconds()

source_batch_id = os.environ.get("SOURCE_BATCH_ID", "").strip()
if source_batch_id:
    logger.info("SOURCE_BATCH_ID from conf: %s", source_batch_id)
    batch_id = source_batch_id
else:
    logger.warning("SOURCE_BATCH_ID not provided or empty — fallback to timestamp batch_id (E2E grouping will not work)")
    batch_id = now.strftime("%Y%m%d%H%M%S")

conn = trino.dbapi.connect(
    host="trino.gdelt.svc.cluster.local", port=8080,
    user="airflow-gold-audit", http_scheme="http"
)
cur = conn.cursor()

input_rows = 0
output_rows = 0
if "{status}" == "success":
    cur.execute(f"SELECT COUNT(*) FROM nessie.silver.gdelt_events_detailed WHERE source_batch_id = '{{batch_id}}'")
    input_rows = cur.fetchone()[0]
    cur.execute(f"SELECT COUNT(*) FROM nessie.gold.gold_llm_context WHERE source_batch_id = '{{batch_id}}'")
    output_rows = cur.fetchone()[0]

started_str = started.strftime("%Y-%m-%d %H:%M:%S.%f")
err_msg = {err_py}
err_sql = "NULL" if err_msg is None else "'" + err_msg.replace("'", "''") + "'"

cur.execute(f\"\"\"
    INSERT INTO nessie.audit.pipeline_batch_runs VALUES (
        '{{batch_id}}', 'gold', '{status}', {{input_rows}}, {{output_rows}},
        TIMESTAMP '{{started_str}}', CURRENT_TIMESTAMP, {{duration}},
        {{err_sql}}, CURRENT_TIMESTAMP
    )
\"\"\")
conn.commit()
logger.info("Gold audit inserted: batch_id=%s status={status} input_rows=%d output_rows=%d duration=%.1fs",
            batch_id, input_rows, output_rows, duration)
"""
    return KubernetesPodOperator(
        task_id=task_id,
        name=name,
        namespace="gdelt",
        image=DBT_IMAGE,
        image_pull_policy="IfNotPresent",
        cmds=["python", "-c"],
        arguments=[code],
        env_vars=[
            k8s.V1EnvVar(name="STARTED_AT",      value="{{ dag_run.start_date.isoformat() }}"),
            k8s.V1EnvVar(name="SOURCE_BATCH_ID", value="{{ dag_run.conf.get('source_batch_id', '') }}"),
        ],
        execution_timeout=timedelta(minutes=5),
        get_logs=True,
        is_delete_operator_pod=True,
        on_finish_action="delete_pod",
        in_cluster=True,
        trigger_rule=trigger_rule,
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
            k8s.V1EnvVar(name="SOURCE_BATCH_ID",  value="{{ dag_run.conf.get('source_batch_id', '') }}"),
        ],
        cmds=["sh", "-c"],
        arguments=[
            """
            set -eux
            mkdir -p "$DBT_TARGET_PATH" "$DBT_LOG_PATH"

            if [ -z "$SOURCE_BATCH_ID" ]; then
              echo "[WARN] SOURCE_BATCH_ID is empty — E2E batch tracking will not work" >&2
            fi

            dbt build \
              --target prod \
              --project-dir /app \
              --profiles-dir /app \
              --log-path "$DBT_LOG_PATH" \
              --target-path "$DBT_TARGET_PATH" \
              --vars "{source_batch_id: $SOURCE_BATCH_ID}"
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

    audit_gold_success = _make_audit_pod(
        "audit_gold_success", "audit-gold-success",
        status="success", trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    audit_gold_failed = _make_audit_pod(
        "audit_gold_failed", "audit-gold-failed",
        status="failed", trigger_rule=TriggerRule.ONE_FAILED,
        err_msg="dbt build failed",
    )

    dbt_transformation >> [audit_gold_success, audit_gold_failed]
