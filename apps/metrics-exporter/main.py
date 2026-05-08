import logging

from fastapi import FastAPI
from fastapi.responses import Response
from prometheus_client import CollectorRegistry, Gauge, generate_latest, CONTENT_TYPE_LATEST

from collectors.pipeline import collect_pipeline_metrics
from collectors.freshness import collect_freshness_metrics
from collectors.e2e import collect_e2e_extra_metrics

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="GDELT Metrics Exporter")

registry = CollectorRegistry()

_exporter_up = Gauge("gdelt_exporter_up", "1 if Trino is reachable", registry=registry)
_freshness_seconds = Gauge(
    "gdelt_pipeline_freshness_seconds",
    "Seconds since last successful E2E batch finished (fallback: any success)",
    registry=registry,
)
_pipeline_health = Gauge(
    "gdelt_pipeline_health",
    "1 if E2E complete batch exists and freshness < 1800s",
    registry=registry,
)
_e2e_complete = Gauge(
    "gdelt_e2e_complete_batch_available",
    "1 if a complete E2E batch (bronze+silver+gold success, same batch_id) exists",
    registry=registry,
)
_output_rows = Gauge(
    "gdelt_latest_batch_output_rows",
    "Output rows of the latest batch per stage",
    ["stage"],
    registry=registry,
)
_stage_success = Gauge(
    "gdelt_pipeline_stage_success",
    "1 if the latest batch for this stage succeeded",
    ["stage"],
    registry=registry,
)
_stage_duration = Gauge(
    "gdelt_pipeline_stage_duration_seconds",
    "Duration of the latest batch for this stage",
    ["stage"],
    registry=registry,
)
_e2e_duration = Gauge(
    "gdelt_e2e_duration_seconds",
    "Wall-clock seconds from bronze start to gold finish for the latest E2E complete batch",
    registry=registry,
)
_retention_ratio = Gauge(
    "gdelt_bronze_to_silver_retention_ratio",
    "silver output_rows / bronze output_rows for the latest E2E complete batch",
    registry=registry,
)
_failed_stage_count = Gauge(
    "gdelt_failed_stage_count",
    "Number of failed stages in the latest batch_id",
    registry=registry,
)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/metrics")
def metrics():
    try:
        rows, e2e_available = collect_pipeline_metrics()
        for row in rows:
            stage = row["stage"]
            _output_rows.labels(stage=stage).set(row["output_rows"] or 0)
            _stage_success.labels(stage=stage).set(1 if row["status"] == "success" else 0)
            _stage_duration.labels(stage=stage).set(row["duration_seconds"] or 0)

        _e2e_complete.set(1 if e2e_available else 0)

        freshness, health_val = collect_freshness_metrics(e2e_available)
        _freshness_seconds.set(freshness)
        _pipeline_health.set(health_val)

        e2e_dur, retention, failed_cnt = collect_e2e_extra_metrics()
        _e2e_duration.set(e2e_dur)
        _retention_ratio.set(retention)
        _failed_stage_count.set(failed_cnt)

        _exporter_up.set(1)
        logger.info(
            "Metrics collected — e2e_available=%s freshness=%.0fs health=%d "
            "e2e_dur=%.1fs retention=%.3f failed=%d",
            e2e_available, freshness, health_val, e2e_dur, retention, failed_cnt,
        )
    except Exception as e:
        logger.error("Collector error: %s", e)
        _exporter_up.set(0)

    return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)
