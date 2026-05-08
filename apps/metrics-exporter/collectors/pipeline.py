import logging
from db.trino import fetch_all

logger = logging.getLogger(__name__)


def collect_pipeline_metrics() -> tuple[list[dict], bool]:
    """E2E complete batch(bronze/silver/gold 모두 success) 기준으로 stage metrics 수집.

    Returns:
        (rows, e2e_available)
        e2e_available=True: 동일 batch_id로 3 stage 모두 success인 batch 존재
        e2e_available=False: fallback — stage별 최신 row
    """
    e2e_sql = """
    WITH e2e_batch AS (
      SELECT batch_id
      FROM nessie.audit.pipeline_batch_runs
      WHERE stage IN ('bronze', 'silver', 'gold')
        AND status = 'success'
      GROUP BY batch_id
      HAVING COUNT(DISTINCT stage) = 3
      ORDER BY MAX(finished_at) DESC
      LIMIT 1
    ),
    stage_latest AS (
      SELECT
        a.stage,
        a.status,
        a.output_rows,
        a.duration_seconds,
        ROW_NUMBER() OVER (PARTITION BY a.stage ORDER BY a.finished_at DESC) AS rn
      FROM nessie.audit.pipeline_batch_runs a
      INNER JOIN e2e_batch ON a.batch_id = e2e_batch.batch_id
    )
    SELECT stage, status, output_rows, duration_seconds
    FROM stage_latest
    WHERE rn = 1
    """
    rows = fetch_all(e2e_sql)
    if rows:
        logger.info("E2E complete batch found — %d stage rows returned", len(rows))
        return rows, True

    logger.warning("No E2E complete batch found — falling back to per-stage latest")
    fallback_sql = """
    WITH ranked AS (
      SELECT
        stage,
        status,
        output_rows,
        duration_seconds,
        ROW_NUMBER() OVER (PARTITION BY stage ORDER BY finished_at DESC) AS rn
      FROM nessie.audit.pipeline_batch_runs
    )
    SELECT stage, status, output_rows, duration_seconds
    FROM ranked
    WHERE rn = 1
    """
    return fetch_all(fallback_sql), False
