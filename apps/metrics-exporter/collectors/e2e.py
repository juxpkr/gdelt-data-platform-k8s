import logging
from db.trino import fetch_one

logger = logging.getLogger(__name__)

_SQL = """
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
e2e_rows AS (
  SELECT stage, output_rows, started_at, finished_at
  FROM nessie.audit.pipeline_batch_runs
  WHERE batch_id = (SELECT batch_id FROM e2e_batch)
    AND status = 'success'
),
failed_count AS (
  SELECT COUNT(*) AS cnt
  FROM nessie.audit.pipeline_batch_runs
  WHERE status = 'failed'
    AND batch_id = (
      SELECT batch_id
      FROM nessie.audit.pipeline_batch_runs
      GROUP BY batch_id
      ORDER BY MAX(finished_at) DESC
      LIMIT 1
    )
)
SELECT
  to_unixtime(MAX(e.finished_at)) - to_unixtime(MIN(e.started_at)) AS e2e_duration_seconds,
  CASE
    WHEN MAX(CASE WHEN e.stage = 'bronze' THEN e.output_rows END) = 0 THEN 0.0
    ELSE CAST(MAX(CASE WHEN e.stage = 'silver' THEN e.output_rows END) AS DOUBLE)
         / MAX(CASE WHEN e.stage = 'bronze' THEN e.output_rows END)
  END AS retention_ratio,
  (SELECT cnt FROM failed_count) AS failed_stage_count
FROM e2e_rows e
"""


def collect_e2e_extra_metrics() -> tuple[float, float, int]:
    """(e2e_duration_seconds, retention_ratio, failed_stage_count) 반환.

    E2E complete batch 없으면 (0.0, 0.0, 0) 반환.
    """
    row = fetch_one(_SQL)
    if row is None or row.get("e2e_duration_seconds") is None:
        logger.warning("E2E extra metrics: no data returned")
        return 0.0, 0.0, 0
    return (
        float(row["e2e_duration_seconds"]),
        float(row["retention_ratio"] or 0.0),
        int(row["failed_stage_count"] or 0),
    )
