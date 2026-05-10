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
  (SELECT batch_id FROM e2e_batch)                                              AS current_batch_id,
  to_unixtime(MAX(e.finished_at)) - to_unixtime(MIN(e.started_at))             AS e2e_duration_seconds,
  CASE
    WHEN MAX(CASE WHEN e.stage = 'bronze' THEN e.output_rows END) = 0 THEN 0.0
    ELSE CAST(MAX(CASE WHEN e.stage = 'silver' THEN e.output_rows END) AS DOUBLE)
         / MAX(CASE WHEN e.stage = 'bronze' THEN e.output_rows END)
  END                                                                            AS retention_ratio,
  (SELECT cnt FROM failed_count)                                                 AS failed_stage_count
FROM e2e_rows e
"""

_GOLD_TOTAL_SQL = "SELECT COUNT(*) AS total FROM nessie.gold.gold_llm_context"


def collect_e2e_extra_metrics() -> tuple[float, float, float, int]:
    """(e2e_duration_seconds, retention_ratio, current_batch_id_numeric, failed_stage_count) 반환.

    E2E complete batch 없으면 (0.0, 0.0, 0.0, 0) 반환.
    """
    row = fetch_one(_SQL)
    if row is None or row.get("e2e_duration_seconds") is None:
        logger.warning("E2E extra metrics: no data returned")
        return 0.0, 0.0, 0.0, 0

    batch_id_str = row.get("current_batch_id") or ""
    try:
        batch_id_numeric = float(batch_id_str) if batch_id_str else 0.0
    except (ValueError, TypeError):
        batch_id_numeric = 0.0

    return (
        float(row["e2e_duration_seconds"]),
        float(row["retention_ratio"] or 0.0),
        batch_id_numeric,
        int(row["failed_stage_count"] or 0),
    )


def collect_gold_total_rows() -> int:
    """gold 테이블 전체 row 수. 실패 시 -1 반환 (caller에서 exporter_up=0 처리)."""
    row = fetch_one(_GOLD_TOTAL_SQL)
    if row is None:
        return -1
    return int(row["total"] or 0)
