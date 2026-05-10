import logging
from db.trino import fetch_one

logger = logging.getLogger(__name__)

_E2E_BATCH_SQL = """
SELECT batch_id
FROM nessie.audit.pipeline_batch_runs
WHERE stage IN ('bronze', 'silver', 'gold')
  AND status = 'success'
GROUP BY batch_id
HAVING COUNT(DISTINCT stage) = 3
ORDER BY MAX(finished_at) DESC
LIMIT 1
"""

_QUALITY_SQL = """
SELECT
  COUNT(*) - COUNT(DISTINCT global_event_id)                                    AS dedup_violations,
  SUM(CASE WHEN global_event_id   IS NULL THEN 1 ELSE 0 END)
  + SUM(CASE WHEN event_date      IS NULL THEN 1 ELSE 0 END)
  + SUM(CASE WHEN event_code      IS NULL THEN 1 ELSE 0 END)
  + SUM(CASE WHEN source_batch_id IS NULL THEN 1 ELSE 0 END)                   AS core_null_count,
  CASE WHEN COUNT(*) = 0 THEN 0.0
       ELSE CAST(SUM(CASE WHEN mention_source_name IS NOT NULL THEN 1 ELSE 0 END) AS double)
            / COUNT(*)
  END                                                                            AS mention_join_ratio,
  CASE WHEN COUNT(*) = 0 THEN 0.0
       ELSE CAST(SUM(CASE WHEN v2_persons IS NOT NULL
                            OR v2_organizations IS NOT NULL
                            OR v2_enhanced_themes IS NOT NULL
                           THEN 1 ELSE 0 END) AS double)
            / COUNT(*)
  END                                                                            AS gkg_coverage_ratio
FROM nessie.silver.gdelt_events_detailed
WHERE source_batch_id = '{batch_id}'
"""

_SAFE_DEFAULTS = {
    "dedup_violations": 0.0,
    "core_null_count": 0.0,
    "mention_join_ratio": 0.0,
    "gkg_coverage_ratio": 0.0,
}


def collect_silver_quality_metrics() -> dict:
    """최신 E2E complete batch 기준 silver quality metric 반환.

    E2E batch 없거나 쿼리 실패 시 safe default(0.0) 반환 — exporter는 죽지 않음.
    """
    try:
        batch_row = fetch_one(_E2E_BATCH_SQL)
        if batch_row is None or not batch_row.get("batch_id"):
            logger.warning("silver_quality: no E2E complete batch found — returning defaults")
            return _SAFE_DEFAULTS.copy()

        batch_id = batch_row["batch_id"]
        row = fetch_one(_QUALITY_SQL.format(batch_id=batch_id))
        if row is None:
            logger.warning("silver_quality: quality query returned None for batch_id=%s", batch_id)
            return _SAFE_DEFAULTS.copy()

        return {
            "dedup_violations":   float(row["dedup_violations"]   or 0),
            "core_null_count":    float(row["core_null_count"]    or 0),
            "mention_join_ratio": float(row["mention_join_ratio"] or 0),
            "gkg_coverage_ratio": float(row["gkg_coverage_ratio"] or 0),
        }
    except Exception as e:
        logger.error("silver_quality: collector error — %s", e)
        return _SAFE_DEFAULTS.copy()
