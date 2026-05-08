import logging
from db.trino import fetch_one

logger = logging.getLogger(__name__)

FRESHNESS_THRESHOLD_SECONDS = 1800


def collect_freshness_metrics(e2e_available: bool) -> tuple[float, int]:
    """freshness_seconds와 pipeline health(1/0)를 반환한다.

    health=1 조건: E2E complete batch가 존재하고 freshness < 1800s
    e2e_available=False이면 health는 항상 0 (E2E 기준 미충족)
    """
    freshness = _get_freshness_seconds(e2e_available)
    health = 1 if (e2e_available and freshness < FRESHNESS_THRESHOLD_SECONDS) else 0
    return freshness, health


def _get_freshness_seconds(e2e_available: bool) -> float:
    """E2E complete batch 기준 freshness. E2E batch 없으면 임의 success 기준 fallback."""
    if e2e_available:
        sql = """
        WITH e2e_batch AS (
          SELECT batch_id
          FROM nessie.audit.pipeline_batch_runs
          WHERE stage IN ('bronze', 'silver', 'gold')
            AND status = 'success'
          GROUP BY batch_id
          HAVING COUNT(DISTINCT stage) = 3
          ORDER BY MAX(finished_at) DESC
          LIMIT 1
        )
        SELECT
          to_unixtime(CURRENT_TIMESTAMP) - to_unixtime(MAX(finished_at)) AS freshness_seconds
        FROM nessie.audit.pipeline_batch_runs
        WHERE batch_id = (SELECT batch_id FROM e2e_batch)
          AND status = 'success'
        """
    else:
        logger.warning("E2E batch unavailable — freshness based on any success row (fallback)")
        sql = """
        SELECT
          to_unixtime(CURRENT_TIMESTAMP) - to_unixtime(MAX(finished_at)) AS freshness_seconds
        FROM nessie.audit.pipeline_batch_runs
        WHERE status = 'success'
        """
    row = fetch_one(sql)
    if row is None or row["freshness_seconds"] is None:
        return float("inf")
    return float(row["freshness_seconds"])
