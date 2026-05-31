import logging
import time
from fastapi import APIRouter, HTTPException
from db.trino import fetch_one, fetch_all
from models.schemas import StatsResponse, TopEventCodeItem
from cache import get_cached, set_cached, make_key

logger = logging.getLogger(__name__)
router = APIRouter()

_WINDOW_DAYS = 7


@router.get("/stats", response_model=StatsResponse)
def get_stats():
    key = make_key("stats")
    cached = get_cached(key)
    if cached is not None:
        return cached
    try:
        t0 = time.monotonic()

        batch_row = fetch_one("""
            SELECT source_batch_id, COUNT(*) AS event_count
            FROM nessie.bronze.gdelt_events
            GROUP BY source_batch_id
            ORDER BY source_batch_id DESC
            LIMIT 1
        """)

        # audit 테이블 집계 — gold 전체 스캔 없이 누적 처리량 계산
        audit_row = fetch_one("""
            SELECT COALESCE(SUM(output_rows), 0) AS total_processed_events
            FROM nessie.audit.pipeline_batch_runs
            WHERE stage = 'gold' AND status = 'success'
        """)
        total_processed = int(audit_row["total_processed_events"]) if audit_row else 0

        # event_date 파티션 기준 7일 필터 — 파티션 pruning으로 실제 스캔 감소
        gold_row = fetch_one(f"""
            SELECT
                COUNT(*)                                                          AS recent_events,
                ROUND(AVG(CASE WHEN avg_tone IS NOT NULL THEN avg_tone END), 4)  AS avg_tone,
                COUNT(CASE WHEN avg_tone < -2 AND num_mentions > 10 THEN 1 END)  AS high_risk_count
            FROM nessie.gold.gold_llm_context
            WHERE event_date >= current_date - interval '{_WINDOW_DAYS}' day
        """)
        recent_events   = int(gold_row["recent_events"])   if gold_row else 0
        avg_tone_val    = float(gold_row["avg_tone"])      if gold_row and gold_row["avg_tone"] is not None else None
        high_risk_count = int(gold_row["high_risk_count"]) if gold_row else 0

        code_rows = fetch_all(f"""
            SELECT
                g.event_code,
                COALESCE(ec.description, g.event_code) AS event_code_name,
                COUNT(*) AS event_count
            FROM nessie.gold.gold_llm_context g
            LEFT JOIN nessie.seeds.event_detail_codes ec ON g.event_code = ec.code
            WHERE g.event_code IS NOT NULL
              AND g.event_date >= current_date - interval '{_WINDOW_DAYS}' day
            GROUP BY g.event_code, ec.description
            ORDER BY event_count DESC
            LIMIT 8
        """)

        top_codes = [
            TopEventCodeItem(
                event_code=r["event_code"],
                event_code_name=r["event_code_name"],
                event_count=int(r["event_count"]),
            )
            for r in (code_rows or [])
        ]

        logger.info("stats built in %.3fs", time.monotonic() - t0)

        result = StatsResponse(
            total_processed_events=total_processed,
            recent_events=recent_events,
            latest_batch_id=batch_row["source_batch_id"] if batch_row else None,
            latest_batch_event_count=int(batch_row["event_count"]) if batch_row else None,
            avg_tone=avg_tone_val,
            high_risk_count=high_risk_count,
            window_days=_WINDOW_DAYS,
            top_event_codes=top_codes,
        )
        set_cached(key, result)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
