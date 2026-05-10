from fastapi import APIRouter, HTTPException
from db.trino import fetch_one, fetch_all
from models.schemas import StatsResponse, TopEventCodeItem
from cache import get_cached, set_cached, make_key

router = APIRouter()


@router.get("/stats", response_model=StatsResponse)
def get_stats():
    key = make_key("stats")
    cached = get_cached(key)
    if cached is not None:
        return cached
    try:
        total_row = fetch_one("SELECT COUNT(*) AS total_events FROM nessie.gold.gold_llm_context")
        total_events = int(total_row["total_events"]) if total_row else 0

        batch_row = fetch_one("""
            SELECT source_batch_id, COUNT(*) AS event_count
            FROM nessie.bronze.gdelt_events
            GROUP BY source_batch_id
            ORDER BY source_batch_id DESC
            LIMIT 1
        """)

        tone_row = fetch_one("""
            SELECT ROUND(AVG(avg_tone), 4) AS avg_tone
            FROM nessie.gold.gold_llm_context
            WHERE avg_tone IS NOT NULL
        """)

        risk_row = fetch_one("""
            SELECT COUNT(*) AS high_risk_count
            FROM nessie.gold.gold_llm_context
            WHERE avg_tone < -2
              AND num_mentions > 10
        """)

        code_rows = fetch_all("""
            SELECT
                g.event_code,
                COALESCE(ec.description, g.event_code) AS event_code_name,
                COUNT(*) AS event_count
            FROM nessie.gold.gold_llm_context g
            LEFT JOIN nessie.seeds.event_detail_codes ec ON g.event_code = ec.code
            WHERE g.event_code IS NOT NULL
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

        result = StatsResponse(
            total_events=total_events,
            latest_batch_id=batch_row["source_batch_id"] if batch_row else None,
            latest_batch_event_count=int(batch_row["event_count"]) if batch_row else None,
            avg_tone=float(tone_row["avg_tone"]) if tone_row and tone_row["avg_tone"] is not None else None,
            high_risk_count=int(risk_row["high_risk_count"]) if risk_row else None,
            top_event_codes=top_codes,
        )
        set_cached(key, result)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
