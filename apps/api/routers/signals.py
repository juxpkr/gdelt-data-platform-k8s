from fastapi import APIRouter, HTTPException, Query
from db.trino import fetch_all
from models.schemas import SignalItem, SignalsResponse

router = APIRouter()


@router.get("/signals", response_model=SignalsResponse)
def get_signals(limit: int = Query(default=30, ge=1, le=100)):
    sql = f"""
        SELECT
            g.global_event_id,
            CAST(g.event_date AS varchar) AS event_date,
            g.action_geo_fullname,
            g.event_code,
            COALESCE(ec.description, g.event_code) AS event_code_name,
            g.avg_tone,
            g.goldstein_scale,
            g.num_mentions,
            g.num_articles,
            g.source_url,
            ROUND(
                (ABS(COALESCE(g.avg_tone, 0)) * LN(COALESCE(g.num_mentions, 1) + 1)) /
                NULLIF(COALESCE(g.goldstein_scale, 0) + 10, 0),
                4
            ) AS risk_score
        FROM nessie.gold.gold_llm_context g
        LEFT JOIN nessie.seeds.event_detail_codes ec ON g.event_code = ec.code
        WHERE g.avg_tone IS NOT NULL
          AND g.avg_tone < -0.5
        ORDER BY risk_score DESC
        LIMIT {limit}
    """
    try:
        rows = fetch_all(sql)
        signals = [
            SignalItem(
                global_event_id=r["global_event_id"],
                event_date=r["event_date"],
                action_geo_fullname=r["action_geo_fullname"],
                event_code=r["event_code"],
                event_code_name=r["event_code_name"],
                avg_tone=float(r["avg_tone"]) if r["avg_tone"] is not None else None,
                goldstein_scale=float(r["goldstein_scale"]) if r["goldstein_scale"] is not None else None,
                num_mentions=int(r["num_mentions"]) if r["num_mentions"] is not None else None,
                num_articles=int(r["num_articles"]) if r["num_articles"] is not None else None,
                source_url=r["source_url"],
                risk_score=float(r["risk_score"]) if r["risk_score"] is not None else None,
            )
            for r in (rows or [])
        ]
        return SignalsResponse(signals=signals)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
