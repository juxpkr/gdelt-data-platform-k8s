import re
from fastapi import APIRouter, HTTPException, Query
from db.trino import fetch_all
from models.schemas import HotspotItem, HotspotsResponse

router = APIRouter()

_METRIC_MAP = {
    "events":    ("event_count",    "DESC"),
    "tone":      ("avg_tone",       "ASC"),
    "goldstein": ("avg_goldstein",  "ASC"),
    "mentions":  ("total_mentions", "DESC"),
}


@router.get("/hotspots", response_model=HotspotsResponse)
def get_hotspots(
    metric: str = Query(default="events"),
    limit: int = Query(default=20, ge=1, le=50),
):
    if metric not in _METRIC_MAP:
        metric = "events"

    order_col, order_dir = _METRIC_MAP[metric]

    sql = f"""
        SELECT
            action_geo_fullname AS location,
            COUNT(*) AS event_count,
            ROUND(AVG(avg_tone), 3) AS avg_tone,
            ROUND(AVG(goldstein_scale), 2) AS avg_goldstein,
            CAST(SUM(num_mentions) AS bigint) AS total_mentions
        FROM nessie.gold.gold_llm_context
        WHERE action_geo_fullname IS NOT NULL
          AND action_geo_fullname != ''
        GROUP BY action_geo_fullname
        ORDER BY {order_col} {order_dir}
        LIMIT {limit}
    """
    try:
        rows = fetch_all(sql)
        hotspots = [
            HotspotItem(
                location=r["location"],
                event_count=int(r["event_count"]),
                avg_tone=float(r["avg_tone"]) if r["avg_tone"] is not None else None,
                avg_goldstein=float(r["avg_goldstein"]) if r["avg_goldstein"] is not None else None,
                total_mentions=int(r["total_mentions"]) if r["total_mentions"] is not None else None,
            )
            for r in (rows or [])
        ]
        return HotspotsResponse(metric=metric, hotspots=hotspots)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
