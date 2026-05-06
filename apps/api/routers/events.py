import re
from fastapi import APIRouter, HTTPException, Query
from db.trino import fetch_all, fetch_one
from models.schemas import EventsResponse, EventItem, EventDetail

router = APIRouter()

# gold.global_event_id 는 varchar 타입 (DESCRIBE 확인 기준)
_NUMERIC_RE = re.compile(r'^\d+$')
_EVENT_CODE_RE = re.compile(r'^\d{2,4}$')
_DATE_RE = re.compile(r'^\d{8}$')


def _sanitize_text(value: str) -> str:
    """공백 제거, 100자 제한, single quote escape."""
    return value.strip()[:100].replace("'", "''")


def _parse_date(value: str) -> str:
    """YYYYMMDD → YYYY-MM-DD (Trino DATE 리터럴용)."""
    return f"{value[:4]}-{value[4:6]}-{value[6:8]}"


@router.get("/events", response_model=EventsResponse)
def get_events(
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    geo: str = Query(default=""),
    event_code: str = Query(default=""),
    date: str = Query(default=""),
    actor1: str = Query(default=""),
):
    conditions = ["1=1"]

    if geo.strip():
        safe = _sanitize_text(geo)
        conditions.append(f"LOWER(g.action_geo_fullname) LIKE LOWER('%{safe}%')")

    if actor1.strip():
        safe = _sanitize_text(actor1)
        conditions.append(f"LOWER(g.actor1_name) LIKE LOWER('%{safe}%')")

    if event_code.strip():
        if not _EVENT_CODE_RE.match(event_code.strip()):
            raise HTTPException(status_code=400, detail="event_code는 2~4자리 숫자여야 합니다")
        conditions.append(f"g.event_code = '{event_code.strip()}'")

    if date.strip():
        if not _DATE_RE.match(date.strip()):
            raise HTTPException(status_code=400, detail="date는 YYYYMMDD 형식이어야 합니다")
        conditions.append(f"g.event_date = DATE '{_parse_date(date.strip())}'")

    where = " AND ".join(conditions)
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
            g.actor1_name,
            g.actor1_country_code,
            g.actor2_name,
            g.actor2_country_code
        FROM nessie.gold.gold_llm_context g
        LEFT JOIN nessie.seeds.event_detail_codes ec ON g.event_code = ec.code
        WHERE {where}
        ORDER BY g.event_date DESC, g.global_event_id DESC
        OFFSET {offset} ROWS
        LIMIT {limit}
    """
    try:
        rows = fetch_all(sql)
        events = [EventItem(**r) for r in rows]
        return EventsResponse(offset=offset, limit=limit, events=events)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/events/{global_event_id}", response_model=EventDetail)
def get_event(global_event_id: str):
    # varchar 타입이지만 GDELT ID는 항상 숫자 — 숫자 검증으로 SQL injection 방지
    if not _NUMERIC_RE.match(global_event_id):
        raise HTTPException(status_code=400, detail="global_event_id는 숫자여야 합니다")

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
            g.actor1_name,
            g.actor1_country_code,
            g.actor2_name,
            g.actor2_country_code,
            g.mention_source_name,
            g.mention_doc_tone,
            g.v2_persons,
            g.v2_organizations,
            g.v2_enhanced_themes,
            g.llm_content_text
        FROM nessie.gold.gold_llm_context g
        LEFT JOIN nessie.seeds.event_detail_codes ec ON g.event_code = ec.code
        WHERE g.global_event_id = '{global_event_id}'
        LIMIT 1
    """
    try:
        row = fetch_one(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    if row is None:
        raise HTTPException(status_code=404, detail="이벤트를 찾을 수 없습니다")
    return EventDetail(**row)
