from typing import Optional
from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    service: str


class TopEventCodeItem(BaseModel):
    event_code: str
    event_code_name: Optional[str]
    event_count: int


class StatsResponse(BaseModel):
    total_events: int
    latest_batch_id: Optional[str]
    latest_batch_event_count: Optional[int]
    avg_tone: Optional[float]
    high_risk_count: Optional[int]
    top_event_codes: list[TopEventCodeItem] = []


class BatchItem(BaseModel):
    source_batch_id: str
    event_count: int
    first_ingested_at: Optional[str]
    last_ingested_at: Optional[str]


class BatchesResponse(BaseModel):
    batches: list[BatchItem]


class EventItem(BaseModel):
    global_event_id: str
    event_date: Optional[str]
    action_geo_fullname: Optional[str]
    event_code: Optional[str]
    event_code_name: Optional[str]
    avg_tone: Optional[float]
    goldstein_scale: Optional[float]
    num_mentions: Optional[int]
    num_articles: Optional[int]
    source_url: Optional[str]
    actor1_name: Optional[str]
    actor1_country_code: Optional[str]
    actor2_name: Optional[str]
    actor2_country_code: Optional[str]


class EventsResponse(BaseModel):
    offset: int
    limit: int
    events: list[EventItem]


class EventDetail(BaseModel):
    global_event_id: str
    event_date: Optional[str]
    action_geo_fullname: Optional[str]
    event_code: Optional[str]
    event_code_name: Optional[str]
    avg_tone: Optional[float]
    goldstein_scale: Optional[float]
    num_mentions: Optional[int]
    num_articles: Optional[int]
    source_url: Optional[str]
    actor1_name: Optional[str]
    actor1_country_code: Optional[str]
    actor2_name: Optional[str]
    actor2_country_code: Optional[str]
    mention_source_name: Optional[str]
    mention_doc_tone: Optional[float]
    v2_persons: Optional[str]
    v2_organizations: Optional[str]
    v2_enhanced_themes: Optional[str]
    llm_content_text: Optional[str]


class SignalItem(BaseModel):
    global_event_id: str
    event_date: Optional[str]
    action_geo_fullname: Optional[str]
    event_code: Optional[str]
    event_code_name: Optional[str]
    avg_tone: Optional[float]
    goldstein_scale: Optional[float]
    num_mentions: Optional[int]
    num_articles: Optional[int]
    source_url: Optional[str]
    risk_score: Optional[float]


class SignalsResponse(BaseModel):
    signals: list[SignalItem]


class HotspotItem(BaseModel):
    location: str
    event_count: int
    avg_tone: Optional[float]
    avg_goldstein: Optional[float]
    total_mentions: Optional[int]


class HotspotsResponse(BaseModel):
    metric: str
    hotspots: list[HotspotItem]


class AuditRunItem(BaseModel):
    batch_id: str
    stage: str
    status: str
    input_rows: Optional[int] = None
    output_rows: Optional[int] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    duration_seconds: Optional[float] = None
    error_message: Optional[str] = None


class AuditRunsResponse(BaseModel):
    runs: list[AuditRunItem]


class AuditBatchDetail(BaseModel):
    batch_id: str
    runs: list[AuditRunItem]
