export interface HealthResponse {
  status: string
  service: string
}

export interface TopEventCodeItem {
  event_code: string
  event_code_name: string | null
  event_count: number
}

export interface StatsResponse {
  total_events: number
  latest_batch_id: string | null
  latest_batch_event_count: number | null
  avg_tone: number | null
  high_risk_count: number | null
  top_event_codes: TopEventCodeItem[]
}

export interface BatchItem {
  source_batch_id: string
  event_count: number
  first_ingested_at: string | null
  last_ingested_at: string | null
}

export interface BatchesResponse {
  batches: BatchItem[]
}

export interface EventItem {
  global_event_id: string
  event_date: string | null
  action_geo_fullname: string | null
  event_code: string | null
  event_code_name: string | null
  avg_tone: number | null
  goldstein_scale: number | null
  num_mentions: number | null
  num_articles: number | null
  source_url: string | null
  actor1_name: string | null
  actor1_country_code: string | null
  actor2_name: string | null
  actor2_country_code: string | null
}

export interface EventsResponse {
  offset: number
  limit: number
  events: EventItem[]
}

export interface EventDetail {
  global_event_id: string
  event_date: string | null
  action_geo_fullname: string | null
  event_code: string | null
  event_code_name: string | null
  avg_tone: number | null
  goldstein_scale: number | null
  num_mentions: number | null
  num_articles: number | null
  source_url: string | null
  actor1_name: string | null
  actor1_country_code: string | null
  actor2_name: string | null
  actor2_country_code: string | null
  mention_source_name: string | null
  mention_doc_tone: number | null
  v2_persons: string | null
  v2_organizations: string | null
  v2_enhanced_themes: string | null
  llm_content_text: string | null
}

export interface SignalItem {
  global_event_id: string
  event_date: string | null
  action_geo_fullname: string | null
  event_code: string | null
  event_code_name: string | null
  avg_tone: number | null
  goldstein_scale: number | null
  num_mentions: number | null
  num_articles: number | null
  source_url: string | null
  risk_score: number | null
}

export interface SignalsResponse {
  signals: SignalItem[]
}

export interface HotspotItem {
  location: string
  event_count: number
  avg_tone: number | null
  avg_goldstein: number | null
  total_mentions: number | null
}

export interface HotspotsResponse {
  metric: string
  hotspots: HotspotItem[]
}

export interface AuditRunItem {
  batch_id: string
  stage: string
  status: string
  input_rows: number | null
  output_rows: number | null
  started_at: string | null
  finished_at: string | null
  duration_seconds: number | null
  error_message: string | null
}

export interface AuditRunsResponse {
  runs: AuditRunItem[]
}
