import type {
  StatsResponse,
  BatchesResponse,
  EventsResponse,
  EventDetail,
  SignalsResponse,
  HotspotsResponse,
} from './types'

const BASE = '/api'

async function apiFetch<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`)
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText)
    throw new Error(`${res.status}: ${text}`)
  }
  return res.json() as Promise<T>
}

export function fetchStats(): Promise<StatsResponse> {
  return apiFetch<StatsResponse>('/stats')
}

export function fetchBatches(): Promise<BatchesResponse> {
  return apiFetch<BatchesResponse>('/batches')
}

export interface EventsParams {
  limit?: number
  offset?: number
  geo?: string
  event_code?: string
  date?: string
  actor1?: string
}

export function fetchEvents(params: EventsParams = {}): Promise<EventsResponse> {
  const q = new URLSearchParams()
  if (params.limit != null) q.set('limit', String(params.limit))
  if (params.offset != null) q.set('offset', String(params.offset))
  if (params.geo) q.set('geo', params.geo)
  if (params.event_code) q.set('event_code', params.event_code)
  if (params.date) q.set('date', params.date)
  if (params.actor1) q.set('actor1', params.actor1)
  return apiFetch<EventsResponse>(`/events?${q.toString()}`)
}

export function fetchEventDetail(id: string): Promise<EventDetail> {
  return apiFetch<EventDetail>(`/events/${id}`)
}

export function fetchSignals(limit = 30): Promise<SignalsResponse> {
  return apiFetch<SignalsResponse>(`/signals?limit=${limit}`)
}

export function fetchHotspots(metric = 'events', limit = 20): Promise<HotspotsResponse> {
  return apiFetch<HotspotsResponse>(`/hotspots?metric=${metric}&limit=${limit}`)
}
