import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { AlertTriangle, ArrowRight } from 'lucide-react'
import { fetchStats, fetchSignals, fetchEvents } from '@/api/client'
import type { StatsResponse, SignalItem, EventItem } from '@/api/types'
import { StatCard } from '@/components/StatCard'
import { EventsTable } from '@/components/EventsTable'
import { EventDetailPanel } from '@/components/EventDetailPanel'

function PageHeader({ title, sub }: { title: string; sub?: string }) {
  return (
    <div className="border-b border-zinc-800 pb-3 mb-6">
      <p className="text-xs font-mono text-amber-400/60 uppercase tracking-widest mb-0.5">GDELT CONSOLE</p>
      <h1 className="text-lg font-mono font-bold text-zinc-100 tracking-wide">{title}</h1>
      {sub && <p className="text-xs font-mono text-zinc-600 mt-0.5">{sub}</p>}
    </div>
  )
}

export { PageHeader }

function formatBatchId(id: string): string {
  if (id.length !== 14) return id
  return `${id.slice(0, 4)}-${id.slice(4, 6)}-${id.slice(6, 8)} ${id.slice(8, 10)}:${id.slice(10, 12)}`
}

function riskClass(score: number | null) {
  if (score == null) return 'text-zinc-600'
  if (score > 5) return 'text-red-400'
  if (score > 2) return 'text-amber-400'
  return 'text-zinc-400'
}

export function Dashboard() {
  const [stats, setStats] = useState<StatsResponse | null>(null)
  const [signals, setSignals] = useState<SignalItem[]>([])
  const [events, setEvents] = useState<EventItem[]>([])
  const [eventsLoading, setEventsLoading] = useState(true)
  const [selectedId, setSelectedId] = useState<string | null>(null)

  useEffect(() => {
    fetchStats().then(setStats).catch(() => {})
    fetchSignals(5).then((r) => setSignals(r.signals)).catch(() => {})
    fetchEvents({ limit: 10, offset: 0 })
      .then((r) => setEvents(r.events))
      .finally(() => setEventsLoading(false))
  }, [])

  const tonePositive = stats?.avg_tone != null && stats.avg_tone >= 0

  return (
    <div className="space-y-6">
      <PageHeader
        title="OVERVIEW DASHBOARD"
        sub="Event pipeline status and serving metrics"
      />

      {/* KPI row */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-px bg-zinc-800">
        <StatCard label="TOTAL PROCESSED" value={stats?.total_processed_events?.toLocaleString() ?? null} sub="cumulative" />
        <StatCard
          label="LATEST BATCH"
          value={stats?.latest_batch_id ? formatBatchId(stats.latest_batch_id) : null}
          sub={stats?.latest_batch_event_count != null ? `${stats.latest_batch_event_count.toLocaleString()} events` : undefined}
        />
        <StatCard
          label={`AVG TONE (${stats?.window_days ?? 7}D)`}
          value={stats?.avg_tone != null ? stats.avg_tone.toFixed(3) : null}
          valueClass={tonePositive ? 'text-emerald-400' : 'text-red-400'}
        />
        <StatCard
          label={`HIGH RISK (${stats?.window_days ?? 7}D)`}
          value={stats?.high_risk_count?.toLocaleString() ?? null}
          valueClass="text-red-400"
        />
        <StatCard
          label="BATCH EVENTS"
          value={stats?.latest_batch_event_count?.toLocaleString() ?? null}
        />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Top event codes */}
        <div className="border border-zinc-800 bg-zinc-900">
          <div className="px-4 py-3 border-b border-zinc-800 flex items-center justify-between">
            <span className="text-xs font-mono text-zinc-400 uppercase tracking-widest">Top Event Types</span>
          </div>
          <div className="divide-y divide-zinc-800/50">
            {(stats?.top_event_codes ?? []).map((c, i) => (
              <div key={c.event_code} className="flex items-center gap-3 px-4 py-2">
                <span className="text-xs font-mono text-zinc-600 w-4">{i + 1}</span>
                <span className="text-xs font-mono text-amber-400/80 w-10">{c.event_code}</span>
                <span className="text-xs font-mono text-zinc-400 flex-1 truncate">{c.event_code_name ?? '—'}</span>
                <span className="text-xs font-mono text-zinc-300 tabular-nums">{c.event_count.toLocaleString()}</span>
              </div>
            ))}
            {!stats && (
              <p className="px-4 py-4 text-xs font-mono text-zinc-700">LOADING...</p>
            )}
          </div>
        </div>

        {/* Top signals preview */}
        <div className="border border-zinc-800 bg-zinc-900">
          <div className="px-4 py-3 border-b border-zinc-800 flex items-center justify-between">
            <span className="text-xs font-mono text-zinc-400 uppercase tracking-widest">High Risk Signals</span>
            <Link to="/signals" className="flex items-center gap-1 text-xs font-mono text-amber-400/60 hover:text-amber-400">
              VIEW ALL <ArrowRight className="h-3 w-3" />
            </Link>
          </div>
          <div className="divide-y divide-zinc-800/50">
            {signals.map((s) => (
              <div key={s.global_event_id} className="flex items-center gap-3 px-4 py-2">
                <AlertTriangle className={`h-3 w-3 flex-shrink-0 ${riskClass(s.risk_score)}`} />
                <div className="flex-1 min-w-0">
                  <p className="text-xs font-mono text-zinc-300 truncate">{s.action_geo_fullname ?? '—'}</p>
                  <p className="text-xs font-mono text-zinc-600 truncate">{s.event_code_name ?? s.event_code ?? '—'}</p>
                </div>
                <div className="text-right">
                  <p className={`text-xs font-mono tabular-nums ${riskClass(s.risk_score)}`}>
                    {s.risk_score?.toFixed(2) ?? '—'}
                  </p>
                  <p className="text-xs font-mono text-zinc-600">{s.avg_tone?.toFixed(2) ?? '—'}</p>
                </div>
              </div>
            ))}
            {signals.length === 0 && (
              <p className="px-4 py-4 text-xs font-mono text-zinc-700">LOADING...</p>
            )}
          </div>
        </div>
      </div>

      {/* Recent events */}
      <div>
        <div className="flex items-center justify-between mb-3">
          <span className="text-xs font-mono text-zinc-500 uppercase tracking-widest">Recent Events</span>
          <Link to="/events" className="flex items-center gap-1 text-xs font-mono text-amber-400/60 hover:text-amber-400">
            EXPLORE <ArrowRight className="h-3 w-3" />
          </Link>
        </div>
        <EventsTable
          events={events}
          onRowClick={(ev) => setSelectedId(ev.global_event_id)}
          offset={0} limit={10} onPageChange={() => {}} loading={eventsLoading}
        />
      </div>

      <EventDetailPanel eventId={selectedId} onClose={() => setSelectedId(null)} />
    </div>
  )
}
