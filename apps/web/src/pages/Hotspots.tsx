import { useEffect, useState } from 'react'
import { MapPin, RefreshCw, TrendingDown, TrendingUp } from 'lucide-react'
import { fetchHotspots } from '@/api/client'
import type { HotspotItem } from '@/api/types'
import { Button } from '@/components/ui/button'
import { PageHeader } from './Dashboard'

type Metric = 'events' | 'tone' | 'mentions'

const METRICS: { value: Metric; label: string }[] = [
  { value: 'events', label: 'EVENT COUNT' },
  { value: 'tone', label: 'AVG TONE' },
  { value: 'mentions', label: 'MENTIONS' },
]

function barWidth(val: number, max: number): string {
  if (max === 0) return '0%'
  return `${Math.max(2, (val / max) * 100).toFixed(1)}%`
}

export function Hotspots() {
  const [hotspots, setHotspots] = useState<HotspotItem[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [metric, setMetric] = useState<Metric>('events')

  function load(m = metric) {
    setLoading(true)
    setError(null)
    fetchHotspots(m, 20)
      .then((r) => setHotspots(r.hotspots))
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false))
  }

  useEffect(() => { load() }, []) // eslint-disable-line react-hooks/exhaustive-deps

  const maxEvents = Math.max(...hotspots.map((h) => h.event_count), 1)
  const maxMentions = Math.max(...hotspots.map((h) => h.total_mentions ?? 0), 1)

  return (
    <div className="space-y-4">
      <PageHeader
        title="GEO HOTSPOTS"
        sub="Top geographic locations by event activity"
      />

      <div className="flex items-center justify-between">
        <div className="flex border border-zinc-800">
          {METRICS.map((m) => (
            <button
              key={m.value}
              onClick={() => { setMetric(m.value); load(m.value) }}
              className={`px-4 py-1.5 text-xs font-mono tracking-widest transition-colors ${
                metric === m.value
                  ? 'bg-amber-400/10 text-amber-400 border-r border-zinc-800'
                  : 'text-zinc-500 hover:text-zinc-300 border-r border-zinc-800 last:border-r-0'
              }`}
            >
              {m.label}
            </button>
          ))}
        </div>
        <Button variant="outline" size="sm" onClick={() => load()} disabled={loading}>
          <RefreshCw className={`h-3 w-3 ${loading ? 'animate-spin' : ''}`} />
        </Button>
      </div>

      {error && <p className="text-xs font-mono text-red-500 border border-red-900/50 bg-red-950/20 px-3 py-2">ERROR: {error}</p>}

      <div className="border border-zinc-800 bg-zinc-900">
        {loading && (
          <div className="px-4 py-8 text-center text-xs font-mono text-zinc-600">LOADING...</div>
        )}
        {!loading && hotspots.length === 0 && (
          <div className="px-4 py-8 text-center text-xs font-mono text-zinc-600">NO DATA</div>
        )}
        {!loading && hotspots.map((h, i) => (
          <div key={h.location} className="flex items-center gap-3 px-4 py-3 border-b border-zinc-800/50 last:border-b-0 hover:bg-zinc-800/40 transition-colors">
            <span className="text-xs font-mono text-zinc-600 w-6 flex-shrink-0">{i + 1}</span>
            <MapPin className="h-3 w-3 text-zinc-600 flex-shrink-0" />
            <div className="flex-1 min-w-0">
              <p className="text-xs font-mono text-zinc-300 truncate">{h.location}</p>
              <div className="mt-1 h-1 bg-zinc-800 rounded-sm overflow-hidden">
                <div
                  className="h-full bg-amber-400/50 rounded-sm"
                  style={{ width: barWidth(h.event_count, maxEvents) }}
                />
              </div>
            </div>
            <div className="flex items-center gap-6 flex-shrink-0">
              <div className="text-right">
                <p className="text-xs font-mono text-zinc-500 uppercase tracking-widest">EVENTS</p>
                <p className="text-xs font-mono text-zinc-200 tabular-nums">{h.event_count.toLocaleString()}</p>
              </div>
              <div className="text-right">
                <p className="text-xs font-mono text-zinc-500 uppercase tracking-widest">TONE</p>
                <p className={`text-xs font-mono tabular-nums flex items-center gap-0.5 justify-end ${
                  (h.avg_tone ?? 0) >= 0 ? 'text-emerald-400' : 'text-red-400'
                }`}>
                  {(h.avg_tone ?? 0) >= 0
                    ? <TrendingUp className="h-3 w-3" />
                    : <TrendingDown className="h-3 w-3" />
                  }
                  {h.avg_tone?.toFixed(2) ?? '—'}
                </p>
              </div>
              <div className="text-right w-16">
                <p className="text-xs font-mono text-zinc-500 uppercase tracking-widest">MNTNS</p>
                <div className="h-1 bg-zinc-800 mt-1 rounded-sm overflow-hidden">
                  <div
                    className="h-full bg-zinc-500 rounded-sm"
                    style={{ width: barWidth(h.total_mentions ?? 0, maxMentions) }}
                  />
                </div>
                <p className="text-xs font-mono text-zinc-500 tabular-nums mt-0.5">
                  {h.total_mentions?.toLocaleString() ?? '—'}
                </p>
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}
