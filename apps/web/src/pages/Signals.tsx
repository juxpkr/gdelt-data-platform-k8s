import { useEffect, useState } from 'react'
import { AlertTriangle, RefreshCw } from 'lucide-react'
import { fetchSignals } from '@/api/client'
import type { SignalItem } from '@/api/types'
import { Button } from '@/components/ui/button'
import { PageHeader } from './Dashboard'
import { EventDetailPanel } from '@/components/EventDetailPanel'

const LIMITS = [10, 30, 50, 100]

function riskClass(score: number | null) {
  if (score == null) return 'text-zinc-600'
  if (score > 5) return 'text-red-400'
  if (score > 2) return 'text-amber-400'
  return 'text-zinc-400'
}

function riskLabel(score: number | null) {
  if (score == null) return { label: '—', cls: 'bg-zinc-800 text-zinc-500' }
  if (score > 5) return { label: 'HIGH', cls: 'bg-red-500/20 text-red-400 border border-red-500/30' }
  if (score > 2) return { label: 'MED', cls: 'bg-amber-500/20 text-amber-400 border border-amber-500/30' }
  return { label: 'LOW', cls: 'bg-zinc-800 text-zinc-400' }
}

export function Signals() {
  const [signals, setSignals] = useState<SignalItem[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [limit, setLimit] = useState(30)
  const [selectedId, setSelectedId] = useState<string | null>(null)

  function load(l = limit) {
    setLoading(true)
    setError(null)
    fetchSignals(l)
      .then((r) => setSignals(r.signals))
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false))
  }

  useEffect(() => { load() }, []) // eslint-disable-line react-hooks/exhaustive-deps

  const highCount = signals.filter((s) => (s.risk_score ?? 0) > 5).length
  const medCount = signals.filter((s) => (s.risk_score ?? 0) > 2 && (s.risk_score ?? 0) <= 5).length

  return (
    <div className="space-y-4">
      <PageHeader
        title="RISK SIGNALS"
        sub="High-risk GDELT event signals sorted by risk score"
      />

      <div className="flex items-center justify-between">
        <div className="flex items-center gap-4">
          <span className="text-xs font-mono text-red-400">
            <AlertTriangle className="inline h-3 w-3 mr-1" />
            {highCount} HIGH
          </span>
          <span className="text-xs font-mono text-amber-400">{medCount} MED</span>
          <span className="text-xs font-mono text-zinc-500">{signals.length - highCount - medCount} LOW</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="flex border border-zinc-800">
            {LIMITS.map((l) => (
              <button
                key={l}
                onClick={() => { setLimit(l); load(l) }}
                className={`px-3 py-1 text-xs font-mono transition-colors ${
                  limit === l
                    ? 'bg-zinc-800 text-zinc-100'
                    : 'text-zinc-500 hover:text-zinc-300'
                }`}
              >
                {l}
              </button>
            ))}
          </div>
          <Button variant="outline" size="sm" onClick={() => load()} disabled={loading}>
            <RefreshCw className={`h-3 w-3 ${loading ? 'animate-spin' : ''}`} />
          </Button>
        </div>
      </div>

      {error && <p className="text-xs font-mono text-red-500 border border-red-900/50 bg-red-950/20 px-3 py-2">ERROR: {error}</p>}

      <div className="border border-zinc-800 overflow-x-auto">
        <table className="w-full text-xs font-mono">
          <thead className="bg-zinc-900 border-b border-zinc-800">
            <tr>
              <th className="px-3 py-2 text-left text-zinc-500 uppercase tracking-widest font-medium w-8">#</th>
              <th className="px-3 py-2 text-left text-zinc-500 uppercase tracking-widest font-medium w-20">RISK</th>
              <th className="px-3 py-2 text-left text-zinc-500 uppercase tracking-widest font-medium">LOCATION</th>
              <th className="px-3 py-2 text-left text-zinc-500 uppercase tracking-widest font-medium">EVENT TYPE</th>
              <th className="px-3 py-2 text-right text-zinc-500 uppercase tracking-widest font-medium w-20">SCORE</th>
              <th className="px-3 py-2 text-right text-zinc-500 uppercase tracking-widest font-medium w-16">TONE</th>
              <th className="px-3 py-2 text-right text-zinc-500 uppercase tracking-widest font-medium w-16">GLDN</th>
              <th className="px-3 py-2 text-right text-zinc-500 uppercase tracking-widest font-medium w-16">MNTNS</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-zinc-800/50">
            {loading && (
              <tr><td colSpan={8} className="px-3 py-8 text-center text-zinc-600">LOADING...</td></tr>
            )}
            {!loading && signals.length === 0 && (
              <tr><td colSpan={8} className="px-3 py-8 text-center text-zinc-600">NO DATA</td></tr>
            )}
            {!loading && signals.map((s, i) => {
              const { label, cls } = riskLabel(s.risk_score)
              return (
                <tr
                  key={s.global_event_id}
                  onClick={() => setSelectedId(s.global_event_id)}
                  className="hover:bg-zinc-800/60 cursor-pointer transition-colors"
                >
                  <td className="px-3 py-2 text-zinc-600">{i + 1}</td>
                  <td className="px-3 py-2">
                    <span className={`inline-flex items-center gap-1 px-1.5 py-0.5 text-xs font-mono font-medium ${cls}`}>
                      <AlertTriangle className="h-2.5 w-2.5" />
                      {label}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-zinc-300 max-w-[200px] truncate">{s.action_geo_fullname ?? '—'}</td>
                  <td className="px-3 py-2 max-w-[200px] truncate">
                    <span className="text-amber-400/80">{s.event_code ?? '—'}</span>
                    {s.event_code_name && <span className="text-zinc-500 ml-1">{s.event_code_name}</span>}
                  </td>
                  <td className={`px-3 py-2 text-right font-bold tabular-nums ${riskClass(s.risk_score)}`}>
                    {s.risk_score?.toFixed(2) ?? '—'}
                  </td>
                  <td className={`px-3 py-2 text-right tabular-nums ${(s.avg_tone ?? 0) >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                    {s.avg_tone?.toFixed(2) ?? '—'}
                  </td>
                  <td className="px-3 py-2 text-right text-zinc-400 tabular-nums">
                    {s.goldstein_scale?.toFixed(1) ?? '—'}
                  </td>
                  <td className="px-3 py-2 text-right text-zinc-400 tabular-nums">
                    {s.num_mentions?.toLocaleString() ?? '—'}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>

      <EventDetailPanel eventId={selectedId} onClose={() => setSelectedId(null)} />
    </div>
  )
}
