import { useEffect, useState } from 'react'
import { RefreshCw, Database, Layers } from 'lucide-react'
import { fetchBatches } from '@/api/client'
import type { BatchItem } from '@/api/types'
import { cn } from '@/lib/utils'
import { PageHeader } from './Dashboard'

const REFRESH_INTERVAL = 30

function formatBatchId(id: string): string {
  if (id.length !== 14) return id
  return `${id.slice(0, 4)}-${id.slice(4, 6)}-${id.slice(6, 8)} ${id.slice(8, 10)}:${id.slice(10, 12)}:${id.slice(12, 14)}`
}

export function BatchMonitor() {
  const [batches, setBatches] = useState<BatchItem[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [countdown, setCountdown] = useState(REFRESH_INTERVAL)

  function load() {
    setLoading(true)
    setError(null)
    setCountdown(REFRESH_INTERVAL)
    fetchBatches()
      .then((r) => setBatches(r.batches))
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false))
  }

  useEffect(() => {
    load()
    const timer = setInterval(() => {
      setCountdown((prev) => {
        if (prev <= 1) { load(); return REFRESH_INTERVAL }
        return prev - 1
      })
    }, 1000)
    return () => clearInterval(timer)
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  const totalEvents = batches.reduce((s, b) => s + b.event_count, 0)

  return (
    <div className="space-y-6">
      <PageHeader title="BATCH MONITOR" sub="Bronze ingestion batch history" />

      {/* 요약 카드 */}
      <div className="grid grid-cols-2 gap-px bg-zinc-800">
        <div className="bg-zinc-900 p-4 flex items-center gap-3">
          <div className="bg-zinc-800 rounded-full p-2">
            <Layers className="h-4 w-4 text-amber-400" />
          </div>
          <div>
            <p className="text-xs font-mono text-zinc-500 uppercase tracking-widest">총 배치</p>
            <p className="text-xl font-mono font-bold text-zinc-100">{loading ? '—' : batches.length}</p>
          </div>
        </div>
        <div className="bg-zinc-900 p-4 flex items-center gap-3">
          <div className="bg-zinc-800 rounded-full p-2">
            <Database className="h-4 w-4 text-emerald-400" />
          </div>
          <div>
            <p className="text-xs font-mono text-zinc-500 uppercase tracking-widest">총 이벤트</p>
            <p className="text-xl font-mono font-bold text-zinc-100">{loading ? '—' : totalEvents.toLocaleString()}</p>
          </div>
        </div>
      </div>

      {/* 새로고침 */}
      <div className="flex items-center justify-between">
        <span className="text-xs font-mono text-zinc-600">{countdown}s 후 갱신</span>
        <button
          onClick={load}
          disabled={loading}
          className="flex items-center gap-1.5 text-xs font-mono text-zinc-500 hover:text-amber-400 disabled:opacity-40 transition-colors"
        >
          <RefreshCw className={cn('h-3 w-3', loading && 'animate-spin')} />
          REFRESH
        </button>
      </div>

      {error && (
        <div className="border border-red-800/50 bg-red-900/20 px-4 py-3 text-xs font-mono text-red-400">
          ERROR: {error}
        </div>
      )}

      <div className="border border-zinc-800">
        <table className="w-full text-xs font-mono">
          <thead className="bg-zinc-950 text-zinc-500 uppercase tracking-widest">
            <tr>
              <th className="px-4 py-3 text-left">Batch ID</th>
              <th className="px-4 py-3 text-right">이벤트 수</th>
              <th className="px-4 py-3 text-left">최초 수집</th>
              <th className="px-4 py-3 text-left">최종 수집</th>
              <th className="px-4 py-3 text-left">상태</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-zinc-800/50">
            {loading && (
              <tr>
                <td colSpan={5} className="px-4 py-8 text-center text-zinc-700">LOADING...</td>
              </tr>
            )}
            {!loading && batches.length === 0 && (
              <tr>
                <td colSpan={5} className="px-4 py-8 text-center text-zinc-700">배치 데이터 없음</td>
              </tr>
            )}
            {!loading && batches.map((b, i) => (
              <tr
                key={b.source_batch_id}
                className={cn(
                  'transition-colors',
                  i === 0 ? 'bg-amber-400/5 hover:bg-amber-400/10' : 'hover:bg-zinc-800/40'
                )}
              >
                <td className="px-4 py-3">
                  <div className="flex items-center gap-2">
                    <span className="text-zinc-300">{formatBatchId(b.source_batch_id)}</span>
                    {i === 0 && (
                      <span className="flex items-center gap-1 text-emerald-400">
                        <span className="h-1.5 w-1.5 rounded-full bg-emerald-500 animate-pulse" />
                        LIVE
                      </span>
                    )}
                  </div>
                </td>
                <td className={cn('px-4 py-3 text-right tabular-nums', i === 0 ? 'text-amber-400 font-semibold' : 'text-zinc-400')}>
                  {b.event_count.toLocaleString()}
                </td>
                <td className="px-4 py-3 text-zinc-600">{b.first_ingested_at ?? '—'}</td>
                <td className="px-4 py-3 text-zinc-600">{b.last_ingested_at ?? '—'}</td>
                <td className="px-4 py-3">
                  {i === 0 ? (
                    <span className="inline-flex items-center px-2 py-0.5 text-xs font-medium bg-emerald-500/20 text-emerald-400 border border-emerald-500/30">latest</span>
                  ) : (
                    <span className="inline-flex items-center px-2 py-0.5 text-xs font-medium bg-zinc-800 text-zinc-500">done</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
