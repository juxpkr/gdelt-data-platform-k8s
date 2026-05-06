import { useEffect, useState } from 'react'
import { RefreshCw, Database, Layers } from 'lucide-react'
import { fetchBatches } from '@/api/client'
import type { BatchItem } from '@/api/types'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/utils'

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
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-sm font-semibold text-gray-700 uppercase tracking-wide">Batch Monitor</h1>
        <div className="flex items-center gap-3">
          <span className="text-xs text-gray-400">
            {countdown}초 후 갱신
          </span>
          <Button variant="outline" size="sm" onClick={load} disabled={loading}>
            <RefreshCw className={cn('h-3 w-3 mr-1.5', loading && 'animate-spin')} />
            새로고침
          </Button>
        </div>
      </div>

      {/* 요약 카드 */}
      <div className="grid grid-cols-2 gap-4">
        <div className="rounded-lg border bg-white p-4 flex items-center gap-3">
          <div className="bg-blue-50 rounded-full p-2">
            <Layers className="h-4 w-4 text-blue-500" />
          </div>
          <div>
            <p className="text-xs text-gray-500">총 배치</p>
            <p className="text-xl font-bold text-gray-900">{loading ? '—' : batches.length}</p>
          </div>
        </div>
        <div className="rounded-lg border bg-white p-4 flex items-center gap-3">
          <div className="bg-emerald-50 rounded-full p-2">
            <Database className="h-4 w-4 text-emerald-500" />
          </div>
          <div>
            <p className="text-xs text-gray-500">총 이벤트</p>
            <p className="text-xl font-bold text-gray-900">{loading ? '—' : totalEvents.toLocaleString()}</p>
          </div>
        </div>
      </div>

      {error && (
        <div className="rounded-md bg-red-50 px-4 py-3 text-sm text-red-700">
          오류: {error}
        </div>
      )}

      <div className="overflow-x-auto rounded-lg border bg-white">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 text-xs text-gray-500 uppercase">
            <tr>
              <th className="px-4 py-3 text-left">Batch ID</th>
              <th className="px-4 py-3 text-right">이벤트 수</th>
              <th className="px-4 py-3 text-left">최초 수집</th>
              <th className="px-4 py-3 text-left">최종 수집</th>
              <th className="px-4 py-3 text-left">상태</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {loading && (
              <tr>
                <td colSpan={5} className="px-4 py-8 text-center text-gray-400">로딩 중...</td>
              </tr>
            )}
            {!loading && batches.length === 0 && (
              <tr>
                <td colSpan={5} className="px-4 py-8 text-center text-gray-400">배치 데이터 없음</td>
              </tr>
            )}
            {!loading && batches.map((b, i) => (
              <tr
                key={b.source_batch_id}
                className={cn('hover:bg-gray-50 transition-colors', i === 0 && 'bg-blue-50 hover:bg-blue-100')}
              >
                <td className="px-4 py-3">
                  <div className="flex items-center gap-2">
                    <span className="font-mono text-xs">{formatBatchId(b.source_batch_id)}</span>
                    {i === 0 && (
                      <span className="inline-flex items-center gap-1 text-xs font-semibold text-emerald-600">
                        <span className="h-1.5 w-1.5 rounded-full bg-emerald-500 animate-pulse" />
                        LIVE
                      </span>
                    )}
                  </div>
                </td>
                <td className={cn('px-4 py-3 text-right font-medium', i === 0 && 'font-semibold text-blue-700')}>
                  {b.event_count.toLocaleString()}
                </td>
                <td className="px-4 py-3 text-xs text-gray-500">{b.first_ingested_at ?? '—'}</td>
                <td className="px-4 py-3 text-xs text-gray-500">{b.last_ingested_at ?? '—'}</td>
                <td className="px-4 py-3">
                  {i === 0 ? (
                    <span className="inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium bg-emerald-500 text-white">latest</span>
                  ) : (
                    <span className="inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium bg-gray-100 text-gray-600">done</span>
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
