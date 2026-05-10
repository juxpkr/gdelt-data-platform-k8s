import { useEffect, useState } from 'react'
import { RefreshCw, AlertCircle } from 'lucide-react'
import { fetchAuditRuns } from '@/api/client'
import type { AuditRunItem } from '@/api/types'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/utils'
import { PageHeader } from './Dashboard'

const REFRESH_INTERVAL = 30

const STAGES = ['bronze', 'silver', 'gold'] as const
const STATUSES = ['success', 'failed'] as const

type StageFilter = (typeof STAGES)[number] | 'all'
type StatusFilter = (typeof STATUSES)[number] | 'all'

function StageBadge({ stage }: { stage: string }) {
  const styles: Record<string, string> = {
    bronze: 'bg-amber-500/15 text-amber-400 border-amber-500/30',
    silver: 'bg-zinc-500/15 text-zinc-300 border-zinc-500/30',
    gold:   'bg-yellow-500/15 text-yellow-400 border-yellow-500/30',
  }
  return (
    <span className={cn(
      'inline-flex items-center px-2 py-0.5 text-xs font-mono font-semibold border uppercase tracking-widest',
      styles[stage] ?? 'bg-zinc-800 text-zinc-400 border-zinc-700'
    )}>
      {stage}
    </span>
  )
}

function StatusBadge({ status }: { status: string }) {
  const ok = status === 'success'
  return (
    <span className={cn(
      'inline-flex items-center gap-1 px-2 py-0.5 text-xs font-mono font-semibold border uppercase tracking-widest',
      ok
        ? 'bg-emerald-500/15 text-emerald-400 border-emerald-500/30'
        : 'bg-red-500/15 text-red-400 border-red-500/30'
    )}>
      {!ok && <AlertCircle className="h-3 w-3" />}
      {status}
    </span>
  )
}

function formatDuration(sec: number | null): string {
  if (sec === null) return '—'
  if (sec < 60) return `${sec.toFixed(1)}s`
  return `${Math.floor(sec / 60)}m ${Math.round(sec % 60)}s`
}

export function Audit() {
  const [runs, setRuns] = useState<AuditRunItem[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [countdown, setCountdown] = useState(REFRESH_INTERVAL)
  const [stageFilter, setStageFilter] = useState<StageFilter>('all')
  const [statusFilter, setStatusFilter] = useState<StatusFilter>('all')

  function load(stage: StageFilter = stageFilter, status: StatusFilter = statusFilter) {
    setLoading(true)
    setError(null)
    setCountdown(REFRESH_INTERVAL)
    fetchAuditRuns({
      limit: 100,
      stage: stage !== 'all' ? stage : undefined,
      status: status !== 'all' ? status : undefined,
    })
      .then((r) => setRuns(r.runs))
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
  }, [stageFilter, statusFilter]) // eslint-disable-line react-hooks/exhaustive-deps

  function handleStageChange(s: StageFilter) {
    setStageFilter(s)
    load(s, statusFilter)
  }

  function handleStatusChange(s: StatusFilter) {
    setStatusFilter(s)
    load(stageFilter, s)
  }

  const failedCount = runs.filter((r) => r.status === 'failed').length

  return (
    <div className="space-y-4">
      <div className="flex items-start justify-between">
        <PageHeader title="AUDIT RUNS" sub="Pipeline batch execution history" />
        <div className="flex items-center gap-3 mt-1">
          <span className="text-xs font-mono text-zinc-600">{countdown}s 후 갱신</span>
          <Button variant="outline" size="sm" onClick={() => load()} disabled={loading} aria-label="refresh">
            <RefreshCw className={cn('h-3 w-3', loading && 'animate-spin')} />
          </Button>
        </div>
      </div>

      {/* KPI row */}
      <div className="grid grid-cols-3 gap-px bg-zinc-800">
        <div className="bg-zinc-900 p-4">
          <p className="text-xs font-mono text-zinc-500 uppercase tracking-widest">Total Runs</p>
          <p className="text-2xl font-mono font-bold text-zinc-100 mt-1">
            {loading ? '—' : runs.length}
          </p>
        </div>
        <div className="bg-zinc-900 p-4">
          <p className="text-xs font-mono text-zinc-500 uppercase tracking-widest">Failed</p>
          <p className={cn(
            'text-2xl font-mono font-bold mt-1',
            failedCount > 0 ? 'text-red-400' : 'text-zinc-100'
          )}>
            {loading ? '—' : failedCount}
          </p>
        </div>
        <div className="bg-zinc-900 p-4">
          <p className="text-xs font-mono text-zinc-500 uppercase tracking-widest">Success Rate</p>
          <p className="text-2xl font-mono font-bold text-zinc-100 mt-1">
            {loading || runs.length === 0
              ? '—'
              : `${((1 - failedCount / runs.length) * 100).toFixed(1)}%`}
          </p>
        </div>
      </div>

      {/* Filters */}
      <div className="flex items-center gap-4">
        <div className="flex items-center gap-1">
          <span className="text-xs font-mono text-zinc-600 mr-2">STAGE</span>
          {(['all', ...STAGES] as StageFilter[]).map((s) => (
            <button
              key={s}
              onClick={() => handleStageChange(s)}
              className={cn(
                'px-3 py-1 text-xs font-mono uppercase tracking-widest border transition-colors',
                stageFilter === s
                  ? 'bg-amber-400/10 text-amber-400 border-amber-400/40'
                  : 'bg-transparent text-zinc-500 border-zinc-800 hover:text-zinc-300 hover:border-zinc-600'
              )}
            >
              {s}
            </button>
          ))}
        </div>
        <div className="flex items-center gap-1">
          <span className="text-xs font-mono text-zinc-600 mr-2">STATUS</span>
          {(['all', ...STATUSES] as StatusFilter[]).map((s) => (
            <button
              key={s}
              onClick={() => handleStatusChange(s)}
              className={cn(
                'px-3 py-1 text-xs font-mono uppercase tracking-widest border transition-colors',
                statusFilter === s
                  ? 'bg-amber-400/10 text-amber-400 border-amber-400/40'
                  : 'bg-transparent text-zinc-500 border-zinc-800 hover:text-zinc-300 hover:border-zinc-600'
              )}
            >
              {s}
            </button>
          ))}
        </div>
      </div>

      {error && (
        <p className="text-xs font-mono text-red-500 border border-red-900/50 bg-red-950/20 px-3 py-2">
          ERROR: {error}
        </p>
      )}

      {/* Table */}
      <div className="border border-zinc-800 overflow-x-auto">
        <table className="w-full text-xs font-mono">
          <thead className="bg-zinc-900 border-b border-zinc-800">
            <tr>
              <th className="px-4 py-2 text-left text-zinc-500 uppercase tracking-widest font-medium whitespace-nowrap">Batch ID</th>
              <th className="px-4 py-2 text-left text-zinc-500 uppercase tracking-widest font-medium">Stage</th>
              <th className="px-4 py-2 text-left text-zinc-500 uppercase tracking-widest font-medium">Status</th>
              <th className="px-4 py-2 text-right text-zinc-500 uppercase tracking-widest font-medium whitespace-nowrap">Input</th>
              <th className="px-4 py-2 text-right text-zinc-500 uppercase tracking-widest font-medium whitespace-nowrap">Output</th>
              <th className="px-4 py-2 text-right text-zinc-500 uppercase tracking-widest font-medium whitespace-nowrap">Duration</th>
              <th className="px-4 py-2 text-left text-zinc-500 uppercase tracking-widest font-medium">Error</th>
              <th className="px-4 py-2 text-left text-zinc-500 uppercase tracking-widest font-medium whitespace-nowrap">Created At</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-zinc-800/50">
            {loading && (
              <tr>
                <td colSpan={8} className="px-4 py-8 text-center text-zinc-600">LOADING...</td>
              </tr>
            )}
            {!loading && runs.length === 0 && (
              <tr>
                <td colSpan={8} className="px-4 py-8 text-center text-zinc-600">NO DATA</td>
              </tr>
            )}
            {!loading && runs.map((r, i) => {
              const failed = r.status === 'failed'
              return (
                <tr
                  key={`${r.batch_id}-${r.stage}-${i}`}
                  className={cn(
                    'transition-colors',
                    failed
                      ? 'bg-red-950/20 hover:bg-red-950/30 border-l-2 border-l-red-500/50'
                      : 'hover:bg-zinc-800/30'
                  )}
                >
                  <td className="px-4 py-2 text-zinc-300 tabular-nums whitespace-nowrap">
                    {r.batch_id}
                  </td>
                  <td className="px-4 py-2">
                    <StageBadge stage={r.stage} />
                  </td>
                  <td className="px-4 py-2">
                    <StatusBadge status={r.status} />
                  </td>
                  <td className="px-4 py-2 text-right tabular-nums text-zinc-400">
                    {(r.input_rows ?? 0) > 0 ? r.input_rows!.toLocaleString() : '—'}
                  </td>
                  <td className="px-4 py-2 text-right tabular-nums text-zinc-400">
                    {(r.output_rows ?? 0) > 0 ? r.output_rows!.toLocaleString() : '—'}
                  </td>
                  <td className="px-4 py-2 text-right tabular-nums text-zinc-400">
                    {formatDuration(r.duration_seconds)}
                  </td>
                  <td className="px-4 py-2 text-red-400 max-w-xs truncate" title={r.error_message ?? undefined}>
                    {r.error_message ?? <span className="text-zinc-700">—</span>}
                  </td>
                  <td className="px-4 py-2 text-zinc-500 whitespace-nowrap">
                    {r.created_at ?? '—'}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
