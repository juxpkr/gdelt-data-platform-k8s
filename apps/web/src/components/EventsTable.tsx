import { ExternalLink } from 'lucide-react'
import type { EventItem } from '@/api/types'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'

interface EventsTableProps {
  events: EventItem[]
  onRowClick: (event: EventItem) => void
  offset: number
  limit: number
  onPageChange: (offset: number) => void
  loading?: boolean
}

function toneBadge(tone: number | null) {
  if (tone == null) return <span className="text-zinc-600 font-mono text-xs">—</span>
  if (tone >= 1) return <Badge variant="success">{tone.toFixed(2)}</Badge>
  if (tone <= -1) return <Badge variant="danger">{tone.toFixed(2)}</Badge>
  return <Badge variant="warning">{tone.toFixed(2)}</Badge>
}

export function EventsTable({ events, onRowClick, offset, limit, onPageChange, loading }: EventsTableProps) {
  const hasPrev = offset > 0
  const hasNext = events.length === limit

  return (
    <div className="space-y-2">
      <div className="overflow-x-auto border border-zinc-800">
        <table className="w-full text-xs font-mono">
          <thead className="bg-zinc-900 border-b border-zinc-800">
            <tr>
              <th className="px-3 py-2 text-left text-zinc-500 uppercase tracking-widest font-medium w-24">DATE</th>
              <th className="px-3 py-2 text-left text-zinc-500 uppercase tracking-widest font-medium">LOCATION</th>
              <th className="px-3 py-2 text-left text-zinc-500 uppercase tracking-widest font-medium">TYPE</th>
              <th className="px-3 py-2 text-right text-zinc-500 uppercase tracking-widest font-medium w-16">TONE</th>
              <th className="px-3 py-2 text-right text-zinc-500 uppercase tracking-widest font-medium w-16">GLDN</th>
              <th className="px-3 py-2 text-right text-zinc-500 uppercase tracking-widest font-medium w-14">MNTNS</th>
              <th className="px-3 py-2 text-center text-zinc-500 uppercase tracking-widest font-medium w-8">URL</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-zinc-800/50">
            {loading && (
              <tr><td colSpan={7} className="px-3 py-6 text-center text-zinc-600">LOADING...</td></tr>
            )}
            {!loading && events.length === 0 && (
              <tr><td colSpan={7} className="px-3 py-6 text-center text-zinc-600">NO DATA</td></tr>
            )}
            {!loading && events.map((ev) => (
              <tr
                key={ev.global_event_id}
                onClick={() => onRowClick(ev)}
                className="hover:bg-zinc-800/60 cursor-pointer transition-colors group"
              >
                <td className="px-3 py-2 text-zinc-500 whitespace-nowrap">{ev.event_date ?? '—'}</td>
                <td className="px-3 py-2 text-zinc-300 max-w-[200px] truncate group-hover:text-zinc-100">
                  {ev.action_geo_fullname ?? '—'}
                </td>
                <td className="px-3 py-2 max-w-[180px] truncate">
                  <span className="text-amber-400/80">{ev.event_code ?? '—'}</span>
                  {ev.event_code_name && ev.event_code_name !== ev.event_code && (
                    <span className="text-zinc-500 ml-1">{ev.event_code_name}</span>
                  )}
                </td>
                <td className="px-3 py-2 text-right">{toneBadge(ev.avg_tone)}</td>
                <td className="px-3 py-2 text-right text-zinc-400">
                  {ev.goldstein_scale != null ? ev.goldstein_scale.toFixed(1) : '—'}
                </td>
                <td className="px-3 py-2 text-right text-zinc-400">
                  {ev.num_mentions?.toLocaleString() ?? '—'}
                </td>
                <td className="px-3 py-2 text-center">
                  {ev.source_url ? (
                    <a href={ev.source_url} target="_blank" rel="noopener noreferrer"
                      onClick={(e) => e.stopPropagation()}
                      className="inline-flex text-zinc-600 hover:text-amber-400 transition-colors">
                      <ExternalLink className="h-3 w-3" />
                    </a>
                  ) : <span className="text-zinc-800">—</span>}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="flex items-center justify-between">
        <span className="text-xs font-mono text-zinc-600">
          {events.length > 0 ? `${offset + 1}–${offset + events.length}` : '—'}
        </span>
        <div className="flex gap-2">
          <Button variant="outline" size="sm" disabled={!hasPrev || loading}
            onClick={() => onPageChange(Math.max(0, offset - limit))}>PREV</Button>
          <Button variant="outline" size="sm" disabled={!hasNext || loading}
            onClick={() => onPageChange(offset + limit)}>NEXT</Button>
        </div>
      </div>
    </div>
  )
}
