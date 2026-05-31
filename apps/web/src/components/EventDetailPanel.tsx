import { useEffect, useState } from 'react'
import { ExternalLink, MapPin } from 'lucide-react'
import { fetchEventDetail } from '@/api/client'
import type { EventDetail } from '@/api/types'
import { Sheet, SheetContent, SheetHeader, SheetTitle, SheetBody } from '@/components/ui/sheet'
import { Badge } from '@/components/ui/badge'

interface EventDetailPanelProps {
  eventId: string | null
  onClose: () => void
}

function splitEntities(value: string | null, max = 8): string[] {
  if (!value) return []
  return value.split(/[;,]/).map(s => s.trim()).filter(Boolean).slice(0, max)
}

export function EventDetailPanel({ eventId, onClose }: EventDetailPanelProps) {
  const [detail, setDetail] = useState<EventDetail | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!eventId) return
    setLoading(true); setError(null); setDetail(null)
    fetchEventDetail(eventId)
      .then(setDetail)
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false))
  }, [eventId])

  const persons = detail ? splitEntities(detail.v2_persons) : []
  const orgs = detail ? splitEntities(detail.v2_organizations) : []
  const themes = detail ? splitEntities(detail.v2_enhanced_themes, 6) : []

  return (
    <Sheet open={!!eventId} onOpenChange={(open) => { if (!open) onClose() }}>
      <SheetContent>
        <SheetHeader>
          <SheetTitle>EVENT DETAIL</SheetTitle>
          <div className="flex items-center gap-3 text-xs font-mono text-zinc-500">
            <span>#{eventId}</span>
            {detail?.event_date && <span className="text-amber-400/60">{detail.event_date}</span>}
          </div>
        </SheetHeader>

        <SheetBody>
          {loading && <p className="text-xs font-mono text-zinc-600">LOADING...</p>}
          {error && <p className="text-xs font-mono text-red-500">ERROR: {error}</p>}

          {detail && (
            <div className="space-y-5">

              {/* 이벤트 제목 + 액터 흐름 + 위치 */}
              <div className="space-y-2">
                <p className="text-base font-semibold text-zinc-100 leading-snug">
                  {detail.event_code_name ?? `EVENT ${detail.event_code}`}
                </p>
                <div className="flex items-center gap-2 flex-wrap text-sm">
                  <span className="text-zinc-200">{detail.actor1_name ?? '미상'}</span>
                  {detail.actor1_country_code && (
                    <span className="text-xs font-mono text-zinc-600">[{detail.actor1_country_code}]</span>
                  )}
                  {detail.actor2_name && (
                    <>
                      <span className="text-zinc-600">→</span>
                      <span className="text-zinc-200">{detail.actor2_name}</span>
                      {detail.actor2_country_code && (
                        <span className="text-xs font-mono text-zinc-600">[{detail.actor2_country_code}]</span>
                      )}
                    </>
                  )}
                </div>
                {detail.action_geo_fullname && (
                  <div className="flex items-center gap-1.5 text-xs text-zinc-500">
                    <MapPin className="h-3 w-3 flex-shrink-0" />
                    <span>{detail.action_geo_fullname}</span>
                  </div>
                )}
              </div>

              {/* 수치 그리드 */}
              <div className="grid grid-cols-2 gap-2">
                {detail.avg_tone != null && (
                  <div className="border border-zinc-800 bg-zinc-900 p-2">
                    <p className="text-xs font-mono text-zinc-600">AVG TONE</p>
                    <p className={`text-lg font-mono font-bold tabular-nums ${detail.avg_tone >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                      {detail.avg_tone.toFixed(2)}
                    </p>
                  </div>
                )}
                {detail.goldstein_scale != null && (
                  <div className="border border-zinc-800 bg-zinc-900 p-2">
                    <p className="text-xs font-mono text-zinc-600">GOLDSTEIN</p>
                    <p className={`text-lg font-mono font-bold tabular-nums ${detail.goldstein_scale >= 0 ? 'text-zinc-300' : 'text-amber-400'}`}>
                      {detail.goldstein_scale.toFixed(1)}
                    </p>
                  </div>
                )}
                {detail.num_mentions != null && (
                  <div className="border border-zinc-800 bg-zinc-900 p-2">
                    <p className="text-xs font-mono text-zinc-600">MENTIONS</p>
                    <p className="text-lg font-mono font-bold text-zinc-200 tabular-nums">{detail.num_mentions.toLocaleString()}</p>
                  </div>
                )}
                {detail.num_articles != null && (
                  <div className="border border-zinc-800 bg-zinc-900 p-2">
                    <p className="text-xs font-mono text-zinc-600">ARTICLES</p>
                    <p className="text-lg font-mono font-bold text-zinc-200 tabular-nums">{detail.num_articles.toLocaleString()}</p>
                  </div>
                )}
              </div>

              {/* 소스 */}
              {(detail.mention_source_name || detail.source_url) && (
                <div className="space-y-1.5 pt-3 border-t border-zinc-800">
                  <p className="text-xs font-mono text-amber-400/70 uppercase tracking-widest">Source</p>
                  {detail.mention_source_name && (
                    <div className="flex items-center justify-between">
                      <span className="text-xs text-zinc-400">{detail.mention_source_name}</span>
                      {detail.mention_doc_tone != null && (
                        <span className={`text-xs font-mono ${detail.mention_doc_tone >= 0 ? 'text-emerald-400/70' : 'text-red-400/70'}`}>
                          tone {detail.mention_doc_tone.toFixed(2)}
                        </span>
                      )}
                    </div>
                  )}
                  {detail.source_url && (
                    <a href={detail.source_url} target="_blank" rel="noopener noreferrer"
                      className="flex items-center gap-1.5 text-xs text-amber-400/60 hover:text-amber-400 break-all">
                      <ExternalLink className="h-3 w-3 flex-shrink-0" />
                      {detail.source_url.slice(0, 60)}{detail.source_url.length > 60 ? '…' : ''}
                    </a>
                  )}
                </div>
              )}

              {/* 엔티티 */}
              {(persons.length > 0 || orgs.length > 0 || themes.length > 0) && (
                <div className="space-y-3 pt-3 border-t border-zinc-800">
                  <p className="text-xs font-mono text-amber-400/70 uppercase tracking-widest">Entities</p>
                  {persons.length > 0 && (
                    <div className="space-y-1">
                      <p className="text-xs font-mono text-zinc-600">인물</p>
                      <div className="flex flex-wrap gap-1">
                        {persons.map(p => <Badge key={p}>{p}</Badge>)}
                      </div>
                    </div>
                  )}
                  {orgs.length > 0 && (
                    <div className="space-y-1">
                      <p className="text-xs font-mono text-zinc-600">기관</p>
                      <div className="flex flex-wrap gap-1">
                        {orgs.map(o => (
                          <Badge key={o} className="bg-blue-500/10 text-blue-400 border border-blue-500/25">{o}</Badge>
                        ))}
                      </div>
                    </div>
                  )}
                  {themes.length > 0 && (
                    <div className="space-y-1">
                      <p className="text-xs font-mono text-zinc-600">테마</p>
                      <div className="flex flex-wrap gap-1">
                        {themes.map(t => (
                          <Badge key={t} className="bg-zinc-800/60 text-zinc-500 text-xs">{t}</Badge>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              )}

            </div>
          )}
        </SheetBody>
      </SheetContent>
    </Sheet>
  )
}
