import { useEffect, useState } from 'react'
import { ExternalLink } from 'lucide-react'
import { fetchEventDetail } from '@/api/client'
import type { EventDetail } from '@/api/types'
import { Sheet, SheetContent, SheetHeader, SheetTitle, SheetBody } from '@/components/ui/sheet'

interface EventDetailPanelProps {
  eventId: string | null
  onClose: () => void
}

function Row({ label, value }: { label: string; value: string | number | null | undefined }) {
  if (value == null || value === '') return null
  return (
    <div className="flex gap-3">
      <dt className="text-xs font-mono text-zinc-600 w-24 flex-shrink-0 pt-0.5">{label}</dt>
      <dd className="text-xs font-mono text-zinc-300 break-words flex-1">{String(value)}</dd>
    </div>
  )
}

function Section({ title }: { title: string }) {
  return (
    <p className="text-xs font-mono text-amber-400/70 uppercase tracking-widest pt-3 border-t border-zinc-800 mt-3">
      {title}
    </p>
  )
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
            <dl className="space-y-1.5">
              <Section title="Location & Event" />
              <Row label="LOCATION" value={detail.action_geo_fullname} />
              <Row label="EVENT CODE" value={detail.event_code} />
              <Row label="EVENT TYPE" value={detail.event_code_name} />

              <Section title="Metrics" />
              <div className="grid grid-cols-2 gap-2 mt-2">
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

              <Section title="Actor" />
              <Row label="ACTOR 1" value={detail.actor1_name} />
              <Row label="COUNTRY" value={detail.actor1_country_code} />
              <Row label="ACTOR 2" value={detail.actor2_name} />
              <Row label="COUNTRY" value={detail.actor2_country_code} />

              <Section title="Source" />
              <Row label="SOURCE" value={detail.mention_source_name} />
              <Row label="SRC TONE" value={detail.mention_doc_tone} />
              {detail.source_url && (
                <a href={detail.source_url} target="_blank" rel="noopener noreferrer"
                  className="flex items-center gap-1.5 text-xs font-mono text-amber-400/60 hover:text-amber-400 break-all mt-1">
                  <ExternalLink className="h-3 w-3 flex-shrink-0" />
                  {detail.source_url.slice(0, 60)}{detail.source_url.length > 60 ? '…' : ''}
                </a>
              )}

              {(detail.v2_persons || detail.v2_organizations || detail.v2_enhanced_themes) && (
                <>
                  <Section title="Entities" />
                  <Row label="PERSONS" value={detail.v2_persons} />
                  <Row label="ORGS" value={detail.v2_organizations} />
                  <Row label="THEMES" value={detail.v2_enhanced_themes} />
                </>
              )}

              <Section title="LLM Context" />
              {detail.llm_content_text ? (
                <div className="border border-zinc-800 bg-black p-3 text-xs font-mono leading-relaxed whitespace-pre-wrap break-words max-h-56 overflow-y-auto text-zinc-400 mt-1">
                  {detail.llm_content_text}
                </div>
              ) : (
                <p className="text-xs font-mono text-zinc-700 italic">NO LLM CONTEXT</p>
              )}
            </dl>
          )}
        </SheetBody>
      </SheetContent>
    </Sheet>
  )
}
