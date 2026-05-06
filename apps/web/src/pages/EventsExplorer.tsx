import { useEffect, useState } from 'react'
import { fetchEvents } from '@/api/client'
import type { EventItem } from '@/api/types'
import { FilterBar, type Filters } from '@/components/FilterBar'
import { EventsTable } from '@/components/EventsTable'
import { EventDetailPanel } from '@/components/EventDetailPanel'

const LIMIT = 50

export function EventsExplorer() {
  const [events, setEvents] = useState<EventItem[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [filters, setFilters] = useState<Filters>({ actor1: '', event_code: '', date: '' })
  const [offset, setOffset] = useState(0)
  const [selectedId, setSelectedId] = useState<string | null>(null)

  function load(f: Filters, off: number) {
    setLoading(true)
    setError(null)
    fetchEvents({ limit: LIMIT, offset: off, ...f })
      .then((r) => setEvents(r.events))
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false))
  }

  useEffect(() => {
    load(filters, offset)
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  function handleSearch(f: Filters) {
    setFilters(f)
    setOffset(0)
    load(f, 0)
  }

  function handlePageChange(off: number) {
    setOffset(off)
    load(filters, off)
  }

  return (
    <div className="space-y-4">
      <h1 className="text-xl font-bold text-gray-900">Events Explorer</h1>
      <FilterBar onSearch={handleSearch} loading={loading} />

      {error && (
        <div className="rounded-md bg-red-50 px-4 py-3 text-sm text-red-700">
          오류: {error}
        </div>
      )}

      <EventsTable
        events={events}
        onRowClick={(ev) => setSelectedId(ev.global_event_id)}
        offset={offset}
        limit={LIMIT}
        onPageChange={handlePageChange}
        loading={loading}
      />

      <EventDetailPanel eventId={selectedId} onClose={() => setSelectedId(null)} />
    </div>
  )
}
