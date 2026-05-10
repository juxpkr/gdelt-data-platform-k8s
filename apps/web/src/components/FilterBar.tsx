import { useState } from 'react'
import { Search, X } from 'lucide-react'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'

export interface Filters {
  geo?: string
  event_code: string
  date: string
  actor1?: string
}

interface FilterBarProps {
  onSearch: (filters: Filters) => void
  loading?: boolean
}

export function FilterBar({ onSearch, loading }: FilterBarProps) {
  const [geo, setGeo] = useState('')
  const [eventCode, setEventCode] = useState('')
  const [date, setDate] = useState('')

  function handleSearch() { onSearch({ geo, event_code: eventCode, date }) }
  function handleClear() {
    setGeo(''); setEventCode(''); setDate('')
    onSearch({ geo: '', event_code: '', date: '' })
  }

  return (
    <div className="flex flex-wrap gap-3 items-end border border-zinc-800 bg-zinc-900 p-3">
      <div className="flex flex-col gap-1">
        <label className="text-xs font-mono text-zinc-500 uppercase tracking-widest">Location</label>
        <Input placeholder="예: Moscow" value={geo} onChange={(e) => setGeo(e.target.value)}
          className="w-44" onKeyDown={(e) => e.key === 'Enter' && handleSearch()} />
      </div>
      <div className="flex flex-col gap-1">
        <label className="text-xs font-mono text-zinc-500 uppercase tracking-widest">Event Code</label>
        <Input placeholder="예: 010" value={eventCode} onChange={(e) => setEventCode(e.target.value)}
          className="w-24" onKeyDown={(e) => e.key === 'Enter' && handleSearch()} />
      </div>
      <div className="flex flex-col gap-1">
        <label className="text-xs font-mono text-zinc-500 uppercase tracking-widest">Date</label>
        <Input placeholder="YYYYMMDD" value={date} onChange={(e) => setDate(e.target.value)}
          className="w-32" onKeyDown={(e) => e.key === 'Enter' && handleSearch()} />
      </div>
      <Button onClick={handleSearch} disabled={loading} className="gap-1.5">
        <Search className="h-3 w-3" /> SEARCH
      </Button>
      <Button variant="ghost" size="sm" onClick={handleClear} disabled={loading} className="gap-1">
        <X className="h-3 w-3" /> CLEAR
      </Button>
    </div>
  )
}
