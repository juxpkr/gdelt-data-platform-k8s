import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { Layout } from '@/components/Layout'
import { Dashboard } from '@/pages/Dashboard'
import { Signals } from '@/pages/Signals'
import { Hotspots } from '@/pages/Hotspots'
import { EventsExplorer } from '@/pages/EventsExplorer'
import { BatchMonitor } from '@/pages/BatchMonitor'
import { Audit } from '@/pages/Audit'

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<Layout />}>
          <Route index element={<Dashboard />} />
          <Route path="signals" element={<Signals />} />
          <Route path="hotspots" element={<Hotspots />} />
          <Route path="events" element={<EventsExplorer />} />
          <Route path="batches" element={<BatchMonitor />} />
          <Route path="audit" element={<Audit />} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}
