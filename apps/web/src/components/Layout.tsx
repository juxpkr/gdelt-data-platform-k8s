import { NavLink, Outlet } from 'react-router-dom'
import { cn } from '@/lib/utils'

const navItems = [
  { to: '/', label: 'DASHBOARD' },
  { to: '/signals', label: 'SIGNALS' },
  { to: '/hotspots', label: 'HOTSPOTS' },
  { to: '/events', label: 'EVENTS' },
  { to: '/batches', label: 'BATCHES' },
  { to: '/audit', label: 'AUDIT' },
]

export function Layout() {
  return (
    <div className="min-h-screen bg-zinc-950">
      <nav className="bg-black border-b border-zinc-800 px-6 h-10 flex items-center gap-0">
        <div className="flex items-center mr-8 border-r border-zinc-800 pr-6">
          <span className="font-mono text-sm font-bold tracking-widest">
            <span className="text-amber-400">GDELT</span>
            <span className="ml-1.5 text-zinc-300">CONSOLE</span>
          </span>
        </div>
        {navItems.map(({ to, label }) => (
          <NavLink
            key={to}
            to={to}
            end={to === '/'}
            className={({ isActive }) =>
              cn(
                'h-10 px-4 flex items-center text-xs font-mono font-medium tracking-widest transition-colors border-b-2',
                isActive
                  ? 'text-amber-400 border-amber-400'
                  : 'text-zinc-500 border-transparent hover:text-zinc-300'
              )
            }
          >
            {label}
          </NavLink>
        ))}
        <div className="ml-auto flex items-center gap-2">
          <span className="text-xs font-mono text-zinc-600">GDELT 2.0</span>
          <span className="h-1.5 w-1.5 rounded-full bg-emerald-500 animate-pulse" />
          <span className="text-xs font-mono text-emerald-500">LIVE</span>
        </div>
      </nav>
      <main className="p-6 max-w-screen-2xl mx-auto">
        <Outlet />
      </main>
    </div>
  )
}
