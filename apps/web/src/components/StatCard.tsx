interface StatCardProps {
  label: string
  value: string | number | null
  sub?: string
  valueClass?: string
}

export function StatCard({ label, value, sub, valueClass }: StatCardProps) {
  return (
    <div className="border border-zinc-800 bg-zinc-900 p-4">
      <p className="text-xs font-mono text-zinc-500 uppercase tracking-widest mb-2">{label}</p>
      <p className={`text-2xl font-mono font-bold text-zinc-100 tabular-nums ${valueClass ?? ''}`}>
        {value ?? '—'}
      </p>
      {sub && <p className="text-xs font-mono text-zinc-600 mt-1">{sub}</p>}
    </div>
  )
}
