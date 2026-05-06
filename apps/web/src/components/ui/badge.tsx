import { cn } from '@/lib/utils'

interface BadgeProps extends React.HTMLAttributes<HTMLSpanElement> {
  variant?: 'default' | 'success' | 'warning' | 'danger'
}

export function Badge({ className, variant = 'default', ...props }: BadgeProps) {
  return (
    <span
      className={cn(
        'inline-flex items-center rounded px-1.5 py-0.5 text-xs font-mono font-medium',
        variant === 'default' && 'bg-zinc-800 text-zinc-400',
        variant === 'success' && 'bg-emerald-500/15 text-emerald-400 border border-emerald-500/30',
        variant === 'warning' && 'bg-amber-500/15 text-amber-400 border border-amber-500/30',
        variant === 'danger' && 'bg-red-500/15 text-red-400 border border-red-500/30',
        className
      )}
      {...props}
    />
  )
}
