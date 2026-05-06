import { cn } from '@/lib/utils'

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: 'default' | 'outline' | 'ghost' | 'active'
  size?: 'sm' | 'default'
}

export function Button({ className, variant = 'default', size = 'default', ...props }: ButtonProps) {
  return (
    <button
      className={cn(
        'inline-flex items-center justify-center rounded font-medium transition-colors disabled:opacity-40 font-mono',
        variant === 'default' && 'bg-amber-500 text-black hover:bg-amber-400',
        variant === 'outline' && 'border border-zinc-700 bg-zinc-900 text-zinc-300 hover:border-zinc-500 hover:text-zinc-100',
        variant === 'ghost' && 'text-zinc-400 hover:text-zinc-100 hover:bg-zinc-800',
        variant === 'active' && 'border border-amber-500 bg-amber-500/10 text-amber-400',
        size === 'default' && 'h-8 px-4 text-sm',
        size === 'sm' && 'h-6 px-2.5 text-xs',
        className
      )}
      {...props}
    />
  )
}
