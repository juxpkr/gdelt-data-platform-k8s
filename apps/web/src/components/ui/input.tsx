import { cn } from '@/lib/utils'

export function Input({ className, ...props }: React.InputHTMLAttributes<HTMLInputElement>) {
  return (
    <input
      className={cn(
        'flex h-7 w-full rounded border border-zinc-700 bg-zinc-800 px-3 py-1 text-xs font-mono text-zinc-100',
        'placeholder:text-zinc-500 focus:outline-none focus:border-amber-500/60 focus:ring-0',
        'disabled:opacity-40',
        className
      )}
      {...props}
    />
  )
}
