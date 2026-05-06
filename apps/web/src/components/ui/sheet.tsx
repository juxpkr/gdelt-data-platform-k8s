import * as Dialog from '@radix-ui/react-dialog'
import { X } from 'lucide-react'
import { cn } from '@/lib/utils'

export const Sheet = Dialog.Root
export const SheetTrigger = Dialog.Trigger
export const SheetClose = Dialog.Close

export function SheetContent({
  className,
  children,
  ...props
}: Dialog.DialogContentProps) {
  return (
    <Dialog.Portal>
      <Dialog.Overlay className="fixed inset-0 z-50 bg-black/70 data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0" />
      <Dialog.Content
        className={cn(
          'fixed right-0 top-0 z-50 h-full w-full max-w-md bg-zinc-950 border-l border-zinc-800',
          'flex flex-col',
          'data-[state=open]:animate-in data-[state=closed]:animate-out',
          'data-[state=closed]:slide-out-to-right data-[state=open]:slide-in-from-right',
          'duration-200',
          className
        )}
        {...props}
      >
        {children}
        <Dialog.Close className="absolute right-4 top-3 text-zinc-600 hover:text-zinc-300 transition-colors">
          <X className="h-4 w-4" />
        </Dialog.Close>
      </Dialog.Content>
    </Dialog.Portal>
  )
}

export function SheetHeader({ className, ...props }: React.HTMLAttributes<HTMLDivElement>) {
  return <div className={cn('flex flex-col space-y-1 px-5 py-4 border-b border-zinc-800 bg-black', className)} {...props} />
}

export function SheetTitle({ className, ...props }: React.HTMLAttributes<HTMLHeadingElement>) {
  return <h2 className={cn('text-xs font-mono font-bold text-amber-400 tracking-widest', className)} {...props} />
}

export function SheetBody({ className, ...props }: React.HTMLAttributes<HTMLDivElement>) {
  return <div className={cn('flex-1 overflow-y-auto px-5 py-4', className)} {...props} />
}
