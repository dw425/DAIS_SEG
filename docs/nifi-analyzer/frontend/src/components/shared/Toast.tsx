import React, { useEffect } from 'react';
import { useUIStore, type ToastEntry } from '../../store/ui';

const TYPE_STYLES: Record<ToastEntry['type'], string> = {
  success: 'bg-green-900/80 border-green-500/50 text-green-200',
  error: 'bg-red-900/80 border-red-500/50 text-red-200',
  warning: 'bg-amber-900/80 border-amber-500/50 text-amber-200',
  info: 'bg-blue-900/80 border-blue-500/50 text-blue-200',
};

const TYPE_ICONS: Record<ToastEntry['type'], string> = {
  success: '\u2713',
  error: '\u2716',
  warning: '\u26A0',
  info: '\u2139',
};

function ToastItem({ toast }: { toast: ToastEntry }) {
  const removeToast = useUIStore((s) => s.removeToast);

  useEffect(() => {
    const timer = setTimeout(() => removeToast(toast.id), 4000);
    return () => clearTimeout(timer);
  }, [toast.id, removeToast]);

  return (
    <div
      className={`flex items-center gap-2 px-4 py-3 rounded-lg border shadow-lg backdrop-blur-sm
        text-sm animate-[slideIn_0.3s_ease-out] ${TYPE_STYLES[toast.type]}`}
    >
      <span className="text-base">{TYPE_ICONS[toast.type]}</span>
      <span className="flex-1">{toast.message}</span>
      <button
        onClick={() => removeToast(toast.id)}
        className="opacity-60 hover:opacity-100 text-xs ml-2"
      >
        &#x2715;
      </button>
    </div>
  );
}

export default function ToastContainer() {
  const toasts = useUIStore((s) => s.toasts);

  if (toasts.length === 0) return null;

  return (
    <div className="fixed top-4 right-4 z-[100] flex flex-col gap-2 max-w-sm">
      {toasts.map((t) => (
        <ToastItem key={t.id} toast={t} />
      ))}
    </div>
  );
}
