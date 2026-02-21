import React, { useState, useRef, useEffect } from 'react';
import { useUIStore, type ToastEntry } from '../../store/ui';

const TYPE_ICONS: Record<ToastEntry['type'], string> = {
  success: '\u2713',
  error: '\u2716',
  warning: '\u26A0',
  info: '\u2139',
};

const TYPE_COLORS: Record<ToastEntry['type'], string> = {
  success: 'text-green-400',
  error: 'text-red-400',
  warning: 'text-amber-400',
  info: 'text-blue-400',
};

function relativeTime(ts: number): string {
  const diff = Math.floor((Date.now() - ts) / 1000);
  if (diff < 60) return `${diff}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}

type FilterType = 'all' | ToastEntry['type'];

export default function NotificationCenter() {
  const [open, setOpen] = useState(false);
  const [filter, setFilter] = useState<FilterType>('all');
  const toasts = useUIStore((s) => s.toasts);
  const errors = useUIStore((s) => s.errors);
  const ref = useRef<HTMLDivElement>(null);

  // Build unified history from toasts (they persist in store)
  const history = toasts.slice().reverse();
  const filtered = filter === 'all' ? history : history.filter((t) => t.type === filter);
  const badgeCount = errors.length + toasts.filter((t) => t.type === 'error').length;

  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [open]);

  const clearAll = () => {
    // Clear toasts via individual removal
    for (const t of toasts) {
      useUIStore.getState().removeToast(t.id);
    }
    useUIStore.getState().clearErrors();
  };

  const filters: FilterType[] = ['all', 'success', 'error', 'warning', 'info'];

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen(!open)}
        className="relative p-2 rounded-lg text-gray-400 hover:text-gray-200 hover:bg-gray-800 transition"
      >
        <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V5a2 2 0 10-4 0v.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9" />
        </svg>
        {badgeCount > 0 && (
          <span className="absolute -top-0.5 -right-0.5 w-4 h-4 rounded-full bg-red-500 text-white text-[10px] flex items-center justify-center font-bold">
            {badgeCount > 9 ? '9+' : badgeCount}
          </span>
        )}
      </button>

      {open && (
        <div className="absolute top-full right-0 mt-1 w-80 bg-gray-900 border border-border rounded-lg shadow-xl z-50 overflow-hidden">
          {/* Header */}
          <div className="flex items-center justify-between px-4 py-2 border-b border-border">
            <span className="text-sm font-medium text-gray-200">Notifications</span>
            <button onClick={clearAll} className="text-xs text-gray-500 hover:text-gray-300 transition">
              Clear All
            </button>
          </div>

          {/* Filters */}
          <div className="flex gap-1 px-3 py-2 border-b border-border">
            {filters.map((f) => (
              <button
                key={f}
                onClick={() => setFilter(f)}
                className={`px-2 py-0.5 rounded text-xs font-medium transition
                  ${filter === f ? 'bg-primary/20 text-primary' : 'text-gray-500 hover:text-gray-300'}`}
              >
                {f === 'all' ? 'All' : f.charAt(0).toUpperCase() + f.slice(1)}
              </button>
            ))}
          </div>

          {/* List */}
          <div className="max-h-64 overflow-y-auto">
            {filtered.length === 0 ? (
              <div className="py-6 text-center text-sm text-gray-500">No notifications</div>
            ) : (
              filtered.map((t) => (
                <div key={t.id} className="flex items-start gap-3 px-4 py-2.5 border-b border-border/30 hover:bg-gray-800/30">
                  <span className={`mt-0.5 text-sm ${TYPE_COLORS[t.type]}`}>{TYPE_ICONS[t.type]}</span>
                  <div className="flex-1 min-w-0">
                    <p className="text-xs text-gray-300 truncate">{t.message}</p>
                    <p className="text-[10px] text-gray-600 mt-0.5">{relativeTime(t.timestamp)}</p>
                  </div>
                </div>
              ))
            )}
          </div>
        </div>
      )}
    </div>
  );
}
