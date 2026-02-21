import { useEffect } from 'react';
import { useUIStore } from '../store/ui';

/**
 * Registers global keyboard shortcuts. Mount once at app root.
 *
 * Shortcuts:
 *   Ctrl/Cmd+K       -> toggle search overlay
 *   Ctrl/Cmd+1..8    -> navigate to steps 1-8 (indices 0-7)
 *   Ctrl/Cmd+9       -> Summary (index 8)
 *   Ctrl/Cmd+0       -> Admin (index 9)
 *   Escape           -> close any open panel/overlay
 */
export function useKeyboardShortcuts() {
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      const mod = e.metaKey || e.ctrlKey;
      if (!mod) {
        // Escape closes all overlays
        if (e.key === 'Escape') {
          const state = useUIStore.getState();
          if (state.searchOpen) {
            useUIStore.getState().setSearchOpen(false);
            e.preventDefault();
          } else if (state.settingsOpen) {
            useUIStore.getState().setSettingsOpen(false);
            e.preventDefault();
          } else if (state.helpOpen) {
            useUIStore.getState().setHelpOpen(false);
            e.preventDefault();
          }
        }
        return;
      }

      // Cmd/Ctrl+K -> toggle search
      if (e.key === 'k' || e.key === 'K') {
        e.preventDefault();
        const current = useUIStore.getState().searchOpen;
        useUIStore.getState().setSearchOpen(!current);
        return;
      }

      // Cmd/Ctrl+1..8 -> steps 0-7
      const num = parseInt(e.key, 10);
      if (num >= 1 && num <= 8) {
        e.preventDefault();
        useUIStore.getState().setActiveStep(num - 1);
        return;
      }

      // Cmd/Ctrl+9 -> Summary (index 8)
      if (e.key === '9') {
        e.preventDefault();
        useUIStore.getState().setActiveStep(8);
        return;
      }

      // Cmd/Ctrl+0 -> Admin (index 9)
      if (e.key === '0') {
        e.preventDefault();
        useUIStore.getState().setActiveStep(9);
        return;
      }
    };

    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, []);
}
