import React from 'react';
import { useUIStore } from '../../store/ui';
import { useSettings } from '../../hooks/useSettings';

function Toggle({ checked, onChange, label }: { checked: boolean; onChange: (v: boolean) => void; label: string }) {
  return (
    <label className="flex items-center justify-between gap-3 py-2">
      <span className="text-sm text-gray-300">{label}</span>
      <button
        type="button"
        onClick={() => onChange(!checked)}
        className={`relative w-10 h-5 rounded-full transition ${checked ? 'bg-primary' : 'bg-gray-700'}`}
      >
        <span
          className={`absolute top-0.5 left-0.5 w-4 h-4 rounded-full bg-white transition-transform
            ${checked ? 'translate-x-5' : 'translate-x-0'}`}
        />
      </button>
    </label>
  );
}

function TextInput({ label, value, onChange, placeholder }: {
  label: string; value: string; onChange: (v: string) => void; placeholder?: string;
}) {
  return (
    <label className="block py-2">
      <span className="text-sm text-gray-300 block mb-1">{label}</span>
      <input
        type="text"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        className="w-full px-3 py-2 rounded-lg bg-gray-800 border border-border text-sm text-gray-200
          placeholder:text-gray-600 focus:outline-none focus:border-primary transition"
      />
    </label>
  );
}

export default function SettingsPanel() {
  const settingsOpen = useUIStore((s) => s.settingsOpen);
  const setSettingsOpen = useUIStore((s) => s.setSettingsOpen);
  const settings = useSettings();

  if (!settingsOpen) return null;

  return (
    <>
      {/* Backdrop */}
      <div className="fixed inset-0 bg-black/40 z-[150]" onClick={() => setSettingsOpen(false)} />

      {/* Drawer */}
      <div className="fixed top-0 right-0 h-full w-80 bg-gray-900 border-l border-border shadow-2xl z-[151]
        animate-[slideInRight_0.2s_ease-out] overflow-y-auto">
        <div className="p-6 space-y-6">
          {/* Header */}
          <div className="flex items-center justify-between">
            <h2 className="text-lg font-semibold text-gray-100">Settings</h2>
            <button
              onClick={() => setSettingsOpen(false)}
              className="p-1 rounded-lg text-gray-400 hover:text-gray-200 hover:bg-gray-800 transition"
            >
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>

          {/* API section */}
          <div>
            <h3 className="text-xs font-semibold uppercase tracking-wider text-gray-500 mb-2">API Configuration</h3>
            <TextInput
              label="API Base URL"
              value={settings.apiBaseUrl}
              onChange={(v) => settings.set('apiBaseUrl', v)}
              placeholder="/api"
            />
          </div>

          {/* Behavior section */}
          <div>
            <h3 className="text-xs font-semibold uppercase tracking-wider text-gray-500 mb-2">Behavior</h3>
            <Toggle
              label="Auto-advance steps"
              checked={settings.autoAdvance}
              onChange={(v) => settings.set('autoAdvance', v)}
            />
            <Toggle
              label="Sidebar mode"
              checked={settings.sidebarMode}
              onChange={(v) => settings.set('sidebarMode', v)}
            />
          </div>

          {/* Defaults section */}
          <div>
            <h3 className="text-xs font-semibold uppercase tracking-wider text-gray-500 mb-2">Databricks Defaults</h3>
            <TextInput
              label="Default Catalog"
              value={settings.defaultCatalog}
              onChange={(v) => settings.set('defaultCatalog', v)}
              placeholder="main"
            />
            <TextInput
              label="Default Schema"
              value={settings.defaultSchema}
              onChange={(v) => settings.set('defaultSchema', v)}
              placeholder="default"
            />
            <TextInput
              label="Default Scope"
              value={settings.defaultScope}
              onChange={(v) => settings.set('defaultScope', v)}
              placeholder="migration"
            />
          </div>

          {/* Reset */}
          <div className="pt-4 border-t border-border">
            <button
              onClick={settings.resetDefaults}
              className="w-full px-4 py-2 rounded-lg bg-red-500/20 text-red-400 text-sm font-medium
                hover:bg-red-500/30 transition"
            >
              Reset to Defaults
            </button>
          </div>
        </div>
      </div>
    </>
  );
}
