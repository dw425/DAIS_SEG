import React, { useState } from 'react';
import { useUIStore } from '../../store/ui';

const STEP_GUIDES = [
  { name: '1. Parse', desc: 'Upload an ETL flow file (NiFi XML/JSON, SSIS DTSX, Airflow DAG, etc.) to extract processors, connections, and metadata.' },
  { name: '2. Analyze', desc: 'Examine the dependency graph, detect cycles, identify external systems, and surface security findings.' },
  { name: '3. Assess', desc: 'Map each processor to its Databricks equivalent, with confidence scores and migration notes.' },
  { name: '4. Convert', desc: 'Generate a Databricks notebook with PySpark/SQL cells that implement the mapped logic.' },
  { name: '5. Report', desc: 'View the gap playbook, risk matrix, and estimated migration timeline.' },
  { name: '6. Final Report', desc: 'Executive summary combining all analysis into a comprehensive migration document.' },
  { name: '7. Validate', desc: 'Score the generated notebook across dimensions like completeness, correctness, and security.' },
  { name: '8. Value Analysis', desc: 'Identify droppable processors, complexity breakdown, ROI estimate, and implementation roadmap.' },
  { name: '9. Summary', desc: 'Overview dashboard showing all pipeline results at a glance.' },
  { name: '10. Admin', desc: 'Debug tools: log viewer, state inspector, code validator.' },
];

const SHORTCUTS = [
  { keys: 'Ctrl/Cmd + K', action: 'Open search' },
  { keys: 'Ctrl/Cmd + 1-8', action: 'Navigate to step 1-8' },
  { keys: 'Ctrl/Cmd + 9', action: 'Go to Summary' },
  { keys: 'Ctrl/Cmd + 0', action: 'Go to Admin' },
  { keys: 'Escape', action: 'Close panels/overlays' },
];

const PLATFORMS = [
  'Apache NiFi', 'SQL Server SSIS', 'Informatica', 'Talend', 'Apache Airflow',
  'dbt', 'Azure Data Factory', 'AWS Glue', 'Snowflake', 'Spark', 'Generic SQL',
  'Matillion', 'Fivetran', 'Stitch', 'Pentaho', 'DataStage', 'Ab Initio',
  'StreamSets', 'Apache Beam', 'Apache Flink', 'Apache Kafka', 'Dagster',
];

const FAQS = [
  { q: 'What file formats are supported?', a: 'XML, JSON, GZ, ZIP, DTSX, PY, SQL files from 22+ ETL platforms.' },
  { q: 'Is my data sent to the cloud?', a: 'All processing runs locally via the backend API. No data is sent externally.' },
  { q: 'Can I re-run individual steps?', a: 'Yes, navigate to any step and click the run button to re-execute it.' },
  { q: 'How is confidence calculated?', a: 'Confidence reflects the completeness of the mapping: 1:1 API matches score 90%+, partial matches 50-89%.' },
];

function Accordion({ title, children, defaultOpen }: { title: string; children: React.ReactNode; defaultOpen?: boolean }) {
  const [open, setOpen] = useState(defaultOpen || false);
  return (
    <div className="border border-border rounded-lg overflow-hidden">
      <button
        onClick={() => setOpen(!open)}
        className="w-full flex items-center justify-between px-4 py-3 text-sm font-medium text-gray-200
          hover:bg-gray-800/50 transition"
      >
        {title}
        <svg className={`w-4 h-4 text-gray-500 transition-transform ${open ? 'rotate-180' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </button>
      {open && <div className="px-4 pb-3 text-sm text-gray-400">{children}</div>}
    </div>
  );
}

export default function HelpPanel() {
  const helpOpen = useUIStore((s) => s.helpOpen);
  const setHelpOpen = useUIStore((s) => s.setHelpOpen);
  const activeStep = useUIStore((s) => s.activeStep);

  if (!helpOpen) return null;

  return (
    <>
      <div className="fixed inset-0 bg-black/40 z-[150]" onClick={() => setHelpOpen(false)} />

      <div className="fixed top-0 right-0 h-full w-96 bg-gray-900 border-l border-border shadow-2xl z-[151]
        animate-[slideInRight_0.2s_ease-out] overflow-y-auto">
        <div className="p-6 space-y-6">
          {/* Header */}
          <div className="flex items-center justify-between">
            <h2 className="text-lg font-semibold text-gray-100">Help</h2>
            <button
              onClick={() => setHelpOpen(false)}
              className="p-1 rounded-lg text-gray-400 hover:text-gray-200 hover:bg-gray-800 transition"
            >
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>

          {/* Keyboard Shortcuts */}
          <div>
            <h3 className="text-xs font-semibold uppercase tracking-wider text-gray-500 mb-3">Keyboard Shortcuts</h3>
            <table className="w-full text-sm">
              <tbody>
                {SHORTCUTS.map((s) => (
                  <tr key={s.keys} className="border-b border-border/50">
                    <td className="py-1.5 pr-3">
                      <kbd className="px-1.5 py-0.5 text-xs font-mono bg-gray-800 text-gray-400 rounded border border-gray-700">
                        {s.keys}
                      </kbd>
                    </td>
                    <td className="py-1.5 text-gray-400">{s.action}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* Step Guide */}
          <div>
            <h3 className="text-xs font-semibold uppercase tracking-wider text-gray-500 mb-3">Step Guide</h3>
            <div className="space-y-2">
              {STEP_GUIDES.map((guide, i) => (
                <Accordion key={i} title={guide.name} defaultOpen={i === activeStep}>
                  {guide.desc}
                </Accordion>
              ))}
            </div>
          </div>

          {/* Supported Platforms */}
          <div>
            <h3 className="text-xs font-semibold uppercase tracking-wider text-gray-500 mb-3">Supported Platforms</h3>
            <div className="grid grid-cols-2 gap-2">
              {PLATFORMS.map((p) => (
                <div key={p} className="flex items-center gap-2 px-2 py-1.5 rounded bg-gray-800/50 text-xs text-gray-300">
                  <span className="w-1.5 h-1.5 rounded-full bg-green-400" />
                  {p}
                </div>
              ))}
            </div>
          </div>

          {/* FAQ */}
          <div>
            <h3 className="text-xs font-semibold uppercase tracking-wider text-gray-500 mb-3">FAQ</h3>
            <div className="space-y-2">
              {FAQS.map((faq, i) => (
                <Accordion key={i} title={faq.q}>
                  {faq.a}
                </Accordion>
              ))}
            </div>
          </div>
        </div>
      </div>
    </>
  );
}
