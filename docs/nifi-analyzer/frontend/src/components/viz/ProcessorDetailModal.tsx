import React, { useEffect, useCallback, useMemo } from 'react';

interface Processor {
  name: string;
  type: string;
  platform: string;
  properties: Record<string, string>;
  group: string;
  state: string;
  scheduling: Record<string, unknown> | null;
}

interface Connection {
  sourceName: string;
  destinationName: string;
  relationship: string;
}

interface Mapping {
  name: string;
  type: string;
  role: string;
  category: string;
  mapped: boolean;
  confidence: number;
  code: string;
  notes: string;
}

interface SecurityFinding {
  processor?: string;
  type?: string;
  severity?: string;
  finding?: string;
  description?: string;
}

export interface ProcessorDetailModalProps {
  processorName: string;
  processors: Processor[];
  connections: Connection[];
  mappings?: Mapping[];
  securityFindings?: Array<Record<string, unknown>>;
  onClose: () => void;
}

export default function ProcessorDetailModal({
  processorName, processors, connections, mappings, securityFindings, onClose,
}: ProcessorDetailModalProps) {
  const processor = useMemo(() => processors.find((p) => p.name === processorName), [processors, processorName]);
  const mapping = useMemo(() => mappings?.find((m) => m.name === processorName), [mappings, processorName]);

  const connectionsIn = useMemo(() => connections.filter((c) => c.destinationName === processorName), [connections, processorName]);
  const connectionsOut = useMemo(() => connections.filter((c) => c.sourceName === processorName), [connections, processorName]);

  const findings = useMemo(() =>
    (securityFindings ?? []).filter((f) => String(f.processor ?? '') === processorName) as SecurityFinding[],
    [securityFindings, processorName]
  );

  const handleKeyDown = useCallback((e: KeyboardEvent) => {
    if (e.key === 'Escape') onClose();
  }, [onClose]);

  useEffect(() => {
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [handleKeyDown]);

  if (!processor) return null;

  const conf = mapping ? (mapping.confidence <= 1 ? mapping.confidence * 100 : mapping.confidence) : 0;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60" onClick={onClose}>
      <div
        className="bg-gray-900 border border-border rounded-xl shadow-2xl w-full max-w-2xl max-h-[80vh] overflow-y-auto m-4"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-border sticky top-0 bg-gray-900 z-10">
          <div>
            <h2 className="text-lg font-semibold text-gray-100">{processor.name}</h2>
            <p className="text-xs text-gray-500 font-mono">{processor.type}</p>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-200 text-lg transition">X</button>
        </div>

        <div className="px-6 py-4 space-y-5">
          {/* Meta */}
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <MetaItem label="Group" value={processor.group || 'Default'} />
            <MetaItem label="State" value={processor.state} />
            <MetaItem label="Platform" value={processor.platform} />
            {mapping && (
              <>
                <MetaItem label="Role" value={mapping.role} />
                <MetaItem label="Category" value={mapping.category || '-'} />
                <MetaItem label="Mapped" value={mapping.mapped ? 'Yes' : 'No'} />
                <MetaItem label="Confidence" value={`${Math.round(conf)}%`} />
              </>
            )}
          </div>

          {/* Properties */}
          {Object.keys(processor.properties).length > 0 && (
            <Section title="Properties">
              <table className="w-full text-xs">
                <tbody className="divide-y divide-border">
                  {Object.entries(processor.properties).map(([k, v]) => (
                    <tr key={k} className="hover:bg-gray-800/30">
                      <td className="py-1.5 pr-3 text-gray-400 font-mono whitespace-nowrap">{k}</td>
                      <td className="py-1.5 text-gray-300 break-all">{v}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </Section>
          )}

          {/* Generated Code */}
          {mapping?.code && (
            <Section title="Generated Code">
              <pre className="text-[11px] text-gray-300 bg-gray-800 rounded-lg px-4 py-3 overflow-x-auto font-mono leading-relaxed border border-border">
                {mapping.code}
              </pre>
            </Section>
          )}

          {/* Connections */}
          {(connectionsIn.length > 0 || connectionsOut.length > 0) && (
            <Section title="Connections">
              {connectionsIn.length > 0 && (
                <div className="mb-2">
                  <p className="text-[10px] text-gray-500 uppercase mb-1">Incoming ({connectionsIn.length})</p>
                  {connectionsIn.map((c, i) => (
                    <div key={i} className="text-xs text-gray-400 py-0.5">
                      <span className="text-gray-300">{c.sourceName}</span>
                      <span className="text-gray-600 mx-1">[{c.relationship}]</span>
                    </div>
                  ))}
                </div>
              )}
              {connectionsOut.length > 0 && (
                <div>
                  <p className="text-[10px] text-gray-500 uppercase mb-1">Outgoing ({connectionsOut.length})</p>
                  {connectionsOut.map((c, i) => (
                    <div key={i} className="text-xs text-gray-400 py-0.5">
                      <span className="text-gray-600 mx-1">[{c.relationship}]</span>
                      <span className="text-gray-300">{c.destinationName}</span>
                    </div>
                  ))}
                </div>
              )}
            </Section>
          )}

          {/* Security Findings */}
          {findings.length > 0 && (
            <Section title={`Security Findings (${findings.length})`}>
              <div className="space-y-2">
                {findings.map((f, i) => (
                  <div key={i} className="text-xs bg-red-500/5 border border-red-500/20 rounded p-2">
                    <span className="text-red-400 font-medium uppercase text-[10px]">{f.severity ?? 'MEDIUM'}</span>
                    <p className="text-gray-300 mt-0.5">{f.finding ?? f.description ?? ''}</p>
                  </div>
                ))}
              </div>
            </Section>
          )}

          {/* Notes */}
          {mapping?.notes && (
            <Section title="Notes">
              <p className="text-xs text-gray-400">{mapping.notes}</p>
            </Section>
          )}
        </div>
      </div>
    </div>
  );
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div>
      <h3 className="text-xs font-medium text-gray-400 uppercase tracking-wide mb-2">{title}</h3>
      {children}
    </div>
  );
}

function MetaItem({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded bg-gray-800/50 px-3 py-2">
      <p className="text-[10px] text-gray-500">{label}</p>
      <p className="text-xs text-gray-200 font-medium">{value}</p>
    </div>
  );
}
