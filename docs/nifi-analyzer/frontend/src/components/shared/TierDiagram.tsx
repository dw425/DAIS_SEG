import React, { useState, useMemo, useRef, useEffect, useCallback } from 'react';

export interface TierDiagramProps {
  processors: Array<{ name: string; type: string; group: string }>;
  connections: Array<{ sourceName: string; destinationName: string; relationship: string }>;
  mappings?: Array<{ name: string; type: string; role: string; category: string; mapped: boolean; confidence: number }>;
}

// ── Role classification ──
const ROLE_PATTERNS: [RegExp, string][] = [
  [/^(Get|Consume|Listen|Fetch|Tail|List|Query|Scan|Select|Generate)/i, 'source'],
  [/^(Put|Publish|Send|Post|Insert|Write|Delete|Store)/i, 'sink'],
  [/^(Route|Distribute|Detect|Funnel)/i, 'route'],
  [/^(Execute|Invoke|Handle)/i, 'process'],
  [/^(Log|Debug|Count|Wait|Notify|Control|Monitor)/i, 'utility'],
  [/Convert|Replace|Update|Jolt|Extract|Evaluate|Transform|Compress|Encrypt|Lookup|Validate|Split|Merge/i, 'transform'],
];

function classifyRole(type: string): string {
  for (const [re, role] of ROLE_PATTERNS) {
    if (re.test(type)) return role;
  }
  return 'transform';
}

// ── Tier & connection config ──
const ROLE_ORDER = ['source', 'transform', 'process', 'route', 'sink', 'utility'] as const;

interface TierConfig {
  label: string;
  color: string;
  bgAlpha: string;
  border: string;
}

const TIER_CONFIG: Record<string, TierConfig> = {
  source: { label: 'TIER 1 — DATA SOURCES & INGESTION', color: '#3B82F6', bgAlpha: 'rgba(59,130,246,0.08)', border: '#2563EB' },
  transform: { label: 'TIER 2 — TRANSFORMATIONS & ENRICHMENT', color: '#22C55E', bgAlpha: 'rgba(34,197,94,0.06)', border: '#16A34A' },
  process: { label: 'TIER 3 — EXECUTION & PROCESSING', color: '#A855F7', bgAlpha: 'rgba(168,85,247,0.08)', border: '#9333EA' },
  route: { label: 'TIER 4 — ROUTING & DISTRIBUTION', color: '#EAB308', bgAlpha: 'rgba(234,179,8,0.08)', border: '#CA8A04' },
  sink: { label: 'TIER 5 — DATA SINKS & OUTPUT', color: '#EF4444', bgAlpha: 'rgba(239,68,68,0.08)', border: '#DC2626' },
  utility: { label: 'TIER 6 — UTILITIES & MONITORING', color: '#6B7280', bgAlpha: 'rgba(107,114,128,0.06)', border: '#4B5563' },
};

const CONN_TYPES = {
  flow: { color: '#3B82F6', label: 'Data Flow', baseWidth: 1.5 },
  success: { color: '#22C55E', label: 'Success Path', baseWidth: 1.5 },
  failure: { color: '#EF4444', label: 'Failure Path', baseWidth: 2 },
  route: { color: '#EAB308', label: 'Routed Path', baseWidth: 1.5 },
  cross_tier: { color: '#A855F7', label: 'Cross-Tier', baseWidth: 2 },
};

interface ProcessorNode {
  id: string;
  name: string;
  type: string;
  group: string;
  role: string;
  category: string;
  connCount: number;
  mapped: boolean;
  confidence: number;
}

// ── Confidence color helper ──
function confColor(c: number): string {
  if (c >= 0.9) return '#22C55E';
  if (c >= 0.7) return '#F59E0B';
  if (c > 0) return '#EF4444';
  return '#475569';
}

// ── Processor box ──
function ProcessorBox({
  node, isHovered, isSelected, isDimmed, onHover, onClick,
}: {
  node: ProcessorNode;
  isHovered: boolean;
  isSelected: boolean;
  isDimmed: boolean;
  onHover: (id: string | null) => void;
  onClick: (id: string) => void;
}) {
  const tier = TIER_CONFIG[node.role] || TIER_CONFIG.transform;
  const borderColor = isSelected ? '#fff' : isHovered ? tier.color : `${tier.border}88`;

  return (
    <div
      onMouseEnter={() => onHover(node.id)}
      onMouseLeave={() => onHover(null)}
      onClick={() => onClick(node.id)}
      style={{
        background: isHovered || isSelected ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.3)',
        border: `${isSelected ? 2 : 1}px solid ${borderColor}`,
        borderRadius: 6,
        padding: '6px 10px',
        cursor: 'pointer',
        minWidth: 130,
        maxWidth: 200,
        transition: 'all 0.15s',
        opacity: isDimmed ? 0.25 : 1,
      }}
    >
      <div style={{ fontSize: 10, fontWeight: 700, color: '#E2E8F0', fontFamily: "'JetBrains Mono', monospace", lineHeight: 1.2, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
        {node.name}
      </div>
      <div style={{ display: 'flex', gap: 4, marginTop: 4, flexWrap: 'wrap', alignItems: 'center' }}>
        <span style={{ fontSize: 8, padding: '1px 5px', borderRadius: 3, background: `${tier.color}22`, color: tier.color, fontWeight: 600 }}>
          {node.type}
        </span>
        {node.connCount > 0 && (
          <span style={{ fontSize: 8, padding: '1px 5px', borderRadius: 3, background: 'rgba(107,114,128,0.2)', color: '#9CA3AF' }}>
            {node.connCount} conn
          </span>
        )}
        {node.confidence > 0 && (
          <span style={{ fontSize: 8, padding: '1px 5px', borderRadius: 3, background: `${confColor(node.confidence)}22`, color: confColor(node.confidence), fontWeight: 600 }}>
            {Math.round(node.confidence * 100)}%
          </span>
        )}
        {!node.mapped && node.confidence === 0 && (
          <span style={{ fontSize: 7, padding: '1px 4px', borderRadius: 3, background: 'rgba(239,68,68,0.15)', color: '#EF4444', fontWeight: 700, textTransform: 'uppercase' }}>
            unmapped
          </span>
        )}
      </div>
      {node.group !== '(root)' && (
        <div style={{ fontSize: 7, color: '#475569', marginTop: 2, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          {node.group}
        </div>
      )}
    </div>
  );
}

// ── Group header (collapsible) ──
function GroupHeader({
  group, count, expanded, onToggle, color,
}: {
  group: string;
  count: number;
  expanded: boolean;
  onToggle: () => void;
  color: string;
}) {
  return (
    <button
      onClick={onToggle}
      style={{
        display: 'flex', alignItems: 'center', gap: 6, background: 'rgba(255,255,255,0.02)',
        border: '1px solid rgba(255,255,255,0.04)', borderRadius: 4,
        cursor: 'pointer', padding: '4px 8px', marginBottom: 4,
      }}
    >
      <span style={{ fontSize: 10, color: '#64748B', transform: expanded ? 'rotate(90deg)' : 'rotate(0)', transition: 'transform 0.15s', display: 'inline-block' }}>&#9654;</span>
      <span style={{ fontSize: 10, fontWeight: 700, color, letterSpacing: '0.02em', fontFamily: "'JetBrains Mono', monospace" }}>
        {group}
      </span>
      <span style={{ fontSize: 9, color: '#64748B', background: 'rgba(255,255,255,0.05)', padding: '0 5px', borderRadius: 8, fontWeight: 600 }}>
        {count}
      </span>
    </button>
  );
}

// ── Detail panel ──
function DetailPanel({
  nodeId, nodes, connections,
}: {
  nodeId: string | null;
  nodes: ProcessorNode[];
  connections: Array<{ sourceName: string; destinationName: string; relationship: string }>;
}) {
  if (!nodeId) return (
    <div style={{ padding: 20, color: '#64748B', textAlign: 'center', fontSize: 12 }}>
      Click any processor to see its dependencies
    </div>
  );

  const node = nodes.find(n => n.id === nodeId);
  if (!node) return null;

  const outgoing = connections.filter(c => c.sourceName === nodeId);
  const incoming = connections.filter(c => c.destinationName === nodeId);
  const tier = TIER_CONFIG[node.role] || TIER_CONFIG.transform;

  return (
    <div style={{ padding: 14 }}>
      <div style={{ fontSize: 12, fontWeight: 800, color: '#E2E8F0', marginBottom: 2, fontFamily: "'JetBrains Mono', monospace" }}>
        {node.name}
      </div>
      <div style={{ fontSize: 9, color: '#64748B', marginBottom: 8 }}>
        {node.type} | {node.group} | <span style={{ color: tier.color }}>{node.role}</span>
      </div>

      {outgoing.length > 0 && (
        <div style={{ marginBottom: 10 }}>
          <div style={{ fontSize: 9, fontWeight: 700, color: '#94A3B8', textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: 4 }}>
            OUTPUTS ({outgoing.length})
          </div>
          {outgoing.slice(0, 20).map((c, i) => (
            <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 4, marginBottom: 3 }}>
              <div style={{ width: 6, height: 2, borderRadius: 1, background: '#22C55E', flexShrink: 0 }} />
              <span style={{ fontSize: 9, color: '#CBD5E1', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {c.destinationName}
              </span>
              <span style={{ fontSize: 8, color: '#475569', flexShrink: 0 }}>({c.relationship})</span>
            </div>
          ))}
          {outgoing.length > 20 && <div style={{ fontSize: 8, color: '#475569' }}>...and {outgoing.length - 20} more</div>}
        </div>
      )}

      {incoming.length > 0 && (
        <div>
          <div style={{ fontSize: 9, fontWeight: 700, color: '#94A3B8', textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: 4 }}>
            INPUTS ({incoming.length})
          </div>
          {incoming.slice(0, 20).map((c, i) => (
            <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 4, marginBottom: 3 }}>
              <div style={{ width: 6, height: 2, borderRadius: 1, background: '#3B82F6', flexShrink: 0 }} />
              <span style={{ fontSize: 9, color: '#CBD5E1', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {c.sourceName}
              </span>
              <span style={{ fontSize: 8, color: '#475569', flexShrink: 0 }}>({c.relationship})</span>
            </div>
          ))}
          {incoming.length > 20 && <div style={{ fontSize: 8, color: '#475569' }}>...and {incoming.length - 20} more</div>}
        </div>
      )}
    </div>
  );
}

// ── Main component ──
export default function TierDiagram({ processors, connections, mappings }: TierDiagramProps) {
  const [hoveredNode, setHoveredNode] = useState<string | null>(null);
  const [selectedNode, setSelectedNode] = useState<string | null>(null);
  const [expandedGroups, setExpandedGroups] = useState<Set<string>>(new Set());
  const [activeView, setActiveView] = useState<'diagram' | 'matrix'>('diagram');
  const containerRef = useRef<HTMLDivElement>(null);
  const innerRef = useRef<HTMLDivElement>(null);
  const nodeRefs = useRef<Record<string, HTMLDivElement | null>>({});
  const [lines, setLines] = useState<Array<{
    fromX: number; fromY: number; toX: number; toY: number;
    color: string; thickness: number; isActive: boolean; isDimmed: boolean;
  }>>([]);
  const [svgSize, setSvgSize] = useState({ w: 0, h: 0 });

  // Build node data — enrich with mapping info if available
  const nodes = useMemo<ProcessorNode[]>(() => {
    const connCounts: Record<string, number> = {};
    connections.forEach(c => {
      connCounts[c.sourceName] = (connCounts[c.sourceName] || 0) + 1;
      connCounts[c.destinationName] = (connCounts[c.destinationName] || 0) + 1;
    });
    // Build mapping lookup by name
    const mappingByName: Record<string, { role: string; category: string; mapped: boolean; confidence: number }> = {};
    if (mappings) {
      for (const m of mappings) {
        mappingByName[m.name] = { role: m.role, category: m.category, mapped: m.mapped, confidence: m.confidence };
      }
    }
    return processors.map(p => {
      const m = mappingByName[p.name];
      return {
        id: p.name,
        name: p.name,
        type: p.type,
        group: p.group || '(root)',
        role: m?.role || classifyRole(p.type),
        category: m?.category || '',
        connCount: connCounts[p.name] || 0,
        mapped: m?.mapped ?? true,
        confidence: m?.confidence ?? 0,
      };
    });
  }, [processors, connections, mappings]);

  // Group nodes by tier, then by processor type within each tier (clear labeling)
  const tierGroups = useMemo(() => {
    return ROLE_ORDER.map(role => {
      const tierNodes = nodes.filter(n => n.role === role);
      // Sub-group by processor type (e.g., "GetFile", "ConsumeKafka")
      const typeMap = new Map<string, ProcessorNode[]>();
      tierNodes.forEach(n => {
        // Use category if available, otherwise processor type
        const groupKey = n.category || n.type;
        const existing = typeMap.get(groupKey) || [];
        existing.push(n);
        typeMap.set(groupKey, existing);
      });
      // Sort groups by size descending
      const groups = Array.from(typeMap.entries()).sort((a, b) => b[1].length - a[1].length);
      return { role, config: TIER_CONFIG[role], groups, totalCount: tierNodes.length };
    }).filter(t => t.totalCount > 0);
  }, [nodes]);

  // Auto-expand groups with <= 8 members, collapse larger ones initially
  useEffect(() => {
    const expanded = new Set<string>();
    tierGroups.forEach(tier => {
      tier.groups.forEach(([group, members]) => {
        if (members.length <= 12) {
          expanded.add(`${tier.role}:${group}`);
        }
      });
    });
    setExpandedGroups(expanded);
  }, [tierGroups]);

  // Connected node set for highlighting
  const connectedNodes = useMemo(() => {
    const active = hoveredNode || selectedNode;
    if (!active) return null;
    const set = new Set<string>();
    set.add(active);
    connections.forEach(c => {
      if (c.sourceName === active) set.add(c.destinationName);
      if (c.destinationName === active) set.add(c.sourceName);
    });
    return set;
  }, [hoveredNode, selectedNode, connections]);

  const toggleGroup = useCallback((key: string) => {
    setExpandedGroups(prev => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }, []);

  const handleClick = useCallback((id: string) => {
    setSelectedNode(prev => prev === id ? null : id);
  }, []);

  const registerRef = useCallback((id: string, el: HTMLDivElement | null) => {
    nodeRefs.current[id] = el;
  }, []);

  // Compute SVG lines from DOM positions (relative to inner content div)
  const computeLines = useCallback(() => {
    const inner = innerRef.current;
    if (!inner || activeView !== 'diagram') return;
    const innerRect = inner.getBoundingClientRect();

    setSvgSize({ w: inner.scrollWidth, h: inner.scrollHeight });

    const active = hoveredNode || selectedNode;
    const newLines = connections.map(conn => {
      const fromEl = nodeRefs.current[conn.sourceName];
      const toEl = nodeRefs.current[conn.destinationName];
      if (!fromEl || !toEl) return null;

      const fromRect = fromEl.getBoundingClientRect();
      const toRect = toEl.getBoundingClientRect();

      const fromX = fromRect.left + fromRect.width / 2 - innerRect.left;
      const fromY = fromRect.top + fromRect.height - innerRect.top;
      const toX = toRect.left + toRect.width / 2 - innerRect.left;
      const toY = toRect.top - innerRect.top;

      const isActive = active ? (conn.sourceName === active || conn.destinationName === active) : false;
      const isDimmed = active ? !isActive : false;

      const color = conn.relationship === 'failure' ? '#EF4444' : isActive ? '#60A5FA' : '#334155';
      const thickness = isActive ? 2 : 1;

      return { fromX, fromY, toX, toY, color, thickness, isActive, isDimmed };
    }).filter(Boolean) as typeof lines;

    setLines(newLines);
  }, [hoveredNode, selectedNode, connections, activeView]);

  useEffect(() => {
    if (activeView !== 'diagram') return;
    const timer = setTimeout(computeLines, 150);
    return () => clearTimeout(timer);
  }, [computeLines, expandedGroups]);

  // Recalculate on scroll
  useEffect(() => {
    const container = containerRef.current;
    if (!container || activeView !== 'diagram') return;
    // Only recalculate active lines on hover/select, not passive scroll (perf)
    const onScroll = () => {
      if (hoveredNode || selectedNode) computeLines();
    };
    container.addEventListener('scroll', onScroll, { passive: true });
    return () => container.removeEventListener('scroll', onScroll);
  }, [computeLines, hoveredNode, selectedNode, activeView]);

  // Stats
  const stats = useMemo(() => {
    const roles: Record<string, number> = {};
    nodes.forEach(n => { roles[n.role] = (roles[n.role] || 0) + 1; });
    const groups = new Set(nodes.map(n => n.group)).size;
    return { total: nodes.length, connections: connections.length, roles, groups };
  }, [nodes, connections]);

  if (!processors || processors.length === 0) {
    return (
      <div className="rounded-lg border border-border bg-gray-800/30 p-8 text-center text-gray-500 text-sm">
        No processors to visualize
      </div>
    );
  }

  return (
    <div style={{ background: '#0A0F1A', borderRadius: 12, border: '1px solid #1E293B', overflow: 'hidden' }}>
      {/* Header */}
      <div style={{
        padding: '10px 16px', borderBottom: '1px solid #1E293B',
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        background: 'rgba(15,23,42,0.9)',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <span style={{ fontSize: 13, fontWeight: 800, color: '#E2E8F0', letterSpacing: '-0.02em' }}>Flow Architecture</span>
          <div style={{ display: 'flex', gap: 2 }}>
            {([['diagram', 'Tier Diagram', '\u25A4'], ['matrix', 'Connection Matrix', '\u229E']] as const).map(([id, label, icon]) => (
              <button key={id} onClick={() => setActiveView(id as 'diagram' | 'matrix')} style={{
                padding: '4px 12px', borderRadius: 6, border: 'none', cursor: 'pointer', fontSize: 10, fontWeight: 600,
                background: activeView === id ? 'rgba(59,130,246,0.2)' : 'transparent',
                color: activeView === id ? '#60A5FA' : '#64748B',
              }}>
                {icon} {label}
              </button>
            ))}
          </div>
        </div>
        {/* Legend */}
        <div style={{ display: 'flex', gap: 10, fontSize: 9 }}>
          {Object.entries(TIER_CONFIG).map(([role, cfg]) => (
            <div key={role} style={{ display: 'flex', alignItems: 'center', gap: 3 }}>
              <div style={{ width: 8, height: 8, borderRadius: '50%', background: cfg.color }} />
              <span style={{ color: '#94A3B8', textTransform: 'capitalize' }}>{role}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Stats bar */}
      <div style={{
        padding: '5px 16px', borderBottom: '1px solid #1E293B', display: 'flex', gap: 16, fontSize: 10, color: '#64748B',
      }}>
        <span><strong style={{ color: '#E2E8F0' }}>{stats.total}</strong> Processors</span>
        <span><strong style={{ color: '#E2E8F0' }}>{stats.connections}</strong> Connections</span>
        <span><strong style={{ color: '#E2E8F0' }}>{stats.groups}</strong> Process Groups</span>
        {Object.entries(stats.roles).map(([role, count]) => (
          <span key={role} style={{ color: TIER_CONFIG[role]?.color || '#64748B' }}>
            <strong>{count}</strong> {role}
          </span>
        ))}
      </div>

      {/* Main content */}
      <div style={{ display: 'flex', maxHeight: 'calc(100vh - 200px)', minHeight: 500, overflow: 'hidden' }}>
        {/* Diagram / Matrix area */}
        <div ref={containerRef} style={{ flex: 1, overflowY: 'auto', overflowX: 'auto', position: 'relative' }}>
          {activeView === 'diagram' ? (
            <div ref={innerRef} style={{ position: 'relative', padding: '16px 20px', minWidth: 800 }}>
              {/* SVG overlay for connection lines */}
              <svg style={{ position: 'absolute', top: 0, left: 0, width: svgSize.w || '100%', height: svgSize.h || '100%', pointerEvents: 'none', zIndex: 1 }}>
                <defs>
                  <marker id="tier-arrow" viewBox="0 0 10 6" refX="10" refY="3" markerWidth="7" markerHeight="5" orient="auto">
                    <path d="M0,0 L10,3 L0,6" fill="#475569" />
                  </marker>
                  <marker id="tier-arrow-active" viewBox="0 0 10 6" refX="10" refY="3" markerWidth="7" markerHeight="5" orient="auto">
                    <path d="M0,0 L10,3 L0,6" fill="#60A5FA" />
                  </marker>
                </defs>
                {lines.map((line, i) => {
                  const midY = (line.fromY + line.toY) / 2;
                  const dx = line.toX - line.fromX;
                  const cpOffset = Math.min(Math.abs(dx) * 0.3, 50);
                  const path = `M${line.fromX},${line.fromY} C${line.fromX},${midY - cpOffset} ${line.toX},${midY + cpOffset} ${line.toX},${line.toY}`;
                  return (
                    <path key={i} d={path}
                      fill="none" stroke={line.color}
                      strokeWidth={line.thickness}
                      opacity={line.isDimmed ? 0.06 : line.isActive ? 0.9 : 0.2}
                      markerEnd={line.isActive ? 'url(#tier-arrow-active)' : 'url(#tier-arrow)'}
                      style={{ transition: 'opacity 0.2s' }}
                    />
                  );
                })}
              </svg>

              {/* Tier bands */}
              {tierGroups.map((tier) => {
                const cfg = tier.config;
                return (
                  <div key={tier.role} style={{
                    background: cfg.bgAlpha,
                    border: `1px solid ${cfg.border}33`,
                    borderRadius: 10,
                    padding: '12px 16px',
                    marginBottom: 10,
                    position: 'relative',
                    zIndex: 2,
                  }}>
                    {/* Tier label */}
                    <div style={{
                      fontSize: 10, fontWeight: 800, color: cfg.color, textTransform: 'uppercase',
                      letterSpacing: '0.08em', marginBottom: 8,
                      display: 'flex', alignItems: 'center', gap: 8,
                    }}>
                      <div style={{ width: 3, height: 14, borderRadius: 2, background: cfg.color }} />
                      {cfg.label}
                      <span style={{ fontSize: 9, color: '#475569', fontWeight: 500, letterSpacing: 0 }}>
                        ({tier.totalCount} processors)
                      </span>
                    </div>

                    {/* Groups within tier */}
                    {tier.groups.map(([group, members]) => {
                      const key = `${tier.role}:${group}`;
                      const isExpanded = expandedGroups.has(key);
                      return (
                        <div key={key} style={{ marginBottom: 6 }}>
                          {tier.groups.length > 1 && (
                            <GroupHeader group={group} count={members.length}
                              expanded={isExpanded} onToggle={() => toggleGroup(key)} color={cfg.color} />
                          )}
                          {(isExpanded || tier.groups.length === 1) && (
                            <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', paddingLeft: tier.groups.length > 1 ? 12 : 0 }}>
                              {members.map(node => (
                                <div key={node.id} ref={el => registerRef(node.id, el)}>
                                  <ProcessorBox
                                    node={node}
                                    isHovered={hoveredNode === node.id}
                                    isSelected={selectedNode === node.id}
                                    isDimmed={connectedNodes !== null && !connectedNodes.has(node.id)}
                                    onHover={setHoveredNode}
                                    onClick={handleClick}
                                  />
                                </div>
                              ))}
                            </div>
                          )}
                          {!isExpanded && tier.groups.length > 1 && (
                            <div style={{ fontSize: 9, color: '#475569', paddingLeft: 12 }}>
                              {members.length} processors collapsed — click to expand
                            </div>
                          )}
                        </div>
                      );
                    })}
                  </div>
                );
              })}
            </div>
          ) : (
            /* Connection Matrix view */
            <div style={{ padding: 16 }}>
              <div style={{ fontSize: 11, fontWeight: 700, color: '#E2E8F0', marginBottom: 4 }}>Connection Matrix</div>
              <div style={{ fontSize: 10, color: '#64748B', marginBottom: 12 }}>
                Top 30 most-connected processors — hover to highlight relationships
              </div>
              <ConnectionMatrix
                nodes={nodes}
                connections={connections}
                hoveredNode={hoveredNode}
                onHover={setHoveredNode}
              />
            </div>
          )}
        </div>

        {/* Right detail panel */}
        <div style={{
          width: 260, borderLeft: '1px solid #1E293B', background: 'rgba(15,23,42,0.6)',
          overflowY: 'auto', flexShrink: 0,
        }}>
          <div style={{ padding: '10px 14px', borderBottom: '1px solid #1E293B' }}>
            <div style={{ fontSize: 10, fontWeight: 700, color: '#64748B', textTransform: 'uppercase', letterSpacing: '0.1em' }}>
              Dependencies
            </div>
          </div>
          <DetailPanel nodeId={selectedNode} nodes={nodes} connections={connections} />

          {/* Connection density */}
          <div style={{ padding: '10px 14px', borderTop: '1px solid #1E293B' }}>
            <div style={{ fontSize: 9, fontWeight: 700, color: '#64748B', textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: 6 }}>
              Top Connected
            </div>
            {[...nodes].sort((a, b) => b.connCount - a.connCount).slice(0, 15).map((n, idx, sorted) => {
              const maxConn = sorted[0]?.connCount || 1;
              const barColor = n.connCount > 6 ? '#EF4444' : n.connCount > 3 ? '#F59E0B' : '#3B82F6';
              return (
                <div key={n.id} style={{ display: 'flex', alignItems: 'center', gap: 4, marginBottom: 3, cursor: 'pointer' }}
                  onClick={() => handleClick(n.id)}
                  onMouseEnter={() => setHoveredNode(n.id)}
                  onMouseLeave={() => setHoveredNode(null)}
                >
                  <div style={{ fontSize: 8, color: '#64748B', width: 14, textAlign: 'right', fontFamily: 'monospace' }}>
                    {n.connCount}
                  </div>
                  <div style={{ flex: 1, height: 4, borderRadius: 2, background: '#1E293B', overflow: 'hidden' }}>
                    <div style={{
                      height: '100%', borderRadius: 2, background: barColor,
                      width: `${(n.connCount / maxConn) * 100}%`, transition: 'width 0.3s',
                    }} />
                  </div>
                  <div style={{ fontSize: 8, color: '#94A3B8', fontFamily: 'monospace', minWidth: 60, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {n.name.length > 14 ? n.name.slice(0, 12) + '..' : n.name}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
}

// ── Connection matrix ──
function ConnectionMatrix({
  nodes, connections, hoveredNode, onHover,
}: {
  nodes: ProcessorNode[];
  connections: Array<{ sourceName: string; destinationName: string; relationship: string }>;
  hoveredNode: string | null;
  onHover: (id: string | null) => void;
}) {
  // Pick top 30 most connected
  const topNodes = useMemo(() => {
    return [...nodes].sort((a, b) => b.connCount - a.connCount).slice(0, 30);
  }, [nodes]);

  const connSet = useMemo(() => {
    const set = new Set<string>();
    connections.forEach(c => {
      set.add(`${c.sourceName}|${c.destinationName}`);
    });
    return set;
  }, [connections]);

  return (
    <div style={{ overflowX: 'auto', overflowY: 'auto', maxHeight: 500 }}>
      <table style={{ borderCollapse: 'collapse', fontSize: 8, fontFamily: "'JetBrains Mono', monospace" }}>
        <thead>
          <tr>
            <th style={{ padding: '3px 6px', background: '#1E293B', color: '#64748B', position: 'sticky', left: 0, zIndex: 2, textAlign: 'left', borderBottom: '1px solid #334155', fontSize: 8 }}>
              Source / Dest
            </th>
            {topNodes.map(n => (
              <th key={n.id}
                onMouseEnter={() => onHover(n.id)}
                onMouseLeave={() => onHover(null)}
                style={{
                  padding: '2px 3px', background: hoveredNode === n.id ? 'rgba(255,255,255,0.1)' : '#1E293B',
                  color: hoveredNode === n.id ? '#fff' : '#94A3B8', cursor: 'pointer',
                  writingMode: 'vertical-lr', textOrientation: 'mixed', minWidth: 20,
                  borderBottom: '1px solid #334155', borderRight: '1px solid #1a1f2e',
                  fontWeight: 500, maxHeight: 80,
                }}
              >
                {n.name.length > 12 ? n.name.slice(0, 10) + '..' : n.name}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {topNodes.map(src => (
            <tr key={src.id}>
              <td
                onMouseEnter={() => onHover(src.id)}
                onMouseLeave={() => onHover(null)}
                style={{
                  padding: '4px 6px', background: hoveredNode === src.id ? 'rgba(255,255,255,0.1)' : '#111827',
                  color: hoveredNode === src.id ? '#fff' : TIER_CONFIG[src.role]?.color || '#94A3B8',
                  position: 'sticky', left: 0, zIndex: 1, cursor: 'pointer',
                  borderBottom: '1px solid #1a1f2e', fontWeight: 600, whiteSpace: 'nowrap',
                  maxWidth: 120, overflow: 'hidden', textOverflow: 'ellipsis',
                }}
              >
                {src.name.length > 18 ? src.name.slice(0, 16) + '..' : src.name}
              </td>
              {topNodes.map(dst => {
                const hasConn = connSet.has(`${src.id}|${dst.id}`);
                const hasRev = connSet.has(`${dst.id}|${src.id}`);
                const isHighlighted = hoveredNode === src.id || hoveredNode === dst.id;
                return (
                  <td key={dst.id} style={{
                    padding: 2,
                    background: hasConn || hasRev
                      ? isHighlighted ? 'rgba(255,255,255,0.15)' : 'rgba(59,130,246,0.15)'
                      : isHighlighted ? 'rgba(255,255,255,0.03)' : 'transparent',
                    borderBottom: '1px solid #1a1f2e', borderRight: '1px solid #1a1f2e',
                    textAlign: 'center', verticalAlign: 'middle',
                  }}>
                    {hasConn && (
                      <div style={{
                        display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
                        width: 14, height: 12, borderRadius: 2, fontSize: 7, fontWeight: 800,
                        background: 'rgba(59,130,246,0.3)', color: '#60A5FA', border: '1px solid rgba(59,130,246,0.5)',
                      }}>&#x2192;</div>
                    )}
                    {hasRev && (
                      <div style={{
                        display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
                        width: 14, height: 12, borderRadius: 2, fontSize: 7, fontWeight: 800,
                        background: 'rgba(168,85,247,0.3)', color: '#A855F7', border: '1px solid rgba(168,85,247,0.5)',
                      }}>&#x2190;</div>
                    )}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
