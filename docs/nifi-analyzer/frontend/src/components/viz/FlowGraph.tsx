import React, { useState, useRef, useCallback, useMemo, useEffect } from 'react';
import { ROLE_COLORS, classifyRole } from '../../utils/processorRoles';
import { normalizeConfidence } from '../../utils/confidence';

interface Processor { name: string; type: string; group: string }
interface Connection { sourceName: string; destinationName: string; relationship: string }
interface Mapping { name: string; type: string; role: string; confidence: number }

export interface FlowGraphProps {
  processors: Processor[];
  connections: Connection[];
  mappings?: Mapping[];
  layout?: 'hierarchical' | 'grouped';
  selectedProcessor?: string | null;
  onSelectProcessor?: (name: string | null) => void;
}

interface NodeLayout { x: number; y: number; w: number; h: number; name: string; type: string; role: string; confidence: number }

function computeLayout(processors: Processor[], connections: Connection[], mappings?: Mapping[], mode: string = 'hierarchical'): NodeLayout[] {
  const mappingMap = new Map<string, Mapping>();
  mappings?.forEach((m) => mappingMap.set(m.name, m));

  const ROLE_ORDER = ['source', 'transform', 'process', 'route', 'sink', 'utility'];
  const NODE_W = 180;
  const NODE_H = 48;
  const GAP_X = 60;
  const GAP_Y = 30;

  // Build adjacency for topological depth
  const outEdges = new Map<string, string[]>();
  const inDeg = new Map<string, number>();
  processors.forEach((p) => { outEdges.set(p.name, []); inDeg.set(p.name, 0); });
  connections.forEach((c) => {
    outEdges.get(c.sourceName)?.push(c.destinationName);
    inDeg.set(c.destinationName, (inDeg.get(c.destinationName) ?? 0) + 1);
  });

  if (mode === 'grouped') {
    // Group by role, lay out columns
    const groups = new Map<string, Processor[]>();
    processors.forEach((p) => {
      const role = mappingMap.get(p.name)?.role ?? classifyRole(p.type);
      if (!groups.has(role)) groups.set(role, []);
      groups.get(role)!.push(p);
    });

    const nodes: NodeLayout[] = [];
    let col = 0;
    ROLE_ORDER.forEach((role) => {
      const group = groups.get(role);
      if (!group || group.length === 0) return;
      group.forEach((p, row) => {
        const m = mappingMap.get(p.name);
        nodes.push({
          x: col * (NODE_W + GAP_X) + 40,
          y: row * (NODE_H + GAP_Y) + 40,
          w: NODE_W, h: NODE_H,
          name: p.name, type: p.type,
          role: m?.role ?? classifyRole(p.type),
          confidence: m?.confidence ?? 0,
        });
      });
      col++;
    });
    return nodes;
  }

  // Hierarchical: BFS levels
  const levels = new Map<string, number>();
  const queue: string[] = [];
  processors.forEach((p) => { if ((inDeg.get(p.name) ?? 0) === 0) { queue.push(p.name); levels.set(p.name, 0); } });
  while (queue.length > 0) {
    const cur = queue.shift()!;
    const lvl = levels.get(cur) ?? 0;
    for (const next of outEdges.get(cur) ?? []) {
      if (!levels.has(next) || levels.get(next)! < lvl + 1) {
        levels.set(next, lvl + 1);
        queue.push(next);
      }
    }
  }
  // Assign unvisited
  processors.forEach((p) => { if (!levels.has(p.name)) levels.set(p.name, 0); });

  const byLevel = new Map<number, Processor[]>();
  processors.forEach((p) => {
    const lv = levels.get(p.name) ?? 0;
    if (!byLevel.has(lv)) byLevel.set(lv, []);
    byLevel.get(lv)!.push(p);
  });

  const nodes: NodeLayout[] = [];
  const sortedLevels = [...byLevel.keys()].sort((a, b) => a - b);
  sortedLevels.forEach((lv) => {
    const group = byLevel.get(lv)!;
    group.forEach((p, row) => {
      const m = mappingMap.get(p.name);
      nodes.push({
        x: lv * (NODE_W + GAP_X) + 40,
        y: row * (NODE_H + GAP_Y) + 40,
        w: NODE_W, h: NODE_H,
        name: p.name, type: p.type,
        role: m?.role ?? classifyRole(p.type),
        confidence: m?.confidence ?? 0,
      });
    });
  });
  return nodes;
}

export default function FlowGraph({ processors, connections, mappings, layout = 'hierarchical', selectedProcessor, onSelectProcessor }: FlowGraphProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [pan, setPan] = useState({ x: 0, y: 0 });
  const [zoom, setZoom] = useState(1);
  const [dragging, setDragging] = useState(false);
  const [dragStart, setDragStart] = useState({ x: 0, y: 0 });
  const [tooltip, setTooltip] = useState<{ x: number; y: number; node: NodeLayout } | null>(null);

  const nodes = useMemo(() => computeLayout(processors, connections, mappings, layout), [processors, connections, mappings, layout]);
  const nodeMap = useMemo(() => new Map(nodes.map((n) => [n.name, n])), [nodes]);

  // Canvas bounds
  const bounds = useMemo(() => {
    if (nodes.length === 0) return { w: 800, h: 400 };
    const maxX = Math.max(...nodes.map((n) => n.x + n.w)) + 80;
    const maxY = Math.max(...nodes.map((n) => n.y + n.h)) + 80;
    return { w: maxX, h: maxY };
  }, [nodes]);

  // Connected edges for selected node
  const connectedEdges = useMemo(() => {
    if (!selectedProcessor) return new Set<number>();
    const set = new Set<number>();
    connections.forEach((c, i) => {
      if (c.sourceName === selectedProcessor || c.destinationName === selectedProcessor) set.add(i);
    });
    return set;
  }, [connections, selectedProcessor]);

  // Mouse handlers for panning
  const handleMouseDown = useCallback((e: React.MouseEvent) => {
    if ((e.target as HTMLElement).closest('[data-node]')) return;
    setDragging(true);
    setDragStart({ x: e.clientX - pan.x, y: e.clientY - pan.y });
  }, [pan]);

  const handleMouseMove = useCallback((e: React.MouseEvent) => {
    if (dragging) {
      setPan({ x: e.clientX - dragStart.x, y: e.clientY - dragStart.y });
    }
  }, [dragging, dragStart]);

  const handleMouseUp = useCallback(() => setDragging(false), []);

  // Use ref-based native listener for wheel to allow { passive: false }
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const handleWheel = (e: WheelEvent) => {
      e.preventDefault();
      setZoom((z) => Math.max(0.2, Math.min(3, z - e.deltaY * 0.001)));
    };
    el.addEventListener('wheel', handleWheel, { passive: false });
    return () => el.removeEventListener('wheel', handleWheel);
  }, []);

  // Mini-map scale
  const miniScale = 0.1;
  const miniW = bounds.w * miniScale;
  const miniH = bounds.h * miniScale;

  return (
    <div className="relative rounded-lg border border-border bg-gray-900/50 overflow-hidden" style={{ height: 500 }}>
      {/* Main canvas */}
      <div
        ref={containerRef}
        className="w-full h-full cursor-grab active:cursor-grabbing overflow-hidden"
        onMouseDown={handleMouseDown}
        onMouseMove={handleMouseMove}
        onMouseUp={handleMouseUp}
        onMouseLeave={handleMouseUp}
      >
        <svg
          width={bounds.w}
          height={bounds.h}
          style={{ transform: `translate(${pan.x}px, ${pan.y}px) scale(${zoom})`, transformOrigin: '0 0' }}
        >
          {/* Connections */}
          {connections.map((c, i) => {
            const src = nodeMap.get(c.sourceName);
            const dst = nodeMap.get(c.destinationName);
            if (!src || !dst) return null;
            const isHighlighted = connectedEdges.has(i);
            return (
              <line
                key={i}
                x1={src.x + src.w} y1={src.y + src.h / 2}
                x2={dst.x} y2={dst.y + dst.h / 2}
                stroke={isHighlighted ? '#fff' : '#4B5563'}
                strokeWidth={isHighlighted ? 2 : 1}
                strokeOpacity={selectedProcessor && !isHighlighted ? 0.2 : 0.7}
                markerEnd="url(#arrow)"
              />
            );
          })}
          <defs>
            <marker id="arrow" viewBox="0 0 10 10" refX="10" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
              <path d="M 0 0 L 10 5 L 0 10 z" fill="#6B7280" />
            </marker>
          </defs>

          {/* Nodes */}
          {nodes.map((node) => {
            const color = ROLE_COLORS[node.role] ?? ROLE_COLORS.utility;
            const isSelected = selectedProcessor === node.name;
            return (
              <g key={node.name} data-node>
                <rect
                  x={node.x} y={node.y} width={node.w} height={node.h}
                  rx={6}
                  fill={isSelected ? color : '#1F2937'}
                  stroke={color}
                  strokeWidth={isSelected ? 2.5 : 1.5}
                  opacity={selectedProcessor && !isSelected && !connectedEdges.size ? 0.4 : 1}
                  className="cursor-pointer"
                  onClick={() => onSelectProcessor?.(isSelected ? null : node.name)}
                  onMouseEnter={(e) => setTooltip({ x: e.clientX, y: e.clientY, node })}
                  onMouseLeave={() => setTooltip(null)}
                />
                <text
                  x={node.x + 8} y={node.y + 20}
                  fill={isSelected ? '#fff' : '#E5E7EB'}
                  fontSize={11} fontFamily="monospace"
                  pointerEvents="none"
                  className="select-none"
                >
                  {node.name.length > 22 ? node.name.slice(0, 20) + '..' : node.name}
                </text>
                <text
                  x={node.x + 8} y={node.y + 36}
                  fill="#9CA3AF" fontSize={9} fontFamily="monospace"
                  pointerEvents="none"
                  className="select-none"
                >
                  {node.type.length > 26 ? node.type.slice(0, 24) + '..' : node.type}
                </text>
              </g>
            );
          })}
        </svg>
      </div>

      {/* Tooltip */}
      {tooltip && (
        <div
          className="fixed z-50 px-3 py-2 rounded-lg bg-gray-800 border border-border shadow-lg text-xs pointer-events-none"
          style={{ left: tooltip.x + 12, top: tooltip.y - 10 }}
        >
          <p className="text-gray-200 font-medium">{tooltip.node.name}</p>
          <p className="text-gray-400">{tooltip.node.type}</p>
          <p className="text-gray-500">
            Confidence: <span className={tooltip.node.confidence >= 0.9 ? 'text-green-400' : tooltip.node.confidence >= 0.7 ? 'text-amber-400' : 'text-red-400'}>
              {normalizeConfidence(tooltip.node.confidence)}%
            </span>
          </p>
        </div>
      )}

      {/* Mini-map */}
      <div
        className="absolute bottom-3 right-3 rounded border border-border bg-gray-900/80 overflow-hidden"
        style={{ width: Math.max(miniW, 100), height: Math.max(miniH, 60) }}
      >
        <svg width={Math.max(miniW, 100)} height={Math.max(miniH, 60)} viewBox={`0 0 ${bounds.w} ${bounds.h}`}>
          {nodes.map((n) => (
            <rect
              key={n.name}
              x={n.x} y={n.y} width={n.w} height={n.h}
              fill={ROLE_COLORS[n.role] ?? '#6B7280'}
              opacity={0.6}
            />
          ))}
          {/* Viewport indicator */}
          <rect
            x={-pan.x / zoom} y={-pan.y / zoom}
            width={(containerRef.current?.clientWidth ?? 800) / zoom}
            height={(containerRef.current?.clientHeight ?? 500) / zoom}
            fill="none" stroke="#fff" strokeWidth={4} opacity={0.4}
          />
        </svg>
      </div>
    </div>
  );
}
