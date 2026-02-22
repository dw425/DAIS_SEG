import React, { useEffect, useRef, useState } from 'react';
import * as d3 from 'd3';
import type { LineageNode, LineageEdge, LineageGraph as LineageGraphData } from '../../types/pipeline';

interface LineageGraphProps {
  data: LineageGraphData;
  width?: number;
  height?: number;
  onNodeClick?: (node: LineageNode) => void;
}

const NODE_COLORS: Record<string, string> = {
  source: '#22c55e',    // green
  transform: '#3b82f6', // blue
  sink: '#f97316',      // orange
};

const NODE_WIDTH = 160;
const NODE_HEIGHT = 40;
const LAYER_GAP = 200;
const NODE_GAP = 60;

export default function LineageGraph({ data, width: propWidth, height: propHeight, onNodeClick }: LineageGraphProps) {
  const svgRef = useRef<SVGSVGElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const [dimensions, setDimensions] = useState({ width: propWidth ?? 900, height: propHeight ?? 500 });
  const [tooltip, setTooltip] = useState<{ x: number; y: number; node: LineageNode } | null>(null);

  // Observe container size
  useEffect(() => {
    if (!containerRef.current) return;
    const observer = new ResizeObserver((entries) => {
      const entry = entries[0];
      if (entry) {
        setDimensions({
          width: propWidth ?? entry.contentRect.width,
          height: propHeight ?? Math.max(400, entry.contentRect.height),
        });
      }
    });
    observer.observe(containerRef.current);
    return () => observer.disconnect();
  }, [propWidth, propHeight]);

  useEffect(() => {
    if (!svgRef.current || !data.nodes.length) return;

    const svg = d3.select(svgRef.current);
    svg.selectAll('*').remove();

    const { width, height } = dimensions;

    // Compute layout: sources on left, transforms in middle, sinks on right
    const layers: Record<string, LineageNode[]> = { source: [], transform: [], sink: [] };
    for (const node of data.nodes) {
      const type = node.type || 'transform';
      (layers[type] ?? layers.transform).push(node);
    }

    const layerOrder = ['source', 'transform', 'sink'];
    const layerX: Record<string, number> = {};
    const margin = 60;
    const usableWidth = width - margin * 2;
    layerOrder.forEach((layer, i) => {
      layerX[layer] = margin + (i * usableWidth) / (layerOrder.length - 1 || 1);
    });

    // Position nodes
    const nodePositions: Record<string, { x: number; y: number }> = {};
    for (const layer of layerOrder) {
      const nodes = layers[layer] || [];
      const totalHeight = nodes.length * (NODE_HEIGHT + NODE_GAP) - NODE_GAP;
      const startY = Math.max(margin, (height - totalHeight) / 2);
      nodes.forEach((node, i) => {
        nodePositions[node.id] = {
          x: layerX[layer],
          y: startY + i * (NODE_HEIGHT + NODE_GAP),
        };
      });
    }

    // Setup zoom/pan
    const g = svg.append('g');
    const zoom = d3.zoom<SVGSVGElement, unknown>()
      .scaleExtent([0.3, 3])
      .on('zoom', (event) => g.attr('transform', event.transform));
    svg.call(zoom);

    // Draw edges
    const edgeGroup = g.append('g').attr('class', 'edges');
    for (const edge of data.edges) {
      const src = nodePositions[edge.source];
      const tgt = nodePositions[edge.target];
      if (!src || !tgt) continue;

      const x1 = src.x + NODE_WIDTH / 2;
      const y1 = src.y + NODE_HEIGHT / 2;
      const x2 = tgt.x - NODE_WIDTH / 2;
      const y2 = tgt.y + NODE_HEIGHT / 2;

      // Bezier curve
      const midX = (x1 + x2) / 2;
      edgeGroup.append('path')
        .attr('d', `M${x1},${y1} C${midX},${y1} ${midX},${y2} ${x2},${y2}`)
        .attr('fill', 'none')
        .attr('stroke', '#4b5563')
        .attr('stroke-width', 1.5)
        .attr('stroke-dasharray', edge.relationship === 'failure' ? '4,4' : 'none')
        .attr('opacity', 0.6)
        .attr('marker-end', 'url(#arrowhead)');
    }

    // Arrow marker
    svg.append('defs').append('marker')
      .attr('id', 'arrowhead')
      .attr('viewBox', '0 0 10 10')
      .attr('refX', 10)
      .attr('refY', 5)
      .attr('markerWidth', 6)
      .attr('markerHeight', 6)
      .attr('orient', 'auto')
      .append('path')
      .attr('d', 'M0,0 L10,5 L0,10 Z')
      .attr('fill', '#6b7280');

    // Highlight critical path
    if (data.criticalPath.length > 1) {
      for (let i = 0; i < data.criticalPath.length - 1; i++) {
        const src = nodePositions[data.criticalPath[i]];
        const tgt = nodePositions[data.criticalPath[i + 1]];
        if (!src || !tgt) continue;
        const x1 = src.x + NODE_WIDTH / 2;
        const y1 = src.y + NODE_HEIGHT / 2;
        const x2 = tgt.x - NODE_WIDTH / 2;
        const y2 = tgt.y + NODE_HEIGHT / 2;
        const midX = (x1 + x2) / 2;
        edgeGroup.append('path')
          .attr('d', `M${x1},${y1} C${midX},${y1} ${midX},${y2} ${x2},${y2}`)
          .attr('fill', 'none')
          .attr('stroke', '#ef4444')
          .attr('stroke-width', 2.5)
          .attr('opacity', 0.8);
      }
    }

    // Draw nodes
    const nodeGroup = g.append('g').attr('class', 'nodes');
    for (const node of data.nodes) {
      const pos = nodePositions[node.id];
      if (!pos) continue;

      const color = NODE_COLORS[node.type] ?? '#6b7280';
      const confidenceColor = node.confidence >= 0.9 ? '#22c55e' : node.confidence >= 0.7 ? '#eab308' : '#ef4444';

      const ng = nodeGroup.append('g')
        .attr('transform', `translate(${pos.x - NODE_WIDTH / 2},${pos.y})`)
        .style('cursor', 'pointer')
        .on('click', () => onNodeClick?.(node))
        .on('mouseenter', (event: MouseEvent) => {
          setTooltip({ x: event.clientX, y: event.clientY, node });
        })
        .on('mouseleave', () => setTooltip(null));

      // Node background
      ng.append('rect')
        .attr('width', NODE_WIDTH)
        .attr('height', NODE_HEIGHT)
        .attr('rx', 6)
        .attr('fill', `${color}15`)
        .attr('stroke', color)
        .attr('stroke-width', 1.5);

      // Node label
      ng.append('text')
        .attr('x', 8)
        .attr('y', 16)
        .attr('fill', '#e5e7eb')
        .attr('font-size', '11px')
        .attr('font-family', 'monospace')
        .text(node.name.length > 18 ? node.name.slice(0, 16) + '..' : node.name);

      // Processor type
      ng.append('text')
        .attr('x', 8)
        .attr('y', 30)
        .attr('fill', '#9ca3af')
        .attr('font-size', '9px')
        .text(node.processorType.length > 20 ? node.processorType.slice(0, 18) + '..' : node.processorType);

      // Confidence badge
      ng.append('rect')
        .attr('x', NODE_WIDTH - 36)
        .attr('y', 4)
        .attr('width', 30)
        .attr('height', 14)
        .attr('rx', 3)
        .attr('fill', `${confidenceColor}30`);

      ng.append('text')
        .attr('x', NODE_WIDTH - 21)
        .attr('y', 14)
        .attr('text-anchor', 'middle')
        .attr('fill', confidenceColor)
        .attr('font-size', '8px')
        .attr('font-weight', 'bold')
        .text(`${Math.round(node.confidence * 100)}%`);

      // Critical path indicator
      if (data.criticalPath.includes(node.id)) {
        ng.append('circle')
          .attr('cx', NODE_WIDTH - 8)
          .attr('cy', NODE_HEIGHT - 8)
          .attr('r', 4)
          .attr('fill', '#ef4444');
      }
    }

    // Layer labels
    for (const layer of layerOrder) {
      const x = layerX[layer];
      const labels: Record<string, string> = { source: 'SOURCES', transform: 'TRANSFORMS', sink: 'SINKS' };
      g.append('text')
        .attr('x', x)
        .attr('y', 24)
        .attr('text-anchor', 'middle')
        .attr('fill', NODE_COLORS[layer] ?? '#6b7280')
        .attr('font-size', '10px')
        .attr('font-weight', 'bold')
        .attr('letter-spacing', '0.05em')
        .text(labels[layer] ?? layer.toUpperCase());
    }

  }, [data, dimensions, onNodeClick]);

  return (
    <div ref={containerRef} className="relative w-full" style={{ minHeight: 400 }}>
      <svg
        ref={svgRef}
        width={dimensions.width}
        height={dimensions.height}
        className="bg-gray-900/50 rounded-lg border border-border"
      />
      {/* Tooltip */}
      {tooltip && (
        <div
          className="fixed z-50 px-3 py-2 rounded-lg bg-gray-800 border border-border shadow-xl text-xs pointer-events-none"
          style={{ left: tooltip.x + 12, top: tooltip.y - 10 }}
        >
          <p className="text-gray-200 font-medium">{tooltip.node.name}</p>
          <p className="text-gray-500">{tooltip.node.processorType}</p>
          <p className="text-gray-400 mt-1">
            Type: <span className="capitalize">{tooltip.node.type}</span> |
            Confidence: {Math.round(tooltip.node.confidence * 100)}%
          </p>
          {tooltip.node.columns && tooltip.node.columns.length > 0 && (
            <p className="text-gray-500 mt-1">Columns: {tooltip.node.columns.join(', ')}</p>
          )}
        </div>
      )}
      {/* Legend */}
      <div className="absolute bottom-2 left-2 flex gap-3 text-[10px] text-gray-500">
        <span className="flex items-center gap-1"><span className="w-2 h-2 rounded bg-green-500" /> Source</span>
        <span className="flex items-center gap-1"><span className="w-2 h-2 rounded bg-blue-500" /> Transform</span>
        <span className="flex items-center gap-1"><span className="w-2 h-2 rounded bg-orange-500" /> Sink</span>
        <span className="flex items-center gap-1"><span className="w-2 h-2 rounded bg-red-500" /> Critical Path</span>
      </div>
    </div>
  );
}
