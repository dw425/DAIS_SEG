import React, { useCallback, useRef } from 'react';

export interface FlowGraphControlsProps {
  zoom: number;
  onZoomIn: () => void;
  onZoomOut: () => void;
  onFitToView: () => void;
  onToggleFullscreen: () => void;
  isFullscreen: boolean;
  layout: 'hierarchical' | 'grouped';
  onLayoutChange: (layout: 'hierarchical' | 'grouped') => void;
  svgRef?: React.RefObject<SVGSVGElement | null>;
}

export default function FlowGraphControls({
  zoom, onZoomIn, onZoomOut, onFitToView, onToggleFullscreen, isFullscreen,
  layout, onLayoutChange, svgRef,
}: FlowGraphControlsProps) {

  const handleExportSVG = useCallback(() => {
    if (!svgRef?.current) return;
    const svgData = new XMLSerializer().serializeToString(svgRef.current);
    const blob = new Blob([svgData], { type: 'image/svg+xml;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'flow-graph.svg';
    a.click();
    URL.revokeObjectURL(url);
  }, [svgRef]);

  return (
    <div className="absolute top-3 right-3 flex flex-col gap-1.5 z-10">
      {/* Zoom controls */}
      <div className="flex flex-col rounded-lg bg-gray-800/90 border border-border overflow-hidden">
        <button onClick={onZoomIn} className="px-3 py-1.5 text-gray-300 hover:bg-gray-700 text-sm font-mono transition" title="Zoom in">+</button>
        <div className="text-center text-[10px] text-gray-500 tabular-nums border-y border-border py-0.5">{Math.round(zoom * 100)}%</div>
        <button onClick={onZoomOut} className="px-3 py-1.5 text-gray-300 hover:bg-gray-700 text-sm font-mono transition" title="Zoom out">-</button>
      </div>

      {/* Fit + Fullscreen */}
      <div className="flex flex-col rounded-lg bg-gray-800/90 border border-border overflow-hidden">
        <button onClick={onFitToView} className="px-3 py-1.5 text-gray-300 hover:bg-gray-700 text-[10px] transition" title="Fit to view">FIT</button>
        <button onClick={onToggleFullscreen} className="px-3 py-1.5 text-gray-300 hover:bg-gray-700 text-[10px] border-t border-border transition" title="Toggle fullscreen">
          {isFullscreen ? 'EXIT' : 'FULL'}
        </button>
      </div>

      {/* Layout selector */}
      <select
        value={layout}
        onChange={(e) => onLayoutChange(e.target.value as 'hierarchical' | 'grouped')}
        className="rounded-lg bg-gray-800/90 border border-border text-[10px] text-gray-300 px-2 py-1.5 focus:outline-none"
      >
        <option value="hierarchical">Hierarchical</option>
        <option value="grouped">Grouped</option>
      </select>

      {/* Export */}
      <button
        onClick={handleExportSVG}
        className="rounded-lg bg-gray-800/90 border border-border text-[10px] text-gray-300 px-2 py-1.5 hover:bg-gray-700 transition"
        title="Export as SVG"
      >
        SVG
      </button>
    </div>
  );
}
