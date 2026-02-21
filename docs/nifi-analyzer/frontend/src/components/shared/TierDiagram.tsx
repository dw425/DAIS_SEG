import React, { useRef, useEffect } from 'react';

interface TierDiagramProps {
  tiers: Record<string, string[]>;
  width?: number;
  height?: number;
}

/**
 * Placeholder tier/flow visualization using basic canvas drawing.
 * In production, this would use D3 for proper force-directed layout.
 */
export default function TierDiagram({ tiers, width = 600, height = 300 }: TierDiagramProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    const dpr = window.devicePixelRatio || 1;
    canvas.width = width * dpr;
    canvas.height = height * dpr;
    ctx.scale(dpr, dpr);

    // Clear
    ctx.fillStyle = '#0a0a1a';
    ctx.fillRect(0, 0, width, height);

    const tierKeys = Object.keys(tiers).sort();
    if (tierKeys.length === 0) {
      ctx.fillStyle = '#6b7280';
      ctx.font = '14px Inter, sans-serif';
      ctx.textAlign = 'center';
      ctx.fillText('No tier data available', width / 2, height / 2);
      return;
    }

    const tierWidth = width / tierKeys.length;
    const colors = ['#3b82f6', '#8b5cf6', '#f59e0b', '#10b981', '#ef4444', '#ec4899'];

    tierKeys.forEach((tier, ti) => {
      const processors = tiers[tier];
      const x = ti * tierWidth + tierWidth / 2;
      const color = colors[ti % colors.length];

      // Tier label
      ctx.fillStyle = color;
      ctx.font = 'bold 12px Inter, sans-serif';
      ctx.textAlign = 'center';
      ctx.fillText(tier, x, 24);

      // Tier column line
      ctx.strokeStyle = color + '30';
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(x, 32);
      ctx.lineTo(x, height - 16);
      ctx.stroke();

      // Processor nodes
      const nodeSpacing = Math.min(36, (height - 60) / Math.max(processors.length, 1));
      processors.forEach((proc, pi) => {
        const y = 50 + pi * nodeSpacing;

        // Node circle
        ctx.fillStyle = color + '40';
        ctx.strokeStyle = color;
        ctx.lineWidth = 1.5;
        ctx.beginPath();
        ctx.arc(x, y, 10, 0, Math.PI * 2);
        ctx.fill();
        ctx.stroke();

        // Processor label
        ctx.fillStyle = '#d1d5db';
        ctx.font = '9px Inter, sans-serif';
        ctx.textAlign = 'center';
        const label = proc.length > 18 ? proc.slice(0, 16) + '..' : proc;
        ctx.fillText(label, x, y + 22);
      });

      // Connection arrows between tiers
      if (ti < tierKeys.length - 1) {
        const nextX = (ti + 1) * tierWidth + tierWidth / 2;
        ctx.strokeStyle = '#4b5563';
        ctx.lineWidth = 1;
        ctx.setLineDash([4, 4]);
        ctx.beginPath();
        ctx.moveTo(x + 14, 50);
        ctx.lineTo(nextX - 14, 50);
        ctx.stroke();
        ctx.setLineDash([]);

        // Arrow head
        ctx.fillStyle = '#4b5563';
        ctx.beginPath();
        ctx.moveTo(nextX - 14, 50);
        ctx.lineTo(nextX - 20, 46);
        ctx.lineTo(nextX - 20, 54);
        ctx.closePath();
        ctx.fill();
      }
    });
  }, [tiers, width, height]);

  return (
    <div className="rounded-lg border border-border overflow-hidden bg-gray-950">
      <div className="px-3 py-2 border-b border-border bg-gray-900/50">
        <span className="text-xs text-gray-400 font-medium">Flow Tier Diagram</span>
      </div>
      <canvas
        ref={canvasRef}
        style={{ width, height }}
        className="block"
      />
    </div>
  );
}
