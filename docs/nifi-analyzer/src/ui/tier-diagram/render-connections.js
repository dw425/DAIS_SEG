/**
 * ui/tier-diagram/render-connections.js — SVG connection rendering
 *
 * Extracted from index.html lines 6666-6757.
 * Creates SVG paths with arrow markers and glow filters between nodes.
 */

/**
 * Render SVG connections between nodes in the tier diagram.
 * Extracted from index.html lines 6666-6757.
 *
 * @param {HTMLElement} container   — the diagram container element
 * @param {Array}       connections — connection data array
 * @param {Object}      nodeEls    — map of node id -> DOM element
 */
export function renderConnections(container, connections, nodeEls) {
  // Remove old SVG
  const oldSvg = container.querySelector('svg.tier-svg');
  if (oldSvg) oldSvg.remove();

  if (!connections.length) return;

  const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  svg.classList.add('tier-svg');
  svg.style.position = 'absolute';
  svg.style.top = '0';
  svg.style.left = '0';
  svg.style.width = container.scrollWidth + 'px';
  svg.style.height = container.scrollHeight + 'px';
  svg.style.pointerEvents = 'none';
  svg.style.zIndex = '1';
  svg.setAttribute('viewBox', `0 0 ${container.scrollWidth} ${container.scrollHeight}`);

  // Defs: arrow markers and glow filter
  const defs = document.createElementNS('http://www.w3.org/2000/svg', 'defs');

  // Glow filter for highlighted paths
  const filter = document.createElementNS('http://www.w3.org/2000/svg', 'filter');
  filter.setAttribute('id', 'glow');
  filter.setAttribute('x', '-50%');
  filter.setAttribute('y', '-50%');
  filter.setAttribute('width', '200%');
  filter.setAttribute('height', '200%');
  const blur = document.createElementNS('http://www.w3.org/2000/svg', 'feGaussianBlur');
  blur.setAttribute('stdDeviation', '3');
  blur.setAttribute('result', 'blur');
  filter.appendChild(blur);
  const merge = document.createElementNS('http://www.w3.org/2000/svg', 'feMerge');
  const mn1 = document.createElementNS('http://www.w3.org/2000/svg', 'feMergeNode');
  mn1.setAttribute('in', 'blur');
  const mn2 = document.createElementNS('http://www.w3.org/2000/svg', 'feMergeNode');
  mn2.setAttribute('in', 'SourceGraphic');
  merge.appendChild(mn1);
  merge.appendChild(mn2);
  filter.appendChild(merge);
  defs.appendChild(filter);

  // Arrow markers for each color
  const arrowColors = {
    'default': '#4B5563', 'blue': '#3B82F6', 'purple': '#6366F1',
    'red': '#EF4444', 'amber': '#F59E0B', 'green': '#21C354', 'white': '#FAFAFA'
  };
  Object.entries(arrowColors).forEach(([name, color]) => {
    const marker = document.createElementNS('http://www.w3.org/2000/svg', 'marker');
    marker.setAttribute('id', 'arrow-' + name);
    marker.setAttribute('viewBox', '0 0 10 8');
    marker.setAttribute('refX', '10');
    marker.setAttribute('refY', '4');
    marker.setAttribute('markerWidth', '8');
    marker.setAttribute('markerHeight', '6');
    marker.setAttribute('orient', 'auto');
    const ap = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    ap.setAttribute('d', 'M0,0 L10,4 L0,8 Z');
    ap.setAttribute('fill', color);
    marker.appendChild(ap);
    defs.appendChild(marker);
  });
  svg.appendChild(defs);

  const cRect = container.getBoundingClientRect();

  connections.forEach(conn => {
    const fromEl = nodeEls[conn.from];
    const toEl = nodeEls[conn.to];
    if (!fromEl || !toEl) return;

    const fromRect = fromEl.getBoundingClientRect();
    const toRect = toEl.getBoundingClientRect();

    const fromX = fromRect.left + fromRect.width / 2 - cRect.left + container.scrollLeft;
    const fromY = fromRect.top + fromRect.height - cRect.top + container.scrollTop;
    const toX = toRect.left + toRect.width / 2 - cRect.left + container.scrollLeft;
    const toY = toRect.top - cRect.top + container.scrollTop;

    const dy = toY - fromY;
    const cp = Math.max(Math.abs(dy) * 0.35, 30);
    const cpx = (toX - fromX) * 0.15;

    const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    path.setAttribute('d', `M${fromX},${fromY} C${fromX + cpx},${fromY + cp} ${toX - cpx},${toY - cp} ${toX},${toY}`);
    const strokeColor = conn.color || '#4B5563';
    path.setAttribute('stroke', strokeColor);
    path.setAttribute('stroke-width', String(conn.width || 1.5));
    path.setAttribute('fill', 'none');

    // Pick arrow marker by closest color
    const arrowId = strokeColor.includes('EF44') ? 'arrow-red'
      : strokeColor.includes('F59E') || strokeColor.includes('F5') ? 'arrow-amber'
      : strokeColor.includes('6366') ? 'arrow-purple'
      : strokeColor.includes('3B82') ? 'arrow-blue'
      : strokeColor.includes('21C3') ? 'arrow-green'
      : 'arrow-default';
    path.setAttribute('marker-end', `url(#${arrowId})`);
    path.setAttribute('opacity', '0.35');
    path.dataset.from = conn.from;
    path.dataset.to = conn.to;
    path.dataset.origColor = strokeColor;
    path.dataset.origWidth = String(conn.width || 1.5);
    if (conn.dash) path.setAttribute('stroke-dasharray', '6,4');
    if (conn.inCycle) path.setAttribute('stroke-dasharray', '8,4');
    svg.appendChild(path);
  });

  container.style.position = 'relative';
  container.insertBefore(svg, container.firstChild);
}
