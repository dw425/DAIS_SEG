/**
 * utils/graph-utils.js — Graph classification utilities
 *
 * Extracted from index.html line ~6072.
 */

import { ROLE_TIER_ORDER } from '../core/constants.js';

/**
 * Determine the dominant processor role for a process group based on
 * counts of each role type.
 *
 * @param {{sources: number, routes: number, transforms: number,
 *          processes: number, sinks: number, utilities: number}} stats
 * @returns {string} one of 'source','route','transform','process','sink','utility'
 */
export function classifyGroupDominantRole(stats) {
  const counts = [
    ['source', stats.sources],
    ['route', stats.routes],
    ['transform', stats.transforms],
    ['process', stats.processes],
    ['sink', stats.sinks],
    ['utility', stats.utilities],
  ];

  // Sort by count descending, then by tier order for tie-breaking
  counts.sort((a, b) =>
    b[1] !== a[1]
      ? b[1] - a[1]
      : ROLE_TIER_ORDER.indexOf(a[0]) - ROLE_TIER_ORDER.indexOf(b[0])
  );

  return counts[0][1] > 0 ? counts[0][0] : 'process';
}
