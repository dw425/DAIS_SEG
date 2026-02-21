/**
 * validators/accelerator-feedback.js — Accelerator feedback generation
 *
 * Extracted from index.html lines 8636-8682.
 * Aggregates intent gaps and line-validation gaps, groups by processor type,
 * and produces structured feedback for the accelerator to address.
 *
 * @module validators/accelerator-feedback
 */

/**
 * Generate accelerator feedback from validation gaps.
 *
 * @param {object} opts
 * @param {Array}  opts.intentGaps    - Gaps from intent analysis
 * @param {Array}  opts.lineGapItems  - Line-validation items with status !== 'good'
 * @param {object} opts.nifiDatabricksMap - The NIFI_DATABRICKS_MAP lookup (type -> {desc, conf, tpl})
 * @returns {{allGaps:Array, gapsByType:Object}}
 */
export function generateFeedback({ intentGaps, lineGapItems, nifiDatabricksMap }) {
  const allGaps = [...intentGaps];
  lineGapItems.forEach(lg => {
    if (!allGaps.some(g => g.proc === lg.name)) {
      const shortType = (lg.type || '').split('.').pop() || 'Unknown';
      const missingProps = (lg.propsMissing || []).slice(0, 3).join(', ');
      const covPct = typeof lg.propCoverage === 'number' ? Math.round(lg.propCoverage) : 0;
      let issue;
      if (lg.status === 'missing') {
        issue = `No Databricks mapping found for ${shortType}`;
      } else if (lg.status === 'no-cell') {
        issue = `Mapped but no notebook cell generated for this processor`;
      } else {
        issue = `Low property coverage (${covPct}%)${missingProps ? ' — missing: ' + missingProps : ''}`;
      }
      allGaps.push({
        proc: lg.name || 'Unknown',
        type: lg.type || '',
        intent: '',
        role: lg.role || '',
        confidence: lg.confidence,
        propCoverage: lg.propCoverage,
        issue,
      });
    }
  });

  // Group by processor type
  const gapsByType = {};
  allGaps.forEach(g => {
    const shortType = g.type.split('.').pop();
    if (!gapsByType[shortType]) gapsByType[shortType] = [];
    gapsByType[shortType].push(g);
  });

  // Enrich each type group with severity and template info
  const enrichedGroups = {};
  Object.entries(gapsByType).sort((a, b) => b[1].length - a[1].length).forEach(([type, gaps]) => {
    const severity = gaps.some(g =>
      g.issue.includes('Missing') || g.issue.includes('Unmapped') || g.issue.includes('No mapping')
    ) ? 'HIGH' : 'MEDIUM';

    const mapEntry = nifiDatabricksMap ? nifiDatabricksMap[type] : null;

    enrichedGroups[type] = {
      gaps,
      severity,
      mapEntry,
    };
  });

  return { allGaps, gapsByType: enrichedGroups };
}
