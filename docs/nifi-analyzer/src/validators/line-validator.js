/**
 * validators/line-validator.js — Processor-to-cell mapping with property coverage
 *
 * Extracted from index.html lines 8348-8438.
 * Validates that each mapped NiFi processor has a corresponding notebook cell
 * and that the cell code reflects the processor's important properties.
 *
 * @module validators/line-validator
 */

const PROP_COVERAGE_GOOD = 70;
const PROP_COVERAGE_PARTIAL = 30;

/**
 * Validate processor-to-cell line mapping and property coverage.
 *
 * @param {object} opts
 * @param {Array}  opts.mappings        - Assessment mappings
 * @param {object} opts.procByName      - O(1) lookup of processor by name
 * @param {Array}  opts.cellTextsLower  - Lowercased cell source array
 * @param {Function} opts.findCellsWithVar - (varName) => Array of cell indices
 * @param {number} [opts.batchSize=100]
 * @param {Function} [opts.onProgress]
 * @returns {Promise<{lineResults:Array, lineMatched:number, lineGaps:number, lineScore:number}>}
 */
export async function validateLines({
  mappings,
  procByName,
  cellTextsLower,
  findCellsWithVar,
  batchSize = 100,
  onProgress,
}) {
  const lineResults = [];
  let lineMatched = 0, lineGaps = 0;

  for (let bi = 0; bi < mappings.length; bi += batchSize) {
    const batch = mappings.slice(bi, bi + batchSize);
    batch.forEach(m => {
      const varName = m.name.replace(/[^a-zA-Z0-9]/g, '_').toLowerCase();
      const matchedCellIndices = findCellsWithVar(varName);
      if (matchedCellIndices.length === 0) {
        const nameLower = m.name.toLowerCase();
        const altMatches = findCellsWithVar(nameLower);
        if (altMatches.length > 0) matchedCellIndices.push(...altMatches);
      }

      const proc = procByName[m.name];
      const props = proc ? proc.properties || {} : {};
      const importantProps = Object.entries(props).filter(([k, v]) => v && !/password|secret|token/i.test(k));
      let propsInCode = 0;
      let propsMissing = [];

      if (matchedCellIndices.length > 0) {
        const allCode = matchedCellIndices.map(i => cellTextsLower[i]).join('\n');
        importantProps.forEach(([k, v]) => {
          const keyNorm = k.toLowerCase().replace(/[\s-]/g, '');
          const valNorm = String(v).toLowerCase().substring(0, 50);
          if (allCode.includes(keyNorm) || allCode.includes(valNorm) || allCode.includes(k.toLowerCase())) {
            propsInCode++;
          } else {
            propsMissing.push(k);
          }
        });
      }

      const safePropCoverage = importantProps.length > 0 ? Math.round((propsInCode / importantProps.length) * 100) : 100;
      const propCoverage = safePropCoverage || 0;
      if (!propsMissing) propsMissing = [];
      const status = !m.mapped ? 'missing' : matchedCellIndices.length === 0 ? 'no-cell' : propCoverage >= PROP_COVERAGE_GOOD ? 'good' : propCoverage >= PROP_COVERAGE_PARTIAL ? 'partial' : 'weak';

      if (status === 'good') lineMatched++;
      else lineGaps++;

      lineResults.push({
        name: m.name, type: m.type, role: m.role, mapped: m.mapped,
        cellCount: matchedCellIndices.length, cellIndices: matchedCellIndices,
        propTotal: importantProps.length, propsInCode, propCoverage: propCoverage || 0, propsMissing: propsMissing || [], status,
        confidence: m.confidence, desc: m.desc
      });
    });
    if (onProgress) {
      onProgress(30 + Math.round((bi / mappings.length) * 25), 'Line validation: ' + Math.min(bi + batchSize, mappings.length) + '/' + mappings.length + ' mappings...');
    }
    await new Promise(r => setTimeout(r, 0));
  }

  const lineScore = mappings.length ? Math.round((lineMatched / mappings.length) * 100) : 0;

  return { lineResults, lineMatched, lineGaps, lineScore };
}
