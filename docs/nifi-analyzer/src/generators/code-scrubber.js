/**
 * generators/code-scrubber.js -- Post-process generated cells to remove
 * dangerous patterns and replace stubs with explicit errors.
 *
 * @module generators/code-scrubber
 */

/**
 * Scrub all generated cells for safety and completeness.
 * @param {Array} cells - notebook cells [{source, type, ...}]
 * @returns {Array} - scrubbed cells (mutated in place for efficiency)
 */
export function scrubGeneratedCode(cells) {
  cells.forEach(cell => {
    if (cell.type !== 'code') return;
    let src = cell.source || '';

    // 1. Replace TODO/FIXME with MANUAL REVIEW REQUIRED
    src = src.replace(/# *TODO[: ]+(.*)/gi, (m, desc) =>
      `# MANUAL REVIEW REQUIRED: ${desc.trim()}`);
    src = src.replace(/# *FIXME[: ]+(.*)/gi, (m, desc) =>
      `# MANUAL REVIEW REQUIRED: ${desc.trim()}`);
    src = src.replace(/# *PLACEHOLDER[: ]+(.*)/gi, (m, desc) =>
      `# MANUAL REVIEW REQUIRED: ${desc.trim()}`);

    // 2. Replace bare except:pass with logged version
    src = src.replace(/except\s*:\s*\n(\s*)pass/g,
      'except Exception as _scrub_e:\n$1print(f"[WARNING] Suppressed error: {_scrub_e}")');

    // 3. Flag empty function bodies
    src = src.replace(/def (\w+)\(([^)]*)\):\s*\n(\s*)pass\b/g,
      'def $1($2):\n$3raise NotImplementedError("$1 requires implementation")');

    // 4. Replace deprecated DBFS paths with Volumes paths (only in string literals, not comments)
    const lines = src.split('\n');
    src = lines.map(line => {
      if (line.trimStart().startsWith('#')) return line;
      return line
        .replace(/["']\/dbfs\/([^"']+)["']/g, '"/Volumes/{catalog}/{schema}/data/$1"')
        .replace(/["']\/mnt\/([^"']+)["']/g, '"/Volumes/{catalog}/{schema}/mounts/$1"');
    }).join('\n');

    cell.source = src;
  });
  return cells;
}
