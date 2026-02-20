/**
 * generators/code-validator.js — Generated Code Validation (Python syntax check)
 *
 * Validates generated notebook code for common issues: unresolved placeholders,
 * unbalanced parentheses, NiFi EL expressions, and missing imports.
 *
 * Extracted from index.html lines 3762-3829.
 *
 * @module generators/code-validator
 */

/**
 * Validate an array of generated code cells for common issues.
 *
 * Checks performed:
 * - Unresolved placeholders ({var_name} patterns, threshold > 3)
 * - Unbalanced parentheses (tolerance > 2)
 * - Unresolved NiFi Expression Language (${...})
 * - Missing imports for known third-party modules
 *
 * @param {Array<string|Object>} cells — code strings or cell objects with .code/.textContent
 * @returns {Array<{ cellIndex: number, valid: boolean, issues: string[] }>}
 */
export function validateGeneratedCode(cells) {
  const results = [];
  if (!cells) return results;

  cells.forEach((cell, i) => {
    const code = typeof cell === 'string' ? cell : (cell.code || cell.textContent || '');
    const issues = [];

    // Check for unresolved placeholders
    const placeholders = code.match(/\{[a-z_]+\}/g) || [];
    if (placeholders.length > 3) {
      issues.push(`${placeholders.length} unresolved placeholders: ${placeholders.slice(0, 3).join(', ')}...`);
    }

    // Check for unbalanced brackets
    const opens = (code.match(/\(/g) || []).length;
    const closes = (code.match(/\)/g) || []).length;
    if (Math.abs(opens - closes) > 2) {
      issues.push(`Unbalanced parentheses: ${opens} open, ${closes} close`);
    }

    // Check for unresolved NiFi EL
    const unresolvedEL = code.match(/\$\{[^}]+\}/g) || [];
    if (unresolvedEL.length > 0) {
      issues.push(`${unresolvedEL.length} unresolved NiFi EL expressions: ${unresolvedEL.slice(0, 2).join(', ')}...`);
    }

    // Check for missing imports
    const usedModules = new Set();
    if (code.includes('requests.')) usedModules.add('requests');
    if (code.includes('boto3.')) usedModules.add('boto3');
    if (code.includes('paramiko.')) usedModules.add('paramiko');
    if (code.includes('pika.')) usedModules.add('pika');
    if (code.includes('stomp.')) usedModules.add('stomp');
    if (code.includes('pymongo')) usedModules.add('pymongo');
    if (code.includes('elasticsearch')) usedModules.add('elasticsearch');

    const importedModules = new Set();
    (code.match(/^import\s+(\w+)/gm) || []).forEach(m => importedModules.add(m.replace('import ', '')));
    (code.match(/^from\s+(\w+)/gm) || []).forEach(m => importedModules.add(m.replace('from ', '')));

    const missingImports = [...usedModules].filter(m => !importedModules.has(m));
    if (missingImports.length > 0) {
      issues.push(`Missing imports: ${missingImports.join(', ')}`);
    }

    results.push({
      cellIndex: i,
      valid: issues.length === 0,
      issues: issues,
    });
  });

  return results;
}
