// ================================================================
// nel/el-evaluator.js — Runtime NiFi Expression Language evaluator
// Extracted from monolith lines 7975-8077
// FIX CRIT: Removed eval() usage — replaced with safe expression
// parser using a whitelist of allowed operations.
// ================================================================

let _elCounter = 0;

/**
 * Safe math expression evaluator.
 * Replaces eval() with a whitelist of allowed arithmetic operations.
 * Supports: +, -, *, /, %, parentheses, and numeric values.
 *
 * @param {string} expression - Math expression string
 * @returns {number|null} Result or null if expression is invalid
 */
function safeMathEval(expression) {
  // Only allow digits, operators, parens, dots, and whitespace
  if (!/^[\d\s+\-*/%().]+$/.test(expression)) return null;

  // Tokenize into numbers and operators
  const tokens = expression.match(/(\d+\.?\d*|[+\-*/%()])/g);
  if (!tokens) return null;

  // Simple recursive-descent parser for arithmetic expressions
  let pos = 0;

  function peek() { return tokens[pos]; }
  function consume() { return tokens[pos++]; }

  function parseExpr() {
    let left = parseTerm();
    while (pos < tokens.length && (peek() === '+' || peek() === '-')) {
      const op = consume();
      const right = parseTerm();
      if (left === null || right === null) return null;
      left = op === '+' ? left + right : left - right;
    }
    return left;
  }

  function parseTerm() {
    let left = parseFactor();
    while (pos < tokens.length && (peek() === '*' || peek() === '/' || peek() === '%')) {
      const op = consume();
      const right = parseFactor();
      if (left === null || right === null) return null;
      if (op === '*') left = left * right;
      else if (op === '/') left = right !== 0 ? Math.floor(left / right) : 0;
      else left = right !== 0 ? left % right : 0;
    }
    return left;
  }

  function parseFactor() {
    if (peek() === '(') {
      consume(); // '('
      const val = parseExpr();
      if (peek() === ')') consume(); // ')'
      return val;
    }
    if (peek() === '-') {
      consume();
      const val = parseFactor();
      return val !== null ? -val : null;
    }
    const num = parseFloat(consume());
    return isNaN(num) ? null : num;
  }

  try {
    const result = parseExpr();
    return result;
  } catch (e) {
    return null;
  }
}

/**
 * Evaluate a NiFi Expression Language expression at runtime.
 * Resolves ${...} blocks against a provided attribute map.
 *
 * This is used for runtime evaluation (e.g., building filenames,
 * resolving dynamic SQL), NOT for PySpark translation.
 *
 * @param {string} expr - Expression string potentially containing ${...} blocks
 * @param {Object} attributes - Map of attribute names to values
 * @returns {string} Evaluated string with all ${...} blocks resolved
 */
export function evaluateNiFiEL(expr, attributes) {
  if (!expr || typeof expr !== 'string' || !expr.includes('${')) return expr;
  return expr.replace(/\$\{([^}]+)\}/g, (fullMatch, inner) => {
    const parts = inner.split(':');
    const attrName = parts[0].trim();
    let val;
    if (attrName === 'now()') val = new Date().toISOString();
    else if (attrName === 'nextInt()' || attrName === 'random()') val = String(1000 + (_elCounter++));
    else if (attrName === 'UUID()' || attrName === 'uuid()') val = 'el_' + (_elCounter++) + '_' + Date.now().toString(36);
    else if (attrName === 'hostname()') val = 'databricks-worker';
    else if (attrName.startsWith('literal(')) {
      const litMatch = attrName.match(/literal\(['"]([^'"]*)['"]\)/);
      val = litMatch ? litMatch[1] : '';
    } else {
      val = attributes[attrName] !== undefined ? attributes[attrName] : undefined;
    }
    if (val === undefined || val === null) val = fullMatch;
    for (let i = 1; i < parts.length; i++) {
      const fn = parts[i].trim();
      if (fn.startsWith('replaceAll(')) {
        const args = fn.match(/replaceAll\(['"]([^'"]*)['"]\s*,\s*['"]([^'"]*)['"]\)/);
        if (args) { try { val = String(val).replace(new RegExp(args[1], 'g'), args[2]); } catch(e) { val = String(val).split(args[1]).join(args[2]); } }
      } else if (fn.startsWith('replace(')) {
        const args = fn.match(/replace\(['"]([^'"]*)['"]\s*,\s*['"]([^'"]*)['"]\)/);
        if (args) val = String(val).split(args[1]).join(args[2]);
      } else if (fn.startsWith('substring(')) {
        const args = fn.match(/substring\((\d+)(?:\s*,\s*(\d+))?\)/);
        if (args) val = String(val).substring(parseInt(args[1]), args[2] ? parseInt(args[2]) : undefined);
      } else if (fn === 'toUpper()') val = String(val).toUpperCase();
      else if (fn === 'toLower()') val = String(val).toLowerCase();
      else if (fn === 'trim()') val = String(val).trim();
      else if (fn === 'length()') val = String(String(val).length);
      else if (fn === 'isEmpty()') val = String(!val || String(val).length === 0);
      else if (fn.startsWith('equals(')) {
        const arg = fn.match(/equals\(["']([^"']*)["']\)/);
        val = arg ? String(String(val) === arg[1]) : 'false';
      } else if (fn.startsWith('contains(')) {
        const arg = fn.match(/contains\(["']([^"']*)["']\)/);
        val = arg ? String(String(val).includes(arg[1])) : 'false';
      } else if (fn.startsWith('startsWith(')) {
        const arg = fn.match(/startsWith\(["']([^"']*)["']\)/);
        val = arg ? String(String(val).startsWith(arg[1])) : 'false';
      } else if (fn.startsWith('endsWith(')) {
        const arg = fn.match(/endsWith\(["']([^"']*)["']\)/);
        val = arg ? String(String(val).endsWith(arg[1])) : 'false';
      } else if (fn.startsWith('append(')) {
        const arg = fn.match(/append\(["']([^"']*)["']\)/);
        if (arg) val = String(val) + arg[1];
      } else if (fn.startsWith('prepend(')) {
        const arg = fn.match(/prepend\(["']([^"']*)["']\)/);
        if (arg) val = arg[1] + String(val);
      } else if (fn.startsWith('toDate(')) { /* keep string as-is */ }
      else if (fn.startsWith('format(')) {
        const arg = fn.match(/format\(["']([^"']*)["']\)/);
        if (arg) { try { const d = new Date(val); if (!isNaN(d)) { val = arg[1].replace('yyyy', d.getFullYear()).replace('MM', String(d.getMonth()+1).padStart(2,'0')).replace('dd', String(d.getDate()).padStart(2,'0')).replace('HH', String(d.getHours()).padStart(2,'0')).replace('mm', String(d.getMinutes()).padStart(2,'0')).replace('ss', String(d.getSeconds()).padStart(2,'0')); } } catch(e) {} }
      } else if (fn.startsWith('minus(')) {
        const arg = fn.match(/minus\((\d+)\)/); if (arg) { const n = parseFloat(val); if (!isNaN(n)) val = String(n - parseInt(arg[1])); }
      } else if (fn.startsWith('plus(')) {
        const arg = fn.match(/plus\((\d+)\)/); if (arg) { const n = parseFloat(val); if (!isNaN(n)) val = String(n + parseInt(arg[1])); }
      } else if (fn.startsWith('multiply(')) {
        const arg = fn.match(/multiply\((\d+)\)/); if (arg) { const n = parseFloat(val); if (!isNaN(n)) val = String(n * parseInt(arg[1])); }
      } else if (fn.startsWith('divide(')) {
        const arg = fn.match(/divide\((\d+)\)/); if (arg) { const n = parseFloat(val); if (!isNaN(n)) val = String(Math.floor(n / parseInt(arg[1]))); }
      } else if (fn.startsWith('literal(')) {
        const arg = fn.match(/literal\(["']([^"']*)["']\)/); if (arg) val = arg[1];
      } else if (fn === 'not()') { val = String(val === 'false' || val === '0' || val === '' || val === 'null'); }
      else if (fn === 'toNumber()') { const n = parseFloat(val); val = isNaN(n) ? '0' : String(n); }
      else if (fn === 'toString()') { val = String(val); }
      else if (fn === 'isNull()') { val = String(val === null || val === undefined || val === ''); }
      else if (fn === 'notNull()') { val = String(val !== null && val !== undefined && val !== ''); }
      else if (fn.startsWith('padLeft(')) { const args = fn.match(/padLeft\((\d+)\s*,\s*['"]([^'"]*)['"]/); if (args) val = String(val).padStart(parseInt(args[1]), args[2]); }
      else if (fn.startsWith('padRight(')) { const args = fn.match(/padRight\((\d+)\s*,\s*['"]([^'"]*)['"]/); if (args) val = String(val).padEnd(parseInt(args[1]), args[2]); }
      else if (fn.startsWith('substringBefore(')) { const args = fn.match(/substringBefore\(['"]([^'"]*)['"]/); if (args) { const idx = String(val).indexOf(args[1]); val = idx >= 0 ? String(val).substring(0, idx) : val; } }
      else if (fn.startsWith('substringAfter(')) { const args = fn.match(/substringAfter\(['"]([^'"]*)['"]/); if (args) { const idx = String(val).indexOf(args[1]); val = idx >= 0 ? String(val).substring(idx + args[1].length) : ''; } }
      else if (fn.startsWith('substringBeforeLast(')) { const args = fn.match(/substringBeforeLast\(['"]([^'"]*)['"]/); if (args) { const idx = String(val).lastIndexOf(args[1]); val = idx >= 0 ? String(val).substring(0, idx) : val; } }
      else if (fn.startsWith('substringAfterLast(')) { const args = fn.match(/substringAfterLast\(['"]([^'"]*)['"]/); if (args) { const idx = String(val).lastIndexOf(args[1]); val = idx >= 0 ? String(val).substring(idx + args[1].length) : ''; } }
      else if (fn === 'getDelimitedField(1)') { val = String(val).split(',')[0] || ''; }
      else if (fn.startsWith('getDelimitedField(')) { const args = fn.match(/getDelimitedField\((\d+)/); if (args) { const parts2 = String(val).split(','); val = parts2[parseInt(args[1])-1] || ''; } }
      else if (fn.startsWith('jsonPath(')) { const args = fn.match(/jsonPath\(['"]([^'"]*)['"]/); if (args) { try { const jv = JSON.parse(val); const jp = args[1].replace(/^\$\./, '').split('.'); let r = jv; jp.forEach(k => { if (r) r = r[k]; }); val = r !== undefined ? String(r) : ''; } catch(e) { } } }
      else if (fn.startsWith('and(')) { const args = fn.match(/and\((.+?)\)/); val = String(val === 'true' && (args ? args[1] === 'true' : false)); }
      else if (fn.startsWith('or(')) { const args = fn.match(/or\((.+?)\)/); val = String(val === 'true' || (args ? args[1] === 'true' : false)); }
      else if (fn.startsWith('gt(')) { const args = fn.match(/gt\((\d+)/); if (args) val = String(parseFloat(val) > parseInt(args[1])); }
      else if (fn.startsWith('ge(')) { const args = fn.match(/ge\((\d+)/); if (args) val = String(parseFloat(val) >= parseInt(args[1])); }
      else if (fn.startsWith('lt(')) { const args = fn.match(/lt\((\d+)/); if (args) val = String(parseFloat(val) < parseInt(args[1])); }
      else if (fn.startsWith('le(')) { const args = fn.match(/le\((\d+)/); if (args) val = String(parseFloat(val) <= parseInt(args[1])); }
      else if (fn.startsWith('mod(')) { const args = fn.match(/mod\((\d+)/); if (args) val = String(parseInt(val) % parseInt(args[1])); }
      else if (fn.startsWith('math(')) {
        // FIX CRIT: Replaced eval() with safe math expression parser
        const args = fn.match(/math\(['"]([^'"]*)['"]/);
        if (args) {
          const mathExpr = args[1].replace(/\bvalue\b/g, String(val));
          const result = safeMathEval(mathExpr);
          if (result !== null) val = String(result);
        }
      }
      else if (fn === 'escapeJson()') { val = JSON.stringify(val).slice(1, -1); }
      else if (fn === 'unescapeJson()') { try { val = JSON.parse('"' + val + '"'); } catch(e) {} }
      else if (fn === 'escapeXml()') { val = String(val).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
      else if (fn === 'escapeCsv()') { val = val.includes(',') ? '"' + val.replace(/"/g, '""') + '"' : val; }
      else if (fn === 'urlEncode()') { val = encodeURIComponent(val); }
      else if (fn === 'urlDecode()') { val = decodeURIComponent(val); }
      else if (fn === 'base64Encode()') { val = btoa(val); }
      else if (fn === 'base64Decode()') { try { val = atob(val); } catch(e) {} }
      else if (fn.startsWith('ifElse(')) {
        const arg = fn.match(/ifElse\(["']([^"']*)["']\s*,\s*["']([^"']*)["']\)/);
        if (arg) val = (val === 'true') ? arg[1] : arg[2];
      }
    }
    return String(val);
  });
}
