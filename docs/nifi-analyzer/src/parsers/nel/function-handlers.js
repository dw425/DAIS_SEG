// ================================================================
// nel/function-handlers.js — NEL function -> PySpark/Python translation
// Extracted from monolith lines 3247-3420
// GAP FIX: Grok pattern -> regex translation
// ================================================================

import { extractNELFuncArgs, unquoteArg, resolveNELArg } from './arg-resolver.js';
import { javaDateToPython } from './java-date-format.js';

// ================================================================
// GAP FIX: Grok pattern -> regexp_extract translations
// ================================================================
const GROK_PATTERNS = {
  'COMBINEDAPACHELOG': '(?<client>\\S+) \\S+ (?<user>\\S+) \\[(?<timestamp>[^\\]]+)\\] "(?<method>\\S+) (?<request>[^"]+) HTTP/\\S+" (?<status>\\d+) (?<size>\\S+) "(?<referrer>[^"]*)" "(?<agent>[^"]*)"',
  'COMMONAPACHELOG':   '(?<client>\\S+) \\S+ (?<user>\\S+) \\[(?<timestamp>[^\\]]+)\\] "(?<method>\\S+) (?<request>[^"]+) HTTP/\\S+" (?<status>\\d+) (?<size>\\S+)',
  'SYSLOGLINE':        '(?<timestamp>\\w{3}\\s+\\d{1,2} \\d{2}:\\d{2}:\\d{2}) (?<host>\\S+) (?<program>[^\\[]+)(?:\\[(?<pid>\\d+)\\])?: (?<message>.*)',
  'TIMESTAMP_ISO8601': '\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?(?:Z|[+-]\\d{2}:?\\d{2})?',
  'IP':                '\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}',
  'NUMBER':            '(?:-?\\d+(?:\\.\\d+)?)',
  'WORD':              '\\w+',
  'DATA':              '.*?',
  'GREEDYDATA':        '.*',
};

/**
 * Resolve a Grok pattern name to its regex equivalent.
 * @param {string} patternName - Grok pattern name (e.g., 'COMBINEDAPACHELOG')
 * @returns {string} Regex pattern string
 */
function resolveGrokPattern(patternName) {
  return GROK_PATTERNS[patternName] || GROK_PATTERNS[patternName.toUpperCase()] || patternName;
}

/**
 * Apply a single NEL function call to a base expression.
 * Translates NiFi Expression Language functions to PySpark column
 * operations or Python string operations.
 *
 * @param {string} base - The base expression code
 * @param {string} call - The function call string (e.g., "toUpper()")
 * @param {string} mode - 'col' for PySpark, 'python' for string ops
 * @returns {string} Transformed expression code
 */
export function applyNELFunction(base, call, mode) {
  var f = extractNELFuncArgs(call);
  var name = f.name.toLowerCase();
  var args = f.args;

  // -- String functions --
  if (name === 'toupper') return mode === 'col' ? 'upper(' + base + ')' : base + '.upper()';
  if (name === 'tolower') return mode === 'col' ? 'lower(' + base + ')' : base + '.lower()';
  if (name === 'trim') return mode === 'col' ? 'trim(' + base + ')' : base + '.strip()';
  if (name === 'length') return mode === 'col' ? 'length(' + base + ')' : 'len(' + base + ')';
  if (name === 'substring') {
    var start = args[0] || '0'; var len = args[1];
    if (mode === 'col') return len ? 'substring(' + base + ', ' + (parseInt(start)+1) + ', ' + len + ')' : 'substring(' + base + ', ' + (parseInt(start)+1) + ', 9999)';
    return len ? base + '[' + start + ':' + (parseInt(start)+parseInt(len)) + ']' : base + '[' + start + ':]';
  }
  if (name === 'replace') {
    var search = unquoteArg(args[0] || ''); var repl = unquoteArg(args[1] || '');
    return mode === 'col' ? 'regexp_replace(' + base + ', "' + search + '", "' + repl + '")' : base + '.replace("' + search + '", "' + repl + '")';
  }
  if (name === 'replaceall') {
    var pat = unquoteArg(args[0] || ''); var rpl = unquoteArg(args[1] || '');
    return mode === 'col' ? 'regexp_replace(' + base + ', "' + pat + '", "' + rpl + '")' : 're.sub(r"' + pat + '", "' + rpl + '", ' + base + ')';
  }
  if (name === 'replacefirst') {
    var pat2 = unquoteArg(args[0] || ''); var rpl2 = unquoteArg(args[1] || '');
    return mode === 'col' ? 'regexp_replace(' + base + ', "(' + pat2 + ')", "' + rpl2 + '")' : 're.sub(r"' + pat2 + '", "' + rpl2 + '", ' + base + ', count=1)';
  }
  if (name === 'startswith') {
    var sw = unquoteArg(args[0] || '');
    return mode === 'col' ? base + '.startswith("' + sw + '")' : base + '.startswith("' + sw + '")';
  }
  if (name === 'endswith') {
    var ew = unquoteArg(args[0] || '');
    return mode === 'col' ? base + '.endswith("' + ew + '")' : base + '.endswith("' + ew + '")';
  }
  if (name === 'contains') {
    var cv = unquoteArg(args[0] || '');
    return mode === 'col' ? base + '.contains("' + cv + '")' : '"' + cv + '" in ' + base;
  }
  if (name === 'matches') {
    var mp = unquoteArg(args[0] || '');
    return mode === 'col' ? base + '.rlike("' + mp + '")' : 're.match(r"' + mp + '", ' + base + ')';
  }
  if (name === 'find') {
    var fp = unquoteArg(args[0] || '');
    return mode === 'col' ? 'regexp_extract(' + base + ', "' + fp + '", 0)' : 're.search(r"' + fp + '", ' + base + ').group(0)';
  }
  if (name === 'split') {
    var sp = unquoteArg(args[0] || ',');
    return mode === 'col' ? 'split(' + base + ', "' + sp + '")' : base + '.split("' + sp + '")';
  }
  if (name === 'substringbefore') {
    var sb = unquoteArg(args[0] || '');
    return mode === 'col' ? 'substring_index(' + base + ', "' + sb + '", 1)' : base + '.split("' + sb + '")[0]';
  }
  if (name === 'substringafter') {
    var sa = unquoteArg(args[0] || '');
    return mode === 'col' ? 'substring_index(' + base + ', "' + sa + '", -1)' : '"' + sa + '".join(' + base + '.split("' + sa + '")[1:])';
  }
  if (name === 'padleft' || name === 'leftpad') {
    var plLen = args[0] || '10'; var plChar = unquoteArg(args[1] || ' ');
    return mode === 'col' ? 'lpad(' + base + ', ' + plLen + ', "' + plChar + '")' : base + '.rjust(' + plLen + ', "' + plChar + '")';
  }
  if (name === 'padright' || name === 'rightpad') {
    var prLen = args[0] || '10'; var prChar = unquoteArg(args[1] || ' ');
    return mode === 'col' ? 'rpad(' + base + ', ' + prLen + ', "' + prChar + '")' : base + '.ljust(' + prLen + ', "' + prChar + '")';
  }
  if (name === 'indexof') {
    var io = unquoteArg(args[0] || '');
    return mode === 'col' ? 'locate("' + io + '", ' + base + ')' : base + '.find("' + io + '")';
  }
  if (name === 'getdelimitedfield') {
    var idx = args[0] || '1'; var delim = unquoteArg(args[1] || ',');
    return mode === 'col' ? 'split(' + base + ', "' + delim + '")[' + (parseInt(idx)-1) + ']' : base + '.split("' + delim + '")[' + (parseInt(idx)-1) + ']';
  }
  if (name === 'append') {
    var av = unquoteArg(args[0] || '');
    return mode === 'col' ? 'concat(' + base + ', lit("' + av + '"))' : base + ' + "' + av + '"';
  }
  if (name === 'prepend') {
    var pv = unquoteArg(args[0] || '');
    return mode === 'col' ? 'concat(lit("' + pv + '"), ' + base + ')' : '"' + pv + '" + ' + base;
  }
  if (name === 'equals') {
    var eq = unquoteArg(args[0] || '');
    return mode === 'col' ? '(' + base + ' == lit("' + eq + '"))' : '(' + base + ' == "' + eq + '")';
  }
  if (name === 'equalsignorecase') {
    var eic = unquoteArg(args[0] || '');
    return mode === 'col' ? '(lower(' + base + ') == lit("' + eic.toLowerCase() + '"))' : '(' + base + '.lower() == "' + eic.toLowerCase() + '")';
  }

  // -- Math functions --
  if (name === 'plus') {
    var pn = args[0] || '0';
    return mode === 'col' ? '(' + base + ' + lit(' + pn + '))' : '(' + base + ' + ' + pn + ')';
  }
  if (name === 'minus') {
    var mn = args[0] || '0';
    return mode === 'col' ? '(' + base + ' - lit(' + mn + '))' : '(' + base + ' - ' + mn + ')';
  }
  if (name === 'multiply') {
    var mul = args[0] || '1';
    return mode === 'col' ? '(' + base + ' * lit(' + mul + '))' : '(' + base + ' * ' + mul + ')';
  }
  if (name === 'divide') {
    var div = args[0] || '1';
    return mode === 'col' ? '(' + base + ' / lit(' + div + '))' : '(' + base + ' / ' + div + ')';
  }
  if (name === 'mod') {
    var modv = args[0] || '1';
    return mode === 'col' ? '(' + base + ' % lit(' + modv + '))' : '(' + base + ' % ' + modv + ')';
  }
  if (name === 'gt') {
    var gtv = args[0] || '0';
    return mode === 'col' ? '(' + base + ' > lit(' + gtv + '))' : '(' + base + ' > ' + gtv + ')';
  }
  if (name === 'lt') {
    var ltv = args[0] || '0';
    return mode === 'col' ? '(' + base + ' < lit(' + ltv + '))' : '(' + base + ' < ' + ltv + ')';
  }
  if (name === 'ge') {
    var gev = args[0] || '0';
    return mode === 'col' ? '(' + base + ' >= lit(' + gev + '))' : '(' + base + ' >= ' + gev + ')';
  }
  if (name === 'le') {
    var lev = args[0] || '0';
    return mode === 'col' ? '(' + base + ' <= lit(' + lev + '))' : '(' + base + ' <= ' + lev + ')';
  }

  // -- Logic functions --
  if (name === 'isempty') return mode === 'col' ? '(' + base + '.isNull() | (' + base + ' == lit("")))' : '(not ' + base + ')';
  if (name === 'isnull') return mode === 'col' ? base + '.isNull()' : '(' + base + ' is None)';
  if (name === 'notnull') return mode === 'col' ? base + '.isNotNull()' : '(' + base + ' is not None)';
  if (name === 'not') return mode === 'col' ? '(~' + base + ')' : '(not ' + base + ')';
  if (name === 'and') {
    var andArg = args[0] ? resolveNELArg(args[0], mode) : 'lit(True)';
    return mode === 'col' ? '(' + base + ' & ' + andArg + ')' : '(' + base + ' and ' + andArg + ')';
  }
  if (name === 'or') {
    var orArg = args[0] ? resolveNELArg(args[0], mode) : 'lit(False)';
    return mode === 'col' ? '(' + base + ' | ' + orArg + ')' : '(' + base + ' or ' + orArg + ')';
  }
  if (name === 'ifelse') {
    var trueVal = args[0] ? resolveNELArg(args[0], mode) : 'lit(True)';
    var falseVal = args[1] ? resolveNELArg(args[1], mode) : 'lit(None)';
    return mode === 'col' ? 'when(' + base + ', ' + trueVal + ').otherwise(' + falseVal + ')' :
      '(' + trueVal + ' if ' + base + ' else ' + falseVal + ')';
  }

  // -- Date functions --
  if (name === 'format') {
    var fmt = unquoteArg(args[0] || 'yyyy-MM-dd');
    return mode === 'col' ? 'date_format(' + base + ', "' + fmt + '")' : base + '.strftime("' + javaDateToPython(fmt) + '")';
  }
  if (name === 'todate') {
    var dfmt = unquoteArg(args[0] || 'yyyy-MM-dd');
    return mode === 'col' ? 'to_timestamp(' + base + ', "' + dfmt + '")' : 'datetime.strptime(' + base + ', "' + javaDateToPython(dfmt) + '")';
  }
  if (name === 'tonumber') return mode === 'col' ? base + '.cast("long")' : 'int(' + base + ')';
  if (name === 'tostring') return mode === 'col' ? base + '.cast("string")' : 'str(' + base + ')';

  // -- GAP FIX: Grok pattern matching --
  if (name === 'grok') {
    var grokPattern = unquoteArg(args[0] || '');
    var resolvedRegex = resolveGrokPattern(grokPattern);
    return mode === 'col'
      ? 'regexp_extract(' + base + ', "' + resolvedRegex + '", 0)'
      : 're.search(r"' + resolvedRegex + '", ' + base + ')';
  }


  // -- Encoding/escaping functions --
  if (name === 'evaluateelstring') {
    // evaluateELString -> f-string / .format()
    return mode === 'col' ? base : base + '  # evaluateELString: use f-string or .format()';
  }
  if (name === 'unescapejson') {
    var schema = args[0] ? unquoteArg(args[0]) : 'STRING';
    return mode === 'col' ? 'from_json(' + base + ', "' + schema + '")' : 'json.loads(' + base + ')';
  }
  if (name === 'unescapexml') {
    var xpath = args[0] ? unquoteArg(args[0]) : '/';
    return mode === 'col' ? 'xpath_string(' + base + ', "' + xpath + '")' : '__import__("html").unescape(' + base + ')';
  }
  if (name === 'unescapecsv') {
    return mode === 'col' ? 'split(' + base + ', ",")' : base + '.split(",")';
  }
  if (name === 'escapejson') {
    return mode === 'col' ? 'to_json(' + base + ')' : 'json.dumps(' + base + ')';
  }
  if (name === 'escapecsv') {
    return mode === 'col' ? 'concat_ws(",", ' + base + ')' : '",".join(' + base + ')';
  }

  // -- Conditional functions --
  if (name === 'in') {
    var inList = args.map(function(a) { return unquoteArg(a); });
    if (mode === 'col') {
      return base + '.isin(' + inList.map(function(v) { return '"' + v + '"'; }).join(', ') + ')';
    }
    return base + ' in [' + inList.map(function(v) { return '"' + v + '"'; }).join(', ') + ']';
  }

  // -- Math namespace functions --
  if (name === 'math:tonumber' || name === 'tonumber') return mode === 'col' ? base + '.cast("double")' : 'float(' + base + ')';
  if (name === 'math:floor') return mode === 'col' ? 'floor(' + base + ')' : 'import math; math.floor(' + base + ')';
  if (name === 'math:ceil' || name === 'math:ceiling') return mode === 'col' ? 'ceil(' + base + ')' : 'import math; math.ceil(' + base + ')';
  if (name === 'math:abs') return mode === 'col' ? 'abs(' + base + ')' : 'abs(' + base + ')';
  if (name === 'math:mod') {
    var mathModV = args[0] || '1';
    return mode === 'col' ? '(' + base + ' % lit(' + mathModV + '))' : '(' + base + ' % ' + mathModV + ')';
  }
  if (name === 'math:multiply') {
    var mathMulV = args[0] || '1';
    return mode === 'col' ? '(' + base + ' * lit(' + mathMulV + '))' : '(' + base + ' * ' + mathMulV + ')';
  }
  if (name === 'math:max') {
    var maxArg = args[0] ? resolveNELArg(args[0], mode) : '0';
    return mode === 'col' ? 'greatest(' + base + ', ' + maxArg + ')' : 'max(' + base + ', ' + maxArg + ')';
  }
  if (name === 'math:min') {
    var minArg = args[0] ? resolveNELArg(args[0], mode) : '0';
    return mode === 'col' ? 'least(' + base + ', ' + minArg + ')' : 'min(' + base + ', ' + minArg + ')';
  }

  // -- Unknown function -> comment --
  return base + '  /* NEL: ' + call + ' */';
}
