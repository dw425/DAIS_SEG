"""Extended NEL function library — 50+ additional function translations.

Extends the core NEL function set from functions.py with comprehensive
coverage of NiFi Expression Language string, math, date, boolean,
attribute, and system functions, each mapped to PySpark SQL equivalents.
"""

from __future__ import annotations

import logging
import typing

from app.engines.parsers.nel.functions import (
    _py_escape,
    _regex_escape,
    apply_nel_function as _base_apply,
    extract_func_args,
    java_date_to_python_full,
    resolve_nel_arg,
    unquote_arg,
)


def apply_nel_function_extended(base: str, call: str, mode: str) -> str:
    """Apply an NEL function call, trying extended handlers first, then base."""
    name, args = extract_func_args(call)
    handler = EXTENDED_FUNCTIONS.get(name.lower())
    if handler:
        return handler(base, args, mode)
    # Fall through to base implementation
    return _base_apply(base, call, mode)


# ── Helpers ──

def _col_or_py(base, col_expr, py_expr, mode):
    return col_expr if mode == "col" else py_expr


# ═══════════════════════════════════════════════════════════
# String functions
# ═══════════════════════════════════════════════════════════

def _tostring(base, args, mode):
    return f'{base}.cast("string")' if mode == "col" else f"str({base})"

def _tonumber(base, args, mode):
    return f'{base}.cast("long")' if mode == "col" else f"int({base})"

def _substring_start(base, args, mode):
    start = args[0] if args else "0"
    end = args[1] if len(args) > 1 else None
    if mode == "col":
        length = str(int(end) - int(start)) if end else "9999"
        return f"substring({base}, {int(start) + 1}, {length})"
    return f"{base}[{start}:{end}]" if end else f"{base}[{start}:]"

def _indexof(base, args, mode):
    val = unquote_arg(args[0]) if args else ""
    if mode == "col":
        return f'(locate("{_py_escape(val)}", {base}) - 1)'
    return f'{base}.find("{_py_escape(val)}")'

def _lastindexof(base, args, mode):
    val = unquote_arg(args[0]) if args else ""
    if mode == "col":
        return f'(length({base}) - locate("{_py_escape(val)}", reverse({base})) - length(lit("{_py_escape(val)}")) + 1)'
    return f'{base}.rfind("{_py_escape(val)}")'

def _contains(base, args, mode):
    val = unquote_arg(args[0]) if args else ""
    return f'{base}.contains("{_py_escape(val)}")' if mode == "col" else f'"{_py_escape(val)}" in {base}'

def _startswith(base, args, mode):
    val = unquote_arg(args[0]) if args else ""
    return f'{base}.startswith("{_py_escape(val)}")'

def _endswith(base, args, mode):
    val = unquote_arg(args[0]) if args else ""
    return f'{base}.endswith("{_py_escape(val)}")'

def _matches(base, args, mode):
    pat = unquote_arg(args[0]) if args else ""
    return f'{base}.rlike("{_py_escape(pat)}")' if mode == "col" else f're.match(r"{_py_escape(pat)}", {base})'

def _find(base, args, mode):
    pat = unquote_arg(args[0]) if args else ""
    if mode == "col":
        return f'regexp_extract({base}, "{_py_escape(pat)}", 0)'
    return f're.search(r"{_py_escape(pat)}", {base}).group(0) if re.search(r"{_py_escape(pat)}", {base}) else ""'

def _replacefirst(base, args, mode):
    pat = unquote_arg(args[0]) if args else ""
    rpl = unquote_arg(args[1]) if len(args) > 1 else ""
    if mode == "col":
        return f'regexp_replace({base}, "{_py_escape(pat)}", "{_py_escape(rpl)}", 1)'
    return f're.sub(r"{_py_escape(pat)}", "{_py_escape(rpl)}", {base}, count=1)'

def _replacelast(base, args, mode):
    pat = unquote_arg(args[0]) if args else ""
    rpl = unquote_arg(args[1]) if len(args) > 1 else ""
    if mode == "col":
        # No direct PySpark equivalent; reverse + replace first + reverse
        return f'reverse(regexp_replace(reverse({base}), "{_py_escape(pat)}", "{_py_escape(rpl)}", 1))'
    return f'"{_py_escape(rpl)}".join({base}.rsplit("{_py_escape(pat)}", 1))'

def _prepend(base, args, mode):
    val = unquote_arg(args[0]) if args else ""
    return f'concat(lit("{_py_escape(val)}"), {base})' if mode == "col" else f'"{_py_escape(val)}" + {base}'

def _append(base, args, mode):
    val = unquote_arg(args[0]) if args else ""
    return f'concat({base}, lit("{_py_escape(val)}"))' if mode == "col" else f'{base} + "{_py_escape(val)}"'

def _padleft(base, args, mode):
    width = args[0] if args else "0"
    char = unquote_arg(args[1]) if len(args) > 1 else " "
    if mode == "col":
        return f'lpad({base}, {width}, "{_py_escape(char)}")'
    return f'{base}.rjust({width}, "{_py_escape(char)}")'

def _padright(base, args, mode):
    width = args[0] if args else "0"
    char = unquote_arg(args[1]) if len(args) > 1 else " "
    if mode == "col":
        return f'rpad({base}, {width}, "{_py_escape(char)}")'
    return f'{base}.ljust({width}, "{_py_escape(char)}")'

def _trim(base, args, mode):
    return f"trim({base})" if mode == "col" else f"{base}.strip()"

def _toupper(base, args, mode):
    return f"upper({base})" if mode == "col" else f"{base}.upper()"

def _tolower(base, args, mode):
    return f"lower({base})" if mode == "col" else f"{base}.lower()"

def _urlencode(base, args, mode):
    if mode == "col":
        return f"url_encode({base})"
    return f"urllib.parse.quote({base})"

def _urldecode(base, args, mode):
    if mode == "col":
        return f"url_decode({base})"
    return f"urllib.parse.unquote({base})"

def _escapejson(base, args, mode):
    if mode == "col":
        return f"regexp_replace({base}, '\"', '\\\\\"')"
    return f"json.dumps({base})[1:-1]"

def _unescapejson(base, args, mode):
    if mode == "col":
        return f"regexp_replace({base}, '\\\\\\\\\"', '\"')"
    return f'{base}.replace(\'\\\\"\', \'"\')' 

def _escapexml(base, args, mode):
    if mode == "col":
        return (
            f"regexp_replace(regexp_replace(regexp_replace("
            f"regexp_replace(regexp_replace({base}, '&', '&amp;'), '<', '&lt;'), "
            f"'>', '&gt;'), '\\'', '&apos;'), '\"', '&quot;')"
        )
    return f"html.escape({base}, quote=True)"

def _unescapexml(base, args, mode):
    if mode == "col":
        return (
            f"regexp_replace(regexp_replace(regexp_replace("
            f"regexp_replace(regexp_replace({base}, '&amp;', '&'), '&lt;', '<'), "
            f"'&gt;', '>'), '&apos;', '\\''), '&quot;', '\"')"
        )
    return f"html.unescape({base})"

def _escapehtml3(base, args, mode):
    return _escapexml(base, args, mode)

def _escapehtml4(base, args, mode):
    return _escapexml(base, args, mode)

def _escapecsv(base, args, mode):
    if mode == "col":
        return f'when({base}.contains(","), concat(lit(\'"\'), {base}, lit(\'"\'))).otherwise({base})'
    return f'f\'\"{{{{{{base}}}}}}"\' if "," in {base} else {base}'


# ═══════════════════════════════════════════════════════════
# Math functions
# ═══════════════════════════════════════════════════════════

def _plus(base, args, mode):
    v = args[0] if args else "0"
    return f"({base} + lit({v}))" if mode == "col" else f"({base} + {v})"

def _minus(base, args, mode):
    v = args[0] if args else "0"
    return f"({base} - lit({v}))" if mode == "col" else f"({base} - {v})"

def _multiply(base, args, mode):
    v = args[0] if args else "1"
    return f"({base} * lit({v}))" if mode == "col" else f"({base} * {v})"

def _divide(base, args, mode):
    v = args[0] if args else "1"
    return f"({base} / lit({v}))" if mode == "col" else f"({base} / {v})"

def _mod(base, args, mode):
    v = args[0] if args else "1"
    return f"({base} % lit({v}))" if mode == "col" else f"({base} % {v})"

def _toradians(base, args, mode):
    if mode == "col":
        return f"radians({base})"
    return f"math.radians({base})"

def _todegrees(base, args, mode):
    if mode == "col":
        return f"degrees({base})"
    return f"math.degrees({base})"

def _math_random(base, args, mode):
    return "rand()" if mode == "col" else "random.random()"

def _math_abs(base, args, mode):
    v = args[0] if args else base
    return f"abs({v})" if mode == "col" else f"abs({v})"

def _math_ceil(base, args, mode):
    v = args[0] if args else base
    return f"ceil({v})" if mode == "col" else f"math.ceil({v})"

def _math_floor(base, args, mode):
    v = args[0] if args else base
    return f"floor({v})" if mode == "col" else f"math.floor({v})"

def _math_round(base, args, mode):
    v = args[0] if args else base
    return f"round({v})" if mode == "col" else f"round({v})"

def _math_sqrt(base, args, mode):
    v = args[0] if args else base
    return f"sqrt({v})" if mode == "col" else f"math.sqrt({v})"

def _math_log(base, args, mode):
    v = args[0] if args else base
    return f"log({v})" if mode == "col" else f"math.log({v})"

def _math_log10(base, args, mode):
    v = args[0] if args else base
    return f"log10({v})" if mode == "col" else f"math.log10({v})"

def _math_pow(base, args, mode):
    b = args[0] if args else base
    exp = args[1] if len(args) > 1 else "2"
    return f"pow({b}, {exp})" if mode == "col" else f"math.pow({b}, {exp})"

def _math_min(base, args, mode):
    a = args[0] if args else base
    b = args[1] if len(args) > 1 else "0"
    return f"least({a}, {b})" if mode == "col" else f"min({a}, {b})"

def _math_max(base, args, mode):
    a = args[0] if args else base
    b = args[1] if len(args) > 1 else "0"
    return f"greatest({a}, {b})" if mode == "col" else f"max({a}, {b})"


# ═══════════════════════════════════════════════════════════
# Date functions
# ═══════════════════════════════════════════════════════════

def _now(base, args, mode):
    return "current_timestamp()" if mode == "col" else "datetime.now()"

def _format(base, args, mode):
    fmt = unquote_arg(args[0]) if args else "yyyy-MM-dd"
    if mode == "col":
        return f'date_format({base}, "{fmt}")'
    py_fmt = java_date_to_python_full(fmt)
    return f'{base}.strftime("{py_fmt}")'

def _todate(base, args, mode):
    fmt = unquote_arg(args[0]) if args else "yyyy-MM-dd"
    if mode == "col":
        return f'to_timestamp({base}, "{fmt}")'
    py_fmt = java_date_to_python_full(fmt)
    return f'datetime.strptime({base}, "{py_fmt}")'

def _tonumber_date(base, args, mode):
    return f'{base}.cast("long")' if mode == "col" else f"int({base}.timestamp())"

def _getyear(base, args, mode):
    return f"year({base})" if mode == "col" else f"{base}.year"

def _getmonth(base, args, mode):
    return f"month({base})" if mode == "col" else f"{base}.month"

def _getdayofmonth(base, args, mode):
    return f"dayofmonth({base})" if mode == "col" else f"{base}.day"

def _gethour(base, args, mode):
    return f"hour({base})" if mode == "col" else f"{base}.hour"

def _getminute(base, args, mode):
    return f"minute({base})" if mode == "col" else f"{base}.minute"

def _getsecond(base, args, mode):
    return f"second({base})" if mode == "col" else f"{base}.second"


# ═══════════════════════════════════════════════════════════
# Boolean / Logic functions
# ═══════════════════════════════════════════════════════════

def _isnull(base, args, mode):
    return f"{base}.isNull()" if mode == "col" else f"({base} is None)"

def _notnull(base, args, mode):
    return f"{base}.isNotNull()" if mode == "col" else f"({base} is not None)"

def _isempty(base, args, mode):
    return f'({base}.isNull() | ({base} == lit("")))' if mode == "col" else f"(not {base})"

def _isblank(base, args, mode):
    if mode == "col":
        return f'({base}.isNull() | (trim({base}) == lit("")))'
    return f"(not {base} or not {base}.strip())"

def _equals(base, args, mode):
    val = unquote_arg(args[0]) if args else ""
    return f'({base} == lit("{_py_escape(val)}"))' if mode == "col" else f'({base} == "{_py_escape(val)}")'

def _equalsignorecase(base, args, mode):
    val = unquote_arg(args[0]) if args else ""
    val_low = _py_escape(val.lower())
    return f'(lower({base}) == lit("{val_low}"))' if mode == "col" else f'({base}.lower() == "{val_low}")'

def _gt(base, args, mode):
    v = args[0] if args else "0"
    return f"({base} > lit({v}))" if mode == "col" else f"({base} > {v})"

def _ge(base, args, mode):
    v = args[0] if args else "0"
    return f"({base} >= lit({v}))" if mode == "col" else f"({base} >= {v})"

def _lt(base, args, mode):
    v = args[0] if args else "0"
    return f"({base} < lit({v}))" if mode == "col" else f"({base} < {v})"

def _le(base, args, mode):
    v = args[0] if args else "0"
    return f"({base} <= lit({v}))" if mode == "col" else f"({base} <= {v})"

def _and(base, args, mode):
    other = resolve_nel_arg(args[0], mode) if args else "True"
    return f"({base} & {other})" if mode == "col" else f"({base} and {other})"

def _or(base, args, mode):
    other = resolve_nel_arg(args[0], mode) if args else "False"
    return f"({base} | {other})" if mode == "col" else f"({base} or {other})"

def _not(base, args, mode):
    return f"(~{base})" if mode == "col" else f"(not {base})"

def _in(base, args, mode):
    vals = ", ".join(a.strip() for a in args)
    if mode == "col":
        return f"{base}.isin([{vals}])"
    return f"({base} in [{vals}])"

def _ifelse(base, args, mode):
    tv = resolve_nel_arg(args[0], mode) if args else "lit(True)"
    fv = resolve_nel_arg(args[1], mode) if len(args) > 1 else "lit(None)"
    return f"when({base}, {tv}).otherwise({fv})" if mode == "col" else f"({tv} if {base} else {fv})"


# ═══════════════════════════════════════════════════════════
# Attribute / multi-value functions
# ═══════════════════════════════════════════════════════════

def _allattributes(base, args, mode):
    attrs = ", ".join(f'col("{unquote_arg(a)}")' for a in args) if args else base
    return f"array({attrs})" if mode == "col" else f"[{', '.join(f'_attrs.get(\"{unquote_arg(a)}\", \"\")' for a in args)}]"

def _anyattribute(base, args, mode):
    if mode == "col":
        attrs = [f'col("{unquote_arg(a)}")' for a in args] if args else [base]
        return " | ".join(f"({a}.isNotNull())" for a in attrs) if len(attrs) > 1 else f"array({attrs[0]})"
    return f"any(_attrs.get(\"{unquote_arg(a)}\", \"\") for a in [{', '.join(repr(unquote_arg(a)) for a in args)}])" if args else base

def _allmatchingattributes(base, args, mode):
    _logger = logging.getLogger(__name__)
    pat = unquote_arg(args[0]) if args else ".*"
    _logger.warning("allMatchingAttributes('%s') requires runtime column resolution — emitting placeholder", pat)
    if mode == "col":
        return f'array()  # USER ACTION: allMatchingAttributes("{pat}") — resolve matching columns at runtime'
    return f'[v for k, v in _attrs.items() if re.match(r"{_py_escape(pat)}", k)]'

def _anymatchingattributes(base, args, mode):
    _logger = logging.getLogger(__name__)
    pat = unquote_arg(args[0]) if args else ".*"
    _logger.warning("anyMatchingAttributes('%s') requires runtime column resolution — emitting placeholder", pat)
    if mode == "col":
        return f'lit(True)  # USER ACTION: anyMatchingAttributes("{pat}") — resolve with OR over matching columns at runtime'
    return f'any(v for k, v in _attrs.items() if re.match(r"{_py_escape(pat)}", k))'

def _alldelineatedvalues(base, args, mode):
    delim = unquote_arg(args[0]) if args else ","
    if mode == "col":
        return f'split({base}, "{_py_escape(delim)}")'
    return f'{base}.split("{_py_escape(delim)}")'

def _anydelineatedvalue(base, args, mode):
    return _alldelineatedvalues(base, args, mode)

def _join(base, args, mode):
    delim = unquote_arg(args[0]) if args else ","
    if mode == "col":
        return f'array_join({base}, "{_py_escape(delim)}")'
    return f'"{_py_escape(delim)}".join({base})'

def _count(base, args, mode):
    return f"size({base})" if mode == "col" else f"len({base})"


# ═══════════════════════════════════════════════════════════
# System functions
# ═══════════════════════════════════════════════════════════

def _uuid(base, args, mode):
    return "expr('uuid()')" if mode == "col" else "str(uuid.uuid4())"

def _nextint(base, args, mode):
    return "monotonically_increasing_id()" if mode == "col" else "random.randint(0, 2**31)"

def _hostname(base, args, mode):
    return "lit(spark.conf.get('spark.driver.host', 'unknown'))" if mode == "col" else "socket.gethostname()"

def _ip(base, args, mode):
    return "lit(spark.conf.get('spark.driver.host', '0.0.0.0'))" if mode == "col" else "socket.gethostbyname(socket.gethostname())"

def _thread(base, args, mode):
    return "lit('spark-worker')" if mode == "col" else "threading.current_thread().name"


# ═══════════════════════════════════════════════════════════
# Function registry
# ═══════════════════════════════════════════════════════════

EXTENDED_FUNCTIONS: dict[str, typing.Callable] = {
    # String
    "tostring": _tostring,
    "tonumber": _tonumber,
    "substring": _substring_start,
    "indexof": _indexof,
    "lastindexof": _lastindexof,
    "contains": _contains,
    "startswith": _startswith,
    "endswith": _endswith,
    "matches": _matches,
    "find": _find,
    "replacefirst": _replacefirst,
    "replacelast": _replacelast,
    "prepend": _prepend,
    "append": _append,
    "padleft": _padleft,
    "padright": _padright,
    "trim": _trim,
    "toupper": _toupper,
    "tolower": _tolower,
    "urlencode": _urlencode,
    "urldecode": _urldecode,
    "escapejson": _escapejson,
    "unescapejson": _unescapejson,
    "escapexml": _escapexml,
    "unescapexml": _unescapexml,
    "escapehtml3": _escapehtml3,
    "escapehtml4": _escapehtml4,
    "escapecsv": _escapecsv,
    # Math
    "plus": _plus,
    "minus": _minus,
    "multiply": _multiply,
    "divide": _divide,
    "mod": _mod,
    "toradians": _toradians,
    "todegrees": _todegrees,
    "math:random": _math_random,
    "math:abs": _math_abs,
    "math:ceil": _math_ceil,
    "math:floor": _math_floor,
    "math:round": _math_round,
    "math:sqrt": _math_sqrt,
    "math:log": _math_log,
    "math:log10": _math_log10,
    "math:pow": _math_pow,
    "math:min": _math_min,
    "math:max": _math_max,
    # Date
    "now": _now,
    "format": _format,
    "todate": _todate,
    "getyear": _getyear,
    "getmonth": _getmonth,
    "getdayofmonth": _getdayofmonth,
    "gethour": _gethour,
    "getminute": _getminute,
    "getsecond": _getsecond,
    # Boolean
    "isnull": _isnull,
    "notnull": _notnull,
    "isempty": _isempty,
    "isblank": _isblank,
    "equals": _equals,
    "equalsignorecase": _equalsignorecase,
    "gt": _gt,
    "ge": _ge,
    "lt": _lt,
    "le": _le,
    "and": _and,
    "or": _or,
    "not": _not,
    "in": _in,
    "ifelse": _ifelse,
    # Attribute
    "allattributes": _allattributes,
    "anyattribute": _anyattribute,
    "allmatchingattributes": _allmatchingattributes,
    "anymatchingattributes": _anymatchingattributes,
    "alldelineatedvalues": _alldelineatedvalues,
    "anydelineatedvalue": _anydelineatedvalue,
    "join": _join,
    "count": _count,
    # System
    "uuid": _uuid,
    "nextint": _nextint,
    "hostname": _hostname,
    "ip": _ip,
    "thread": _thread,
}
