"""NEL function handlers — translate NEL functions to PySpark/Python.

Ported from nel/function-handlers.js, nel/arg-resolver.js, nel/java-date-format.js,
and nel/variable-context.js.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

# ----------------------------------------------------------------
# Variable context classification (from variable-context.js)
# ----------------------------------------------------------------
_SECRET_RE = re.compile(r"password|token|secret|credential|api[_.]?key|private[_.]?key|passphrase", re.IGNORECASE)
_CONFIG_RE = re.compile(r"^(s3|kafka|jdbc|nifi|aws|azure|gcp|http|ftp|smtp|ssl|sasl)\.", re.IGNORECASE)
_CONFIG_SUFFIX_RE = re.compile(r"\.url$|\.host$|\.port$|\.path$|\.bucket$|\.region$", re.IGNORECASE)


def resolve_variable_context(var_name: str) -> dict:
    """Classify a NEL variable into secret / config / column."""
    lower = var_name.lower()
    if _SECRET_RE.search(lower):
        scope = (
            "kafka"
            if "kafka" in lower
            else "jdbc"
            if "jdbc" in lower
            else "aws"
            if ("s3" in lower or "aws" in lower)
            else "azure"
            if "azure" in lower
            else "es"
            if "es" in lower
            else "app"
        )
        return {"type": "secret", "code": f'dbutils.secrets.get(scope="{scope}", key="{var_name}")'}
    if _CONFIG_RE.match(var_name) or _CONFIG_SUFFIX_RE.search(lower):
        return {"type": "config", "code": f'dbutils.widgets.get("{var_name}")'}
    return {"type": "column", "code": f'col("{var_name}")'}


# ----------------------------------------------------------------
# Java date format -> Python strftime (from java-date-format.js)
# ----------------------------------------------------------------
def java_date_to_python(fmt: str) -> str:
    """Convert Java SimpleDateFormat to Python strftime."""
    return (
        fmt.replace("yyyy", "%Y")
        .replace("MM", "%m")
        .replace("dd", "%d")
        .replace("HH", "%H")
        .replace("mm", "%M")
        .replace("SSS", "%f")
        .replace("ss", "%S")
        .replace("EEEE", "%A")
    )


# Apply E{1-3} -> %a after EEEE to avoid double replacement
_E_SHORT_RE = re.compile(r"E{1,3}")


def java_date_to_python_full(fmt: str) -> str:
    s = java_date_to_python(fmt)
    s = _E_SHORT_RE.sub("%a", s)
    return s.replace("Z", "%z")


# ----------------------------------------------------------------
# Escape helpers
# ----------------------------------------------------------------
def _py_escape(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")


def _regex_escape(s: str) -> str:
    return re.sub(r"[.*+?^${}()|[\]\\]", r"\\\g<0>", s)


# ----------------------------------------------------------------
# Arg extraction (from arg-resolver.js)
# ----------------------------------------------------------------
def extract_func_args(call: str) -> tuple[str, list[str]]:
    """Extract function name and arguments from 'funcName(arg1, arg2)'."""
    m = re.match(r"^([\w:]+)\((.*)\)$", call, re.DOTALL)
    if not m:
        return call, []
    name = m.group(1)
    raw_args = m.group(2).strip()
    if not raw_args:
        return name, []

    args: list[str] = []
    current = ""
    depth = 0
    in_str = False
    str_char = ""
    for ch in raw_args:
        if in_str:
            current += ch
            if ch == str_char:
                in_str = False
        elif ch in ("'", '"'):
            in_str = True
            str_char = ch
            current += ch
        elif ch == "(":
            depth += 1
            current += ch
        elif ch == ")":
            depth -= 1
            current += ch
        elif ch == "," and depth == 0:
            args.append(current.strip())
            current = ""
        else:
            current += ch
    if current.strip():
        args.append(current.strip())
    return name, args


def unquote_arg(a: str) -> str:
    """Remove surrounding quotes."""
    if len(a) >= 2 and ((a[0] == "'" and a[-1] == "'") or (a[0] == '"' and a[-1] == '"')):
        return a[1:-1]
    return a


def resolve_nel_arg(arg: str, mode: str) -> str:
    """Resolve a single NEL argument to PySpark or Python code."""
    from app.engines.parsers.nel.parser import parse_nel_expression

    arg = arg.strip()
    if re.match(r"^'([^']*)'$", arg):
        val = _py_escape(arg[1:-1])
        return f'lit("{val}")' if mode == "col" else f'"{val}"'
    if re.match(r"^\d+$", arg):
        return f"lit({arg})" if mode == "col" else arg
    if re.match(r"^\d+\.\d+$", arg):
        return f"lit({arg})" if mode == "col" else arg
    if ":" in arg:
        return parse_nel_expression(arg, mode)
    ctx = resolve_variable_context(arg)
    if mode == "col":
        return f'col("{arg}")' if ctx["type"] == "column" else ctx["code"]
    return f'_attrs.get("{arg}", "")' if ctx["type"] == "column" else ctx["code"]


# ----------------------------------------------------------------
# Grok patterns
# ----------------------------------------------------------------
GROK_PATTERNS = {
    "COMBINEDAPACHELOG": (
        r"(?P<client>\S+) \S+ (?P<user>\S+) \[(?P<timestamp>[^\]]+)\] "
        r'"(?P<method>\S+) (?P<request>[^"]+) HTTP/\S+" '
        r'(?P<status>\d+) (?P<size>\S+) "(?P<referrer>[^"]*)" "(?P<agent>[^"]*)"'
    ),
    "COMMONAPACHELOG": (
        r"(?P<client>\S+) \S+ (?P<user>\S+) \[(?P<timestamp>[^\]]+)\] "
        r'"(?P<method>\S+) (?P<request>[^"]+) HTTP/\S+" '
        r"(?P<status>\d+) (?P<size>\S+)"
    ),
    "TIMESTAMP_ISO8601": (
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
        r"(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?"
    ),
    "IP": r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}",
    "NUMBER": r"(?:-?\d+(?:\.\d+)?)",
    "WORD": r"\w+",
    "DATA": r".*?",
    "GREEDYDATA": r".*",
}


def resolve_grok_pattern(name: str) -> str:
    return GROK_PATTERNS.get(name) or GROK_PATTERNS.get(name.upper(), name)


# ----------------------------------------------------------------
# Function application (from function-handlers.js)
# ----------------------------------------------------------------
def apply_nel_function(base: str, call: str, mode: str) -> str:
    """Apply a single NEL function call to a base expression."""
    name, args = extract_func_args(call)
    name_lower = name.lower()

    # String functions
    if name_lower == "toupper":
        return f"upper({base})" if mode == "col" else f"{base}.upper()"
    if name_lower == "tolower":
        return f"lower({base})" if mode == "col" else f"{base}.lower()"
    if name_lower == "trim":
        return f"trim({base})" if mode == "col" else f"{base}.strip()"
    if name_lower == "length":
        return f"length({base})" if mode == "col" else f"len({base})"
    if name_lower == "substring":
        start = args[0] if args else "0"
        end = args[1] if len(args) > 1 else None
        if mode == "col":
            length = str(int(end) - int(start)) if end else "9999"
            return f"substring({base}, {int(start) + 1}, {length})"
        return f"{base}[{start}:{end}]" if end else f"{base}[{start}:]"
    if name_lower == "replace":
        s = unquote_arg(args[0]) if args else ""
        r = unquote_arg(args[1]) if len(args) > 1 else ""
        if mode == "col":
            return f'regexp_replace({base}, "{_py_escape(_regex_escape(s))}", "{_py_escape(r)}")'
        return f'{base}.replace("{_py_escape(s)}", "{_py_escape(r)}")'
    if name_lower == "replaceall":
        pat = unquote_arg(args[0]) if args else ""
        rpl = unquote_arg(args[1]) if len(args) > 1 else ""
        if mode == "col":
            return f'regexp_replace({base}, "{_py_escape(pat)}", "{_py_escape(rpl)}")'
        return f're.sub(r"{_py_escape(pat)}", "{_py_escape(rpl)}", {base})'
    if name_lower == "contains":
        cv = unquote_arg(args[0]) if args else ""
        return f'{base}.contains("{_py_escape(cv)}")' if mode == "col" else f'"{_py_escape(cv)}" in {base}'
    if name_lower == "startswith":
        sv = unquote_arg(args[0]) if args else ""
        return f'{base}.startswith("{_py_escape(sv)}")'
    if name_lower == "endswith":
        ev = unquote_arg(args[0]) if args else ""
        return f'{base}.endswith("{_py_escape(ev)}")'
    if name_lower == "matches":
        mp = unquote_arg(args[0]) if args else ""
        return f'{base}.rlike("{_py_escape(mp)}")' if mode == "col" else f're.match(r"{_py_escape(mp)}", {base})'
    if name_lower == "split":
        sp = unquote_arg(args[0]) if args else ","
        if mode == "col":
            return f'split({base}, "{_py_escape(_regex_escape(sp))}")'
        return f'{base}.split("{_py_escape(sp)}")'
    if name_lower == "substringbefore":
        sb = unquote_arg(args[0]) if args else ""
        if mode == "col":
            return f'substring_index({base}, "{_py_escape(sb)}", 1)'
        return f'{base}.split("{_py_escape(sb)}")[0]'
    if name_lower == "substringafter":
        sa = unquote_arg(args[0]) if args else ""
        sa_len = len(sa)
        if mode == "col":
            esc_sa = _py_escape(sa)
            return (
                f'when(locate("{esc_sa}", {base}) > 0, '
                f'substring({base}, locate("{esc_sa}", {base}) + {sa_len}, 9999))'
                f'.otherwise(lit(""))'
            )
        return f'"{_py_escape(sa)}".join({base}.split("{_py_escape(sa)}")[1:])'
    if name_lower == "append":
        av = unquote_arg(args[0]) if args else ""
        return f'concat({base}, lit("{_py_escape(av)}"))' if mode == "col" else f'{base} + "{_py_escape(av)}"'
    if name_lower == "prepend":
        pv = unquote_arg(args[0]) if args else ""
        return f'concat(lit("{_py_escape(pv)}"), {base})' if mode == "col" else f'"{_py_escape(pv)}" + {base}'
    if name_lower == "equals":
        eq = unquote_arg(args[0]) if args else ""
        return f'({base} == lit("{_py_escape(eq)}"))' if mode == "col" else f'({base} == "{_py_escape(eq)}")'
    if name_lower == "equalsignorecase":
        eic = unquote_arg(args[0]) if args else ""
        eic_low = _py_escape(eic.lower())
        if mode == "col":
            return f'(lower({base}) == lit("{eic_low}"))'
        return f'({base}.lower() == "{eic_low}")'

    # Math functions
    if name_lower in ("plus", "minus", "multiply", "divide", "mod"):
        op_sym = {"plus": "+", "minus": "-", "multiply": "*", "divide": "/", "mod": "%"}
        default = "0" if name_lower in ("plus", "minus") else "1"
        v = args[0] if args else default
        op = op_sym[name_lower]
        if mode == "col":
            return f"({base} {op} lit({v}))"
        return f"({base} {op} {v})"
    if name_lower in ("gt", "ge", "lt", "le"):
        op_map = {"gt": ">", "ge": ">=", "lt": "<", "le": "<="}
        v = args[0] if args else "0"
        op = op_map[name_lower]
        return f"({base} {op} lit({v}))" if mode == "col" else f"({base} {op} {v})"

    # Logic functions
    if name_lower == "isempty":
        return f'({base}.isNull() | ({base} == lit("")))' if mode == "col" else f"(not {base})"
    if name_lower == "isnull":
        return f"{base}.isNull()" if mode == "col" else f"({base} is None)"
    if name_lower == "notnull":
        return f"{base}.isNotNull()" if mode == "col" else f"({base} is not None)"
    if name_lower == "not":
        return f"(~{base})" if mode == "col" else f"(not {base})"
    if name_lower == "ifelse":
        tv = resolve_nel_arg(args[0], mode) if args else "lit(True)"
        fv = resolve_nel_arg(args[1], mode) if len(args) > 1 else "lit(None)"
        return f"when({base}, {tv}).otherwise({fv})" if mode == "col" else f"({tv} if {base} else {fv})"

    # Date functions
    if name_lower == "format":
        fmt = unquote_arg(args[0]) if args else "yyyy-MM-dd"
        if mode == "col":
            return f'date_format({base}, "{fmt}")'
        py_fmt = java_date_to_python_full(fmt)
        return f'{base}.strftime("{py_fmt}")'
    if name_lower == "todate":
        fmt = unquote_arg(args[0]) if args else "yyyy-MM-dd"
        if mode == "col":
            return f'to_timestamp({base}, "{fmt}")'
        py_fmt = java_date_to_python_full(fmt)
        return f'datetime.strptime({base}, "{py_fmt}")'
    if name_lower == "tonumber":
        return f'{base}.cast("long")' if mode == "col" else f"int({base})"
    if name_lower == "tostring":
        return f'{base}.cast("string")' if mode == "col" else f"str({base})"

    # Grok
    if name_lower == "grok":
        pattern = unquote_arg(args[0]) if args else ""
        resolved = resolve_grok_pattern(pattern)
        esc_resolved = _py_escape(resolved)
        if mode == "col":
            return f'regexp_extract({base}, "{esc_resolved}", 0)'
        return f're.search(r"{esc_resolved}", {base})'

    # Encoding functions
    if name_lower == "base64encode":
        if mode == "col":
            return f"base64(encode({base}, 'UTF-8'))"
        return f"base64.b64encode(str({base}).encode()).decode()"
    if name_lower == "base64decode":
        return f"unbase64({base}).cast('string')" if mode == "col" else f"base64.b64decode(str({base})).decode()"

    # Unknown -> comment
    return f"{base}  /* NEL: {call} */"
