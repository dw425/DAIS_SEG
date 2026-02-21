"""NEL recursive-descent parser — ported from nel/parser.js.

Parses NiFi Expression Language expressions into PySpark column ops or Python string ops.
"""

from __future__ import annotations

import re

from app.engines.parsers.nel.functions import (
    apply_nel_function,
    resolve_nel_arg,
    resolve_variable_context,
)
from app.engines.parsers.nel.tokenizer import tokenize_nel_chain


def parse_nel_expression(expr: str, mode: str = "col") -> str:
    """Parse a single NEL expression (content inside ${...}) into PySpark or Python code.

    Args:
        expr: Expression content without surrounding ${}.
        mode: 'col' for PySpark column ops, 'python' for string ops.

    Returns:
        Translated PySpark or Python code string.
    """
    parts = tokenize_nel_chain(expr)
    if not parts:
        return '""'

    base = parts[0].strip()
    chain = parts[1:]

    # System functions (no base variable)
    if re.match(r"^now\(\)$", base, re.IGNORECASE):
        result = "current_timestamp()" if mode == "col" else "datetime.now()"
    elif re.match(r"^UUID\(\)$", base, re.IGNORECASE):
        result = 'expr("uuid()")' if mode == "col" else "str(uuid.uuid4())"
    elif re.match(r"^hostname\(\)$", base, re.IGNORECASE):
        result = (
            'lit(spark.conf.get("spark.databricks.clusterUsageTags.clusterName", "unknown"))'
            if mode == "col"
            else "socket.gethostname()"
        )
    elif re.match(r"^nextInt\(\)$|^random\(\)$", base, re.IGNORECASE):
        result = '(rand() * 2147483647).cast("int")' if mode == "col" else "random.randint(0, 2147483647)"
    elif m := re.match(r"^literal\('((?:[^'\\]|\\.)*)'\)$", base, re.IGNORECASE):
        lit_val = m.group(1).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
        result = f'lit("{lit_val}")' if mode == "col" else f'"{lit_val}"'
    elif m := re.match(r"^literal\((\d+)\)$", base, re.IGNORECASE):
        result = f"lit({m.group(1)})" if mode == "col" else m.group(1)
    # Math namespace
    elif m := re.match(r"^math:abs\((.+)\)$", base, re.IGNORECASE):
        arg = resolve_nel_arg(m.group(1), mode)
        result = f"abs({arg})"
    elif m := re.match(r"^math:ceil\((.+)\)$", base, re.IGNORECASE):
        arg = resolve_nel_arg(m.group(1), mode)
        result = f"ceil({arg})" if mode == "col" else f"math.ceil({arg})"
    elif m := re.match(r"^math:floor\((.+)\)$", base, re.IGNORECASE):
        arg = resolve_nel_arg(m.group(1), mode)
        result = f"floor({arg})" if mode == "col" else f"math.floor({arg})"
    elif m := re.match(r"^math:round\((.+)\)$", base, re.IGNORECASE):
        arg = resolve_nel_arg(m.group(1), mode)
        result = f"round({arg})"
    else:
        # Variable reference
        ctx = resolve_variable_context(base)
        if mode == "col":
            result = f'col("{base}")' if ctx["type"] == "column" else ctx["code"]
        else:
            if ctx["type"] == "column":
                result = f'_attrs.get("{base}", "")'
            else:
                result = ctx["code"]

    # Apply chained function calls
    for segment in chain:
        result = apply_nel_function(result, segment.strip(), mode)

    return result


def translate_nel_string(text: str, mode: str = "col") -> str:
    """Translate all ${...} expressions in a string to PySpark/Python code.

    Non-expression text is preserved as string literals. Multiple expressions
    are concatenated.
    """
    parts: list[str] = []
    pos = 0

    while pos < len(text):
        idx = text.find("${", pos)
        if idx < 0:
            # Remaining text is literal
            remaining = text[pos:]
            if remaining:
                escaped = remaining.replace("\\", "\\\\").replace('"', '\\"')
                parts.append(f'lit("{escaped}")' if mode == "col" else f'"{escaped}"')
            break

        # Literal text before ${
        if idx > pos:
            literal = text[pos:idx]
            escaped = literal.replace("\\", "\\\\").replace('"', '\\"')
            parts.append(f'lit("{escaped}")' if mode == "col" else f'"{escaped}"')

        # Find matching }
        depth = 0
        end = idx + 2
        while end < len(text):
            if text[end] == "{":
                depth += 1
            elif text[end] == "}":
                if depth == 0:
                    break
                depth -= 1
            end += 1

        if end >= len(text):
            # Unmatched ${, treat rest as literal
            remaining = text[idx:]
            escaped = remaining.replace("\\", "\\\\").replace('"', '\\"')
            parts.append(f'lit("{escaped}")' if mode == "col" else f'"{escaped}"')
            break

        expr = text[idx + 2 : end]
        parts.append(parse_nel_expression(expr, mode))
        pos = end + 1

    if not parts:
        return '""'
    if len(parts) == 1:
        return parts[0]
    if mode == "col":
        return "concat(" + ", ".join(parts) + ")"
    return " + ".join(parts)
