"""NEL tokenizer — split NiFi Expression Language chains on top-level colons.

Ported from nel/tokenizer.js.
"""


def tokenize_nel_chain(expr: str) -> list[str]:
    """Split a NEL chain on top-level colons, respecting nested parens and quotes.

    Example: "filename:substringBefore('.'):toUpper()" -> ["filename", "substringBefore('.')", "toUpper()"]
    """
    parts: list[str] = []
    current = ""
    depth = 0
    in_str = False
    str_char = ""

    for ch in expr:
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
        elif ch == ":" and depth == 0:
            parts.append(current)
            current = ""
        else:
            current += ch

    if current:
        parts.append(current)
    return parts
