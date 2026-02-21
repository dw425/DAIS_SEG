"""Track NiFi FlowFile attribute creation/reading across processors.

Ported from attribute-flow.js.
"""

import logging
import re

from app.models.processor import Connection, Processor

logger = logging.getLogger(__name__)

_INTERNAL_PROPS = {
    "Destination",
    "Return Type",
    "Path Not Found Behavior",
    "Null Value Representation",
    "Character Set",
    "Maximum Buffer Size",
    "Maximum Capture Group Length",
    "Enable Canonical Equivalence",
    "Enable Case-insensitive Matching",
    "Permit Whitespace and Comments",
    "Include Zero Capture Groups",
    "Delete Attributes Expression",
    "Store State",
    "Stateful Variables Initial Value",
}

_ATTRIBUTE_CREATORS = {
    "UpdateAttribute",
    "PutAttribute",
    "EvaluateJsonPath",
    "EvaluateXPath",
    "EvaluateXQuery",
    "ExtractText",
    "ExtractGrok",
    "ExtractHL7Attributes",
}

_BUILTIN_FUNCS = {"now", "nextInt", "random", "UUID", "uuid", "hostname", "ip", "literal", "thread", "entryDate"}

_EL_REF_RE = re.compile(r"\$\{([^}:]+)")


def _extract_el_references(text: str) -> list[str]:
    """Extract attribute references from NiFi Expression Language strings.

    Handles nested ${...} by tracking brace depth rather than relying on
    a simple regex that breaks on nested expressions like
    ${literal(${other}):toUpper()}.
    """
    refs: list[str] = []
    pos = 0
    while pos < len(text):
        idx = text.find("${", pos)
        if idx < 0:
            break
        # Extract the base variable name (before first : or })
        start = idx + 2
        end = start
        depth = 0
        while end < len(text):
            ch = text[end]
            if ch == "{":
                depth += 1
            elif ch == "}":
                if depth == 0:
                    break
                depth -= 1
            elif ch == ":" and depth == 0:
                break
            end += 1
        name = text[start:end].strip()
        # Skip names that are function calls or nested expressions
        if name and "(" not in name and "$" not in name and len(name) <= 80:
            refs.append(name)
        pos = end + 1
    return refs


def analyze_attribute_flow(
    processors: list[Processor],
    connections: list[Connection],
) -> dict:
    """Analyze attribute flow across all processors.

    Returns:
        {
            "attribute_map": {attr: {"creators": [], "readers": [], "modifiers": []}},
            "processor_attributes": {proc_name: {"creates": [], "reads": []}},
            "attribute_lineage": {attr: [{"proc": name, "action": "create"|"read"|"modify"}]},
        }
    """
    logger.info("Analyzing attribute flow: %d processors, %d connections", len(processors), len(connections))
    attribute_map: dict[str, dict] = {}
    processor_attrs: dict[str, dict] = {}

    def ensure_attr(name: str) -> dict:
        if name not in attribute_map:
            attribute_map[name] = {"creators": [], "readers": [], "modifiers": []}
        return attribute_map[name]

    def ensure_proc(name: str) -> dict:
        if name not in processor_attrs:
            processor_attrs[name] = {"creates": [], "reads": []}
        return processor_attrs[name]

    for p in processors:
        if not p.name:
            continue
        proc_info = ensure_proc(p.name)

        # Attributes created by this processor
        if p.type in _ATTRIBUTE_CREATORS:
            for k in p.properties:
                if k in _INTERNAL_PROPS or k.startswith("nifi-") or k.startswith("Record "):
                    continue
                attr = ensure_attr(k)
                if attr["creators"] and p.type in ("UpdateAttribute", "PutAttribute"):
                    if p.name not in attr["modifiers"]:
                        attr["modifiers"].append(p.name)
                else:
                    if p.name not in attr["creators"]:
                        attr["creators"].append(p.name)
                if k not in proc_info["creates"]:
                    proc_info["creates"].append(k)

        # Attributes read by this processor — use depth-aware extraction
        # to correctly handle nested ${...} expressions
        for v in p.properties.values():
            if not v or not isinstance(v, str) or "${" not in v:
                continue
            for attr_name in _extract_el_references(v):
                if attr_name.lower() in {f.lower() for f in _BUILTIN_FUNCS}:
                    continue
                attr = ensure_attr(attr_name)
                if p.name not in attr["readers"]:
                    attr["readers"].append(p.name)
                if attr_name not in proc_info["reads"]:
                    proc_info["reads"].append(attr_name)

    # Build lineage
    attribute_lineage: dict[str, list[dict]] = {}
    for attr_name, entry in attribute_map.items():
        lineage: list[dict] = []
        for proc in entry["creators"]:
            lineage.append({"proc": proc, "action": "create"})
        for proc in entry["modifiers"]:
            lineage.append({"proc": proc, "action": "modify"})
        for proc in entry["readers"]:
            lineage.append({"proc": proc, "action": "read"})
        if lineage:
            attribute_lineage[attr_name] = lineage

    logger.info("Attribute flow analysis complete: %d attributes tracked", len(attribute_map))
    return {
        "attribute_map": attribute_map,
        "processor_attributes": processor_attrs,
        "attribute_lineage": attribute_lineage,
    }
