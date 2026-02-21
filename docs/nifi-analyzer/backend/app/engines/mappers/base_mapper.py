"""Base mapper utilities shared across all platform mappers."""

import logging
import re
from pathlib import Path

import yaml

from app.models.pipeline import AnalysisResult, AssessmentResult, MappingEntry, ParseResult

logger = logging.getLogger(__name__)

_CONSTANTS_DIR = Path(__file__).parent.parent.parent / "constants" / "processor_maps"

# Regex for unresolved template placeholders (e.g., {input_directory}, {table_name})
_UNRESOLVED_PLACEHOLDER_RE = re.compile(r"\{[a-z][a-z0-9_]*\}")


def normalize_yaml(raw: dict) -> dict[str, dict]:
    """Normalize YAML mappings into a {type: entry} dict.

    Handles both formats:
      1. List under 'mappings' key: {mappings: [{type: "X", ...}, ...]}
      2. Direct dict keyed by type: {"X": {...}, ...}
    """
    if "mappings" in raw and isinstance(raw["mappings"], list):
        return {
            e["type"]: e
            for e in raw["mappings"]
            if isinstance(e, dict) and "type" in e
        }
    return {k: v for k, v in raw.items() if isinstance(v, dict)}


def load_yaml(platform: str) -> dict[str, dict]:
    """Load and normalize a platform's YAML mapping file."""
    yaml_path = _CONSTANTS_DIR / f"{platform}_databricks.yaml"
    if not yaml_path.exists():
        logger.warning("YAML mapping not found: %s", yaml_path)
        return {}
    with open(yaml_path) as f:
        raw = yaml.safe_load(f) or {}
    return normalize_yaml(raw)


def safe_var(name: str) -> str:
    """Convert a processor name to a safe Python variable name."""
    s = re.sub(r"[^a-zA-Z0-9_]", "_", name).strip("_")
    if not s or s[0].isdigit():
        s = "p_" + s
    return s.lower()


def infer_role(proc_type: str) -> str:
    """Infer a basic role from processor type name.

    Handles both simple names (GetFile) and fully-qualified names
    (org.apache.nifi.processors.standard.GetFile).
    """
    # Strip package prefix for role inference
    simple = proc_type.rsplit(".", 1)[-1] if "." in proc_type else proc_type
    t = simple.lower()
    if any(w in t for w in ("source", "input", "read", "get", "fetch", "consume", "listen", "query", "scan", "select", "generate", "list", "tail")):
        return "source"
    if any(w in t for w in ("sink", "output", "write", "put", "publish", "send", "insert", "store", "delete")):
        return "sink"
    if any(w in t for w in ("route", "distribute", "detect", "funnel")):
        return "route"
    if any(w in t for w in ("execute", "invoke", "handle")):
        return "process"
    if any(w in t for w in ("log", "debug", "count", "wait", "notify", "control", "monitor")):
        return "utility"
    if any(w in t for w in ("transform", "convert", "replace", "merge", "split", "jolt", "extract", "evaluate", "compress", "encrypt", "lookup", "validate")):
        return "transform"
    return "transform"


def _count_unresolved(code: str) -> int:
    """Count unresolved template placeholders remaining in generated code."""
    return len(_UNRESOLVED_PLACEHOLDER_RE.findall(code))


def _annotate_unresolved(code: str) -> str:
    """Add TODO comments for unresolved placeholders."""
    unresolved = _UNRESOLVED_PLACEHOLDER_RE.findall(code)
    if not unresolved:
        return code
    warning = (
        f"# WARNING: {len(unresolved)} unresolved placeholder(s): {', '.join(unresolved)}\n"
        f"# TODO: supply these values via processor properties or manual configuration\n"
    )
    return warning + code


def map_platform_generic(
    platform: str,
    parse_result: ParseResult,
    analysis_result: AnalysisResult,
) -> AssessmentResult:
    """Generic mapper that loads YAML and maps processors for any platform."""
    table = load_yaml(platform)
    mappings: list[MappingEntry] = []
    packages: set[str] = set()
    unmapped = 0

    for p in parse_result.processors:
        # Support both simple and fully-qualified processor types
        simple_type = p.type.rsplit(".", 1)[-1] if "." in p.type else p.type
        entry = table.get(p.type) or table.get(simple_type)
        role = infer_role(p.type)

        if entry:
            code = entry.get("template", "")
            sname = safe_var(p.name)
            # Substitute properties first so longer placeholders like
            # {input_directory} are resolved before short aliases.
            for prop_key, prop_val in p.properties.items():
                placeholder = "{" + prop_key.lower().replace(" ", "_") + "}"
                code = code.replace(placeholder, str(prop_val))
            # Short generic aliases — longest first to prevent partial matches
            code = code.replace("{input}", sname)
            code = code.replace("{name}", sname)
            code = code.replace("{in}", sname)
            code = code.replace("{v}", sname)

            for pkg in entry.get("imports", []):
                if pkg:
                    packages.add(pkg)

            # Adjust confidence based on unresolved placeholder count
            base_confidence = entry.get("confidence", 0.7)
            unresolved_count = _count_unresolved(code)
            if unresolved_count > 0:
                confidence = max(0.3, base_confidence - (unresolved_count * 0.1))
                code = _annotate_unresolved(code)
                notes = (
                    entry.get("notes", entry.get("description", ""))
                    + f" [{unresolved_count} unresolved placeholder(s)]"
                )
            else:
                confidence = base_confidence
                notes = entry.get("notes", entry.get("description", ""))

            mappings.append(
                MappingEntry(
                    name=p.name,
                    type=p.type,
                    role=entry.get("role", role),
                    category=entry.get("category", ""),
                    mapped=True,
                    confidence=confidence,
                    code=code,
                    notes=notes,
                )
            )
        else:
            unmapped += 1
            mappings.append(
                MappingEntry(
                    name=p.name,
                    type=p.type,
                    role=role,
                    category="Unmapped",
                    mapped=False,
                    confidence=0.0,
                    code=f"# UNMAPPED: {platform} {p.type} ({p.name}) -- manual migration required",
                    notes=f"No Databricks mapping found for {p.type}",
                )
            )

    return AssessmentResult(
        mappings=mappings,
        packages=sorted(packages),
        unmapped_count=unmapped,
    )
