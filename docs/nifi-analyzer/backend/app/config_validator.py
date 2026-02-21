"""Startup configuration validator — ensures YAML files and mapper modules are sound."""

import importlib
import logging
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

_YAML_DIR = Path(__file__).resolve().parent / "constants" / "processor_maps"

_MAPPER_MODULES = [
    "app.engines.parsers",
    "app.engines.analyzers",
    "app.engines.mappers",
    "app.engines.generators",
    "app.engines.validators",
    "app.engines.reporters",
]


def validate_config() -> dict:
    """Check that all YAML mapping files are parseable and mapper modules importable.

    Returns a summary dict.  Logs warnings for any issues found but does **not**
    raise — the application can still start with partial functionality.
    """
    results: dict = {"yaml_files": {}, "modules": {}, "errors": []}

    # --- YAML files ---
    if _YAML_DIR.is_dir():
        for path in sorted(_YAML_DIR.glob("*.yaml")):
            try:
                with open(path, encoding="utf-8") as fh:
                    data = yaml.safe_load(fh)
                count = len(data) if isinstance(data, dict) else 0
                results["yaml_files"][path.name] = {"ok": True, "entries": count}
            except Exception as exc:
                msg = f"YAML parse error in {path.name}: {exc}"
                logger.warning(msg)
                results["yaml_files"][path.name] = {"ok": False, "error": str(exc)}
                results["errors"].append(msg)
    else:
        msg = f"YAML directory not found: {_YAML_DIR}"
        logger.warning(msg)
        results["errors"].append(msg)

    # --- Engine modules ---
    for mod_name in _MAPPER_MODULES:
        try:
            importlib.import_module(mod_name)
            results["modules"][mod_name] = {"ok": True}
        except Exception as exc:
            msg = f"Module import failed for {mod_name}: {exc}"
            logger.warning(msg)
            results["modules"][mod_name] = {"ok": False, "error": str(exc)}
            results["errors"].append(msg)

    if results["errors"]:
        logger.warning("Config validation found %d issue(s)", len(results["errors"]))
    else:
        logger.info(
            "Config validation passed — %d YAML files, %d modules OK",
            len(results["yaml_files"]),
            len(results["modules"]),
        )

    return results
