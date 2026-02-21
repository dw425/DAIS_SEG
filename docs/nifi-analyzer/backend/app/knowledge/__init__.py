"""NiFi Knowledge Base — Central access layer for all KB modules.

Usage:
    from app.knowledge import lookup_processor, lookup_nel_function, lookup_service, lookup_relationship
"""

from __future__ import annotations

try:
    from .nifi_processor_kb import (
        lookup_processor,
        get_all_processors,
        get_processors_by_category,
        get_processors_by_role,
    )
except ImportError:
    lookup_processor = None  # type: ignore[assignment]
    get_all_processors = lambda: {}  # noqa: E731
    get_processors_by_category = lambda c: []  # noqa: E731
    get_processors_by_role = lambda r: []  # noqa: E731

try:
    from .nifi_nel_kb import (
        lookup_nel_function,
        get_all_nel_functions,
        get_nel_functions_by_category,
    )
except ImportError:
    lookup_nel_function = None  # type: ignore[assignment]
    get_all_nel_functions = lambda: {}  # noqa: E731
    get_nel_functions_by_category = lambda c: []  # noqa: E731

try:
    from .nifi_services_kb import (
        lookup_service,
        get_all_services,
        get_services_by_category,
    )
except ImportError:
    lookup_service = None  # type: ignore[assignment]
    get_all_services = lambda: {}  # noqa: E731
    get_services_by_category = lambda c: []  # noqa: E731

try:
    from .nifi_relationships_kb import (
        lookup_relationship,
        get_all_relationships,
        get_relationships_by_strategy,
        get_relationships_for_processor,
    )
except ImportError:
    lookup_relationship = None  # type: ignore[assignment]
    get_all_relationships = lambda: {}  # noqa: E731
    get_relationships_by_strategy = lambda s: []  # noqa: E731
    get_relationships_for_processor = lambda p: []  # noqa: E731


__all__ = [
    "lookup_processor",
    "get_all_processors",
    "get_processors_by_category",
    "get_processors_by_role",
    "lookup_nel_function",
    "get_all_nel_functions",
    "get_nel_functions_by_category",
    "lookup_service",
    "get_all_services",
    "get_services_by_category",
    "lookup_relationship",
    "get_all_relationships",
    "get_relationships_by_strategy",
    "get_relationships_for_processor",
]
