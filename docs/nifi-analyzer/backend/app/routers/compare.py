"""Flow comparison router — compare multiple parse results side by side."""

import logging

from fastapi import APIRouter
from pydantic import Field

from app.models.processor import CamelModel

router = APIRouter()
logger = logging.getLogger(__name__)


class CompareRequest(CamelModel):
    """Request body with multiple parse results to compare."""
    flows: list[dict] = Field(..., min_length=1, max_length=10)
    labels: list[str] = []


@router.post("/compare")
async def compare_flows(req: CompareRequest) -> dict:
    """Compare multiple parsed flow results and return a comparison matrix."""
    results = []
    for i, flow in enumerate(req.flows):
        processors = flow.get("processors", [])
        connections = flow.get("connections", [])
        groups = flow.get("processGroups", [])
        services = flow.get("controllerServices", [])

        # Compute type distribution
        type_counts: dict[str, int] = {}
        for p in processors:
            ptype = p.get("type", "unknown").split(".")[-1]
            type_counts[ptype] = type_counts.get(ptype, 0) + 1

        label = req.labels[i] if i < len(req.labels) else f"Flow {i + 1}"

        results.append({
            "label": label,
            "processorCount": len(processors),
            "connectionCount": len(connections),
            "processGroupCount": len(groups),
            "controllerServiceCount": len(services),
            "typeDistribution": type_counts,
            "uniqueTypes": len(type_counts),
            "platform": flow.get("platform", "unknown"),
            "warnings": len(flow.get("warnings", [])),
        })

    # Build comparison matrix
    all_types: set[str] = set()
    for r in results:
        all_types.update(r["typeDistribution"].keys())

    matrix = {
        t: [r["typeDistribution"].get(t, 0) for r in results]
        for t in sorted(all_types)
    }

    return {
        "flows": results,
        "comparisonMatrix": matrix,
        "allTypes": sorted(all_types),
    }
