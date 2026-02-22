"""Effort estimator — per-component migration effort estimation.

Provides:
- Hours estimate per processor based on complexity, confidence, manual requirements
- Critical path identification (longest chain of manual processors)
- Team skill requirements (SQL, PySpark, Streaming, Delta Lake)

Inspired by Databricks Labs' Lakebridge effort estimation format.
"""

from __future__ import annotations

import logging

from app.models.pipeline import AssessmentResult, ParseResult

logger = logging.getLogger(__name__)

# Hours estimates by complexity/confidence
_EFFORT_MATRIX = {
    # (mapped, confidence_range) -> base hours
    (True, "high"):    1,    # >= 0.9 confidence
    (True, "medium"):  4,    # 0.7-0.9 confidence
    (True, "low"):     12,   # < 0.7 confidence
    (False, "any"):    20,   # unmapped
}

# Skill requirements by processor category/type
_SKILL_MAP: dict[str, list[str]] = {
    # Sources
    "source": ["PySpark", "Delta Lake"],
    "GetFile": ["PySpark", "Auto Loader"],
    "ConsumeKafka": ["PySpark", "Structured Streaming"],
    "ConsumeKafka_2_6": ["PySpark", "Structured Streaming"],
    "QueryDatabaseTable": ["PySpark", "JDBC", "SQL"],
    "ExecuteSQL": ["PySpark", "SQL"],
    "SelectHiveQL": ["PySpark", "SQL", "HiveQL"],

    # Transforms
    "transform": ["PySpark"],
    "RouteOnAttribute": ["PySpark", "Workflow Orchestration"],
    "JoltTransformJSON": ["PySpark", "JSON Processing"],
    "ExecuteScript": ["PySpark", "UDF Development"],
    "ConvertRecord": ["PySpark", "Schema Management"],
    "UpdateAttribute": ["PySpark"],

    # Sinks
    "sink": ["PySpark", "Delta Lake"],
    "PutDatabaseRecord": ["PySpark", "JDBC", "SQL"],
    "PublishKafka": ["PySpark", "Structured Streaming"],
    "PutS3Object": ["PySpark", "Cloud Storage"],
    "PutHDFS": ["PySpark", "Delta Lake"],

    # Complex types
    "MergeContent": ["PySpark", "Streaming"],
    "SplitJson": ["PySpark", "JSON Processing"],
    "ValidateRecord": ["PySpark", "Schema Management"],
}


def _get_complexity(mapped: bool, confidence: float) -> str:
    """Determine complexity level."""
    if not mapped:
        return "critical"
    if confidence >= 0.9:
        return "low"
    if confidence >= 0.7:
        return "medium"
    return "high"


def _get_hours(mapped: bool, confidence: float) -> float:
    """Estimate hours for a single processor."""
    if not mapped:
        return _EFFORT_MATRIX[(False, "any")]
    if confidence >= 0.9:
        return _EFFORT_MATRIX[(True, "high")]
    if confidence >= 0.7:
        return _EFFORT_MATRIX[(True, "medium")]
    return _EFFORT_MATRIX[(True, "low")]


def _get_skills(proc_type: str, role: str) -> list[str]:
    """Get required skills for a processor."""
    skills = _SKILL_MAP.get(proc_type, [])
    if not skills:
        skills = _SKILL_MAP.get(role, ["PySpark"])
    return skills


def compute_effort_estimate(
    parsed: ParseResult,
    assessment: AssessmentResult,
) -> dict:
    """Compute per-component effort estimation.

    Returns:
    {
        "entries": [...],
        "totalHours": N,
        "criticalPathHours": N,
        "skillRequirements": {skill: count, ...},
        "teamSizeRecommendation": N,
        "estimatedWeeks": N,
    }
    """
    entries: list[dict] = []
    total_hours = 0.0
    skill_counts: dict[str, int] = {}

    # Build connection map for critical path
    downstream: dict[str, list[str]] = {}
    for conn in parsed.connections:
        downstream.setdefault(conn.source_name, []).append(conn.destination_name)

    # Process each mapping
    manual_processors: set[str] = set()
    proc_hours: dict[str, float] = {}

    for m in assessment.mappings:
        hours = _get_hours(m.mapped, m.confidence)
        complexity = _get_complexity(m.mapped, m.confidence)
        skills = _get_skills(m.type, m.role)

        # Track skills
        for skill in skills:
            skill_counts[skill] = skill_counts.get(skill, 0) + 1

        # Track manual processors for critical path
        if complexity in ("high", "critical"):
            manual_processors.add(m.name)
        proc_hours[m.name] = hours

        total_hours += hours

        entries.append({
            "processorName": m.name,
            "processorType": m.type,
            "hoursEstimate": hours,
            "complexity": complexity,
            "requiredSkills": skills,
            "isOnCriticalPath": False,  # Will be updated below
        })

    # Find critical path (longest chain of manual/high-effort processors)
    critical_path_hours = 0.0
    if manual_processors:
        # Simple longest-path through manual processors
        memo: dict[str, float] = {}

        def _longest_path(name: str) -> float:
            if name in memo:
                return memo[name]
            hours = proc_hours.get(name, 0)
            max_downstream = 0.0
            for ds in downstream.get(name, []):
                if ds in manual_processors:
                    max_downstream = max(max_downstream, _longest_path(ds))
            memo[name] = hours + max_downstream
            return memo[name]

        for name in manual_processors:
            path_hours = _longest_path(name)
            critical_path_hours = max(critical_path_hours, path_hours)

        # Mark processors on critical path
        # Find the start of the critical path
        if memo:
            critical_start = max(memo, key=memo.get)  # type: ignore
            # Backtrack to mark entries on the critical path
            current = critical_start
            critical_names: set[str] = set()
            while current:
                critical_names.add(current)
                best_next = None
                best_hours = 0
                for ds in downstream.get(current, []):
                    if ds in manual_processors and memo.get(ds, 0) > best_hours:
                        best_hours = memo[ds]
                        best_next = ds
                current = best_next  # type: ignore

            for entry in entries:
                if entry["processorName"] in critical_names:
                    entry["isOnCriticalPath"] = True

    # Team size recommendation (based on parallel work tracks)
    unique_skill_areas = len(set(s for skills in _SKILL_MAP.values() for s in skills if skill_counts.get(s, 0) > 0))
    team_size = max(1, min(unique_skill_areas, 5))  # 1-5 engineers

    # Estimated weeks (40 hours/week, with team parallelism)
    estimated_weeks = max(1, round(total_hours / (40 * team_size)))

    return {
        "entries": entries,
        "totalHours": round(total_hours, 1),
        "criticalPathHours": round(critical_path_hours, 1),
        "skillRequirements": skill_counts,
        "teamSizeRecommendation": team_size,
        "estimatedWeeks": estimated_weeks,
    }
