"""Pipeline-level models: parse results, analysis, assessment, generation, validation."""

from pydantic import BaseModel

from app.models.processor import Connection, ControllerService, ProcessGroup, Processor


class Warning(BaseModel):
    """A diagnostic warning emitted during parsing or analysis."""

    severity: str  # "critical", "warning", "info"
    message: str
    source: str = ""


class ParseResult(BaseModel):
    """Normalized output from any ETL parser."""

    platform: str
    version: str = ""
    processors: list[Processor] = []
    connections: list[Connection] = []
    process_groups: list[ProcessGroup] = []
    controller_services: list[ControllerService] = []
    metadata: dict = {}
    warnings: list[Warning] = []


class AnalysisResult(BaseModel):
    """Output from the analysis engine."""

    dependency_graph: dict = {}
    external_systems: list[dict] = []
    cycles: list[list[str]] = []
    flow_metrics: dict = {}
    security_findings: list[dict] = []
    stages: list[dict] = []


class MappingEntry(BaseModel):
    """A single processor-to-Databricks mapping entry."""

    name: str
    type: str
    role: str
    mapped: bool
    confidence: float
    code: str = ""
    notes: str = ""


class AssessmentResult(BaseModel):
    """Output from the mapper/assessment engine."""

    mappings: list[MappingEntry] = []
    packages: list[str] = []
    unmapped_count: int = 0


class NotebookCell(BaseModel):
    """A single cell in a generated Databricks notebook."""

    type: str  # "code" | "markdown"
    source: str
    label: str = ""


class NotebookResult(BaseModel):
    """Output from the notebook generator."""

    cells: list[NotebookCell] = []
    workflow: dict = {}


class ValidationScore(BaseModel):
    """A score for a single validation dimension."""

    dimension: str
    score: float
    details: str = ""


class ValidationResult(BaseModel):
    """Output from the validation engine."""

    overall_score: float = 0
    scores: list[ValidationScore] = []
    gaps: list[dict] = []
    errors: list[str] = []
