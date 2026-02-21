"""Pipeline-level models: parse results, analysis, assessment, generation, validation."""

from app.models.processor import CamelModel, Connection, ControllerService, ProcessGroup, Processor


class Warning(CamelModel):
    """A diagnostic warning emitted during parsing or analysis."""

    severity: str  # "critical", "warning", "info"
    message: str
    source: str = ""


class ParameterEntry(CamelModel):
    """A single parameter within a NiFi Parameter Context."""

    key: str
    value: str = ""
    sensitive: bool = False
    inferred_type: str = "string"  # "string", "numeric", "secret"
    databricks_variable: str = ""  # mapped Databricks Asset Bundle variable name


class ParameterContext(CamelModel):
    """A NiFi Parameter Context — maps to Databricks Asset Bundle variables."""

    name: str
    parameters: list[ParameterEntry] = []


class ParseResult(CamelModel):
    """Normalized output from any ETL parser."""

    platform: str
    version: str = ""
    processors: list[Processor] = []
    connections: list[Connection] = []
    process_groups: list[ProcessGroup] = []
    controller_services: list[ControllerService] = []
    parameter_contexts: list[ParameterContext] = []  # Phase 1: parameter context extraction
    metadata: dict = {}
    warnings: list[Warning] = []


class CycleClassification(CamelModel):
    """Classification of a detected cycle for Databricks translation."""

    cycle_nodes: list[str] = []
    category: str = ""  # "error_retry", "data_reevaluation", "pagination"
    description: str = ""
    databricks_translation: str = ""  # e.g. "max_retries", "for_each_task", "while_loop"


class TaskCluster(CamelModel):
    """A group of contiguous processors merged into a single logical execution unit."""

    id: str
    processors: list[str] = []  # processor names
    entry_processor: str = ""
    exit_processors: list[str] = []
    connections: list[str] = []  # internal connection descriptions


class BackpressureConfig(CamelModel):
    """NiFi backpressure settings mapped to Databricks Auto Loader options."""

    connection_source: str = ""
    connection_destination: str = ""
    nifi_object_threshold: int = 0
    nifi_data_size_threshold: str = ""
    databricks_max_files_per_trigger: int | None = None
    databricks_max_bytes_per_trigger: str | None = None


class AnalysisResult(CamelModel):
    """Output from the analysis engine."""

    dependency_graph: dict = {}
    external_systems: list[dict] = []
    cycles: list[list[str]] = []
    cycle_classifications: list[CycleClassification] = []  # Phase 2: classified cycles
    task_clusters: list[TaskCluster] = []  # Phase 2: clustered tasks
    backpressure_configs: list[BackpressureConfig] = []  # Phase 2: backpressure translation
    flow_metrics: dict = {}
    security_findings: list[dict] = []
    stages: list[dict] = []
    # Advanced conversion features
    transaction_analysis: dict = {}
    state_analysis: dict = {}
    schema_analysis: dict = {}
    execution_mode_analysis: dict = {}
    site_to_site_analysis: dict = {}
    process_group_analysis: dict = {}
    attribute_translation: dict = {}
    connection_generation: dict = {}
    cicd_generation: dict = {}


class MappingEntry(CamelModel):
    """A single processor-to-Databricks mapping entry."""

    name: str
    type: str
    role: str
    category: str = ""
    mapped: bool
    confidence: float
    code: str = ""
    notes: str = ""


class AssessmentResult(CamelModel):
    """Output from the mapper/assessment engine."""

    mappings: list[MappingEntry] = []
    packages: list[str] = []
    unmapped_count: int = 0


class NotebookCell(CamelModel):
    """A single cell in a generated Databricks notebook."""

    type: str  # "code" | "markdown"
    source: str
    label: str = ""


class NotebookResult(CamelModel):
    """Output from the notebook generator."""

    cells: list[NotebookCell] = []
    workflow: dict = {}


class ValidationScore(CamelModel):
    """A score for a single validation dimension."""

    dimension: str
    score: float
    details: str = ""


class ValidationResult(CamelModel):
    """Output from the validation engine."""

    overall_score: float = 0
    scores: list[ValidationScore] = []
    gaps: list[dict] = []
    errors: list[str] = []
