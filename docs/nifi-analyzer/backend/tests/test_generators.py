"""Tests for the notebook generator."""

from app.engines.generators.autoloader_generator import (
    is_autoloader_candidate,
    is_jdbc_source,
    is_kafka_source,
)
from app.engines.generators.cell_builders import build_imports_cell
from app.engines.generators.code_scrubber import scrub_code
from app.engines.generators.code_validator import validate_python_syntax
from app.engines.generators.connection_generator import generate_connections
from app.engines.generators.dab_generator import _period_to_cron, generate_dab
from app.engines.generators.notebook_generator import generate_notebook
from app.engines.generators.workflow_generator import generate_workflow
from app.models.config import DatabricksConfig
from app.models.pipeline import AssessmentResult, MappingEntry, NotebookResult, ParseResult
from app.models.processor import Connection, ControllerService, Processor


def test_generate_basic_notebook():
    pr = ParseResult(
        platform="nifi",
        processors=[Processor(name="src", type="GetFile", platform="nifi")],
        metadata={"source_file": "test.xml"},
    )
    assessment = AssessmentResult(
        mappings=[
            MappingEntry(
                name="src",
                type="GetFile",
                role="source",
                mapped=True,
                confidence=0.9,
                code='df_src = spark.read.format("csv").load("/data")',
            ),
        ],
    )
    config = DatabricksConfig()

    result = generate_notebook(pr, assessment, config)
    assert isinstance(result, NotebookResult)
    assert len(result.cells) >= 4  # title, imports, config, at least one step
    assert any(c.label == "title" for c in result.cells)
    assert any(c.label == "config" for c in result.cells)
    assert result.workflow is not None


def test_notebook_cells_have_types():
    pr = ParseResult(platform="nifi", processors=[], metadata={"source_file": "test.xml"})
    assessment = AssessmentResult(mappings=[])
    result = generate_notebook(pr, assessment, DatabricksConfig())
    for cell in result.cells:
        assert cell.type in ("code", "markdown")


def test_workflow_generation():
    pr = ParseResult(
        platform="nifi",
        processors=[Processor(name="p1", type="GetFile", platform="nifi")],
        metadata={"source_file": "flow.xml"},
    )
    assessment = AssessmentResult(
        mappings=[MappingEntry(name="p1", type="GetFile", role="source", mapped=True, confidence=0.9, code="x = 1")],
    )
    result = generate_notebook(pr, assessment, DatabricksConfig())
    assert "name" in result.workflow
    assert "tasks" in result.workflow


def test_validate_python_syntax_valid():
    errors = validate_python_syntax("x = 1\nprint(x)")
    assert len(errors) == 0


def test_validate_python_syntax_invalid():
    errors = validate_python_syntax("def foo(\n  x = ")
    assert len(errors) >= 1


# ---------------------------------------------------------------------------
# Improvement Cycle 1: Code Scrubber — connection string and .option() scrubbing
# ---------------------------------------------------------------------------

def test_scrub_code_connection_string():
    """Code scrubber should detect and replace embedded credentials in JDBC URLs."""
    code = 'url = "jdbc:mysql://admin:supersecret@db.example.com:3306/mydb"'
    scrubbed = scrub_code(code)
    assert "supersecret" not in scrubbed
    assert "dbutils.secrets.get" in scrubbed


def test_scrub_code_option_password():
    """Code scrubber should detect .option('password', 'literal') patterns."""
    code = '.option("password", "my_secret_pw")'
    scrubbed = scrub_code(code)
    assert "my_secret_pw" not in scrubbed
    assert "dbutils.secrets.get" in scrubbed


def test_scrub_code_preserves_safe_code():
    """Code scrubber should not mangle code without secrets."""
    code = 'df = spark.read.format("csv").load("/data")\nprint("hello")'
    assert scrub_code(code) == code


# ---------------------------------------------------------------------------
# Improvement Cycle 1: Auto Loader — fully-qualified processor type support
# ---------------------------------------------------------------------------

def test_autoloader_fully_qualified_type():
    """Auto Loader detector should match fully-qualified NiFi processor class names."""
    assert is_autoloader_candidate("org.apache.nifi.processors.standard.GetFile") is True
    assert is_autoloader_candidate("GetFile") is True
    assert is_autoloader_candidate("org.apache.nifi.processors.standard.PutFile") is False


def test_kafka_fully_qualified_type():
    """Kafka detector should match fully-qualified NiFi processor class names."""
    assert is_kafka_source("org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_6") is True
    assert is_kafka_source("ConsumeKafka") is True


def test_jdbc_fully_qualified_type():
    """JDBC detector should match fully-qualified NiFi processor class names."""
    assert is_jdbc_source("org.apache.nifi.processors.standard.QueryDatabaseTable") is True
    assert is_jdbc_source("ExecuteSQL") is True


# ---------------------------------------------------------------------------
# Improvement Cycle 1: DAB Generator — seconds-to-cron conversion
# ---------------------------------------------------------------------------

def test_period_to_cron_seconds():
    """_period_to_cron should handle seconds by rounding up to 1 minute minimum."""
    result = _period_to_cron("30 sec")
    assert result is not None
    assert "0/1" in result  # 30 sec -> 1 minute


def test_period_to_cron_minutes():
    result = _period_to_cron("5 min")
    assert result is not None
    assert "0/5" in result


def test_period_to_cron_large_seconds():
    """120 seconds should convert to 2 minutes."""
    result = _period_to_cron("120 sec")
    assert result is not None
    assert "0/2" in result


# ---------------------------------------------------------------------------
# Improvement Cycle 1: Workflow Generator — DAG topology from connections
# ---------------------------------------------------------------------------

def test_workflow_dag_topology():
    """Workflow generator should use NiFi connections for task dependencies."""
    pr = ParseResult(
        platform="nifi",
        processors=[
            Processor(name="src1", type="GetFile", platform="nifi"),
            Processor(name="src2", type="GetS3Object", platform="nifi"),
            Processor(name="merge", type="MergeContent", platform="nifi"),
        ],
        connections=[
            Connection(source_name="src1", destination_name="merge"),
            Connection(source_name="src2", destination_name="merge"),
        ],
        metadata={"source_file": "flow.xml"},
    )
    assessment = AssessmentResult(
        mappings=[
            MappingEntry(name="src1", type="GetFile", role="source", mapped=True, confidence=0.9, code="x=1"),
            MappingEntry(name="src2", type="GetS3Object", role="source", mapped=True, confidence=0.9, code="x=2"),
            MappingEntry(name="merge", type="MergeContent", role="transform", mapped=True, confidence=0.8, code="x=3"),
        ],
    )
    workflow = generate_workflow(pr, assessment, DatabricksConfig())
    tasks = workflow["tasks"]

    # src1 and src2 should have no deps (parallel roots)
    src1_task = next(t for t in tasks if "src1" in t["task_key"])
    src2_task = next(t for t in tasks if "src2" in t["task_key"])
    merge_task = next(t for t in tasks if "merge" in t["task_key"])

    assert "depends_on" not in src1_task
    assert "depends_on" not in src2_task
    assert "depends_on" in merge_task
    assert len(merge_task["depends_on"]) == 2  # depends on both sources


def test_workflow_tasks_have_cluster_key():
    """Every task should have a job_cluster_key."""
    pr = ParseResult(
        platform="nifi",
        processors=[Processor(name="p1", type="GetFile", platform="nifi")],
        metadata={"source_file": "flow.xml"},
    )
    assessment = AssessmentResult(
        mappings=[MappingEntry(name="p1", type="GetFile", role="source", mapped=True, confidence=0.9, code="x = 1")],
    )
    workflow = generate_workflow(pr, assessment, DatabricksConfig())
    for task in workflow["tasks"]:
        assert "job_cluster_key" in task


# ---------------------------------------------------------------------------
# Improvement Cycle 2: imports cell — F alias + selective wildcard
# ---------------------------------------------------------------------------

def test_imports_cell_has_f_alias():
    """Imports cell must include 'import functions as F' so processor_translators code works."""
    assessment = AssessmentResult(mappings=[], packages=[])
    code = build_imports_cell(assessment)
    assert "as F" in code
    # Also ensure basic convenience functions are imported bare
    assert "col" in code
    assert "lit" in code


# ---------------------------------------------------------------------------
# Improvement Cycle 2: Kafka source vs sink separation
# ---------------------------------------------------------------------------

def test_publish_kafka_not_treated_as_source():
    """PublishKafka is a SINK — is_kafka_source must return False for it."""
    assert is_kafka_source("PublishKafka") is False
    assert is_kafka_source("PublishKafka_2_6") is False
    assert is_kafka_source("PublishKafkaRecord_2_6") is False
    # Consumers should still work
    assert is_kafka_source("ConsumeKafka") is True
    assert is_kafka_source("ConsumeKafka_2_6") is True


# ---------------------------------------------------------------------------
# Improvement Cycle 2: DAB generator — no conflicting num_workers + autoscale
# ---------------------------------------------------------------------------

def test_dab_job_cluster_no_conflicting_sizing():
    """DAB job clusters must NOT have both num_workers and autoscale simultaneously."""
    pr = ParseResult(
        platform="nifi",
        processors=[
            Processor(
                name="p1",
                type="GetFile",
                platform="nifi",
                scheduling={"concurrentlySchedulableTaskCount": "4"},
            ),
        ],
        metadata={"source_file": "flow.xml"},
    )
    assessment = AssessmentResult(
        mappings=[
            MappingEntry(name="p1", type="GetFile", role="source", mapped=True, confidence=0.9, code="x = 1"),
        ],
    )
    result = generate_dab(pr, assessment, DatabricksConfig())
    clusters = result["bundle_structure"]["resources"]["jobs"]["migration_job"]["job_clusters"]
    for cluster in clusters:
        nc = cluster["new_cluster"]
        has_num = "num_workers" in nc
        has_auto = "autoscale" in nc
        assert not (has_num and has_auto), (
            f"Cluster config has both num_workers and autoscale: {nc}"
        )


# ---------------------------------------------------------------------------
# Improvement Cycle 2: connection_generator — correct JDBC port parsing
# ---------------------------------------------------------------------------

def test_connection_generator_parses_jdbc_port():
    """Connection generator should extract port from JDBC URL, not hardcode 443."""
    pr = ParseResult(
        platform="nifi",
        controller_services=[
            ControllerService(
                name="MyDB Pool",
                type="DBCPConnectionPool",
                properties={
                    "Database Connection URL": "jdbc:mysql://dbhost:3306/mydb",
                    "Database Driver Class Name": "com.mysql.cj.jdbc.Driver",
                    "Database User": "appuser",
                    "Password": "${db_pass}",
                },
            ),
        ],
    )
    result = generate_connections(pr)
    assert len(result["connection_mappings"]) == 1
    mapping = result["connection_mappings"][0]
    assert mapping["port"] == "3306"
    assert mapping["host"] == "dbhost"
    assert mapping["database"] == "mydb"
    # Verify the SQL uses correct port too
    sql_entry = result["sql_statements"][0]
    assert "3306" in sql_entry["sql"]
    assert "443" not in sql_entry["sql"]


def test_connection_generator_default_port_for_postgresql():
    """When port is omitted from JDBC URL, use the correct default for the DB type."""
    pr = ParseResult(
        platform="nifi",
        controller_services=[
            ControllerService(
                name="PG Pool",
                type="DBCPConnectionPool",
                properties={
                    "Database Connection URL": "jdbc:postgresql://pghost/mydb",
                    "Database User": "pguser",
                },
            ),
        ],
    )
    result = generate_connections(pr)
    mapping = result["connection_mappings"][0]
    assert mapping["port"] == "5432"
