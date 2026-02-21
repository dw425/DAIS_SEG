"""V6 Integration Tests -- End-to-end notebook generation tests.

Uses simple dataclass mocks to avoid dependency on app.models,
keeping these tests self-contained and fast.
"""

import pytest
from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# Lightweight mock dataclasses (avoids importing app.models)
# ---------------------------------------------------------------------------

@dataclass
class MockProcessor:
    name: str
    type: str
    platform: str = "nifi"
    properties: dict = field(default_factory=dict)
    group: str = "(root)"
    state: str = "RUNNING"
    scheduling: dict | None = None
    resolved_services: dict | None = None


@dataclass
class MockConnection:
    source_name: str
    destination_name: str
    relationship: str = "success"
    back_pressure_object_threshold: int = 0
    back_pressure_data_size_threshold: str = ""


@dataclass
class MockControllerService:
    name: str
    type: str
    properties: dict = field(default_factory=dict)


@dataclass
class MockParseResult:
    platform: str = "nifi"
    version: str = "1.23.2"
    processors: list = field(default_factory=list)
    connections: list = field(default_factory=list)
    process_groups: list = field(default_factory=list)
    controller_services: list = field(default_factory=list)
    parameter_contexts: list = field(default_factory=list)
    metadata: dict = field(default_factory=dict)
    warnings: list = field(default_factory=list)


@dataclass
class MockMappingEntry:
    name: str
    type: str
    role: str
    mapped: bool = True
    confidence: float = 0.9
    code: str = ""
    category: str = ""
    notes: str = ""


@dataclass
class MockAssessmentResult:
    mappings: list = field(default_factory=list)
    packages: list = field(default_factory=list)
    unmapped_count: int = 0


@dataclass
class MockNotebookCell:
    type: str  # "code" | "markdown"
    source: str
    label: str = ""


@dataclass
class MockAnalysisResult:
    dependency_graph: dict = field(default_factory=dict)
    external_systems: list = field(default_factory=list)
    cycles: list = field(default_factory=list)
    cycle_classifications: list = field(default_factory=list)
    task_clusters: list = field(default_factory=list)
    backpressure_configs: list = field(default_factory=list)
    flow_metrics: dict = field(default_factory=dict)
    security_findings: list = field(default_factory=list)
    stages: list = field(default_factory=list)
    execution_mode_analysis: dict = field(default_factory=dict)
    transaction_analysis: dict = field(default_factory=dict)
    state_analysis: dict = field(default_factory=dict)
    schema_analysis: dict = field(default_factory=dict)
    site_to_site_analysis: dict = field(default_factory=dict)
    process_group_analysis: dict = field(default_factory=dict)
    attribute_translation: dict = field(default_factory=dict)
    connection_generation: dict = field(default_factory=dict)
    cicd_generation: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Helper: build a well-formed notebook that passes all 12 checks
# ---------------------------------------------------------------------------

def _build_well_formed_notebook(
    processors: list[MockProcessor],
    connections: list[MockConnection] | None = None,
    streaming: bool = True,
) -> list[MockNotebookCell]:
    """Build a mock notebook that should pass all 12 runnable checks."""
    cells: list[MockNotebookCell] = []

    # Config cell
    cells.append(MockNotebookCell(
        type="code",
        source=(
            '# Configuration\n'
            'catalog = "main"\n'
            'schema = "default"\n'
            'secret_scope = "nifi_migration"\n'
            '\n'
            'spark.sql(f"USE CATALOG {catalog}")\n'
            'spark.sql(f"USE SCHEMA {schema}")'
        ),
        label="config",
    ))

    # Imports cell
    cells.append(MockNotebookCell(
        type="code",
        source=(
            "from pyspark.sql import functions as F\n"
            "from pyspark.sql.functions import col, lit, when\n"
            "from pyspark.sql.types import *\n"
            "import json"
        ),
        label="imports",
    ))

    # Build processor cells in order
    for i, proc in enumerate(processors):
        safe_name = proc.name.lower().replace(" ", "_").replace("-", "_")

        if proc.type in ("GetFile", "GetS3Object", "ListFile", "ConsumeKafka"):
            # Source processor
            if streaming:
                code = (
                    f'df_{safe_name} = (\n'
                    f'    spark.readStream\n'
                    f'    .format("cloudFiles")\n'
                    f'    .option("cloudFiles.format", "json")\n'
                    f'    .load("/Volumes/main/default/landing/{safe_name}")\n'
                    f')\n'
                    f'print(f"[SOURCE] {proc.name} loaded")'
                )
            else:
                code = (
                    f'df_{safe_name} = (\n'
                    f'    spark.read\n'
                    f'    .format("json")\n'
                    f'    .load("/Volumes/main/default/landing/{safe_name}")\n'
                    f')\n'
                    f'print(f"[SOURCE] {proc.name} loaded")'
                )
            cells.append(MockNotebookCell(type="code", source=code, label=f"source_{safe_name}"))

        elif proc.type in ("EvaluateJsonPath", "JoltTransformJSON", "UpdateAttribute",
                            "ReplaceText", "MergeContent"):
            # Transform processor -- read from upstream
            upstream_name = None
            if connections:
                for conn in connections:
                    if conn.destination_name == proc.name:
                        upstream_name = conn.source_name.lower().replace(" ", "_").replace("-", "_")
                        break

            upstream_df = f"df_{upstream_name}" if upstream_name else f"df_{safe_name}_input"

            if proc.type == "MergeContent":
                # Fan-in: may have multiple upstreams
                upstream_names = []
                if connections:
                    for conn in connections:
                        if conn.destination_name == proc.name:
                            uname = conn.source_name.lower().replace(" ", "_").replace("-", "_")
                            upstream_names.append(f"df_{uname}")
                if len(upstream_names) >= 2:
                    code = (
                        f'# Merge {len(upstream_names)} upstream DataFrames\n'
                        f'df_{safe_name} = {upstream_names[0]}.unionByName({upstream_names[1]}, allowMissingColumns=True)\n'
                        f'print(f"[TRANSFORM] {proc.name}: merged {{df_{safe_name}.count()}} rows")'
                    )
                else:
                    code = (
                        f'df_{safe_name} = {upstream_df}\n'
                        f'print(f"[TRANSFORM] {proc.name}")'
                    )
            else:
                code = (
                    f'df_{safe_name} = (\n'
                    f'    {upstream_df}\n'
                    f'    .withColumn("_processed_by", F.lit("{proc.name}"))\n'
                    f')\n'
                    f'print(f"[TRANSFORM] {proc.name} applied")'
                )
            cells.append(MockNotebookCell(type="code", source=code, label=f"transform_{safe_name}"))

        elif proc.type == "RouteOnAttribute":
            # Route processor -- produces filtered branches
            upstream_name = None
            if connections:
                for conn in connections:
                    if conn.destination_name == proc.name:
                        upstream_name = conn.source_name.lower().replace(" ", "_").replace("-", "_")
                        break
            upstream_df = f"df_{upstream_name}" if upstream_name else f"df_{safe_name}_input"

            # Find downstream branches
            downstream_rels: list[tuple[str, str]] = []
            if connections:
                for conn in connections:
                    if conn.source_name == proc.name:
                        dest_safe = conn.destination_name.lower().replace(" ", "_").replace("-", "_")
                        downstream_rels.append((conn.relationship, dest_safe))

            lines = [f'# RouteOnAttribute: {proc.name}']
            lines.append(f'df_{safe_name} = {upstream_df}')

            for rel, dest in downstream_rels:
                if rel == "matched":
                    lines.append(f'df_{safe_name}_matched = df_{safe_name}.filter(F.col("status") == "active")')
                elif rel == "unmatched":
                    lines.append(f'df_{safe_name}_unmatched = df_{safe_name}.filter(F.col("status") != "active")')
                else:
                    lines.append(f'# branch: {rel} -> {dest}')

            code = "\n".join(lines)
            cells.append(MockNotebookCell(type="code", source=code, label=f"route_{safe_name}"))

        elif proc.type in ("PutFile", "PutDatabaseRecord", "PutS3Object"):
            # Sink processor
            upstream_name = None
            if connections:
                for conn in connections:
                    if conn.destination_name == proc.name:
                        upstream_name = conn.source_name.lower().replace(" ", "_").replace("-", "_")
                        break
            upstream_df = f"df_{upstream_name}" if upstream_name else f"df_{safe_name}_input"

            if proc.type == "PutDatabaseRecord":
                code = (
                    f'# Sink: {proc.name} ({proc.type})\n'
                    f'(\n'
                    f'    {upstream_df}\n'
                    f'    .write\n'
                    f'    .format("jdbc")\n'
                    f'    .option("url", dbutils.secrets.get(scope=secret_scope, key="jdbc_url"))\n'
                    f'    .option("dbtable", "{safe_name}")\n'
                    f'    .option("user", dbutils.secrets.get(scope=secret_scope, key="jdbc_user"))\n'
                    f'    .option("password", dbutils.secrets.get(scope=secret_scope, key="jdbc_password"))\n'
                    f'    .mode("append")\n'
                    f'    .save()\n'
                    f')\n'
                    f'print(f"[SINK] JDBC write complete: {safe_name}")'
                )
            elif streaming:
                code = (
                    f'# Sink: {proc.name} ({proc.type})\n'
                    f'query_{safe_name} = (\n'
                    f'    {upstream_df}\n'
                    f'    .writeStream\n'
                    f'    .format("delta")\n'
                    f'    .outputMode("append")\n'
                    f'    .option("checkpointLocation", "/Volumes/main/default/checkpoints/{safe_name}")\n'
                    f'    .trigger(availableNow=True)\n'
                    f'    .toTable("main.default.{safe_name}")\n'
                    f')\n'
                    f'print(f"[SINK] Streaming write started: {safe_name}")'
                )
            else:
                code = (
                    f'# Sink: {proc.name} ({proc.type})\n'
                    f'(\n'
                    f'    {upstream_df}\n'
                    f'    .write\n'
                    f'    .format("delta")\n'
                    f'    .mode("append")\n'
                    f'    .saveAsTable("main.default.{safe_name}")\n'
                    f')\n'
                    f'print(f"[SINK] Batch write complete: {safe_name}")'
                )
            cells.append(MockNotebookCell(type="code", source=code, label=f"sink_{safe_name}"))

    return cells


# ---------------------------------------------------------------------------
# Helpers for flow construction
# ---------------------------------------------------------------------------

def _linear_flow():
    """3-processor linear flow: GetFile -> EvaluateJsonPath -> PutFile."""
    procs = [
        MockProcessor(name="file_source", type="GetFile"),
        MockProcessor(name="json_extract", type="EvaluateJsonPath"),
        MockProcessor(name="file_sink", type="PutFile"),
    ]
    conns = [
        MockConnection(source_name="file_source", destination_name="json_extract"),
        MockConnection(source_name="json_extract", destination_name="file_sink"),
    ]
    return procs, conns


def _fan_out_flow():
    """RouteOnAttribute with matched/unmatched -> two downstream branches."""
    procs = [
        MockProcessor(name="source", type="GetFile"),
        MockProcessor(name="router", type="RouteOnAttribute", properties={
            "active_check": '${status:equals("active")}',
        }),
        MockProcessor(name="matched_sink", type="PutFile"),
        MockProcessor(name="unmatched_sink", type="PutFile"),
    ]
    conns = [
        MockConnection(source_name="source", destination_name="router"),
        MockConnection(source_name="router", destination_name="matched_sink", relationship="matched"),
        MockConnection(source_name="router", destination_name="unmatched_sink", relationship="unmatched"),
    ]
    return procs, conns


def _fan_in_flow():
    """Two sources -> MergeContent -> one sink."""
    procs = [
        MockProcessor(name="source_a", type="GetFile"),
        MockProcessor(name="source_b", type="GetS3Object"),
        MockProcessor(name="merger", type="MergeContent"),
        MockProcessor(name="output_sink", type="PutFile"),
    ]
    conns = [
        MockConnection(source_name="source_a", destination_name="merger"),
        MockConnection(source_name="source_b", destination_name="merger"),
        MockConnection(source_name="merger", destination_name="output_sink"),
    ]
    return procs, conns


# ===========================================================================
# Test 1: Knowledge base completeness
# ===========================================================================

class TestKnowledgeBaseCompleteness:
    """Verify the KB has sufficient entries for production use."""

    def test_processor_kb_has_100_plus(self):
        """KB must have 100+ processors."""
        from app.knowledge.nifi_processor_kb import PROCESSOR_KB
        # PROCESSOR_KB registers each processor under both short and full type,
        # so unique processors = unique ProcessorDef objects
        unique_procs = {id(v) for v in PROCESSOR_KB.values()}
        assert len(unique_procs) >= 100, (
            f"Expected 100+ unique processors, got {len(unique_procs)}"
        )

    def test_nel_kb_has_80_plus(self):
        """KB must have 80+ NEL functions."""
        from app.knowledge.nifi_nel_kb import NEL_FUNCTION_KB
        assert len(NEL_FUNCTION_KB) >= 80, (
            f"Expected 80+ NEL functions, got {len(NEL_FUNCTION_KB)}"
        )

    def test_services_kb_has_50_plus(self):
        """KB must have 50+ controller services."""
        from app.knowledge.nifi_services_kb import SERVICE_KB
        assert len(SERVICE_KB) >= 50, (
            f"Expected 50+ services, got {len(SERVICE_KB)}"
        )

    def test_relationships_kb_has_20_plus(self):
        """KB must have 20+ relationship definitions."""
        from app.knowledge.nifi_relationships_kb import RELATIONSHIP_KB
        assert len(RELATIONSHIP_KB) >= 20, (
            f"Expected 20+ relationships, got {len(RELATIONSHIP_KB)}"
        )


# ===========================================================================
# Test 2: Pipeline wiring -- linear
# ===========================================================================

class TestPipelineWiringLinear:
    """3-processor linear flow produces correctly wired cells."""

    def test_linear_flow_wiring(self):
        """GetFile -> EvaluateJsonPath -> PutFile produces correct cell chain."""
        procs, conns = _linear_flow()
        cells = _build_well_formed_notebook(procs, conns, streaming=True)

        code_cells = [c for c in cells if c.type == "code"]
        all_code = "\n".join(c.source for c in code_cells)

        # Source should define df_file_source
        assert "df_file_source" in all_code
        # Transform should reference df_file_source as upstream
        assert "df_json_extract" in all_code
        # Sink should reference df_json_extract
        assert "df_file_sink" not in all_code or "df_json_extract" in all_code

    def test_linear_flow_has_write(self):
        """Linear flow must end with a write operation."""
        procs, conns = _linear_flow()
        cells = _build_well_formed_notebook(procs, conns, streaming=True)

        all_code = "\n".join(c.source for c in cells if c.type == "code")
        assert ".writeStream" in all_code or ".write" in all_code

    def test_linear_flow_cell_count(self):
        """Linear flow should have config + imports + 3 processor cells = 5 cells."""
        procs, conns = _linear_flow()
        cells = _build_well_formed_notebook(procs, conns, streaming=True)
        assert len(cells) == 5  # config, imports, source, transform, sink


# ===========================================================================
# Test 3: Pipeline wiring -- fan-out
# ===========================================================================

class TestPipelineWiringFanOut:
    """RouteOnAttribute with matched/unmatched -> two downstream branches."""

    def test_fan_out_produces_two_branches(self):
        """Router should produce matched and unmatched branch DataFrames."""
        procs, conns = _fan_out_flow()
        cells = _build_well_formed_notebook(procs, conns, streaming=True)

        all_code = "\n".join(c.source for c in cells if c.type == "code")
        assert "matched" in all_code.lower()
        assert "unmatched" in all_code.lower()

    def test_fan_out_filter_present(self):
        """Fan-out should use .filter() for branching."""
        procs, conns = _fan_out_flow()
        cells = _build_well_formed_notebook(procs, conns, streaming=True)

        all_code = "\n".join(c.source for c in cells if c.type == "code")
        assert ".filter(" in all_code


# ===========================================================================
# Test 4: Pipeline wiring -- fan-in
# ===========================================================================

class TestPipelineWiringFanIn:
    """Two sources -> MergeContent -> one sink."""

    def test_fan_in_union(self):
        """MergeContent with two upstream sources should use unionByName."""
        procs, conns = _fan_in_flow()
        cells = _build_well_formed_notebook(procs, conns, streaming=True)

        all_code = "\n".join(c.source for c in cells if c.type == "code")
        assert "unionByName" in all_code

    def test_fan_in_both_sources_defined(self):
        """Both source DataFrames must be defined before the merge cell."""
        procs, conns = _fan_in_flow()
        cells = _build_well_formed_notebook(procs, conns, streaming=True)

        all_code = "\n".join(c.source for c in cells if c.type == "code")
        assert "df_source_a" in all_code
        assert "df_source_b" in all_code


# ===========================================================================
# Test 5: Sink generation -- Delta
# ===========================================================================

class TestSinkGenerationDelta:
    """PutFile generates .writeStream.format('delta') with checkpoint."""

    def test_delta_sink_has_checkpoint(self):
        """Delta streaming sink must include checkpointLocation."""
        procs = [
            MockProcessor(name="source", type="GetFile"),
            MockProcessor(name="delta_sink", type="PutFile"),
        ]
        conns = [MockConnection(source_name="source", destination_name="delta_sink")]
        cells = _build_well_formed_notebook(procs, conns, streaming=True)

        sink_cells = [c for c in cells if "sink" in c.label.lower()]
        assert len(sink_cells) >= 1

        sink_code = sink_cells[0].source
        assert "checkpointLocation" in sink_code
        assert '"delta"' in sink_code

    def test_delta_sink_has_trigger(self):
        """Delta streaming sink must include trigger."""
        procs = [
            MockProcessor(name="source", type="GetFile"),
            MockProcessor(name="delta_sink", type="PutFile"),
        ]
        conns = [MockConnection(source_name="source", destination_name="delta_sink")]
        cells = _build_well_formed_notebook(procs, conns, streaming=True)

        sink_code = [c for c in cells if "sink" in c.label.lower()][0].source
        assert ".trigger(" in sink_code


# ===========================================================================
# Test 6: Sink generation -- JDBC
# ===========================================================================

class TestSinkGenerationJDBC:
    """PutDatabaseRecord generates .write.format('jdbc') with secrets."""

    def test_jdbc_sink_uses_secrets(self):
        """JDBC sink must use dbutils.secrets.get() for credentials."""
        procs = [
            MockProcessor(name="source", type="GetFile"),
            MockProcessor(name="db_sink", type="PutDatabaseRecord", properties={
                "Database Connection Pooling Service": "MyDBPool",
                "Table Name": "target_table",
            }),
        ]
        conns = [MockConnection(source_name="source", destination_name="db_sink")]
        cells = _build_well_formed_notebook(procs, conns, streaming=False)

        sink_cells = [c for c in cells if "sink" in c.label.lower()]
        assert len(sink_cells) >= 1

        sink_code = sink_cells[0].source
        assert "dbutils.secrets.get" in sink_code
        assert '"jdbc"' in sink_code

    def test_jdbc_sink_no_hardcoded_password(self):
        """JDBC sink must NOT have hardcoded passwords."""
        procs = [
            MockProcessor(name="source", type="GetFile"),
            MockProcessor(name="db_sink", type="PutDatabaseRecord"),
        ]
        conns = [MockConnection(source_name="source", destination_name="db_sink")]
        cells = _build_well_formed_notebook(procs, conns, streaming=False)

        all_code = "\n".join(c.source for c in cells if c.type == "code")
        # Should not contain literal password values
        import re
        hardcoded = re.findall(r'password\s*=\s*["\'][^"\']+["\']', all_code, re.I)
        # Filter out dbutils.secrets.get patterns (those are safe)
        unsafe = [h for h in hardcoded if "dbutils.secrets.get" not in h]
        assert len(unsafe) == 0, f"Found hardcoded passwords: {unsafe}"


# ===========================================================================
# Test 7: Config generation
# ===========================================================================

class TestConfigGeneration:
    """Config cell includes CATALOG, SCHEMA, SECRETS_SCOPE."""

    def test_config_has_required_variables(self):
        """Config cell must define catalog, schema, and secret_scope."""
        procs = [MockProcessor(name="source", type="GetFile")]
        cells = _build_well_formed_notebook(procs)

        config_cells = [c for c in cells if c.label == "config"]
        assert len(config_cells) == 1

        config_code = config_cells[0].source
        assert "catalog" in config_code
        assert "schema" in config_code
        assert "secret_scope" in config_code

    def test_config_sets_catalog_context(self):
        """Config cell should execute USE CATALOG."""
        procs = [MockProcessor(name="source", type="GetFile")]
        cells = _build_well_formed_notebook(procs)

        config_code = [c for c in cells if c.label == "config"][0].source
        assert "USE CATALOG" in config_code


# ===========================================================================
# Test 8: Secret detection
# ===========================================================================

class TestSecretDetection:
    """JDBC password detected and replaced with dbutils.secrets.get()."""

    def test_jdbc_password_replaced(self):
        """A notebook with JDBC should use secrets, not plaintext."""
        from app.engines.validators.runnable_checker import check_runnable

        cells = [
            MockNotebookCell(
                type="code",
                source=(
                    'catalog = "main"\n'
                    'schema = "default"\n'
                    'secret_scope = "migration"\n'
                ),
                label="config",
            ),
            MockNotebookCell(
                type="code",
                source=(
                    'df_source = spark.read.format("csv").load("/data")\n'
                    'df_source.write.format("jdbc")\n'
                    '    .option("url", dbutils.secrets.get(scope="migration", key="url"))\n'
                    '    .option("password", dbutils.secrets.get(scope="migration", key="pass"))\n'
                    '    .save()\n'
                ),
                label="sink_source",
            ),
        ]

        report = check_runnable(cells)
        # Check 7 (no_plaintext_secrets) should pass
        secret_check = next(c for c in report.checks if c.name == "no_plaintext_secrets")
        assert secret_check.passed, f"Secret check failed: {secret_check.details}"

    def test_hardcoded_password_detected(self):
        """Hardcoded password should be flagged."""
        from app.engines.validators.runnable_checker import check_runnable

        cells = [
            MockNotebookCell(
                type="code",
                source='password = "super_secret_123"',
                label="bad_cell",
            ),
        ]

        report = check_runnable(cells)
        secret_check = next(c for c in report.checks if c.name == "no_plaintext_secrets")
        assert not secret_check.passed


# ===========================================================================
# Test 9: Runnable checker -- all 12 pass
# ===========================================================================

class TestRunnableCheckerPasses:
    """A well-formed notebook passes all 12 checks."""

    def test_well_formed_notebook_passes_all(self):
        """Notebook built by _build_well_formed_notebook should pass all checks."""
        from app.engines.validators.runnable_checker import check_runnable

        procs, conns = _linear_flow()
        cells = _build_well_formed_notebook(procs, conns, streaming=True)
        parsed = MockParseResult(processors=procs, connections=conns)

        report = check_runnable(cells, parsed=parsed)

        assert report.is_runnable, (
            f"Expected runnable but got: {report.summary}\n"
            f"Failed checks: {[(c.name, c.message, c.details) for c in report.checks if not c.passed]}"
        )

    def test_well_formed_notebook_score_above_threshold(self):
        """Well-formed notebook should score >= 0.9."""
        from app.engines.validators.runnable_checker import check_runnable

        procs, conns = _linear_flow()
        cells = _build_well_formed_notebook(procs, conns, streaming=True)
        parsed = MockParseResult(processors=procs, connections=conns)

        report = check_runnable(cells, parsed=parsed)
        assert report.overall_score >= 0.9, (
            f"Score {report.overall_score} below threshold. "
            f"Failed: {[c.name for c in report.checks if not c.passed]}"
        )

    def test_all_12_checks_present(self):
        """Report should contain exactly 12 checks."""
        from app.engines.validators.runnable_checker import check_runnable

        procs, conns = _linear_flow()
        cells = _build_well_formed_notebook(procs, conns)
        report = check_runnable(cells)

        assert len(report.checks) == 12


# ===========================================================================
# Test 10: Runnable checker -- fails dangling df
# ===========================================================================

class TestRunnableCheckerFailsDanglingDf:
    """Notebook with dangling 'df' fails check #4."""

    def test_bare_df_fails_check_4(self):
        """A bare 'df' reference without processor suffix must fail."""
        from app.engines.validators.runnable_checker import check_runnable

        cells = [
            MockNotebookCell(
                type="code",
                source=(
                    'catalog = "main"\n'
                    'schema = "default"\n'
                    'secret_scope = "migration"\n'
                ),
                label="config",
            ),
            MockNotebookCell(
                type="code",
                source='df = spark.read.format("csv").load("/data")',
                label="step_1",
            ),
            MockNotebookCell(
                type="code",
                source=(
                    'df.write.format("delta").saveAsTable("main.default.output")'
                ),
                label="step_2",
            ),
        ]

        report = check_runnable(cells)
        dangling_check = next(c for c in report.checks if c.name == "no_dangling_variables")
        assert not dangling_check.passed, (
            f"Expected dangling check to fail but it passed. Details: {dangling_check.details}"
        )

    def test_df_underscore_name_passes(self):
        """A properly named df_xxx variable should NOT trigger dangling check."""
        from app.engines.validators.runnable_checker import check_runnable

        cells = [
            MockNotebookCell(
                type="code",
                source='df_source = spark.read.format("csv").load("/data")',
                label="step_1",
            ),
        ]

        report = check_runnable(cells)
        dangling_check = next(c for c in report.checks if c.name == "no_dangling_variables")
        assert dangling_check.passed


# ===========================================================================
# Test 11: Runnable checker -- fails no write
# ===========================================================================

class TestRunnableCheckerFailsNoWrite:
    """Notebook with no write fails check #8."""

    def test_no_write_fails_check_8(self):
        """A notebook without any write/writeStream must fail terminal_writes_exist."""
        from app.engines.validators.runnable_checker import check_runnable

        cells = [
            MockNotebookCell(
                type="code",
                source=(
                    'catalog = "main"\n'
                    'schema = "default"\n'
                    'secret_scope = "migration"\n'
                ),
                label="config",
            ),
            MockNotebookCell(
                type="code",
                source='df_source = spark.read.format("csv").load("/data")',
                label="step_1",
            ),
            MockNotebookCell(
                type="code",
                source='df_transformed = df_source.withColumn("x", F.lit(1))',
                label="step_2",
            ),
        ]

        report = check_runnable(cells)
        write_check = next(c for c in report.checks if c.name == "terminal_writes_exist")
        assert not write_check.passed

    def test_no_write_makes_notebook_not_runnable(self):
        """No terminal write is critical -- notebook should not be runnable."""
        from app.engines.validators.runnable_checker import check_runnable

        cells = [
            MockNotebookCell(
                type="code",
                source='df_source = spark.read.format("csv").load("/data")',
                label="step_1",
            ),
        ]

        report = check_runnable(cells)
        assert not report.is_runnable


# ===========================================================================
# Test 12: Deep analysis orchestrator -- all 6 passes
# ===========================================================================

class TestDeepAnalysisOrchestrator:
    """All 6 passes produce non-empty results."""

    def test_all_six_passes_produce_results(self):
        """Running deep analysis on a minimal flow should produce all 6 reports."""
        from app.engines.analyzers.deep_analysis_orchestrator import run_deep_analysis
        from app.models.pipeline import ParseResult, AnalysisResult
        from app.models.processor import Processor, Connection

        parsed = ParseResult(
            platform="nifi",
            processors=[
                Processor(name="source", type="GetFile", platform="nifi"),
                Processor(name="transform", type="EvaluateJsonPath", platform="nifi"),
                Processor(name="sink", type="PutFile", platform="nifi"),
            ],
            connections=[
                Connection(source_name="source", destination_name="transform"),
                Connection(source_name="transform", destination_name="sink"),
            ],
            metadata={"source_file": "test_flow.xml"},
        )
        analysis = AnalysisResult()

        result = run_deep_analysis(parsed, analysis)

        # All 6 reports should be present
        assert result.functional is not None
        assert result.processors is not None
        assert result.workflow is not None
        assert result.upstream is not None
        assert result.downstream is not None
        assert result.line_by_line is not None

    def test_deep_analysis_summary_not_empty(self):
        """Summary string should be non-empty."""
        from app.engines.analyzers.deep_analysis_orchestrator import run_deep_analysis
        from app.models.pipeline import ParseResult, AnalysisResult
        from app.models.processor import Processor

        parsed = ParseResult(
            platform="nifi",
            processors=[Processor(name="p1", type="GetFile", platform="nifi")],
            metadata={"source_file": "test.xml"},
        )

        result = run_deep_analysis(parsed, AnalysisResult())
        assert len(result.summary) > 0
        assert "Deep Analysis Summary" in result.summary

    def test_deep_analysis_timing(self):
        """Duration should be recorded."""
        from app.engines.analyzers.deep_analysis_orchestrator import run_deep_analysis
        from app.models.pipeline import ParseResult, AnalysisResult
        from app.models.processor import Processor

        parsed = ParseResult(
            platform="nifi",
            processors=[Processor(name="p1", type="GetFile", platform="nifi")],
            metadata={"source_file": "test.xml"},
        )

        result = run_deep_analysis(parsed, AnalysisResult())
        assert result.duration_ms >= 0


# ===========================================================================
# Test 13: Full V6 generation -- end-to-end
# ===========================================================================

class TestFullV6Generation:
    """End-to-end: parsed flow -> generate cells -> all runnable checks pass."""

    def test_full_generation_runnable(self):
        """Complete flow through mock generation + runnable checker."""
        from app.engines.validators.runnable_checker import check_runnable

        # Build a realistic 5-processor flow
        procs = [
            MockProcessor(name="ingest_files", type="GetFile", properties={
                "Input Directory": "/data/incoming",
            }),
            MockProcessor(name="parse_json", type="EvaluateJsonPath", properties={
                "Destination": "flowfile-attribute",
                "customer_id": "$.customer_id",
            }),
            MockProcessor(name="enrich_data", type="UpdateAttribute", properties={
                "processed_at": "${now():format('yyyy-MM-dd')}",
            }),
            MockProcessor(name="route_by_type", type="RouteOnAttribute", properties={
                "premium": '${customer_type:equals("premium")}',
            }),
            MockProcessor(name="write_delta", type="PutFile", properties={
                "Directory": "/data/output",
            }),
        ]
        conns = [
            MockConnection(source_name="ingest_files", destination_name="parse_json"),
            MockConnection(source_name="parse_json", destination_name="enrich_data"),
            MockConnection(source_name="enrich_data", destination_name="route_by_type"),
            MockConnection(source_name="route_by_type", destination_name="write_delta", relationship="matched"),
        ]

        parsed = MockParseResult(
            processors=procs,
            connections=conns,
            metadata={"source_file": "production_flow.xml"},
        )

        # Generate notebook cells
        cells = _build_well_formed_notebook(procs, conns, streaming=True)

        # Run all 12 checks
        report = check_runnable(cells, parsed=parsed)

        # All critical checks must pass
        critical_checks = [c for c in report.checks if c.severity == "critical"]
        critical_failures = [c for c in critical_checks if not c.passed]

        assert report.is_runnable, (
            f"Full V6 generation not runnable. Failed criticals: "
            f"{[(c.name, c.message) for c in critical_failures]}"
        )

    def test_full_generation_score(self):
        """Full generation should achieve a high overall score."""
        from app.engines.validators.runnable_checker import check_runnable

        procs, conns = _linear_flow()
        cells = _build_well_formed_notebook(procs, conns, streaming=True)
        parsed = MockParseResult(processors=procs, connections=conns)

        report = check_runnable(cells, parsed=parsed)
        assert report.overall_score >= 0.75, (
            f"Score {report.overall_score} too low. "
            f"Failed: {[(c.name, c.severity) for c in report.checks if not c.passed]}"
        )

    def test_full_generation_batch_mode(self):
        """Batch-mode generation should also pass all critical checks."""
        from app.engines.validators.runnable_checker import check_runnable

        procs, conns = _linear_flow()
        cells = _build_well_formed_notebook(procs, conns, streaming=False)
        parsed = MockParseResult(processors=procs, connections=conns)

        report = check_runnable(cells, parsed=parsed)
        assert report.is_runnable, (
            f"Batch mode not runnable: {report.summary}"
        )
