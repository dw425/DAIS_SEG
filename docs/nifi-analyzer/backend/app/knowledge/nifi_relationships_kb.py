"""
NiFi Connection Relationship Knowledge Base -- 25+ relationship types.

Every NiFi processor relationship is cataloged with the processors that
use it, its semantic meaning, a conversion strategy tag, and a PySpark
code-pattern template that the code generator can emit.

Conversion Strategies
---------------------
pass_through   -- Data flows onward unchanged (the normal happy path).
filter         -- Rows that don't match a condition are routed here; use
                  ``df.filter()`` to split into matched/unmatched frames.
try_except     -- The relationship indicates an error; wrap in try/except
                  or use a ``withColumn`` that catches nulls / bad casts.
error_table    -- Write failed rows to a quarantine / error Delta table.
explode        -- One-to-many split; use ``df.select(F.explode(...))`` to
                  fan out child records.
union          -- Fan-in; ``df1.unionByName(df2)`` to merge streams.
branch         -- Conditional routing; use ``F.when(...).otherwise(...)``
                  or multiple filtered DataFrames.
retry          -- Transient failure; emit retry logic with exponential
                  backoff or dead-letter semantics.
discard        -- Relationship is informational only (e.g. ``original``
                  after a successful clone); no code needed.

Placeholders
------------
{df}             -- Source DataFrame variable name.
{condition}      -- Filter/branch condition expression.
{error_table}    -- Fully qualified error / quarantine table name.
{output_df}      -- Result DataFrame variable name.
{explode_col}    -- Column to explode.
{join_df}        -- DataFrame to join / union.

Usage
-----
>>> from app.knowledge.nifi_relationships_kb import lookup_relationship
>>> rel = lookup_relationship("failure")
>>> rel.conversion_strategy
'try_except'
>>> rel.pyspark_pattern
'try:\\n    {output_df} = <transform>({df})\\nexcept Exception as e:\\n    {output_df}_err = {df}.withColumn("_error", F.lit(str(e)))\\n    {output_df}_err.write.mode("append").saveAsTable("{error_table}")'
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class RelationshipDef:
    """Definition for a single NiFi connection relationship type."""

    name: str
    processors: list[str]  # which processors commonly use this relationship
    description: str
    conversion_strategy: str  # pass_through, filter, try_except, error_table, explode, union, branch, retry, discard
    pyspark_pattern: str  # code pattern template


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

RELATIONSHIP_KB: dict[str, RelationshipDef] = {}


def _register(rel: RelationshipDef) -> None:
    """Register a relationship definition in the knowledge base."""
    RELATIONSHIP_KB[rel.name] = rel


# ===================================================================
# CORE RELATIONSHIPS
# ===================================================================

_register(RelationshipDef(
    name="success",
    processors=[
        "ExecuteSQL", "ExecuteScript", "UpdateAttribute", "ReplaceText",
        "ConvertRecord", "PutDatabaseRecord", "PutFile", "PutS3Object",
        "PutAzureBlobStorage", "PutHDFS", "PutKafka", "PutKafkaRecord",
        "PublishKafka", "PublishKafkaRecord", "InvokeHTTP", "JoltTransformJSON",
        "TransformXml", "MergeContent", "SplitJson", "SplitText",
        "SplitRecord", "SplitAvro", "SplitXml", "RouteOnAttribute",
        "RouteOnContent", "EvaluateJsonPath", "EvaluateXPath",
        "EvaluateXQuery", "AttributeRollingWindow", "CompressContent",
        "EncryptContent", "FetchFile", "FetchS3Object", "FetchSFTP",
        "GenerateFlowFile", "GetFile", "GetSFTP", "GetS3Object",
        "ListFile", "ListS3", "ListSFTP", "ListenHTTP", "HandleHttpRequest",
        "HandleHttpResponse", "LookupAttribute", "LookupRecord",
        "QueryRecord", "ValidateRecord", "UpdateRecord", "ForkRecord",
        "PartitionRecord", "ConvertJSONToSQL", "PutSQL", "PutDatabaseRecord",
        "PutElasticsearchRecord", "PutMongo", "PutSolrRecord",
        "PutSplunk", "PutSyslog", "PutEmail", "PutSlack",
        "PutTCP", "PutUDP", "ExecuteProcess", "ExecuteStreamCommand",
        "ExtractText", "HashAttribute", "HashContent", "IdentifyMimeType",
        "LogAttribute", "LogMessage", "ModifyBytes", "ReplaceTextWithMapping",
        "RouteText", "UnpackContent", "Wait", "Notify",
    ],
    description="The FlowFile was processed successfully. This is the primary happy-path relationship present on virtually every NiFi processor.",
    conversion_strategy="pass_through",
    pyspark_pattern="""{output_df} = <transform>({df})""",
))

_register(RelationshipDef(
    name="failure",
    processors=[
        "ExecuteSQL", "ExecuteScript", "PutDatabaseRecord", "PutFile",
        "PutS3Object", "PutAzureBlobStorage", "PutHDFS", "PutKafka",
        "PublishKafka", "PublishKafkaRecord", "InvokeHTTP", "JoltTransformJSON",
        "ConvertRecord", "ReplaceText", "EvaluateJsonPath", "EvaluateXPath",
        "SplitJson", "SplitText", "SplitRecord", "MergeContent",
        "CompressContent", "EncryptContent", "FetchFile", "FetchS3Object",
        "FetchSFTP", "LookupAttribute", "LookupRecord", "QueryRecord",
        "ValidateRecord", "UpdateRecord", "ForkRecord", "PartitionRecord",
        "ConvertJSONToSQL", "PutSQL", "PutMongo", "PutElasticsearchRecord",
        "PutSolrRecord", "PutSplunk", "ExtractText", "HashContent",
        "ExecuteProcess", "ExecuteStreamCommand", "TransformXml",
        "UnpackContent", "Wait", "Notify",
    ],
    description="The FlowFile could not be processed due to an error (exception, malformed data, connectivity issue, etc.). Failures should be caught and routed to an error-handling path.",
    conversion_strategy="try_except",
    pyspark_pattern="""try:
    {output_df} = <transform>({df})
except Exception as e:
    {output_df}_err = {df}.withColumn("_error", F.lit(str(e)))
    {output_df}_err.write.mode("append").saveAsTable("{error_table}")""",
))

_register(RelationshipDef(
    name="original",
    processors=[
        "SplitJson", "SplitText", "SplitRecord", "SplitAvro", "SplitXml",
        "SplitContent", "ForkRecord", "DuplicateFlowFile", "MergeContent",
        "UnpackContent", "SegmentContent", "ReplaceText", "JoltTransformJSON",
    ],
    description="The original (unsplit/unmodified) FlowFile is emitted alongside the derived FlowFiles. Typically used for auditing or downstream joins.",
    conversion_strategy="discard",
    pyspark_pattern="""# 'original' relationship -- the source DataFrame '{df}' is still available
# No additional code needed unless auditing is required:
# {df}.write.mode("append").saveAsTable("{error_table}_audit")""",
))

# ===================================================================
# FILTER / ROUTING RELATIONSHIPS
# ===================================================================

_register(RelationshipDef(
    name="matched",
    processors=[
        "RouteOnAttribute", "RouteOnContent", "RouteText", "LookupRecord",
        "LookupAttribute", "ScanAttribute", "ScanContent", "DetectDuplicate",
        "QueryRecord", "PartitionRecord",
    ],
    description="The FlowFile satisfied the matching condition (attribute expression, content regex, lookup hit, etc.).",
    conversion_strategy="filter",
    pyspark_pattern="""{output_df}_matched = {df}.filter({condition})""",
))

_register(RelationshipDef(
    name="unmatched",
    processors=[
        "RouteOnAttribute", "RouteOnContent", "RouteText", "LookupRecord",
        "LookupAttribute", "ScanAttribute", "ScanContent", "DetectDuplicate",
        "QueryRecord", "PartitionRecord", "EvaluateJsonPath", "EvaluateXPath",
    ],
    description="The FlowFile did not satisfy any matching condition. Represents the 'else' / default branch.",
    conversion_strategy="filter",
    pyspark_pattern="""{output_df}_unmatched = {df}.filter(~({condition}))""",
))

# ===================================================================
# SPLIT / EXPLODE RELATIONSHIPS
# ===================================================================

_register(RelationshipDef(
    name="split",
    processors=[
        "SplitJson", "SplitText", "SplitRecord", "SplitAvro", "SplitXml",
        "SplitContent", "SegmentContent", "ForkRecord", "UnpackContent",
    ],
    description="Individual child FlowFiles produced by splitting the original. Each split segment becomes a separate FlowFile.",
    conversion_strategy="explode",
    pyspark_pattern="""{output_df} = {df}.select("*", F.explode(F.col("{explode_col}")).alias("_exploded"))""",
))

# ===================================================================
# MERGE / UNION RELATIONSHIPS
# ===================================================================

_register(RelationshipDef(
    name="merged",
    processors=["MergeContent", "MergeRecord"],
    description="The merged FlowFile that combines multiple input FlowFiles into one (by bin-packing, defragmenting, or concatenation).",
    conversion_strategy="union",
    pyspark_pattern="""{output_df} = {df}.unionByName({join_df}, allowMissingColumns=True)""",
))

# ===================================================================
# HTTP / REST RELATIONSHIPS
# ===================================================================

_register(RelationshipDef(
    name="response",
    processors=["InvokeHTTP", "HandleHttpResponse", "PostHTTP"],
    description="The HTTP response body received from the remote endpoint.",
    conversion_strategy="pass_through",
    pyspark_pattern="""{output_df} = {df}.withColumn("_http_response", F.lit(<response_body>))""",
))

_register(RelationshipDef(
    name="request",
    processors=["HandleHttpRequest", "ListenHTTP"],
    description="The incoming HTTP request FlowFile (from HandleHttpRequest or ListenHTTP).",
    conversion_strategy="pass_through",
    pyspark_pattern="""{output_df} = spark.read.json(<request_payload_path>)""",
))

_register(RelationshipDef(
    name="no_retry",
    processors=["InvokeHTTP", "PostHTTP"],
    description="The request failed with a non-retryable HTTP status code (4xx client error). Do not retry; route to error handling.",
    conversion_strategy="error_table",
    pyspark_pattern="""{output_df}_no_retry = {df}.withColumn("_error", F.lit("Non-retryable HTTP error"))
{output_df}_no_retry.write.mode("append").saveAsTable("{error_table}")""",
))

# ===================================================================
# RETRY / TRANSIENT FAILURE RELATIONSHIPS
# ===================================================================

_register(RelationshipDef(
    name="retry",
    processors=["InvokeHTTP", "PostHTTP", "PutS3Object", "PutAzureBlobStorage",
                 "PutKafka", "PublishKafka", "PutDatabaseRecord", "PutSQL",
                 "PutElasticsearchRecord", "PutMongo", "FetchS3Object"],
    description="The operation failed with a transient/retryable error (5xx server error, timeout, throttle). The FlowFile should be retried.",
    conversion_strategy="retry",
    pyspark_pattern="""import tenacity

@tenacity.retry(
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_exponential(multiplier=1, min=2, max=30),
    reraise=True,
)
def _retry_operation({df}):
    return <transform>({df})

try:
    {output_df} = _retry_operation({df})
except Exception as e:
    {output_df}_dead_letter = {df}.withColumn("_error", F.lit(str(e)))
    {output_df}_dead_letter.write.mode("append").saveAsTable("{error_table}")""",
))

_register(RelationshipDef(
    name="retries_exceeded",
    processors=["RetryFlowFile", "InvokeHTTP"],
    description="The FlowFile has exhausted all retry attempts. Route to permanent error handling / dead-letter queue.",
    conversion_strategy="error_table",
    pyspark_pattern="""{output_df}_dead = {df}.withColumn("_error", F.lit("Retries exceeded"))
{output_df}_dead.write.mode("append").saveAsTable("{error_table}")""",
))

_register(RelationshipDef(
    name="penalized",
    processors=["RetryFlowFile"],
    description="The FlowFile is being penalized (delayed) before its next retry attempt.",
    conversion_strategy="retry",
    pyspark_pattern="""import time
time.sleep({penalty_seconds})  # Simulate NiFi penalty period
{output_df} = <transform>({df})""",
))

# ===================================================================
# VALIDATION RELATIONSHIPS
# ===================================================================

_register(RelationshipDef(
    name="valid",
    processors=["ValidateRecord", "ValidateXml", "ValidateCsv"],
    description="The FlowFile passed validation (schema conformance, data-quality checks).",
    conversion_strategy="filter",
    pyspark_pattern="""{output_df}_valid = {df}.filter({condition})""",
))

_register(RelationshipDef(
    name="invalid",
    processors=["ValidateRecord", "ValidateXml", "ValidateCsv"],
    description="The FlowFile failed validation. Bad rows should be quarantined.",
    conversion_strategy="error_table",
    pyspark_pattern="""{output_df}_invalid = {df}.filter(~({condition}))
{output_df}_invalid.write.mode("append").saveAsTable("{error_table}")""",
))

_register(RelationshipDef(
    name="compatible",
    processors=["ValidateRecord"],
    description="The record's schema is compatible with the expected schema (possibly with coercion).",
    conversion_strategy="pass_through",
    pyspark_pattern="""{output_df}_compatible = {df}  # Schema is compatible; proceed""",
))

_register(RelationshipDef(
    name="incompatible",
    processors=["ValidateRecord"],
    description="The record's schema is incompatible with the expected schema and cannot be coerced.",
    conversion_strategy="error_table",
    pyspark_pattern="""{output_df}_incompatible = {df}.withColumn("_error", F.lit("Schema incompatible"))
{output_df}_incompatible.write.mode("append").saveAsTable("{error_table}")""",
))

# ===================================================================
# QUERY / RESULTS RELATIONSHIPS
# ===================================================================

_register(RelationshipDef(
    name="results",
    processors=["ExecuteSQL", "ExecuteSQLRecord", "QueryDatabaseTable",
                 "GenerateTableFetch", "QueryRecord", "QueryElasticsearch"],
    description="The query result set (rows returned by a SQL query or search). May be empty if the query returned no rows.",
    conversion_strategy="pass_through",
    pyspark_pattern="""{output_df} = spark.read.format("jdbc").options(**jdbc_opts).option("query", sql).load()""",
))

# ===================================================================
# WAIT / NOTIFY / SIGNAL RELATIONSHIPS
# ===================================================================

_register(RelationshipDef(
    name="wait",
    processors=["Wait"],
    description="The FlowFile is waiting for a signal (from the Notify processor). It will be held until the signal arrives or the timeout expires.",
    conversion_strategy="retry",
    pyspark_pattern="""# Polling loop to wait for signal
import time
_signal_received = False
for _attempt in range({max_wait_attempts}):
    _signal_received = <check_signal>()
    if _signal_received:
        break
    time.sleep({poll_interval_seconds})

if _signal_received:
    {output_df} = <transform>({df})
else:
    {output_df}_timeout = {df}.withColumn("_error", F.lit("Wait timeout exceeded"))
    {output_df}_timeout.write.mode("append").saveAsTable("{error_table}")""",
))

_register(RelationshipDef(
    name="expired",
    processors=["Wait"],
    description="The FlowFile's wait time has expired without receiving the expected signal.",
    conversion_strategy="error_table",
    pyspark_pattern="""{output_df}_expired = {df}.withColumn("_error", F.lit("Wait expired -- signal not received"))
{output_df}_expired.write.mode("append").saveAsTable("{error_table}")""",
))

# ===================================================================
# DEDUPLICATION RELATIONSHIPS
# ===================================================================

_register(RelationshipDef(
    name="duplicate",
    processors=["DetectDuplicate", "DeduplicateRecord"],
    description="The FlowFile is a duplicate of a previously seen FlowFile (based on cache key / hash).",
    conversion_strategy="filter",
    pyspark_pattern="""{output_df}_duplicates = {df}.exceptAll({df}.dropDuplicates([{dedup_columns}]))""",
))

_register(RelationshipDef(
    name="non_duplicate",
    processors=["DetectDuplicate", "DeduplicateRecord"],
    description="The FlowFile is not a duplicate; it is the first occurrence of its key.",
    conversion_strategy="filter",
    pyspark_pattern="""{output_df}_unique = {df}.dropDuplicates([{dedup_columns}])""",
))

# ===================================================================
# COMMUNICATION FAILURE RELATIONSHIPS
# ===================================================================

_register(RelationshipDef(
    name="comms_failure",
    processors=["InvokeHTTP", "PostHTTP", "PutS3Object", "FetchS3Object",
                 "PutAzureBlobStorage", "FetchAzureBlobStorage",
                 "PutKafka", "PublishKafka", "PutSFTP", "FetchSFTP",
                 "PutSQL", "PutDatabaseRecord", "PutMongo",
                 "PutElasticsearchRecord"],
    description="A communication failure occurred (network timeout, DNS resolution failure, connection refused). Distinct from application-level errors.",
    conversion_strategy="retry",
    pyspark_pattern="""import tenacity

@tenacity.retry(
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_exponential(multiplier=1, min=5, max=60),
    retry=tenacity.retry_if_exception_type((ConnectionError, TimeoutError)),
    reraise=True,
)
def _retry_comms({df}):
    return <transform>({df})

try:
    {output_df} = _retry_comms({df})
except Exception as e:
    {output_df}_comms_err = {df}.withColumn("_error", F.lit(f"Comms failure: {{e}}"))
    {output_df}_comms_err.write.mode("append").saveAsTable("{error_table}")""",
))

# ===================================================================
# MONITORING / ALERTING RELATIONSHIPS
# ===================================================================

_register(RelationshipDef(
    name="inactive",
    processors=["MonitorActivity"],
    description="No FlowFiles have been seen for the configured inactivity threshold. Fires once when activity stops.",
    conversion_strategy="branch",
    pyspark_pattern="""# Check for inactivity: if no new rows since last run, trigger alert
_last_count = spark.sql("SELECT COUNT(*) as cnt FROM {source_table} WHERE _ingest_ts > '{last_run_ts}'").first().cnt
if _last_count == 0:
    print("ALERT: No activity detected since {last_run_ts}")
    # Optionally write to alerting system""",
))

_register(RelationshipDef(
    name="activity_restored",
    processors=["MonitorActivity"],
    description="Activity has resumed after a period of inactivity. Fires once when the first FlowFile is seen after inactivity.",
    conversion_strategy="pass_through",
    pyspark_pattern="""# Activity restored -- resume normal processing
{output_df} = <transform>({df})
print("INFO: Activity restored -- processing resumed")""",
))

# ===================================================================
# ROUTING RELATIONSHIPS (dynamic / named)
# ===================================================================

_register(RelationshipDef(
    name="route_on_attribute_dynamic",
    processors=["RouteOnAttribute"],
    description="Dynamic relationship created for each user-defined route in RouteOnAttribute. Each route evaluates an NiFi Expression Language condition.",
    conversion_strategy="branch",
    pyspark_pattern="""# RouteOnAttribute: create one filtered DataFrame per route
{output_df}_route1 = {df}.filter({condition_1})
{output_df}_route2 = {df}.filter({condition_2})
# ... one filter per NiFi route
{output_df}_unmatched = {df}.filter(~({condition_1}) & ~({condition_2}))""",
))

_register(RelationshipDef(
    name="route_on_content_dynamic",
    processors=["RouteOnContent"],
    description="Dynamic relationship created for each content-matching rule in RouteOnContent.",
    conversion_strategy="branch",
    pyspark_pattern="""# RouteOnContent: filter by content/column patterns
{output_df}_route1 = {df}.filter(F.col("_content").rlike({pattern_1}))
{output_df}_route2 = {df}.filter(F.col("_content").rlike({pattern_2}))
{output_df}_unmatched = {df}.filter(~F.col("_content").rlike({pattern_1}) & ~F.col("_content").rlike({pattern_2}))""",
))

_register(RelationshipDef(
    name="query_record_dynamic",
    processors=["QueryRecord"],
    description="Dynamic relationship for each named SQL query in QueryRecord. Each query produces a separate output relationship.",
    conversion_strategy="branch",
    pyspark_pattern="""# QueryRecord: each named query becomes a separate DataFrame
{df}.createOrReplaceTempView("_input")
{output_df}_query1 = spark.sql("{sql_query_1}")
{output_df}_query2 = spark.sql("{sql_query_2}")""",
))

_register(RelationshipDef(
    name="partition_record_dynamic",
    processors=["PartitionRecord"],
    description="Dynamic relationship for each partition value in PartitionRecord. Records are grouped by the partition field value.",
    conversion_strategy="branch",
    pyspark_pattern="""# PartitionRecord: split DataFrame by partition column values
_partitions = {df}.select("{partition_col}").distinct().collect()
_partitioned_dfs = {{
    row["{partition_col}"]: {df}.filter(F.col("{partition_col}") == row["{partition_col}"])
    for row in _partitions
}}""",
))


# ===================================================================
# LOOKUP / HELPER FUNCTIONS
# ===================================================================

def lookup_relationship(name: str) -> Optional[RelationshipDef]:
    """Look up a relationship definition by name.

    Performs an exact match first, then a case-insensitive search, and
    finally a normalized search (replacing spaces/hyphens with underscores).

    Parameters
    ----------
    name:
        The relationship name (e.g. ``"failure"``, ``"comms.failure"``,
        ``"no retry"``).

    Returns
    -------
    RelationshipDef | None
        The matching definition, or ``None`` if not found.
    """
    # Exact match
    if name in RELATIONSHIP_KB:
        return RELATIONSHIP_KB[name]

    # Normalize: dots, hyphens, spaces -> underscores; lowercase
    normalized = name.replace(".", "_").replace("-", "_").replace(" ", "_").lower()
    for key, rel in RELATIONSHIP_KB.items():
        if key.lower() == normalized:
            return rel

    # Partial match (e.g. "comms failure" matches "comms_failure")
    for key, rel in RELATIONSHIP_KB.items():
        key_normalized = key.replace(".", "_").replace("-", "_").replace(" ", "_").lower()
        if key_normalized == normalized:
            return rel

    return None


def get_all_relationships() -> dict[str, RelationshipDef]:
    """Return the entire relationship knowledge base (defensive copy).

    Returns
    -------
    dict[str, RelationshipDef]
        A shallow copy keyed by relationship name.
    """
    return dict(RELATIONSHIP_KB)


def get_relationships_by_strategy(strategy: str) -> list[RelationshipDef]:
    """Return all relationships that use a given conversion strategy.

    Parameters
    ----------
    strategy:
        One of: ``pass_through``, ``filter``, ``try_except``, ``error_table``,
        ``explode``, ``union``, ``branch``, ``retry``, ``discard``.

    Returns
    -------
    list[RelationshipDef]
        Matching definitions sorted by name.
    """
    return sorted(
        [rel for rel in RELATIONSHIP_KB.values() if rel.conversion_strategy == strategy],
        key=lambda rel: rel.name,
    )


def get_relationships_for_processor(processor: str) -> list[RelationshipDef]:
    """Return all relationships available on a given processor.

    Parameters
    ----------
    processor:
        Processor short type (e.g. ``"InvokeHTTP"``).

    Returns
    -------
    list[RelationshipDef]
        All relationships that list the processor, sorted by name.
    """
    return sorted(
        [rel for rel in RELATIONSHIP_KB.values() if processor in rel.processors],
        key=lambda rel: rel.name,
    )


def get_conversion_strategies() -> list[str]:
    """Return a sorted list of all distinct conversion strategies.

    Returns
    -------
    list[str]
        Distinct strategy names in alphabetical order.
    """
    return sorted({rel.conversion_strategy for rel in RELATIONSHIP_KB.values()})


def search_relationships(query: str) -> list[RelationshipDef]:
    """Full-text search across relationship names, descriptions, and patterns.

    Parameters
    ----------
    query:
        Case-insensitive search term.

    Returns
    -------
    list[RelationshipDef]
        All matching definitions, sorted by name.
    """
    query_lower = query.lower()
    results = []
    for rel in RELATIONSHIP_KB.values():
        if (query_lower in rel.name.lower()
                or query_lower in rel.description.lower()
                or query_lower in rel.pyspark_pattern.lower()
                or query_lower in rel.conversion_strategy.lower()
                or any(query_lower in p.lower() for p in rel.processors)):
            results.append(rel)
    return sorted(results, key=lambda rel: rel.name)
