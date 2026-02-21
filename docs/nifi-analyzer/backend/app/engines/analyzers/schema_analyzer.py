"""Schema inference & evolution analyzer.

Detects NiFi Record Readers/Writers in controller services, injects Auto Loader
schema evolution configuration for schema-less data, and generates StructType
definitions from NiFi record schemas when available.
"""

import re

from app.models.pipeline import ParseResult
from app.models.processor import ControllerService, Processor

# Record Reader/Writer controller service types
_RECORD_READER_RE = re.compile(r"(Reader|RecordReader|CSVReader|JsonTreeReader|AvroReader|"
                                r"JsonPathReader|XMLReader|GrokReader|SyslogReader|"
                                r"Syslog5424Reader|CEFReader)", re.I)
_RECORD_WRITER_RE = re.compile(r"(Writer|RecordWriter|CSVRecordSetWriter|JsonRecordSetWriter|"
                                r"AvroRecordSetWriter|XMLRecordSetWriter|FreeFormTextRecordSetWriter|"
                                r"ParquetRecordSetWriter)", re.I)

# Schema-related properties in NiFi controller services
_SCHEMA_ACCESS_PROP = "Schema Access Strategy"
_SCHEMA_TEXT_PROP = "Schema Text"
_SCHEMA_NAME_PROP = "Schema Name"
_SCHEMA_REGISTRY_PROP = "Schema Registry"

# NiFi Avro schema type to PySpark type mapping
_AVRO_TO_SPARK_TYPE = {
    "string": "StringType()",
    "int": "IntegerType()",
    "long": "LongType()",
    "float": "FloatType()",
    "double": "DoubleType()",
    "boolean": "BooleanType()",
    "bytes": "BinaryType()",
    "null": "NullType()",
}

# Processors that operate on records (need schema awareness)
_RECORD_PROCESSORS = re.compile(
    r"(ConvertRecord|QueryRecord|LookupRecord|UpdateRecord|ValidateRecord|"
    r"SplitRecord|MergeRecord|PartitionRecord|PublishKafkaRecord|"
    r"ConsumeKafkaRecord|PutDatabaseRecord|ForkRecord)",
    re.I,
)


def analyze_schemas(parse_result: ParseResult) -> dict:
    """Analyze schema inference and evolution requirements.

    Returns:
        {
            "record_services": [...],
            "schema_definitions": [...],
            "schema_evolution_configs": [...],
            "processors_needing_schema": [...],
            "summary": {...},
        }
    """
    controllers = parse_result.controller_services
    processors = parse_result.processors

    # Identify record reader/writer services
    record_services = _detect_record_services(controllers)

    # Build map of services to their schemas
    service_schemas = _extract_service_schemas(controllers)

    # Identify processors that need schema awareness
    schema_processors = _detect_schema_processors(processors, controllers)

    # Generate schema definitions from available NiFi schemas
    schema_defs = _generate_schema_definitions(service_schemas)

    # Generate Auto Loader schema evolution configs for schema-less sources
    evolution_configs = _generate_evolution_configs(processors, controllers, service_schemas)

    return {
        "record_services": record_services,
        "schema_definitions": schema_defs,
        "schema_evolution_configs": evolution_configs,
        "processors_needing_schema": schema_processors,
        "summary": {
            "record_readers": sum(1 for s in record_services if s["role"] == "reader"),
            "record_writers": sum(1 for s in record_services if s["role"] == "writer"),
            "schemas_defined": len(schema_defs),
            "schema_less_sources": len(evolution_configs),
        },
    }


def _detect_record_services(controllers: list[ControllerService]) -> list[dict]:
    """Detect Record Reader/Writer controller services."""
    services = []
    for cs in controllers:
        role = None
        if _RECORD_READER_RE.search(cs.type):
            role = "reader"
        elif _RECORD_WRITER_RE.search(cs.type):
            role = "writer"
        if role:
            schema_strategy = cs.properties.get(_SCHEMA_ACCESS_PROP, "")
            services.append({
                "name": cs.name,
                "type": cs.type,
                "role": role,
                "schema_strategy": schema_strategy,
                "has_explicit_schema": bool(cs.properties.get(_SCHEMA_TEXT_PROP, "")),
                "schema_registry": cs.properties.get(_SCHEMA_REGISTRY_PROP, ""),
            })
    return services


def _extract_service_schemas(controllers: list[ControllerService]) -> dict[str, str]:
    """Extract schema text from controller services that define schemas."""
    schemas: dict[str, str] = {}
    for cs in controllers:
        schema_text = cs.properties.get(_SCHEMA_TEXT_PROP, "")
        if schema_text and schema_text.strip():
            schemas[cs.name] = schema_text.strip()
    return schemas


def _detect_schema_processors(
    processors: list[Processor],
    controllers: list[ControllerService],
) -> list[dict]:
    """Detect processors that operate on records and need schema configuration."""
    cs_names = {cs.name for cs in controllers}
    result = []

    for p in processors:
        if not _RECORD_PROCESSORS.search(p.type):
            continue

        reader_service = ""
        writer_service = ""
        for key, val in p.properties.items():
            if isinstance(val, str) and val in cs_names:
                if "reader" in key.lower() or "record-reader" in key.lower():
                    reader_service = val
                elif "writer" in key.lower() or "record-writer" in key.lower():
                    writer_service = val

        result.append({
            "processor": p.name,
            "type": p.type,
            "group": p.group,
            "reader_service": reader_service,
            "writer_service": writer_service,
        })

    return result


def _generate_schema_definitions(service_schemas: dict[str, str]) -> list[dict]:
    """Generate PySpark StructType definitions from NiFi Avro/JSON schemas."""
    definitions = []

    for service_name, schema_text in service_schemas.items():
        safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", service_name).strip("_").lower()
        struct_code = _avro_schema_to_structtype(schema_text, safe_name)

        definitions.append({
            "service_name": service_name,
            "original_schema": schema_text[:500],  # truncate for response size
            "pyspark_schema": struct_code,
            "variable_name": f"{safe_name}_schema",
        })

    return definitions


def _avro_schema_to_structtype(schema_text: str, name: str) -> str:
    """Best-effort conversion of Avro-style schema text to PySpark StructType."""
    # Try to parse as JSON-like Avro schema
    fields: list[str] = []

    # Match field patterns like {"name": "x", "type": "string"}
    for m in re.finditer(
        r'"name"\s*:\s*"(\w+)"[^}]*"type"\s*:\s*"(\w+)"',
        schema_text,
    ):
        field_name = m.group(1)
        field_type = m.group(2)
        spark_type = _AVRO_TO_SPARK_TYPE.get(field_type, "StringType()")
        fields.append(f'    StructField("{field_name}", {spark_type}, True)')

    if fields:
        field_str = ",\n".join(fields)
        return f"{name}_schema = StructType([\n{field_str}\n])"

    # Fallback: generate a placeholder with schema inference
    return (
        f"# Schema for '{name}' — original NiFi schema could not be auto-converted\n"
        f"# Use Auto Loader schema inference or define manually:\n"
        f"# USER ACTION: replace placeholder path with actual sample data location\n"
        f'{name}_schema = spark.read.format("json").load("/Volumes/{{catalog}}/{{schema}}/sample_data").schema'
    )


def _generate_evolution_configs(
    processors: list[Processor],
    controllers: list[ControllerService],
    service_schemas: dict[str, str],
) -> list[dict]:
    """Generate Auto Loader schema evolution configs for schema-less sources."""
    configs = []

    # Build set of services with explicit schemas
    services_with_schema = set(service_schemas.keys())

    # Find source processors that use record readers without explicit schemas
    cs_map = {cs.name: cs for cs in controllers}

    for p in processors:
        if not p.type.startswith(("Get", "List", "Consume", "Fetch")):
            continue

        # Check if this processor references a reader service without a schema
        has_schema = False
        for val in p.properties.values():
            if isinstance(val, str) and val in services_with_schema:
                has_schema = True
                break

        if has_schema:
            continue

        safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", p.name).strip("_").lower()
        schema_path = f"/Volumes/{{catalog}}/{{schema}}/schemas/{safe_name}"

        code = (
            f"# Auto Loader schema evolution for '{p.name}' ({p.type})\n"
            f"# No explicit schema found — enable schema inference and evolution\n"
            f"df = (\n"
            f"    spark.readStream\n"
            f"    .format(\"cloudFiles\")\n"
            f"    .option(\"cloudFiles.format\", \"{_infer_format(p)}\")\n"
            f"    .option(\"cloudFiles.schemaLocation\", \"{schema_path}\")\n"
            f"    .option(\"cloudFiles.inferColumnTypes\", \"true\")\n"
            f"    .option(\"cloudFiles.schemaEvolutionMode\", \"addNewColumns\")\n"
            f"    .load(\"/Volumes/{{catalog}}/{{schema}}/landing/{safe_name}\")\n"
            f")"
        )

        configs.append({
            "processor": p.name,
            "type": p.type,
            "schema_location": schema_path,
            "code_snippet": code,
        })

    return configs


def _infer_format(p: Processor) -> str:
    """Infer data format from processor type and properties."""
    t = p.type.lower()
    if "json" in t:
        return "json"
    if "csv" in t:
        return "csv"
    if "avro" in t:
        return "avro"
    if "parquet" in t:
        return "parquet"
    return "json"
