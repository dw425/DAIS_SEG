"""Site-to-Site to Delta Sharing translation analyzer.

Detects NiFi Remote Process Groups and Site-to-Site connections, generates Delta
Sharing configuration as the replacement, and produces Lakehouse Federation config
for cross-database queries.
"""

import re

from app.models.pipeline import ParseResult
from app.models.processor import Processor

# Remote Process Group and Site-to-Site indicators
_REMOTE_PG_RE = re.compile(r"(RemoteProcessGroup|Remote\s*Input\s*Port|Remote\s*Output\s*Port)", re.I)
_S2S_PROP_RE = re.compile(r"(Remote\s*URL|Transport\s*Protocol|Remote\s*NiFi|Site.to.Site)", re.I)

# Processors that indicate remote data transfer
_REMOTE_TRANSFER_PROCESSORS = {
    "InvokeHTTP", "PostHTTP", "GetHTTP",
    "PutS3Object", "GetS3Object",
    "PutHDFS", "GetHDFS",
    "PutSFTP", "GetSFTP",
}


def analyze_site_to_site(parse_result: ParseResult) -> dict:
    """Analyze Site-to-Site connections and generate Delta Sharing replacements.

    Returns:
        {
            "remote_connections": [...],
            "delta_sharing_configs": [...],
            "federation_configs": [...],
            "summary": {...},
        }
    """
    processors = parse_result.processors
    process_groups = parse_result.process_groups

    remote_connections = _detect_remote_connections(processors, process_groups)
    delta_sharing = _generate_delta_sharing_configs(remote_connections, parse_result)
    federation = _generate_federation_configs(remote_connections, parse_result)

    return {
        "remote_connections": remote_connections,
        "delta_sharing_configs": delta_sharing,
        "federation_configs": federation,
        "summary": {
            "remote_connection_count": len(remote_connections),
            "delta_shares_needed": len(delta_sharing),
            "federation_connections": len(federation),
        },
    }


def _detect_remote_connections(
    processors: list[Processor],
    process_groups: list,
) -> list[dict]:
    """Detect Remote Process Groups and Site-to-Site connections."""
    connections = []

    for p in processors:
        is_remote = False
        remote_url = ""
        transport = ""
        direction = "unknown"

        # Check processor type
        if _REMOTE_PG_RE.search(p.type):
            is_remote = True

        # Check properties for S2S configuration
        for key, val in p.properties.items():
            if not isinstance(val, str):
                continue
            if _S2S_PROP_RE.search(key):
                is_remote = True
                if "url" in key.lower():
                    remote_url = val
                elif "transport" in key.lower():
                    transport = val

        if not is_remote:
            continue

        # Determine direction
        if p.type.lower().startswith(("put", "publish", "send", "post")):
            direction = "outbound"
        elif p.type.lower().startswith(("get", "consume", "fetch", "listen")):
            direction = "inbound"
        elif "output" in p.type.lower():
            direction = "outbound"
        elif "input" in p.type.lower():
            direction = "inbound"

        connections.append({
            "processor": p.name,
            "type": p.type,
            "group": p.group,
            "remote_url": remote_url,
            "transport_protocol": transport or "HTTP",
            "direction": direction,
        })

    return connections


def _generate_delta_sharing_configs(
    remote_connections: list[dict],
    parse_result: ParseResult,
) -> list[dict]:
    """Generate Delta Sharing configurations to replace Site-to-Site."""
    configs = []

    for conn in remote_connections:
        safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", conn["processor"]).strip("_").lower()

        if conn["direction"] == "outbound":
            # Upstream: write to governed Delta table, share downstream
            code = (
                f"# Delta Sharing: Replace S2S outbound '{conn['processor']}'\n"
                f"# Step 1: Write to governed Delta table\n"
                f"(\n"
                f"    df.write\n"
                f"    .format(\"delta\")\n"
                f"    .mode(\"append\")\n"
                f"    .option(\"mergeSchema\", \"true\")\n"
                f"    .saveAsTable(f\"{{catalog}}.{{schema}}.{safe_name}_shared\")\n"
                f")\n"
                f"\n"
                f"# Step 2: Create Delta Share for downstream consumers\n"
                f"# Run once via SQL:\n"
                f"# CREATE SHARE IF NOT EXISTS {safe_name}_share;\n"
                f"# ALTER SHARE {safe_name}_share ADD TABLE {{catalog}}.{{schema}}.{safe_name}_shared;\n"
                f"# GRANT SELECT ON SHARE {safe_name}_share TO RECIPIENT downstream_consumer;"
            )
        else:
            # Downstream: read from Delta Share
            code = (
                f"# Delta Sharing: Replace S2S inbound '{conn['processor']}'\n"
                f"# Read from shared Delta table\n"
                f"df = (\n"
                f"    spark.read\n"
                f"    .format(\"deltaSharing\")\n"
                f"    .load(f\"{{catalog}}.{{schema}}.{safe_name}_shared\")\n"
                f")\n"
                f"\n"
                f"# Alternative: For streaming reads from share\n"
                f"# df = spark.readStream.format(\"deltaSharing\").load(share_table)"
            )

        configs.append({
            "processor": conn["processor"],
            "direction": conn["direction"],
            "original_remote_url": conn["remote_url"],
            "share_name": f"{safe_name}_share",
            "table_name": f"{safe_name}_shared",
            "code_snippet": code,
        })

    return configs


def _generate_federation_configs(
    remote_connections: list[dict],
    parse_result: ParseResult,
) -> list[dict]:
    """Generate Lakehouse Federation configs for cross-database queries."""
    configs = []

    # Extract unique remote URLs as federation targets
    remote_urls = {
        conn["remote_url"]
        for conn in remote_connections
        if conn["remote_url"]
    }

    for url in remote_urls:
        conn_name = _url_to_connection_name(url)

        code = (
            f"# Lakehouse Federation: Cross-database query for {url}\n"
            f"# Create foreign catalog connection (run once)\n"
            f"# CREATE CONNECTION IF NOT EXISTS {conn_name}\n"
            f"#   TYPE <CONNECTION_TYPE>  -- Replace with: mysql, postgresql, snowflake, etc.\n"
            f"#   OPTIONS (host '{url}');\n"
            f"\n"
            f"# Create foreign catalog\n"
            f"# CREATE FOREIGN CATALOG IF NOT EXISTS {conn_name}_catalog\n"
            f"#   USING CONNECTION {conn_name};\n"
            f"\n"
            f"# Query across catalogs\n"
            f"# df = spark.table(f\"{conn_name}_catalog.schema.table\")"
        )

        configs.append({
            "remote_url": url,
            "connection_name": conn_name,
            "code_snippet": code,
            "processors": [
                c["processor"] for c in remote_connections if c["remote_url"] == url
            ],
        })

    return configs


def _url_to_connection_name(url: str) -> str:
    """Convert a remote URL to a safe connection name."""
    name = re.sub(r"https?://", "", url)
    name = re.sub(r"[^a-zA-Z0-9]", "_", name).strip("_").lower()
    return name[:60] if name else "remote_connection"
