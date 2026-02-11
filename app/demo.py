"""SEG Demo — Standalone Streamlit app for local synthetic environment generation.

Run with: streamlit run app/demo.py

No Databricks, no Spark, no live database required. Parses DDL, Excel, JSON,
YAML, or XML input, generates synthetic Pandas DataFrames, simulates the
medallion pipeline, and shows validation confidence scores — all locally.
"""

from __future__ import annotations

import io
import json
import zipfile

import pandas as pd
import streamlit as st

# Initialize config before any scorer imports
from dais_seg.config import SEGConfig, set_config

set_config(SEGConfig())

from dais_seg.source_loader import (
    BlueprintAssembler,
    DDLParser,
    ETLMappingParser,
    FormatDetector,
    InputFormat,
    LocalGenerator,
    SchemaDefinitionParser,
    XMLParser,
)

# --------------------------------------------------------------------------- #
#  Page config
# --------------------------------------------------------------------------- #

st.set_page_config(
    page_title="SEG Demo — Synthetic Environment Generator",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.title("SEG — Synthetic Environment Generator")
st.caption(
    "Upload DDL, Excel, JSON, YAML, or XML — generate a full synthetic environment locally"
)

# --------------------------------------------------------------------------- #
#  Session state
# --------------------------------------------------------------------------- #

for key in ("demo_parsed", "demo_blueprint", "demo_tables", "demo_medallion", "demo_validation"):
    if key not in st.session_state:
        st.session_state[key] = None

# --------------------------------------------------------------------------- #
#  Pipeline stages as tabs
# --------------------------------------------------------------------------- #

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "1. Load & Parse",
    "2. Blueprint",
    "3. Generate",
    "4. Conform",
    "5. Validate",
])


# =========================================================================== #
#  STEP 1: LOAD & PARSE
# =========================================================================== #

with tab1:
    st.header("Step 1: Load & Parse Input")

    col_upload, col_paste = st.columns(2)

    with col_upload:
        st.subheader("Upload File")
        uploaded = st.file_uploader(
            "Choose a file",
            type=["sql", "ddl", "csv", "tsv", "xlsx", "xls", "json", "yaml", "yml", "xml"],
            help="DDL (.sql), ETL mappings (.csv/.xlsx), JSON/YAML schemas, XML schemas",
            key="demo_upload",
        )

    with col_paste:
        st.subheader("Paste Text")
        pasted = st.text_area(
            "Paste DDL, schema, or mapping",
            height=200,
            placeholder=(
                "CREATE TABLE customers (\n"
                "  id INT PRIMARY KEY,\n"
                "  name VARCHAR(100) NOT NULL,\n"
                "  email VARCHAR(200),\n"
                "  status VARCHAR(20) CHECK (status IN ('active','inactive'))\n"
                ");"
            ),
            key="demo_paste",
        )

    if st.button("Parse Input", type="primary", key="btn_parse"):
        content = ""
        filename = None
        parsed = None

        if uploaded:
            filename = uploaded.name
            if filename.lower().endswith((".xlsx", ".xls")):
                file_bytes = uploaded.read()
                try:
                    parser = ETLMappingParser()
                    parsed = parser.parse_excel(file_bytes, source_name=filename)
                except Exception as e:
                    st.error(f"Excel parse error: {e}")
            else:
                content = uploaded.read().decode("utf-8")
        elif pasted.strip():
            content = pasted.strip()
        else:
            st.warning("Upload a file or paste text to get started.")

        if content:
            fmt = FormatDetector.detect(content, filename)

            parser = None
            if fmt in (InputFormat.DDL, InputFormat.RAW_TEXT):
                parser = DDLParser()
            elif fmt == InputFormat.ETL_MAPPING:
                parser = ETLMappingParser()
            elif fmt in (InputFormat.SCHEMA_JSON, InputFormat.SCHEMA_YAML):
                parser = SchemaDefinitionParser()
            elif fmt == InputFormat.SCHEMA_XML:
                parser = XMLParser()
            elif fmt == InputFormat.ENV_FILE:
                st.info("This looks like a .env credentials file — not a schema definition.")
            elif fmt == InputFormat.UNKNOWN:
                st.warning("Could not detect format. Trying DDL parser as fallback...")
                parser = DDLParser()

            if parser:
                try:
                    parsed = parser.parse(content, source_name=filename or "Pasted Input")
                except Exception as e:
                    st.error(f"Parse error: {e}")

            if parsed:
                st.info(f"Detected format: **{fmt.value}**")

        if parsed:
            st.session_state.demo_parsed = parsed
            # Reset downstream state
            st.session_state.demo_blueprint = None
            st.session_state.demo_tables = None
            st.session_state.demo_medallion = None
            st.session_state.demo_validation = None

    # Show parsed results
    parsed = st.session_state.demo_parsed
    if parsed:
        st.markdown("---")
        total_cols = sum(len(t.columns) for t in parsed.tables)
        total_fks = sum(len(t.foreign_keys) for t in parsed.tables)

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Tables", len(parsed.tables))
        m2.metric("Columns", total_cols)
        m3.metric("Foreign Keys", total_fks)
        m4.metric("Source Type", parsed.source_type)

        if parsed.parse_warnings:
            for w in parsed.parse_warnings:
                st.warning(w)

        for table in parsed.tables:
            with st.expander(f"Table: **{table.name}** ({len(table.columns)} columns)", expanded=True):
                col_rows = []
                for c in table.columns:
                    col_rows.append({
                        "Column": c.name,
                        "Type": c.data_type,
                        "Raw Type": c.raw_type or c.data_type,
                        "Nullable": "Yes" if c.nullable else "No",
                        "PK": "Y" if c.is_primary_key else "",
                        "Unique": "Y" if c.is_unique else "",
                        "Default": c.default_value or "",
                        "Check": ", ".join(c.check_constraints) if c.check_constraints else "",
                    })
                st.dataframe(pd.DataFrame(col_rows), use_container_width=True, hide_index=True)

                if table.foreign_keys:
                    st.markdown("**Foreign Keys:**")
                    for fk in table.foreign_keys:
                        st.markdown(f"- `{fk.fk_column}` → `{fk.referenced_table}({fk.referenced_column})`")

        st.success(f"Parsed **{len(parsed.tables)} tables** with **{total_cols} columns** — proceed to Step 2")


# =========================================================================== #
#  STEP 2: BLUEPRINT ASSEMBLY
# =========================================================================== #

with tab2:
    st.header("Step 2: Blueprint Assembly")

    parsed = st.session_state.demo_parsed
    if not parsed:
        st.info("Complete Step 1 first — parse a file or paste text.")
    else:
        cfg1, cfg2 = st.columns(2)
        with cfg1:
            row_count = st.number_input(
                "Rows per table", min_value=10, max_value=10000, value=1000, step=100,
                key="demo_rows",
            )
        with cfg2:
            seed = st.number_input("Random seed", min_value=0, value=42, key="demo_seed")

        if st.button("Assemble Blueprint", type="primary", key="btn_assemble"):
            assembler = BlueprintAssembler()
            bp = assembler.assemble(parsed, row_count=row_count)
            st.session_state.demo_blueprint = bp
            # Reset downstream
            st.session_state.demo_tables = None
            st.session_state.demo_medallion = None
            st.session_state.demo_validation = None

        bp = st.session_state.demo_blueprint
        if bp:
            st.markdown("---")

            b1, b2, b3, b4 = st.columns(4)
            b1.metric("Blueprint ID", bp["blueprint_id"][:8] + "...")
            b2.metric("Tables", len(bp["tables"]))
            b3.metric("Relationships", len(bp["relationships"]))
            b4.metric("Source", bp["source_system"]["type"])

            # Relationship graph
            if bp["relationships"]:
                st.subheader("Relationships")
                for rel in bp["relationships"]:
                    from_t = rel["from_table"].split(".")[-1]
                    to_t = rel["to_table"].split(".")[-1]
                    join = rel["join_columns"][0]
                    st.markdown(
                        f"`{from_t}`.{join['from_column']} → `{to_t}`.{join['to_column']}"
                    )

            # Per-table stats preview
            st.subheader("Table Specifications")
            for t in bp["tables"]:
                with st.expander(f"**{t['name']}** — {t['row_count']} rows, {len(t['columns'])} columns"):
                    stats_rows = []
                    for c in t["columns"]:
                        s = c.get("stats", {})
                        stats_rows.append({
                            "Column": c["name"],
                            "Type": c["data_type"],
                            "PK": "Y" if c.get("is_primary_key") else "",
                            "Null%": f"{s.get('null_ratio', 0):.0%}",
                            "Min": s.get("min", ""),
                            "Max": s.get("max", ""),
                            "Mean": round(s["mean"], 2) if isinstance(s.get("mean"), (int, float)) else "",
                            "Distinct": s.get("distinct_count", ""),
                        })
                    st.dataframe(pd.DataFrame(stats_rows), use_container_width=True, hide_index=True)

            # Download
            bp_json = json.dumps(bp, indent=2, default=str)
            st.download_button(
                "Download Blueprint JSON",
                data=bp_json,
                file_name=f"blueprint_{bp['blueprint_id'][:8]}.json",
                mime="application/json",
            )

            st.success("Blueprint assembled — proceed to Step 3")


# =========================================================================== #
#  STEP 3: GENERATE SYNTHETIC DATA
# =========================================================================== #

with tab3:
    st.header("Step 3: Generate Synthetic Data")

    bp = st.session_state.demo_blueprint
    if not bp:
        st.info("Complete Step 2 first — assemble a blueprint.")
    else:
        st.markdown(f"Blueprint: `{bp['blueprint_id'][:8]}...` — {len(bp['tables'])} tables")

        if st.button("Generate Synthetic Data", type="primary", key="btn_generate"):
            progress = st.progress(0, text="Starting generation...")
            gen = LocalGenerator()

            def on_progress(table_name, idx, total):
                progress.progress(idx / total, text=f"Generating {table_name} ({idx}/{total})...")

            seed = st.session_state.get("demo_seed", 42)
            tables = gen.generate(bp, seed=seed, on_progress=on_progress)
            progress.progress(1.0, text="Generation complete!")

            st.session_state.demo_tables = tables
            # Reset downstream
            st.session_state.demo_medallion = None
            st.session_state.demo_validation = None

        tables = st.session_state.demo_tables
        if tables:
            st.markdown("---")

            total_rows = sum(len(df) for df in tables.values())
            g1, g2, g3 = st.columns(3)
            g1.metric("Tables Generated", len(tables))
            g2.metric("Total Rows", f"{total_rows:,}")
            g3.metric("Total Columns", sum(len(df.columns) for df in tables.values()))

            for name, df in tables.items():
                with st.expander(f"**{name}** — {len(df):,} rows, {len(df.columns)} columns", expanded=True):
                    # Data preview
                    st.markdown("**Data Preview** (first 20 rows)")
                    st.dataframe(df.head(20), use_container_width=True, hide_index=True)

                    # Column statistics
                    st.markdown("**Column Statistics**")
                    desc = df.describe(include="all").transpose()
                    desc.index.name = "Column"
                    st.dataframe(desc, use_container_width=True)

            # Download all as CSV zip
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                for name, df in tables.items():
                    csv_data = df.to_csv(index=False)
                    zf.writestr(f"{name}.csv", csv_data)
            zip_buffer.seek(0)

            st.download_button(
                "Download All Tables (CSV ZIP)",
                data=zip_buffer,
                file_name="synthetic_data.zip",
                mime="application/zip",
            )

            st.success(f"Generated **{total_rows:,} rows** across **{len(tables)} tables** — proceed to Step 4")


# =========================================================================== #
#  STEP 4: CONFORM (MEDALLION SIMULATION)
# =========================================================================== #

with tab4:
    st.header("Step 4: Conform — Medallion Architecture")

    tables = st.session_state.demo_tables
    bp = st.session_state.demo_blueprint
    if not tables or not bp:
        st.info("Complete Step 3 first — generate synthetic data.")
    else:
        if st.button("Run Medallion Pipeline", type="primary", key="btn_conform"):
            with st.spinner("Running Bronze → Silver → Gold simulation..."):
                gen = LocalGenerator()
                medallion = gen.conform_medallion(tables, bp)
                st.session_state.demo_medallion = medallion
                # Reset downstream
                st.session_state.demo_validation = None

        medallion = st.session_state.demo_medallion
        if medallion:
            st.markdown("---")

            # Stats summary
            s1, s2, s3 = st.columns(3)
            s1.metric("Tables Processed", medallion.stats.get("tables_processed", 0))
            s2.metric("Rows Dropped", medallion.stats.get("rows_dropped", 0))
            s3.metric("Nulls Cleaned", medallion.stats.get("nulls_cleaned", 0))

            # Three-column medallion view
            st.subheader("Medallion Layers")

            for table_name in medallion.bronze:
                st.markdown(f"### Table: `{table_name}`")
                col_b, col_s, col_g = st.columns(3)

                with col_b:
                    bronze_df = medallion.bronze[table_name]
                    st.markdown("**Bronze** (Raw)")
                    st.metric("Rows", f"{len(bronze_df):,}")
                    st.dataframe(bronze_df.head(10), use_container_width=True, hide_index=True)

                with col_s:
                    silver_df = medallion.silver[table_name]
                    st.markdown("**Silver** (Cleaned)")
                    dropped = len(bronze_df) - len(silver_df)
                    st.metric("Rows", f"{len(silver_df):,}", delta=f"-{dropped}" if dropped else None)
                    st.dataframe(silver_df.head(10), use_container_width=True, hide_index=True)

                with col_g:
                    gold_df = medallion.gold[table_name]
                    st.markdown("**Gold** (Aggregated)")
                    st.metric("Columns Profiled", len(gold_df))
                    st.dataframe(gold_df, use_container_width=True, hide_index=True)

            # Quality rules
            st.markdown("---")
            st.subheader("Quality Rules")

            if medallion.quality_rules:
                # DLT-style SQL
                st.markdown("**Generated DLT Expectations:**")
                dlt_lines = []
                for rule in medallion.quality_rules:
                    dlt_lines.append(f"  CONSTRAINT {rule['name']} EXPECT ({rule['expression']})")
                st.code("\n".join(dlt_lines), language="sql")

                # Results
                st.markdown("**Quality Check Results:**")
                qr_rows = []
                for r in medallion.quality_results:
                    passed = r.get("passed", True)
                    qr_rows.append({
                        "Status": "PASS" if passed else "FAIL",
                        "Rule": r["rule"],
                        "Table": r["table"],
                        "Column": r.get("column", ""),
                        "Violations": r.get("violations", 0),
                        "Total Rows": r.get("total_rows", 0),
                    })
                qr_df = pd.DataFrame(qr_rows)
                st.dataframe(qr_df, use_container_width=True, hide_index=True)

                passed_count = sum(1 for r in medallion.quality_results if r.get("passed"))
                total_count = len(medallion.quality_results)
                st.metric("Quality Pass Rate", f"{passed_count}/{total_count} ({passed_count/max(total_count,1):.0%})")
            else:
                st.info("No quality rules generated for this schema.")

            st.success("Medallion pipeline complete — proceed to Step 5")


# =========================================================================== #
#  STEP 5: VALIDATE & SCORE
# =========================================================================== #

with tab5:
    st.header("Step 5: Validate & Confidence Score")

    tables = st.session_state.demo_tables
    bp = st.session_state.demo_blueprint
    medallion = st.session_state.demo_medallion
    if not tables or not bp:
        st.info("Complete Steps 3-4 first — generate data and run medallion pipeline.")
    else:
        if st.button("Run Validation", type="primary", key="btn_validate"):
            with st.spinner("Running schema + fidelity + quality validation..."):
                gen = LocalGenerator()
                quality_results = medallion.quality_results if medallion else None
                validation = gen.validate_local(bp, tables, quality_results)
                st.session_state.demo_validation = validation

        validation = st.session_state.demo_validation
        if validation:
            st.markdown("---")

            # Workspace-level summary
            avg_score = sum(v.overall_score for v in validation) / len(validation)
            green_count = sum(1 for v in validation if v.overall_score >= 0.90)
            amber_count = sum(1 for v in validation if 0.70 <= v.overall_score < 0.90)
            red_count = sum(1 for v in validation if v.overall_score < 0.70)

            if avg_score >= 0.90:
                level, icon, color = "GREEN", "🟢", "green"
            elif avg_score >= 0.70:
                level, icon, color = "AMBER", "🟡", "orange"
            else:
                level, icon, color = "RED", "🔴", "red"

            st.markdown(f"## {icon} Overall Confidence: **{level}** — {avg_score:.1%}")
            st.markdown(
                f"**{len(validation)} tables validated:** "
                f"{green_count} green, {amber_count} amber, {red_count} red"
            )

            # Dimension breakdown
            ws1, ws2, ws3, ws4 = st.columns(4)
            avg_schema = sum(v.schema_score for v in validation) / len(validation)
            avg_fidelity = sum(v.fidelity_score for v in validation) / len(validation)
            avg_quality = sum(v.quality_score for v in validation) / len(validation)
            avg_pipeline = sum(v.pipeline_score for v in validation) / len(validation)

            ws1.metric("Schema Parity (25%)", f"{avg_schema:.1%}")
            ws2.metric("Data Fidelity (35%)", f"{avg_fidelity:.1%}")
            ws3.metric("Quality (20%)", f"{avg_quality:.1%}")
            ws4.metric("Pipeline (20%)", f"{avg_pipeline:.1%}")

            # Per-table cards
            st.markdown("---")
            st.subheader("Per-Table Results")

            for v in validation:
                if v.overall_score >= 0.90:
                    t_icon = "🟢"
                elif v.overall_score >= 0.70:
                    t_icon = "🟡"
                else:
                    t_icon = "🔴"

                with st.expander(
                    f"{t_icon} **{v.table_name}** — {v.overall_score:.1%}",
                    expanded=v.overall_score < 0.90,
                ):
                    d1, d2, d3, d4 = st.columns(4)
                    d1.metric("Schema", f"{v.schema_score:.0%}")
                    d2.metric("Fidelity", f"{v.fidelity_score:.0%}")
                    d3.metric("Quality", f"{v.quality_score:.0%}")
                    d4.metric("Pipeline", f"{v.pipeline_score:.0%}")

                    # Progress bars for each dimension
                    st.progress(v.schema_score, text=f"Schema Parity: {v.schema_score:.0%}")
                    st.progress(v.fidelity_score, text=f"Data Fidelity: {v.fidelity_score:.0%}")
                    st.progress(v.quality_score, text=f"Quality Compliance: {v.quality_score:.0%}")
                    st.progress(v.pipeline_score, text=f"Pipeline Integrity: {v.pipeline_score:.0%}")

                    if v.recommendations:
                        st.markdown("**Recommendations:**")
                        for rec in v.recommendations:
                            st.markdown(f"- {rec}")

                    if v.missing_columns:
                        st.warning(f"Missing columns: {', '.join(v.missing_columns)}")
                    if v.type_mismatches:
                        st.warning(f"Type mismatches: {v.type_mismatches}")

            # Scoring methodology
            st.markdown("---")
            st.subheader("Scoring Methodology")
            st.markdown(
                "| Dimension | Weight | Description |\n"
                "|-----------|--------|-------------|\n"
                "| Schema Parity | 25% | Columns, types, constraints match blueprint |\n"
                "| Data Fidelity | 35% | Distributions, null ratios, cardinality match |\n"
                "| Quality Compliance | 20% | DLT expectations pass rate |\n"
                "| Pipeline Integrity | 20% | Medallion pipeline success |"
            )
            st.markdown(
                "| Level | Threshold | Meaning |\n"
                "|-------|-----------|--------|\n"
                "| 🟢 Green | >= 90% | Ready for production cutover |\n"
                "| 🟡 Amber | >= 70% | Needs attention before cutover |\n"
                "| 🔴 Red | < 70% | Not ready — review recommendations |"
            )

            st.success("Validation complete! Your synthetic environment is ready.")
