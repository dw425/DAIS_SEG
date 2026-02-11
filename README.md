# DAIS SEG — Synthetic Environment Generation

**AI-Driven Parallel Migration on Databricks**

Data + AI Summit 2026 | Blueprint

---

## Overview

SEG uses **Databricks Foundation Models** to profile source systems, generate statistically faithful **synthetic Delta Tables** in isolated Workspaces, and validate migration readiness — all without touching customer production data.

**Genie** provides the natural language interface: *"Give me a synthetic copy of the orders database at 10% scale."*

The result is radical parallelism — multiple teams testing in independent Workspaces simultaneously, compressing 9-month migration timelines to weeks.

## Architecture

```
Source Systems ──► Foundation Model Agent ──► Synthetic Workspace ──► Medallion Target ──► Validation Engine
  (Oracle,          (Lakehouse Federation     (Delta Table gen,       (Bronze/Silver/       (Schema, Fidelity,
   SQL Server,       Unity Catalog crawl       Workbook replication,   Gold DLT pipelines,   Confidence scoring,
   Teradata,         Profile & map)            Governance mirror)      Unity Catalog)         Genie dashboards)
   Snowflake)
```

## Four Pillars

| # | Pillar | Module | Description |
|---|--------|--------|-------------|
| 01 | **Profile** | `src/dais_seg/profiler/` | Unity Catalog discovery via Lakehouse Federation, distribution profiling, Source Blueprint generation |
| 02 | **Generate** | `src/dais_seg/generator/` | Synthetic Delta Table generation preserving schema, FKs, distributions, nulls, and edge cases |
| 03 | **Conform** | `src/dais_seg/conformer/` | Medallion architecture (Bronze/Silver/Gold) with DLT-style quality expectations |
| 04 | **Validate** | `src/dais_seg/validator/` | Like-for-like validation with green/amber/red confidence scoring |

## Tech Stack

Mosaic AI Model Serving, Genie, Unity Catalog, Lakeflow, Delta Lake, Apache Spark, DLT, Serverless Compute, Databricks SDK

## Quick Start

### As a Databricks App

```bash
# Configure
cp .env.example .env  # Set DATABRICKS_HOST, DATABRICKS_TOKEN, etc.

# Deploy
databricks bundle deploy --target dev

# The app will be available at your Databricks workspace URL
```

### Via Notebooks

1. Build and install the wheel on your cluster
2. Run notebooks in order: `01_profile_source` → `02_generate_synthetic` → `03_conform_medallion` → `04_validate_workspace`
3. Or use `05_orchestrate_parallel` for multi-workstream execution

### Local Development

```bash
pip install -e ".[dev]"
pytest
```

## Project Structure

```
├── app/                          # Databricks App (Gradio UI)
│   └── main.py                   # Entry point — Genie chat + dashboard
├── notebooks/                    # Databricks Notebooks (one per pillar)
│   ├── 01_profile_source.py
│   ├── 02_generate_synthetic.py
│   ├── 03_conform_medallion.py
│   ├── 04_validate_workspace.py
│   └── 05_orchestrate_parallel.py
├── src/dais_seg/                 # Core Python library
│   ├── profiler/                 # Pillar 1: Source profiling
│   ├── generator/                # Pillar 2: Synthetic generation
│   ├── conformer/                # Pillar 3: Medallion conformance
│   ├── validator/                # Pillar 4: Validation + scoring
│   ├── workspace_manager/        # Parallel orchestration
│   ├── genie/                    # Genie NL interface
│   └── config/                   # Configuration management
├── schemas/                      # JSON schemas (Source Blueprint)
├── resources/                    # DLT pipelines, workflow templates
├── tests/                        # Test suite
├── databricks.yml                # Databricks Asset Bundle config
├── app.yaml                      # Databricks App config
└── pyproject.toml                # Python package config
```
