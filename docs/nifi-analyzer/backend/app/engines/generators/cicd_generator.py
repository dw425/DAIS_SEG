"""NiFi Registry to Git/DAB CI/CD translation generator.

Generates GitHub Actions workflows for CI/CD, databricks bundle deploy commands
for dev/staging/prod targets, and maps NiFi Registry versioned flows to Git
branches/tags.
"""

import logging
import re
from datetime import datetime, timezone

from app.models.pipeline import ParseResult

logger = logging.getLogger(__name__)


def generate_cicd(parse_result: ParseResult) -> dict:
    """Generate CI/CD configuration for the migrated pipeline.

    Returns:
        {
            "github_workflow": str,
            "dab_deploy_commands": {...},
            "registry_mapping": [...],
            "bundle_config": str,
            "summary": {...},
        }
    """
    flow_name = parse_result.metadata.get("source_file", "migration")
    safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", flow_name).lower()
    version = parse_result.version or "1.0.0"

    # Generate GitHub Actions workflow
    github_workflow = _generate_github_workflow(safe_name, parse_result)

    # Generate DAB deploy commands
    dab_commands = _generate_dab_commands(safe_name)

    # Map NiFi Registry flows to Git
    registry_mapping = _map_registry_to_git(parse_result)

    # Generate bundle config YAML
    bundle_config = _generate_bundle_config(safe_name, parse_result)

    logger.info("CI/CD generated: GitHub Actions workflow for %s", safe_name)
    return {
        "github_workflow": github_workflow,
        "dab_deploy_commands": dab_commands,
        "registry_mapping": registry_mapping,
        "bundle_config": bundle_config,
        "summary": {
            "workflow_file": ".github/workflows/deploy.yml",
            "targets": ["dev", "staging", "prod"],
            "flow_version": version,
        },
    }


def _generate_github_workflow(safe_name: str, parse_result: ParseResult) -> str:
    """Generate .github/workflows/deploy.yml content."""
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    return f"""# Auto-generated CI/CD pipeline for NiFi migration: {safe_name}
# Generated: {generated_at}
name: Deploy {safe_name}

on:
  push:
    branches: [main, staging]
    paths:
      - 'src/migrations/{safe_name}/**'
      - 'databricks.yml'
  pull_request:
    branches: [main]

env:
  DATABRICKS_HOST: ${{{{ secrets.DATABRICKS_HOST }}}}
  DATABRICKS_TOKEN: ${{{{ secrets.DATABRICKS_TOKEN }}}}

jobs:
  validate:
    name: Validate Bundle
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install Databricks CLI
        uses: databricks/setup-cli@main

      - name: Validate bundle
        run: databricks bundle validate

      - name: Run tests
        run: databricks bundle run --no-wait {safe_name}_test_job

  deploy-dev:
    name: Deploy to Dev
    needs: validate
    if: github.ref == 'refs/heads/main' || github.event_name == 'pull_request'
    runs-on: ubuntu-latest
    environment: dev
    steps:
      - uses: actions/checkout@v4

      - name: Install Databricks CLI
        uses: databricks/setup-cli@main

      - name: Deploy to dev
        run: databricks bundle deploy --target dev

      - name: Run smoke tests
        run: databricks bundle run --target dev {safe_name}_smoke_test

  deploy-staging:
    name: Deploy to Staging
    needs: deploy-dev
    if: github.ref == 'refs/heads/staging' || github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    environment: staging
    steps:
      - uses: actions/checkout@v4

      - name: Install Databricks CLI
        uses: databricks/setup-cli@main

      - name: Deploy to staging
        run: databricks bundle deploy --target staging

      - name: Run integration tests
        run: databricks bundle run --target staging {safe_name}_integration_test

  deploy-prod:
    name: Deploy to Production
    needs: deploy-staging
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    environment:
      name: prod
      url: ${{{{ steps.deploy.outputs.dashboard_url }}}}
    steps:
      - uses: actions/checkout@v4

      - name: Install Databricks CLI
        uses: databricks/setup-cli@main

      - name: Deploy to production
        id: deploy
        run: |
          databricks bundle deploy --target prod
          echo "dashboard_url=$DATABRICKS_HOST" >> $GITHUB_OUTPUT
"""


def _generate_dab_commands(safe_name: str) -> dict:
    """Generate databricks bundle deploy commands for each target."""
    return {
        "dev": {
            "deploy": f"databricks bundle deploy --target dev",
            "run": f"databricks bundle run --target dev migration_{safe_name}",
            "destroy": f"databricks bundle destroy --target dev --auto-approve",
        },
        "staging": {
            "deploy": f"databricks bundle deploy --target staging",
            "run": f"databricks bundle run --target staging migration_{safe_name}",
            "destroy": f"databricks bundle destroy --target staging --auto-approve",
        },
        "prod": {
            "deploy": f"databricks bundle deploy --target prod",
            "run": f"databricks bundle run --target prod migration_{safe_name}",
            "destroy": "# DANGER: production destroy requires manual confirmation",
        },
    }


def _map_registry_to_git(parse_result: ParseResult) -> list[dict]:
    """Map NiFi Registry versioned flows to Git branches/tags."""
    mappings = []
    version = parse_result.version or "1.0.0"
    metadata = parse_result.metadata

    # Main flow version
    mappings.append({
        "nifi_registry_item": metadata.get("source_file", "flow"),
        "nifi_version": version,
        "git_branch": "main",
        "git_tag": f"v{version}",
        "notes": "Main production flow mapped to main branch",
    })

    # Map process groups as potential sub-flows
    for pg in parse_result.process_groups:
        safe_pg = re.sub(r"[^a-zA-Z0-9_-]", "_", pg.name).lower()
        mappings.append({
            "nifi_registry_item": pg.name,
            "nifi_version": version,
            "git_branch": f"feature/{safe_pg}",
            "git_tag": f"v{version}-{safe_pg}",
            "notes": f"Process Group '{pg.name}' mapped to feature branch",
        })

    return mappings


def _generate_bundle_config(safe_name: str, parse_result: ParseResult) -> str:
    """Generate databricks.yml bundle configuration."""
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    proc_count = len(parse_result.processors)

    return f"""# Databricks Asset Bundle configuration
# Auto-generated from NiFi flow: {safe_name}
# Generated: {generated_at}

bundle:
  name: nifi_migration_{safe_name}

workspace:
  root_path: /Workspace/migrations/{safe_name}

variables:
  catalog:
    description: Unity Catalog name
    default: main
  schema:
    description: Schema name
    default: default

resources:
  jobs:
    migration_{safe_name}:
      name: "Migration: {safe_name}"
      description: "Migrated from NiFi flow ({proc_count} processors)"
      schedule:
        quartz_cron_expression: "0 0 * * * ?"  # hourly
        timezone_id: UTC
      job_clusters:
        - job_cluster_key: migration_cluster
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            num_workers: 2
            node_type_id: i3.xlarge
      tasks:
        - task_key: main_pipeline
          job_cluster_key: migration_cluster
          notebook_task:
            notebook_path: ./src/migrations/{safe_name}/main.py
            base_parameters:
              catalog: "${{var.catalog}}"
              schema: "${{var.schema}}"

    {safe_name}_test_job:
      name: "Test: {safe_name}"
      tasks:
        - task_key: run_tests
          notebook_task:
            notebook_path: ./src/migrations/{safe_name}/tests/test_pipeline.py

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: ${{var.dev_host}}

  staging:
    mode: development
    workspace:
      host: ${{var.staging_host}}

  prod:
    mode: production
    workspace:
      host: ${{var.prod_host}}
    run_as:
      service_principal_name: sp-migration-{safe_name}
"""
