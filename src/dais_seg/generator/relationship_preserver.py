"""Relationship preserver — ensures referential integrity in synthetic data.

When generating synthetic Delta Tables, foreign key relationships, join
behaviors, and cross-table dependencies must be preserved exactly as they
exist in the source. This module builds a dependency graph and coordinates
generation order to guarantee valid FK chains.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class TableDependency:
    """Represents a table and its FK dependencies in the generation graph."""

    table_name: str
    schema: str
    depends_on: list[str] = field(default_factory=list)
    referenced_by: list[str] = field(default_factory=list)
    generation_order: int = 0


class RelationshipPreserver:
    """Manages referential integrity during synthetic data generation.

    Builds a dependency graph from blueprint relationships, determines
    generation order (parent tables first), and provides FK value pools
    so child tables reference valid parent keys.
    """

    def __init__(self, blueprint: dict):
        self.blueprint = blueprint
        self.tables = {t["name"]: t for t in blueprint.get("tables", [])}
        self.relationships = blueprint.get("relationships", [])
        self.dependency_graph: dict[str, TableDependency] = {}
        self._build_graph()

    def get_generation_order(self) -> list[str]:
        """Return table names in dependency-safe generation order.

        Parent tables (referenced by FKs) are generated first so their
        primary key pools are available when generating child tables.
        """
        visited = set()
        order = []

        def visit(table_name: str):
            if table_name in visited:
                return
            visited.add(table_name)
            dep = self.dependency_graph.get(table_name)
            if dep:
                for parent in dep.depends_on:
                    visit(parent)
            order.append(table_name)

        for table_name in self.dependency_graph:
            visit(table_name)

        logger.info(f"Generation order: {order}")
        return order

    def get_fk_constraints(self, table_name: str) -> list[dict]:
        """Get FK constraints for a table — which columns must reference which parent pools."""
        table_spec = self.tables.get(table_name, {})
        fks = table_spec.get("foreign_keys", [])
        constraints = []
        for fk in fks:
            constraints.append({
                "child_column": fk.get("column", ""),
                "parent_table": fk.get("references_table", ""),
                "parent_column": fk.get("references_column", ""),
                "orphan_ratio": fk.get("orphan_ratio", 0.0),
            })
        return constraints

    def build_pk_pool(
        self, table_name: str, generated_data: dict[str, list[Any]]
    ) -> dict[str, list[Any]]:
        """Extract primary key values from already-generated table data.

        Returns a mapping of column_name -> list of generated PK values
        that child tables can sample from for FK columns.
        """
        table_spec = self.tables.get(table_name, {})
        pk_pool = {}
        for col in table_spec.get("columns", []):
            if col.get("is_primary_key"):
                col_name = col["name"]
                if col_name in generated_data:
                    pk_pool[col_name] = [v for v in generated_data[col_name] if v is not None]
        return pk_pool

    def apply_fk_values(
        self,
        table_name: str,
        generated_data: dict[str, list[Any]],
        pk_pools: dict[str, dict[str, list[Any]]],
    ) -> dict[str, list[Any]]:
        """Replace FK columns in generated data with valid parent key references.

        For each FK constraint, samples from the parent table's PK pool
        to ensure referential integrity. Respects orphan_ratio from the
        blueprint to inject a realistic fraction of orphaned references.
        """
        import random

        constraints = self.get_fk_constraints(table_name)
        for constraint in constraints:
            parent_table = constraint["parent_table"]
            parent_column = constraint["parent_column"]
            child_column = constraint["child_column"]
            orphan_ratio = constraint.get("orphan_ratio", 0.0)

            parent_pool = pk_pools.get(parent_table, {}).get(parent_column, [])
            if not parent_pool:
                logger.warning(
                    f"No PK pool for {parent_table}.{parent_column} — "
                    f"FK {table_name}.{child_column} will use generated values"
                )
                continue

            if child_column not in generated_data:
                continue

            row_count = len(generated_data[child_column])
            fk_values = []
            for _ in range(row_count):
                if random.random() < orphan_ratio:
                    # Deliberately create an orphan reference
                    fk_values.append(f"orphan_{random.randint(1, 99999)}")
                else:
                    fk_values.append(random.choice(parent_pool))

            generated_data[child_column] = fk_values
            logger.debug(
                f"Applied FK: {table_name}.{child_column} -> "
                f"{parent_table}.{parent_column} ({row_count} rows)"
            )

        return generated_data

    def _build_graph(self) -> None:
        """Build the dependency graph from blueprint relationships and FK metadata."""
        # Initialize all tables
        for table_name in self.tables:
            self.dependency_graph[table_name] = TableDependency(
                table_name=table_name,
                schema=self.tables[table_name].get("schema", ""),
            )

        # Add edges from relationships
        for rel in self.relationships:
            from_table = rel.get("from_table", "").split(".")[-1]
            to_table = rel.get("to_table", "").split(".")[-1]

            if from_table in self.dependency_graph:
                self.dependency_graph[from_table].depends_on.append(to_table)
            if to_table in self.dependency_graph:
                self.dependency_graph[to_table].referenced_by.append(from_table)

        # Add edges from per-table FK metadata
        for table_name, table_spec in self.tables.items():
            for fk in table_spec.get("foreign_keys", []):
                parent = fk.get("references_table", "")
                if parent and parent != table_name:
                    if parent not in self.dependency_graph[table_name].depends_on:
                        self.dependency_graph[table_name].depends_on.append(parent)
