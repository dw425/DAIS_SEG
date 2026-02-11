"""Local synthetic data generator — Pandas-based, no Spark required.

Wraps DistributionSampler + RelationshipPreserver to generate synthetic
Pandas DataFrames from a blueprint. Also provides local medallion
simulation and validation for the demo mode.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from dais_seg.generator.distribution_sampler import DistributionSampler
from dais_seg.generator.relationship_preserver import RelationshipPreserver

logger = logging.getLogger(__name__)


@dataclass
class MedallionResult:
    """Result of local medallion simulation."""

    bronze: dict[str, pd.DataFrame] = field(default_factory=dict)
    silver: dict[str, pd.DataFrame] = field(default_factory=dict)
    gold: dict[str, pd.DataFrame] = field(default_factory=dict)
    quality_rules: list[dict] = field(default_factory=list)
    quality_results: list[dict] = field(default_factory=list)
    stats: dict = field(default_factory=dict)


@dataclass
class LocalValidationResult:
    """Result of local validation."""

    table_name: str
    schema_score: float = 1.0
    fidelity_score: float = 1.0
    quality_score: float = 1.0
    pipeline_score: float = 1.0
    overall_score: float = 1.0
    missing_columns: list[str] = field(default_factory=list)
    type_mismatches: list[dict] = field(default_factory=list)
    fidelity_details: dict = field(default_factory=dict)
    recommendations: list[str] = field(default_factory=list)


# Type compatibility map (mirrors SchemaValidator.TYPE_EQUIVALENCES)
TYPE_EQUIVALENCES = {
    "int": {"integer", "int", "int64", "int32"},
    "integer": {"integer", "int", "int64", "int32"},
    "bigint": {"long", "bigint", "int64"},
    "smallint": {"smallint", "int16", "int"},
    "float": {"float", "float64", "float32"},
    "double": {"double", "float64"},
    "decimal": {"decimal", "float64", "float"},
    "numeric": {"decimal", "numeric", "float64"},
    "string": {"string", "varchar", "char", "text", "object"},
    "varchar": {"string", "varchar", "char", "text", "object"},
    "char": {"string", "varchar", "char", "text", "object"},
    "text": {"string", "varchar", "char", "text", "object"},
    "boolean": {"boolean", "bool"},
    "date": {"date", "object"},
    "timestamp": {"timestamp", "datetime", "datetime64[ns]", "object"},
}


class LocalGenerator:
    """Generate synthetic Pandas DataFrames from a blueprint — no Spark needed."""

    def generate(
        self,
        blueprint: dict,
        seed: int = 42,
        on_progress: Any = None,
    ) -> dict[str, pd.DataFrame]:
        """Generate synthetic data for all tables in the blueprint.

        Args:
            blueprint: Blueprint dict (from BlueprintAssembler).
            seed: Random seed for reproducibility.
            on_progress: Optional callback(table_name, index, total).

        Returns:
            Dict mapping table_name → pd.DataFrame with synthetic data.
        """
        sampler = DistributionSampler(seed=seed)
        preserver = RelationshipPreserver(blueprint)
        order = preserver.get_generation_order()
        pk_pools: dict[str, dict[str, list]] = {}
        results: dict[str, pd.DataFrame] = {}

        total = len(order)
        for idx, table_name in enumerate(order):
            table_spec = self._find_table(blueprint, table_name)
            if not table_spec:
                logger.warning(f"Table {table_name} in generation order but not in blueprint")
                continue

            row_count = table_spec.get("row_count", 1000)
            data: dict[str, list] = {}

            for col_spec in table_spec.get("columns", []):
                values = sampler.sample_column(col_spec, row_count)
                data[col_spec["name"]] = values

            # Apply FK constraints
            data = preserver.apply_fk_values(table_name, data, pk_pools)

            # Build PK pool for child tables
            pk_pools[table_name] = preserver.build_pk_pool(table_name, data)

            results[table_name] = pd.DataFrame(data)

            if on_progress:
                on_progress(table_name, idx + 1, total)

        logger.info(f"Generated {len(results)} tables locally")
        return results

    def conform_medallion(
        self,
        tables: dict[str, pd.DataFrame],
        blueprint: dict,
    ) -> MedallionResult:
        """Simulate medallion conformance using Pandas.

        Bronze: raw data as-is
        Silver: cleaned — drop all-null rows, deduplicate PKs, enforce types
        Gold: aggregated — counts, means, value distributions
        """
        result = MedallionResult()
        total_dropped = 0
        total_nulls_cleaned = 0

        for table_name, df in tables.items():
            table_spec = self._find_table(blueprint, table_name)

            # ---- Bronze: raw data ----
            result.bronze[table_name] = df.copy()

            # ---- Silver: cleaned ----
            silver_df = df.copy()
            rows_before = len(silver_df)

            # Drop all-null rows
            silver_df = silver_df.dropna(how="all")
            dropped = rows_before - len(silver_df)
            total_dropped += dropped

            # Deduplicate on PK columns
            if table_spec:
                pk_cols = [
                    c["name"] for c in table_spec.get("columns", [])
                    if c.get("is_primary_key")
                ]
                if pk_cols:
                    existing = [c for c in pk_cols if c in silver_df.columns]
                    if existing:
                        before_dedup = len(silver_df)
                        silver_df = silver_df.drop_duplicates(subset=existing, keep="first")
                        total_dropped += before_dedup - len(silver_df)

            # Count nulls cleaned (for non-nullable columns)
            if table_spec:
                for col_spec in table_spec.get("columns", []):
                    col_name = col_spec["name"]
                    if not col_spec.get("nullable", True) and col_name in silver_df.columns:
                        null_count = silver_df[col_name].isna().sum()
                        if null_count > 0:
                            total_nulls_cleaned += null_count
                            # Fill with defaults instead of dropping
                            silver_df[col_name] = silver_df[col_name].fillna(
                                self._default_fill(col_spec.get("data_type", "varchar"))
                            )

            result.silver[table_name] = silver_df

            # ---- Gold: aggregated ----
            gold_data = self._build_gold_aggregation(silver_df, table_spec)
            result.gold[table_name] = gold_data

            # ---- Quality rules ----
            rules, rule_results = self._evaluate_quality_rules(silver_df, table_spec, table_name)
            result.quality_rules.extend(rules)
            result.quality_results.extend(rule_results)

        result.stats = {
            "rows_dropped": total_dropped,
            "nulls_cleaned": total_nulls_cleaned,
            "tables_processed": len(tables),
        }

        return result

    def validate_local(
        self,
        blueprint: dict,
        generated: dict[str, pd.DataFrame],
        quality_results: list[dict] | None = None,
    ) -> list[LocalValidationResult]:
        """Run local schema + fidelity validation."""
        results = []

        for table_spec in blueprint.get("tables", []):
            table_name = table_spec["name"]
            df = generated.get(table_name)

            if df is None:
                results.append(LocalValidationResult(
                    table_name=table_name,
                    schema_score=0.0,
                    fidelity_score=0.0,
                    overall_score=0.0,
                    recommendations=[f"Table {table_name} was not generated"],
                ))
                continue

            # Schema validation
            schema_result = self._validate_schema(df, table_spec)

            # Fidelity validation
            fidelity_result = self._validate_fidelity(df, table_spec)

            # Quality score from rule results
            quality_score = 1.0
            if quality_results:
                table_violations = [
                    r for r in quality_results
                    if r.get("table") == table_name and not r.get("passed", True)
                ]
                if table_violations:
                    quality_score = max(0.0, 1.0 - len(table_violations) * 0.1)

            # Pipeline score (always 1.0 in demo — pipeline "succeeded")
            pipeline_score = 1.0

            # Weighted overall (matches ConfidenceScorer weights)
            overall = (
                schema_result["score"] * 0.25
                + fidelity_result["score"] * 0.35
                + quality_score * 0.20
                + pipeline_score * 0.20
            )

            recommendations = []
            if schema_result["missing"]:
                recommendations.append(f"Missing columns: {', '.join(schema_result['missing'])}")
            if schema_result["mismatches"]:
                recommendations.append(f"Type mismatches in {len(schema_result['mismatches'])} columns")
            if not fidelity_result["row_count_match"]:
                recommendations.append(
                    f"Row count: expected {table_spec.get('row_count', 0)}, "
                    f"got {len(df)}"
                )

            results.append(LocalValidationResult(
                table_name=table_name,
                schema_score=schema_result["score"],
                fidelity_score=fidelity_result["score"],
                quality_score=quality_score,
                pipeline_score=pipeline_score,
                overall_score=round(overall, 4),
                missing_columns=schema_result["missing"],
                type_mismatches=schema_result["mismatches"],
                fidelity_details=fidelity_result,
                recommendations=recommendations,
            ))

        return results

    def _find_table(self, blueprint: dict, table_name: str) -> dict | None:
        """Find table spec in blueprint by name."""
        for t in blueprint.get("tables", []):
            if t["name"] == table_name:
                return t
        return None

    def _default_fill(self, data_type: str) -> Any:
        """Get a sensible fill value for a data type."""
        fills = {
            "int": 0, "bigint": 0, "smallint": 0, "integer": 0,
            "float": 0.0, "double": 0.0, "decimal": 0.0, "numeric": 0.0,
            "varchar": "", "char": "", "text": "", "string": "",
            "boolean": False, "bool": False,
            "date": "2020-01-01", "timestamp": "2020-01-01T00:00:00",
        }
        return fills.get(data_type.lower(), "")

    def _build_gold_aggregation(
        self, df: pd.DataFrame, table_spec: dict | None
    ) -> pd.DataFrame:
        """Build a gold-layer aggregation summary for a table."""
        agg_rows = []

        for col in df.columns:
            row: dict[str, Any] = {"column": col}
            series = df[col]

            row["non_null_count"] = int(series.notna().sum())
            row["null_count"] = int(series.isna().sum())

            # Try numeric aggregation
            numeric = pd.to_numeric(series, errors="coerce")
            if numeric.notna().sum() > len(series) * 0.5:
                row["min"] = round(float(numeric.min()), 4) if numeric.notna().any() else None
                row["max"] = round(float(numeric.max()), 4) if numeric.notna().any() else None
                row["mean"] = round(float(numeric.mean()), 4) if numeric.notna().any() else None
                row["stddev"] = round(float(numeric.std()), 4) if numeric.notna().any() else None
            else:
                # Categorical
                row["distinct_values"] = int(series.nunique())
                top = series.value_counts().head(5)
                row["top_values"] = ", ".join(f"{v}({c})" for v, c in top.items())

            agg_rows.append(row)

        return pd.DataFrame(agg_rows)

    def _evaluate_quality_rules(
        self,
        df: pd.DataFrame,
        table_spec: dict | None,
        table_name: str,
    ) -> tuple[list[dict], list[dict]]:
        """Generate and evaluate quality rules for a table."""
        rules = []
        results = []

        if not table_spec:
            return rules, results

        for col_spec in table_spec.get("columns", []):
            col_name = col_spec["name"]
            if col_name not in df.columns:
                continue

            # NOT NULL check for non-nullable columns
            if not col_spec.get("nullable", True):
                rule = {
                    "name": f"{table_name}_{col_name}_not_null",
                    "table": table_name,
                    "expression": f"`{col_name}` IS NOT NULL",
                    "action": "warn",
                }
                rules.append(rule)

                null_count = int(df[col_name].isna().sum())
                results.append({
                    "rule": rule["name"],
                    "table": table_name,
                    "column": col_name,
                    "passed": null_count == 0,
                    "violations": null_count,
                    "total_rows": len(df),
                })

            # PK uniqueness check
            if col_spec.get("is_primary_key"):
                rule = {
                    "name": f"{table_name}_{col_name}_unique",
                    "table": table_name,
                    "expression": f"`{col_name}` is unique",
                    "action": "warn",
                }
                rules.append(rule)

                dup_count = int(df[col_name].duplicated().sum())
                results.append({
                    "rule": rule["name"],
                    "table": table_name,
                    "column": col_name,
                    "passed": dup_count == 0,
                    "violations": dup_count,
                    "total_rows": len(df),
                })

            # Range check for numeric columns with stats
            stats = col_spec.get("stats", {})
            if stats.get("min") is not None and stats.get("max") is not None:
                data_type = col_spec.get("data_type", "").lower()
                if data_type in ("int", "bigint", "smallint", "float", "double", "decimal"):
                    rule = {
                        "name": f"{table_name}_{col_name}_range",
                        "table": table_name,
                        "expression": f"`{col_name}` BETWEEN {stats['min']} AND {stats['max']}",
                        "action": "warn",
                    }
                    rules.append(rule)

                    numeric = pd.to_numeric(df[col_name], errors="coerce")
                    out_of_range = int(
                        ((numeric < stats["min"]) | (numeric > stats["max"])).sum()
                    )
                    results.append({
                        "rule": rule["name"],
                        "table": table_name,
                        "column": col_name,
                        "passed": out_of_range == 0,
                        "violations": out_of_range,
                        "total_rows": len(df),
                    })

        return rules, results

    def _validate_schema(self, df: pd.DataFrame, table_spec: dict) -> dict:
        """Local schema validation — compare DataFrame columns against blueprint."""
        expected = {c["name"]: c for c in table_spec.get("columns", [])}
        actual_cols = set(df.columns)

        missing = [c for c in expected if c not in actual_cols]
        extra = [c for c in actual_cols if c not in expected]
        mismatches = []
        matched = 0

        for col_name, col_spec in expected.items():
            if col_name not in actual_cols:
                continue
            expected_type = col_spec["data_type"].lower()
            actual_dtype = str(df[col_name].dtype).lower()

            if self._types_compatible(expected_type, actual_dtype):
                matched += 1
            else:
                mismatches.append({
                    "column": col_name,
                    "expected": expected_type,
                    "actual": actual_dtype,
                })

        total = len(expected) if expected else 1
        score = matched / total

        return {
            "score": round(score, 4),
            "missing": missing,
            "extra": extra,
            "mismatches": mismatches,
            "matched": matched,
            "total": total,
        }

    def _validate_fidelity(self, df: pd.DataFrame, table_spec: dict) -> dict:
        """Local data fidelity validation — compare stats against blueprint."""
        expected_count = table_spec.get("row_count", 0)
        actual_count = len(df)
        row_count_match = abs(actual_count - expected_count) / max(expected_count, 1) <= 0.05

        col_scores = []
        for col_spec in table_spec.get("columns", []):
            col_name = col_spec["name"]
            stats = col_spec.get("stats", {})
            if not stats or col_name not in df.columns:
                col_scores.append(1.0)
                continue

            checks_passed = 0
            checks_total = 0

            # Null ratio check
            expected_null = stats.get("null_ratio", 0.0)
            actual_null = df[col_name].isna().mean()
            checks_total += 1
            if abs(actual_null - expected_null) <= 0.05:
                checks_passed += 1

            # Cardinality check
            expected_distinct = stats.get("distinct_count")
            if expected_distinct:
                actual_distinct = df[col_name].nunique()
                ratio = min(actual_distinct, expected_distinct) / max(actual_distinct, expected_distinct, 1)
                checks_total += 1
                if ratio >= 0.5:
                    checks_passed += 1

            col_scores.append(checks_passed / max(checks_total, 1))

        avg_col = sum(col_scores) / len(col_scores) if col_scores else 1.0
        row_score = 1.0 if row_count_match else 0.5
        overall = row_score * 0.2 + avg_col * 0.8

        return {
            "score": round(overall, 4),
            "row_count_match": row_count_match,
            "row_count_expected": expected_count,
            "row_count_actual": actual_count,
        }

    def _types_compatible(self, expected: str, actual: str) -> bool:
        """Check type compatibility between blueprint type and Pandas dtype."""
        if expected == actual:
            return True
        equivalents = TYPE_EQUIVALENCES.get(expected, {expected})
        return actual in equivalents
