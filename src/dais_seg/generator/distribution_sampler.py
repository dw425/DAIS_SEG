"""Distribution-aware data sampler for synthetic column generation.

Given a column's statistical profile from the blueprint, generates synthetic
values that match the original distribution — preserving mean, variance,
cardinality, null ratios, and value frequencies.
"""

from __future__ import annotations

import random
import string
from typing import Any, Optional

import numpy as np
from scipy import stats as scipy_stats


class DistributionSampler:
    """Generates synthetic column values that match profiled distributions.

    Supports numeric (normal, uniform, skewed), categorical (frequency-based),
    string (pattern-based), and date/timestamp generation — all driven by
    the statistical fingerprints captured during source profiling.
    """

    def __init__(self, seed: int = 42):
        self.rng = np.random.default_rng(seed)
        self.py_rng = random.Random(seed)

    def sample_column(
        self,
        col_spec: dict,
        row_count: int,
    ) -> list[Any]:
        """Generate synthetic values for a column based on its blueprint spec.

        Args:
            col_spec: Column specification from the blueprint (includes stats).
            row_count: Number of rows to generate.

        Returns:
            List of synthetic values matching the profiled distribution.
        """
        col_stats = col_spec.get("stats", {})
        data_type = col_spec.get("data_type", "string").lower()
        null_ratio = col_stats.get("null_ratio", 0.0)

        # Generate non-null values
        non_null_count = int(row_count * (1 - null_ratio))
        null_count = row_count - non_null_count

        base_type = data_type.split("(")[0]

        if base_type in ("int", "bigint", "smallint", "tinyint", "integer", "long"):
            values = self._sample_integer(col_stats, non_null_count)
        elif base_type in ("float", "double", "decimal", "numeric"):
            values = self._sample_float(col_stats, non_null_count)
        elif base_type in ("string", "varchar", "char", "text"):
            values = self._sample_string(col_stats, non_null_count)
        elif base_type in ("date",):
            values = self._sample_date(col_stats, non_null_count)
        elif base_type in ("timestamp", "datetime"):
            values = self._sample_timestamp(col_stats, non_null_count)
        elif base_type in ("boolean", "bool"):
            values = self._sample_boolean(col_stats, non_null_count)
        else:
            values = self._sample_string(col_stats, non_null_count)

        # Inject nulls at random positions
        result = list(values) + [None] * null_count
        self.py_rng.shuffle(result)
        return result

    def _sample_integer(self, stats: dict, count: int) -> list[int]:
        """Sample integers matching the profiled distribution."""
        top_values = stats.get("top_values", [])

        # If high-cardinality categorical, use frequency-based sampling
        if top_values and self._is_categorical(stats):
            return self._sample_from_frequencies(top_values, count)

        min_val = stats.get("min", 0)
        max_val = stats.get("max", 1000)
        mean = stats.get("mean")
        stddev = stats.get("stddev")

        if mean is not None and stddev is not None and stddev > 0:
            values = self.rng.normal(mean, stddev, count)
            values = np.clip(values, min_val, max_val)
            return [int(round(v)) for v in values]

        return [int(v) for v in self.rng.integers(int(min_val), int(max_val) + 1, count)]

    def _sample_float(self, stats: dict, count: int) -> list[float]:
        """Sample floats matching the profiled distribution."""
        mean = stats.get("mean", 0.0)
        stddev = stats.get("stddev", 1.0)
        min_val = stats.get("min", float("-inf"))
        max_val = stats.get("max", float("inf"))

        if mean is not None and stddev is not None and stddev > 0:
            values = self.rng.normal(mean, stddev, count)
            values = np.clip(values, min_val, max_val)
            return [round(float(v), 4) for v in values]

        return [round(float(v), 4) for v in self.rng.uniform(min_val, max_val, count)]

    def _sample_string(self, stats: dict, count: int) -> list[str]:
        """Sample strings — from frequencies if categorical, or from pattern."""
        top_values = stats.get("top_values", [])

        # Categorical strings: sample from observed frequencies
        if top_values:
            return self._sample_from_frequencies(top_values, count)

        # Pattern-based generation
        pattern = stats.get("pattern")
        if pattern:
            return [self._generate_from_pattern(pattern) for _ in range(count)]

        # Fallback: random strings with observed length distribution
        min_len = stats.get("min_length", 5)
        max_len = stats.get("max_length", 20)
        if min_len is None:
            min_len = 5
        if max_len is None:
            max_len = 20

        return [
            "".join(self.py_rng.choices(string.ascii_letters + string.digits, k=self.py_rng.randint(min_len, max_len)))
            for _ in range(count)
        ]

    def _sample_date(self, stats: dict, count: int) -> list[str]:
        """Sample date values within the profiled range."""
        from datetime import date, timedelta

        min_date = self._parse_date(stats.get("min", "2020-01-01"))
        max_date = self._parse_date(stats.get("max", "2025-12-31"))
        delta_days = (max_date - min_date).days
        if delta_days <= 0:
            delta_days = 365

        return [
            (min_date + timedelta(days=int(self.rng.integers(0, delta_days + 1)))).isoformat()
            for _ in range(count)
        ]

    def _sample_timestamp(self, stats: dict, count: int) -> list[str]:
        """Sample timestamp values within the profiled range."""
        dates = self._sample_date(stats, count)
        return [
            f"{d}T{self.py_rng.randint(0,23):02d}:{self.py_rng.randint(0,59):02d}:{self.py_rng.randint(0,59):02d}"
            for d in dates
        ]

    def _sample_boolean(self, stats: dict, count: int) -> list[bool]:
        """Sample boolean values based on observed frequencies."""
        top_values = stats.get("top_values", [])
        true_ratio = 0.5
        for tv in top_values:
            if str(tv.get("value", "")).lower() in ("true", "1", "yes"):
                true_ratio = tv.get("frequency", 0.5)
                break
        return [bool(v) for v in self.rng.choice([True, False], count, p=[true_ratio, 1 - true_ratio])]

    def _sample_from_frequencies(self, top_values: list[dict], count: int) -> list[Any]:
        """Sample values based on observed frequency distribution."""
        if not top_values:
            return [None] * count

        values = [tv["value"] for tv in top_values]
        freqs = np.array([tv.get("frequency", 1.0 / len(top_values)) for tv in top_values])
        freqs = freqs / freqs.sum()  # Normalize

        indices = self.rng.choice(len(values), count, p=freqs)
        return [values[i] for i in indices]

    def _is_categorical(self, stats: dict) -> bool:
        """Determine if a column is categorical based on cardinality."""
        distinct = stats.get("distinct_count", 0)
        return distinct > 0 and distinct <= 100

    def _generate_from_pattern(self, pattern: str) -> str:
        """Generate a string matching a simple regex pattern."""
        result = []
        i = 0
        while i < len(pattern):
            c = pattern[i]
            if c == "\\d":
                result.append(str(self.py_rng.randint(0, 9)))
                i += 2
            elif c == "[" and "]" in pattern[i:]:
                end = pattern.index("]", i)
                chars = pattern[i + 1 : end]
                if "A-Z" in chars:
                    result.append(self.py_rng.choice(string.ascii_uppercase))
                elif "a-z" in chars:
                    result.append(self.py_rng.choice(string.ascii_lowercase))
                elif "0-9" in chars:
                    result.append(str(self.py_rng.randint(0, 9)))
                else:
                    result.append(self.py_rng.choice(chars))
                i = end + 1
            elif c in string.ascii_letters or c in "-_.@":
                result.append(c)
                i += 1
            else:
                i += 1
        return "".join(result) if result else "synth_value"

    @staticmethod
    def _parse_date(val: Any):
        """Parse a date string or return a default."""
        from datetime import date
        if isinstance(val, date):
            return val
        try:
            return date.fromisoformat(str(val)[:10])
        except (ValueError, TypeError):
            return date(2020, 1, 1)
