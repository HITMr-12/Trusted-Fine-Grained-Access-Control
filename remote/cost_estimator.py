"""Policy-aware cardinality estimates for opaque Remote Scan nodes.

The module deliberately depends only on Spark DataFrames, so the same estimator can
be called by the Remote API and by an offline benchmark.  ``observe`` is test-only:
production callers receive estimates, never the protected cardinality itself.
"""
from dataclasses import asdict, dataclass
from math import ceil, sqrt
from time import perf_counter

from pyspark.sql import DataFrame


@dataclass
class CostEnvelope:
    method: str
    estimated_rows: int
    lower_rows: int
    upper_rows: int
    estimated_bytes: int
    row_width_bytes: int
    confidence: float
    estimation_ms: float

    def to_dict(self):
        return asdict(self)


def estimate(df: DataFrame, method: str, base_rows: int, row_width: int = 32,
             sample_fraction: float = 0.01, seed: int = 42) -> CostEnvelope:
    """Estimate a fully policy-filtered and query-filtered Remote Scan output."""
    started = perf_counter()
    if method == "fixed":
        rows, lower, upper, confidence = 1000, 0, max(base_rows, 1000), 0.0
    elif method == "heuristic":
        # Conventional fallback: equality policy (10%) and one residual predicate (1/3).
        rows = max(1, round(base_rows * 0.1 / 3.0))
        lower, upper, confidence = 0, base_rows, 0.1
    elif method == "policy_sample":
        sampled = df.sample(False, sample_fraction, seed).count()
        rows = max(0, round(sampled / sample_fraction))
        # Normal approximation; deliberately exposed as a range to the optimizer.
        standard_error = sqrt(max(rows * (1.0 - sample_fraction), 1.0) / sample_fraction)
        margin = ceil(1.96 * standard_error)
        lower, upper, confidence = max(0, rows - margin), min(base_rows, rows + margin), 0.95
    else:
        raise ValueError(f"unknown estimate method: {method}")
    elapsed = (perf_counter() - started) * 1000
    return CostEnvelope(method, rows, lower, upper, rows * row_width,
                        row_width, confidence, round(elapsed, 3))


def q_error(estimated: int, actual: int) -> float:
    if estimated == actual == 0:
        return 1.0
    return max((estimated + 1) / (actual + 1), (actual + 1) / (estimated + 1))
