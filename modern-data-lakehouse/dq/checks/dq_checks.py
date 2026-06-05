"""
dq_checks.py
~~~~~~~~~~~~
Data quality checks applied after transformations and before loading.

Each check returns a DQResult with:
  - passed (bool)
  - check_name (str)
  - details (dict)

The DQRunner collects all results and raises on failures if configured.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.config_loader import get_config
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class DQResult:
    check_name: str
    passed: bool
    details: Dict = field(default_factory=dict)

    def __str__(self):
        status = "✅ PASS" if self.passed else "❌ FAIL"
        return f"{status}  [{self.check_name}]  {self.details}"


# ---------------------------------------------------------------------------
# Individual check functions
# ---------------------------------------------------------------------------

def check_not_empty(df: DataFrame, table_name: str = "") -> DQResult:
    """Fail if the DataFrame has zero rows."""
    count = df.count()
    return DQResult(
        check_name="not_empty",
        passed=count > 0,
        details={"row_count": count, "table": table_name},
    )


def check_no_nulls(
    df: DataFrame,
    columns: List[str],
    threshold: Optional[float] = None,
) -> List[DQResult]:
    """
    Check that specified columns have an acceptable null rate.
    threshold: max allowed fraction of nulls (0.0 = zero nulls allowed).
    """
    cfg = get_config()
    threshold = threshold if threshold is not None else cfg["data_quality"]["null_threshold"]
    total = df.count()
    results = []

    for col in columns:
        if col not in df.columns:
            results.append(DQResult(
                check_name=f"no_nulls:{col}",
                passed=False,
                details={"error": f"Column '{col}' not found"},
            ))
            continue

        null_count = df.filter(F.col(col).isNull()).count()
        null_rate = null_count / total if total > 0 else 0.0
        passed = null_rate <= threshold
        results.append(DQResult(
            check_name=f"no_nulls:{col}",
            passed=passed,
            details={
                "null_count": null_count,
                "null_rate": round(null_rate, 4),
                "threshold": threshold,
            },
        ))

    return results


def check_unique(df: DataFrame, columns: List[str]) -> DQResult:
    """Fail if the combination of `columns` contains duplicates."""
    total = df.count()
    distinct = df.select(*columns).distinct().count()
    passed = total == distinct
    return DQResult(
        check_name=f"unique:{','.join(columns)}",
        passed=passed,
        details={"total_rows": total, "distinct_rows": distinct, "duplicates": total - distinct},
    )


def check_value_range(
    df: DataFrame,
    column: str,
    min_val=None,
    max_val=None,
) -> DQResult:
    """Fail if any value in `column` falls outside [min_val, max_val]."""
    condition = F.lit(True)
    if min_val is not None:
        condition = condition & (F.col(column) >= min_val)
    if max_val is not None:
        condition = condition & (F.col(column) <= max_val)

    out_of_range = df.filter(~condition).count()
    return DQResult(
        check_name=f"value_range:{column}",
        passed=out_of_range == 0,
        details={"out_of_range_count": out_of_range, "min": min_val, "max": max_val},
    )


def check_allowed_values(
    df: DataFrame, column: str, allowed: List
) -> DQResult:
    """Fail if `column` contains values not in the allowed list."""
    violations = df.filter(~F.col(column).isin(allowed)).count()
    return DQResult(
        check_name=f"allowed_values:{column}",
        passed=violations == 0,
        details={"violation_count": violations, "allowed": allowed},
    )


def check_schema(df: DataFrame, expected_cols: List[str]) -> DQResult:
    """Fail if any expected column is missing from the DataFrame."""
    missing = [c for c in expected_cols if c not in df.columns]
    return DQResult(
        check_name="schema_check",
        passed=len(missing) == 0,
        details={"missing_columns": missing, "actual_columns": df.columns},
    )


# ---------------------------------------------------------------------------
# DQ Runner
# ---------------------------------------------------------------------------

class DQRunner:
    """
    Collect and evaluate a suite of DQ checks.
    Raises RuntimeError on failure if `fail_on_error=True` (from config).
    """

    def __init__(self, table_name: str):
        self.table_name = table_name
        self._results: List[DQResult] = []
        self._fail_on_error = get_config()["data_quality"]["fail_on_error"]

    def add_result(self, result) -> None:
        if isinstance(result, list):
            self._results.extend(result)
        else:
            self._results.append(result)

    def evaluate(self) -> List[DQResult]:
        failures = [r for r in self._results if not r.passed]

        for r in self._results:
            logger.info("[DQ][%s] %s", self.table_name, r)

        if failures and self._fail_on_error:
            summary = "\n".join(str(r) for r in failures)
            raise RuntimeError(
                f"Data quality failed for [{self.table_name}] — "
                f"{len(failures)} check(s):\n{summary}"
            )

        return self._results
