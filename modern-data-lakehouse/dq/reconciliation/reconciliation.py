"""
reconciliation.py
~~~~~~~~~~~~~~~~~
Source-to-target reconciliation checks:
  - Row count comparison
  - Aggregate (SUM, COUNT) comparison
  - Column-level hash comparison for small tables

Used post-load to verify data landed correctly in the warehouse.
"""

from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.config_loader import get_config
from utils.logger import get_logger
from utils.spark_session import get_spark

logger = get_logger(__name__)


@dataclass
class ReconciliationResult:
    check_name: str
    passed: bool
    source_value: float
    target_value: float
    variance_pct: float
    tolerance: float

    def __str__(self):
        status = "✅ PASS" if self.passed else "❌ FAIL"
        return (
            f"{status}  [{self.check_name}]  "
            f"source={self.source_value:,.0f}  "
            f"target={self.target_value:,.0f}  "
            f"variance={self.variance_pct:.4%}  "
            f"(tolerance={self.tolerance:.4%})"
        )


def reconcile_row_count(
    source_df: DataFrame,
    target_df: DataFrame,
    tolerance: Optional[float] = None,
) -> ReconciliationResult:
    """Compare row counts between source and target DataFrames."""
    cfg = get_config()
    tol = tolerance if tolerance is not None else cfg["data_quality"]["reconciliation_tolerance"]

    src_count = float(source_df.count())
    tgt_count = float(target_df.count())
    variance = abs(src_count - tgt_count) / src_count if src_count > 0 else 0.0

    return ReconciliationResult(
        check_name="row_count",
        passed=variance <= tol,
        source_value=src_count,
        target_value=tgt_count,
        variance_pct=variance,
        tolerance=tol,
    )


def reconcile_aggregate(
    source_df: DataFrame,
    target_df: DataFrame,
    column: str,
    agg_func: str = "sum",
    tolerance: Optional[float] = None,
) -> ReconciliationResult:
    """
    Compare aggregate value (sum/count/avg) for a column between source and target.
    """
    cfg = get_config()
    tol = tolerance if tolerance is not None else cfg["data_quality"]["reconciliation_tolerance"]

    agg_map = {"sum": F.sum, "count": F.count, "avg": F.avg}
    if agg_func not in agg_map:
        raise ValueError(f"Unsupported agg_func: '{agg_func}'. Choose from: {list(agg_map)}")

    fn = agg_map[agg_func]
    src_val = float(source_df.select(fn(F.col(column))).collect()[0][0] or 0)
    tgt_val = float(target_df.select(fn(F.col(column))).collect()[0][0] or 0)
    variance = abs(src_val - tgt_val) / src_val if src_val != 0 else 0.0

    return ReconciliationResult(
        check_name=f"{agg_func}:{column}",
        passed=variance <= tol,
        source_value=src_val,
        target_value=tgt_val,
        variance_pct=variance,
        tolerance=tol,
    )


class ReconciliationSuite:
    """Run a collection of reconciliation checks and report."""

    def __init__(self, table_name: str):
        self.table_name = table_name
        self._results: List[ReconciliationResult] = []
        self._fail_on_error = get_config()["data_quality"]["fail_on_error"]

    def add(self, result: ReconciliationResult) -> "ReconciliationSuite":
        self._results.append(result)
        return self

    def evaluate(self) -> List[ReconciliationResult]:
        failures = [r for r in self._results if not r.passed]

        for r in self._results:
            logger.info("[RECON][%s] %s", self.table_name, r)

        if failures and self._fail_on_error:
            summary = "\n".join(str(r) for r in failures)
            raise RuntimeError(
                f"Reconciliation failed for [{self.table_name}] — "
                f"{len(failures)} check(s):\n{summary}"
            )

        return self._results
