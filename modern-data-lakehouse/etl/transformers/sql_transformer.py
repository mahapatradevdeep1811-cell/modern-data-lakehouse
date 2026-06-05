"""
sql_transformer.py
~~~~~~~~~~~~~~~~~~
Execute Spark SQL transformations from inline strings or .sql files.
Supports multi-step pipelines where the output of one SQL becomes the
input view for the next.
"""

from pathlib import Path
from typing import Dict, List, Optional, Union

from pyspark.sql import DataFrame

from etl.transformers.base_transformer import BaseTransformer


class SqlTransformer(BaseTransformer):
    """
    Transform a DataFrame using one or more Spark SQL statements.

    Usage — single SQL string:
        SqlTransformer("clean_orders", sql="SELECT * FROM src WHERE amount > 0")

    Usage — SQL file:
        SqlTransformer("clean_orders", sql_file="etl/sql/clean_orders.sql")

    Usage — pipeline (multiple steps):
        SqlTransformer(
            "order_pipeline",
            pipeline=[
                ("filter_step",   "SELECT * FROM src WHERE amount > 0"),
                ("enrich_step",   "SELECT *, current_timestamp() AS etl_ts FROM filter_step"),
            ]
        )
    """

    def __init__(
        self,
        name: str,
        sql: Optional[str] = None,
        sql_file: Optional[Union[str, Path]] = None,
        pipeline: Optional[List[tuple]] = None,
        params: Optional[Dict[str, str]] = None,
    ):
        super().__init__(name)
        if not any([sql, sql_file, pipeline]):
            raise ValueError("Provide one of: sql, sql_file, or pipeline.")

        self._sql = sql
        self._sql_file = Path(sql_file) if sql_file else None
        self._pipeline = pipeline or []
        self._params = params or {}

    # ------------------------------------------------------------------
    def transform(self, df: DataFrame, input_view: str = "src", **kwargs) -> DataFrame:
        """
        Register `df` as `input_view`, run SQL, return result.
        """
        self._register_temp_view(df, input_view)

        if self._pipeline:
            return self._run_pipeline()
        elif self._sql_file:
            return self._run_file(input_view)
        else:
            return self._run_sql(self._sql, label=self.name)

    # ------------------------------------------------------------------
    def _run_sql(self, sql: str, label: str) -> DataFrame:
        resolved = self._resolve_params(sql)
        self.logger.info("Running SQL step [%s]", label)
        self.logger.debug("SQL:\n%s", resolved)
        return self.spark.sql(resolved)

    def _run_file(self, input_view: str) -> DataFrame:
        if not self._sql_file.exists():
            raise FileNotFoundError(f"SQL file not found: {self._sql_file}")
        sql = self._sql_file.read_text()
        return self._run_sql(sql, label=self._sql_file.name)

    def _run_pipeline(self) -> DataFrame:
        result = None
        for step_name, sql in self._pipeline:
            result = self._run_sql(sql, label=step_name)
            self._register_temp_view(result, step_name)
            self.logger.info("Step [%s] produced %d rows", step_name, result.count())
        return result

    def _resolve_params(self, sql: str) -> str:
        """Replace {param} placeholders with values from self._params."""
        for key, value in self._params.items():
            sql = sql.replace(f"{{{key}}}", value)
        return sql
