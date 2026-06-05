"""
dataframe_transformer.py
~~~~~~~~~~~~~~~~~~~~~~~~
Fluent DataFrame API transformer for common ETL operations:
  - Column renaming & casting
  - Null handling
  - Deduplication
  - Adding audit columns (etl_ts, batch_id, source_system)
  - Flattening nested / semi-structured JSON fields
"""

from datetime import datetime
from typing import Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DataType

from etl.transformers.base_transformer import BaseTransformer


class DataFrameTransformer(BaseTransformer):
    """
    Chainable transformer that applies a sequence of DataFrame operations.

    Example
    -------
    result = (
        DataFrameTransformer("orders_clean")
        .rename_columns({"order_id": "id", "customer_name": "customer"})
        .cast_columns({"amount": "double", "order_date": "date"})
        .fill_nulls({"amount": 0.0, "status": "UNKNOWN"})
        .drop_duplicates(["id"])
        .add_audit_columns(batch_id="2024-01-01", source="orders_csv")
        .transform(raw_df)
    )
    """

    def __init__(self, name: str):
        super().__init__(name)
        self._steps: List[callable] = []

    # ------------------------------------------------------------------
    # Builder methods
    # ------------------------------------------------------------------

    def rename_columns(self, mapping: Dict[str, str]) -> "DataFrameTransformer":
        def _step(df):
            for old, new in mapping.items():
                if old in df.columns:
                    df = df.withColumnRenamed(old, new)
            return df
        self._steps.append(("rename_columns", _step))
        return self

    def cast_columns(self, mapping: Dict[str, str]) -> "DataFrameTransformer":
        def _step(df):
            for col, dtype in mapping.items():
                if col in df.columns:
                    df = df.withColumn(col, F.col(col).cast(dtype))
            return df
        self._steps.append(("cast_columns", _step))
        return self

    def fill_nulls(self, mapping: Dict[str, any]) -> "DataFrameTransformer":
        def _step(df):
            return df.fillna(mapping)
        self._steps.append(("fill_nulls", _step))
        return self

    def drop_columns(self, cols: List[str]) -> "DataFrameTransformer":
        def _step(df):
            return df.drop(*[c for c in cols if c in df.columns])
        self._steps.append(("drop_columns", _step))
        return self

    def drop_duplicates(self, subset: Optional[List[str]] = None) -> "DataFrameTransformer":
        def _step(df):
            return df.dropDuplicates(subset) if subset else df.dropDuplicates()
        self._steps.append(("drop_duplicates", _step))
        return self

    def filter_rows(self, condition: str) -> "DataFrameTransformer":
        def _step(df):
            return df.filter(condition)
        self._steps.append(("filter_rows", _step))
        return self

    def add_audit_columns(
        self,
        batch_id: Optional[str] = None,
        source: Optional[str] = None,
    ) -> "DataFrameTransformer":
        def _step(df):
            df = df.withColumn("_etl_loaded_at", F.current_timestamp())
            df = df.withColumn("_batch_id", F.lit(batch_id or datetime.utcnow().strftime("%Y%m%d%H%M%S")))
            if source:
                df = df.withColumn("_source_system", F.lit(source))
            return df
        self._steps.append(("add_audit_columns", _step))
        return self

    def flatten_json_column(
        self, col_name: str, prefix: Optional[str] = None
    ) -> "DataFrameTransformer":
        """Expand a MapType or StructType column into individual columns."""
        def _step(df):
            if col_name not in df.columns:
                self.logger.warning("Column '%s' not found — skipping flatten.", col_name)
                return df
            field = [f for f in df.schema.fields if f.name == col_name][0]
            from pyspark.sql.types import StructType, MapType
            if isinstance(field.dataType, StructType):
                for sub_field in field.dataType.fields:
                    new_name = f"{prefix}_{sub_field.name}" if prefix else sub_field.name
                    df = df.withColumn(new_name, F.col(f"{col_name}.{sub_field.name}"))
                df = df.drop(col_name)
            elif isinstance(field.dataType, MapType):
                self.logger.warning(
                    "MapType flattening requires known keys; skipping '%s'.", col_name
                )
            return df
        self._steps.append((f"flatten_{col_name}", _step))
        return self

    # ------------------------------------------------------------------
    # Execute
    # ------------------------------------------------------------------

    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        for step_name, step_fn in self._steps:
            self.logger.debug("Applying step: %s", step_name)
            df = step_fn(df)
        self.logger.info("[%s] transformation complete — %d cols", self.name, len(df.columns))
        return df
