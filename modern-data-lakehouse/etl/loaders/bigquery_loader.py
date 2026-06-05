"""
bigquery_loader.py
~~~~~~~~~~~~~~~~~~
Writes Spark DataFrames to Google BigQuery using the BigQuery Spark connector.
Supports: append, overwrite, and merge (via MERGE DML) modes.
"""

from typing import List, Optional

from pyspark.sql import DataFrame

from etl.loaders.base_loader import BaseLoader
from utils.config_loader import get_config


class BigQueryLoader(BaseLoader):
    """
    Load a DataFrame into a BigQuery table.

    Parameters
    ----------
    target_table : str
        Destination table in "dataset.table" or "project.dataset.table" format.
    write_mode : str
        "append" | "overwrite" | "merge"
    merge_keys : list of str
        Primary key columns for MERGE. Required when write_mode="merge".
    partition_field : str, optional
        Column name to use for date/range partitioning.
    cluster_fields : list of str, optional
        Columns for clustering (improves query cost).
    """

    def __init__(
        self,
        target_table: str,
        write_mode: str = "append",
        merge_keys: Optional[List[str]] = None,
        partition_field: Optional[str] = None,
        cluster_fields: Optional[List[str]] = None,
    ):
        super().__init__(target_table)
        self.write_mode = write_mode
        self.merge_keys = merge_keys or []
        self.partition_field = partition_field
        self.cluster_fields = cluster_fields or []

        cfg = get_config()
        bq_cfg = cfg.get("bigquery", {})
        connector_cfg = bq_cfg.get("spark_connector", {})

        self._project = bq_cfg.get("project", "")
        self._dataset = bq_cfg.get("dataset", "")
        self._temp_bucket = connector_cfg.get("temporaryGcsBucket", "")
        self._materialization_dataset = connector_cfg.get(
            "materializationDataset", "lakehouse_staging"
        )

        # Resolve full table reference
        if "." not in target_table:
            self._full_table = f"{self._project}.{self._dataset}.{target_table}"
        else:
            self._full_table = target_table

    def load(self, df: DataFrame, **kwargs) -> None:
        self._log_load(df)

        bq_write_mode = {
            "append": "append",
            "overwrite": "overwrite",
            "merge": "overwrite",  # merge uses staging overwrite first
        }.get(self.write_mode, "append")

        target = self._full_table if self.write_mode != "merge" else self._staging_table()

        writer = (
            df.write
            .format("bigquery")
            .option("table", target)
            .option("temporaryGcsBucket", self._temp_bucket)
            .mode(bq_write_mode)
        )

        if self.partition_field:
            writer = writer.option("partitionField", self.partition_field)
        if self.cluster_fields:
            writer = writer.option("clusteredFields", ",".join(self.cluster_fields))

        writer.save()
        self.logger.info("BigQuery write complete: [%s] mode=%s", target, bq_write_mode)

        if self.write_mode == "merge":
            self._run_merge_dml(df)

    # ------------------------------------------------------------------
    def _staging_table(self) -> str:
        parts = self._full_table.split(".")
        parts[-2] = self._materialization_dataset
        parts[-1] = f"{parts[-1]}_etl_stage"
        return ".".join(parts)

    def _run_merge_dml(self, df: DataFrame) -> None:
        if not self.merge_keys:
            raise ValueError("merge_keys must be set for merge mode.")

        from google.cloud import bigquery
        client = bigquery.Client(project=self._project)

        staging = self._staging_table()
        join_clause = " AND ".join([f"T.{k} = S.{k}" for k in self.merge_keys])
        update_cols = [c for c in df.columns if c not in self.merge_keys]
        update_clause = ", ".join([f"T.{c} = S.{c}" for c in update_cols])
        insert_cols = ", ".join(df.columns)
        insert_vals = ", ".join([f"S.{c}" for c in df.columns])

        merge_sql = f"""
            MERGE `{self._full_table}` T
            USING `{staging}` S
            ON {join_clause}
            WHEN MATCHED THEN
              UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN
              INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        self.logger.info("Running BigQuery MERGE DML into [%s]", self._full_table)
        query_job = client.query(merge_sql)
        query_job.result()  # Block until complete
        self.logger.info(
            "MERGE complete — rows affected: %s", query_job.num_dml_affected_rows
        )
