"""
snowflake_loader.py
~~~~~~~~~~~~~~~~~~~
Writes Spark DataFrames to Snowflake using the official Spark-Snowflake connector.
Supports: append, overwrite, and merge (upsert) modes.
"""

from typing import List, Optional

from pyspark.sql import DataFrame

from etl.loaders.base_loader import BaseLoader
from utils.config_loader import get_config


class SnowflakeLoader(BaseLoader):
    """
    Load a DataFrame into a Snowflake table.

    Parameters
    ----------
    target_table : str
        Destination table name (unqualified; database/schema come from config).
    write_mode : str
        "append" | "overwrite" | "merge"
    merge_keys : list of str
        Primary key columns for MERGE (upsert). Required when write_mode="merge".
    """

    def __init__(
        self,
        target_table: str,
        write_mode: str = "append",
        merge_keys: Optional[List[str]] = None,
    ):
        super().__init__(target_table)
        self.write_mode = write_mode
        self.merge_keys = merge_keys or []

        cfg = get_config()
        sf_cfg = cfg.get("snowflake", {})
        connector_cfg = sf_cfg.get("spark_connector", {})

        self._sf_options = {
            "sfURL": connector_cfg.get("sfURL", ""),
            "sfUser": sf_cfg.get("user", ""),
            "sfPassword": sf_cfg.get("password", ""),
            "sfRole": sf_cfg.get("role", "SYSADMIN"),
            "sfWarehouse": connector_cfg.get("sfWarehouse", ""),
            "sfDatabase": connector_cfg.get("sfDatabase", ""),
            "sfSchema": connector_cfg.get("sfSchema", ""),
            "dbtable": target_table,
        }

    def load(self, df: DataFrame, **kwargs) -> None:
        self._log_load(df)

        if self.write_mode == "merge":
            self._merge(df)
        else:
            (
                df.write
                .format("net.snowflake.spark.snowflake")
                .options(**self._sf_options)
                .mode(self.write_mode)
                .save()
            )
            self.logger.info(
                "Snowflake write complete: [%s] mode=%s", self.target_table, self.write_mode
            )

    def _merge(self, df: DataFrame) -> None:
        """Upsert using Snowflake MERGE via a temp staging table."""
        if not self.merge_keys:
            raise ValueError("merge_keys must be set for merge mode.")

        staging_table = f"{self.target_table}_etl_stage"
        staging_opts = {**self._sf_options, "dbtable": staging_table}

        # Step 1: Write to staging
        (
            df.write
            .format("net.snowflake.spark.snowflake")
            .options(**staging_opts)
            .mode("overwrite")
            .save()
        )
        self.logger.info("Staging table [%s] loaded.", staging_table)

        # Step 2: Issue MERGE SQL via Snowflake JDBC
        join_clause = " AND ".join(
            [f"t.{k} = s.{k}" for k in self.merge_keys]
        )
        update_cols = [c for c in df.columns if c not in self.merge_keys]
        update_clause = ", ".join([f"t.{c} = s.{c}" for c in update_cols])
        insert_cols = ", ".join(df.columns)
        insert_vals = ", ".join([f"s.{c}" for c in df.columns])

        merge_sql = f"""
            MERGE INTO {self.target_table} t
            USING {staging_table} s ON {join_clause}
            WHEN MATCHED THEN UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        self.logger.info("Executing MERGE into [%s]", self.target_table)

        # Execute via Snowflake's run_query utility (using stored procedure or JDBC)
        self.spark.sparkContext._jvm \
            .net.snowflake.spark.snowflake.Utils \
            .runQuery(self._sf_options, merge_sql)

        self.logger.info("MERGE complete: [%s]", self.target_table)
