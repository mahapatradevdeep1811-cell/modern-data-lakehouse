"""
spark_lakehouse_operator.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Custom Airflow operator that runs a PySpark ETL module as a spark-submit job,
with automatic Snowflake/BigQuery connector JAR injection and structured logging.
"""

from __future__ import annotations

import os
from typing import Dict, List, Optional

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class SparkLakehouseOperator(BaseOperator):
    """
    Submit a PySpark script via spark-submit with the correct JARs and configs.

    Parameters
    ----------
    script_path : str
        Path to the Python script relative to the project root.
    warehouse_target : str, optional
        "snowflake" or "bigquery". Defaults to WAREHOUSE_TARGET env var.
    spark_conf : dict, optional
        Additional Spark configs (merged with base config).
    py_files : list of str, optional
        Extra Python files/zips to ship to executors.
    application_args : list of str, optional
        Arguments passed to the script's main().
    """

    ui_color = "#3d85c8"
    template_fields = ("script_path", "application_args")

    CONNECTOR_PACKAGES = {
        "snowflake": [
            "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4",
            "net.snowflake:snowflake-jdbc:3.14.4",
        ],
        "bigquery": [
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0",
        ],
    }
    DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.1.0"

    @apply_defaults
    def __init__(
        self,
        script_path: str,
        warehouse_target: Optional[str] = None,
        spark_conf: Optional[Dict[str, str]] = None,
        py_files: Optional[List[str]] = None,
        application_args: Optional[List[str]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.script_path = script_path
        self.warehouse_target = warehouse_target or os.getenv("WAREHOUSE_TARGET", "snowflake")
        self.spark_conf = spark_conf or {}
        self.py_files = py_files or []
        self.application_args = application_args or []

    def execute(self, context):
        import subprocess

        packages = [self.DELTA_PACKAGE] + self.CONNECTOR_PACKAGES.get(self.warehouse_target, [])

        cmd = [
            "spark-submit",
            "--packages", ",".join(packages),
        ]

        # Merge base spark conf with extra conf
        base_conf = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
        }
        merged_conf = {**base_conf, **self.spark_conf}
        for k, v in merged_conf.items():
            cmd += ["--conf", f"{k}={v}"]

        if self.py_files:
            cmd += ["--py-files", ",".join(self.py_files)]

        cmd.append(self.script_path)
        cmd.extend(self.application_args)

        self.log.info("spark-submit command: %s", " ".join(cmd))

        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            self.log.error("Spark job failed:\n%s", result.stderr)
            raise RuntimeError(f"spark-submit failed (exit {result.returncode})")

        self.log.info("Spark job output:\n%s", result.stdout)
        return result.returncode
