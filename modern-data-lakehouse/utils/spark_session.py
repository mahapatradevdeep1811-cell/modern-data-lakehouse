"""
spark_session.py
~~~~~~~~~~~~~~~~
Singleton Spark session factory.
Applies all configs from base.yaml and injects the correct warehouse
connector JARs based on the active warehouse target.
"""

from __future__ import annotations

import logging
from typing import Optional

from pyspark.sql import SparkSession

from utils.config_loader import get_config, get_warehouse_target

logger = logging.getLogger(__name__)

_SESSION: Optional[SparkSession] = None

# Maven coordinates for warehouse connectors
_CONNECTOR_PACKAGES = {
    "snowflake": [
        "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4",
        "net.snowflake:snowflake-jdbc:3.14.4",
    ],
    "bigquery": [
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0",
    ],
}

# Delta Lake package (always included)
_DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.1.0"


def get_spark() -> SparkSession:
    """Return (or create) the global SparkSession."""
    global _SESSION

    if _SESSION is not None and not _SESSION.sparkContext._jsc.sc().isStopped():
        return _SESSION

    cfg = get_config()
    spark_cfg = cfg.get("spark", {})
    target = get_warehouse_target()

    packages = [_DELTA_PACKAGE] + _CONNECTOR_PACKAGES.get(target, [])
    packages_str = ",".join(packages)

    logger.info("Initialising Spark session (warehouse: %s)", target)

    builder = (
        SparkSession.builder
        .appName(spark_cfg.get("app_name", "LakehousePipeline"))
        .master(spark_cfg.get("master", "local[*]"))
        .config("spark.jars.packages", packages_str)
    )

    for key, value in spark_cfg.get("configs", {}).items():
        builder = builder.config(key, value)

    _SESSION = builder.getOrCreate()
    _SESSION.sparkContext.setLogLevel(cfg.get("app", {}).get("log_level", "WARN"))

    logger.info("Spark session started: %s", _SESSION.sparkContext.applicationId)
    return _SESSION


def stop_spark() -> None:
    """Gracefully stop the Spark session."""
    global _SESSION
    if _SESSION:
        _SESSION.stop()
        _SESSION = None
        logger.info("Spark session stopped.")
