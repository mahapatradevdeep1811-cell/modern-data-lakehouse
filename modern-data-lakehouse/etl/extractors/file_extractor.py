"""
file_extractor.py
~~~~~~~~~~~~~~~~~
Extractor for structured and semi-structured files:
  - CSV
  - JSON / JSONL (newline-delimited)
  - Parquet
  - Delta Lake tables
"""

from typing import Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from etl.extractors.base_extractor import BaseExtractor


class FileExtractor(BaseExtractor):
    """
    Read files from the local filesystem, S3, GCS, or ADLS.

    Parameters
    ----------
    source_id : str
        Logical name for this source (used in logging & lineage).
    path : str
        File or directory path (supports glob patterns).
    format : str
        One of: csv, json, jsonl, parquet, delta, orc.
    schema : StructType, optional
        Explicit schema. If None, schema is inferred (slower on large files).
    options : dict, optional
        Extra reader options forwarded to Spark (e.g. header, multiLine).
    """

    SUPPORTED_FORMATS = {"csv", "json", "jsonl", "parquet", "delta", "orc"}

    def __init__(
        self,
        source_id: str,
        path: str,
        format: str,
        schema: Optional[StructType] = None,
        options: Optional[Dict[str, str]] = None,
    ):
        super().__init__(source_id)
        if format not in self.SUPPORTED_FORMATS:
            raise ValueError(
                f"Unsupported format '{format}'. Choose from: {self.SUPPORTED_FORMATS}"
            )
        self.path = path
        self.format = format
        self.schema = schema
        self.options = options or {}

    def extract(self, **kwargs) -> DataFrame:
        self.logger.info("Reading [%s] from path: %s", self.format.upper(), self.path)

        # Normalise format aliases
        spark_format = "json" if self.format == "jsonl" else self.format

        reader = self.spark.read.format(spark_format)

        # Apply default options per format
        if self.format == "csv":
            defaults = {"header": "true", "inferSchema": "true", "nullValue": "NULL"}
            for k, v in defaults.items():
                if k not in self.options:
                    self.options[k] = v

        if self.format == "jsonl":
            self.options.setdefault("multiLine", "false")

        for k, v in self.options.items():
            reader = reader.option(k, v)

        if self.schema:
            reader = reader.schema(self.schema)

        df = reader.load(self.path)
        self._log_stats(df)
        return df
