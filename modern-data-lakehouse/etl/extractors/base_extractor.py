"""
base_extractor.py
~~~~~~~~~~~~~~~~~
Abstract base class for all data source extractors.
"""

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame

from utils.logger import get_logger
from utils.spark_session import get_spark


class BaseExtractor(ABC):
    """
    Every extractor reads from a source and returns a raw Spark DataFrame.
    Subclasses must implement `extract()`.
    """

    def __init__(self, source_id: str):
        self.source_id = source_id
        self.spark = get_spark()
        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def extract(self, **kwargs) -> DataFrame:
        """Read raw data and return a DataFrame."""
        ...

    def _log_stats(self, df: DataFrame) -> None:
        count = df.count()
        cols = len(df.columns)
        self.logger.info(
            "Extracted [%s]: %d rows × %d columns", self.source_id, count, cols
        )
