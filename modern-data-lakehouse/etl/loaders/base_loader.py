"""
base_loader.py
~~~~~~~~~~~~~~
Abstract base class for warehouse loaders.
"""

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame

from utils.logger import get_logger
from utils.spark_session import get_spark


class BaseLoader(ABC):
    def __init__(self, target_table: str):
        self.target_table = target_table
        self.spark = get_spark()
        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def load(self, df: DataFrame, **kwargs) -> None:
        """Write `df` to the target table."""
        ...

    def _log_load(self, df: DataFrame) -> None:
        self.logger.info(
            "Loading %d rows into [%s]", df.count(), self.target_table
        )
