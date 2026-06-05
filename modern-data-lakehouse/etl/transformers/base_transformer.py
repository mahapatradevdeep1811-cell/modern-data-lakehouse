"""
base_transformer.py
~~~~~~~~~~~~~~~~~~~
Abstract base class for all transformers.
"""

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame

from utils.logger import get_logger
from utils.spark_session import get_spark


class BaseTransformer(ABC):
    def __init__(self, name: str):
        self.name = name
        self.spark = get_spark()
        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def transform(self, df: DataFrame, **kwargs) -> DataFrame:
        """Apply transformations and return the result DataFrame."""
        ...

    def _register_temp_view(self, df: DataFrame, view_name: str) -> None:
        df.createOrReplaceTempView(view_name)
        self.logger.debug("Registered temp view: %s", view_name)
