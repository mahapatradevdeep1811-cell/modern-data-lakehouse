"""
jdbc_extractor.py
~~~~~~~~~~~~~~~~~
Extractor for any JDBC-compatible source (PostgreSQL, MySQL, Oracle, etc.).
Supports partitioned reads for large tables.
"""

from typing import Dict, Optional

from pyspark.sql import DataFrame

from etl.extractors.base_extractor import BaseExtractor


class JdbcExtractor(BaseExtractor):
    """
    Read a table or query result from a JDBC source.

    For large tables, set `partition_column`, `lower_bound`, `upper_bound`,
    and `num_partitions` to enable parallel reads.
    """

    def __init__(
        self,
        source_id: str,
        jdbc_url: str,
        table_or_query: str,
        driver: str,
        user: str,
        password: str,
        extra_options: Optional[Dict[str, str]] = None,
        partition_column: Optional[str] = None,
        lower_bound: Optional[int] = None,
        upper_bound: Optional[int] = None,
        num_partitions: int = 4,
    ):
        super().__init__(source_id)
        self.jdbc_url = jdbc_url
        self.table_or_query = table_or_query
        self.driver = driver
        self.user = user
        self.password = password
        self.extra_options = extra_options or {}
        self.partition_column = partition_column
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
        self.num_partitions = num_partitions

    def extract(self, **kwargs) -> DataFrame:
        self.logger.info("JDBC read [%s] from: %s", self.source_id, self.jdbc_url)

        # Wrap raw SQL in a subquery so Spark treats it as a table
        table = (
            self.table_or_query
            if not self.table_or_query.strip().upper().startswith("SELECT")
            else f"({self.table_or_query}) AS src"
        )

        options: Dict[str, str] = {
            "url": self.jdbc_url,
            "dbtable": table,
            "driver": self.driver,
            "user": self.user,
            "password": self.password,
            **self.extra_options,
        }

        if self.partition_column:
            if not all([self.lower_bound, self.upper_bound]):
                raise ValueError(
                    "lower_bound and upper_bound are required for partitioned JDBC reads."
                )
            options.update(
                {
                    "partitionColumn": self.partition_column,
                    "lowerBound": str(self.lower_bound),
                    "upperBound": str(self.upper_bound),
                    "numPartitions": str(self.num_partitions),
                }
            )

        df = self.spark.read.format("jdbc").options(**options).load()
        self._log_stats(df)
        return df
