"""
test_dq_checks.py
~~~~~~~~~~~~~~~~~
Unit tests for the data quality check functions.
Uses chispa + pytest; no actual Spark cluster needed (local[*]).
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)

from dq.checks.dq_checks import (
    DQRunner,
    check_allowed_values,
    check_no_nulls,
    check_not_empty,
    check_schema,
    check_unique,
    check_value_range,
)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("dq_unit_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture()
def sample_df(spark):
    schema = StructType([
        StructField("id",     IntegerType(), False),
        StructField("name",   StringType(),  True),
        StructField("amount", DoubleType(),  True),
        StructField("status", StringType(),  True),
    ])
    data = [
        (1, "Alice", 100.0, "ACTIVE"),
        (2, "Bob",   200.0, "ACTIVE"),
        (3, "Carol",  50.0, "INACTIVE"),
    ]
    return spark.createDataFrame(data, schema)


# ---------------------------------------------------------------------------

class TestCheckNotEmpty:
    def test_passes_non_empty(self, sample_df):
        result = check_not_empty(sample_df)
        assert result.passed

    def test_fails_empty(self, spark):
        empty = spark.createDataFrame([], schema="id INT")
        result = check_not_empty(empty)
        assert not result.passed


class TestCheckNoNulls:
    def test_passes_no_nulls(self, sample_df):
        results = check_no_nulls(sample_df, ["id", "status"], threshold=0.0)
        assert all(r.passed for r in results)

    def test_fails_null_above_threshold(self, spark):
        data = [(1, None), (2, "x"), (3, "y"), (4, None)]
        df = spark.createDataFrame(data, "id INT, name STRING")
        results = check_no_nulls(df, ["name"], threshold=0.0)
        assert not results[0].passed

    def test_passes_null_within_threshold(self, spark):
        data = [(1, None), (2, "x"), (3, "y"), (4, "z")]
        df = spark.createDataFrame(data, "id INT, name STRING")
        results = check_no_nulls(df, ["name"], threshold=0.3)  # 25% < 30%
        assert results[0].passed


class TestCheckUnique:
    def test_passes_unique(self, sample_df):
        result = check_unique(sample_df, ["id"])
        assert result.passed

    def test_fails_duplicates(self, spark):
        data = [(1, "a"), (1, "b"), (2, "c")]
        df = spark.createDataFrame(data, "id INT, val STRING")
        result = check_unique(df, ["id"])
        assert not result.passed
        assert result.details["duplicates"] == 1


class TestCheckValueRange:
    def test_passes_within_range(self, sample_df):
        result = check_value_range(sample_df, "amount", min_val=0, max_val=500)
        assert result.passed

    def test_fails_below_min(self, spark):
        data = [(1, -10.0), (2, 50.0)]
        df = spark.createDataFrame(data, "id INT, amount DOUBLE")
        result = check_value_range(df, "amount", min_val=0)
        assert not result.passed


class TestCheckSchema:
    def test_passes_all_present(self, sample_df):
        result = check_schema(sample_df, ["id", "name", "amount"])
        assert result.passed

    def test_fails_missing_column(self, sample_df):
        result = check_schema(sample_df, ["id", "nonexistent_col"])
        assert not result.passed
        assert "nonexistent_col" in result.details["missing_columns"]


class TestCheckAllowedValues:
    def test_passes_valid_values(self, sample_df):
        result = check_allowed_values(sample_df, "status", ["ACTIVE", "INACTIVE"])
        assert result.passed

    def test_fails_invalid_value(self, spark):
        data = [(1, "ACTIVE"), (2, "DELETED")]
        df = spark.createDataFrame(data, "id INT, status STRING")
        result = check_allowed_values(df, "status", ["ACTIVE", "INACTIVE"])
        assert not result.passed
        assert result.details["violation_count"] == 1


class TestDQRunner:
    def test_runner_collects_and_passes(self, sample_df):
        runner = DQRunner("test_table")
        runner.add_result(check_not_empty(sample_df))
        runner.add_result(check_unique(sample_df, ["id"]))
        results = runner.evaluate()
        assert all(r.passed for r in results)
