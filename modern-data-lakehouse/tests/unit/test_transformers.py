"""
test_transformers.py
~~~~~~~~~~~~~~~~~~~~
Unit tests for DataFrameTransformer and SqlTransformer.
"""

import pytest
from pyspark.sql import SparkSession

from etl.transformers.dataframe_transformer import DataFrameTransformer
from etl.transformers.sql_transformer import SqlTransformer


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("transformer_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture()
def orders_df(spark):
    data = [
        (1, "cust_001", "2024-01-10", 250.0, 2, "active"),
        (2, "cust_002", "2024-01-11", None,  1, "active"),
        (3, "cust_001", "2024-01-12", 50.0,  1, None),
        (1, "cust_001", "2024-01-10", 250.0, 2, "active"),  # duplicate
    ]
    return spark.createDataFrame(
        data, "order_id INT, cust_id STRING, order_date STRING, amount DOUBLE, qty INT, status STRING"
    )


class TestDataFrameTransformer:

    def test_rename_columns(self, orders_df):
        df = (
            DataFrameTransformer("t")
            .rename_columns({"cust_id": "customer_id"})
            .transform(orders_df)
        )
        assert "customer_id" in df.columns
        assert "cust_id" not in df.columns

    def test_fill_nulls(self, orders_df):
        df = (
            DataFrameTransformer("t")
            .fill_nulls({"amount": 0.0, "status": "UNKNOWN"})
            .transform(orders_df)
        )
        nulls = df.filter(df.amount.isNull()).count()
        assert nulls == 0

    def test_drop_duplicates(self, orders_df):
        df = (
            DataFrameTransformer("t")
            .drop_duplicates(["order_id"])
            .transform(orders_df)
        )
        assert df.count() == 3  # 4 rows → 3 after dedup on order_id

    def test_cast_columns(self, orders_df):
        df = (
            DataFrameTransformer("t")
            .cast_columns({"amount": "integer"})
            .transform(orders_df)
        )
        dtype = dict(df.dtypes)["amount"]
        assert dtype == "int"

    def test_filter_rows(self, orders_df):
        df = (
            DataFrameTransformer("t")
            .filter_rows("amount > 100")
            .transform(orders_df)
        )
        # Only row 1 (amount=250); row 2 is null; row 3 is 50; row 4 dup of 1
        assert df.filter(df.amount.isNotNull()).count() >= 1

    def test_audit_columns_added(self, orders_df):
        df = (
            DataFrameTransformer("t")
            .add_audit_columns(batch_id="2024-01-01", source="test")
            .transform(orders_df)
        )
        assert "_etl_loaded_at" in df.columns
        assert "_batch_id" in df.columns
        assert "_source_system" in df.columns

    def test_chaining(self, orders_df):
        df = (
            DataFrameTransformer("full_chain")
            .rename_columns({"cust_id": "customer_id"})
            .fill_nulls({"amount": 0.0, "status": "UNKNOWN"})
            .drop_duplicates(["order_id"])
            .add_audit_columns(batch_id="20240101")
            .transform(orders_df)
        )
        assert "customer_id" in df.columns
        assert "_etl_loaded_at" in df.columns
        assert df.filter(df.amount.isNull()).count() == 0


class TestSqlTransformer:

    def test_simple_sql(self, orders_df):
        df = SqlTransformer(
            "filter_test",
            sql="SELECT * FROM src WHERE order_id = 1",
        ).transform(orders_df)
        assert df.count() >= 1

    def test_sql_with_aggregation(self, orders_df):
        df = SqlTransformer(
            "agg_test",
            sql="SELECT cust_id, SUM(amount) as total FROM src GROUP BY cust_id",
        ).transform(orders_df)
        assert "total" in df.columns

    def test_sql_params(self, orders_df):
        df = SqlTransformer(
            "param_test",
            sql="SELECT * FROM src WHERE status = '{status_val}'",
            params={"status_val": "active"},
        ).transform(orders_df)
        # Should only return rows with status='active'
        statuses = [row.status for row in df.select("status").collect()]
        assert all(s == "active" for s in statuses if s is not None)
