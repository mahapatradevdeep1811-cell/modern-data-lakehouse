"""
lakehouse_main_dag.py
~~~~~~~~~~~~~~~~~~~~~
Apache Airflow DAG for the Modern Data Lakehouse pipeline.

Pipeline stages:
  1. Extract        — read raw CSV/JSON/Parquet sources
  2. Transform      — Spark SQL + DataFrame API transformations
  3. DQ Checks      — schema, nulls, uniqueness, value range
  4. Load           — write curated datasets to Snowflake / BigQuery
  5. Reconcile      — source-to-target row count & aggregate checks
  6. Notify         — Slack alert on success or failure

Trigger: Daily at 02:00 UTC.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

# --------------------------------------------------------------------------
# Default args
# --------------------------------------------------------------------------

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

# --------------------------------------------------------------------------
# Task callables
# --------------------------------------------------------------------------

def _extract(**context) -> None:
    from etl.extractors.file_extractor import FileExtractor
    from utils.spark_session import get_spark

    spark = get_spark()
    logical_date = context["logical_date"].strftime("%Y-%m-%d")

    extractor = FileExtractor(
        source_id="orders_raw",
        path=f"data/raw/orders/{logical_date}/*.csv",
        format="csv",
    )
    df = extractor.extract()

    # Persist to staging as Parquet for downstream tasks
    df.write.mode("overwrite").parquet(f"data/staging/orders/{logical_date}")
    context["ti"].xcom_push(key="staging_path", value=f"data/staging/orders/{logical_date}")
    context["ti"].xcom_push(key="row_count", value=df.count())


def _transform(**context) -> None:
    from etl.transformers.dataframe_transformer import DataFrameTransformer
    from etl.transformers.sql_transformer import SqlTransformer
    from utils.spark_session import get_spark

    spark = get_spark()
    logical_date = context["logical_date"].strftime("%Y-%m-%d")
    staging_path = context["ti"].xcom_pull(key="staging_path")

    raw_df = spark.read.parquet(staging_path)

    # Step 1 — clean & cast via DataFrameTransformer
    clean_df = (
        DataFrameTransformer("orders_clean")
        .rename_columns({"order_id": "id", "cust_id": "customer_id"})
        .cast_columns({"amount": "double", "order_date": "date", "qty": "integer"})
        .fill_nulls({"status": "PENDING", "amount": 0.0})
        .drop_duplicates(["id"])
        .add_audit_columns(batch_id=logical_date, source="orders_csv")
        .transform(raw_df)
    )

    # Step 2 — business logic via Spark SQL
    enriched_df = SqlTransformer(
        "orders_enriched",
        sql="""
            SELECT
                id,
                customer_id,
                order_date,
                amount,
                qty,
                ROUND(amount / NULLIF(qty, 0), 2) AS unit_price,
                status,
                CASE
                    WHEN amount >= 1000 THEN 'HIGH'
                    WHEN amount >= 100  THEN 'MEDIUM'
                    ELSE 'LOW'
                END AS amount_tier,
                _etl_loaded_at,
                _batch_id,
                _source_system
            FROM src
            WHERE order_date IS NOT NULL
        """,
    ).transform(clean_df)

    enriched_df.write.mode("overwrite").parquet(f"data/curated/orders/{logical_date}")
    context["ti"].xcom_push(key="curated_path", value=f"data/curated/orders/{logical_date}")
    context["ti"].xcom_push(key="curated_count", value=enriched_df.count())


def _run_dq_checks(**context) -> None:
    from dq.checks.dq_checks import (
        DQRunner, check_not_empty, check_no_nulls,
        check_unique, check_value_range, check_schema,
    )
    from utils.spark_session import get_spark

    spark = get_spark()
    curated_path = context["ti"].xcom_pull(key="curated_path")
    df = spark.read.parquet(curated_path)

    runner = DQRunner("orders_curated")
    runner.add_result(check_not_empty(df, "orders_curated"))
    runner.add_result(check_schema(df, ["id", "customer_id", "order_date", "amount", "status"]))
    runner.add_result(check_no_nulls(df, ["id", "customer_id", "order_date"]))
    runner.add_result(check_unique(df, ["id"]))
    runner.add_result(check_value_range(df, "amount", min_val=0))
    runner.evaluate()


def _load(**context) -> None:
    from etl.loaders.loader_factory import get_loader
    from utils.spark_session import get_spark

    spark = get_spark()
    curated_path = context["ti"].xcom_pull(key="curated_path")
    df = spark.read.parquet(curated_path)

    loader = get_loader(
        target_table="orders_curated",
        write_mode="merge",
        merge_keys=["id"],
        partition_field="order_date",          # BigQuery-specific; ignored by Snowflake
        cluster_fields=["customer_id", "status"],
    )
    loader.load(df)


def _reconcile(**context) -> None:
    from dq.reconciliation.reconciliation import (
        ReconciliationSuite, reconcile_row_count, reconcile_aggregate,
    )
    from utils.spark_session import get_spark

    spark = get_spark()
    curated_path = context["ti"].xcom_pull(key="curated_path")
    source_df = spark.read.parquet(curated_path)

    # Read back from the warehouse for target
    from utils.config_loader import get_warehouse_target
    target = get_warehouse_target()
    if target == "snowflake":
        from utils.config_loader import get_config
        cfg = get_config()
        sf = cfg["snowflake"]["spark_connector"]
        target_df = spark.read \
            .format("net.snowflake.spark.snowflake") \
            .options(**{**sf, "dbtable": "orders_curated"}) \
            .load()
    else:
        target_df = spark.read \
            .format("bigquery") \
            .option("table", "orders_curated") \
            .load()

    suite = ReconciliationSuite("orders_curated")
    suite.add(reconcile_row_count(source_df, target_df))
    suite.add(reconcile_aggregate(source_df, target_df, column="amount", agg_func="sum"))
    suite.evaluate()


# --------------------------------------------------------------------------
# DAG definition
# --------------------------------------------------------------------------

with DAG(
    dag_id="lakehouse_main_dag",
    description="Modern Data Lakehouse — daily ETL pipeline (Snowflake / BigQuery)",
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["lakehouse", "etl", "pyspark"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    extract = PythonOperator(
        task_id="extract",
        python_callable=_extract,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=_transform,
    )

    dq_checks = PythonOperator(
        task_id="dq_checks",
        python_callable=_run_dq_checks,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=_load,
    )

    reconcile = PythonOperator(
        task_id="reconcile",
        python_callable=_reconcile,
    )

    # Pipeline order
    start >> extract >> transform >> dq_checks >> load >> reconcile >> end
