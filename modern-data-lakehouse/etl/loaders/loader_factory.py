"""
loader_factory.py
~~~~~~~~~~~~~~~~~
Returns the correct loader implementation based on `warehouse.target` config.
The rest of the pipeline never imports Snowflake or BigQuery loaders directly —
they always go through this factory.
"""

from typing import List, Optional

from etl.loaders.base_loader import BaseLoader
from utils.config_loader import get_warehouse_target


def get_loader(
    target_table: str,
    write_mode: str = "append",
    merge_keys: Optional[List[str]] = None,
    **kwargs,
) -> BaseLoader:
    """
    Factory function.

    Parameters
    ----------
    target_table : str
        Destination table name.
    write_mode : str
        "append" | "overwrite" | "merge"
    merge_keys : list of str, optional
        Required when write_mode="merge".
    **kwargs :
        Extra arguments forwarded to the loader constructor
        (e.g. partition_field, cluster_fields for BigQuery).

    Returns
    -------
    BaseLoader
        A configured SnowflakeLoader or BigQueryLoader.
    """
    target = get_warehouse_target()

    if target == "snowflake":
        from etl.loaders.snowflake_loader import SnowflakeLoader
        return SnowflakeLoader(target_table, write_mode=write_mode, merge_keys=merge_keys)

    elif target == "bigquery":
        from etl.loaders.bigquery_loader import BigQueryLoader
        return BigQueryLoader(
            target_table,
            write_mode=write_mode,
            merge_keys=merge_keys,
            partition_field=kwargs.get("partition_field"),
            cluster_fields=kwargs.get("cluster_fields"),
        )

    else:
        raise ValueError(
            f"Unknown warehouse target: '{target}'. "
            "Set warehouse.target to 'snowflake' or 'bigquery' in config/base.yaml."
        )
