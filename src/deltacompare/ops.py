"""
Delta table operations with no CSV or Parquet equivalent.

With CSV or Parquet, changing an existing row means rewriting the
whole dataset — there is no in-place update, no delete, and no way to
upsert. Delta supports all three, and keeps a version history.

Note on storage growth: Delta is copy-on-write. An UPDATE touching one
row rewrites the entire file containing it and leaves the original in
place for time travel. Storage therefore grows with every operation
until VACUUM removes files past the retention window.
"""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def upsert(spark: SparkSession, path: str, updates: DataFrame, key: str = "ID") -> None:
    """Update rows matching on `key`, insert those that don't match."""
    table = DeltaTable.forPath(spark, path)
    (
        table.alias("target")
        .merge(updates.alias("source"), f"target.{key} = source.{key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def update_where(spark: SparkSession, path: str, condition, assignments: dict) -> None:
    """Update rows matching `condition` in place."""
    DeltaTable.forPath(spark, path).update(condition=condition, set=assignments)


def delete_where(spark: SparkSession, path: str, condition) -> None:
    """Delete rows matching `condition`."""
    DeltaTable.forPath(spark, path).delete(condition=condition)


def history(spark: SparkSession, path: str) -> DataFrame:
    """Version history: one row per operation, with metrics."""
    return DeltaTable.forPath(spark, path).history().select(
        "version", "operation", "operationMetrics"
    )


def vacuum(spark: SparkSession, path: str, retention_hours: float = 168.0) -> None:
    """
    Remove files no longer referenced by the table.

    The default 168 hours (7 days) is Delta's minimum safe retention.
    Passing 0 requires disabling a safety check and permanently
    destroys time travel — acceptable in a demo, never in production,
    because concurrent readers holding an older snapshot will fail.
    """
    if retention_hours < 168.0:
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    try:
        DeltaTable.forPath(spark, path).vacuum(retention_hours)
    finally:
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
