"""
Read/write operations across CSV, Parquet, and Delta.

A single interface for all three formats so comparison code doesn't
branch on format. CSV options (header, delimiter) are defined once
here — in the original exercise they were repeated at every call site,
which is exactly how a read and a write end up disagreeing.

The `schema` parameter on `read` is deliberate: passing None infers
the schema, which is the behaviour that breaks under drift and is the
core thing this project demonstrates.
"""

from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

FORMATS = ("csv", "parquet", "delta")

_CSV_OPTIONS = {
    "header": "true",
    "delimiter": "|",
}


def _check(fmt: str) -> None:
    if fmt not in FORMATS:
        raise ValueError(f"unsupported format {fmt!r}, expected one of {FORMATS}")


def write(df: DataFrame, path: str, fmt: str, mode: str = "overwrite") -> None:
    """Write `df` to `path` in `fmt`."""
    _check(fmt)
    writer = df.write.mode(mode)
    if fmt == "csv":
        writer = writer.options(**_CSV_OPTIONS)
        writer.csv(path)
    else:
        writer.format(fmt).save(path)


def append(df: DataFrame, path: str, fmt: str, merge_schema: bool = False) -> None:
    """
    Append `df` to existing storage at `path`.

    `merge_schema` is only honoured by Delta and Parquet. CSV has no
    schema in the storage layer at all, so the option does not exist —
    appending a differently-shaped DataFrame simply writes files whose
    columns no longer line up with the rest.
    """
    _check(fmt)
    writer = df.write.mode("append")
    if fmt == "csv":
        writer.options(**_CSV_OPTIONS).csv(path)
    elif fmt == "parquet":
        if merge_schema:
            writer = writer.option("mergeSchema", "true")
        writer.parquet(path)
    else:
        if merge_schema:
            writer = writer.option("mergeSchema", "true")
        writer.format("delta").save(path)


def read(
    spark: SparkSession,
    path: str,
    fmt: str,
    schema: Optional[StructType] = None,
    merge_schema: bool = False,
) -> DataFrame:
    """
    Read from `path`.

    For CSV, `schema=None` means Spark infers types from the data —
    the behaviour that silently degrades every column to string once
    files with mismatched columns are present.
    """
    _check(fmt)
    if fmt == "csv":
        reader = spark.read.options(**_CSV_OPTIONS)
        if schema is not None:
            return reader.schema(schema).csv(path)
        return reader.option("inferSchema", "true").csv(path)

    reader = spark.read
    if merge_schema:
        reader = reader.option("mergeSchema", "true")
    return reader.format(fmt).load(path)
