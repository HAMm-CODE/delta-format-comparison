"""
Schema drift simulation.

Models the realistic failure case where an upstream producer changes
its output: columns get added, renamed, unit-converted, or dropped,
and the change reaches storage without anyone updating the consumer.

Two batch builders:

- `make_batch`   — new rows, schema unchanged. Every format handles this.
- `apply_drift`  — the same rows with a drifted schema. This is where
                   CSV, Parquet, and Delta diverge sharply.

The drift applied mirrors four changes that occur together in practice:
a column added, a second column added, a column renamed with a unit
conversion (F -> C), and a column dropped.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def make_batch(
    df: DataFrame,
    city: str,
    rows: int,
    newest_first: bool = True,
    suffix: str = "_Z1",
) -> DataFrame:
    """
    Select `rows` records for `city` and tag their IDs with `suffix`.

    Tagging IDs makes appended rows identifiable after the fact, which
    is how the comparison shows *which* rows survived each format's
    handling of the append.
    """
    order = F.col("Start_Time").desc() if newest_first else F.col("Start_Time").asc()
    return (
        df.filter(F.col("City") == city)
        .orderBy(order)
        .limit(rows)
        .withColumn("ID", F.concat(F.col("ID"), F.lit(suffix)))
    )


def apply_drift(df: DataFrame, suffix: str = "_Z2") -> DataFrame:
    """
    Return `df` with a drifted schema.

    Changes applied:
      - ID re-tagged with `suffix`
      - AddedColumn1 added (derived from City)
      - AddedColumn2 added (constant)
      - Temperature_F renamed to Temperature_C, values converted
      - Description dropped

    Net effect: 8 columns in, 9 columns out, with only 6 in common.
    """
    return (
        df.withColumn("ID", F.regexp_replace("ID", r"_Z1$", suffix))
        .withColumn("AddedColumn1", F.concat(F.lit("prefix-"), F.col("City")))
        .withColumn("AddedColumn2", F.lit("New column"))
        .withColumn(
            "Temperature_C",
            F.round((F.col("Temperature_F") - F.lit(32)) * F.lit(5) / F.lit(9), 4),
        )
        .drop("Temperature_F", "Description")
    )
