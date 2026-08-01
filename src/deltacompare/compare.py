"""
The experiment.

For each format: write a baseline, append a same-schema batch, then
append a drifted batch. Read back and record what survived.

Measured outcomes:

  rows           — how many rows remain readable
  columns        — how many of the 11 distinct columns are visible
  types          — did original column types survive
  drift_rejected — did the format refuse the drifted append

The formats differ most in that last column. CSV and Parquet accept
the drifted append silently and corrupt themselves. Delta validates
the incoming schema against the table schema, rejects the write, and
leaves the table exactly as it was — the append is atomic, so a
rejected write does no partial damage.
"""

from dataclasses import asdict, dataclass
from typing import Optional

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession

from .formats import append, read, write
from .storage import dir_size_mb


@dataclass
class Result:
    fmt: str
    merge_schema: bool
    rows: int
    columns: int
    size_mb: float
    temperature_f_type: Optional[str]
    start_time_type: Optional[str]
    drift_rejected: bool

    def as_dict(self) -> dict:
        return asdict(self)


def run_format(
    spark: SparkSession,
    baseline: DataFrame,
    same_schema_batch: DataFrame,
    drifted_batch: DataFrame,
    path: str,
    fmt: str,
    merge_schema: bool = False,
) -> Result:
    """Write baseline, append both batches, then read back and measure."""
    write(baseline, path, fmt)
    append(same_schema_batch, path, fmt, merge_schema=merge_schema)

    drift_rejected = False
    try:
        append(drifted_batch, path, fmt, merge_schema=merge_schema)
    except AnalysisException:
        # Delta validates the incoming schema against the table schema and
        # refuses mismatched writes unless mergeSchema is enabled. This is
        # a result, not a failure: the table is still readable below.
        drift_rejected = True

    back = read(spark, path, fmt, merge_schema=merge_schema)
    types = dict(back.dtypes)

    return Result(
        fmt=fmt,
        merge_schema=merge_schema,
        rows=back.count(),
        columns=len(back.columns),
        size_mb=dir_size_mb(spark, path),
        temperature_f_type=types.get("Temperature_F"),
        start_time_type=types.get("Start_Time"),
        drift_rejected=drift_rejected,
    )
