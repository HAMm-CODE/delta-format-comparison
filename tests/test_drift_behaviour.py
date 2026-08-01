"""
Tests asserting how each format behaves under schema drift.

These encode the project's central findings. If a future Spark or
Delta version changes any of this behaviour, CI fails and the README
gets corrected — the claims stay honest.
"""

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import functions as F

from deltacompare.drift import apply_drift, make_batch
from deltacompare.formats import append, read, write
from deltacompare.generate import generate_accidents

ROWS = 2_000
BATCH = 25


@pytest.fixture(scope="module")
def batches(spark):
    baseline = generate_accidents(spark, rows=ROWS).cache()
    same = make_batch(baseline, "Los Angeles", BATCH).cache()
    drifted = apply_drift(same).cache()
    return baseline, same, drifted


def test_drift_changes_column_set(batches):
    _, same, drifted = batches
    assert len(same.columns) == 8
    assert len(drifted.columns) == 9
    assert "Temperature_F" in same.columns
    assert "Temperature_C" in drifted.columns
    assert "Description" not in drifted.columns


def test_csv_degrades_all_types_to_string(spark, tmp_data_dir, batches):
    """CSV keeps the rows but loses every type once columns misalign."""
    baseline, same, drifted = batches
    path = f"{tmp_data_dir}/csv"

    write(baseline, path, "csv")
    append(same, path, "csv")
    append(drifted, path, "csv")

    back = read(spark, path, "csv")
    types = dict(back.dtypes)
    assert types["Temperature_F"] == "string"
    assert types["Start_Time"] == "string"


def test_parquet_silently_drops_columns_without_merge_schema(spark, tmp_data_dir, batches):
    """The most dangerous outcome: no error, missing data."""
    baseline, same, drifted = batches
    path = f"{tmp_data_dir}/parquet"

    write(baseline, path, "parquet")
    append(same, path, "parquet")
    append(drifted, path, "parquet")

    back = read(spark, path, "parquet")

    # Parquet without mergeSchema adopts a single file's footer as the
    # schema for the whole dataset, and which file it picks is not
    # deterministic. Some columns are always lost; *which* ones vary
    # between runs. Nothing is raised either way.
    assert len(back.columns) < 11
    all_columns = set(baseline.columns) | set(drifted.columns)
    assert set(back.columns) < all_columns


def test_parquet_recovers_columns_with_merge_schema(spark, tmp_data_dir, batches):
    baseline, same, drifted = batches
    path = f"{tmp_data_dir}/parquet_merge"

    write(baseline, path, "parquet")
    append(same, path, "parquet")
    append(drifted, path, "parquet")

    back = read(spark, path, "parquet", merge_schema=True)
    assert len(back.columns) == 11
    assert dict(back.dtypes)["Temperature_F"] == "double"


def test_delta_rejects_drifted_append(spark, tmp_data_dir, batches):
    """Delta refuses the write rather than corrupting the table."""
    baseline, same, drifted = batches
    path = f"{tmp_data_dir}/delta"

    write(baseline, path, "delta")
    append(same, path, "delta")

    with pytest.raises(AnalysisException):
        append(drifted, path, "delta")


def test_delta_table_intact_after_rejected_append(spark, tmp_data_dir, batches):
    """The rejected write is atomic: the table is unchanged."""
    baseline, same, drifted = batches
    path = f"{tmp_data_dir}/delta_atomic"

    write(baseline, path, "delta")
    append(same, path, "delta")
    before = read(spark, path, "delta").count()

    with pytest.raises(AnalysisException):
        append(drifted, path, "delta")

    back = read(spark, path, "delta")
    assert back.count() == before == ROWS + BATCH
    assert len(back.columns) == 8
    assert dict(back.dtypes)["Temperature_F"] == "double"


def test_delta_absorbs_drift_with_merge_schema(spark, tmp_data_dir, batches):
    baseline, same, drifted = batches
    path = f"{tmp_data_dir}/delta_merge"

    write(baseline, path, "delta")
    append(same, path, "delta", merge_schema=True)
    append(drifted, path, "delta", merge_schema=True)

    back = read(spark, path, "delta")
    assert back.count() == ROWS + 2 * BATCH
    assert len(back.columns) == 11
    assert dict(back.dtypes)["Temperature_F"] == "double"
    assert back.filter(F.col("Temperature_C").isNotNull()).count() == BATCH
