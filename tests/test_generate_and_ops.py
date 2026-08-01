"""Tests for the data generator and Delta operations."""

from pyspark.sql import functions as F

from deltacompare.formats import read, write
from deltacompare.generate import generate_accidents
from deltacompare.ops import delete_where, history, update_where, upsert, vacuum
from deltacompare.schema import ACCIDENTS_SCHEMA
from deltacompare.storage import dir_size_bytes


def test_generator_matches_declared_schema(spark):
    df = generate_accidents(spark, rows=100)
    assert [f.name for f in df.schema.fields] == [f.name for f in ACCIDENTS_SCHEMA.fields]
    assert [f.dataType for f in df.schema.fields] == [f.dataType for f in ACCIDENTS_SCHEMA.fields]


def test_generator_is_deterministic(spark):
    """Same seed must give identical data, or size comparisons are meaningless."""
    a = generate_accidents(spark, rows=500, seed=7).collect()
    b = generate_accidents(spark, rows=500, seed=7).collect()
    assert a == b


def test_generator_has_no_nulls_in_required_columns(spark):
    df = generate_accidents(spark, rows=1_000)
    for col in ("ID", "Start_Time", "End_Time", "City", "State", "Temperature_F"):
        assert df.filter(F.col(col).isNull()).count() == 0


def test_upsert_updates_matched_and_inserts_unmatched(spark, tmp_data_dir):
    path = f"{tmp_data_dir}/upsert"
    df = generate_accidents(spark, rows=1_000)
    write(df, path, "delta")

    existing = df.limit(10).withColumn("Temperature_F", F.lit(-99.0))
    new = (
        df.limit(5)
        .withColumn("ID", F.concat(F.col("ID"), F.lit("_NEW")))
        .withColumn("Temperature_F", F.lit(-99.0))
    )
    upsert(spark, path, existing.union(new))

    back = read(spark, path, "delta")
    assert back.count() == 1_005
    assert back.filter(F.col("Temperature_F") == -99.0).count() == 15


def test_update_and_delete(spark, tmp_data_dir):
    path = f"{tmp_data_dir}/update_delete"
    df = generate_accidents(spark, rows=1_000)
    write(df, path, "delta")

    ca_count = read(spark, path, "delta").filter(F.col("State") == "CA").count()
    update_where(spark, path, F.col("State") == "CA", {"Temperature_F": F.lit(999.0)})
    assert read(spark, path, "delta").filter(F.col("Temperature_F") == 999.0).count() == ca_count

    delete_where(spark, path, F.col("Temperature_F") == 999.0)
    back = read(spark, path, "delta")
    assert back.count() == 1_000 - ca_count
    assert back.filter(F.col("Temperature_F") == 999.0).count() == 0


def test_vacuum_reclaims_space_without_changing_data(spark, tmp_data_dir):
    """Copy-on-write grows storage; vacuum reclaims it, data unchanged."""
    path = f"{tmp_data_dir}/vacuum"
    df = generate_accidents(spark, rows=2_000)
    write(df, path, "delta")

    update_where(spark, path, F.col("State") == "CA", {"Temperature_F": F.lit(1.0)})
    update_where(spark, path, F.col("State") == "IL", {"Temperature_F": F.lit(2.0)})
    grown = dir_size_bytes(spark, path)
    rows_before = read(spark, path, "delta").count()

    vacuum(spark, path, retention_hours=0)

    assert dir_size_bytes(spark, path) < grown
    assert read(spark, path, "delta").count() == rows_before


def test_history_records_each_operation(spark, tmp_data_dir):
    path = f"{tmp_data_dir}/history"
    df = generate_accidents(spark, rows=500)
    write(df, path, "delta")
    update_where(spark, path, F.col("State") == "CA", {"Temperature_F": F.lit(5.0)})
    delete_where(spark, path, F.col("State") == "IL")

    ops = [r["operation"] for r in history(spark, path).collect()]
    assert "WRITE" in ops and "UPDATE" in ops and "DELETE" in ops
