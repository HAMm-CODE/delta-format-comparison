"""
Synthetic accident data generator.

Produces data matching the shape of the original exercise dataset
(ID, Start_Time, End_Time, Description, City, County, State,
Temperature_F) without depending on the CC BY-NC-SA-licensed Kaggle
US Accidents dataset.

Values are derived deterministically from the row index via `hash`,
so a given (rows, seed) pair always yields byte-identical output.
That matters here: storage-size comparisons between CSV, Parquet, and
Delta are only meaningful if the underlying data is reproducible.

High repetition in City/County/State/Description is intentional — it
mirrors the concentration in the original dataset and is what drives
Parquet's dictionary encoding to compress ~3x better than CSV.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# (city, county, state) kept together so the three columns stay consistent
_LOCATIONS = [
    ("Los Angeles", "Los Angeles", "CA"),
    ("Chicago", "Cook", "IL"),
    ("Houston", "Harris", "TX"),
    ("Whitehall", "Lehigh", "PA"),
    ("Phoenix", "Maricopa", "AZ"),
    ("Miami", "Miami-Dade", "FL"),
    ("Cochise", "Cochise", "AZ"),
    ("Pittsburgh", "Allegheny", "PA"),
]

_DESCRIPTIONS = [
    "Road closed due to accident. Roadwork. Lane blocked.",
    "Incident reported. Traffic backed up in the area.",
    "Closed at exit ramp - accident cleared, minor delays.",
    "Multi-vehicle collision. Right lane blocked.",
    "Accident on shoulder. Traffic slowing down.",
]

_START_EPOCH = 1_451_606_400  # 2016-01-01T00:00:00Z
_STEP_SECONDS = 1_100         # ~6.9 years of spread across ~198K rows


def generate_accidents(
    spark: SparkSession,
    rows: int = 198_082,
    seed: int = 42,
) -> DataFrame:
    """Generate a deterministic synthetic accidents DataFrame."""
    cities = F.array(*[F.lit(c) for c, _, _ in _LOCATIONS])
    counties = F.array(*[F.lit(c) for _, c, _ in _LOCATIONS])
    states = F.array(*[F.lit(s) for _, _, s in _LOCATIONS])
    descriptions = F.array(*[F.lit(d) for d in _DESCRIPTIONS])

    def hash_mod(salt: int, modulus: int):
        """Deterministic non-negative value in [0, modulus)."""
        return F.pmod(F.hash(F.col("id") + F.lit(seed * 1000 + salt)), F.lit(modulus))

    base = (
        spark.range(rows)
        .withColumn("loc_idx", hash_mod(1, len(_LOCATIONS)) + 1)
        .withColumn("desc_idx", hash_mod(2, len(_DESCRIPTIONS)) + 1)
        .withColumn("start_epoch", F.lit(_START_EPOCH) + F.col("id") * _STEP_SECONDS)
        .withColumn("duration_s", hash_mod(3, 18_000) + 300)
        .withColumn("temp_tenths", hash_mod(4, 900) + 100)
    )

    # NOTE: 'ID' is aliased in the final select, not via withColumn, because
    # Spark resolves column names case-insensitively by default — creating
    # 'ID' earlier would silently overwrite range()'s 'id' column.
    return base.select(
        F.concat(F.lit("A-"), (F.col("id") + 3_000_000).cast("string")).alias("ID"),
        F.timestamp_seconds("start_epoch").alias("Start_Time"),
        F.timestamp_seconds(F.col("start_epoch") + F.col("duration_s")).alias("End_Time"),
        F.element_at(descriptions, F.col("desc_idx")).alias("Description"),
        F.element_at(cities, F.col("loc_idx")).alias("City"),
        F.element_at(counties, F.col("loc_idx")).alias("County"),
        F.element_at(states, F.col("loc_idx")).alias("State"),
        (F.col("temp_tenths") / F.lit(10.0)).alias("Temperature_F"),
    )
