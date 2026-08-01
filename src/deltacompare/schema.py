"""
Explicit schema for the accidents dataset, matching the shape used in
the original exercise: ID, Start_Time, End_Time, Description, City,
County, State, Temperature_F.

Defining this explicitly (rather than relying on inferSchema) avoids
a full extra read pass over the data and guarantees consistent types
across CSV, Parquet, and Delta — which is the whole point of this
project: showing what schema drift does to each format.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType,
)

ACCIDENTS_SCHEMA = StructType([
    StructField("ID", StringType(), nullable=False),
    StructField("Start_Time", TimestampType(), nullable=False),
    StructField("End_Time", TimestampType(), nullable=True),
    StructField("Description", StringType(), nullable=True),
    StructField("City", StringType(), nullable=False),
    StructField("County", StringType(), nullable=True),
    StructField("State", StringType(), nullable=False),
    StructField("Temperature_F", DoubleType(), nullable=True),
])
