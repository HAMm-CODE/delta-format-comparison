"""
SparkSession construction.

The original exercise ran inside Databricks, where `spark` is injected
into the notebook and Delta is preconfigured. Outside Databricks the
Delta extensions must be registered explicitly, which is what
`configure_spark_with_delta_pip` does — it resolves the matching
delta-spark JARs and wires up the catalog.

`get_spark()` returns a session that behaves the same locally and on
Databricks: on Databricks the existing session is reused, so the extra
configuration is harmless.
"""

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark(app_name: str = "deltacompare", cores: int = 2) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(f"local[{cores}]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # keep shuffle partitions low: this runs on small machines and the
        # default of 200 creates pointless tiny files that distort the
        # storage-size comparisons this project is measuring
        .config("spark.sql.shuffle.partitions", str(cores * 2))
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
