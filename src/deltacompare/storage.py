"""
Portable storage inspection.

The original exercise used `dbutils.fs.ls`, which only exists inside
Databricks. These functions use the Hadoop FileSystem API via the
active SparkSession, so they work identically on a local filesystem,
DBFS, ABFS, or S3.

One behavioural fix over the original: `dir_size_bytes` recurses into
subdirectories, so Delta tables are measured correctly. The original
`folderSizeInKB` only summed top-level entries and therefore silently
excluded the `_delta_log/` contents.
"""

from pyspark.sql import SparkSession

# metadata files excluded from data-size totals
_METADATA_PREFIXES = ("_", ".")


def _fs_and_path(spark: SparkSession, path: str):
    jvm = spark._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs, hadoop_path


def dir_size_bytes(spark: SparkSession, path: str, include_metadata: bool = True) -> int:
    """Total size in bytes of everything under `path`, recursively."""
    fs, hadoop_path = _fs_and_path(spark, path)
    if not fs.exists(hadoop_path):
        return 0

    total = 0
    for status in fs.listStatus(hadoop_path):
        name = status.getPath().getName()
        if status.isDirectory():
            if not include_metadata and name.startswith(_METADATA_PREFIXES):
                continue
            total += dir_size_bytes(spark, status.getPath().toString(), include_metadata)
        else:
            if not include_metadata and name.startswith(_METADATA_PREFIXES):
                continue
            total += status.getLen()
    return total


def dir_size_mb(spark: SparkSession, path: str, include_metadata: bool = True) -> float:
    return round(dir_size_bytes(spark, path, include_metadata) / 1024 / 1024, 2)


def list_files(spark: SparkSession, path: str) -> list[tuple[str, int]]:
    """(relative_path, size_bytes) for every file under `path`, recursively."""
    fs, hadoop_path = _fs_and_path(spark, path)
    if not fs.exists(hadoop_path):
        return []

    results: list[tuple[str, int]] = []
    for status in fs.listStatus(hadoop_path):
        p = status.getPath().toString()
        if status.isDirectory():
            results.extend(list_files(spark, p))
        else:
            results.append((p, status.getLen()))
    return sorted(results)
