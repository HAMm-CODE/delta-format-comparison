"""Run the format comparison end to end and print a results table."""

import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from deltacompare.compare import run_format  # noqa: E402
from deltacompare.config import Paths  # noqa: E402
from deltacompare.drift import apply_drift, make_batch  # noqa: E402
from deltacompare.generate import generate_accidents  # noqa: E402
from deltacompare.session import get_spark  # noqa: E402

ROWS = 198_082


def main() -> None:
    paths = Paths.from_env()
    spark = get_spark("format-comparison")

    # Clear previous output. Delta's overwrite tombstones old files rather
    # than deleting them (they remain for time travel until VACUUM), so a
    # re-run against a dirty path reports inflated sizes.
    shutil.rmtree(paths.out, ignore_errors=True)

    baseline = generate_accidents(spark, rows=ROWS).cache()
    la = make_batch(baseline, "Los Angeles", 75, newest_first=True)
    chicago = make_batch(baseline, "Chicago", 50, newest_first=False)
    same_schema = la.union(chicago).cache()
    drifted = apply_drift(same_schema).cache()

    expected_rows = ROWS + 2 * same_schema.count()
    print(f"baseline rows      : {ROWS:,}")
    print(f"appended per batch : {same_schema.count()}")
    print(f"expected final rows: {expected_rows:,}")
    print("expected columns   : 11\n")

    runs = [
        ("csv", False),
        ("parquet", False),
        ("parquet", True),
        ("delta", False),
        ("delta", True),
    ]

    results = []
    for fmt, merge in runs:
        suffix = "_merge" if merge else ""
        path = f"{paths.out}/cmp_{fmt}{suffix}"
        results.append(
            run_format(spark, baseline, same_schema, drifted, path, fmt, merge_schema=merge)
        )

    header = (
        f"{'format':9}{'merge':7}{'rows':>10}{'cols':>6}{'size MB':>9}"
        f"  {'Temperature_F':<14}{'Start_Time':<11}{'drift rejected':<14}"
    )
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r.fmt:9}{str(r.merge_schema):7}{r.rows:>10,}{r.columns:>6}{r.size_mb:>9}  "
            f"{str(r.temperature_f_type):<14}{str(r.start_time_type):<11}"
            f"{('YES' if r.drift_rejected else 'no'):<14}"
        )

    spark.stop()


if __name__ == "__main__":
    main()
