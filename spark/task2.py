"""
# AI-ASSISTED: A4
Task 2 - PySpark: Parquet filtering and projection.

Fixed filter: PULocationID = 161 AND fare_amount > 30.00
  Query A returns all columns for matching rows.
  Query B returns only tpep_pickup_datetime, tpep_dropoff_datetime,
  trip_distance and fare_amount for the same rows.

Timing protocol (section 5):
  - the SparkSession is created BEFORE the timed region
  - the timed region ends only after collect() has materialised the result
  - one unrecorded warm-up run, then three recorded runs, per query

Also writes spark/task2_explain.txt using df.explain(mode="formatted").

Run:
    python spark/task2.py data/taxi_2m.parquet 2000000
"""

import io
import sys
import time
from contextlib import redirect_stdout
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import append_timing

EXPLAIN_FILE = Path(__file__).resolve().parent / "task2_explain.txt"

PROJECTION = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
]


def build_query(spark, input_path, project):
    """
    Define the DataFrame for Query A (project=False) or Query B (project=True).
    Nothing is executed here; Spark is lazy until an action is called.
    """
    df = spark.read.parquet(str(input_path)).filter(
        (F.col("PULocationID") == 161) & (F.col("fare_amount") > 30.00)
    )
    if project:
        df = df.select(*PROJECTION)
    return df


def run_once(spark, input_path, project):
    start = time.perf_counter()
    df = build_query(spark, input_path, project)
    rows = df.collect()  # materialises the complete result
    elapsed = time.perf_counter() - start
    return elapsed, rows


def capture_explain(spark, input_path):
    """Write formatted physical plans for both queries to task2_explain.txt."""
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        for label, project in (("QUERY A - all columns", False),
                               ("QUERY B - four columns", True)):
            print("=" * 70)
            print(f"Spark df.explain(mode='formatted') - {label}")
            print("=" * 70)
            build_query(spark, input_path, project).explain(mode="formatted")
            print()

    EXPLAIN_FILE.write_text(buffer.getvalue(), encoding="utf-8")
    print(f"  explain written: {EXPLAIN_FILE.name}")


def main(input_path, dataset_rows):
    input_path = Path(input_path)

    # Session startup happens outside every timed region.
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("task2_filter_projection")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"PySpark {spark.version} | {input_path.name} | "
          f"{int(dataset_rows):,} rows")

    capture_explain(spark, input_path)

    for condition, project in (("query_a_all_columns", False),
                               ("query_b_projection", True)):
        print(f"\n{condition}")
        print("  warm-up run (not recorded)...")
        _, rows = run_once(spark, input_path, project)
        print(f"  matching rows: {len(rows):,} | "
              f"columns returned: {len(rows[0]) if rows else 0}")

        for run_number in (1, 2, 3):
            elapsed, _ = run_once(spark, input_path, project)
            append_timing(
                system="spark",
                task="task2",
                condition=condition,
                dataset_rows=dataset_rows,
                input_format="parquet",
                input_path=input_path,
                run_number=run_number,
                time_seconds=elapsed,
                status="OK",
                tool_version=spark.version,
            )

    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
