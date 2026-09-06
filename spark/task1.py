"""
Task 1 - PySpark DataFrame implementation.

Computation: for every non-null PULocationID, count trips and average
fare_amount, considering only rows where fare_amount is non-null and >= 0.

Timing protocol (section 5):
  - the SparkSession is created BEFORE the timed region
  - the timed region ends only after collect() has fully materialised the
    result; defining a lazy DataFrame is not a completed run
  - one unrecorded warm-up run, then three recorded runs

Note on schema: the CSV is read with inferSchema=False and only the two
columns the query needs are cast. inferSchema=True would make Spark scan the
file an extra time purely to guess types, which is not part of the
computation being compared.

Run:
    python spark/task1.py data/taxi_1m.csv 1000000
"""

import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import append_timing, save_result


def run_once(spark, input_path):
    """One complete run: read, filter, aggregate, materialise."""
    start = time.perf_counter()

    df = spark.read.csv(str(input_path), header=True, inferSchema=False)

    result = (
        df.select(
            F.col("PULocationID").cast("int").alias("PULocationID"),
            F.col("fare_amount").cast("double").alias("fare_amount"),
        )
        .filter(
            F.col("PULocationID").isNotNull()
            & F.col("fare_amount").isNotNull()
            & (F.col("fare_amount") >= 0)
        )
        .groupBy("PULocationID")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("fare_amount").alias("avg_fare"),
        )
    )

    rows = result.collect()  # materialises the complete result
    elapsed = time.perf_counter() - start

    return elapsed, [(r["PULocationID"], r["trip_count"], r["avg_fare"])
                     for r in rows]


def main(input_path, dataset_rows):
    input_path = Path(input_path)

    # Session startup happens outside every timed region.
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("task1_aggregation")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"PySpark {spark.version} | {input_path.name} | "
          f"{int(dataset_rows):,} rows")

    print("warm-up run (not recorded)...")
    run_once(spark, input_path)

    rows = None
    for run_number in (1, 2, 3):
        elapsed, rows = run_once(spark, input_path)
        append_timing(
            system="spark",
            task="task1",
            condition="aggregation",
            dataset_rows=dataset_rows,
            input_format="csv",
            input_path=input_path,
            run_number=run_number,
            time_seconds=elapsed,
            status="OK",
            tool_version=spark.version,
        )

    save_result("spark", "task1", rows)
    print("\nsample of result:")
    for row in sorted(rows, key=lambda r: int(r[0]))[:5]:
        print(f"  PULocationID={row[0]}  trips={row[1]}  avg_fare={row[2]:.4f}")

    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
