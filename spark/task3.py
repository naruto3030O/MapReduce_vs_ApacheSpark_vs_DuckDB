"""
# AI-ASSISTED: A5
Task 3 - PySpark: join with the taxi-zone lookup.

Join PULocationID with LocationID, keep rows with fare_amount >= 0, total
fare_amount per Borough + Zone, return the five pickup zones with the highest
total fare revenue. Ties broken by Zone name ascending.

Timing protocol (section 5):
  - the SparkSession is created BEFORE the timed region
  - the timed region ends only after collect() has materialised the result
  - one unrecorded warm-up run, then three recorded runs

Section 9 asks whether Spark chooses a broadcast strategy for the small
lookup table. No broadcast hint is applied here: the physical plan is printed
so that whatever Spark chooses on its own can be reported honestly.

Run:
    python spark/task3.py data/taxi_2m.parquet data/taxi_zone_lookup.csv 2000000
"""

import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import append_timing


def build_query(spark, input_path, lookup_path):
    """Define the DataFrame. Nothing executes until an action is called."""
    trips = spark.read.parquet(str(input_path))
    zones = spark.read.csv(str(lookup_path), header=True, inferSchema=True)

    return (
        trips.filter(F.col("fare_amount") >= 0)
        .join(zones, trips["PULocationID"] == zones["LocationID"])
        .groupBy("Borough", "Zone")
        .agg(F.sum("fare_amount").alias("total_fare"))
        .orderBy(F.col("total_fare").desc(), F.col("Zone").asc())
        .limit(5)
    )


def run_once(spark, input_path, lookup_path):
    start = time.perf_counter()
    rows = build_query(spark, input_path, lookup_path).collect()
    elapsed = time.perf_counter() - start
    return elapsed, rows


def main(input_path, lookup_path, dataset_rows):
    input_path = Path(input_path)
    lookup_path = Path(lookup_path)

    # Session startup happens outside every timed region.
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("task3_zone_join")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"PySpark {spark.version} | {input_path.name} + "
          f"{lookup_path.name} | {int(dataset_rows):,} rows")

    threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    print(f"\nspark.sql.autoBroadcastJoinThreshold = {threshold}")
    print(f"zone lookup file size = {lookup_path.stat().st_size:,} bytes")

    print("\nPhysical plan (join strategy):")
    build_query(spark, input_path, lookup_path).explain(mode="formatted")

    print("\nwarm-up run (not recorded)...")
    run_once(spark, input_path, lookup_path)

    rows = None
    for run_number in (1, 2, 3):
        elapsed, rows = run_once(spark, input_path, lookup_path)
        append_timing(
            system="spark",
            task="task3",
            condition="join_top5",
            dataset_rows=dataset_rows,
            input_format="parquet",
            input_path=input_path,
            run_number=run_number,
            time_seconds=elapsed,
            status="OK",
            tool_version=spark.version,
        )

    print("\ntop 5 zones by total fare revenue:")
    print(f"  {'Borough':<15} {'Zone':<32} {'total_fare':>14}")
    for row in rows:
        print(f"  {row['Borough']:<15} {row['Zone']:<32} "
              f"{row['total_fare']:>14,.2f}")

    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
