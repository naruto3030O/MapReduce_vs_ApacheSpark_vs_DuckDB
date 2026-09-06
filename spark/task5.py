"""
# AI-ASSISTED: A7
Task 5 - Spark: repeated work on the same data, uncached and cached.

Condition 1 (spark_uncached): load the Parquet DataFrame once and run the
    aggregation five times without calling cache() or persist().

Condition 2 (spark_cached): cache the input DataFrame, force the cache to
    materialise with count(), then run the same aggregation five times. The
    one-time count() is timed separately and recorded with
    condition=spark_cache_build and run_number=0, as required by section 14.
    It is not included in the five query runtimes.

Note on protocol: there is deliberately NO warm-up run. Section 5 asks for a
warm-up "unless the task explicitly asks for five repeated runs", and Task 5
does. Whether the first run is slower than the later ones is the observation
being made.

Python, Spark and the SparkSession are not restarted between the five runs
within a condition.

Run:
    python spark/task5.py data/taxi_1m.parquet 1000000
"""

import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import append_timing


def aggregate(df):
    """The Task 1 aggregation, defined over an already-loaded DataFrame."""
    return (
        df.filter(
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


def five_runs(df, condition, dataset_rows, input_path, tool_version):
    """Run the aggregation five times over df, recording each runtime."""
    times = []
    rows = None
    for run_number in (1, 2, 3, 4, 5):
        start = time.perf_counter()
        rows = aggregate(df).collect()  # materialises the complete result
        elapsed = time.perf_counter() - start
        times.append(elapsed)

        append_timing(
            system="spark",
            task="task5",
            condition=condition,
            dataset_rows=dataset_rows,
            input_format="parquet",
            input_path=input_path,
            run_number=run_number,
            time_seconds=elapsed,
            status="OK",
            tool_version=tool_version,
        )

    print(f"  groups returned: {len(rows)}")
    print(f"  run 1: {times[0]:.4f}s | run 5: {times[4]:.4f}s | "
          f"change: {(times[0] - times[4]) / times[0] * 100:+.1f}%")
    return times


def main(input_path, dataset_rows):
    input_path = Path(input_path)

    # Session startup happens outside every timed region.
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("task5_caching")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"PySpark {spark.version} | {input_path.name} | "
          f"{int(dataset_rows):,} rows")
    print("five repeated runs per condition, no warm-up")

    # ---------------------------------------------------------------
    # Condition 1: uncached
    # ---------------------------------------------------------------
    print("\nspark_uncached (no cache/persist called)")
    df_uncached = spark.read.parquet(str(input_path))
    uncached = five_runs(df_uncached, "spark_uncached", dataset_rows,
                         input_path, spark.version)

    # ---------------------------------------------------------------
    # Condition 2: cached
    # ---------------------------------------------------------------
    print("\nspark_cache_build (one-time, reported separately)")
    df_cached = spark.read.parquet(str(input_path)).cache()

    start = time.perf_counter()
    df_cached.count()  # forces the cache to materialise
    build_time = time.perf_counter() - start

    append_timing(
        system="spark",
        task="task5",
        condition="spark_cache_build",
        dataset_rows=dataset_rows,
        input_format="parquet",
        input_path=input_path,
        run_number=0,
        time_seconds=build_time,
        status="OK",
        tool_version=spark.version,
    )
    print(f"  cache build (count): {build_time:.4f}s")

    print("\nspark_cached")
    cached = five_runs(df_cached, "spark_cached", dataset_rows, input_path,
                       spark.version)

    # ---------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------
    med_un = sorted(uncached)[2]
    med_ca = sorted(cached)[2]
    print("\nsummary")
    print(f"  uncached median: {med_un:.4f}s")
    print(f"  cached median:   {med_ca:.4f}s")
    print(f"  saving per run:  {med_un - med_ca:.4f}s")
    if med_un > med_ca:
        breakeven = build_time / (med_un - med_ca)
        print(f"  cache build cost {build_time:.4f}s pays for itself after "
              f"{breakeven:.1f} runs")
    else:
        print("  caching did not reduce the median runtime")

    df_cached.unpersist()
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
