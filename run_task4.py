"""
# AI-ASSISTED: A6
Task 4 - scaling experiment.

Repeats the Task 1 aggregation on CSV input at four fixed sizes using all
three systems. The aggregation logic is not re-written here: this driver
imports the implementations already used and verified in Task 1, so the
computation being scaled is provably the same one.

Timing protocol (section 5):
  - engine startup happens outside every timed region
  - one unrecorded warm-up per system/dataset pair, then three recorded runs
  - any run exceeding 900 seconds is recorded as TIMEOUT_900 and that
    system/dataset combination is not repeated

Run everything:
    python run_task4.py

Run one system only (useful if a run has to be repeated):
    python run_task4.py duckdb
    python run_task4.py spark
    python run_task4.py mapreduce
"""

import importlib.util
import sys
import time
from pathlib import Path

from config import DATA_DIR, append_timing

PROJECT_ROOT = Path(__file__).resolve().parent
TIMEOUT_SECONDS = 900

DATASETS = [
    (100_000, "taxi_100k.csv"),
    (500_000, "taxi_500k.csv"),
    (1_000_000, "taxi_1m.csv"),
    (2_000_000, "taxi_2m.csv"),
]


def load_module(name, relative_path):
    """Import a module by file path, since these folders are not packages."""
    spec = importlib.util.spec_from_file_location(
        name, PROJECT_ROOT / relative_path
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def record(system, dataset_rows, input_path, run_number, elapsed, status,
           tool_version):
    append_timing(
        system=system,
        task="task4",
        condition="aggregation",
        dataset_rows=dataset_rows,
        input_format="csv",
        input_path=input_path,
        run_number=run_number,
        time_seconds=elapsed,
        status=status,
        tool_version=tool_version,
    )


def measure(system, tool_version, dataset_rows, input_path, run_once):
    """
    Warm-up plus three recorded runs for one system/dataset pair.
    run_once() must execute the aggregation and return elapsed seconds.
    Returns True if the pair completed, False if it timed out.
    """
    print(f"\n{system} | {input_path.name} | {dataset_rows:,} rows")
    print("  warm-up run (not recorded)...")

    warmup = run_once()
    if warmup > TIMEOUT_SECONDS:
        print(f"  warm-up exceeded {TIMEOUT_SECONDS}s - recording TIMEOUT_900")
        record(system, dataset_rows, input_path, 1, TIMEOUT_SECONDS,
               "TIMEOUT_900", tool_version)
        return False

    for run_number in (1, 2, 3):
        elapsed = run_once()
        if elapsed > TIMEOUT_SECONDS:
            print(f"  run {run_number} exceeded {TIMEOUT_SECONDS}s")
            record(system, dataset_rows, input_path, run_number,
                   TIMEOUT_SECONDS, "TIMEOUT_900", tool_version)
            return False
        record(system, dataset_rows, input_path, run_number, elapsed, "OK",
               tool_version)

    return True


def run_duckdb():
    import duckdb

    mod = load_module("duckdb_task1", "duckdb/run_task1.py")
    con = duckdb.connect()  # opened outside every timed region

    for dataset_rows, filename in DATASETS:
        path = DATA_DIR / filename
        query = mod.load_query(path)
        ok = measure(
            "duckdb", duckdb.__version__, dataset_rows, path,
            lambda: mod.run_once(con, query)[0],
        )
        if not ok:
            break

    con.close()


def run_spark():
    from pyspark.sql import SparkSession

    mod = load_module("spark_task1", "spark/task1.py")

    # Session startup happens outside every timed region.
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("task4_scaling")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    for dataset_rows, filename in DATASETS:
        path = DATA_DIR / filename
        ok = measure(
            "spark", spark.version, dataset_rows, path,
            lambda: mod.run_once(spark, path)[0],
        )
        if not ok:
            break

    spark.stop()


def run_mapreduce():
    import mrjob

    mod = load_module("mr_task1", "mapreduce/task1.py")

    for dataset_rows, filename in DATASETS:
        path = DATA_DIR / filename
        ok = measure(
            "mapreduce", mrjob.__version__, dataset_rows, path,
            lambda: mod.run_once(path)[0],
        )
        if not ok:
            print("  stopping mapreduce after timeout; larger sizes not run")
            break


RUNNERS = {
    "duckdb": run_duckdb,
    "spark": run_spark,
    "mapreduce": run_mapreduce,
}


if __name__ == "__main__":
    started = time.perf_counter()

    if len(sys.argv) > 1:
        RUNNERS[sys.argv[1]]()
    else:
        # Fastest systems first so problems surface early; mrjob last
        # because it is by far the slowest at 2M rows.
        run_duckdb()
        run_spark()
        run_mapreduce()

    print(f"\ntotal elapsed: {time.perf_counter() - started:.1f}s")
    print("next: python plot_scaling.py")
