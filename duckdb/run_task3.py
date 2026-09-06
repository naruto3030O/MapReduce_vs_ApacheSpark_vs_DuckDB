"""
# AI-ASSISTED: A5
Task 3 - DuckDB timing runner for the zone-lookup join.

The query lives in duckdb/task3.sql (required by section 16). This script
loads it, substitutes both input paths, and applies the timing protocol from
section 5:

  - the DuckDB connection is opened BEFORE the timed region
  - the timed region ends only after fetchall() has materialised the result
  - one unrecorded warm-up run, then three recorded runs

Run:
    python duckdb/run_task3.py data/taxi_2m.parquet data/taxi_zone_lookup.csv 2000000
"""

import sys
import time
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import append_timing

SQL_FILE = Path(__file__).resolve().parent / "task3.sql"


def load_query(input_path, lookup_path):
    sql = SQL_FILE.read_text(encoding="utf-8")
    # DuckDB needs forward slashes in string literals on Windows.
    sql = sql.replace("{{INPUT}}", str(input_path).replace("\\", "/"))
    sql = sql.replace("{{LOOKUP}}", str(lookup_path).replace("\\", "/"))
    return sql


def run_once(con, query):
    start = time.perf_counter()
    rows = con.execute(query).fetchall()  # materialises the complete result
    elapsed = time.perf_counter() - start
    return elapsed, rows


def main(input_path, lookup_path, dataset_rows):
    input_path = Path(input_path)
    lookup_path = Path(lookup_path)
    query = load_query(input_path, lookup_path)

    # Connection opened outside every timed region.
    con = duckdb.connect()

    print(f"DuckDB {duckdb.__version__} | {input_path.name} + "
          f"{lookup_path.name} | {int(dataset_rows):,} rows")

    print("\nEXPLAIN (join strategy):")
    for row in con.execute("EXPLAIN " + query).fetchall():
        print(row[-1])

    print("\nwarm-up run (not recorded)...")
    run_once(con, query)

    rows = None
    for run_number in (1, 2, 3):
        elapsed, rows = run_once(con, query)
        append_timing(
            system="duckdb",
            task="task3",
            condition="join_top5",
            dataset_rows=dataset_rows,
            input_format="parquet",
            input_path=input_path,
            run_number=run_number,
            time_seconds=elapsed,
            status="OK",
            tool_version=duckdb.__version__,
        )

    print("\ntop 5 zones by total fare revenue:")
    print(f"  {'Borough':<15} {'Zone':<32} {'total_fare':>14}")
    for borough, zone, total in rows:
        print(f"  {borough:<15} {zone:<32} {total:>14,.2f}")

    con.close()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
