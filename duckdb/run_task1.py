"""
Task 1 - DuckDB timing runner.

The query itself lives in duckdb/task1.sql (section 16 requires that file).
This script loads it, substitutes the input path, and applies the timing
protocol from section 5:

  - the DuckDB connection is opened BEFORE the timed region
  - the timed region ends only after fetchall() has materialised the result
  - one unrecorded warm-up run, then three recorded runs

Run:
    python duckdb/run_task1.py data/taxi_1m.csv 1000000
"""

import sys
import time
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import append_timing, save_result

SQL_FILE = Path(__file__).resolve().parent / "task1.sql"


def load_query(input_path):
    sql = SQL_FILE.read_text(encoding="utf-8")
    # DuckDB needs forward slashes in string literals on Windows.
    return sql.replace("{{INPUT}}", str(input_path).replace("\\", "/"))


def run_once(con, query):
    start = time.perf_counter()
    rows = con.execute(query).fetchall()  # materialises the complete result
    elapsed = time.perf_counter() - start
    return elapsed, rows


def main(input_path, dataset_rows):
    input_path = Path(input_path)
    query = load_query(input_path)

    # Connection opened outside every timed region.
    con = duckdb.connect()

    print(f"DuckDB {duckdb.__version__} | {input_path.name} | "
          f"{int(dataset_rows):,} rows")

    print("warm-up run (not recorded)...")
    run_once(con, query)

    rows = None
    for run_number in (1, 2, 3):
        elapsed, rows = run_once(con, query)
        append_timing(
            system="duckdb",
            task="task1",
            condition="aggregation",
            dataset_rows=dataset_rows,
            input_format="csv",
            input_path=input_path,
            run_number=run_number,
            time_seconds=elapsed,
            status="OK",
            tool_version=duckdb.__version__,
        )

    save_result("duckdb", "task1", rows)
    print("\nsample of result:")
    for row in sorted(rows, key=lambda r: int(r[0]))[:5]:
        print(f"  PULocationID={row[0]}  trips={row[1]}  avg_fare={row[2]:.4f}")

    con.close()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
