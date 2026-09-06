"""
# AI-ASSISTED: A7
Task 5 - DuckDB: repeated work on the same data.

Runs the Task 1 aggregation five times against taxi_1m.parquet inside a
single DuckDB connection, and records all five runtimes.

Note on protocol: there is deliberately NO warm-up run here. Section 5 asks
for a warm-up "unless the task explicitly asks for five repeated runs", and
Task 5 does. Whether the first run is slower than the later ones is the
observation being made, so discarding it would hide the effect.

The connection is not closed or restarted between the five runs.

Run:
    python duckdb/run_task5.py data/taxi_1m.parquet 1000000
"""

import sys
import time
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import append_timing

SQL_FILE = Path(__file__).resolve().parent / "task5.sql"


def load_query(input_path):
    sql = SQL_FILE.read_text(encoding="utf-8")
    # DuckDB needs forward slashes in string literals on Windows.
    return sql.replace("{{INPUT}}", str(input_path).replace("\\", "/"))


def main(input_path, dataset_rows):
    input_path = Path(input_path)
    query = load_query(input_path)

    # Connection opened outside every timed region and kept open throughout.
    con = duckdb.connect()

    print(f"DuckDB {duckdb.__version__} | {input_path.name} | "
          f"{int(dataset_rows):,} rows")
    print("five repeated runs, same connection, no warm-up\n")

    times = []
    for run_number in (1, 2, 3, 4, 5):
        start = time.perf_counter()
        rows = con.execute(query).fetchall()
        elapsed = time.perf_counter() - start
        times.append(elapsed)

        append_timing(
            system="duckdb",
            task="task5",
            condition="duckdb_repeated",
            dataset_rows=dataset_rows,
            input_format="parquet",
            input_path=input_path,
            run_number=run_number,
            time_seconds=elapsed,
            status="OK",
            tool_version=duckdb.__version__,
        )

    print(f"\ngroups returned: {len(rows)}")
    print(f"run 1: {times[0]:.4f}s | run 5: {times[4]:.4f}s | "
          f"change: {(times[0] - times[4]) / times[0] * 100:+.1f}%")

    con.close()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
