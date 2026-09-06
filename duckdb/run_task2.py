"""
# AI-ASSISTED: A4
Task 2 - DuckDB timing runner and EXPLAIN capture.

The queries live in duckdb/task2.sql (required by section 16). This script
loads them, substitutes the input path, applies the timing protocol from
section 5, and writes duckdb/task2_explain.txt.

  - the DuckDB connection is opened BEFORE the timed region
  - the timed region ends only after fetchall() has materialised the result
  - one unrecorded warm-up run, then three recorded runs, per query

Run:
    python duckdb/run_task2.py data/taxi_2m.parquet 2000000
"""

import sys
import time
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import append_timing

HERE = Path(__file__).resolve().parent
SQL_FILE = HERE / "task2.sql"
EXPLAIN_FILE = HERE / "task2_explain.txt"


def load_queries(input_path):
    """Return (query_a, query_b) with the input path substituted in."""
    sql = SQL_FILE.read_text(encoding="utf-8")
    # DuckDB needs forward slashes in string literals on Windows.
    sql = sql.replace("{{INPUT}}", str(input_path).replace("\\", "/"))
    parts = [p.strip() for p in sql.split("--@SPLIT")]
    if len(parts) != 2:
        raise ValueError("task2.sql must contain exactly one --@SPLIT marker")
    return parts[0], parts[1]


def run_once(con, query):
    start = time.perf_counter()
    rows = con.execute(query).fetchall()  # materialises the complete result
    elapsed = time.perf_counter() - start
    return elapsed, rows


def capture_explain(con, query_a, query_b):
    """Write the EXPLAIN output for both queries to task2_explain.txt."""
    lines = []
    for label, query in (("QUERY A - all columns", query_a),
                         ("QUERY B - four columns", query_b)):
        lines.append("=" * 70)
        lines.append(f"DuckDB EXPLAIN - {label}")
        lines.append("=" * 70)
        lines.append(query)
        lines.append("-" * 70)
        for row in con.execute("EXPLAIN " + query).fetchall():
            lines.append(str(row[-1]))
        lines.append("")

    EXPLAIN_FILE.write_text("\n".join(lines), encoding="utf-8")
    print(f"  explain written: {EXPLAIN_FILE.name}")


def main(input_path, dataset_rows):
    input_path = Path(input_path)
    query_a, query_b = load_queries(input_path)

    # Connection opened outside every timed region.
    con = duckdb.connect()

    print(f"DuckDB {duckdb.__version__} | {input_path.name} | "
          f"{int(dataset_rows):,} rows")

    capture_explain(con, query_a, query_b)

    for condition, query in (("query_a_all_columns", query_a),
                             ("query_b_projection", query_b)):
        print(f"\n{condition}")
        print("  warm-up run (not recorded)...")
        _, rows = run_once(con, query)
        print(f"  matching rows: {len(rows):,} | "
              f"columns returned: {len(rows[0]) if rows else 0}")

        for run_number in (1, 2, 3):
            elapsed, _ = run_once(con, query)
            append_timing(
                system="duckdb",
                task="task2",
                condition=condition,
                dataset_rows=dataset_rows,
                input_format="parquet",
                input_path=input_path,
                run_number=run_number,
                time_seconds=elapsed,
                status="OK",
                tool_version=duckdb.__version__,
            )

    con.close()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
