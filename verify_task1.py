"""
Verify that the three Task 1 implementations agree numerically.

Section 7 requires that MapReduce, Spark and DuckDB produce the same result
up to normal floating-point rounding. Run this after all three scripts:

    python verify_task1.py
"""

import csv
from pathlib import Path

RESULTS = Path(__file__).resolve().parent / "results"
SYSTEMS = ["mapreduce", "spark", "duckdb"]
TOLERANCE = 1e-6


def load(system):
    path = RESULTS / f"task1_{system}.csv"
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as f:
        return {
            int(r["PULocationID"]): (int(r["trip_count"]), float(r["avg_fare"]))
            for r in csv.DictReader(f)
        }


def main():
    data = {}
    for system in SYSTEMS:
        result = load(system)
        if result is None:
            print(f"MISSING: results/task1_{system}.csv has not been created")
            return
        data[system] = result
        print(f"{system:11s} {len(result)} groups, "
              f"{sum(c for c, _ in result.values()):,} trips counted")

    reference = data["duckdb"]
    all_ok = True

    for system in ["mapreduce", "spark"]:
        other = data[system]
        problems = []

        missing = set(reference) - set(other)
        extra = set(other) - set(reference)
        if missing:
            problems.append(f"{len(missing)} keys missing (e.g. "
                            f"{sorted(missing)[:3]})")
        if extra:
            problems.append(f"{len(extra)} unexpected keys (e.g. "
                            f"{sorted(extra)[:3]})")

        for key in sorted(set(reference) & set(other)):
            ref_count, ref_avg = reference[key]
            got_count, got_avg = other[key]
            if ref_count != got_count:
                problems.append(f"PULocationID {key}: count "
                                f"{got_count} != {ref_count}")
            if abs(ref_avg - got_avg) > TOLERANCE:
                problems.append(f"PULocationID {key}: avg_fare "
                                f"{got_avg:.6f} != {ref_avg:.6f}")

        if problems:
            all_ok = False
            print(f"\nMISMATCH {system} vs duckdb:")
            for p in problems[:10]:
                print(f"  {p}")
            if len(problems) > 10:
                print(f"  ... and {len(problems) - 10} more")
        else:
            print(f"\nOK: {system} matches duckdb exactly")

    print("\n" + ("ALL THREE IMPLEMENTATIONS AGREE" if all_ok
                  else "DIFFERENCES FOUND - fix before recording timings"))


if __name__ == "__main__":
    main()
