"""
Task 1 - MapReduce implementation (mrjob).

Computation: for every non-null PULocationID, count trips and average
fare_amount, considering only rows where fare_amount is non-null and >= 0.

MapReduce structure (section 7):
    mapper key   = PULocationID
    mapper value = fare_amount for that single trip
    shuffle      = all fare_amount values for the same PULocationID are
                   grouped together and delivered to one reducer call
    reducer      = receives one PULocationID and every fare belonging to it,
                   then emits (trip_count, avg_fare)

Note on the runner: the mrjob 'local' runner fails on Windows 11 with
[WinError 2] when spawning step subprocesses, so the 'inline' runner is used.
The mapper / shuffle / reducer logic is identical; only per-step process
startup is avoided. See README.md.

Run standalone (prints the job output, untimed): #run this when data manipulation is asked in viva
    python mapreduce/task1.py -r inline data/taxi_1m.csv

Run the timed experiment (warm-up + 3 recorded runs):
    python mapreduce/task1.py --bench data/taxi_1m.csv 1000000
"""

import csv
import io
import sys
import time
from pathlib import Path

from mrjob.job import MRJob

# Column positions in the TLC yellow-taxi CSV. These are validated against the
# real header line every run (see mapper), so a wrong index fails loudly
# instead of silently producing wrong numbers.
PU_IDX = 7
FARE_IDX = 10


class MRTask1(MRJob):
    """Trip count and average fare per pickup location."""

    def mapper(self, _, line):
        # csv.reader handles quoted fields correctly; a plain split(',') would
        # not. The CSV header is not data, and mrjob has no concept of a
        # header row, so it is detected and skipped here.
        row = next(csv.reader(io.StringIO(line)))

        if row[PU_IDX] == "PULocationID":
            if row[FARE_IDX] != "fare_amount":
                raise ValueError(
                    f"Column layout unexpected: index {FARE_IDX} is "
                    f"'{row[FARE_IDX]}', not 'fare_amount'. "
                    "Check PU_IDX / FARE_IDX against the CSV header."
                )
            return  # skip the header line

        pu = row[PU_IDX].strip()
        fare = row[FARE_IDX].strip()

        # Filter: PULocationID non-null, fare_amount non-null and >= 0.
        # An empty string is how a NULL appears in the CSV.
        if not pu or not fare:
            return

        fare_value = float(fare)
        if fare_value < 0:
            return

        yield pu, fare_value

    def reducer(self, pu_location_id, fares):
        # Everything that crossed the shuffle boundary for this key arrives
        # here as an iterator of individual fare values.
        count = 0
        total = 0.0
        for fare in fares:
            count += 1
            total += fare
        yield pu_location_id, (count, total / count)


def run_once(input_path):
    """
    Execute one complete MapReduce job and fully materialise the result.
    Returns (elapsed_seconds, rows).
    """
    job = MRTask1(args=["-r", "inline", "--no-output", str(input_path)])

    start = time.perf_counter()
    with job.make_runner() as runner:
        runner.run()
        rows = [
            (pu, count, avg)
            for pu, (count, avg) in job.parse_output(runner.cat_output())
        ]
    elapsed = time.perf_counter() - start

    return elapsed, rows


def bench(input_path, dataset_rows):
    import mrjob

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from config import append_timing, save_result

    input_path = Path(input_path)
    print(f"mrjob {mrjob.__version__} | {input_path.name} | "
          f"{int(dataset_rows):,} rows")

    print("warm-up run (not recorded)...")
    run_once(input_path)

    rows = None
    for run_number in (1, 2, 3):
        elapsed, rows = run_once(input_path)
        append_timing(
            system="mapreduce",
            task="task1",
            condition="aggregation",
            dataset_rows=dataset_rows,
            input_format="csv",
            input_path=input_path,
            run_number=run_number,
            time_seconds=elapsed,
            status="OK",
            tool_version=mrjob.__version__,
        )

    save_result("mapreduce", "task1", rows)
    print("\nsample of result:")
    for row in sorted(rows, key=lambda r: int(r[0]))[:5]:
        print(f"  PULocationID={row[0]}  trips={row[1]}  avg_fare={row[2]:.4f}")


if __name__ == "__main__":
    if "--bench" in sys.argv:
        i = sys.argv.index("--bench")
        bench(sys.argv[i + 1], sys.argv[i + 2])
    else:
        MRTask1.run()
