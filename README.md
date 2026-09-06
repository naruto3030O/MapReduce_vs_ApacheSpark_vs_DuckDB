# BDA Assignment 1 — MapReduce vs Spark vs DuckDB

Student ID: 2023481

A controlled comparison of MapReduce (mrjob), Apache Spark and DuckDB on the
January 2026 NYC Yellow Taxi trip records. All timings in
`results/timings.csv` were measured on one physical laptop with the data
stored locally.

## Environment

All timed experiments were run inside a Python 3.11 virtual environment
(`.venv`) in the project root.

| Package | Version |
|---------|---------|
| Python | 3.11.9 |
| mrjob | 0.7.4 |
| PySpark | 4.2.0 |
| DuckDB | 1.5.5 |
| pandas | 2.3.3 |
| pyarrow | 25.0.1 |
| matplotlib | 3.11.1 |

### Setup

    py -3.11 -m venv .venv
    .\.venv\Scripts\Activate.ps1
    python -m pip install mrjob pyspark duckdb "pandas<3.0.0" pyarrow matplotlib

Activate `.venv` before running anything. Every recorded run used this
environment, and `config.py` writes the running Python version into each row
of `timings.csv`.

## Machine configuration

| Field | Value |
|---|---|
| os | Windows 11 25H2 (OS Build 26200.9278) |
| cpu_model | AMD Ryzen 7 8845HS w/ Radeon 780M Graphics |
| cpu_base_ghz | 3.80 |
| cpu_physical_cores | 8 |
| cpu_logical_processors | 16 |
| ram_gb | 16 |
| storage_size_gb | 954 |
| storage_type | NVMe SSD |
| gpu_model | NVIDIA GeForce RTX 4050 Laptop GPU |
| gpu_clock_mhz | 3105 |
| gpu_memory_gb | 6.0 |

These values are repeated in every row of `results/timings.csv`.

## Expected data files

The raw datasets are not included in the submission. Place these in `data/`
before running anything:

| File | Source |
|---|---|
| `yellow_tripdata_2026-01.parquet` | NYC TLC trip record data |
| `taxi_zone_lookup.csv` | NYC TLC misc data |

Then generate the four samples, from the project root:

    duckdb < data_prep/prepare_data.sql

This creates `taxi_100k`, `taxi_500k`, `taxi_1m` and `taxi_2m` in both CSV and
Parquet inside `data/`. Paths in the script are relative to the project root.

Verify the row counts are exactly 100,000 / 500,000 / 1,000,000 / 2,000,000
before running any timed task.

## Running the tasks

All commands are run from the project root with `.venv` active. Each script
appends its measured runs to `results/timings.csv`.

### Task 1 — one aggregation, three systems

    python duckdb/run_task1.py data/taxi_1m.csv 1000000
    python spark/task1.py data/taxi_1m.csv 1000000
    python mapreduce/task1.py --bench data/taxi_1m.csv 1000000
    python verify_task1.py

`verify_task1.py` compares the three result files group by group and must
report `ALL THREE IMPLEMENTATIONS AGREE` before the timings are meaningful.

To run the MapReduce job normally and print its output instead of timing it:

    python mapreduce/task1.py -r inline data/taxi_1m.csv

### Task 2 — Parquet filtering and projection

    python duckdb/run_task2.py data/taxi_2m.parquet 2000000
    python spark/task2.py data/taxi_2m.parquet 2000000

Also writes `duckdb/task2_explain.txt` and `spark/task2_explain.txt`.

### Task 3 — join with the zone lookup

    python duckdb/run_task3.py data/taxi_2m.parquet data/taxi_zone_lookup.csv 2000000
    python spark/task3.py data/taxi_2m.parquet data/taxi_zone_lookup.csv 2000000

Both print their execution plan before the timed runs. No broadcast hint is
applied; whatever Spark chooses on its own is what is reported.

### Task 4 — scaling experiment

    python run_task4.py
    python plot_scaling.py

`run_task4.py` runs all three systems at all four sizes (36 recorded runs) and
writes `results/scaling_plot.png` via `plot_scaling.py`. It does not
re-implement the aggregation — it imports the same functions used in Task 1,
so the computation being scaled is provably identical.

One system can be re-run on its own:

    python run_task4.py duckdb
    python run_task4.py spark
    python run_task4.py mapreduce

### Task 5 — repeated work on the same data

    python spark/task5.py data/taxi_1m.parquet 1000000
    python duckdb/run_task5.py data/taxi_1m.parquet 1000000

## Non-default settings and platform deviations

1. **Python 3.11, not the latest release.** mrjob 0.7.4 imports the
   standard-library `pipes` module, removed in Python 3.13. On Python 3.14
   every mrjob run fails at import with
   `ModuleNotFoundError: No module named 'pipes'`. The project therefore
   targets Python 3.11.

2. **mrjob `inline` runner, not `local`.** On Windows 11 the `local` runner
   fails when spawning step subprocesses with
   `[WinError 2] The system cannot find the file specified`, reproduced both
   with default settings and with an explicit `--python-bin`. All mrjob jobs
   run with `-r inline`. The mapper, shuffle (group-by-key) and reducer logic
   are identical; only per-step process startup is avoided, which lowers
   mrjob's fixed overhead at small input sizes. This is accounted for when
   interpreting the Task 4 scaling curve.

3. **Spark reads CSV with `inferSchema=False`.** Type inference makes Spark
   scan the file an extra time purely to guess types, which is not part of the
   computation being compared. Only `PULocationID` and `fare_amount` are cast.

4. **pandas pinned below 3.0.0.** PySpark 4.2.0 warns that pandas >= 3.0.0 is
   not yet fully supported.

5. **Spark warnings on Windows.** Spark logs missing `winutils.exe` /
   `HADOOP_HOME` and `NativeCodeLoader` warnings. These do not affect the
   measured tasks, which read local files and materialise results in memory
   without writing Hadoop output.

6. **No GPU acceleration.** GPU details are recorded as machine context only;
   mrjob, Spark local mode and DuckDB all execute on CPU.

7. **No warm-up run in Task 5.** Section 5 requires a warm-up except where a
   task asks for five repeated runs. Task 5 does, and whether the first run is
   slower than the later ones is the observation being made, so discarding it
   would hide the effect.

8. **`{{INPUT}}` placeholders in the DuckDB `.sql` files** are substituted at
   run time by the matching `run_taskN.py`. To run a `.sql` file directly in
   the DuckDB CLI, replace the placeholder with the file path first.

## Timing protocol

Every measured run followed section 5: the laptop was on power with avoidable
heavy workloads closed, all four datasets sat on the same internal NVMe drive,
and the SparkSession and DuckDB connection were created before the timed
region so that engine startup is never included. Each timed region ends only
after the complete result has been materialised — `collect()` in Spark,
`fetchall()` in DuckDB, and full parsing of the job output for mrjob. Results
were not printed inside the timed region. Every task used one unrecorded
warm-up plus three recorded runs, except Task 5 as noted above. No run
exceeded 900 seconds, so no `TIMEOUT_900` rows appear and
`results/errors.txt` is not included.

## Repository layout

    README.md
    config.py              machine config + timings.csv writer, shared by all scripts
    run_task4.py           Task 4 driver (imports the Task 1 implementations)
    plot_scaling.py        builds results/scaling_plot.png from timings.csv
    verify_task1.py        checks the three Task 1 implementations agree
    data_prep/prepare_data.sql
    mapreduce/task1.py
    spark/task1.py task2.py task2_explain.txt task3.py task5.py
    duckdb/task1.sql run_task1.py task2.sql run_task2.py task2_explain.txt
           task3.sql run_task3.py task5.sql run_task5.py
    results/timings.csv scaling_plot.png
    ai/ai_usage.md
    report/observations.pdf

`config.py`, `run_task4.py`, `plot_scaling.py` and `verify_task1.py` sit in
the project root because they are shared across tasks rather than belonging to
one system. The DuckDB `run_taskN.py` files exist because section 16 requires
the queries themselves to live in `.sql` files; the runners load those files,
substitute the input path and apply the timing protocol.
