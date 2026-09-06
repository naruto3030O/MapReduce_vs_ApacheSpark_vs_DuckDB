# Task 1 — one aggregation, three systems

**Input:** `data/taxi_1m.csv` (101.7 MB, 1,000,000 rows)

**Computation:** for every non-null `PULocationID`, the number of trips and
the average `fare_amount`, counting only rows where `fare_amount` is non-null
and greater than or equal to zero.

## Correctness

All three implementations produce 256 groups and 984,065 counted trips.
`verify_task1.py` compares them group by group and reports exact agreement on
both the counts and the averages. The roughly 16,000 rows that do not appear
in the totals are the ones removed by the filter — null pickup location, or a
null or negative fare.

I ran the correctness check before recording any timings, because three
implementations that disagree are not measuring the same thing and their
timings would not be comparable.

## Median timings

One unrecorded warm-up run, then three recorded runs, as required by the
timing protocol.

| System | Runs (s) | Median (s) | Relative |
|---|---|---|---|
| DuckDB | 0.148 / 0.142 / 0.148 | 0.148 | 1× |
| Spark | 1.054 / 0.830 / 0.805 | 0.830 | 5.6× slower |
| mrjob (inline runner) | 6.538 / 6.707 / 6.568 | 6.568 | 44× slower |

The SparkSession and the DuckDB connection were both created before the timed
region, so engine startup is not included in these numbers. Each run ends only
after the complete result has been materialised — `collect()` in Spark and
`fetchall()` in DuckDB — so no lazily defined DataFrame is being counted as a
finished run.

## MapReduce structure

- **Mapper key:** `PULocationID`, taken from column index 7 of the CSV.
- **Mapper value:** that single trip's `fare_amount`, converted to a float.
- **What crosses the shuffle:** one float per surviving row. The framework
  groups these values by key, so that every fare belonging to one pickup
  location arrives together at a single reducer call.
- **Reducer:** receives one location and an iterator over all its fares,
  accumulates a running count and a running sum, and emits the count and
  sum divided by count.

**A design decision I made.** Rows that fail the filter are discarded inside
the mapper, so they never cross the shuffle boundary at all. I could have
emitted everything and filtered in the reducer, and the answer would be the
same, but filtering early means less data has to be grouped and transferred.
On a single machine that saves memory and sorting work. On a real cluster the
shuffle is network traffic between machines, which is the expensive resource
in MapReduce, so filtering in the mapper is the standard approach.

**Why I did not use a combiner.** A combiner that emitted partial averages
would be wrong, because the average of several averages is not the true
average unless each one is weighted by how many rows it came from. A correct
combiner would have to emit (count, sum) pairs and leave the division to the
reducer. I chose to keep the code simple rather than add a combiner that
would have to be carefully reasoned about for a job this small.

## Column indices and header handling

`PU_IDX = 7` and `FARE_IDX = 10` are the positions of `PULocationID` and
`fare_amount` in the TLC yellow-taxi CSV.

mrjob has no concept of a CSV header. It hands the mapper raw text lines,
including the header line, which is why a naive job counts one row too many —
I saw exactly this when a smoke test on a 1000-row file returned 1001.

My mapper solves both problems in one place. When it sees a row whose field at
index 7 is literally the string `PULocationID`, it knows this is the header
line, checks that index 10 really is `fare_amount`, and then skips the line.
If the column order ever changed, that check fails and the job raises an error
immediately instead of silently averaging the wrong column. So the header is
not just skipped — it is used to validate that my hardcoded indices are still
correct.

I parse each line with `csv.reader` rather than `split(",")`, so a quoted
field containing a comma cannot shift the field positions.

Spark and DuckDB both understand `header=True`, so neither of them needed any
of this.

## Which implementation needed the most conceptual plumbing

DuckDB was the most direct. The requirement maps onto SQL almost word for
word: a `WHERE` clause for the filter and a `GROUP BY` for the grouping.
Spark was close behind, expressing the same relational operations as method
calls on a DataFrame.

MapReduce needed by far the most plumbing, and not because it is longer. It
was the only version where I had to build the grouping myself. I had to decide
what to emit as the key and what to emit as the value, reason about what
crosses the shuffle boundary, handle the header manually, know the physical
column positions in the file, and think about whether a combiner would even be
correct. In the other two the engine supplies the grouping; in MapReduce the
grouping *is* the program, and everything else is arranged around it.

## Prediction outcome

I predicted Spark would win. My reasoning was that it would split the CSV
across partitions and parse it on all 16 logical processors, and that session
startup was excluded from the timed region anyway.

Spark lost to DuckDB by a factor of 5.6.

What I got wrong was treating parallelism as the thing that made Spark
special. DuckDB is also multi-threaded and vectorised, so it used the same
cores I was counting on — but with no JVM, no Catalyst planning on every
query, no task scheduling, and no Python-to-JVM data boundary. On a single
machine, with a 100 MB file that fits many times over in 16 GB of RAM, all of
Spark's coordination machinery is a cost with nothing on the other side of the
ledger, because there are no other machines to coordinate.

What I learned is that the useful question is not "can this tool use many
cores" but "what does this tool's design assume, and does that assumption hold
here?" Spark assumes the data is too large for one machine. That assumption is
false for this file, so the architecture built to handle it becomes overhead.

## How to read the mrjob number

The mrjob figure measures a local, single-process runner — specifically the
inline runner, because the local runner fails on Windows. It is not a
measurement of Hadoop, and it must not be read as one.

What it measures is the cost of executing the MapReduce programming model in
Python: roughly a million interpreted mapper calls, each constructing a CSV
reader and parsing a float, with no vectorisation and no query optimiser. What
it does not measure is the thing MapReduce actually exists for, which is
fault-tolerant batch processing spread across many machines, on data that
would never fit on this laptop.

The inline runner also skips per-step process startup, which makes mrjob's
fixed overhead slightly lower than the local runner's would be. That works in
mrjob's favour at small input sizes, and I take it into account when
interpreting the Task 4 scaling curve.
