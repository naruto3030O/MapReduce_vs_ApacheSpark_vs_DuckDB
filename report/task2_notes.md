# Task 2 — Parquet filtering and projection

**Input:** `data/taxi_2m.parquet` (40.1 MB, 2,000,000 rows)
**Filter:** `PULocationID = 161 AND fare_amount > 30.00`
**Query A:** all columns for matching rows
**Query B:** only `tpep_pickup_datetime`, `tpep_dropoff_datetime`,
`trip_distance`, `fare_amount` for the same rows

## Median timings

One unrecorded warm-up run, then three recorded runs, per query per system.

| System | Query A (all columns) | Query B (four columns) | B faster by |
|---|---|---|---|
| DuckDB | 0.078 s | 0.025 s | 3.1× |
| Spark | 0.330 s | 0.198 s | 1.7× |

Both systems got faster purely because I asked for fewer columns. The rows
matched and the filter applied are identical in every case.

## Why fewer columns costs less

Parquet is a columnar format. Each column is stored as its own contiguous
chunk of the file, so a query can read only the chunks it needs and leave the
rest on disk untouched. The file has 20 columns; Query B needs 4 of them for
output plus 1 more for the filter, so it touches roughly a quarter of the
data that Query A does.

This is the whole answer to the question the task asks. In a row-oriented
format such as CSV the two queries would cost the same, because every field
of a row sits next to every other field — to reach `fare_amount` you must
read past all the columns in front of it. There is no way to skip a column
you do not want. Cost in CSV scales with rows; cost in Parquet scales with
rows *and* columns, and the second one is under the query's control.

## Evidence in the Spark plan

The `ReadSchema` line under `Scan parquet` is the direct evidence.

- Query A: `Output [20]` — all twenty columns are read.
- Query B: `Output [5]` — only five.

The five is worth noticing. I asked for four columns, but the scan reads
`tpep_pickup_datetime`, `tpep_dropoff_datetime`, `trip_distance`,
`PULocationID` and `fare_amount`. `PULocationID` is not in my result — it is
read because the filter needs it, and operator (4) `Project` drops it
afterwards. So the columns a query reads are the ones it returns plus the
ones it filters on, not just the ones in the SELECT list.

`PushedFilters` shows all four predicates reaching the Parquet reader:
`IsNotNull(PULocationID)`, `IsNotNull(fare_amount)`, `EqualTo(PULocationID,161)`
and `GreaterThan(fare_amount,30.0)`. Spark added the two null checks itself,
since a null can never satisfy either comparison.

## Evidence in the DuckDB plan

The `READ_PARQUET` operator lists its `Projections` directly.

- Query A: all twenty columns.
- Query B: only `fare_amount`, `tpep_pickup_datetime`,
  `tpep_dropoff_datetime`, `trip_distance`.

Underneath, `Filters: PULocationID=161, fare_amount>30.0` shows the predicate
is part of the scan itself rather than a separate step above it.

## The difference between the two plans

Spark pushes the filter to the Parquet reader but still keeps a separate
`Filter` operator above the scan. That is not redundant. Pushdown into
Parquet works at row-group granularity: the reader uses each row group's
min/max statistics to skip groups that cannot contain a match, but any group
holding even one matching row is read in full. Spark therefore has to
re-evaluate the condition row by row afterwards.

DuckDB applies the predicate inside the scan, row by row, which is why
`PULocationID` never appears in its Query B projection list at all. The
column is consumed during scanning and never passed upward. Spark has to hand
it to the Filter operator and then discard it with an extra Project.

## Why DuckDB gained more from projection than Spark

DuckDB got 3.1× faster from projection; Spark only 1.7×. Projection reduces
the bytes read, but it cannot reduce the work an engine does before it
touches the file. Spark plans the query, schedules tasks and moves results
across the Python-to-JVM boundary on every run, and none of that shrinks when
I ask for fewer columns. That fixed cost dilutes the saving. DuckDB has
almost no fixed cost, so a reduction in bytes read shows up almost fully in
the wall-clock time.

## Something I did not expect

`taxi_2m.parquet` is 40.1 MB while `taxi_1m.csv` is 101.7 MB. Twice the rows
in 40% of the space, because Parquet is compressed and stores like values
together.

The consequence is more striking than the file size. DuckDB's Query A over
2M rows took 0.078 s, while its Task 1 aggregation over 1M rows took 0.148 s.
Twice the data in roughly half the time. Nothing about the engine changed
between those two measurements — the difference is entirely that one query
had to parse text and the other read a compressed binary column layout. On
this evidence the input format mattered more than the row count.

## Note on the estimate in the DuckDB plan

The `~400,000 rows` figure in the DuckDB plan is the optimiser's cardinality
estimate, and it is identical for both queries. It is not the number of rows
actually returned. The real count is reported by the runner after the warm-up
run.
