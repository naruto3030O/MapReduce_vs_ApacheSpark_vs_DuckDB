# Task 3 — join with the taxi-zone lookup

**Inputs:** `data/taxi_2m.parquet` (40.1 MB, 2,000,000 rows) and
`data/taxi_zone_lookup.csv` (12,331 bytes, 265 zones)

**Computation:** join `PULocationID` with `LocationID`, keep rows with
`fare_amount >= 0`, total `fare_amount` per Borough + Zone, return the five
pickup zones with the highest total revenue, ties broken by Zone ascending.

## Result

Both systems returned exactly the same five rows and the same totals.

| Borough | Zone | total_fare |
|---|---|---|
| Queens | JFK Airport | 6,849,853.78 |
| Queens | LaGuardia Airport | 2,825,841.42 |
| Manhattan | Midtown Center | 1,475,111.82 |
| Manhattan | Upper East Side South | 1,281,774.03 |
| Manhattan | Upper East Side North | 1,223,447.97 |

The two airports dominate, which makes sense: airport trips are long and
flat-rated, so a moderate number of trips produces very high total revenue.
JFK alone brings in more than twice LaGuardia and more than four times the
busiest Manhattan zone.

## Median timings

| System | Runs (s) | Median (s) | Relative |
|---|---|---|---|
| DuckDB | 0.032 / 0.033 / 0.031 | 0.032 | 1× |
| Spark | 0.811 / 0.678 / 0.630 | 0.678 | 21× slower |

## Does Spark choose a broadcast?

Yes, and without any hint from me. The plan shows
`BroadcastHashJoin Inner BuildRight` fed by a `BroadcastExchange` on the
lookup side.

The reason is visible in the numbers the script printed before the plan:
`spark.sql.autoBroadcastJoinThreshold` is 10,485,760 bytes and the lookup
file is 12,331 bytes — roughly 850 times under the threshold. Spark estimates
the small side will fit comfortably in memory, so it builds a hash table from
the 265 zone rows, ships a copy to every task, and streams the 2M taxi rows
past it. `BuildRight` confirms the lookup is the side being built into the
hash table.

This is the right decision. The alternative, a sort-merge join, would require
shuffling both sides by the join key — moving two million rows across the
network on a real cluster in order to meet a table of 265 rows. Broadcasting
copies 12 KB instead. I did not force this behaviour, and did not need to.

## Where the data actually moves

The join is not the source of data movement here, which surprised me. The
`Exchange` at step (9) is, and it belongs to the `groupBy`, not the join:

```
(9) Exchange
Arguments: hashpartitioning(Borough#38, Zone#39, 200), ENSURE_REQUIREMENTS
```

Spark does a partial aggregation first (step 8, `partial_sum`), shuffles the
partial sums so that all rows for a given Borough + Zone land in the same
partition, then finishes the aggregation (step 10). Because the partial
aggregation happens before the shuffle, what crosses it is one partial sum
per group per partition, not two million rows.

The `hashpartitioning(..., 200)` is worth noticing. Spark shuffles into 200
partitions by default, for a result that has only a few hundred groups and
ultimately returns five rows. Most of those partitions will be empty or hold
a single row. The plan ends with `AdaptiveSparkPlan ... isFinalPlan=false`,
meaning adaptive query execution may coalesce those partitions at runtime,
but the plan as compiled is sized for a cluster, not for this laptop.

Step (11) is `TakeOrderedAndProject`, not a full sort. Spark keeps only the
top 5 per partition and merges, rather than ordering every group.

## Projection pushdown again

The Parquet scan reads only two columns: `Output [2]: [PULocationID,
fare_amount]`, with `ReadSchema` confirming the same. Neither Borough nor
Zone comes from the Parquet file, and none of the other 18 columns is
touched. The `fare_amount >= 0` filter is pushed into the scan as well.

DuckDB does the same, reading only `PULocationID` and `fare_amount` from the
Parquet file with `Filters: fare_amount>=0.0` inside the scan operator.

## What DuckDB does differently

DuckDB's plan is `HASH_JOIN` over `READ_PARQUET` and `READ_CSV`, then
`HASH_GROUP_BY`, then `TOP_N`. Structurally the same strategy — hash the
small side, stream the large one — but with no exchange operator anywhere,
because there is nothing to exchange between. Everything runs in one process
with shared memory, so the concept of moving data between partitions does not
arise.

That is the whole 21× gap. Spark did not choose a worse plan; it chose a good
one and then paid to execute a distributed plan on a machine with nothing to
distribute to. On a real cluster with the data spread across many nodes, that
same `Exchange` would be doing necessary work.

One small detail in the DuckDB plan: the join condition is
`CAST(PULocationID AS BIGINT) = LocationID`. `PULocationID` is a 32-bit
integer in the Parquet file, while DuckDB's CSV sniffer inferred
`LocationID` as a 64-bit integer, so one side is widened before comparison.
DuckDB's estimate of ~616 rows for the lookup is also wrong — the file has
265 — because it estimates CSV cardinality from file size rather than
reading it first.

## MapReduce version (description only, not implemented)

I would distribute the small lookup to the workers rather than perform a
reduce-side join. Each mapper loads `taxi_zone_lookup.csv` into an in-memory
dictionary keyed by `LocationID` before processing any records, since 265
rows is trivially small. For every taxi row with `fare_amount >= 0`, the
mapper looks up the pickup zone locally and emits the composite key
`(Borough, Zone)` with the value `fare_amount`. What is grouped at the
reducer is therefore every fare belonging to one Borough + Zone pair, and the
reducer simply sums them and emits the total. A second, single-reducer job
would then take the top five by total.

I would not use a reduce-side join. That would require emitting both datasets
keyed by `LocationID`, tagged by source, so that the reducer could match them
— which means shuffling all two million taxi rows across the network purely
to pair them with a 12 KB table. Shipping the lookup to the mappers moves
kilobytes instead of gigabytes, and it lets the join happen in the map phase
so that no join-related data crosses the shuffle at all. The only reason to
prefer a reduce-side join would be a lookup table too large to hold in each
mapper's memory, which is not the case here.

## Prediction outcome

I predicted DuckDB for this scenario, reasoning that the lookup would fit in
memory as a hash table, that neither engine would need to move the large
table on a single machine, and that DuckDB's lower per-query overhead would
decide it. That was correct, and the plans confirm the mechanism.

My "what would prove me wrong" test was whether Spark's plan showed a shuffle
rather than a broadcast for the small lookup. It showed a broadcast, so
Spark's join strategy was not the problem. The margin came from elsewhere —
the shuffle Spark performs for the aggregation, and the general cost of
running a distributed execution model on one machine.
