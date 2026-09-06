# Part A — Predictions

These predictions were written before running the timed experiments for
Tasks 2, 3, 4 and 5.

Honesty note: scenarios 1, 2, 3 and 5 were reasoned out before any timed run.
Scenario 4 was written after Task 1 had already been measured at 1M rows, so
my prediction about the shape of the scaling curve is partly informed by that
one data point. I am recording this rather than pretending otherwise.

| Scenario | Predicted best tool | Reason |
|---|---|---|
| Aggregate a 1M-row CSV by pickup location | Spark | Spark splits the ~100 MB CSV across partitions and parses it on all 16 logical processors. CSV parsing is CPU-bound, so it should parallelise well, and the timing protocol excludes SparkSession startup from the measured region. |
| Filter a 2M-row Parquet file and return four columns | DuckDB | Parquet stores each column separately, so both engines can read only the four columns asked for and skip row groups that cannot match the filter. Both do this, so the winner is whichever has less machinery between the query and the file, and DuckDB runs in-process with no planning or scheduling layer. |
| Join 2M taxi rows with the small zone lookup | DuckDB | The zone lookup is only a few hundred rows, so it fits in memory and can be built into a hash table that the 2M-row side streams past. On one machine there is no network, so neither engine has to move the large table, and DuckDB's lower per-query overhead should decide it. |
| Scale the same aggregation from 100K to 2M rows | DuckDB | I expect DuckDB fastest at every size. mrjob should have the steepest curve because its cost is one interpreted Python function call per row, which grows directly with input size. Spark's curve should look flattest in relative terms, because its fixed per-query planning cost dominates at 100K and matters less at 2M. I do not expect a crossover inside this range. |
| Repeat the same analytical query several times on the same data | DuckDB | After the first run the file is in the OS page cache, so nobody is paying real disk cost on later runs. Spark's cache() stores an already-parsed copy in memory, which should help it most because parsing is its biggest cost, but it has to pay a one-time build cost first. DuckDB re-reads from the page cache each time and is already fast enough that I expect it to stay ahead. |

## What would prove each prediction wrong

**Scenario 1.** If DuckDB is also multi-threaded, then parallelism is not
Spark's unique advantage, and Spark's per-query planning and Python-to-JVM
boundary become pure overhead on a single machine.

**Scenario 2.** If Query A (all columns) and Query B (four columns) take
about the same time, then projection is not actually reducing the bytes read
and my understanding of columnar storage is wrong.

**Scenario 3.** If Spark's physical plan shows a shuffle rather than a
broadcast for the small lookup, then it is moving the large table
unnecessarily and the join strategy matters more than raw engine overhead.

**Scenario 4.** If Spark's curve is flat while DuckDB's rises, that would
mean Spark's parallelism is winning at scale and my range was simply too
small to show the crossover.

**Scenario 5.** If Spark cached beats DuckDB, then avoiding repeated parsing
matters more than avoiding engine overhead, and caching is worth its build
cost sooner than I assumed.
