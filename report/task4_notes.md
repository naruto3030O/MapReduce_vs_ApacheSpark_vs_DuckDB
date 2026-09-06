# Task 4 — scaling experiment

The same aggregation as Task 1 (trip count and average fare by
`PULocationID`), on CSV input, at four sizes, in all three systems. One
unrecorded warm-up per system/dataset pair, then three recorded runs. 36
recorded runs in total, all completed; no timeouts.

The aggregation code was not rewritten for this task. `run_task4.py` imports
the same functions used in Task 1, so the computation being scaled is
provably identical to the one already verified for numerical agreement.

## Median execution time (seconds)

| Rows | MapReduce | Spark | DuckDB |
|---|---|---|---|
| 100,000 | 1.182 | 0.422 | 0.094 |
| 500,000 | 3.554 | 0.620 | 0.107 |
| 1,000,000 | 6.635 | 0.694 | 0.144 |
| 2,000,000 | 12.802 | 0.904 | 0.248 |

Plot: `results/scaling_plot.png`. The y-axis is logarithmic because DuckDB and
mrjob differ by more than two orders of magnitude; on a linear axis the Spark
and DuckDB curves collapse onto the bottom of the chart and their shapes
cannot be read.

## Shape of the curves

Runtime for each system is close to a straight line of the form
*fixed overhead + per-row cost × rows*. Fitting the endpoints gives:

| System | Fixed overhead | Cost per row | Fixed cost as % of the 100K run |
|---|---|---|---|
| MapReduce | ~0.57 s | ~6.12 µs | 48% |
| Spark | ~0.40 s | ~0.25 µs | 94% |
| DuckDB | ~0.09 s | ~0.08 µs | 91% |

MapReduce grows almost perfectly linearly, because its cost is dominated by
work that happens once per row: an interpreted Python function call, a CSV
parse and a float conversion, two million times over. Its per-row cost is
roughly 75 times DuckDB's, and that ratio does not improve with scale.

Spark's curve is the flattest in relative terms — twenty times the data for
only 2.1 times the runtime — because at 100K rows about 94% of what I
measured is fixed cost rather than data processing. Planning the query,
scheduling tasks and coordinating partitions costs roughly the same whether
there are 100,000 rows or 2,000,000. This is the clearest evidence in the
whole assignment that a benchmark taken at one input size can be misleading:
at 100K rows I was mostly measuring Spark's overhead, not its throughput.

DuckDB is fastest at every size, and its fixed cost is also 91% of its 100K
runtime — the same pattern as Spark, but with a fixed cost roughly four times
smaller and a per-row cost roughly three times smaller.

Because DuckDB is lower on *both* terms, extrapolating these two lines shows
no crossover: the absolute gap between Spark and DuckDB widens with size even
though the ratio narrows from 4.5× at 100K to 3.6× at 2M. Growing the data on
this machine would not let Spark catch up. What would change the answer is
data that no longer fits on one machine, which is the situation Spark's design
actually targets and which this experiment cannot reach.

## Prediction outcome

My scenario 4 prediction was that DuckDB would be fastest at every size, that
mrjob would have the steepest curve because its cost is one interpreted
function call per row, that Spark's curve would look flattest in relative
terms because its fixed planning cost dominates at 100K, and that there would
be no crossover inside this range.

All four parts held. DuckDB was fastest at all four sizes, mrjob's per-row
cost was 75 times DuckDB's, Spark grew by only 2.1× across a 20× increase in
data, and no curves crossed.

I should record that this prediction was made after Task 1 had already been
measured at 1M rows, so I knew the ordering at one point on the curve before
predicting the shape. The prediction about *shape* was still genuinely open.

## Interpretation limits

These mrjob timings measure a local, single-process inline runner. They do not
predict Hadoop-cluster performance and must not be read that way. What the
mrjob curve shows is the per-row cost of executing the MapReduce programming
model in Python, not the value of MapReduce, which lies in fault-tolerant
batch processing across many machines on data far larger than this.

The inline runner also avoids per-step process startup, so mrjob's fixed
overhead here is lower than the local runner's would be. That works in
mrjob's favour at 100K rows, where fixed cost is a large share of the total.
It does not affect the 2M result, where per-row cost dominates.
