# Task 5 — repeated work on the same data

**Input:** `data/taxi_1m.parquet`, same aggregation as Task 1.
Five repeated runs per condition, no warm-up, nothing restarted between runs
within a condition.

There is deliberately no warm-up here. Section 5 asks for one "unless the
task explicitly asks for five repeated runs", and this task does. Whether the
first run is slower than the later ones is the observation being made, so
discarding it would hide the effect being studied.

## Results (seconds)

| Run | Spark uncached | Spark cached | DuckDB |
|---|---|---|---|
| 1 | 2.271 | 0.443 | 0.026 |
| 2 | 0.388 | 0.305 | 0.005 |
| 3 | 0.337 | 0.253 | 0.004 |
| 4 | 0.304 | 0.278 | 0.004 |
| 5 | 0.296 | 0.243 | 0.004 |
| **median** | **0.337** | **0.278** | **0.004** |

Spark cache build (`count()`, reported separately, run_number 0): **1.341 s**

## Do later runs get faster?

Yes, in every condition — including DuckDB, which has no user-level cache at
all, and including Spark's cached condition, where the cache was already
materialised before run 1.

The effect is large and it is concentrated almost entirely in the first run:

- Spark uncached: run 1 is 7.7× slower than run 5
- Spark cached: run 1 is 1.8× slower than run 5
- DuckDB: run 1 is 6.4× slower than run 5

After run 2, all three conditions are essentially flat. This tells me the
first-run penalty is a one-off cost, not something that decays gradually.

## Which mechanism is responsible

Three different things are usually described as "caching", and this
experiment separates them.

**Operating-system page cache.** The first query has to read
`taxi_1m.parquet` from the NVMe SSD. After that the file is in RAM and no
condition pays real disk cost again. This affects every system equally and
explains why DuckDB — which does no application-level caching of the data —
still shows a 6.4× first-run penalty.

**JVM startup and JIT compilation.** Spark's uncached run 1 at 2.271 s is far
more than the page cache alone can explain, since DuckDB read the same file
in 0.026 s. Spark's first query triggers lazy initialisation of the Parquet
reader, schema resolution, and JIT compilation of the whole-stage codegen
Java. Once the JVM has compiled that code it is reused, which is why runs
2–5 drop to around 0.3 s.

**Spark's explicit `cache()`.** This stores an already-decoded copy of the
DataFrame in executor memory so the Parquet file does not have to be re-read
and re-decoded. It is the only one of the three that a programmer chooses.

## How much did `cache()` actually help

Less than I expected: the median fell from 0.337 s to 0.278 s, a saving of
0.059 s per run, about 17%.

The cache cost 1.341 s to build, so it pays for itself after **22.9 runs**.
Under 23 repetitions of this query, caching is a net loss.

The reason the saving is small is that `cache()` only removes the cost of
re-reading and decoding Parquet — and Parquet is already compact, columnar
and, after run 1, sitting in the OS page cache. What it does not remove is
Spark's per-query planning, task scheduling and shuffle for the aggregation,
which happen identically whether the input came from a file or from memory.
Caching removed the cheap part of the work and left the expensive part.

**An honest limitation of my method.** The cached condition ran in the same
SparkSession, immediately after the uncached one. By then the JVM was warm
and the file was in the page cache, which is why cached run 1 (0.443 s) looks
so much better than uncached run 1 (2.271 s). That difference is mostly JIT
warm-up, not `cache()`. Comparing the medians of runs 2–5 is the fair
comparison, and that is what the 17% figure is based on. To isolate `cache()`
properly I would need a fresh session for each condition.

## DuckDB

DuckDB's steady-state runtime is 0.004 s, roughly 70× faster than Spark
cached and 84× faster than Spark uncached. It reaches this without any
explicit caching: the OS holds the file, DuckDB keeps the Parquet metadata,
and the query itself reads only two columns of a compressed columnar file.

Worth comparing across tasks: the same aggregation over the same million rows
took 0.148 s from CSV in Task 1 and 0.004 s from Parquet here — a 37× gap
from file format and warm cache combined, with no change to the engine or the
query.

## When is paying to cache justified

**Justified:** when the same parsed dataset will be reused many times and the
per-run saving is real. Concretely, more than 23 repetitions of this query on
this data would repay the 1.341 s build. The stronger case is an iterative
workload — machine-learning training passing over the same features hundreds
of times, or an interactive session where an analyst explores one dataset
repeatedly. The build cost is paid once and amortised over many reads, and if
the input is expensive to produce (a long upstream join or a slow parse) the
per-run saving is much larger than the 17% I measured here.

**Not justified:** when the data is read a handful of times, as in this
experiment. Caching also consumes executor memory that other stages could
use, and if the dataset does not fit, Spark spills to disk and the cache can
be slower than re-reading a well-compressed Parquet file. It is also pointless
when the underlying source is already fast — caching a Parquet file that the
OS is holding in RAM mostly duplicates data that is already in memory.

## Prediction outcome

I predicted DuckDB for this scenario, reasoning that the page cache would help
everyone after the first read, that Spark's `cache()` would help it most
because parsing is its biggest cost but would have to repay a build cost
first, and that DuckDB would stay ahead.

That was correct. My "what would prove me wrong" test was whether Spark cached
would beat DuckDB, which would have meant avoiding repeated parsing mattered
more than avoiding engine overhead. It did not come close — Spark cached is
still around 70× slower.

The part I got only partly right is the size of the effect. I expected
`cache()` to help Spark substantially, since parsing was its dominant cost in
Task 1. It helped by 17%, because the input here is Parquet rather than CSV,
and parsing Parquet was never the expensive part.
