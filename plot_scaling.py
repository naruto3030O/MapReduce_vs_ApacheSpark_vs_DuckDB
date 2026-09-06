"""
# AI-ASSISTED: A6
Task 4 - scaling plot.

Reads results/timings.csv, takes the median of the three recorded runs for
each system/dataset pair, and writes results/scaling_plot.png with all three
systems on the same chart.

Timeouts are plotted at 900 seconds and marked clearly as timeouts rather
than shown as completed runtimes, as required by section 10.

Run:
    python plot_scaling.py
"""

import csv
from collections import defaultdict
from pathlib import Path
from statistics import median

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

RESULTS_DIR = Path(__file__).resolve().parent / "results"
TIMINGS_CSV = RESULTS_DIR / "timings.csv"
PLOT_FILE = RESULTS_DIR / "scaling_plot.png"

SYSTEMS = ["mapreduce", "spark", "duckdb"]
STYLE = {
    "mapreduce": ("tab:red", "o", "MapReduce (mrjob, inline runner)"),
    "spark": ("tab:blue", "s", "Spark (local mode)"),
    "duckdb": ("tab:green", "^", "DuckDB"),
}


def load_task4():
    """Return {system: {rows: (median_seconds, timed_out)}} for task4 rows."""
    runs = defaultdict(lambda: defaultdict(list))
    timeouts = defaultdict(set)

    with open(TIMINGS_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["task"] != "task4":
                continue
            system = row["system"]
            rows = int(row["dataset_rows"])
            if row["status"] == "TIMEOUT_900":
                timeouts[system].add(rows)
            runs[system][rows].append(float(row["time_seconds"]))

    result = {}
    for system in runs:
        result[system] = {
            rows: (median(times), rows in timeouts[system])
            for rows, times in runs[system].items()
        }
    return result


def main():
    data = load_task4()
    if not data:
        print("No task4 rows found in results/timings.csv - run run_task4.py first")
        return

    fig, ax = plt.subplots(figsize=(8, 5.5))

    for system in SYSTEMS:
        if system not in data:
            continue
        colour, marker, label = STYLE[system]
        sizes = sorted(data[system])
        medians = [data[system][s][0] for s in sizes]

        ax.plot(sizes, medians, marker=marker, color=colour, label=label,
                linewidth=1.8, markersize=7)

        for size in sizes:
            value, timed_out = data[system][size]
            if timed_out:
                ax.annotate("TIMEOUT (900 s)", (size, value),
                            textcoords="offset points", xytext=(0, 12),
                            ha="center", fontsize=8, color=colour,
                            fontweight="bold")
                ax.scatter([size], [value], marker="x", s=140, color=colour,
                           zorder=5)

    ax.set_xlabel("Dataset size (rows)")
    ax.set_ylabel("Median execution time (seconds, log scale)")
    ax.set_title("Task 4 - aggregation scaling on CSV input\n"
                 "trip count and average fare by PULocationID",
                 fontsize=11)
    ax.set_yscale("log")
    ax.set_xticks([100_000, 500_000, 1_000_000, 2_000_000])
    ax.set_xticklabels(["100K", "500K", "1M", "2M"])
    ax.grid(True, which="both", alpha=0.3, linestyle=":")
    ax.legend(frameon=False)

    fig.tight_layout()
    fig.savefig(PLOT_FILE, dpi=200)
    print(f"written: {PLOT_FILE}")

    print("\nmedian seconds by system and size:")
    header = f"  {'rows':>10}" + "".join(f"{s:>13}" for s in SYSTEMS)
    print(header)
    for size in [100_000, 500_000, 1_000_000, 2_000_000]:
        line = f"  {size:>10,}"
        for system in SYSTEMS:
            if system in data and size in data[system]:
                value, timed_out = data[system][size]
                cell = "TIMEOUT" if timed_out else f"{value:.3f}"
            else:
                cell = "-"
            line += f"{cell:>13}"
        print(line)


if __name__ == "__main__":
    main()
