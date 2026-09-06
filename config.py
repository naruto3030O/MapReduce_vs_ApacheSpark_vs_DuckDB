"""
Shared machine configuration and results/timings.csv writer.

Every timed script imports from here so that the machine configuration is
written identically into every row of timings.csv, as required by section 14
of the assignment.


"""

import csv
import platform
from pathlib import Path


STUDENT_ID = "2023481"

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
RESULTS_DIR = PROJECT_ROOT / "results"
TIMINGS_CSV = RESULTS_DIR / "timings.csv"

# ---------------------------------------------------------------------------
# Machine configuration (section 4). Recorded once, repeated in every row.
# ---------------------------------------------------------------------------
MACHINE = {
    "os": "Windows 11 25H2 (OS Build 26200.9278)",
    "cpu_model": "AMD Ryzen 7 8845HS w/ Radeon 780M Graphics",
    "cpu_base_ghz": "3.80",
    "cpu_physical_cores": "8",
    "cpu_logical_processors": "16",
    "ram_gb": "16",
    "storage_size_gb": "954",
    "storage_type": "NVMe SSD",
    "gpu_model": "NVIDIA GeForce RTX 4050 Laptop GPU",
    "gpu_clock_mhz": "3105",
    "gpu_memory_gb": "6.0",
    "python_version": platform.python_version(),
}

# Exact column order from section 14. Do not reorder or remove columns.
TIMINGS_HEADER = [
    "student_id", "system", "task", "condition", "dataset_rows",
    "input_format", "input_mb", "run_number", "time_seconds", "status",
    "os", "cpu_model", "cpu_base_ghz", "cpu_physical_cores",
    "cpu_logical_processors", "ram_gb", "storage_size_gb", "storage_type",
    "gpu_model", "gpu_clock_mhz", "gpu_memory_gb", "python_version",
    "tool_version",
]


def file_size_mb(path):
    """Actual on-disk size in MB, as required for the input_mb column."""
    return round(Path(path).stat().st_size / (1024 * 1024), 1)


def append_timing(system, task, condition, dataset_rows, input_format,
                  input_path, run_number, time_seconds, status, tool_version):
    """Append one measured run to results/timings.csv, creating it if needed."""
    RESULTS_DIR.mkdir(exist_ok=True)
    new_file = not TIMINGS_CSV.exists()

    row = {
        "student_id": STUDENT_ID,
        "system": system,
        "task": task,
        "condition": condition,
        "dataset_rows": dataset_rows,
        "input_format": input_format,
        "input_mb": file_size_mb(input_path),
        "run_number": run_number,
        "time_seconds": round(time_seconds, 3),
        "status": status,
        "tool_version": tool_version,
        **MACHINE,
    }

    with open(TIMINGS_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=TIMINGS_HEADER)
        if new_file:
            writer.writeheader()
        writer.writerow(row)

    print(f"  recorded: {system} {task} run {run_number} = "
          f"{time_seconds:.3f}s ({status})")


def save_result(system, task, rows):
    """
    Save a task result as results/<task>_<system>.csv so the three
    implementations can be compared for numerical agreement.

    rows: iterable of (PULocationID, trip_count, avg_fare)
    """
    RESULTS_DIR.mkdir(exist_ok=True)
    out = RESULTS_DIR / f"{task}_{system}.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["PULocationID", "trip_count", "avg_fare"])
        for pu, count, avg in sorted(rows, key=lambda r: int(r[0])):
            writer.writerow([int(pu), int(count), f"{float(avg):.6f}"])
    print(f"  result written: {out.name} ({len(rows)} groups)")
