"""Combine every per-component benchmark CSV into a single throughput plot.

Reads all CSVs in logs/benchmark-results/. Files whose first column is a worker
count (n_alert_workers / n_enrichment_workers / n_filter_workers) are drawn as
throughput curves against the number of workers; files that vary batch_size
(the kafka consumer) are drawn as a single horizontal constant.

The figure and a merged CSV are written to logs/benchmark-combined/.
"""

import glob
import os
import re

import matplotlib.pyplot as plt
import pandas as pd

RESULTS_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "logs", "benchmark-results")
)
OUT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "logs", "benchmark-combined")
)
os.makedirs(OUT_DIR, exist_ok=True)
OUT_BASE = os.path.join(OUT_DIR, "combined-benchmarks")

# Hardcoded reference: the Argus alert rate.
ARGUS_RATE = 2800

# Legend labels keyed by CSV filename.
LABELS = {
    "alert-worker-only-na=[1-28].csv": "alert worker",
    "enrichment-only-cpu-ne=[1-28].csv": "enrichment worker (cpu)",
    "enrichment-only-gpu=1-ne=[1-28].csv": "enrichment worker (gpu=1)",
    "enrichment-only-gpu=2-ne=[1-28].csv": "enrichment worker (gpu=2)",
    "filter-only-nf=[1-28].csv": "filter worker",
    "kafka-consumer-only-batch_size=50.csv": "kafka consumer",
}

# Explicit curve colors keyed by CSV filename (others are auto-assigned).
# Red is reserved for the argus rate reference line, so pin filter off the
# default cycle (which would otherwise land it on red).
COLORS = {
    "filter-only-nf=[1-28].csv": "tab:purple",
}

# The full-pipeline benchmark (gpu, varying enrichment workers, fixed alert and
# filter workers). Its label is built from the worker counts in the filename so
# it stays correct if those change. It is highlighted to stand out.
FULL_PIPELINE_PATTERN = re.compile(r"gpu-na=(?P<na>\d+)-ne=\[[^\]]*\]-nf=(?P<nf>\d+)")

# Shades of red cycled through for the full-pipeline curves: each is the headline
# result, so they stay red, but when several are plotted at once a slightly
# different shade keeps them apart.
FULL_PIPELINE_COLORS = [
    "#ef4444",  # lighter soft red
    "#7f1d1d",  # deep dark red
    "#b91c1c",  # strong muted red
    "#dc2626",  # balanced red
]

def make_label(basename):
    """Return the legend label for a CSV, building the full-pipeline one live."""
    if basename in LABELS:
        return LABELS[basename]
    match = FULL_PIPELINE_PATTERN.search(basename)
    if match:
        return (
            f"full pipeline (gpu, {match.group('na')} alert workers, "
            f"{match.group('nf')} filter workers)"
        )
    return basename


# Curves drawn greyed out: the CPU enrichment worker is not used in production
# (prod runs on GPU). It is kept on the plot only to show the difference
# against the production setup.
GREYED_OUT = {
    "enrichment-only-cpu-ne=[1-28].csv",
}


fig, ax = plt.subplots(figsize=(11, 7))
combined_rows = []
reference_labels = []  # constant lines to list first in the legend
full_pipeline_labels = []  # listed last, just before the argus rate

for path in sorted(glob.glob(os.path.join(RESULTS_DIR, "*.csv"))):
    basename = os.path.basename(path)
    frame = pd.read_csv(path)
    x_column = frame.columns[0]
    label = make_label(basename)

    if x_column == "batch_size":
        # A constant rather than a curve (kafka consumer at a fixed batch size).
        value = frame["throughput_alerts_per_s"].iloc[0]
        ax.axhline(value, color="teal", linestyle="--", linewidth=1.5, label=label)
        reference_labels.append(label)
    else:
        frame = frame.sort_values(x_column)
        plot_kwargs = {"marker": "o", "markersize": 4}
        if basename in GREYED_OUT:
            # Configurations not used in production: faded, as a comparison only.
            plot_kwargs.update(color="grey", alpha=0.5, linestyle="--")
        elif FULL_PIPELINE_PATTERN.search(basename):
            # The full pipeline is the headline result: make it stand out most.
            # Cycle the shade so several full-pipeline curves stay distinct.
            color = FULL_PIPELINE_COLORS[len(full_pipeline_labels) % len(FULL_PIPELINE_COLORS)]
            full_pipeline_labels.append(label)
            plot_kwargs.update(color=color, linewidth=3, markersize=6, zorder=10)
        elif basename in COLORS:
            plot_kwargs["color"] = COLORS[basename]
        ax.plot(
            frame[x_column],
            frame["throughput_alerts_per_s"],
            label=label,
            **plot_kwargs,
        )

    for _, row in frame.iterrows():
        combined_rows.append(
            {
                "series": label,
                "x": row[x_column],
                "wall_time_s": row["wall_time_s"],
                "throughput_alerts_per_s": row["throughput_alerts_per_s"],
            }
        )

argus_label = f"argus rate ({ARGUS_RATE} alerts/s)"
ax.axhline(ARGUS_RATE, color="red", linestyle="-.", linewidth=2, label=argus_label)

ax.set_xlabel("number of workers")
ax.set_ylabel("throughput (alerts / s)")
ax.set_title("Benchmark throughput")
ax.grid(True, alpha=0.3)

# Legend order: reference lines first, then curves, then full pipeline, with the
# argus rate last.
handles, labels = ax.get_legend_handles_labels()
label_to_handle = dict(zip(labels, handles))
tail = full_pipeline_labels + [argus_label]
ordered = reference_labels + [name for name in labels if name not in reference_labels + tail]
ordered += tail
ax.legend([label_to_handle[name] for name in ordered], ordered)

fig.tight_layout()
fig.savefig(OUT_BASE + ".png", dpi=150)
pd.DataFrame(combined_rows).to_csv(OUT_BASE + ".csv", index=False)

print(f"Wrote {OUT_BASE}.png")
print(f"Wrote {OUT_BASE}.csv")
