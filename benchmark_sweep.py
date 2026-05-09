"""Sweep over the number of workers and plot wall time and throughput."""

import argparse
import os
import subprocess
import sys

import matplotlib.pyplot as plt

WORKER_KINDS = ("alert", "enrichment", "filter")
EXPECTED_ALERTS = 29142  # must match _run.sh

parser = argparse.ArgumentParser(
    description="Sweep over the number of workers and plot wall time and throughput"
)
parser.add_argument(
    "--vary",
    choices=WORKER_KINDS,
    default="enrichment",
    help="Which worker type to sweep over",
)
parser.add_argument("--min", type=int, default=1)
parser.add_argument("--max", type=int, default=10)
parser.add_argument("--n-alert-workers", type=int, default=3)
parser.add_argument("--n-enrichment-workers", type=int, default=4)
parser.add_argument("--n-filter-workers", type=int, default=2)
parser.add_argument("--boom-repo-dir", default=".")
parser.add_argument("--timeout", type=int, default=600)
parser.add_argument("--out", default=None, help="Output plot path")
parser.add_argument(
    "--from-logs",
    action="store_true",
    help="Skip running; rebuild plot/CSV from existing wall_time.txt files",
)
args = parser.parse_args()

out_path = args.out or f"logs/worker_sweep_{args.vary}.png"
run_py = os.path.join(args.boom_repo_dir, "tests", "throughput", "run.py")

results: list[tuple[int, float, float]] = []
for n in range(args.min, args.max + 1):
    counts = {
        "alert": args.n_alert_workers,
        "enrichment": args.n_enrichment_workers,
        "filter": args.n_filter_workers,
        args.vary: n
    }

    wall_time_path = (
        f"logs/boom-na={counts['alert']}"
        f"-ne={counts['enrichment']}"
        f"-nf={counts['filter']}/wall_time.txt"
    )
    if args.from_logs:
        if not os.path.exists(wall_time_path):
            print(f"  -> no log for {args.vary}={n}, skipping", file=sys.stderr)
            continue
    else:
        print(f"\n=== Running with {args.vary}_workers={n} ===", flush=True)
        cmd = [
            "uv",
            "run",
            run_py,
            "--n-alert-workers", str(counts["alert"]),
            "--n-enrichment-workers", str(counts["enrichment"]),
            "--n-filter-workers", str(counts["filter"]),
            "--boom-repo-dir", args.boom_repo_dir,
            "--timeout", str(args.timeout),
        ]
        rc = subprocess.run(cmd).returncode
        if rc != 0 or not os.path.exists(wall_time_path):
            print(f"  -> run failed (rc={rc}), skipping", file=sys.stderr)
            continue
    with open(wall_time_path) as f:
        wall_time_s = float(f.read().strip())
    throughput = EXPECTED_ALERTS / wall_time_s
    results.append((n, wall_time_s, throughput))
    print(f"  {args.vary}={n} -> {wall_time_s:.1f} s ({throughput:.0f} alerts/s)")

if not results:
    sys.exit("No successful runs")

xs = [r[0] for r in results]
walls = [r[1] for r in results]
tputs = [r[2] for r in results]

fixed = {k: v for k, v in {
    "alert": args.n_alert_workers,
    "enrichment": args.n_enrichment_workers,
    "filter": args.n_filter_workers,
}.items() if k != args.vary}
fixed_str = ", ".join(f"{k}={v}" for k, v in fixed.items())

os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
ax1.plot(xs, walls, marker="o")
ax1.set_xlabel(f"Number of {args.vary} workers")
ax1.set_ylabel("Wall time (s)")
ax1.set_title(f"Wall time vs. {args.vary} workers")
ax1.grid(True, alpha=0.3)

ax2.plot(xs, tputs, marker="o", color="C2")
ax2.set_xlabel(f"Number of {args.vary} workers")
ax2.set_ylabel("Throughput (alerts/s)")
ax2.set_title(f"Throughput vs. {args.vary} workers")
ax2.grid(True, alpha=0.3)

fig.suptitle(f"BOOM benchmark — varying {args.vary} workers ({fixed_str})")
fig.tight_layout()
fig.savefig(out_path, dpi=120)
print(f"\nPlot saved to {out_path}")

csv_path = os.path.splitext(out_path)[0] + ".csv"
with open(csv_path, "w") as f:
    f.write(f"n_{args.vary}_workers,wall_time_s,throughput_alerts_per_s\n")
    for n, t, tput in results:
        f.write(f"{n},{t:.2f},{tput:.2f}\n")
print(f"CSV saved to {csv_path}")
