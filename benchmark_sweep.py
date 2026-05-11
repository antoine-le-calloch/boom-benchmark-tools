"""Sweep over the number of workers and plot wall time and throughput."""

import argparse
import datetime
import os
import subprocess
import sys

import matplotlib.pyplot as plt

WORKER_KINDS = ("alert", "enrichment", "filter")
SWEEP_KINDS = WORKER_KINDS + ("gpu",)
EXPECTED_ALERTS = 29142  # must match _run.sh

parser = argparse.ArgumentParser(
    description="Sweep over the number of workers and plot wall time and throughput"
)
parser.add_argument(
    "--vary",
    choices=SWEEP_KINDS,
    default="enrichment",
    help="What to sweep over: a worker count, or 'gpu' to vary the number of GPU "
         "devices (sets BOOM_GPU__DEVICE_IDS=0,1,...,n-1 per iteration)",
)
parser.add_argument("--min", type=int, default=1)
parser.add_argument("--max", type=int, default=10)
parser.add_argument("--n-alert-workers", type=int, default=7)
parser.add_argument("--n-enrichment-workers", type=int, default=23)
parser.add_argument("--n-filter-workers", type=int, default=6)
parser.add_argument("--boom-repo-dir", default=".")
parser.add_argument("--timeout", type=int, default=600)
parser.add_argument("--out", default=None, help="Output plot path")
parser.add_argument(
    "--from-logs",
    action="store_true",
    help="Skip running; rebuild plot/CSV from existing wall_time.txt files",
)
parser.add_argument(
    "--gpu",
    action="store_true",
    help="Enable GPU benchmark mode (sets BOOM_GPU__ENABLED=true for sub-runs)",
)
parser.add_argument(
    "--gpu-device-ids",
    default=None,
    help="Comma-separated GPU device IDs to use (e.g. 0,1,2,3). Implies --gpu.",
)
args = parser.parse_args()

if args.vary == "gpu" and args.gpu_device_ids:
    sys.exit("--gpu-device-ids is incompatible with --vary gpu (the sweep generates them)")

gpu_mode = args.vary == "gpu" or args.gpu or bool(args.gpu_device_ids)
if gpu_mode:
    print("GPU benchmark mode enabled. Setting BOOM_GPU__ENABLED=true.")
    os.environ["BOOM_GPU__ENABLED"] = "true"
    if args.gpu_device_ids:
        n_devices = len(args.gpu_device_ids.split(","))
        print(f"Using GPU device IDs: {args.gpu_device_ids} (count: {n_devices})")
        os.environ["BOOM_GPU__DEVICE_IDS"] = args.gpu_device_ids
else:
    print("GPU benchmark mode disabled. To enable, pass --gpu.")
    os.environ["BOOM_GPU__ENABLED"] = "false"


timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
# `gpu-` prefix marks worker sweeps run in GPU mode (so they don't collide
# with CPU-mode sweeps that share the same worker ranges). `--vary gpu`
# already encodes "gpu" in the suffix, so we don't double-mark it.
gpu_prefix = "gpu-" if gpu_mode and args.vary != "gpu" else ""
gpu_suffix = f"-gpu=[{args.min}-{args.max}]" if args.vary == "gpu" else ""
out_path = args.out or (f"logs/"
                        f"{gpu_prefix}"
                        f"na={args.n_alert_workers if args.vary != 'alert' else f'[{args.min}-{args.max}]'}-"
                        f"ne={args.n_enrichment_workers if args.vary != 'enrichment' else f'[{args.min}-{args.max}]'}-"
                        f"nf={args.n_filter_workers if args.vary != 'filter' else f'[{args.min}-{args.max}]'}"
                        f"{gpu_suffix}-"
                        f"{timestamp}")
plot_path = out_path + ".png"
csv_path = out_path + ".csv"
run_py = os.path.join(args.boom_repo_dir, "tests", "throughput", "run.py")

results: list[tuple[int, float, float]] = []
for n in range(args.min, args.max + 1):
    if n != 5 and n != 10 and n != 15:
        continue
    counts = {
        "alert": args.n_alert_workers,
        "enrichment": args.n_enrichment_workers,
        "filter": args.n_filter_workers,
    }
    if args.vary in WORKER_KINDS:
        counts[args.vary] = n
        label = f"{args.vary}_workers={n}"
    else:
        device_ids = ",".join(str(i) for i in range(n))
        os.environ["BOOM_GPU__DEVICE_IDS"] = device_ids
        label = f"n_gpus={n} (device_ids={device_ids})"

    wall_time_path = (
        f"logs/boom-na={counts['alert']}"
        f"-ne={counts['enrichment']}"
        f"-nf={counts['filter']}/wall_time.txt"
    )
    if args.from_logs:
        if args.vary == "gpu":
            sys.exit("--from-logs is not supported with --vary gpu (all iterations "
                     "share the same wall_time.txt path)")
        if not os.path.exists(wall_time_path):
            print(f"  -> no log for {args.vary}={n}, skipping", file=sys.stderr)
            continue
    else:
        print(f"\n=== Running with {label} ===", flush=True)
        cmd = [
            sys.executable,
            run_py,
            "--apptainer",
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

vary_label = "GPUs" if args.vary == "gpu" else f"{args.vary} workers"
csv_col_name = "n_gpus" if args.vary == "gpu" else f"n_{args.vary}_workers"

os.makedirs(os.path.dirname(plot_path) or ".", exist_ok=True)
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
ax1.plot(xs, walls, marker="o")
ax1.set_xlabel(f"Number of {vary_label}")
ax1.set_ylabel("Wall time (s)")
ax1.set_title(f"Wall time vs. {vary_label}")
ax1.grid(True, alpha=0.3)

ax2.plot(xs, tputs, marker="o", color="C2")
ax2.set_xlabel(f"Number of {vary_label}")
ax2.set_ylabel("Throughput (alerts/s)")
ax2.set_title(f"Throughput vs. {vary_label}")
ax2.grid(True, alpha=0.3)

gpu_tag = " GPU" if gpu_mode and args.vary != "gpu" else ""
fig.suptitle(f"BOOM{gpu_tag} benchmark — varying {vary_label} ({fixed_str})")
fig.tight_layout()
fig.savefig(plot_path, dpi=120)
print(f"\nPlot saved to {plot_path}")

with open(csv_path, "w") as f:
    f.write(f"{csv_col_name},wall_time_s,throughput_alerts_per_s\n")
    for n, t, tput in results:
        f.write(f"{n},{t:.2f},{tput:.2f}\n")
print(f"CSV saved to {csv_path}")
