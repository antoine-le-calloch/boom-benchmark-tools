"""Sweep over the number of workers and plot wall time and throughput."""

import argparse
import datetime
import os
import subprocess
import sys

import matplotlib.pyplot as plt

WORKER_KINDS = ("alert", "enrichment", "filter")
SWEEP_KINDS = WORKER_KINDS + ("gpu", "process", "batch")
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
parser.add_argument("--step", type=int, default=1)
parser.add_argument("--n-alert-workers", type=int, default=20)
parser.add_argument("--n-enrichment-workers", type=int, default=28)
parser.add_argument("--n-filter-workers", type=int, default=18)
parser.add_argument(
    "--n-processes", type=int, default=1,
    help="kafka_consumer --processes value (only used with --kafka-consumer-only; "
         "swept when --vary process).",
)
parser.add_argument(
    "--batch-size", type=int, default=None,
    help="Override KAFKA_BATCH_SIZE (sent via BOOM_KAFKA_BATCH_SIZE env var). "
         "Only used with --kafka-consumer-only. Swept when --vary batch (in "
         "which case --min/--max are the range of batch sizes).",
)
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
parser.add_argument(
    "--warm",
    action="store_true",
    help=(
        "Warm-sweep mode: start MongoDB / Valkey / Kafka and run the producer "
        "exactly once before the loop, then reuse them across iterations via "
        "fast in-server state resets. Saves the per-iteration cost of "
        "mongorestore, NED import, Kafka topic creation, and re-producing the "
        "alerts."
    ),
)
parser.add_argument(
    "--init",
    action="store_true",
    help=(
        "Run only the warm setup (start services, mongorestore, NED, kafka "
        "producer) and exit. Use before a series of --bench-only invocations "
        "to amortize setup across many sweeps."
    ),
)
parser.add_argument(
    "--clean",
    action="store_true",
    help="Run only the warm teardown (stop all services) and exit.",
)
parser.add_argument(
    "--bench-only",
    action="store_true",
    help=(
        "Run only the bench loop, assuming services are already up from a "
        "prior --init invocation. Skips setup and teardown."
    ),
)
parser.add_argument(
    "--alert-only",
    action="store_true",
    help=(
        "Per-iteration: run_alert_only.py to measure alert-worker wall time "
        "(consumer first message -> LLEN ZTF_alerts_enrichment_queue == "
        "EXPECTED_ALERTS). Forces n_enrichment_workers=0 and n_filter_workers=0. "
        "Only --vary alert is meaningful here (the alert worker does no GPU "
        "inference)."
    ),
)
parser.add_argument(
    "--enrichment-only",
    action="store_true",
    help=(
        "Per-iteration: run_alert_only.py to fill the enrichment queue, then "
        "run_enrichment_only.py to measure the enrichment-worker wall time "
        "(starting enrichment worker -> LLEN ZTF_alerts_filter_queue == "
        "EXPECTED_ALERTS). Sweeping makes sense over --vary enrichment or gpu."
    ),
)
parser.add_argument(
    "--filter-only",
    action="store_true",
    help=(
        "Per-iteration: chain run_alert_only.py + run_enrichment_only.py to "
        "fill the filter queue, then run_filter_only.py to measure the "
        "filter-worker wall time (starting filter worker -> every alert "
        "processed by every filter, counted from scheduler 'passed filter' log "
        "lines / N_FILTERS). Only --vary filter is meaningful (no GPU inference)."
    ),
)
parser.add_argument(
    "--kafka-consumer-only",
    action="store_true",
    help=(
        "Per-iteration: run_kafka_consumer_only.py to measure the kafka_consumer "
        "wall time (consumer first message -> LLEN ZTF_alerts_packets_queue == "
        "EXPECTED_ALERTS). No scheduler runs; all worker counts forced to 0. "
        "Use --vary process --min M --max N to sweep the kafka_consumer "
        "--processes value across [M, N]."
    ),
)
parser.add_argument(
    "--alert-worker-only",
    action="store_true",
    help=(
        "Per-iteration: run_alert_worker_only.py to measure pure alert-worker "
        "wall time (last 'alert worker ready' log -> LLEN "
        "ZTF_alerts_enrichment_queue == EXPECTED_ALERTS). The packets queue is "
        "pre-filled at the start of each iteration by an --exit-on-eof "
        "kafka_consumer run, so the measurement excludes the kafka -> valkey "
        "transfer cost. Only --vary alert is meaningful."
    ),
)
args = parser.parse_args()

stage_only_flags = [
    args.alert_only,
    args.enrichment_only,
    args.filter_only,
    args.kafka_consumer_only,
    args.alert_worker_only,
]
if sum(stage_only_flags) > 1:
    sys.exit(
        "--alert-only / --enrichment-only / --filter-only / "
        "--kafka-consumer-only / --alert-worker-only are mutually exclusive"
    )

phase_only_flags = [args.init, args.clean, args.bench_only]
if sum(phase_only_flags) > 1:
    sys.exit("--init / --clean / --bench-only are mutually exclusive")
if any(phase_only_flags) and args.warm:
    sys.exit(
        "--init / --clean / --bench-only are incompatible with --warm "
        "(--warm is the all-in-one mode; the new flags split it into stages)"
    )
if any(phase_only_flags) and args.from_logs:
    sys.exit("--init / --clean / --bench-only are incompatible with --from-logs")

if args.vary == "gpu" and args.gpu_device_ids:
    sys.exit("--gpu-device-ids is incompatible with --vary gpu (the sweep generates them)")
if args.warm and args.from_logs:
    sys.exit("--warm and --from-logs are incompatible (warm mode actively runs the bench)")
if args.alert_only and args.vary != "alert":
    sys.exit(
        f"--alert-only is only meaningful with --vary alert (got --vary {args.vary}). "
        f"Enrichment/filter workers are forced to 0, and the alert worker does "
        f"no GPU inference, so sweeping enrichment/filter/gpu has no effect."
    )
if args.enrichment_only and args.vary not in ("enrichment", "gpu"):
    sys.exit(
        f"--enrichment-only is only meaningful with --vary enrichment or --vary gpu "
        f"(got --vary {args.vary}). Alert/filter workers are forced to 0."
    )
if args.filter_only and args.vary != "filter":
    sys.exit(
        f"--filter-only is only meaningful with --vary filter (got --vary {args.vary}). "
        f"Alert/enrichment workers are forced to 0, and the filter worker does "
        f"no GPU inference."
    )
if args.alert_worker_only and args.vary != "alert":
    sys.exit(
        f"--alert-worker-only is only meaningful with --vary alert (got --vary {args.vary}). "
        f"Enrichment/filter workers are forced to 0, and the alert worker does no "
        f"GPU inference."
    )
if args.kafka_consumer_only and args.vary not in ("process", "batch"):
    sys.exit(
        f"--kafka-consumer-only is only meaningful with --vary process or "
        f"--vary batch (got --vary {args.vary}). No scheduler runs, so "
        f"alert/enrichment/filter/gpu sweep dims have no effect here."
    )
if args.vary == "process" and not args.kafka_consumer_only:
    sys.exit(
        "--vary process is only meaningful with --kafka-consumer-only "
        "(it sweeps kafka_consumer --processes, which is not exposed in any other mode)."
    )
if args.vary == "batch" and not args.kafka_consumer_only:
    sys.exit(
        "--vary batch is only meaningful with --kafka-consumer-only "
        "(it sweeps BOOM_KAFKA_BATCH_SIZE, which only affects the consumer)."
    )

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
stage_only_prefix = ""
if args.alert_only:
    stage_only_prefix = "alert-only-"
elif args.enrichment_only:
    stage_only_prefix = "enrichment-only-"
elif args.filter_only:
    stage_only_prefix = "filter-only-"
elif args.kafka_consumer_only:
    stage_only_prefix = "kafka-consumer-only-"
elif args.alert_worker_only:
    stage_only_prefix = "alert-worker-only-"
if args.alert_only:
    workers_part = f"na=[{args.min}-{args.max}]"
elif args.enrichment_only:
    workers_part = (f"ne={args.n_enrichment_workers}" if args.vary == "gpu"
                    else f"ne=[{args.min}-{args.max}]")
elif args.filter_only:
    workers_part = f"nf=[{args.min}-{args.max}]"
elif args.alert_worker_only:
    workers_part = f"na=[{args.min}-{args.max}]"
elif args.kafka_consumer_only:
    if args.vary == "process":
        workers_part = f"np=[{args.min}-{args.max}]"
    else:  # "batch"
        workers_part = f"np={args.n_processes}-bs=[{args.min}-{args.max}]"
else:
    workers_part = (
        f"na={args.n_alert_workers if args.vary != 'alert' else f'[{args.min}-{args.max}]'}-"
        f"ne={args.n_enrichment_workers if args.vary != 'enrichment' else f'[{args.min}-{args.max}]'}-"
        f"nf={args.n_filter_workers if args.vary != 'filter' else f'[{args.min}-{args.max}]'}"
    )
out_path = args.out or (f"logs/benchmark-sweep/"
                        f"{stage_only_prefix}"
                        f"{gpu_prefix}"
                        f"{workers_part}"
                        f"{gpu_suffix}-"
                        f"{timestamp}")
plot_path = out_path + ".png"
csv_path = out_path + ".csv"
run_py = os.path.join(args.boom_repo_dir, "tests", "throughput", "run.py")
run_alert_only_py = os.path.join(
    args.boom_repo_dir, "tests", "throughput", "run_alert_only.py"
)
run_enrichment_only_py = os.path.join(
    args.boom_repo_dir, "tests", "throughput", "run_enrichment_only.py"
)
run_filter_only_py = os.path.join(
    args.boom_repo_dir, "tests", "throughput", "run_filter_only.py"
)
run_kafka_consumer_only_py = os.path.join(
    args.boom_repo_dir, "tests", "throughput", "run_kafka_consumer_only.py"
)
run_alert_worker_only_py = os.path.join(
    args.boom_repo_dir, "tests", "throughput", "run_alert_worker_only.py"
)


def run_phase(phase: str, counts: dict[str, int]) -> int:
    """Invoke run.py for a given phase and return its exit code.

    The worker counts are forwarded so that run.py can write the corresponding
    config.yaml, even for phases where they have no effect (setup/teardown
    write nothing benchmark-shaped; bench/full need them for BOOM startup).
    """
    cmd = [
        sys.executable,
        run_py,
        "--apptainer",
        "--n-alert-workers", str(counts["alert"]),
        "--n-enrichment-workers", str(counts["enrichment"]),
        "--n-filter-workers", str(counts["filter"]),
        "--boom-repo-dir", args.boom_repo_dir,
        "--timeout", str(args.timeout),
        "--phase", phase,
    ]
    return subprocess.run(cmd).returncode


def run_alert_only_bench(counts: dict[str, int]) -> int:
    """Invoke run_alert_only.py for a single bench iteration.

    Setup/teardown still go through run.py since the underlying services
    (mongo, valkey, kafka, boom apptainer instance) are identical; only the
    bench phase differs (config rewritten with n_enrichment=0, n_filter=0 and
    LLEN-based completion signal).
    """
    cmd = [
        sys.executable,
        run_alert_only_py,
        "--apptainer",
        "--n-alert-workers", str(counts["alert"]),
        "--boom-repo-dir", args.boom_repo_dir,
        "--timeout", str(args.timeout),
    ]
    return subprocess.run(cmd).returncode


def run_enrichment_only_bench(counts: dict[str, int]) -> int:
    """Invoke run_enrichment_only.py for a single bench iteration."""
    cmd = [
        sys.executable,
        run_enrichment_only_py,
        "--apptainer",
        "--n-enrichment-workers", str(counts["enrichment"]),
        "--boom-repo-dir", args.boom_repo_dir,
        "--timeout", str(args.timeout),
    ]
    return subprocess.run(cmd).returncode


def run_filter_only_bench(counts: dict[str, int]) -> int:
    """Invoke run_filter_only.py for a single bench iteration."""
    cmd = [
        sys.executable,
        run_filter_only_py,
        "--apptainer",
        "--n-filter-workers", str(counts["filter"]),
        "--boom-repo-dir", args.boom_repo_dir,
        "--timeout", str(args.timeout),
    ]
    return subprocess.run(cmd).returncode


def run_kafka_consumer_only_bench(n_processes: int,
                                  batch_size: int | None) -> int:
    """Invoke run_kafka_consumer_only.py for a single bench iteration.

    n_processes is forwarded as `kafka_consumer --processes N` and tags the
    logs_dir (boom-kafka-consumer-only-np=N). batch_size, when provided,
    sets BOOM_KAFKA_BATCH_SIZE in the consumer and adds `-bs=B` to the
    logs_dir tag.
    """
    cmd = [
        sys.executable,
        run_kafka_consumer_only_py,
        "--apptainer",
        "--n-processes", str(n_processes),
        "--boom-repo-dir", args.boom_repo_dir,
        "--timeout", str(args.timeout),
    ]
    if batch_size is not None:
        cmd += ["--batch-size", str(batch_size)]
    return subprocess.run(cmd).returncode


def run_alert_worker_only_bench(counts: dict[str, int]) -> int:
    """Invoke run_alert_worker_only.py for a single bench iteration."""
    cmd = [
        sys.executable,
        run_alert_worker_only_py,
        "--apptainer",
        "--n-alert-workers", str(counts["alert"]),
        "--boom-repo-dir", args.boom_repo_dir,
        "--timeout", str(args.timeout),
    ]
    return subprocess.run(cmd).returncode


def gpus_for_path(n: int) -> int:
    """Mirror the gpu=N component of the logs_dir that run.py writes to.

    run.py derives this from BOOM_GPU__ENABLED + BOOM_GPU__DEVICE_IDS; we
    replicate the same logic so that wall_time_path here matches the path
    where wall_time.txt is actually written.
    """
    if os.environ.get("BOOM_GPU__ENABLED", "false").lower() != "true":
        return 0
    if args.vary == "gpu":
        return n
    device_ids_env = os.environ.get("BOOM_GPU__DEVICE_IDS", "0")
    return len([d for d in device_ids_env.split(",") if d.strip()])

setup_counts = None
if args.init:
    # Mirror the warm-setup worker-count derivation: counts only matter for
    # config.yaml writes, which the bench phase overwrites anyway, but we
    # populate them so run.py has something coherent to serialize.
    setup_counts = {
        "alert": args.min if args.vary == "alert" else args.n_alert_workers,
        "enrichment": args.min if args.vary == "enrichment" else args.n_enrichment_workers,
        "filter": args.min if args.vary == "filter" else args.n_filter_workers,
    }
    if args.vary == "gpu":
        os.environ["BOOM_GPU__DEVICE_IDS"] = ",".join(str(i) for i in range(args.min))
    print("\n=== Init: starting services + running producer (one-time) ===", flush=True)
    init_rc = run_phase("setup", setup_counts)
    if init_rc != 0:
        sys.exit(f"Init failed (rc={init_rc})")
    print("\n=== Init complete; services left running. ===", flush=True)
    print("Run benchmark_sweep.py --bench-only ... to use them.")
    print("Run benchmark_sweep.py --clean to stop them.")
    sys.exit(0)

if args.clean:
    print("\n=== Clean: stopping all services ===", flush=True)
    clean_counts = {
        "alert": args.n_alert_workers,
        "enrichment": args.n_enrichment_workers,
        "filter": args.n_filter_workers,
    }
    sys.exit(run_phase("teardown", clean_counts))

if args.warm:
    # Warm setup uses the first iteration's worker counts; they only matter for
    # config.yaml, which is overwritten every bench iteration anyway. For a
    # --vary gpu sweep we also need to set BOOM_GPU__DEVICE_IDS up front so
    # the apptainer instance start picks up a sensible default; the bench loop
    # rebinds it per iteration before exec-ing the scheduler.
    setup_counts = {
        "alert": args.min if args.vary == "alert" else args.n_alert_workers,
        "enrichment": args.min if args.vary == "enrichment" else args.n_enrichment_workers,
        "filter": args.min if args.vary == "filter" else args.n_filter_workers,
    }
    if args.vary == "gpu":
        os.environ["BOOM_GPU__DEVICE_IDS"] = ",".join(str(i) for i in range(args.min))
    print("\n=== Warm setup: starting services + running producer (one-time) ===", flush=True)
    setup_rc = run_phase("setup", setup_counts)
    if setup_rc != 0:
        sys.exit(f"Warm setup failed (rc={setup_rc}); aborting sweep")

results: list[tuple[int, float, float]] = []
for n in range(args.min, args.max + 1, args.step):
    counts = {
        "alert": args.n_alert_workers,
        "enrichment": args.n_enrichment_workers,
        "filter": args.n_filter_workers,
    }
    if args.vary in WORKER_KINDS:
        counts[args.vary] = n
        label = f"{args.vary}_workers={n}"
    elif args.vary == "gpu":
        device_ids = ",".join(str(i) for i in range(n))
        os.environ["BOOM_GPU__DEVICE_IDS"] = device_ids
        label = f"n_gpus={n} (device_ids={device_ids})"
    elif args.vary == "process":
        label = f"n_processes={n}"
    else:  # "batch" -- BOOM_KAFKA_BATCH_SIZE
        label = f"batch_size={n}"

    if args.alert_only:
        # Alert + filter workers run no GPU inference, so run_alert_only.py /
        # run_filter_only.py drop the `-gpu=` segment from their logs_dir.
        # Only enrichment-only includes it, since enrichment is the only stage
        # that actually loads ONNX models on the GPU.
        wall_time_path = (
            f"logs/boom-alert-only-na={counts['alert']}"
            f"/alert_worker_wall_time.txt"
        )
    elif args.enrichment_only:
        wall_time_path = (
            f"logs/boom-enrichment-only-ne={counts['enrichment']}"
            f"-gpu={gpus_for_path(n)}/enrichment_worker_wall_time.txt"
        )
    elif args.filter_only:
        wall_time_path = (
            f"logs/boom-filter-only-nf={counts['filter']}"
            f"/filter_worker_wall_time.txt"
        )
    elif args.kafka_consumer_only:
        # With --vary process, n is the kafka_consumer --processes value and
        # batch_size is fixed (or default). With --vary batch, n is the batch
        # size and processes is fixed.
        if args.vary == "process":
            np_value, bs_value = n, args.batch_size
        else:  # "batch"
            np_value, bs_value = args.n_processes, n
        suffix = f"-bs={bs_value}" if bs_value is not None else ""
        wall_time_path = (
            f"logs/boom-kafka-consumer-only-np={np_value}{suffix}"
            f"/kafka_consumer_wall_time.txt"
        )
    elif args.alert_worker_only:
        wall_time_path = (
            f"logs/boom-alert-worker-only-na={counts['alert']}"
            f"/alert_worker_wall_time.txt"
        )
    else:
        wall_time_path = (
            f"logs/boom-na={counts['alert']}"
            f"-ne={counts['enrichment']}"
            f"-nf={counts['filter']}"
            f"-gpu={gpus_for_path(n)}/wall_time.txt"
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
        if (args.alert_only or args.enrichment_only or args.filter_only
                or args.kafka_consumer_only or args.alert_worker_only):
            # The "*_only" benchmarks rely on services that --warm (or a prior
            # --init invocation, reused via --bench-only) brings up exactly
            # once. Without either there is no setup phase and the downstream
            # shell scripts would refuse to run.
            if not (args.warm or args.bench_only):
                sys.exit(
                    "--alert-only / --enrichment-only / --filter-only / "
                    "--kafka-consumer-only / --alert-worker-only require "
                    "--warm or --bench-only (the *_only scripts assume services "
                    "are already running)."
                )
            # Each "*_only" stage assumes the prior stage's output is sitting
            # in the corresponding valkey queue. The first iteration of the
            # sweep starts from an empty state, and every subsequent iteration
            # has just drained the input queue of the target stage — so we
            # re-run every prerequisite stage at the start of each iteration
            # to refill the relevant queues. Yes, this is slow for sweeps that
            # only vary the last stage, but it is the only way to keep the
            # measurement of stage N independent of the stage-(N-1) workload.
            #
            # --kafka-consumer-only and --alert-worker-only are self-contained
            # (the latter prefills its own packets queue inside its shell
            # script) and therefore need no prerequisite-stage refill.
            rc = 0
            if args.enrichment_only or args.filter_only:
                rc = run_alert_only_bench(counts)
            if rc == 0 and args.filter_only:
                rc = run_enrichment_only_bench(counts)
            if rc == 0:
                if args.alert_only:
                    rc = run_alert_only_bench(counts)
                elif args.enrichment_only:
                    rc = run_enrichment_only_bench(counts)
                elif args.filter_only:
                    rc = run_filter_only_bench(counts)
                elif args.kafka_consumer_only:
                    if args.vary == "process":
                        rc = run_kafka_consumer_only_bench(n, args.batch_size)
                    else:  # "batch"
                        rc = run_kafka_consumer_only_bench(args.n_processes, n)
                else:  # args.alert_worker_only
                    rc = run_alert_worker_only_bench(counts)
        else:
            phase = "bench" if args.warm or args.bench_only else "full"
            rc = run_phase(phase, counts)
        if rc != 0 or not os.path.exists(wall_time_path):
            print(f"  -> run failed (rc={rc}), skipping", file=sys.stderr)
            continue
    with open(wall_time_path) as f:
        wall_time_s = float(f.read().strip())
    throughput = EXPECTED_ALERTS / wall_time_s
    results.append((n, wall_time_s, throughput))
    print(f"  {args.vary}={n} -> {wall_time_s:.1f} s ({throughput:.0f} alerts/s)")

if args.warm:
    print("\n=== Warm teardown: stopping all services ===", flush=True)
    run_phase("teardown", setup_counts)

if not results:
    sys.exit("No successful runs")

xs = [r[0] for r in results]
walls = [r[1] for r in results]
tputs = [r[2] for r in results]

# In stage-only modes, the irrelevant stages have n_workers=0 (forced by the
# run_*_only.py wrappers). Listing them as "fixed" in the subtitle is
# misleading — they are not "fixed at args.n_*_workers", they are disabled.
# So the legend only mentions stages that are actually active for this mode.
stage_only = (
    "alert" if args.alert_only
    else "enrichment" if args.enrichment_only
    else "filter" if args.filter_only
    else None
)
fixed_stages = {stage_only} if stage_only else set(WORKER_KINDS)
fixed = {k: v for k, v in {
    "alert": args.n_alert_workers,
    "enrichment": args.n_enrichment_workers,
    "filter": args.n_filter_workers,
}.items() if k != args.vary and k in fixed_stages}
fixed_str = ", ".join(f"{k}={v}" for k, v in fixed.items())

if args.vary == "gpu":
    vary_label = "GPUs"
    csv_col_name = "n_gpus"
    x_axis_label = "Number of GPUs"
elif args.vary == "process":
    vary_label = "consumer processes"
    csv_col_name = "n_processes"
    x_axis_label = "Number of consumer processes"
elif args.vary == "batch":
    vary_label = "KAFKA_BATCH_SIZE"
    csv_col_name = "batch_size"
    x_axis_label = "KAFKA_BATCH_SIZE"
else:
    vary_label = f"{args.vary} workers"
    csv_col_name = f"n_{args.vary}_workers"
    x_axis_label = f"Number of {vary_label}"

os.makedirs(os.path.dirname(plot_path) or ".", exist_ok=True)
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
ax1.plot(xs, walls, marker="o")
ax1.set_xlabel(x_axis_label)
ax1.set_ylabel("Wall time (s)")
ax1.set_title(f"Wall time vs. {vary_label}")
ax1.grid(True, alpha=0.3)

ax2.plot(xs, tputs, marker="o", color="C2")
ax2.set_xlabel(x_axis_label)
ax2.set_ylabel("Throughput (alerts/s)")
ax2.set_title(f"Throughput vs. {vary_label}")
ax2.grid(True, alpha=0.3)

gpu_tag = " GPU" if gpu_mode and args.vary != "gpu" else ""

# Main title is the stage name. The subtitle below lists what that stage
# actually does, so the reader sees both the scope and the workload.
STAGE_TITLES = {
    "alert": "BOOM alert ingestion",
    "enrichment": "BOOM enrichment",
    "filter": "BOOM filtering",
}
STAGE_OPERATIONS = {
    "alert": "kafka consume + avro decode + crossmatch + Mongo insert",
    "enrichment": "ML inference + alert features + babamul Kafka push + Mongo update",
    "filter": "filter pipelines (Mongo aggregations) + Kafka produce",
}
if stage_only is not None:
    main_title = f"{STAGE_TITLES[stage_only]}{gpu_tag}"
    # Only mention the varying axis when it isn't the stage itself; otherwise
    # it's redundant (e.g. --alert-only only allows --vary alert).
    if args.vary != stage_only:
        main_title += f" — varying {vary_label}"
    if fixed_str:
        main_title += f" ({fixed_str})"
    subtitle = STAGE_OPERATIONS[stage_only]
else:
    main_title = f"BOOM{gpu_tag} benchmark — varying {vary_label} ({fixed_str})"
    subtitle = None

fig.suptitle(main_title, fontsize=12)
if subtitle:
    # tight_layout below doesn't know about fig.text(), so reserve vertical
    # space at the top so the subtitle and suptitle don't collide with the
    # axes.
    fig.text(0.5, 0.91, subtitle, ha="center", fontsize=9, style="italic")
fig.tight_layout(rect=[0, 0, 1, 0.90] if subtitle else None)
fig.savefig(plot_path, dpi=120)
print(f"\nPlot saved to {plot_path}")

with open(csv_path, "w") as f:
    f.write(f"{csv_col_name},wall_time_s,throughput_alerts_per_s\n")
    for n, t, tput in results:
        f.write(f"{n},{t:.2f},{tput:.2f}\n")
print(f"CSV saved to {csv_path}")
