#!/usr/bin/env python3
"""Run one controlled experiment stage; consume only benchmark JSON output."""

import argparse
import datetime
import json
import os
from pathlib import Path
import random
import statistics
import subprocess
import sys


STAGES = ("queues", "batching", "coalescing", "topology", "ownership", "retention", "idle", "finalists")
VALUE_OPTIONS = {
    "host", "port", "table", "input", "token-env", "ca-file", "producers", "streams",
    "connections", "io-threads", "queue", "batch", "batch-records", "batch-bytes",
    "batch-delay-us", "retention", "idle", "spin-us", "write-coalesce-bytes",
    "buffer-bytes", "queue-capacity", "max-inflight", "sample-every",
    "admission-timeout-ms", "warmup", "duration", "drain-timeout",
}
FLAG_OPTIONS = {"test-plaintext"}


def validate_options(options):
    unknown = set(options) - VALUE_OPTIONS - FLAG_OPTIONS
    if unknown:
        raise ValueError("unknown benchmark configuration keys: " + ", ".join(sorted(unknown)))
    for key in ("host", "port", "table", "input"):
        if not options.get(key):
            raise ValueError("configuration requires explicit " + key)
    for key in FLAG_OPTIONS:
        if key in options and not isinstance(options[key], bool):
            raise ValueError(key + " must be boolean")
    for key in VALUE_OPTIONS & options.keys():
        if isinstance(options[key], (dict, list, bool)) or options[key] is None:
            raise ValueError(key + " must be a string or number")
    if int(options.get("connections", 1)) > int(options.get("streams", 1)):
        raise ValueError("connections must not exceed streams")
    if int(options.get("streams", 1)) > int(options.get("producers", 1)):
        raise ValueError("streams must not exceed producers")
    if int(options.get("io-threads", 1)) > int(options.get("connections", 1)):
        raise ValueError("io-threads must not exceed connections")


def command_for(bench, options):
    validate_options(options)
    argv = [str(bench)]
    for name in sorted(options):
        if name in FLAG_OPTIONS:
            if options[name]:
                argv.append("--" + name)
        else:
            argv.extend(("--" + name, str(options[name])))
    return argv


def variants_for(stage, baseline, producer_counts, finalists=None):
    variants = []
    for producers in producer_counts:
        base = dict(baseline, producers=producers)
        if stage == "queues":
            settings = [(q, {"queue": q}) for q in ("mutex", "atomic")]
        elif stage == "batching":
            settings = [
                ("single", {"batch": "single", "batch-delay-us": 0}),
                ("explicit", {"batch": "explicit", "batch-delay-us": 0}),
                ("auto_1m_now", {"batch": "auto", "batch-bytes": 1048576, "batch-delay-us": 0}),
                ("auto_64k_now", {"batch": "auto", "batch-bytes": 65536, "batch-delay-us": 0}),
                ("auto_1m_50us", {"batch": "auto", "batch-bytes": 1048576, "batch-delay-us": 50}),
            ]
        elif stage == "coalescing":
            settings = [("direct", {"write-coalesce-bytes": 0}),
                        ("coalesce_64k", {"write-coalesce-bytes": 65536}),
                        ("coalesce_256k", {"write-coalesce-bytes": 262144})]
        elif stage == "topology":
            settings = [("shared_stream", {"streams": 1, "connections": 1, "io-threads": 1})]
            if producers > 1:
                settings.extend([
                    ("shared_connection", {"streams": producers, "connections": 1, "io-threads": 1}),
                    ("separate_connections", {"streams": producers, "connections": producers, "io-threads": 1}),
                ])
        elif stage == "ownership":
            base.update(streams=producers, connections=producers)
            settings = [("shared_owner", {"io-threads": 1})]
            if producers > 1:
                settings.append(("dedicated_owners", {"io-threads": producers}))
        elif stage == "retention":
            settings = [(mode, {"retention": mode}) for mode in ("sent", "acked")]
        elif stage == "idle":
            settings = [(mode, {"idle": mode}) for mode in ("sleep", "spin", "busy")]
        else:
            if not finalists:
                raise ValueError("finalists stage requires a nonempty finalists array")
            settings = []
            for finalist in finalists:
                if not isinstance(finalist, dict) or not isinstance(finalist.get("name"), str) or not isinstance(finalist.get("options"), dict):
                    raise ValueError("each finalist needs name and options")
                if "producers" in finalist["options"]:
                    raise ValueError("finalist options must not override the producer sweep")
                settings.append((finalist["name"], finalist["options"]))
        for name, overrides in settings:
            options = dict(base, **overrides)
            validate_options(options)
            variants.append({"name": name + "_p" + str(producers), "options": options})
    names = [variant["name"] for variant in variants]
    if len(names) != len(set(names)):
        raise ValueError("variant names must be unique")
    return variants


def interleave(variants, repeats, seed):
    """Each round runs every variant once, with reproducible shuffled order."""
    rng = random.Random(seed)
    jobs = []
    for repeat in range(repeats):
        ordered = list(variants)
        rng.shuffle(ordered)
        jobs.extend(dict(variant, repeat=repeat + 1) for variant in ordered)
    return jobs


def normalize_result_units(result):
    """Accept older benchmark binaries while saving throughput in decimal MB/s."""
    measurement = result.get("measurement")
    if isinstance(measurement, dict):
        for prefix in ("acked", "cohort_acked"):
            old = prefix + "_bytes_per_second"
            new = prefix + "_mb_per_second"
            if old in measurement:
                rate = measurement.pop(old)
                measurement.setdefault(new, rate / 1_000_000)
    if result.get("schema_version") == 1:
        result["schema_version"] = 2


def append_result(output, row, closing, first):
    """Replace only the closing bracket, keeping completed rows readable between runs."""
    output.seek(closing)
    if not first:
        output.write(",\n")
    output.write("\n".join("  " + line for line in json.dumps(row, indent=2).splitlines()))
    closing = output.tell()
    output.write("\n]\n")
    output.flush()
    return closing


def summarize(rows):
    groups = {}
    for row in rows:
        group = groups.setdefault(row["variant"], {"successful": [], "failed": 0})
        result = row["result"]
        if row["exit_code"] == 0 and result.get("ok") and "measurement" in result:
            group["successful"].append(result["measurement"])
        else:
            group["failed"] += 1
    summaries = []
    for name, group in sorted(groups.items()):
        summary = {"variant": name, "successful_runs": len(group["successful"]), "failed_runs": group["failed"]}
        for metric in ("acked_records_per_second", "acked_mb_per_second", "cohort_acked_records_per_second",
                       "cohort_acked_mb_per_second", "cohort_cpu_ns_per_acked_record"):
            values = [item[metric] for item in group["successful"]]
            if values:
                med = statistics.median(values)
                summary[metric] = {
                    "median": med, "min": min(values), "max": max(values),
                    "relative_range": (max(values) - min(values)) / med if med else None,
                }
        summaries.append(summary)
    return summaries


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bench", required=True, type=Path)
    parser.add_argument("--config", required=True, type=Path, help="JSON object with baseline and optional finalists")
    parser.add_argument("--stage", required=True, choices=STAGES)
    parser.add_argument("--producers", default="1,2,4,8,16")
    parser.add_argument("--repeats", type=int)
    parser.add_argument("--seed", type=int, default=20260922)
    parser.add_argument("--output", type=Path, help="new pretty JSON array file (required unless --dry-run)")
    parser.add_argument("--dry-run", action="store_true", help="print commands without running or writing files")
    args = parser.parse_args()
    try:
        config = json.loads(args.config.read_text())
        baseline = config["baseline"]
        if not isinstance(baseline, dict):
            raise ValueError("baseline must be an object")
        producers = [int(value) for value in args.producers.split(",")]
        if not producers or any(value <= 0 for value in producers) or len(set(producers)) != len(producers):
            raise ValueError("producer counts must be distinct positive integers")
        repeats = args.repeats if args.repeats is not None else (5 if args.stage == "finalists" else 3)
        if repeats < 1:
            raise ValueError("repeats must be positive")
        variants = variants_for(args.stage, baseline, producers, config.get("finalists"))
        jobs = interleave(variants, repeats, args.seed)
        bench = args.bench.resolve()
        commands = [dict(job, command=command_for(bench, job["options"])) for job in jobs]
        if args.dry_run:
            print(json.dumps({"stage": args.stage, "seed": args.seed, "runs": commands}, indent=2))
            return 0
        if args.output is None:
            raise ValueError("--output is required for execution")
        if not bench.is_file() or not os.access(bench, os.X_OK):
            raise ValueError("benchmark executable is missing or not executable")
        # Validate every effective configuration before the first ingestion run.
        for variant in variants:
            validation = subprocess.run(command_for(bench, variant["options"]) + ["--dry-run"],
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                        universal_newlines=True, timeout=60, check=False)
            if validation.returncode:
                raise ValueError("benchmark rejected configuration " + variant["name"])
        rows = []
        with args.output.open("x") as output:
            output.write("[\n")
            closing = output.tell()
            output.write("]\n")
            output.flush()
            for i, job in enumerate(commands):
                print("[{}/{}] {} repeat {}".format(i + 1, len(jobs), job["name"], job["repeat"]), file=sys.stderr)
                options = job["options"]
                # The process also opens connections and streams before both phases.
                timeout = (float(options.get("warmup", 10)) + float(options.get("duration", 60)) +
                           2 * float(options.get("drain-timeout", 60)) +
                           30 * (int(options.get("connections", 1)) + int(options.get("streams", 1))) + 30)
                started = datetime.datetime.now(datetime.timezone.utc).isoformat()
                try:
                    run = subprocess.run(job["command"], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                         universal_newlines=True, timeout=timeout, check=False)
                    code = run.returncode
                    try:
                        result = json.loads(run.stdout)
                        if not isinstance(result, dict):
                            raise ValueError("not an object")
                    except (ValueError, json.JSONDecodeError):
                        result = {"ok": False, "error": "benchmark returned invalid JSON"}
                    if run.stderr:
                        # SDK/benchmark diagnostics have no token values; do not persist subprocess output.
                        print(run.stderr, end="", file=sys.stderr)
                except subprocess.TimeoutExpired:
                    code = -1
                    result = {"ok": False, "error": "matrix process timeout"}
                normalize_result_units(result)
                row = {"stage": args.stage, "variant": job["name"], "repeat": job["repeat"],
                       "seed": args.seed, "started_at": started, "command": job["command"],
                       "exit_code": code, "result": result}
                closing = append_result(output, row, closing, not rows)
                rows.append(row)
        summary = {"stage": args.stage, "results_file": str(args.output), "variants": summarize(rows),
                   "interpretation": "Compare repeatability and CPU cost; these summaries do not establish a winner or exclude a service bottleneck."}
        print(json.dumps(summary, indent=2))
        return 0 if all(row["exit_code"] == 0 and row["result"].get("ok") for row in rows) else 1
    except (OSError, ValueError, KeyError, TypeError, subprocess.TimeoutExpired) as error:
        print("run_matrix: " + str(error), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
