#!/usr/bin/env python3
"""Issue rate-limited cached lookups and retain every latency sample."""

import argparse
import csv
import json
import os
import statistics
import time
from pathlib import Path


def percentile(values, percent):
    if not values:
        return None
    ordered = sorted(values)
    rank = (len(ordered) - 1) * percent / 100.0
    lo = int(rank)
    hi = min(lo + 1, len(ordered) - 1)
    fraction = rank - lo
    return ordered[lo] * (1.0 - fraction) + ordered[hi] * fraction


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", required=True, type=Path)
    parser.add_argument("--rate", required=True, type=float,
                        help="target operations per second")
    parser.add_argument("--stop-file", required=True, type=Path)
    parser.add_argument("--start-file", required=True, type=Path)
    parser.add_argument("--ready-file", required=True, type=Path)
    parser.add_argument("--samples", required=True, type=Path)
    parser.add_argument("--summary", required=True, type=Path)
    args = parser.parse_args()
    if args.rate <= 0:
        parser.error("--rate must be positive")

    os.stat(args.path)  # Fault the path into the MDS and client caches.
    args.ready_file.touch()
    while not args.start_file.exists() and not args.stop_file.exists():
        time.sleep(0.001)
    interval_ns = int(1_000_000_000 / args.rate)
    next_issue = time.monotonic_ns()
    started = next_issue
    samples = []
    errors = 0

    while not args.stop_file.exists():
        now = time.monotonic_ns()
        if now < next_issue:
            time.sleep((next_issue - now) / 1_000_000_000)
        op_started = time.monotonic_ns()
        try:
            os.stat(args.path)
        except OSError:
            errors += 1
        samples.append((op_started, time.monotonic_ns() - op_started))
        next_issue += interval_ns
        if next_issue < time.monotonic_ns() - interval_ns:
            next_issue = time.monotonic_ns()

    finished = time.monotonic_ns()
    args.samples.parent.mkdir(parents=True, exist_ok=True)
    with args.samples.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(("monotonic_ns", "latency_ns"))
        writer.writerows(samples)

    latencies = [latency for _, latency in samples]
    elapsed_s = (finished - started) / 1_000_000_000
    summary = {
        "operations": len(samples),
        "errors": errors,
        "elapsed_s": elapsed_s,
        "throughput_ops_s": len(samples) / elapsed_s if elapsed_s else None,
        "latency_p50_ns": percentile(latencies, 50),
        "latency_p95_ns": percentile(latencies, 95),
        "latency_p99_ns": percentile(latencies, 99),
        "latency_mean_ns": statistics.fmean(latencies) if latencies else None,
    }
    args.summary.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n",
                            encoding="utf-8")
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
