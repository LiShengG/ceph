#!/usr/bin/env python3
"""Summarize and validate raw dirfrag fetch benchmark samples."""

import argparse
import csv
import hashlib
import math
import random
import statistics
from collections import defaultdict
from pathlib import Path


DIMENSIONS = (
    "commit", "client_type", "workload", "directory_entries", "effective_batch",
    "dispatch_delay_ms", "mode",
)
PAIR_DIMENSIONS = (
    "client_type", "workload", "directory_entries", "effective_batch",
    "dispatch_delay_ms",
)
METRICS = (
    "client_latency_ms",
    "fetch_fraction_percent",
    "nonfetch_residual_ms",
    "dir_fetch_latency_ms",
    "dir_fetch_decode_latency_ms",
    "dir_fetch_batch_latency_sum_ms",
    "dir_fetch_batch_latency_mean_ms",
    "dir_fetch_estimated_overlap_ms",
    "dir_fetch_batches",
    "dir_fetch_omap_bytes",
    "dir_fetch_peak_omap_bytes",
    "mds_requests",
    "rss_peak_kb",
    "rss_delta_kb",
    "mds_task_clock_ms",
    "mds_cycles",
    "mds_instructions",
    "mds_context_switches",
    "lookup_throughput_ops_s",
    "lookup_latency_p50_ms",
    "lookup_latency_p95_ms",
    "lookup_latency_p99_ms",
)

SUMMARY_BOOTSTRAP_METHOD = "nonparametric percentile bootstrap of median"
RELATIVE_BOOTSTRAP_METHOD = (
    "independent nonparametric percentile bootstrap of median ratio"
)
PAIRED_BOOTSTRAP_METHOD = (
    "adjacent true/false pairs within mirrored four-sample blocks; "
    "nonparametric percentile bootstrap of median paired contrast"
)


def number(value):
    if value is None or value == "":
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def percentile(values, percent):
    ordered = sorted(values)
    rank = (len(ordered) - 1) * percent / 100.0
    lo = int(rank)
    hi = min(lo + 1, len(ordered) - 1)
    fraction = rank - lo
    return ordered[lo] * (1.0 - fraction) + ordered[hi] * fraction


def bootstrap_median_ci(values, iterations, seed):
    if len(values) == 1:
        return values[0], values[0]
    rng = random.Random(seed)
    estimates = []
    for _ in range(iterations):
        estimates.append(statistics.median(rng.choices(values, k=len(values))))
    return percentile(estimates, 2.5), percentile(estimates, 97.5)


def bootstrap_relative_median_ci(false_values, true_values, iterations, seed):
    rng = random.Random(seed)
    estimates = []
    for _ in range(iterations):
        false_median = statistics.median(
            rng.choices(false_values, k=len(false_values)))
        true_median = statistics.median(
            rng.choices(true_values, k=len(true_values)))
        if false_median:
            estimates.append((true_median / false_median - 1.0) * 100.0)
    if not estimates:
        return None, None
    return percentile(estimates, 2.5), percentile(estimates, 97.5)


def summarize(values, iterations, seed):
    ci_low, ci_high = bootstrap_median_ci(values, iterations, seed)
    return {
        "samples": len(values),
        "median": statistics.median(values),
        "p95": percentile(values, 95),
        "mean": statistics.fmean(values),
        "stdev": statistics.stdev(values) if len(values) > 1 else 0.0,
        "median_ci95_low": ci_low,
        "median_ci95_high": ci_high,
    }


def stable_seed(parts):
    digest = hashlib.sha256("\0".join(parts).encode()).digest()
    return int.from_bytes(digest[:8], "little")


def read_samples(paths):
    rows = []
    for path in paths:
        with path.open(newline="", encoding="utf-8") as stream:
            rows.extend(csv.DictReader(stream))
    measured = [row for row in rows
                if row.get("phase", "measure") == "measure"]
    for row in measured:
        if not row.get("client_type"):
            row["client_type"] = "fuse"
        client = number(row.get("client_latency_ms"))
        fetch = number(row.get("dir_fetch_latency_ms"))
        if client is not None and fetch is not None:
            row["fetch_fraction_percent"] = (
                fetch / client * 100.0 if client else None)
            row["nonfetch_residual_ms"] = client - fetch
    return measured


def validate_correctness(rows):
    errors = []
    for row in rows:
        label = "order={0} mode={1} size={2} batch={3} delay={4}".format(
            row.get("execution_order"), row.get("mode"),
            row.get("directory_entries"), row.get("effective_batch"),
            row.get("dispatch_delay_ms"))
        expected = number(row.get("directory_entries"))
        observed = number(row.get("entries"))
        if expected != observed:
            errors.append(f"{label}: entries {observed} != {expected}")
        complete = number(row.get("dir_fetch_complete"))
        if complete != 1:
            errors.append(f"{label}: dir_fetch_complete is {complete}, expected 1")

    by_condition = defaultdict(list)
    for row in rows:
        key = (row.get("commit", ""),) + tuple(
            row.get(name, "") for name in PAIR_DIMENSIONS)
        by_condition[key].append(row)
    for key, condition_rows in by_condition.items():
        modes = {row["mode"] for row in condition_rows}
        if not {"false", "true"}.issubset(modes):
            continue
        hashes = {row.get("hash") for row in condition_rows}
        if len(hashes) != 1:
            errors.append(f"condition {key}: modes produced different hashes")
        for field in ("dir_fetch_batches", "dir_fetch_omap_bytes"):
            values = {number(row.get(field)) for row in condition_rows}
            if len(values) != 1:
                errors.append(f"condition {key}: modes disagree on {field}: {values}")
    return errors


def build_summaries(rows, iterations):
    grouped = defaultdict(list)
    for row in rows:
        key = tuple(row.get(name, "") for name in DIMENSIONS)
        grouped[key].append(row)

    summaries = []
    for key, group_rows in sorted(grouped.items()):
        dimensions = dict(zip(DIMENSIONS, key))
        for metric in METRICS:
            values = [number(row.get(metric)) for row in group_rows]
            values = [value for value in values if value is not None]
            if not values:
                continue
            seed = stable_seed([*key, metric])
            summaries.append({
                **dimensions,
                "metric": metric,
                "bootstrap_iterations": iterations,
                "bootstrap_seed": seed,
                "bootstrap_method": SUMMARY_BOOTSTRAP_METHOD,
                **summarize(values, iterations, seed),
            })
    return summaries


def build_comparisons(summaries, rows=None, iterations=2000):
    lookup = {}
    for summary in summaries:
        key = tuple(summary[name] for name in PAIR_DIMENSIONS)
        key += (summary["mode"], summary["metric"])
        lookup.setdefault(key, []).append(summary)

    comparisons = []
    pair_keys = {tuple(summary[name] for name in PAIR_DIMENSIONS)
                 for summary in summaries}
    for key in sorted(pair_keys):
        metrics = {summary["metric"] for summary in summaries
                   if tuple(summary[name] for name in PAIR_DIMENSIONS) == key}
        for metric in sorted(metrics):
            false_rows = lookup.get(key + ("false", metric), [])
            true_rows = lookup.get(key + ("true", metric), [])
            if not false_rows or not true_rows:
                continue
            # A normal HEAD run has one commit per mode.  Refuse to silently
            # combine commits if several CSV inputs happen to overlap.
            if len(false_rows) != 1 or len(true_rows) != 1:
                continue
            baseline = false_rows[0]
            enabled = true_rows[0]
            before = baseline["median"]
            delta = ((enabled["median"] / before) - 1.0) * 100 if before else None
            relative_seed = stable_seed([*key, metric, "relative"])
            relative_ci_low = relative_ci_high = None
            if rows is not None:
                false_values = [number(row.get(metric)) for row in rows
                                if row.get("mode") == "false" and
                                row.get("commit", "") == baseline["commit"] and
                                tuple(row.get(name, "") for name in
                                      PAIR_DIMENSIONS) == key]
                true_values = [number(row.get(metric)) for row in rows
                               if row.get("mode") == "true" and
                               row.get("commit", "") == enabled["commit"] and
                               tuple(row.get(name, "") for name in
                                     PAIR_DIMENSIONS) == key]
                false_values = [value for value in false_values
                                if value is not None]
                true_values = [value for value in true_values
                               if value is not None]
                if false_values and true_values:
                    relative_ci_low, relative_ci_high = (
                        bootstrap_relative_median_ci(
                            false_values, true_values, iterations,
                            relative_seed))
            comparisons.append({
                **dict(zip(PAIR_DIMENSIONS, key)),
                "metric": metric,
                "false_commit": baseline["commit"],
                "true_commit": enabled["commit"],
                "false_median": before,
                "true_median": enabled["median"],
                "relative_change_percent": delta,
                "relative_change_ci95_low": relative_ci_low,
                "relative_change_ci95_high": relative_ci_high,
                "bootstrap_iterations": iterations,
                "bootstrap_seed": relative_seed,
                "bootstrap_method": RELATIVE_BOOTSTRAP_METHOD,
                "false_ci95_low": baseline["median_ci95_low"],
                "false_ci95_high": baseline["median_ci95_high"],
                "true_ci95_low": enabled["median_ci95_low"],
                "true_ci95_high": enabled["median_ci95_high"],
            })
    return comparisons


def build_paired_sensitivity(rows, iterations):
    """Summarize adjacent T/F contrasts inside balanced four-sample blocks."""
    grouped = defaultdict(list)
    for row in rows:
        key = (row.get("commit", ""),) + tuple(
            row.get(name, "") for name in PAIR_DIMENSIONS)
        grouped[key].append(row)

    results = []
    for key, condition_rows in sorted(grouped.items()):
        ordered = sorted(condition_rows,
                         key=lambda row: number(row.get("execution_order")))
        contrasts = defaultdict(lambda: {"difference": [], "relative": []})
        complete_blocks = 0
        for offset in range(0, len(ordered), 4):
            block = ordered[offset:offset + 4]
            if len(block) != 4:
                continue
            modes = tuple(row.get("mode") for row in block)
            if modes not in (("false", "true", "true", "false"),
                             ("true", "false", "false", "true")):
                continue
            complete_blocks += 1
            for pair in (block[:2], block[2:]):
                false_row = next(row for row in pair
                                 if row.get("mode") == "false")
                true_row = next(row for row in pair
                                if row.get("mode") == "true")
                for metric in METRICS:
                    before = number(false_row.get(metric))
                    after = number(true_row.get(metric))
                    if before is None or after is None:
                        continue
                    contrasts[metric]["difference"].append(after - before)
                    if before > 0:
                        contrasts[metric]["relative"].append(
                            (after / before - 1.0) * 100.0)

        dimensions = {
            "commit": key[0],
            **dict(zip(PAIR_DIMENSIONS, key[1:])),
        }
        for metric, values in sorted(contrasts.items()):
            differences = values["difference"]
            relatives = values["relative"]
            if not differences:
                continue
            seed = stable_seed([*key, metric, "paired"])
            difference_summary = summarize(differences, iterations, seed)
            relative_summary = (summarize(relatives, iterations, seed ^ 1)
                                if relatives else None)
            row = {
                **dimensions,
                "metric": metric,
                "complete_blocks": complete_blocks,
                "pairs": len(differences),
                "bootstrap_iterations": iterations,
                "bootstrap_seed": seed,
                "bootstrap_method": PAIRED_BOOTSTRAP_METHOD,
                **{f"difference_{name}": value
                   for name, value in difference_summary.items()
                   if name != "samples"},
                "relative_pairs": len(relatives),
            }
            if relative_summary:
                row.update({
                    f"relative_change_percent_{name}": value
                    for name, value in relative_summary.items()
                    if name != "samples"
                })
            results.append(row)
    return results


def build_client_comparisons(summaries, rows=None, iterations=2000):
    dimensions = (
        "workload", "directory_entries", "effective_batch",
        "dispatch_delay_ms", "mode",
    )
    lookup = {}
    for summary in summaries:
        key = tuple(summary[name] for name in dimensions)
        key += (summary["client_type"], summary["metric"])
        lookup.setdefault(key, []).append(summary)

    comparisons = []
    keys = {tuple(summary[name] for name in dimensions)
            for summary in summaries}
    for key in sorted(keys):
        metrics = {summary["metric"] for summary in summaries
                   if tuple(summary[name] for name in dimensions) == key}
        for metric in sorted(metrics):
            fuse_rows = lookup.get(key + ("fuse", metric), [])
            kernel_rows = lookup.get(key + ("kernel", metric), [])
            if len(fuse_rows) != 1 or len(kernel_rows) != 1:
                continue
            fuse = fuse_rows[0]
            kernel = kernel_rows[0]
            before = fuse["median"]
            delta = ((kernel["median"] / before) - 1.0) * 100.0 \
                if before else None
            seed = stable_seed([*key, metric, "kernel-vs-fuse"])
            ci_low = ci_high = None
            if rows is not None:
                fuse_values = [number(row.get(metric)) for row in rows
                               if row.get("client_type") == "fuse" and
                               tuple(row.get(name, "") for name in dimensions)
                               == key]
                kernel_values = [number(row.get(metric)) for row in rows
                                 if row.get("client_type") == "kernel" and
                                 tuple(row.get(name, "") for name in dimensions)
                                 == key]
                fuse_values = [value for value in fuse_values
                               if value is not None]
                kernel_values = [value for value in kernel_values
                                 if value is not None]
                if fuse_values and kernel_values:
                    ci_low, ci_high = bootstrap_relative_median_ci(
                        fuse_values, kernel_values, iterations, seed)
            comparisons.append({
                **dict(zip(dimensions, key)),
                "metric": metric,
                "fuse_commit": fuse["commit"],
                "kernel_commit": kernel["commit"],
                "fuse_median": fuse["median"],
                "kernel_median": kernel["median"],
                "kernel_relative_to_fuse_percent": delta,
                "relative_change_ci95_low": ci_low,
                "relative_change_ci95_high": ci_high,
                "bootstrap_iterations": iterations,
                "bootstrap_seed": seed,
                "bootstrap_method": RELATIVE_BOOTSTRAP_METHOD,
                "comparison_scope": (
                    "descriptive only: separate client runs at different times"),
            })
    return comparisons


def build_outlier_flags(rows):
    """Flag descriptive Tukey-fence anomalies without excluding any sample."""
    grouped = defaultdict(list)
    for row in rows:
        key = tuple(row.get(name, "") for name in DIMENSIONS)
        grouped[key].append(row)

    flags = []
    for key, group_rows in sorted(grouped.items()):
        dimensions = dict(zip(DIMENSIONS, key))
        for metric in METRICS:
            observations = [(row, number(row.get(metric))) for row in group_rows]
            observations = [(row, value) for row, value in observations
                            if value is not None]
            if len(observations) < 4:
                continue
            values = [value for _, value in observations]
            q1 = percentile(values, 25)
            q3 = percentile(values, 75)
            iqr = q3 - q1
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr
            for row, value in observations:
                if value < lower or value > upper:
                    flags.append({
                        **dimensions,
                        "metric": metric,
                        "execution_order": row.get("execution_order"),
                        "sample_dir": row.get("sample_dir"),
                        "value": value,
                        "q1": q1,
                        "q3": q3,
                        "lower_fence": lower,
                        "upper_fence": upper,
                        "rule": "Tukey 1.5 IQR descriptive flag",
                        "included_in_analysis": True,
                    })
    return flags


def build_parent_comparisons(summaries):
    external_metrics = {
        "client_latency_ms", "rss_peak_kb", "rss_delta_kb",
        "mds_task_clock_ms", "mds_cycles", "mds_instructions",
        "mds_context_switches",
    }
    grouped = defaultdict(list)
    for summary in summaries:
        if summary["metric"] not in external_metrics:
            continue
        key = tuple(summary[name] for name in PAIR_DIMENSIONS)
        key += (summary["mode"], summary["metric"])
        grouped[key].append(summary)

    comparisons = []
    conditions = {tuple(summary[name] for name in PAIR_DIMENSIONS)
                  for summary in summaries}
    for key in sorted(conditions):
        for metric in sorted(external_metrics):
            heads = grouped.get(key + ("false", metric), [])
            parents = grouped.get(key + ("legacy-parent", metric), [])
            if len(heads) != 1 or len(parents) != 1:
                continue
            head = heads[0]
            parent = parents[0]
            delta = ((parent["median"] / head["median"]) - 1.0) * 100 \
                if head["median"] else None
            intervals_overlap = not (
                head["median_ci95_high"] < parent["median_ci95_low"] or
                parent["median_ci95_high"] < head["median_ci95_low"])
            comparisons.append({
                **dict(zip(PAIR_DIMENSIONS, key)),
                "metric": metric,
                "head_commit": head["commit"],
                "parent_commit": parent["commit"],
                "head_false_median": head["median"],
                "parent_median": parent["median"],
                "parent_relative_change_percent": delta,
                "confidence_intervals_overlap": intervals_overlap,
                "passes_5pct_or_ci_overlap": (
                    delta is not None and abs(delta) <= 5.0) or intervals_overlap,
            })
    return comparisons


def performance_findings(comparisons):
    findings = []
    for item in comparisons:
        delta = item["relative_change_percent"]
        if delta is None:
            continue
        metric = item["metric"]
        delay = number(item["dispatch_delay_ms"])
        workload = item["workload"]
        client_type = item.get("client_type", "unknown")
        if delay == 0 and metric in ("dir_fetch_latency_ms", "mds_task_clock_ms"):
            findings.append((delta <= 5.0,
                             f"0 ms {client_type} {metric} change is "
                             f"{delta:+.2f}%"))
        if (workload == "concurrent" and metric == "lookup_latency_p99_ms"):
            findings.append((delta <= 5.0,
                             f"concurrent lookup p99 change is {delta:+.2f}%"))
    return findings


def write_csv(path, rows):
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def fmt(value):
    return "n/a" if value is None else f"{value:.4g}"


def write_report(path, rows, comparisons, parent_comparisons, errors, findings,
                 outlier_flags=()):
    lines = ["# CephFS dirfrag fetch benchmark", "",
             f"Measured samples: {len(rows)}", "",
             "Bootstrap intervals use a deterministic, nonparametric percentile "
             "bootstrap (2,000 iterations by default). Summary intervals cover "
             "the median; relative-change intervals independently resample each "
             "mode and use the median ratio.", ""]
    if errors:
        lines.extend(("## Correctness failures", ""))
        lines.extend(f"- {error}" for error in errors)
        lines.append("")
    else:
        lines.extend(("Correctness checks passed.", ""))
    lines.extend((f"Descriptive outlier flags: {len(outlier_flags)} (Tukey "
                  "1.5 IQR; every flagged sample remains included).", ""))

    lines.extend(("## Pipelined relative to disabled", "",
                  "| client | workload | entries | batch | delay ms | metric | false median | true median | change |",
                  "|---|---|---:|---:|---:|---|---:|---:|---:|"))
    for item in comparisons:
        lines.append("| {client_type} | {workload} | {directory_entries} | {effective_batch} | "
                     "{dispatch_delay_ms} | {metric} | {false} | {true} | {delta} |".format(
                         **item, false=fmt(item["false_median"]),
                         true=fmt(item["true_median"]),
                         delta=("n/a" if item["relative_change_percent"] is None
                                else f'{item["relative_change_percent"]:+.2f}%')))
    if parent_comparisons:
        lines.extend(("", "## Parent versus HEAD disabled", "",
                      "| entries | delay ms | metric | HEAD false | parent | change | gate |",
                      "|---:|---:|---|---:|---:|---:|---|"))
        for item in parent_comparisons:
            delta = item["parent_relative_change_percent"]
            lines.append("| {directory_entries} | {dispatch_delay_ms} | {metric} | "
                         "{head} | {parent} | {delta} | {gate} |".format(
                             **item, head=fmt(item["head_false_median"]),
                             parent=fmt(item["parent_median"]),
                             delta="n/a" if delta is None else f"{delta:+.2f}%",
                             gate=("PASS" if item["passes_5pct_or_ci_overlap"]
                                   else "FAIL")))
    lines.extend(("", "## Regression gates", ""))
    if findings:
        lines.extend(f"- {'PASS' if passed else 'FAIL'}: {message}"
                     for passed, message in findings)
    else:
        lines.append("- No comparable gate conditions were present.")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("samples", nargs="+", type=Path)
    parser.add_argument("--output", type=Path, default=Path("analysis"))
    parser.add_argument("--bootstrap-iterations", type=int, default=2000)
    parser.add_argument("--strict-performance", action="store_true")
    args = parser.parse_args()
    if args.bootstrap_iterations <= 0:
        parser.error("--bootstrap-iterations must be positive")

    rows = read_samples(args.samples)
    if not rows:
        parser.error("no measured samples found")
    args.output.mkdir(parents=True, exist_ok=True)
    errors = validate_correctness(rows)
    summaries = build_summaries(rows, args.bootstrap_iterations)
    comparisons = build_comparisons(summaries, rows, args.bootstrap_iterations)
    paired_sensitivity = build_paired_sensitivity(
        rows, args.bootstrap_iterations)
    client_comparisons = build_client_comparisons(
        summaries, rows, args.bootstrap_iterations)
    outlier_flags = build_outlier_flags(rows)
    parent_comparisons = build_parent_comparisons(summaries)
    findings = performance_findings(comparisons)
    write_csv(args.output / "summary.csv", summaries)
    write_csv(args.output / "comparisons.csv", comparisons)
    write_csv(args.output / "paired-sensitivity.csv", paired_sensitivity)
    write_csv(args.output / "client-comparisons.csv", client_comparisons)
    write_csv(args.output / "outlier-flags.csv", outlier_flags)
    write_csv(args.output / "parent-comparisons.csv", parent_comparisons)
    write_report(args.output / "report.md", rows, comparisons,
                 parent_comparisons, errors, findings, outlier_flags)

    parent_gate_metrics = {"client_latency_ms", "mds_task_clock_ms"}
    parent_failures = any(
        item["metric"] in parent_gate_metrics and
        not item["passes_5pct_or_ci_overlap"]
        for item in parent_comparisons)
    if errors or (args.strict_performance and
                  (any(not passed for passed, _ in findings) or parent_failures)):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
