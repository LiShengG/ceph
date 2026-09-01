#!/usr/bin/env python3
"""Run controlled cold-cache CephFS dirfrag fetch experiments on vstart."""

import argparse
import concurrent.futures
import configparser
import csv
import json
import os
import platform
import random
import re
import shutil
import signal
import stat
import subprocess
import sys
import threading
import time
import tempfile
from pathlib import Path


SAMPLE_FIELDS = (
    "commit", "client_type", "mode", "workload", "directory_entries", "requested_batch",
    "effective_batch", "dispatch_delay_ms", "phase", "round",
    "execution_order", "client_latency_ms", "entries", "hash",
    "dir_fetch_complete", "dir_fetch_latency_ms",
    "fetch_latency_samples", "dir_fetch_decode_latency_ms",
    "decode_latency_samples", "dir_fetch_batch_latency_mean_ms",
    "dir_fetch_batch_latency_sum_ms", "dir_fetch_estimated_overlap_ms",
    "batch_latency_samples", "dir_fetch_batches",
    "dir_fetch_omap_bytes", "dir_fetch_peak_omap_bytes", "mds_requests",
    "rss_start_kb",
    "rss_peak_kb", "rss_delta_kb", "mds_task_clock_ms", "mds_cycles",
    "mds_instructions", "mds_context_switches", "lookup_throughput_ops_s",
    "lookup_latency_p50_ms", "lookup_latency_p95_ms",
    "lookup_latency_p99_ms", "mds_pid", "mode_confirmed",
    "fragment_dirs_confirmed",
    "keys_per_op_confirmed", "osd_omap_limit_confirmed",
    "dispatch_probability_confirmed",
    "dispatch_duration_confirmed", "sample_dir",
    "client_mount_fstype", "client_mount_source",
)


class CommandFailed(RuntimeError):
    pass


def run_command(command, *, cwd=None, env=None, check=True, capture=True):
    result = subprocess.run(
        [str(part) for part in command], cwd=cwd, env=env, text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    )
    if check and result.returncode:
        raise CommandFailed("command failed ({0}): {1}\nstdout:\n{2}\nstderr:\n{3}".format(
            result.returncode, " ".join(str(part) for part in command),
            result.stdout or "", result.stderr or ""))
    return result


def json_from_output(output):
    output = output.strip()
    try:
        return json.loads(output)
    except json.JSONDecodeError:
        start = min((position for position in (output.find("{"), output.find("["))
                     if position >= 0), default=-1)
        if start >= 0:
            return json.loads(output[start:])
        raise


def is_mounted(path):
    target = str(path.resolve()).replace(" ", "\\040")
    with open("/proc/self/mountinfo", encoding="utf-8") as stream:
        return any(line.split()[4] == target for line in stream)


def mount_details(path):
    target = str(path.resolve()).replace(" ", "\\040")
    with open("/proc/self/mountinfo", encoding="utf-8") as stream:
        for line in stream:
            before, separator, after = line.rstrip().partition(" - ")
            fields = before.split()
            if separator and len(fields) >= 6 and fields[4] == target:
                fs_fields = after.split()
                return {
                    "mountpoint": fields[4].replace("\\040", " "),
                    "root": fields[3].replace("\\040", " "),
                    "mount_options": fields[5].split(","),
                    "fstype": fs_fields[0],
                    "source": fs_fields[1],
                    "super_options": fs_fields[2].split(","),
                }
    return None


def normalize_mon_address(value):
    value = str(value)
    match = re.match(r"^v[12]:(.+?)(?:/\d+)?$", value)
    return match.group(1) if match else value.rsplit("/", 1)[0]


def kernel_mon_addresses(mon_dump):
    addresses = []
    for mon in mon_dump.get("mons", []):
        vectors = mon.get("public_addrs", {}).get("addrvec", [])
        v1 = [entry.get("addr") for entry in vectors
              if entry.get("type") == "v1" and entry.get("addr")]
        candidates = v1 or [entry.get("addr") for entry in vectors
                            if entry.get("addr")]
        if not candidates and mon.get("addr"):
            candidates = [mon["addr"]]
        for address in candidates[:1]:
            normalized = normalize_mon_address(address)
            if normalized not in addresses:
                addresses.append(normalized)
    if not addresses:
        raise RuntimeError("no monitor address found for kernel mount")
    return addresses


def resolve_fs_name(fs_list, requested=None):
    names = [item.get("name") for item in fs_list if item.get("name")]
    if requested:
        if requested not in names:
            raise RuntimeError(
                f"requested filesystem {requested!r} not present: {names}")
        return requested
    if len(names) != 1:
        raise RuntimeError(
            f"kernel client requires one filesystem or --fs-name: {names}")
    return names[0]


def read_keyring_secret(path, entity="client.admin"):
    parser = configparser.RawConfigParser()
    with path.open(encoding="utf-8") as stream:
        parser.read_file(stream)
    if not parser.has_option(entity, "key"):
        raise RuntimeError(f"{path} lacks {entity} key")
    return parser.get(entity, "key").strip()


def kernel_mount_command(helper, monitors, mountpoint, secretfile, fs_name):
    source = f"{','.join(monitors)}:/"
    options = ",".join((
        "name=admin", f"secretfile={secretfile}",
        f"mds_namespace={fs_name}", "noshare"))
    return [helper, source, mountpoint, "-o", options]


def read_vmrss_kb(pid):
    with open(f"/proc/{pid}/status", encoding="utf-8") as stream:
        for line in stream:
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    raise RuntimeError(f"VmRSS missing for pid {pid}")


def time_counter(counter):
    """Return (sum_seconds, count) for a perf time or time-average value."""
    if isinstance(counter, dict):
        return float(counter.get("sum", 0)), int(counter.get("avgcount", 0))
    return float(counter or 0), 1


def source_for_build(build_dir, fallback):
    cache = build_dir / "CMakeCache.txt"
    if cache.exists():
        match = re.search(r"^CMAKE_HOME_DIRECTORY:INTERNAL=(.*)$",
                          cache.read_text(errors="replace"), re.M)
        if match:
            return Path(match.group(1)).resolve()
    return fallback.resolve()


def matrix_conditions(name, preliminary_entries=100_000):
    if name == "core":
        return [("cold", 100_000, batch, delay)
                for batch in (128, 1024) for delay in (0, 1, 5)]
    if name == "scale":
        return [("cold", entries, 1024, delay)
                for entries in (10_000, 100_000, 1_000_000)
                for delay in (0, 5)]
    if name == "concurrency":
        return [("concurrent", 1_000_000, 1024, delay)
                for delay in (1, 5)]
    if name == "parent":
        return [("cold", 100_000, 1024, delay) for delay in (0, 5)]
    if name == "smoke":
        return [("cold", 10_000, 128, 0)]
    if name == "preliminary":
        return [("cold", preliminary_entries, 1024, 0)]
    raise ValueError(name)


def dirfrag_count(dump):
    if isinstance(dump, dict):
        dump = dump.get("frags")
    if not isinstance(dump, list):
        raise RuntimeError(f"unexpected dirfrag ls output: {dump!r}")
    return len(dump)


def cached_inode_numbers(dump):
    if isinstance(dump, dict):
        dump = dump.get("inodes")
    if not isinstance(dump, list):
        raise RuntimeError(f"unexpected dump cache output: {dump!r}")
    numbers = set()
    for inode in dump:
        if not isinstance(inode, dict) or "ino" not in inode:
            raise RuntimeError(f"dump cache inode lacks ino: {inode!r}")
        value = inode["ino"]
        numbers.add(int(value, 0) if isinstance(value, str) else int(value))
    return numbers


def target_cache_state(dump, target_inode):
    if isinstance(dump, dict):
        dump = dump.get("inodes")
    if not isinstance(dump, list):
        raise RuntimeError(f"unexpected dump cache output: {dump!r}")
    matches = []
    for inode in dump:
        if not isinstance(inode, dict) or "ino" not in inode:
            raise RuntimeError(f"dump cache inode lacks ino: {inode!r}")
        value = inode["ino"]
        number = int(value, 0) if isinstance(value, str) else int(value)
        if number == target_inode:
            matches.append(inode)
    if len(matches) > 1:
        raise RuntimeError(
            f"target inode {target_inode} appears {len(matches)} times in cache dump")
    if not matches:
        return {
            "present": False,
            "dirfrag_count": 0,
            "dentry_count": 0,
            "complete_dirfrag_count": 0,
            "inode_states": [],
            "inode_pins": {},
            "dirfrag_states": [],
            "dirfrag_pins": [],
        }
    inode = matches[0]
    dirfrags = inode.get("dirfrags", [])
    if not isinstance(dirfrags, list):
        raise RuntimeError(
            f"target inode {target_inode} has invalid dirfrags: {dirfrags!r}")
    return {
        "present": True,
        "dirfrag_count": len(dirfrags),
        "dentry_count": sum(len(frag.get("dentries", [])) for frag in dirfrags),
        "complete_dirfrag_count": sum(
            1 for frag in dirfrags if "complete" in frag.get("states", [])),
        "inode_states": inode.get("states", []),
        "inode_pins": inode.get("pins", {}),
        "dirfrag_states": [frag.get("states", []) for frag in dirfrags],
        "dirfrag_pins": [frag.get("pins", {}) for frag in dirfrags],
    }


def validate_respawn_cache(cache_dump, target_inode, *, after_stat=False,
                           allow_empty_inode=False):
    state = target_cache_state(cache_dump, target_inode)
    loaded_dir = (state["dirfrag_count"] or state["dentry_count"] or
                  state["complete_dirfrag_count"])
    if not after_stat and state["present"] and not allow_empty_inode:
        raise RuntimeError(
            f"target inode {target_inode} remains in MDS cache after respawn; "
            "journal replay may have reloaded it")
    if not after_stat and allow_empty_inode and loaded_dir:
        raise RuntimeError(
            f"target inode {target_inode} has dirfrag/dentry cache after "
            f"respawn: {state}")
    if after_stat and loaded_dir:
        raise RuntimeError(
            f"target inode {target_inode} was loaded before readdir: {state}")
    return {
        **state,
        "target_inode": target_inode,
        "cached_inode_count": len(cached_inode_numbers(cache_dump)),
        "stage": "after-stat" if after_stat else "after-respawn",
    }


def active_mds_count(status):
    fsmap = status.get("fsmap", {})
    active_by_info = 0
    for filesystem in fsmap.get("filesystems", []):
        info = filesystem.get("mdsmap", {}).get("info", {})
        active_by_info += sum(1 for daemon in info.values()
                              if daemon.get("state") == "up:active")
    active_by_rank = sum(1 for rank in fsmap.get("by_rank", [])
                         if rank.get("status") == "up:active")
    return max(active_by_info, active_by_rank)


def dataset_needs_creation(existing, expected, reuse_existing):
    if reuse_existing:
        if existing != expected:
            raise RuntimeError(
                f"reused dataset has {existing} entries, expected {expected}")
        return False
    if existing < 0 or existing > expected:
        raise RuntimeError(f"dataset has {existing} entries, expected at most {expected}")
    return existing < expected


DATASET_ENTRY_RE = re.compile(r"^entry-([0-9]{16})$")


def scan_dataset_entries(directory, expected):
    """Strictly validate a complete or resumable benchmark dataset."""
    present = bytearray(expected)
    count = 0
    with os.scandir(directory) as stream:
        for entry in stream:
            match = DATASET_ENTRY_RE.fullmatch(entry.name)
            if match is None:
                raise RuntimeError(f"unexpected dataset entry name: {entry.path}")
            index = int(match.group(1))
            if index >= expected:
                raise RuntimeError(
                    f"dataset entry index {index} is outside [0, {expected}): {entry.path}")
            if present[index]:
                raise RuntimeError(f"duplicate dataset entry index {index}: {entry.path}")
            metadata = entry.stat(follow_symlinks=False)
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_size != 0:
                raise RuntimeError(
                    f"dataset entry is not a regular zero-length file: {entry.path}")
            present[index] = 1
            count += 1
    return present, count


def create_empty_dataset_entry(path):
    """Create one entry, accepting EEXIST only after strict lstat validation."""
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        metadata = os.lstat(path)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size != 0:
            raise RuntimeError(
                f"raced dataset entry is not a regular zero-length file: {path}")
        return False
    else:
        os.close(descriptor)
        return True


def create_dataset_entries(directory, expected, present, workers, progress=None):
    """Fill missing entries with a bounded set of fixed-stride workers."""
    if len(present) != expected:
        raise ValueError("dataset presence bitmap has the wrong length")
    if workers <= 0 or workers > 32:
        raise ValueError("create workers must be between 1 and 32")

    completed = sum(present)
    next_report = ((completed // 10000) + 1) * 10000
    lock = threading.Lock()
    stop = threading.Event()

    def create_stride(worker):
        nonlocal completed, next_report
        for index in range(worker, expected, workers):
            if stop.is_set():
                return
            if present[index]:
                continue
            create_empty_dataset_entry(directory / f"entry-{index:016d}")
            with lock:
                completed += 1
                while progress is not None and completed >= next_report:
                    progress(next_report)
                    next_report += 10000

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=workers)
    futures = [executor.submit(create_stride, worker) for worker in range(workers)]
    try:
        for future in concurrent.futures.as_completed(futures):
            future.result()
    except BaseException:
        stop.set()
        for future in futures:
            future.cancel()
        executor.shutdown(wait=True, cancel_futures=True)
        raise
    else:
        executor.shutdown(wait=True)
    return completed


def validate_prepared_dataset(result, expected_entries, expected_hash=None):
    if result.get("entries") != expected_entries:
        raise RuntimeError(
            f"failed to prepare {expected_entries} entries: {result}")
    if expected_hash is not None and result.get("hash") != expected_hash:
        raise RuntimeError(
            f"dataset hash {result.get('hash')} does not match expected "
            f"{expected_hash}")


def validate_prepare_fragments(actual, expected):
    if actual != expected:
        raise RuntimeError(
            f"dataset has {actual} dirfrags, expected {expected}")


def ab_order(repetitions, rng):
    """Return F/T/T/F or its mirror in randomized four-sample blocks."""
    if repetitions % 2:
        pairs = [[False, True] if rng.randrange(2) else [True, False]
                 for _ in range(repetitions)]
        return [mode for pair in pairs for mode in pair]
    result = []
    for _ in range(repetitions // 2):
        block = ([False, True, True, False] if rng.randrange(2)
                 else [True, False, False, True])
        result.extend(block)
    return result


class RssSampler:
    def __init__(self, pid, output, interval_s=0.010):
        self.pid = pid
        self.output = output
        self.interval_s = interval_s
        self.stop_event = threading.Event()
        self.samples = []
        self.thread = threading.Thread(target=self._sample, daemon=True)

    def _sample(self):
        while not self.stop_event.is_set():
            try:
                self.samples.append((time.monotonic_ns(), read_vmrss_kb(self.pid)))
            except (FileNotFoundError, ProcessLookupError):
                break
            self.stop_event.wait(self.interval_s)

    def start(self):
        self._sample_once()
        self.thread.start()

    def _sample_once(self):
        self.samples.append((time.monotonic_ns(), read_vmrss_kb(self.pid)))

    def stop(self):
        self.stop_event.set()
        self.thread.join()
        try:
            self._sample_once()
        except (FileNotFoundError, ProcessLookupError):
            pass
        with self.output.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.writer(stream)
            writer.writerow(("monotonic_ns", "vmrss_kb"))
            writer.writerows(self.samples)
        values = [value for _, value in self.samples]
        return values[0], max(values)


class DirfragBenchmark:
    def __init__(self, args):
        self.args = args
        self.build_dir = args.build_dir.resolve()
        self.source_dir = source_for_build(
            self.build_dir, Path(__file__).resolve().parents[4])
        self.bin_dir = self.build_dir / "bin"
        self.output = args.output.resolve()
        self.output.mkdir(parents=True, exist_ok=True)
        self.mount = args.mount.resolve()
        self.lookup_mount = args.lookup_mount.resolve()
        self.conf = args.conf.resolve() if args.conf else self.build_dir / "ceph.conf"
        self.ceph = self._binary("ceph")
        self.ceph_fuse = (self._binary("ceph-fuse")
                          if args.client_type == "fuse" else None)
        self.mount_ceph = (Path(shutil.which("mount.ceph"))
                           if args.client_type == "kernel" and
                           shutil.which("mount.ceph") else None)
        self.umount = (Path(shutil.which("umount"))
                       if args.client_type == "kernel" and
                       shutil.which("umount") else None)
        if args.client_type == "kernel" and not self.mount_ceph:
            raise RuntimeError("mount.ceph is required for --client-type kernel")
        if args.client_type == "kernel" and not self.umount:
            raise RuntimeError("umount is required for --client-type kernel")
        self.driver = self._ensure_driver()
        self.lookup_driver = Path(__file__).with_name("lookup_load.py")
        self.command_log = self.output / "commands.log"
        self.samples_csv = self.output / "samples.csv"
        self.commit = args.commit or self._git_commit()
        self.mds_pid = args.mds_pid or self._discover_mds_pid()
        self.osd_ids = []
        self.execution_order = 0
        self.original_options = {}
        self.flags_added = []
        self.resolved_fs_name = args.fs_name
        self.kernel_monitors = []
        self.kernel_secret_file = None
        self.kernel_client_info = None
        self.kernel_preflight_result = None

    def _binary(self, name):
        candidate = self.bin_dir / name
        if candidate.exists():
            return candidate
        found = shutil.which(name)
        if found:
            return Path(found)
        raise RuntimeError(f"cannot find {name} in {self.bin_dir} or PATH")

    def _ensure_driver(self):
        if self.args.driver:
            return self.args.driver.resolve()
        destination = self.output / "bin" / "readdir_hash"
        destination.parent.mkdir(parents=True, exist_ok=True)
        source = Path(__file__).with_name("readdir_hash.c")
        if not destination.exists() or destination.stat().st_mtime < source.stat().st_mtime:
            compiler = shutil.which("cc")
            if not compiler:
                raise RuntimeError("cc is required to build readdir_hash")
            run_command([compiler, "-O2", "-std=c11", "-Wall", "-Wextra",
                         "-Werror", "-D_POSIX_C_SOURCE=200809L", source,
                         "-o", destination])
        return destination

    def _git_commit(self):
        result = run_command(["git", "rev-parse", "HEAD"], cwd=self.source_dir)
        return result.stdout.strip()

    def _live_mds_pids(self):
        candidates = []
        for pid_file in sorted((self.build_dir / "out").glob("mds.*.pid")):
            try:
                pid = int(pid_file.read_text(encoding="utf-8").strip())
                command = Path(f"/proc/{pid}/cmdline").read_bytes()
                if b"ceph-mds" in command:
                    candidates.append(pid)
            except (OSError, ValueError):
                pass
        return candidates

    def _discover_mds_pid(self):
        candidates = self._live_mds_pids()
        if len(candidates) != 1:
            raise RuntimeError("expected one live build/out/mds.*.pid; pass --mds-pid")
        return candidates[0]

    def _log_command(self, command):
        with self.command_log.open("a", encoding="utf-8") as stream:
            stream.write("{0} {1}\n".format(time.time_ns(),
                                             " ".join(str(x) for x in command)))

    def command(self, command, **kwargs):
        self._log_command(command)
        return run_command(command, **kwargs)

    def ceph_command(self, arguments, *, json_output=False, check=True):
        command = [self.ceph, "-c", self.conf]
        if json_output:
            command += ["--format", "json"]
        command += arguments
        result = self.command(command, check=check)
        return json_from_output(result.stdout) if json_output else result.stdout.strip()

    def tell(self, daemon, arguments, *, json_output=False, check=True):
        return self.ceph_command(["tell", daemon, *arguments],
                                 json_output=json_output, check=check)

    def get_option(self, daemon, name):
        value = self.tell(daemon, ["config", "get", name], json_output=True)
        if isinstance(value, dict):
            value = value.get(name, value.get("value", value))
        return str(value).strip().lower()

    def set_option(self, daemon, name, value, remember=True):
        if remember and (daemon, name) not in self.original_options:
            self.original_options[(daemon, name)] = self.get_option(daemon, name)
        text_value = str(value).lower() if isinstance(value, bool) else str(value)
        # injectargs updates legacy and developer options which are consumed
        # directly from g_conf(), even when they do not have a runtime flag.
        option = "--{0}={1}".format(name.replace("_", "-"), text_value)
        self.tell(daemon, ["injectargs", option])
        confirmed = self.get_option(daemon, name)
        if text_value.lower() not in confirmed:
            raise RuntimeError(f"{daemon} {name}: set {text_value}, got {confirmed}")
        return confirmed

    def set_osd_option(self, name, value, remember=True):
        confirmations = []
        for osd_id in self.osd_ids:
            confirmations.append(self.set_option(f"osd.{osd_id}", name, value,
                                                 remember=remember))
        if len(set(confirmations)) != 1:
            raise RuntimeError(f"OSDs disagree on {name}: {confirmations}")
        return confirmations[0]

    def restore_options(self):
        for (daemon, name), value in reversed(list(self.original_options.items())):
            try:
                self.set_option(daemon, name, value, remember=False)
            except Exception as error:  # Preserve the experiment result.
                print(f"warning: failed to restore {daemon} {name}: {error}",
                      file=sys.stderr)

    def validate_cluster(self):
        deadline = time.monotonic() + 60
        while True:
            status = self.ceph_command(["status"], json_output=True)
            mon_count = status.get("monmap", {}).get("num_mons")
            osdmap = status.get("osdmap", {}).get(
                "osdmap", status.get("osdmap", {}))
            osd_up = osdmap.get("num_up_osds")
            active = active_mds_count(status)
            if mon_count == 1 and osd_up == 3 and active == 1:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"benchmark requires 1 MON, 3 up OSDs and 1 active MDS; "
                    f"observed mon={mon_count}, osd_up={osd_up}, "
                    f"active_mds={active}")
            time.sleep(1)
        self.osd_ids = [str(value) for value in
                        self.ceph_command(["osd", "ls"], json_output=True)]
        if len(self.osd_ids) != 3:
            raise RuntimeError(f"expected three OSD ids, got {self.osd_ids}")
        return status

    def quiesce_background(self):
        if self.args.no_quiesce:
            return
        dump = self.ceph_command(["osd", "dump"], json_output=True)
        current = set(str(dump.get("flags", "")).split(","))
        for flag in ("noscrub", "nodeep-scrub", "norecover", "nobackfill"):
            if flag not in current:
                self.ceph_command(["osd", "set", flag])
                self.flags_added.append(flag)

    def restore_background(self):
        for flag in reversed(self.flags_added):
            try:
                self.ceph_command(["osd", "unset", flag])
            except Exception as error:
                print(f"warning: failed to unset {flag}: {error}", file=sys.stderr)

    def metadata(self, status):
        cache = self.build_dir / "CMakeCache.txt"
        cache_text = cache.read_text(errors="replace") if cache.exists() else ""

        def cache_value(name):
            match = re.search(rf"^{re.escape(name)}:[^=]*=(.*)$", cache_text, re.M)
            return match.group(1) if match else None

        cpu_model = None
        with open("/proc/cpuinfo", encoding="utf-8") as stream:
            for line in stream:
                if line.startswith("model name"):
                    cpu_model = line.split(":", 1)[1].strip()
                    break
        mem_total_kb = None
        with open("/proc/meminfo", encoding="utf-8") as stream:
            for line in stream:
                if line.startswith("MemTotal:"):
                    mem_total_kb = int(line.split()[1])
                    break
        osd_store = None
        if self.osd_ids:
            try:
                osd_store = self.get_option(f"osd.{self.osd_ids[0]}",
                                            "osd_objectstore")
            except Exception:
                pass
        data = {
            "commit": self.commit,
            "created_unix_ns": time.time_ns(),
            "hostname": platform.node(),
            "kernel": platform.release(),
            "platform": platform.platform(),
            "cpu_model": cpu_model,
            "logical_cpus": os.cpu_count(),
            "mem_total_kb": mem_total_kb,
            "build_type": cache_value("CMAKE_BUILD_TYPE"),
            "allocator": cache_value("ALLOCATOR"),
            "object_store": osd_store,
            "ceph_version": self.command([self.bin_dir / "ceph-mds", "--version"]).stdout.strip(),
            "cluster_status": status,
            "mds_pid": self.mds_pid,
            "arguments": vars(self.args),
            "client": self.kernel_client_info or {
                "type": self.args.client_type,
                "kernel_release": platform.release(),
                "resolved_fs_name": self.resolved_fs_name,
                "mount_helper": (str(self.mount_ceph)
                                 if self.mount_ceph else None),
                "monitors": self.kernel_monitors,
            },
            "client_preflight": self.kernel_preflight_result,
        }
        # argparse contains Paths, so serialize them explicitly.
        (self.output / "environment.json").write_text(
            json.dumps(data, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8")

    def setup_kernel_client(self):
        if self.args.client_type != "kernel":
            return
        fs_list = self.ceph_command(["fs", "ls"], json_output=True)
        self.resolved_fs_name = resolve_fs_name(fs_list, self.args.fs_name)
        mon_dump = self.ceph_command(["mon", "dump"], json_output=True)
        self.kernel_monitors = kernel_mon_addresses(mon_dump)
        secret = read_keyring_secret(self.build_dir / "keyring")
        fd, filename = tempfile.mkstemp(
            prefix="ceph-dirfrag-kernel-", suffix=".secret", dir="/tmp",
            text=True)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                stream.write(secret + "\n")
        except Exception:
            os.close(fd)
            Path(filename).unlink(missing_ok=True)
            raise
        self.kernel_secret_file = Path(filename)
        helper_version = None
        rpm = shutil.which("rpm")
        if rpm:
            result = run_command([rpm, "-qf", self.mount_ceph], check=False)
            if result.returncode == 0:
                helper_version = result.stdout.strip()
        self.kernel_client_info = {
            "mount_helper": str(self.mount_ceph),
            "mount_helper_package": helper_version,
            "kernel_release": platform.release(),
            "fs_name": self.resolved_fs_name,
            "monitors": self.kernel_monitors,
            "secretfile": str(self.kernel_secret_file),
            "secretfile_mode": oct(self.kernel_secret_file.stat().st_mode & 0o777),
        }

    def cleanup_kernel_client(self):
        if self.kernel_secret_file:
            self.kernel_secret_file.unlink(missing_ok=True)
            self.kernel_secret_file = None

    def mount_client(self, mountpoint):
        mountpoint.mkdir(parents=True, exist_ok=True)
        if is_mounted(mountpoint):
            return
        if self.args.client_type == "kernel":
            if not self.kernel_secret_file:
                raise RuntimeError("kernel client secretfile is not initialized")
            command = kernel_mount_command(
                self.mount_ceph, self.kernel_monitors, mountpoint,
                self.kernel_secret_file, self.resolved_fs_name)
        else:
            command = [self.ceph_fuse, "-c", self.conf, "-n", "client.admin"]
            if self.args.fs_name:
                command += ["--client_fs", self.args.fs_name]
            if mountpoint == self.lookup_mount:
                # Keep the small directory hot in the MDS, but prevent repeated
                # stat calls from being absorbed by the ceph-fuse metadata cache.
                command += ["--client_cache_size=0", "--client_caps_release_delay=0"]
            command.append(mountpoint)
        self.command(command)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if is_mounted(mountpoint):
                return
            time.sleep(0.1)
        raise RuntimeError(
            f"{self.args.client_type} client did not mount {mountpoint}")

    def unmount_client(self, mountpoint):
        if not is_mounted(mountpoint):
            return
        if self.args.client_type == "kernel":
            self.command([self.umount, mountpoint])
        else:
            fusermount = shutil.which("fusermount3") or shutil.which("fusermount")
            if not fusermount:
                raise RuntimeError("fusermount3 or fusermount is required")
            self.command([fusermount, "-u", mountpoint])
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if not is_mounted(mountpoint):
                return
            time.sleep(0.1)
        raise RuntimeError(f"failed to unmount {mountpoint}")

    def kernel_mount_preflight(self, entries=50_000):
        if self.args.client_type != "kernel":
            return
        mountpoint = self.args.kernel_preflight_mount.resolve()
        result = {
            "client_type": "kernel",
            "mountpoint": str(mountpoint),
            "target_entries": entries,
            "client": self.kernel_client_info,
        }
        try:
            self.mount_client(mountpoint)
            details = mount_details(mountpoint)
            if not details or details["fstype"] != "ceph":
                raise RuntimeError(
                    f"kernel preflight expected ceph mount, got {details}")
            target = self.dataset_path(entries, mountpoint)
            target_stat = os.stat(target)
            result.update({
                "mount": details,
                "target": str(target),
                "target_inode": target_stat.st_ino,
                "target_mode": oct(target_stat.st_mode & 0o7777),
                "stat_only": True,
                "success": True,
            })
        except Exception as error:
            result.update({"success": False, "error": str(error)})
            raise
        finally:
            try:
                self.unmount_client(mountpoint)
            finally:
                result["unmounted"] = not is_mounted(mountpoint)
                self.kernel_preflight_result = result
                (self.output / "kernel-mount-preflight.json").write_text(
                    json.dumps(result, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8")
        if not result["unmounted"]:
            raise RuntimeError("kernel preflight mount remained mounted")

    def dataset_path(self, entries, mountpoint=None):
        root = mountpoint or self.mount
        path_entries = getattr(self.args, "dataset_path_entries", None)
        return root / "dirfrag-fetch-data" / f"dir-{path_entries or entries}"

    def prepare_datasets(self, sizes):
        # Dataset creation itself can cross the MDS fragmentation threshold.
        # Enforce and confirm one-dirfrag semantics before mounting a client.
        self.set_option(self.args.mds_target, "mds_bal_fragment_dirs", False)
        self.mount_client(self.mount)
        root = self.mount / "dirfrag-fetch-data"
        if self.args.reuse_existing_cluster_data:
            if not root.is_dir():
                raise RuntimeError(f"reused dataset root is missing: {root}")
        else:
            root.mkdir(exist_ok=True)
        hot = root / "lookup-hot"
        hot_entry = hot / "entry"
        if self.args.reuse_existing_cluster_data:
            if not hot.is_dir() or not hot_entry.is_file():
                raise RuntimeError(
                    f"reused lookup dataset is incomplete: {hot_entry}")
        else:
            hot.mkdir(exist_ok=True)
            hot_entry.touch(exist_ok=True)
        manifest = {}
        for entries in sorted(sizes):
            directory = self.dataset_path(entries)
            index_upper_bound = (
                getattr(self.args, "dataset_path_entries", None) or entries)
            if self.args.reuse_existing_cluster_data:
                if not directory.is_dir():
                    raise RuntimeError(f"reused dataset is missing: {directory}")
            else:
                directory.mkdir(exist_ok=True)
            present, existing_count = scan_dataset_entries(
                directory, index_upper_bound)
            create = dataset_needs_creation(
                existing_count, entries,
                self.args.reuse_existing_cluster_data)
            if create and index_upper_bound != entries:
                raise RuntimeError(
                    "dataset path/index namespace override cannot create entries")
            resume_path = self.output / f"create-resume-{entries}.json"
            resume = {
                "bounded_tasks": self.args.create_workers,
                "completed": existing_count,
                "directory": str(directory),
                "expected": entries,
                "fixed_stride": True,
                "index_upper_bound": index_upper_bound,
                "initial_entries": existing_count,
                "name_pattern": DATASET_ENTRY_RE.pattern,
                "scan_completed": True,
                "started_unix_ns": time.time_ns(),
                "status": "creating" if create else "already-complete",
                "workers": self.args.create_workers,
            }
            resume_path.write_text(
                json.dumps(resume, indent=2, sort_keys=True) + "\n",
                encoding="utf-8")
            if create:
                try:
                    completed = create_dataset_entries(
                        directory, entries, present, self.args.create_workers,
                        progress=lambda count: print(
                            f"prepared {count}/{entries} in {directory}",
                            file=sys.stderr, flush=True))
                except BaseException as error:
                    try:
                        _, checkpoint_entries = scan_dataset_entries(
                            directory, index_upper_bound)
                    except Exception:
                        checkpoint_entries = None
                    resume.update({
                        "completed": checkpoint_entries,
                        "error": f"{type(error).__name__}: {error}",
                        "finished_unix_ns": time.time_ns(),
                        "status": "interrupted" if isinstance(
                            error, KeyboardInterrupt) else "failed",
                    })
                    resume_path.write_text(
                        json.dumps(resume, indent=2, sort_keys=True) + "\n",
                        encoding="utf-8")
                    raise
                resume.update({
                    "completed": completed,
                    "finished_unix_ns": time.time_ns(),
                    "status": "complete",
                })
                resume_path.write_text(
                    json.dumps(resume, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8")
            result = json_from_output(self.command([self.driver, directory]).stdout)
            validate_prepared_dataset(
                result, entries, self.args.expected_dataset_hash)
            filesystem_path = "/" + directory.relative_to(self.mount).as_posix()
            fragment_dump = self.tell(
                self.args.mds_target,
                ["dirfrag", "ls", filesystem_path], json_output=True)
            fragments = dirfrag_count(fragment_dump)
            validate_prepare_fragments(
                fragments, self.args.expected_prepare_fragments)
            manifest[str(entries)] = {
                **result,
                "fragments": fragments,
                "inode": directory.stat().st_ino,
                "path_entries": index_upper_bound,
                "path": filesystem_path,
            }
        self.tell(self.args.mds_target, ["flush", "journal"])
        (self.output / "datasets.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return manifest

    def perf_dump(self):
        return self.tell(self.args.mds_target, ["perf", "dump"], json_output=True)

    def reset_perf(self):
        self.tell(self.args.mds_target, ["perf", "reset", "all"])

    def cache_counts(self, dump=None):
        memory = (dump if dump is not None else self.perf_dump()).get(
            "mds_mem", {})
        return {key: memory.get(key) for key in ("ino", "dir", "dn")}

    def flush_journal_for_respawn(self, sample_dir):
        # The first flush can itself race with a final client-lock UPDATE after
        # unmount. CephFS test_flush.py documents that a second flush then
        # leaves only the new SUBTREEMAP, preventing that UPDATE from replaying
        # dirty target dentries after respawn.
        results = []
        for number in range(1, 3):
            started = time.monotonic()
            attempts = []
            attempts_path = (
                sample_dir / f"flush-journal-{number:02d}-attempts.json"
            )
            while True:
                try:
                    result = self.tell(
                        self.args.mds_target, ["flush", "journal"])
                except CommandFailed as error:
                    attempts.append({
                        "attempt": len(attempts) + 1,
                        "elapsed_seconds": time.monotonic() - started,
                        "success": False,
                        "error": str(error),
                    })
                    attempts_path.write_text(
                        json.dumps(attempts, indent=2, sort_keys=True) + "\n",
                        encoding="utf-8")
                    if time.monotonic() - started >= 120:
                        raise
                    time.sleep(0.5)
                    continue
                attempts.append({
                    "attempt": len(attempts) + 1,
                    "elapsed_seconds": time.monotonic() - started,
                    "success": True,
                })
                attempts_path.write_text(
                    json.dumps(attempts, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8")
                break
            results.append(result)
            (sample_dir / f"flush-journal-{number:02d}.json").write_text(
                str(result).rstrip() + "\n", encoding="utf-8")
        return results

    def capture_pre_respawn_state(self, sample_dir, target_inode,
                                  target_entries):
        # A full dump of a hot 50K+ cache is both intrusive and liable to hit
        # the MDS dump-cache deadline. It is not a coldness proof: only the
        # small post-respawn and post-stat dumps are used for that verdict.
        perf = self.perf_dump()
        counters = self.cache_counts(perf)
        status = self.ceph_command(["status"], json_output=True)
        cache_evidence = {
            "scope": "counters-only",
            "full_cache_dump": False,
            "mds_mem": counters,
            "target_inode": target_inode,
            "target_entries": target_entries,
        }
        (sample_dir / "perf-before-respawn.json").write_text(
            json.dumps(perf, indent=2, sort_keys=True) + "\n",
            encoding="utf-8")
        (sample_dir / "status-before-respawn.json").write_text(
            json.dumps(status, indent=2, sort_keys=True) + "\n",
            encoding="utf-8")
        (sample_dir / "cache-before-respawn.json").write_text(
            json.dumps(cache_evidence, indent=2, sort_keys=True) + "\n",
            encoding="utf-8")
        return perf, counters, status, cache_evidence

    def capture_full_cache(self, sample_dir, filename):
        cache = self.tell(
            self.args.mds_target, ["dump", "cache"], json_output=True)
        (sample_dir / filename).write_text(
            json.dumps(cache, indent=2, sort_keys=True) + "\n",
            encoding="utf-8")
        return cache

    def wait_for_mds_respawn(self, old_pid, timeout_s=120,
                             poll_interval_s=0.5, initial_delay_s=2.0):
        # The admin command waits one second before execv(). Do not mistake
        # the still-running pre-exec daemon for the restarted MDS.
        if initial_delay_s:
            time.sleep(initial_delay_s)
        deadline = time.monotonic() + timeout_s
        last_error = None
        while time.monotonic() < deadline:
            try:
                status = self.ceph_command(["status"], json_output=True)
                pids = self._live_mds_pids()
                if (active_mds_count(status) == 1 and len(pids) == 1 and
                        pids[0] != old_pid):
                    self.mds_pid = pids[0]
                    perf = self.perf_dump()
                    return {
                        "old_pid": old_pid,
                        "new_pid": self.mds_pid,
                        "status": status,
                        "perf_dump": perf,
                    }
                last_error = RuntimeError(
                    f"active_mds={active_mds_count(status)}, live_pids={pids}")
            except Exception as error:
                last_error = error
            time.sleep(poll_interval_s)
        raise RuntimeError(
            "MDS did not return up:active with a working perf dump after "
            f"respawn: {last_error}")

    def respawn_mds_twice(self, sample_dir, target_inode, evidence,
                          evidence_path):
        cache_after = None
        perf_after = None
        for number in range(1, 3):
            old_pid = self.mds_pid
            respawn_command = [self.ceph, "-c", self.conf, "tell",
                               self.args.mds_target, "respawn"]
            respawn_result = self.command(respawn_command, check=False)
            ready = self.wait_for_mds_respawn(old_pid)
            perf_after = ready.pop("perf_dump")
            after = self.cache_counts(perf_after)
            suffix = f"{number:02d}"
            (sample_dir / f"perf-after-respawn-{suffix}.json").write_text(
                json.dumps(perf_after, indent=2, sort_keys=True) + "\n",
                encoding="utf-8")
            (sample_dir / f"status-after-respawn-{suffix}.json").write_text(
                json.dumps(ready["status"], indent=2, sort_keys=True) + "\n",
                encoding="utf-8")
            cache_after = self.capture_full_cache(
                sample_dir, f"cache-after-respawn-{suffix}.json")
            evidence["respawns"].append({
                "number": number,
                "command": {
                    "returncode": respawn_result.returncode,
                    "stdout": respawn_result.stdout,
                    "stderr": respawn_result.stderr,
                },
                "ready": ready,
                "mds_mem": after,
                "target_state": target_cache_state(
                    cache_after, target_inode),
            })
            evidence_path.write_text(
                json.dumps(evidence, indent=2, sort_keys=True) + "\n",
                encoding="utf-8")
        return cache_after, perf_after

    def cold_mount(self, target, target_inode, target_entries, sample_dir,
                   mode, batch, delay, configured_before):
        self.unmount_client(self.mount)
        flush_results = self.flush_journal_for_respawn(sample_dir)
        _, before, status_before, cache_before = (
            self.capture_pre_respawn_state(
                sample_dir, target_inode, target_entries))

        initial_pid = self.mds_pid
        evidence = {
            "target": str(target),
            "target_inode": target_inode,
            "target_entries": target_entries,
            "flush_journal": flush_results,
            "before": before,
            "cache_before_respawn": cache_before,
            "status_before_respawn": status_before,
            "configured_before_respawn": configured_before,
            "respawns": [],
        }
        evidence_path = sample_dir / "mds-respawn.json"
        evidence_path.write_text(
            json.dumps(evidence, indent=2, sort_keys=True) + "\n",
            encoding="utf-8")

        cache_after, perf_after = self.respawn_mds_twice(
            sample_dir, target_inode, evidence, evidence_path)

        evidence["after"] = evidence["respawns"][-1]["mds_mem"]
        evidence["target_state_after_respawn"] = (
            evidence["respawns"][-1]["target_state"])
        # Stable aliases retain compatibility with earlier result readers.
        (sample_dir / "perf-after-respawn.json").write_text(
            json.dumps(perf_after, indent=2, sort_keys=True) + "\n",
            encoding="utf-8")
        (sample_dir / "cache-after-respawn.json").write_text(
            json.dumps(cache_after, indent=2, sort_keys=True) + "\n",
            encoding="utf-8")
        try:
            validation = validate_respawn_cache(
                cache_after, target_inode, allow_empty_inode=True)
            evidence["validation_after_respawn"] = validation
        except Exception as error:
            evidence["validation_after_respawn_error"] = str(error)
            evidence_path.write_text(
                json.dumps(evidence, indent=2, sort_keys=True) + "\n",
                encoding="utf-8")
            raise

        # execv restores config-file defaults. Reapply and read back every MDS
        # and OSD setting before any client can fetch the target dirfrag.
        configured_after = self.configure_condition(
            mode, batch, delay, remember=False)
        confirmed_after = self.confirm_after(mode)
        if configured_after != confirmed_after:
            raise RuntimeError(
                "configuration reapplication failed after MDS respawn: "
                f"set={configured_after}, read={confirmed_after}")
        evidence["configured_after_respawn"] = configured_after
        evidence["confirmed_after_respawn"] = confirmed_after
        evidence_path.write_text(
            json.dumps(evidence, indent=2, sort_keys=True) + "\n",
            encoding="utf-8")

        self.mount_client(self.mount)
        mounted_client = mount_details(self.mount)
        if not mounted_client:
            raise RuntimeError(f"client mount disappeared: {self.mount}")
        expected_fstype = "ceph" if self.args.client_type == "kernel" else "fuse.ceph-fuse"
        if mounted_client["fstype"] != expected_fstype:
            raise RuntimeError(
                f"unexpected client mount type: {mounted_client['fstype']}, "
                f"expected {expected_fstype}")
        # Resolve every ancestor but do not open the target directory.  The
        # perf reset below then isolates one cold fetch of the target dirfrag.
        os.stat(target)
        perf_after_stat = self.perf_dump()
        after_stat = self.cache_counts(perf_after_stat)
        cache_after_stat = self.capture_full_cache(
            sample_dir, "cache-after-stat.json")
        evidence["after_stat"] = after_stat
        evidence["target_state_after_stat"] = target_cache_state(
            cache_after_stat, target_inode)
        evidence_path.write_text(
            json.dumps(evidence, indent=2, sort_keys=True) + "\n",
            encoding="utf-8")
        try:
            stat_validation = validate_respawn_cache(
                cache_after_stat, target_inode, after_stat=True)
            evidence["validation_after_stat"] = stat_validation
        except Exception as error:
            evidence["validation_after_stat_error"] = str(error)
            evidence_path.write_text(
                json.dumps(evidence, indent=2, sort_keys=True) + "\n",
                encoding="utf-8")
            raise
        evidence_path.write_text(
            json.dumps(evidence, indent=2, sort_keys=True) + "\n",
            encoding="utf-8")
        return {
            "before": before,
            "after": evidence["after"],
            "after_stat": after_stat,
            "target_inode": target_inode,
            "old_mds_pid": initial_pid,
            "new_mds_pid": self.mds_pid,
            "respawns": evidence["respawns"],
            "validation_after_respawn": validation,
            "validation_after_stat": stat_validation,
            "configured_after_respawn": configured_after,
            "client_mount": mounted_client,
        }

    def start_perf_stat(self, output):
        perf = shutil.which("perf")
        if not perf:
            if self.args.allow_missing_perf:
                return None
            raise RuntimeError("perf is not installed (or pass --allow-missing-perf)")
        command = [perf, "stat", "-x", ",", "-o", output,
                   "-e", "task-clock,cycles,instructions,context-switches",
                   "-p", str(self.mds_pid)]
        self._log_command(command)
        process = subprocess.Popen(command, stdout=subprocess.DEVNULL,
                                   stderr=subprocess.PIPE, text=True)
        time.sleep(0.05)
        if process.poll() is not None:
            error = process.stderr.read()
            if self.args.allow_missing_perf:
                return None
            raise RuntimeError(f"perf stat failed to attach: {error}")
        return process

    def stop_perf_stat(self, process):
        if not process:
            return
        process.send_signal(signal.SIGINT)
        try:
            _, error = process.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            _, error = process.communicate()
        if process.returncode not in (0, -signal.SIGINT):
            if not self.args.allow_missing_perf:
                raise RuntimeError(f"perf stat failed: {error}")

    @staticmethod
    def parse_perf_stat(path):
        result = {}
        if not path.exists():
            return result
        with path.open(encoding="utf-8") as stream:
            for raw in stream:
                if not raw or raw.startswith("#"):
                    continue
                columns = raw.rstrip().split(",")
                if len(columns) < 3 or columns[0].startswith("<"):
                    continue
                try:
                    value = float(columns[0].replace(" ", ""))
                except ValueError:
                    continue
                unit = columns[1].strip()
                event = columns[2].strip()
                if event == "task-clock":
                    if unit == "msec":
                        result["mds_task_clock_ms"] = value
                    elif unit == "sec":
                        result["mds_task_clock_ms"] = value * 1000
                elif event == "cycles":
                    result["mds_cycles"] = value
                elif event == "instructions":
                    result["mds_instructions"] = value
                elif event == "context-switches":
                    result["mds_context_switches"] = value
        return result

    def should_profile(self, mode, entries, batch, delay, round_number, phase):
        if not self.args.profile_match or phase != "measure":
            return False
        actual = f"{mode},{entries},{batch},{delay},{round_number}"
        return actual == self.args.profile_match

    def start_profile(self, output):
        perf = shutil.which("perf")
        if not perf:
            raise RuntimeError("perf is required for --profile-match")
        command = [perf, "record", "-F", str(self.args.profile_frequency),
                   "-g", "-p", str(self.mds_pid), "-o", output]
        self._log_command(command)
        process = subprocess.Popen(command, stdout=subprocess.DEVNULL,
                                   stderr=subprocess.PIPE, text=True)
        time.sleep(0.05)
        if process.poll() is not None:
            raise RuntimeError(f"perf record failed: {process.stderr.read()}")
        return process

    def finish_profile(self, process, data_path):
        process.send_signal(signal.SIGINT)
        _, error = process.communicate(timeout=30)
        if process.returncode not in (0, -signal.SIGINT):
            raise RuntimeError(f"perf record failed: {error}")
        perf_script = data_path.with_suffix(".script")
        with perf_script.open("w", encoding="utf-8") as stream:
            result = subprocess.run(["perf", "script", "-i", data_path],
                                    stdout=stream, stderr=subprocess.PIPE,
                                    text=True)
        if result.returncode:
            raise RuntimeError(f"perf script failed: {result.stderr}")
        if not self.args.flamegraph_tools:
            return
        collapse = self.args.flamegraph_tools / "stackcollapse-perf.pl"
        render = self.args.flamegraph_tools / "flamegraph.pl"
        if not collapse.exists() or not render.exists():
            raise RuntimeError("--flamegraph-tools lacks stackcollapse-perf.pl/flamegraph.pl")
        folded = data_path.with_suffix(".folded")
        svg = data_path.with_suffix(".svg")
        with perf_script.open(encoding="utf-8") as source, \
                folded.open("w", encoding="utf-8") as destination:
            result = subprocess.run([collapse], stdin=source, stdout=destination,
                                    stderr=subprocess.PIPE, text=True)
        if result.returncode:
            raise RuntimeError(f"stackcollapse-perf.pl failed: {result.stderr}")
        with folded.open(encoding="utf-8") as source, \
                svg.open("w", encoding="utf-8") as destination:
            result = subprocess.run([render, "--title", "CDir OMAP decode"],
                                    stdin=source, stdout=destination,
                                    stderr=subprocess.PIPE, text=True)
        if result.returncode:
            raise RuntimeError(f"flamegraph.pl failed: {result.stderr}")

    def start_lookup(self, sample_dir):
        self.mount_client(self.lookup_mount)
        hot = self.lookup_mount / "dirfrag-fetch-data" / "lookup-hot" / "entry"
        os.stat(hot)
        stop_file = sample_dir / "lookup.stop"
        start_file = sample_dir / "lookup.start"
        ready_file = sample_dir / "lookup.ready"
        summary = sample_dir / "lookup-summary.json"
        samples = sample_dir / "lookup-latencies.csv"
        command = [sys.executable, self.lookup_driver, "--path", hot,
                   "--rate", str(self.args.lookup_rate), "--stop-file", stop_file,
                   "--start-file", start_file,
                   "--ready-file", ready_file, "--summary", summary,
                   "--samples", samples]
        self._log_command(command)
        process = subprocess.Popen([str(part) for part in command],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   text=True)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if ready_file.exists():
                return process, start_file, stop_file, summary
            if process.poll() is not None:
                stdout, stderr = process.communicate()
                raise RuntimeError(f"lookup load exited early: {stdout}\n{stderr}")
            time.sleep(0.01)
        process.terminate()
        raise RuntimeError("lookup load did not become ready")

    @staticmethod
    def start_lookup_measurement(state):
        if state:
            state[1].touch()

    @staticmethod
    def signal_lookup(state):
        if state:
            state[2].touch()

    @staticmethod
    def finish_lookup(state):
        if not state:
            return {}
        process, _, _, summary = state
        stdout, stderr = process.communicate(timeout=15)
        if process.returncode:
            raise RuntimeError(f"lookup load failed: {stdout}\n{stderr}")
        return json.loads(summary.read_text(encoding="utf-8"))

    def configure_condition(self, mode, batch, delay, remember=True):
        confirmations = {}
        confirmations["fragment_dirs"] = self.set_option(
            self.args.mds_target, "mds_bal_fragment_dirs", False,
            remember=remember)
        if mode != "legacy-parent":
            confirmations["mode"] = self.set_option(
                self.args.mds_target, "mds_dir_fetch_pipelined", mode == "true",
                remember=remember)
        else:
            confirmations["mode"] = "unavailable"
        confirmations["keys"] = self.set_option(
            self.args.mds_target, "mds_dir_keys_per_op", batch,
            remember=remember)
        confirmations["osd_omap_limit"] = self.set_osd_option(
            "osd_max_omap_entries_per_request", 1024, remember=remember)
        confirmations["probability"] = self.set_osd_option(
            "osd_debug_inject_dispatch_delay_probability", 1 if delay else 0,
            remember=remember)
        confirmations["duration"] = self.set_osd_option(
            "osd_debug_inject_dispatch_delay_duration", delay / 1000.0,
            remember=remember)
        return confirmations

    def extract_counters(self, dump, legacy=False):
        mds = dump.get("mds", {})
        fetch_sum, fetch_count = time_counter(mds["dir_fetch_latency"]) \
            if "dir_fetch_latency" in mds else (None, 0)
        decode_sum, decode_count = time_counter(mds["dir_fetch_decode_latency"]) \
            if "dir_fetch_decode_latency" in mds else (None, 0)
        batch_sum, batch_count = time_counter(mds["dir_fetch_batch_latency"]) \
            if "dir_fetch_batch_latency" in mds else (None, 0)
        return {
            # The v16 parent predates dir_fetch_complete.  Its dir_fetch
            # counter is reset immediately before the one target traversal,
            # so use it only as the legacy sample's completion guard.
            "dir_fetch_complete": (
                mds.get("dir_fetch") if legacy
                else mds.get("dir_fetch_complete")),
            "dir_fetch_latency_ms": fetch_sum * 1000 if fetch_sum is not None else None,
            "fetch_latency_samples": fetch_count,
            "dir_fetch_decode_latency_ms": decode_sum * 1000 if decode_sum is not None else None,
            "decode_latency_samples": decode_count,
            "dir_fetch_batch_latency_mean_ms": (
                batch_sum * 1000 / batch_count if batch_count else None),
            "dir_fetch_batch_latency_sum_ms": (
                batch_sum * 1000 if batch_sum is not None else None),
            "dir_fetch_batches": mds.get("dir_fetch_batches"),
            "dir_fetch_omap_bytes": mds.get("dir_fetch_omap_bytes"),
            "dir_fetch_peak_omap_bytes": mds.get("dir_fetch_peak_omap_bytes"),
            "mds_requests": mds.get("request"),
            "batch_latency_samples": batch_count,
        }

    def confirm_after(self, mode):
        if mode == "legacy-parent":
            mode_value = "unavailable"
        else:
            mode_value = self.get_option(self.args.mds_target,
                                         "mds_dir_fetch_pipelined")
        probabilities = [self.get_option(f"osd.{osd_id}",
                                         "osd_debug_inject_dispatch_delay_probability")
                         for osd_id in self.osd_ids]
        durations = [self.get_option(f"osd.{osd_id}",
                                     "osd_debug_inject_dispatch_delay_duration")
                     for osd_id in self.osd_ids]
        limits = [self.get_option(f"osd.{osd_id}",
                                  "osd_max_omap_entries_per_request")
                  for osd_id in self.osd_ids]
        if (len(set(probabilities)) != 1 or len(set(durations)) != 1 or
                len(set(limits)) != 1):
            raise RuntimeError("OSD OMAP/delay configuration changed during sample")
        return {
            "fragment_dirs": self.get_option(
                self.args.mds_target, "mds_bal_fragment_dirs"),
            "mode": mode_value,
            "keys": self.get_option(self.args.mds_target, "mds_dir_keys_per_op"),
            "osd_omap_limit": limits[0],
            "probability": probabilities[0],
            "duration": durations[0],
        }

    def run_sample(self, workload, entries, batch, delay, mode, phase,
                   round_number, manifest):
        self.execution_order += 1
        sample_name = (f"{self.execution_order:05d}-{phase}-{workload}-n{entries}-"
                       f"b{batch}-d{delay}-{mode}")
        sample_dir = self.output / "raw" / sample_name
        sample_dir.mkdir(parents=True)
        target = self.dataset_path(entries)
        configured = self.configure_condition(mode, batch, delay)
        expected = manifest[str(entries)]
        cache = self.cold_mount(
            target, expected["inode"], entries, sample_dir,
            mode, batch, delay, configured)
        configured = cache["configured_after_respawn"]

        lookup_state = None
        if workload == "concurrent":
            lookup_state = self.start_lookup(sample_dir)
        self.reset_perf()

        stat_path = sample_dir / "perf-stat.csv"
        stat_process = None
        profile_process = None
        profile_path = sample_dir / "perf.data"
        rss = None
        driver_result = None
        try:
            stat_process = self.start_perf_stat(stat_path)
            if self.should_profile(mode, entries, batch, delay, round_number, phase):
                profile_process = self.start_profile(profile_path)
            rss = RssSampler(self.mds_pid, sample_dir / "rss.csv")
            rss.start()
            self.start_lookup_measurement(lookup_state)
            driver_result = json_from_output(
                self.command([self.driver, target]).stdout)
        finally:
            self.signal_lookup(lookup_state)
            if rss:
                rss_start, rss_peak = rss.stop()
            try:
                self.stop_perf_stat(stat_process)
            finally:
                if profile_process:
                    self.finish_profile(profile_process, profile_path)
            lookup_summary = self.finish_lookup(lookup_state)

        perf_dump = self.perf_dump()
        (sample_dir / "perf-dump.json").write_text(
            json.dumps(perf_dump, indent=2, sort_keys=True) + "\n",
            encoding="utf-8")
        confirmed = self.confirm_after(mode)
        if confirmed != configured:
            raise RuntimeError(
                f"configuration changed during sample: before={configured}, "
                f"after={confirmed}")
        counters = self.extract_counters(
            perf_dump, legacy=(mode == "legacy-parent"))
        if all(counters.get(name) is not None for name in
               ("dir_fetch_batch_latency_sum_ms",
                "dir_fetch_decode_latency_ms", "dir_fetch_latency_ms")):
            counters["dir_fetch_estimated_overlap_ms"] = (
                counters["dir_fetch_batch_latency_sum_ms"] +
                counters["dir_fetch_decode_latency_ms"] -
                counters["dir_fetch_latency_ms"])
        perf_values = self.parse_perf_stat(stat_path)
        required_perf = {
            "mds_task_clock_ms", "mds_cycles", "mds_instructions",
            "mds_context_switches",
        }
        missing_perf = required_perf - set(perf_values)
        if missing_perf and not self.args.allow_missing_perf:
            raise RuntimeError(f"perf stat did not report {sorted(missing_perf)}")

        if driver_result["entries"] != entries or driver_result["hash"] != expected["hash"]:
            raise RuntimeError(
                f"incorrect traversal: {driver_result}, expected entries/hash {expected}")
        if counters["dir_fetch_complete"] != 1:
            raise RuntimeError(
                f"sample triggered {counters['dir_fetch_complete']} complete fetches, expected 1")
        if mode != "legacy-parent":
            if counters["fetch_latency_samples"] != 1:
                raise RuntimeError(
                    "fetch latency sample count is not exactly one: "
                    f"{counters['fetch_latency_samples']}")
            if counters["decode_latency_samples"] != 1:
                raise RuntimeError(
                    "decode latency sample count is not exactly one: "
                    f"{counters['decode_latency_samples']}")
            if counters["batch_latency_samples"] != counters["dir_fetch_batches"]:
                raise RuntimeError("batch latency sample count does not match dir_fetch_batches")
            for name in ("dir_fetch_latency_ms", "dir_fetch_decode_latency_ms",
                         "dir_fetch_batch_latency_sum_ms"):
                if counters[name] is None or counters[name] <= 0:
                    raise RuntimeError(f"{name} must be non-zero")
            expected_mode = "true" if mode == "true" else "false"
            if expected_mode not in confirmed["mode"]:
                raise RuntimeError(f"mode changed during fetch: {confirmed['mode']}")
        if (workload == "concurrent" and
                (counters["mds_requests"] or 0) < lookup_summary.get("operations", 0)):
            raise RuntimeError(
                "lookup load was served from the client cache: "
                f"MDS requests={counters['mds_requests']}, "
                f"lookup operations={lookup_summary.get('operations')}")

        row = {
            "commit": self.commit,
            "client_type": self.args.client_type,
            "mode": mode,
            "workload": workload,
            "directory_entries": entries,
            "requested_batch": batch,
            "effective_batch": min(batch, 1024),
            "dispatch_delay_ms": delay,
            "phase": phase,
            "round": round_number,
            "execution_order": self.execution_order,
            "client_latency_ms": driver_result["elapsed_ns"] / 1_000_000,
            "entries": driver_result["entries"],
            "hash": driver_result["hash"],
            **{key: counters.get(key) for key in SAMPLE_FIELDS if key in counters},
            "rss_start_kb": rss_start,
            "rss_peak_kb": rss_peak,
            "rss_delta_kb": rss_peak - rss_start,
            **perf_values,
            "lookup_throughput_ops_s": lookup_summary.get("throughput_ops_s"),
            "lookup_latency_p50_ms": (
                lookup_summary.get("latency_p50_ns") / 1_000_000
                if lookup_summary.get("latency_p50_ns") is not None else None),
            "lookup_latency_p95_ms": (
                lookup_summary.get("latency_p95_ns") / 1_000_000
                if lookup_summary.get("latency_p95_ns") is not None else None),
            "lookup_latency_p99_ms": (
                lookup_summary.get("latency_p99_ns") / 1_000_000
                if lookup_summary.get("latency_p99_ns") is not None else None),
            "mds_pid": self.mds_pid,
            "mode_confirmed": confirmed["mode"],
            "fragment_dirs_confirmed": confirmed["fragment_dirs"],
            "keys_per_op_confirmed": confirmed["keys"],
            "osd_omap_limit_confirmed": confirmed["osd_omap_limit"],
            "dispatch_probability_confirmed": confirmed["probability"],
            "dispatch_duration_confirmed": confirmed["duration"],
            "sample_dir": str(sample_dir.relative_to(self.output)),
            "client_mount_fstype": cache["client_mount"]["fstype"],
            "client_mount_source": cache["client_mount"]["source"],
        }
        details = {
            "sample": row,
            "configured_before": configured,
            "confirmed_after": confirmed,
            "cache": cache,
            "driver": driver_result,
            "counters": counters,
            "perf_stat": perf_values,
            "lookup": lookup_summary,
        }
        (sample_dir / "sample.json").write_text(
            json.dumps(details, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        self.append_sample(row)
        print(f"sample {self.execution_order}: {mode} n={entries} b={batch} "
              f"delay={delay}ms client={row['client_latency_ms']:.3f}ms",
              file=sys.stderr)
        return row

    def append_sample(self, row):
        exists = self.samples_csv.exists() and self.samples_csv.stat().st_size > 0
        with self.samples_csv.open("a", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=SAMPLE_FIELDS,
                                    extrasaction="ignore")
            if not exists:
                writer.writeheader()
            writer.writerow(row)

    def calibrate_delay(self, manifest):
        if self.args.legacy or self.args.skip_delay_calibration:
            return
        observed = {}
        for delay in (0, 1, 5):
            row = self.run_sample("cold", 10_000, 128, delay, "true",
                                  "calibration", delay, manifest)
            observed[str(delay)] = row["dir_fetch_batch_latency_mean_ms"]
        baseline = observed["0"]
        failures = []
        for delay in (1, 5):
            shift = observed[str(delay)] - baseline
            if shift < delay * 0.5:
                failures.append(
                    f"{delay} ms injection shifted mean batch latency by only {shift:.3f} ms")
        result = {"mean_batch_latency_ms": observed, "failures": failures}
        (self.output / "delay-calibration.json").write_text(
            json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        if failures:
            raise RuntimeError("dispatch-delay calibration failed: " + "; ".join(failures))

    def run_conditions(self, conditions, manifest, repetitions, warmups):
        rng = random.Random(self.args.seed)
        conditions = list(dict.fromkeys(conditions))
        rng.shuffle(conditions)
        if self.args.legacy:
            mode_values = ["legacy-parent"]
        elif self.args.mode == "both":
            mode_values = None
        else:
            mode_values = [self.args.mode]
        for workload, entries, batch, delay in conditions:
            for phase, count in (("warmup", warmups), ("measure", repetitions)):
                if mode_values is None:
                    order = ["true" if value else "false"
                             for value in ab_order(count, rng)]
                else:
                    order = mode_values * count
                rounds = {"false": 0, "true": 0, "legacy-parent": 0}
                for mode in order:
                    round_number = rounds[mode]
                    rounds[mode] += 1
                    self.run_sample(workload, entries, batch, delay, mode, phase,
                                    round_number, manifest)

    def analyze(self):
        analyzer = Path(__file__).with_name("analyze.py")
        self.command([sys.executable, analyzer, self.samples_csv,
                      "--output", self.output / "analysis"])

    def execute(self, conditions, repetitions, warmups):
        try:
            status = self.validate_cluster()
            self.quiesce_background()
            if self.args.client_type == "kernel":
                self.setup_kernel_client()
                if not self.args.skip_kernel_preflight:
                    requested_sizes = {
                        entries for _, entries, _, _ in conditions
                    }
                    if 50_000 not in requested_sizes:
                        raise RuntimeError(
                            "kernel preflight requires the retained "
                            "50000-entry dataset")
                    self.kernel_mount_preflight(50_000)
            self.metadata(status)
            if self.args.kernel_preflight_only:
                return
            self.set_option(self.args.mds_target, "mds_bal_fragment_dirs", False)
            sizes = {10_000, 100_000, 1_000_000}
            if self.args.only_required_sizes:
                sizes = {entries for _, entries, _, _ in conditions}
                if not self.args.legacy and not self.args.skip_delay_calibration:
                    sizes.add(10_000)
            manifest = self.prepare_datasets(sizes)
            if self.args.prepare_only:
                return
            self.calibrate_delay(manifest)
            self.run_conditions(conditions, manifest, repetitions, warmups)
            self.analyze()
        finally:
            for mountpoint in (self.mount, self.lookup_mount,
                               self.args.kernel_preflight_mount.resolve()):
                try:
                    self.unmount_client(mountpoint)
                except Exception as error:
                    print(f"warning: failed to unmount {mountpoint}: {error}",
                          file=sys.stderr)
            self.restore_options()
            self.restore_background()
            self.cleanup_kernel_client()


def start_vstart(args, source_dir):
    build_dir = args.build_dir.resolve()
    environment = os.environ.copy()
    environment.update({
        "CEPH_NUM_MON": "1", "CEPH_NUM_OSD": "3", "CEPH_NUM_MDS": "1",
        "CEPH_NUM_MGR": "1", "CEPH_NUM_FS": "1", "CEPH_MAX_MDS": "1",
    })
    command = [source_dir / "src" / "vstart.sh"]
    if not args.reuse_existing_cluster_data:
        command.append("-n")
    command.append("-d")
    action = "reusing" if args.reuse_existing_cluster_data else "starting"
    print(f"{action} 1 MON / 3 OSD / 1 MDS vstart cluster", file=sys.stderr)
    run_command(command, cwd=build_dir, env=environment, capture=False)


def stop_vstart(args, source_dir):
    try:
        run_command([source_dir / "src" / "stop.sh"], cwd=args.build_dir.resolve(),
                    capture=False)
    except Exception as error:
        print(f"warning: failed to stop vstart cluster: {error}", file=sys.stderr)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Controlled vstart benchmark for pipelined CDir OMAP fetches")
    parser.add_argument("--build-dir", type=Path, default=Path("build"))
    parser.add_argument("--conf", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mount", type=Path,
                        default=Path("/tmp/cephfs-dirfrag-fetch-client"))
    parser.add_argument("--lookup-mount", type=Path,
                        default=Path("/tmp/cephfs-dirfrag-lookup-client"))
    parser.add_argument("--driver", type=Path)
    parser.add_argument("--mds-target", default="mds.0")
    parser.add_argument("--mds-pid", type=int)
    parser.add_argument("--fs-name")
    parser.add_argument("--client-type", choices=("fuse", "kernel"),
                        default="fuse",
                        help="CephFS client implementation (default: fuse)")
    parser.add_argument("--kernel-preflight-mount", type=Path,
                        default=Path("/tmp/cephfs-dirfrag-kernel-preflight"))
    parser.add_argument("--kernel-preflight-only", action="store_true",
                        help="validate one kernel mount/stat/unmount and exit")
    parser.add_argument("--skip-kernel-preflight", action="store_true",
                        help="skip in-run preflight after a separate successful one")
    parser.add_argument("--commit")
    parser.add_argument("--matrix", action="append",
                        choices=("all", "core", "scale", "concurrency",
                                 "parent", "smoke", "preliminary"), default=[])
    parser.add_argument("--mode", choices=("both", "false", "true"),
                        default="both",
                        help="pipeline modes to sample (default: both)")
    parser.add_argument("--preliminary-entries", type=int, default=100_000,
                        help="directory size for --matrix preliminary")
    parser.add_argument(
        "--dataset-path-entries", type=int,
        help=("use dir-N and validate entry indexes against N while keeping "
              "the single condition's expected entry count unchanged"))
    parser.add_argument("--repetitions", type=int)
    parser.add_argument("--warmups", type=int)
    parser.add_argument("--seed", type=int, default=454572)
    parser.add_argument("--lookup-rate", type=float, default=1000.0)
    parser.add_argument("--legacy", action="store_true",
                        help="parent commit: do not set/read the pipeline option")
    parser.add_argument("--only-required-sizes", action="store_true")
    parser.add_argument("--skip-delay-calibration", action="store_true")
    parser.add_argument("--allow-missing-perf", action="store_true")
    parser.add_argument("--create-workers", type=int, default=1,
                        help="fixed-stride dataset creation workers (1-32)")
    parser.add_argument("--prepare-only", action="store_true",
                        help="prepare and validate datasets, then stop before samples")
    parser.add_argument("--expected-prepare-fragments", type=int, default=1,
                        help="required leaf dirfrags during dataset preparation")
    parser.add_argument("--expected-dataset-hash",
                        help="required 128-bit hexadecimal preparation digest")
    parser.add_argument("--no-quiesce", action="store_true")
    parser.add_argument("--profile-match",
                        help="mode,entries,batch,delay_ms,round (for example true,100000,1024,5,0)")
    parser.add_argument("--profile-frequency", type=int, default=499)
    parser.add_argument("--flamegraph-tools", type=Path)
    parser.add_argument("--start-cluster", action="store_true")
    parser.add_argument("--keep-cluster", action="store_true")
    parser.add_argument("--reuse-existing-cluster-data", action="store_true",
                        help="restart vstart without -n and only validate datasets")
    args = parser.parse_args()
    if args.repetitions is not None and args.repetitions <= 0:
        parser.error("--repetitions must be positive")
    if args.warmups is not None and args.warmups < 0:
        parser.error("--warmups cannot be negative")
    if args.profile_frequency <= 0:
        parser.error("--profile-frequency must be positive")
    if args.preliminary_entries <= 0:
        parser.error("--preliminary-entries must be positive")
    if args.dataset_path_entries is not None:
        if args.dataset_path_entries <= 0:
            parser.error("--dataset-path-entries must be positive")
        if args.preliminary_entries > args.dataset_path_entries:
            parser.error(
                "--dataset-path-entries cannot be smaller than "
                "--preliminary-entries")
        if not args.reuse_existing_cluster_data:
            parser.error(
                "--dataset-path-entries requires --reuse-existing-cluster-data")
        if not args.only_required_sizes:
            parser.error("--dataset-path-entries requires --only-required-sizes")
        if args.matrix != ["preliminary"]:
            parser.error(
                "--dataset-path-entries requires exactly one "
                "--matrix preliminary")
    if args.create_workers <= 0 or args.create_workers > 32:
        parser.error("--create-workers must be between 1 and 32")
    if args.expected_prepare_fragments <= 0:
        parser.error("--expected-prepare-fragments must be positive")
    if args.expected_prepare_fragments != 1 and not args.prepare_only:
        parser.error(
            "--expected-prepare-fragments other than 1 requires --prepare-only")
    if args.expected_dataset_hash is not None:
        args.expected_dataset_hash = args.expected_dataset_hash.lower()
        if not re.fullmatch(r"[0-9a-f]{32}", args.expected_dataset_hash):
            parser.error("--expected-dataset-hash must be 32 hexadecimal digits")
    if args.legacy and args.mode != "both":
        parser.error("--mode cannot be used with --legacy")
    if ((args.kernel_preflight_only or args.skip_kernel_preflight) and
            args.client_type != "kernel"):
        parser.error("kernel preflight options require --client-type kernel")
    if args.kernel_preflight_only and args.skip_kernel_preflight:
        parser.error("--kernel-preflight-only conflicts with --skip-kernel-preflight")
    return args


def main():
    args = parse_args()
    source_dir = source_for_build(args.build_dir.resolve(),
                                  Path(__file__).resolve().parents[4])
    matrices = args.matrix or (["parent"] if args.legacy else ["core"])
    if "all" in matrices:
        matrices = ["core", "scale", "concurrency"]
    if (args.preliminary_entries != 100_000 and
            matrices != ["preliminary"]):
        raise SystemExit(
            "--preliminary-entries requires exactly --matrix preliminary")
    if args.legacy and any(name != "parent" for name in matrices):
        raise SystemExit("--legacy is only valid with --matrix parent")
    conditions = []
    for name in matrices:
        conditions.extend(matrix_conditions(name, args.preliminary_entries))
    conditions = list(dict.fromkeys(conditions))
    repetitions = args.repetitions
    if repetitions is None:
        repetitions = 10 if args.legacy else (1 if matrices == ["smoke"] else 20)
    warmups = args.warmups
    if warmups is None:
        warmups = 0 if matrices == ["smoke"] else 3

    started = False
    if args.start_cluster:
        start_vstart(args, source_dir)
        started = True
    try:
        benchmark = DirfragBenchmark(args)
        benchmark.execute(conditions, repetitions, warmups)
    finally:
        if started and not args.keep_cluster:
            stop_vstart(args, source_dir)


if __name__ == "__main__":
    main()
