#!/usr/bin/env python3

import tempfile
import unittest
import sys
import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import analyze
import run


class TestExperimentTools(unittest.TestCase):
    def sample(self, mode, order, latency, commit="head"):
        peak = 100 if mode == "true" else 1000
        return {
            "commit": commit,
            "client_type": "fuse",
            "mode": mode,
            "workload": "cold",
            "directory_entries": "100000",
            "effective_batch": "1024",
            "dispatch_delay_ms": "0",
            "phase": "measure",
            "round": str(order),
            "execution_order": str(order),
            "entries": "100000",
            "hash": "abc",
            "dir_fetch_complete": "1",
            "dir_fetch_batches": "98",
            "dir_fetch_omap_bytes": "1000",
            "dir_fetch_peak_omap_bytes": str(peak),
            "dir_fetch_batch_latency_sum_ms": str(latency / 3),
            "client_latency_ms": str(latency),
            "mds_task_clock_ms": str(latency / 2),
        }

    def test_ab_order_is_balanced_in_mirrored_blocks(self):
        order = run.ab_order(20, run.random.Random(7))
        self.assertEqual(order.count(False), 20)
        self.assertEqual(order.count(True), 20)
        for offset in range(0, len(order), 4):
            self.assertIn(order[offset:offset + 4],
                          ([False, True, True, False],
                           [True, False, False, True]))
        self.assertEqual({tuple(order[offset:offset + 4])
                          for offset in range(0, len(order), 4)},
                         {(False, True, True, False),
                         (True, False, False, True)})

    def test_client_type_defaults_to_fuse_and_accepts_kernel(self):
        with mock.patch.object(sys, "argv", ["run.py", "--output", "out"]):
            args = run.parse_args()
            self.assertEqual(args.client_type, "fuse")
            self.assertFalse(args.prepare_only)
            self.assertEqual(args.expected_prepare_fragments, 1)
            self.assertIsNone(args.dataset_path_entries)
        with mock.patch.object(sys, "argv", [
                "run.py", "--output", "out", "--client-type", "kernel"]):
            self.assertEqual(run.parse_args().client_type, "kernel")
        with mock.patch.object(sys, "argv", [
                "run.py", "--output", "out", "--kernel-preflight-only"]):
            with self.assertRaises(SystemExit):
                run.parse_args()

    def test_multiple_prepare_fragments_require_prepare_only(self):
        with mock.patch.object(sys, "argv", [
                "run.py", "--output", "out",
                "--expected-prepare-fragments", "16"]):
            with self.assertRaises(SystemExit):
                run.parse_args()
        with mock.patch.object(sys, "argv", [
                "run.py", "--output", "out", "--prepare-only",
                "--expected-prepare-fragments", "16",
                "--expected-dataset-hash",
                "2A10A95928CED1198A4AB2F3DB37BB60"]):
            args = run.parse_args()
            self.assertTrue(args.prepare_only)
            self.assertEqual(args.expected_prepare_fragments, 16)
            self.assertEqual(
                args.expected_dataset_hash,
                "2a10a95928ced1198a4ab2f3db37bb60")

    def test_kernel_mon_and_fs_resolution(self):
        dump = {"mons": [{"public_addrs": {"addrvec": [
            {"type": "v2", "addr": "v2:10.0.0.1:3300/0"},
            {"type": "v1", "addr": "v1:10.0.0.1:6789/0"},
        ]}}]}
        self.assertEqual(run.kernel_mon_addresses(dump),
                         ["10.0.0.1:6789"])
        self.assertEqual(run.resolve_fs_name([{"name": "cephfs"}]),
                         "cephfs")
        with self.assertRaisesRegex(RuntimeError, "one filesystem"):
            run.resolve_fs_name([{"name": "a"}, {"name": "b"}])

    def test_kernel_secret_and_mount_command_never_embed_secret(self):
        with tempfile.TemporaryDirectory() as directory:
            keyring = Path(directory) / "keyring"
            keyring.write_text(
                "[client.admin]\n\tkey = sensitive-value\n",
                encoding="utf-8")
            self.assertEqual(run.read_keyring_secret(keyring),
                             "sensitive-value")
            secretfile = Path(directory) / "secret"
            command = run.kernel_mount_command(
                Path("/usr/sbin/mount.ceph"), ["10.0.0.1:6789"],
                Path("/mnt/test"), secretfile, "cephfs")
        rendered = " ".join(str(value) for value in command)
        self.assertIn(f"secretfile={secretfile}", rendered)
        self.assertIn("mds_namespace=cephfs", rendered)
        self.assertNotIn("sensitive-value", rendered)

    def test_kernel_mount_and_unmount_use_only_kernel_helpers(self):
        calls = []
        benchmark = SimpleNamespace(
            args=SimpleNamespace(client_type="kernel"),
            kernel_secret_file=Path("/tmp/test-secret"),
            mount_ceph=Path("/usr/sbin/mount.ceph"),
            umount=Path("/usr/bin/umount"),
            kernel_monitors=["10.0.0.1:6789"],
            resolved_fs_name="cephfs",
            command=lambda command: calls.append(command),
        )
        with tempfile.TemporaryDirectory() as directory:
            mountpoint = Path(directory) / "mount"
            with mock.patch.object(run, "is_mounted",
                                   side_effect=[False, True]):
                run.DirfragBenchmark.mount_client(benchmark, mountpoint)
            with mock.patch.object(run, "is_mounted",
                                   side_effect=[True, False]):
                run.DirfragBenchmark.unmount_client(benchmark, mountpoint)
        self.assertEqual(calls[0][0], Path("/usr/sbin/mount.ceph"))
        self.assertEqual(calls[1], [Path("/usr/bin/umount"), mountpoint])
        self.assertFalse(any("ceph-fuse" in str(value)
                             for command in calls for value in command))

    def test_preliminary_matrix_is_one_requested_condition(self):
        self.assertEqual(run.matrix_conditions("preliminary"),
                         [("cold", 100_000, 1024, 0)])
        self.assertEqual(run.matrix_conditions("preliminary", 100),
                         [("cold", 100, 1024, 0)])

    def test_dataset_path_override_is_explicit_and_strictly_constrained(self):
        valid = [
            "run.py", "--output", "out", "--matrix", "preliminary",
            "--preliminary-entries", "988140",
            "--dataset-path-entries", "1000000",
            "--reuse-existing-cluster-data", "--only-required-sizes",
        ]
        with mock.patch.object(sys, "argv", valid):
            args = run.parse_args()
        self.assertEqual(args.dataset_path_entries, 1_000_000)
        self.assertFalse(args.start_cluster)

        required_options = (
            "--reuse-existing-cluster-data", "--only-required-sizes")
        for option in required_options:
            with self.subTest(missing=option), mock.patch.object(
                    sys, "argv", [value for value in valid if value != option]):
                with self.assertRaises(SystemExit):
                    run.parse_args()
        with mock.patch.object(sys, "argv", [
                "run.py", "--output", "out", "--matrix", "core",
                "--dataset-path-entries", "1000000",
                "--reuse-existing-cluster-data", "--only-required-sizes"]):
            with self.assertRaises(SystemExit):
                run.parse_args()
        with mock.patch.object(sys, "argv", [
                "run.py", "--output", "out", "--matrix", "preliminary",
                "--preliminary-entries", "1000001",
                "--dataset-path-entries", "1000000",
                "--reuse-existing-cluster-data", "--only-required-sizes"]):
            with self.assertRaises(SystemExit):
                run.parse_args()

    def test_dataset_path_override_keeps_condition_namespace_separate(self):
        benchmark = SimpleNamespace(
            args=SimpleNamespace(dataset_path_entries=1_000_000),
            mount=Path("/mount"))
        self.assertEqual(
            run.DirfragBenchmark.dataset_path(benchmark, 988_140),
            Path("/mount/dirfrag-fetch-data/dir-1000000"))
        benchmark.args.dataset_path_entries = None
        self.assertEqual(
            run.DirfragBenchmark.dataset_path(benchmark, 988_140),
            Path("/mount/dirfrag-fetch-data/dir-988140"))

    def test_skipped_kernel_preflight_does_not_require_50000_dataset(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            benchmark = SimpleNamespace(
                args=SimpleNamespace(
                    client_type="kernel",
                    skip_kernel_preflight=True,
                    kernel_preflight_only=False,
                    kernel_preflight_mount=root / "preflight",
                    mds_target="mds.0",
                    only_required_sizes=True,
                    legacy=False,
                    skip_delay_calibration=True,
                    prepare_only=False,
                ),
                mount=root / "client",
                lookup_mount=root / "lookup",
                validate_cluster=lambda: {},
                quiesce_background=lambda: None,
                setup_kernel_client=lambda: None,
                metadata=lambda status: None,
                set_option=lambda *args, **kwargs: None,
                prepare_datasets=lambda sizes: {},
                calibrate_delay=lambda manifest: None,
                run_conditions=lambda *args: None,
                analyze=lambda: None,
                unmount_client=lambda mountpoint: None,
                restore_options=lambda: None,
                restore_background=lambda: None,
                cleanup_kernel_client=lambda: None,
            )
            run.DirfragBenchmark.execute(
                benchmark, [("cold", 100_000, 1024, 0)], 1, 0)

    def test_prepare_only_skips_calibration_samples_and_analysis(self):
        calls = []
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            benchmark = SimpleNamespace(
                args=SimpleNamespace(
                    client_type="fuse", skip_kernel_preflight=False,
                    kernel_preflight_only=False,
                    kernel_preflight_mount=root / "preflight",
                    mds_target="mds.0", only_required_sizes=True,
                    legacy=False, skip_delay_calibration=True,
                    prepare_only=True,
                ),
                mount=root / "client", lookup_mount=root / "lookup",
                validate_cluster=lambda: {},
                quiesce_background=lambda: None,
                metadata=lambda status: None,
                set_option=lambda *args, **kwargs: None,
                prepare_datasets=lambda sizes: calls.append(
                    ("prepare", sizes)) or {"ok": True},
                calibrate_delay=lambda manifest: calls.append(("calibrate",)),
                run_conditions=lambda *args: calls.append(("samples",)),
                analyze=lambda: calls.append(("analyze",)),
                unmount_client=lambda mountpoint: None,
                restore_options=lambda: None,
                restore_background=lambda: None,
                cleanup_kernel_client=lambda: None,
            )
            run.DirfragBenchmark.execute(
                benchmark, [("cold", 1_000_000, 1024, 0)], 20, 3)
        self.assertEqual(calls, [("prepare", {1_000_000})])

    def test_prepare_validation_accepts_expected_16_fragments_and_hash(self):
        digest = "2a10a95928ced1198a4ab2f3db37bb60"
        run.validate_prepared_dataset(
            {"entries": 1_000_000, "hash": digest}, 1_000_000, digest)
        run.validate_prepare_fragments(16, 16)
        with self.assertRaisesRegex(RuntimeError, "dirfrags"):
            run.validate_prepare_fragments(1, 16)

    def test_dirfrag_count_accepts_admin_socket_shape(self):
        dump = {"frags": [{"value": 0, "bits": 0, "str": "0/0"}]}
        self.assertEqual(run.dirfrag_count(dump), 1)
        with self.assertRaises(RuntimeError):
            run.dirfrag_count({"unexpected": []})

    def test_respawn_cache_requires_absent_target_then_empty_stat_inode(self):
        cache_dump = {"inodes": [{"ino": 1}, {"ino": "0x64"}]}
        result = run.validate_respawn_cache(cache_dump, target_inode=200)
        self.assertFalse(result["present"])
        self.assertEqual(run.cached_inode_numbers(cache_dump), {1, 100})
        after_stat = {"inodes": [{"ino": 200, "dirfrags": []}]}
        result = run.validate_respawn_cache(
            after_stat, target_inode=200, after_stat=True)
        self.assertTrue(result["present"])
        self.assertEqual(result["dentry_count"], 0)

    def test_respawn_cache_rejects_resident_or_preloaded_target(self):
        with self.assertRaisesRegex(RuntimeError, "journal replay"):
            run.validate_respawn_cache(
                {"inodes": [{"ino": 200, "dirfrags": []}]}, 200)
        with self.assertRaisesRegex(RuntimeError, "loaded before readdir"):
            run.validate_respawn_cache(
                {"inodes": [{"ino": 200, "dirfrags": [
                    {"states": ["auth", "complete"], "dentries": [{"x": 1}]},
                ]}]}, 200, after_stat=True)

    def test_final_respawn_allows_only_empty_inode_metadata(self):
        empty = {"inodes": [{"ino": 200, "states": ["auth"],
                              "pins": {}, "dirfrags": []}]}
        result = run.validate_respawn_cache(
            empty, 200, allow_empty_inode=True)
        self.assertTrue(result["present"])
        self.assertEqual(result["dirfrag_states"], [])
        with self.assertRaisesRegex(RuntimeError, "dirfrag/dentry cache"):
            run.validate_respawn_cache(
                {"inodes": [{"ino": 200, "dirfrags": [
                    {"states": ["auth"], "pins": {}, "dentries": []},
                ]}]}, 200, allow_empty_inode=True)

    def test_wait_for_respawn_requires_active_mds_pid_and_perf(self):
        statuses = iter([
            {"fsmap": {"by_rank": [{"status": "up:active"}]}},
            {"fsmap": {"by_rank": [{"status": "up:active"}]}},
        ])
        pids = iter([[99], [123]])
        benchmark = SimpleNamespace(
            mds_pid=99,
            ceph_command=lambda *args, **kwargs: next(statuses),
            _live_mds_pids=lambda: next(pids),
            perf_dump=lambda: {"mds_mem": {"ino": 13}},
        )
        result = run.DirfragBenchmark.wait_for_mds_respawn(
            benchmark, old_pid=99, timeout_s=1, poll_interval_s=0,
            initial_delay_s=0)
        self.assertEqual(result["old_pid"], 99)
        self.assertEqual(result["new_pid"], 123)
        self.assertEqual(benchmark.mds_pid, 123)

    def test_two_respawns_chain_new_pids_and_save_each_stage(self):
        waits = []
        cache_files = []
        benchmark = SimpleNamespace(
            mds_pid=100,
            ceph=Path("/build/bin/ceph"),
            conf=Path("/build/ceph.conf"),
            args=SimpleNamespace(mds_target="mds.0"),
            command=lambda *args, **kwargs: SimpleNamespace(
                returncode=0, stdout="Respawning!", stderr=""),
            cache_counts=lambda perf: dict(perf["mds_mem"]),
            capture_full_cache=lambda sample_dir, filename: (
                cache_files.append(filename) or []),
        )
        next_pids = iter([101, 102])

        def wait(old_pid):
            waits.append(old_pid)
            benchmark.mds_pid = next(next_pids)
            return {
                "old_pid": old_pid,
                "new_pid": benchmark.mds_pid,
                "status": {"fsmap": {"by_rank": [
                    {"status": "up:active"}]}},
                "perf_dump": {"mds_mem": {"ino": 14, "dir": 12, "dn": 11}},
            }

        benchmark.wait_for_mds_respawn = wait
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            evidence = {"respawns": []}
            cache, perf = run.DirfragBenchmark.respawn_mds_twice(
                benchmark, output, 200, evidence, output / "respawn.json")
            self.assertEqual(cache, [])
            self.assertEqual(perf["mds_mem"]["ino"], 14)
            self.assertTrue((output / "perf-after-respawn-01.json").exists())
            self.assertTrue((output / "status-after-respawn-02.json").exists())
        self.assertEqual(waits, [100, 101])
        self.assertEqual(benchmark.mds_pid, 102)
        self.assertEqual(cache_files, ["cache-after-respawn-01.json",
                                       "cache-after-respawn-02.json"])
        self.assertEqual(len(evidence["respawns"]), 2)

    def test_respawn_reapplication_sets_all_options_without_remembering(self):
        calls = []
        benchmark = SimpleNamespace(
            args=SimpleNamespace(mds_target="mds.0"),
            osd_ids=["0", "1", "2"],
            set_option=lambda daemon, name, value, remember=True: (
                calls.append(("set", daemon, name, value, remember)) or
                str(value).lower()),
            set_osd_option=lambda name, value, remember=True: (
                calls.append(("osd", name, value, remember)) or
                str(value).lower()),
        )
        configured = run.DirfragBenchmark.configure_condition(
            benchmark, "false", 1024, 0, remember=False)
        self.assertEqual(configured["mode"], "false")
        self.assertIn(("set", "mds.0", "mds_bal_fragment_dirs", False, False),
                      calls)
        self.assertTrue(all(call[-1] is False for call in calls))

    def test_single_mode_runs_once_per_repetition(self):
        calls = []
        benchmark = SimpleNamespace(
            args=SimpleNamespace(seed=7, legacy=False, mode="true"),
            run_sample=lambda *args: calls.append(args),
        )
        run.DirfragBenchmark.run_conditions(
            benchmark, run.matrix_conditions("preliminary"), {}, 1, 0)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][4:7], ("true", "measure", 0))

    def test_reuse_dataset_never_enters_creation(self):
        self.assertFalse(run.dataset_needs_creation(50_000, 50_000, True))
        with self.assertRaisesRegex(RuntimeError, "reused dataset"):
            run.dataset_needs_creation(0, 50_000, True)
        self.assertTrue(run.dataset_needs_creation(0, 50_000, False))
        self.assertTrue(run.dataset_needs_creation(17, 50_000, False))

    def test_strict_dataset_scan_and_parallel_resume(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            for index in (0, 2, 7):
                (directory / f"entry-{index:016d}").touch()
            present, count = run.scan_dataset_entries(directory, 8)
            self.assertEqual(count, 3)
            self.assertEqual([index for index, value in enumerate(present) if value],
                             [0, 2, 7])
            progress = []
            completed = run.create_dataset_entries(
                directory, 8, present, 4, progress.append)
            self.assertEqual(completed, 8)
            rescanned, count = run.scan_dataset_entries(directory, 8)
            self.assertEqual(count, 8)
            self.assertTrue(all(rescanned))
            self.assertEqual(progress, [])

    def test_sparse_high_index_is_valid_in_larger_path_namespace(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            for index in (0, 999_999):
                (directory / f"entry-{index:016d}").touch()
            present, count = run.scan_dataset_entries(directory, 1_000_000)
            self.assertEqual(count, 2)
            self.assertTrue(present[0])
            self.assertTrue(present[999_999])
            self.assertFalse(run.dataset_needs_creation(count, 2, True))

    def test_partial_dataset_count_and_hash_are_both_required(self):
        digest = "1831597691d9924262114f70f4a317cf"
        result = {"entries": 988_140, "hash": digest}
        run.validate_prepared_dataset(result, 988_140, digest)
        with self.assertRaisesRegex(RuntimeError, "failed to prepare"):
            run.validate_prepared_dataset(result, 988_139, digest)
        with self.assertRaisesRegex(RuntimeError, "does not match"):
            run.validate_prepared_dataset(result, 988_140, "0" * 32)

    def test_dataset_scan_rejects_invalid_entries(self):
        cases = (("bad-name", b""), ("entry-0000000000000002", b""),
                 ("entry-0000000000000000", b"not-empty"))
        for name, contents in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary)
                (directory / name).write_bytes(contents)
                with self.assertRaises(RuntimeError):
                    run.scan_dataset_entries(directory, 2)

    def test_dataset_scan_rejects_symlink(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            target = directory / "target"
            target.touch()
            os.symlink(target, directory / "entry-0000000000000000")
            with self.assertRaisesRegex(RuntimeError, "regular zero-length"):
                run.scan_dataset_entries(directory, 1)

    def test_eexist_requires_regular_zero_length_file(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "entry-0000000000000000"
            path.touch()
            self.assertFalse(run.create_empty_dataset_entry(path))
            path.write_text("x", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "regular zero-length"):
                run.create_empty_dataset_entry(path)

    def test_prepare_disables_fragmentation_before_mount(self):
        calls = []

        class StopBeforeMount(RuntimeError):
            pass

        benchmark = SimpleNamespace(
            args=SimpleNamespace(mds_target="mds.0"),
            set_option=lambda *args: calls.append(("set", args)) or "false",
            mount_client=lambda *args: (
                calls.append(("mount", args)) or (_ for _ in ()).throw(
                    StopBeforeMount())),
            mount=Path("/mount"),
        )
        with self.assertRaises(StopBeforeMount):
            run.DirfragBenchmark.prepare_datasets(benchmark, {1})
        self.assertEqual(calls[0], (
            "set", ("mds.0", "mds_bal_fragment_dirs", False)))
        self.assertEqual(calls[1][0], "mount")

    def test_reuse_vstart_omits_new_flag(self):
        args = SimpleNamespace(
            build_dir=Path("/build"), reuse_existing_cluster_data=True)
        with mock.patch.object(run, "run_command") as invoked:
            run.start_vstart(args, Path("/source"))
        command = invoked.call_args.args[0]
        self.assertEqual(command, [Path("/source/src/vstart.sh"), "-d"])

    def test_perf_reset_explicitly_resets_all_counters(self):
        calls = []
        benchmark = SimpleNamespace(
            args=SimpleNamespace(mds_target="mds.0"),
            tell=lambda *args, **kwargs: calls.append((args, kwargs)),
        )
        run.DirfragBenchmark.reset_perf(benchmark)
        self.assertEqual(calls, [(("mds.0", ["perf", "reset", "all"]), {})])

    def test_respawn_flushes_journal_twice(self):
        calls = []
        benchmark = SimpleNamespace(
            args=SimpleNamespace(mds_target="mds.0"),
            tell=lambda *args, **kwargs: (
                calls.append((args, kwargs)) or f"result-{len(calls)}"),
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            results = run.DirfragBenchmark.flush_journal_for_respawn(
                benchmark, output)
            self.assertEqual(
                (output / "flush-journal-01.json").read_text(), "result-1\n")
            self.assertEqual(
                (output / "flush-journal-02.json").read_text(), "result-2\n")
        self.assertEqual(results, ["result-1", "result-2"])
        self.assertEqual(calls, [
            (("mds.0", ["flush", "journal"]), {}),
            (("mds.0", ["flush", "journal"]), {}),
        ])

    def test_respawn_flush_retries_transient_command_failure(self):
        calls = []

        def tell(*args, **kwargs):
            calls.append((args, kwargs))
            if len(calls) == 1:
                raise run.CommandFailed("MDS is temporarily laggy")
            return f"result-{len(calls)}"

        benchmark = SimpleNamespace(
            args=SimpleNamespace(mds_target="mds.0"),
            tell=tell,
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            with mock.patch.object(run.time, "sleep") as sleep:
                results = run.DirfragBenchmark.flush_journal_for_respawn(
                    benchmark, output)
            attempts = json.loads(
                (output / "flush-journal-01-attempts.json").read_text())
        self.assertEqual(results, ["result-2", "result-3"])
        self.assertEqual(len(calls), 3)
        sleep.assert_called_once_with(0.5)
        self.assertEqual([item["success"] for item in attempts],
                         [False, True])
        self.assertIn("temporarily laggy", attempts[0]["error"])

    def test_pre_respawn_capture_is_counters_only_and_post_is_full(self):
        calls = []
        benchmark = SimpleNamespace(
            args=SimpleNamespace(mds_target="mds.0"),
            perf_dump=lambda: {"mds_mem": {"ino": 50017, "dir": 15,
                                            "dn": 50014}},
            cache_counts=lambda perf: dict(perf["mds_mem"]),
            ceph_command=lambda *args, **kwargs: (
                calls.append(("ceph", args, kwargs)) or {"fsmap": {}}),
            tell=lambda *args, **kwargs: (
                calls.append(("tell", args, kwargs)) or [{"ino": 1}]),
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            _, counters, _, evidence = (
                run.DirfragBenchmark.capture_pre_respawn_state(
                    benchmark, output, 200, 50_000))
            self.assertEqual(counters["dn"], 50014)
            self.assertEqual(evidence["scope"], "counters-only")
            self.assertFalse(evidence["full_cache_dump"])
            self.assertFalse(any(call[0] == "tell" for call in calls))
            run.DirfragBenchmark.capture_full_cache(
                benchmark, output, "cache-after-respawn.json")
            run.DirfragBenchmark.capture_full_cache(
                benchmark, output, "cache-after-stat.json")
            self.assertEqual(sum(call[0] == "tell" for call in calls), 2)
            self.assertTrue((output / "cache-after-respawn.json").exists())
            self.assertTrue((output / "cache-after-stat.json").exists())

    def test_summary_comparison_and_correctness(self):
        rows = [self.sample("false", i, value)
                for i, value in enumerate((10, 11, 12, 13))]
        rows += [self.sample("true", i + 4, value)
                 for i, value in enumerate((8, 9, 10, 11))]
        self.assertEqual(analyze.validate_correctness(rows), [])
        summaries = analyze.build_summaries(rows, 100)
        comparisons = analyze.build_comparisons(summaries)
        latency = next(item for item in comparisons
                       if item["metric"] == "client_latency_ms")
        self.assertLess(latency["relative_change_percent"], 0)
        peak = next(item for item in comparisons
                    if item["metric"] == "dir_fetch_peak_omap_bytes")
        self.assertEqual(peak["relative_change_percent"], -90)

        comparisons = analyze.build_comparisons(summaries, rows, 100)
        latency = next(item for item in comparisons
                       if item["metric"] == "client_latency_ms")
        self.assertIsNotNone(latency["relative_change_ci95_low"])
        self.assertLessEqual(latency["relative_change_ci95_low"],
                             latency["relative_change_percent"])
        self.assertGreaterEqual(latency["relative_change_ci95_high"],
                                latency["relative_change_percent"])
        batch_sum = next(item for item in summaries
                         if item["metric"] ==
                         "dir_fetch_batch_latency_sum_ms")
        self.assertEqual(batch_sum["bootstrap_iterations"], 100)

    def test_read_samples_infers_fuse_and_derives_client_residuals(self):
        row = self.sample("false", 1, 10)
        row.pop("client_type")
        row["dir_fetch_latency_ms"] = "4"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "samples.csv"
            import csv
            with path.open("w", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(stream, fieldnames=list(row))
                writer.writeheader()
                writer.writerow(row)
            loaded = analyze.read_samples([path])[0]
        self.assertEqual(loaded["client_type"], "fuse")
        self.assertEqual(loaded["fetch_fraction_percent"], 40)
        self.assertEqual(loaded["nonfetch_residual_ms"], 6)

    def test_client_comparison_is_separate_run_descriptive(self):
        fuse = [self.sample("false", i, value)
                for i, value in enumerate((10, 11, 12, 13))]
        kernel = [self.sample("false", i + 4, value)
                  for i, value in enumerate((2, 3, 4, 5))]
        for row in kernel:
            row["client_type"] = "kernel"
        rows = fuse + kernel
        summaries = analyze.build_summaries(rows, 100)
        comparisons = analyze.build_client_comparisons(
            summaries, rows, 100)
        latency = next(item for item in comparisons
                       if item["metric"] == "client_latency_ms")
        self.assertLess(latency["kernel_relative_to_fuse_percent"], 0)
        self.assertIn("separate client runs", latency["comparison_scope"])

    def test_extract_counters_preserves_latency_counts(self):
        dump = {"mds": {
            "dir_fetch_latency": {"sum": 1.0, "avgcount": 1},
            "dir_fetch_decode_latency": {"sum": 0.8, "avgcount": 1},
            "dir_fetch_batch_latency": {"sum": 0.2, "avgcount": 49},
            "dir_fetch_complete": 1,
            "dir_fetch_batches": 49,
        }}
        counters = run.DirfragBenchmark.extract_counters(None, dump)
        self.assertEqual(counters["fetch_latency_samples"], 1)
        self.assertEqual(counters["decode_latency_samples"], 1)
        self.assertEqual(counters["batch_latency_samples"], 49)

    def test_extract_counters_uses_v16_parent_fetch_as_legacy_guard(self):
        dump = {"mds": {"dir_fetch": 1}}
        counters = run.DirfragBenchmark.extract_counters(
            None, dump, legacy=True)
        self.assertEqual(counters["dir_fetch_complete"], 1)
        counters = run.DirfragBenchmark.extract_counters(None, dump)
        self.assertIsNone(counters["dir_fetch_complete"])

    def test_paired_sensitivity_uses_mirrored_block_neighbors(self):
        rows = [self.sample(mode, order, latency)
                for order, (mode, latency) in enumerate((
                    ("false", 10), ("true", 8),
                    ("true", 9), ("false", 12)), start=1)]
        paired = analyze.build_paired_sensitivity(rows, 100)
        latency = next(item for item in paired
                       if item["metric"] == "client_latency_ms")
        self.assertEqual(latency["complete_blocks"], 1)
        self.assertEqual(latency["pairs"], 2)
        self.assertEqual(latency["difference_mean"], -2.5)
        self.assertEqual(latency["relative_pairs"], 2)

    def test_outlier_flags_are_descriptive_and_retained(self):
        rows = [self.sample("false", order, latency)
                for order, latency in enumerate((10, 10, 10, 100), start=1)]
        flags = analyze.build_outlier_flags(rows)
        latency = next(item for item in flags
                       if item["metric"] == "client_latency_ms")
        self.assertEqual(latency["value"], 100)
        self.assertTrue(latency["included_in_analysis"])

    def test_correctness_detects_work_mismatch(self):
        rows = [self.sample("false", 0, 10), self.sample("true", 1, 9)]
        rows[1]["dir_fetch_omap_bytes"] = "999"
        self.assertTrue(any("dir_fetch_omap_bytes" in error
                            for error in analyze.validate_correctness(rows)))

    def test_parent_gate_accepts_overlapping_confidence_intervals(self):
        rows = [self.sample("false", i, value)
                for i, value in enumerate((10, 12, 14, 16))]
        rows += [self.sample("legacy-parent", i + 4, value, commit="parent")
                 for i, value in enumerate((11, 13, 15, 17))]
        summaries = analyze.build_summaries(rows, 100)
        comparisons = analyze.build_parent_comparisons(summaries)
        latency = next(item for item in comparisons
                       if item["metric"] == "client_latency_ms")
        self.assertTrue(latency["passes_5pct_or_ci_overlap"])
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "report.md"
            analyze.write_report(report, rows, [], comparisons, [], [])
            self.assertIn("Parent versus HEAD disabled",
                          report.read_text(encoding="utf-8"))

    def test_perf_stat_csv_parser(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "perf.csv"
            path.write_text(
                "12.5,msec,task-clock,1,100.00,,\n"
                "1000,,cycles,1,100.00,,\n"
                "2000,,instructions,1,100.00,,\n"
                "3,,context-switches,1,100.00,,\n",
                encoding="utf-8")
            parsed = run.DirfragBenchmark.parse_perf_stat(path)
        self.assertEqual(parsed["mds_task_clock_ms"], 12.5)
        self.assertEqual(parsed["mds_cycles"], 1000)
        self.assertEqual(parsed["mds_instructions"], 2000)
        self.assertEqual(parsed["mds_context_switches"], 3)


if __name__ == "__main__":
    unittest.main()
