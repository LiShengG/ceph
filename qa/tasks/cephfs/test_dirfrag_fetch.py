"""
Tests for the batched fetch of a dirfrag from its RADOS object.

Reading a large dirfrag takes several omap_get_vals() round trips: the OSD
clamps each one to osd_max_omap_entries_per_request, so a fragment with more
keys than that arrives in pieces.  These tests drive that multi-batch path and
check that the fragment the MDS ends up with matches what was written,
with the pipelined decode both enabled (the default) and disabled.
"""

import logging
from io import StringIO

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.fuse_mount import FuseMount

log = logging.getLogger(__name__)


class TestDirfragFetch(CephFSTestCase):
    # A standby is needed by the failover-mid-fetch test; the extra daemons are
    # harmless (idle standbys) for the rest.
    MDSS_REQUIRED = 2

    # Small enough that a few hundred files span many batches, so the tests
    # stay quick while still exercising the cursor across batches.
    KEYS_PER_OP = 16
    NFILES = 200

    def _configure(self, **kwargs):
        for k, v in kwargs.items():
            self.config_set('mds', str(k), str(v))

    def setUp(self):
        super(TestDirfragFetch, self).setUp()
        # Keep the directory in a single fragment: we want to test batching
        # within one dirfrag, not fragmentation.
        self._configure(mds_bal_fragment_dirs=False,
                        mds_dir_keys_per_op=self.KEYS_PER_OP)

    def _dir_fetch_count(self):
        return self.fs.rank_asok(['perf', 'dump', 'mds'])['mds']['dir_fetch_complete']

    def _dir_fetch_metrics(self):
        return self.fs.rank_asok(['perf', 'dump', 'mds'])['mds']

    def _dir_fetch_batches(self):
        return self._dir_fetch_metrics()['dir_fetch_batches']

    def _wait_mds_config(self, key, expected):
        """Wait until the active rank has consumed a monitor config update."""
        if isinstance(expected, bool):
            expected = str(expected).lower()
        else:
            expected = str(expected)

        def current():
            out = self.fs.rank_asok(['config', 'get', key])
            return str(out[key]).lower()

        self.wait_until_equal(current, expected, timeout=30, period=0.1)

    def _path_to_underlying_ino(self, path):
        ino = self.mount_a.path_to_ino(path)
        if isinstance(self.mount_a, FuseMount):
            # fuse_ll.cc's FINO_INO stores the Ceph inode in the low 48 bits;
            # snapshot st_ino values carry a per-snapshot stag above them.
            return ino & ((1 << 48) - 1)
        return ino

    def _drop_caches(self):
        """
        Force the next access to re-read the dirfrag from RADOS: flush the
        journal so everything is on disk, then drop the MDS cache and the
        client's cache.
        """
        self.mount_a.umount_wait()
        self.fs.flush()
        self.fs.rank_tell(['cache', 'drop'])
        self.mount_a.mount_wait()

    def _create_files(self, dirname, count, prefix="file"):
        self.mount_a.run_shell(["mkdir", "-p", dirname])
        self.mount_a.run_python(f"""
import os
d = os.path.join("{self.mount_a.hostfs_mntpt}", "{dirname}")
for i in range({count}):
    open(os.path.join(d, "{prefix}_%06d" % i), "w").close()
""")

    def _listdir(self, dirname):
        out = self.mount_a.run_shell(["ls", "-1", dirname]).stdout.getvalue()
        return sorted(x for x in out.split("\n") if x)

    def _check_readdir_after_fetch(self, dirname, expected):
        before = self._dir_fetch_count()
        self._drop_caches()
        listing = self._listdir(dirname)
        after = self._dir_fetch_count()

        # The listing must be exactly what we wrote -- no entries dropped by
        # the batching, none duplicated.
        self.assertEqual(len(listing), len(expected),
                         "listed {0} entries, expected {1}".format(
                             len(listing), len(expected)))
        self.assertEqual(listing, expected)

        # And it must have come off disk, otherwise the test proved nothing.
        self.assertGreater(after, before,
                           "dirfrag was served from cache, fetch path untested")

    # ------------------------------------------------------------------
    # helpers for the failure / race / fragmentation tests below
    # ------------------------------------------------------------------

    def _dirfrag_oid(self, dirname, frag="00000000"):
        """RADOS object name of a dirfrag, e.g. '10000000001.00000000'."""
        ino = self.mount_a.path_to_ino(dirname)
        return "{0:x}.{1}".format(ino, frag)

    def _dirfrag_keys(self, oid):
        """The omap keys of a dirfrag object, as written on disk."""
        out = self.fs.radosmo(["listomapkeys", oid], stdout=StringIO())
        if isinstance(out, bytes):
            out = out.decode("utf-8")
        return [k for k in out.split("\n") if k] if out else []

    def _damage_ls(self, rank=0):
        return self.fs.rank_asok(["damage", "ls"], rank=rank)

    def _offline_setomapval(self, oid, key, value):
        """
        Overwrite an omap value directly on disk, the way test_damage.py does:
        unmount, flush the journal so the dirfrag is on disk, fail the rank,
        scribble on the object, then bring everything back.
        """
        self.mount_a.umount_wait()
        self.fs.flush()
        self.fs.fail()
        self.fs.radosm(["setomapval", oid, key, value])
        self.fs.set_joinable()
        self.fs.wait_for_daemons()
        self.mount_a.mount_wait()

    def test_multi_batch_fetch_pipelined(self):
        """
        That a dirfrag spanning many omap batches is read back intact with
        the pipelined decode enabled (the default).
        """
        self._configure(mds_dir_fetch_pipelined=True)

        dirname = "pipelined"
        self._create_files(dirname, self.NFILES)
        expected = self._listdir(dirname)
        self.assertEqual(len(expected), self.NFILES)

        self._check_readdir_after_fetch(dirname, expected)

    def test_multi_batch_fetch_not_pipelined(self):
        """
        That the same dirfrag reads back identically with the pipelined
        decode disabled, i.e. the kill switch really restores the old path.
        """
        self._configure(mds_dir_fetch_pipelined=False)

        dirname = "not_pipelined"
        self._create_files(dirname, self.NFILES)
        expected = self._listdir(dirname)
        self.assertEqual(len(expected), self.NFILES)

        self._check_readdir_after_fetch(dirname, expected)

    def test_both_modes_agree(self):
        """
        That both modes produce the same listing for the same directory.
        """
        dirname = "agree"
        self._create_files(dirname, self.NFILES)
        expected = self._listdir(dirname)

        self._configure(mds_dir_fetch_pipelined=True)
        self._drop_caches()
        pipelined = self._listdir(dirname)

        self._configure(mds_dir_fetch_pipelined=False)
        self._drop_caches()
        buffered = self._listdir(dirname)

        self.assertEqual(pipelined, expected)
        self.assertEqual(buffered, expected)

    def test_full_fetch_perf_counters(self):
        """That one cold fetch reports comparable work in both modes."""
        dirname = "perf_counters"
        self._create_files(dirname, self.NFILES)
        expected = self._listdir(dirname)

        def fetch_metrics(pipelined):
            self._configure(mds_dir_fetch_pipelined=pipelined)
            self._drop_caches()
            # Resolve the target inode before reset without opening its
            # dirfrag, so the listing below is the only complete fetch.
            self.mount_a.run_shell(["stat", dirname])
            self.fs.rank_asok(["perf", "reset", "all"])
            self.assertEqual(self._listdir(dirname), expected)
            return self.fs.rank_asok(["perf", "dump", "mds"])["mds"]

        buffered = fetch_metrics(False)
        pipelined = fetch_metrics(True)

        for metrics in (buffered, pipelined):
            self.assertEqual(metrics["dir_fetch_complete"], 1)
            self.assertGreater(metrics["dir_fetch_batches"], 1)
            self.assertGreater(metrics["dir_fetch_omap_bytes"], 0)
            self.assertEqual(metrics["dir_fetch_latency"]["avgcount"], 1)
            self.assertEqual(metrics["dir_fetch_decode_latency"]["avgcount"], 1)
            self.assertEqual(metrics["dir_fetch_batch_latency"]["avgcount"],
                             metrics["dir_fetch_batches"])

        # Loading a cold dirfrag's fnode establishes the version baseline; it
        # must not look like a concurrent commit and make the buffered path
        # discard and re-read its first batch.
        self.assertEqual(buffered["dir_fetch_batches"],
                         pipelined["dir_fetch_batches"])
        self.assertEqual(buffered["dir_fetch_omap_bytes"],
                         pipelined["dir_fetch_omap_bytes"])
        self.assertEqual(buffered["dir_fetch_peak_omap_bytes"],
                         buffered["dir_fetch_omap_bytes"])
        self.assertLess(pipelined["dir_fetch_peak_omap_bytes"],
                        pipelined["dir_fetch_omap_bytes"])

    def test_single_batch_fetch(self):
        """
        That a dirfrag small enough to arrive in one batch still works -- the
        pipeline must not require a second batch to finish the fetch.
        """
        self._configure(mds_dir_fetch_pipelined=True,
                        mds_dir_keys_per_op=1024)

        dirname = "single_batch"
        count = 10
        self._create_files(dirname, count)
        expected = self._listdir(dirname)
        self.assertEqual(len(expected), count)

        self._check_readdir_after_fetch(dirname, expected)

    def test_empty_dir_fetch(self):
        """
        That fetching an empty dirfrag completes.  The batch handler has to
        cope with a reply that carries no keys at all.
        """
        self._configure(mds_dir_fetch_pipelined=True)

        dirname = "empty"
        self.mount_a.run_shell(["mkdir", "-p", dirname])

        self._drop_caches()
        self.assertEqual(self._listdir(dirname), [])

    def test_fetch_with_concurrent_creates(self):
        """
        That entries created while a fetch is in flight are not lost.

        The pipelined path decodes each batch as it lands, so a create that
        lands behind the cursor is visible in the cache but may be missing
        from the batch still being read.  The new dentry lives in the
        fragment's item map either way, so the final listing must contain
        everything.

        The overlap is best effort -- the listing may well finish before the
        creates start -- so treat this as a smoke test for the race rather
        than a deterministic reproducer.
        """
        self._configure(mds_dir_fetch_pipelined=True)

        dirname = "concurrent"
        self._create_files(dirname, self.NFILES)

        self._drop_caches()

        # Kick off the fetch and add entries to the same fragment while it
        # is running.  "zzz_" sorts after every "file_" name, so these land
        # behind the cursor.
        p = self.mount_a.run_shell(["ls", "-1", dirname], wait=False)
        self._create_files(dirname, 20, prefix="zzz")
        p.wait()

        listing = self._listdir(dirname)
        self.assertEqual(len(listing), self.NFILES + 20)
        for i in range(20):
            self.assertIn("zzz_%06d" % i, listing)

    def test_fetch_after_unlink(self):
        """
        That names removed before the fetch do not come back.  A dirfrag
        committed while it is being read used to restart the whole fetch;
        the pipelined path carries on instead, so unlinked names must stay
        unlinked.
        """
        self._configure(mds_dir_fetch_pipelined=True)

        dirname = "unlinked"
        self._create_files(dirname, self.NFILES)

        removed = ["file_%06d" % i for i in range(0, self.NFILES, 10)]
        for name in removed:
            self.mount_a.run_shell(["rm", "-f", f"{dirname}/{name}"])

        expected = self._listdir(dirname)
        self.assertEqual(len(expected), self.NFILES - len(removed))

        self._check_readdir_after_fetch(dirname, expected)
        for name in removed:
            self.assertNotIn(name, self._listdir(dirname))

    def test_fetch_with_snapshot(self):
        """
        That a fragment carrying snapshotted dentries reads back over several
        batches.  Snap dentries take a different branch in the decode loop
        and are the reason the batch is walked in reverse.
        """
        self._configure(mds_dir_fetch_pipelined=True)

        dirname = "snapped"
        self._create_files(dirname, self.NFILES)

        self.mount_a.run_shell(["mkdir", f"{dirname}/.snap/snap1"])
        try:
            # Remove some entries so the snapshot holds dentries the head
            # revision no longer has.
            for i in range(0, self.NFILES, 10):
                self.mount_a.run_shell(["rm", "-f", f"{dirname}/file_%06d" % i])

            expected = self._listdir(dirname)
            self._check_readdir_after_fetch(dirname, expected)

            # The snapshot must still show every original entry.
            snap_listing = self._listdir(f"{dirname}/.snap/snap1")
            self.assertEqual(len(snap_listing), self.NFILES)
        finally:
            self.mount_a.run_shell(["rmdir", f"{dirname}/.snap/snap1"])

    def test_buffered_restart_preserves_snap_purge_state(self):
        """
        A real commit between buffered batches restarts the fetch.  The first
        attempt must not advance snap_purged_thru before it has decoded the
        stale snapshot keys, or the restarted fetch will treat them as live
        and omit the dirty commit that removes them from OMAP.
        """
        dirname = "snap_restart"
        victim = "file_000042"
        self._create_files(dirname, self.NFILES)
        expected = self._listdir(dirname)

        self.mount_a.run_shell(["mkdir", f"{dirname}/.snap/s1"])
        self.mount_a.run_shell(["rm", "-f", f"{dirname}/{victim}"])
        self.mount_a.run_shell(["touch", f"{dirname}/{victim}"])
        self.assertIn(victim, self._listdir(f"{dirname}/.snap/s1"))

        # Releasing caps materializes the snapshot COW key in Pacific.  Prove
        # the exact key exists before destroying the snapshot, then preserve
        # it on disk as stale input for the fetch under test.
        self.fs.flush()
        oid = self._dirfrag_oid(dirname)
        self._drop_caches()
        before_destroy = self._dirfrag_keys(oid)
        victim_snap_keys = [
            key for key in before_destroy
            if key.startswith(victim + "_") and not key.endswith("_head")]
        self.assertTrue(victim_snap_keys)

        self.mount_a.run_shell(["rmdir", f"{dirname}/.snap/s1"])
        self.fs.flush()
        self._drop_caches()
        stale_keys = self._dirfrag_keys(oid)
        for key in victim_snap_keys:
            self.assertIn(key, stale_keys)

        self._configure(
            mds_dir_fetch_pipelined=False,
            mds_inject_dir_fetch_mark_dirty_after_batches=1)
        self._wait_mds_config('mds_dir_fetch_pipelined', False)
        self._wait_mds_config(
            'mds_inject_dir_fetch_mark_dirty_after_batches', 1)

        dirty_key = 'mds_inject_dir_fetch_mark_dirty_after_batches'
        process = None
        try:
            self.mount_a.run_shell(["stat", dirname])
            process = self._start_stalled_fetch(dirname, delay_ms=5000)

            # The first callback has marked the dirfrag dirty.  Disable the
            # one-shot hook before flushing so the restarted fetch is not
            # dirtied again.  flush() waits for an actual CDir commit, making
            # committed_version advance while batch two remains delayed.
            self.config_rm('mds', dirty_key)
            self.fs.rank_asok(['config', 'set', dirty_key, '0'])
            self.fs.flush()
            self._assert_fetch_on_first_batch()
        finally:
            self.config_rm('mds', dirty_key)
            self.fs.rank_asok(['config', 'set', dirty_key, '0'])
            if process is not None:
                self._finish_stalled_fetch(process)
            self.fs.rank_asok(['config', 'unset', dirty_key])

        background = sorted(
            name for name in process.stdout.getvalue().split("\n") if name)
        self.assertEqual(background, expected)
        self.assertEqual(self._listdir(dirname), expected)

        # Persist the successful fetch's transactional horizon update and
        # stale_items removals, then prove the destroyed snapshot key is gone.
        self.fs.flush()
        after = self._dirfrag_keys(oid)
        for key in victim_snap_keys:
            self.assertNotIn(key, after)

        metrics = self._dir_fetch_metrics()
        normal_batches = ((len(stale_keys) + self.KEYS_PER_OP - 1) //
                          self.KEYS_PER_OP)
        self.assertGreater(metrics['dir_fetch_batches'], normal_batches)
        self.assertEqual(metrics['dir_fetch_complete'], 1)

    # ==================================================================
    # failure paths
    # ==================================================================

    def _run_middle_batch_eio(self, pipelined):
        """
        Inject -EIO on the 3rd batch of a multi-batch fetch and check the MDS
        reports it instead of asserting/respawning.  Shared by the pipelined
        and kill-switch variants so both error paths are covered.
        """
        self._configure(mds_dir_fetch_pipelined=pipelined)
        self._wait_mds_config('mds_dir_fetch_pipelined', pipelined)

        dirname = "eio_%s" % ("pipe" if pipelined else "buf")
        self._create_files(dirname, self.NFILES)
        expected = self._listdir(dirname)

        self._drop_caches()
        self.mount_a.run_shell(["stat", dirname])
        self.fs.rank_asok(["perf", "reset", "all"])
        # Fail partway through: with KEYS_PER_OP=16 and NFILES=200 there are
        # ~13 batches, so the 3rd is squarely in the middle.
        inject_key = 'mds_inject_dir_fetch_error_after_batches'
        damage_id = None
        self.config_set('mds', inject_key, '3')
        self.fs.rank_asok(['config', 'set', inject_key, '3'])

        try:
            p = self.mount_a.run_shell(["ls", "-1", dirname], check_status=False,
                                       wait=False)
            p.wait()
            # A FUSE readdir may already have returned entries from earlier
            # batches when a later batch fails.  In that case ls can exit 0,
            # but it must not return a complete listing.
            listing = sorted(p.stdout.getvalue().strip().splitlines())
            self.assertTrue(p.exitstatus != 0 or listing != expected,
                            "readdir returned a complete listing despite "
                            "injected -EIO")

            # The rank must still be up: a mid-pipeline error must not trip the
            # ceph_assert in _omap_fetch_finish and respawn us.
            self.assertTrue(self.fs.rank_is_running(rank=0))
            metrics = self._dir_fetch_metrics()
            # Loading the cold fnode is the version baseline, not a concurrent
            # commit, so both modes reach the injected third batch directly.
            expected_batches = 3
            self.assertEqual(metrics["dir_fetch_batches"], expected_batches)
            self.assertEqual(metrics["dir_fetch_complete"], 0)

            # And the damage must be recorded.
            damage = self._damage_ls()
            self.assertEqual(len(damage), 1, "expected one damaged dirfrag")
            damage_id = damage[0]['id']
        finally:
            self.config_rm('mds', inject_key)
            self.fs.rank_asok(['config', 'set', inject_key, '0'])

        # Once the injection is gone and the damage entry cleared, a cold
        # re-read must return the fragment intact -- the on-disk data was never
        # touched, only the in-memory fetch failed.
        try:
            if damage_id is not None:
                self.fs.rank_asok(["damage", "rm", str(damage_id)])
            self._drop_caches()
            self.assertEqual(self._listdir(dirname), expected)
        finally:
            self.fs.rank_asok(['config', 'unset', inject_key])

    def test_middle_batch_eio_pipelined(self):
        """A -EIO on a non-first batch is reported, not asserted (pipelined)."""
        self._run_middle_batch_eio(pipelined=True)

    def test_middle_batch_eio_not_pipelined(self):
        """The kill-switch path reports a mid-fetch -EIO the same way."""
        self._run_middle_batch_eio(pipelined=False)

    def test_corrupt_dentry_middle_batch(self):
        """
        A single corrupt dentry must not derail the rest of the fetch: the
        fragment still lists every other name, across all later batches, and
        the MDS stays up with the damage recorded.
        """
        self._configure(mds_dir_fetch_pipelined=True)
        self._wait_mds_config('mds_dir_fetch_pipelined', True)

        dirname = "corrupt_dentry"
        self._create_files(dirname, self.NFILES)
        victim = "file_000000"          # sorts into the very first batch
        oid = self._dirfrag_oid(dirname)

        self._offline_setomapval(oid, victim + "_head", "deadbeef" * 10)

        self.mount_a.run_shell(["stat", dirname])
        self.fs.rank_asok(["perf", "reset", "all"])
        listing = self._listdir(dirname)
        metrics = self._dir_fetch_metrics()
        self.assertEqual(metrics["dir_fetch_complete"], 1)
        self.assertGreater(metrics["dir_fetch_batches"], 1)

        self.assertTrue(self.fs.rank_is_running(rank=0))
        # everything but the corrupted dentry, including high-sorting names
        # that only exist in later batches.
        self.assertNotIn(victim, listing)
        self.assertEqual(len(listing), self.NFILES - 1)
        self.assertIn("file_%06d" % (self.NFILES - 1), listing)

        damage = self._damage_ls()
        self.assertGreaterEqual(len(damage), 1)

    def test_decode_fail_pipeline_continues(self):
        """
        A decode failure in a *middle* batch must not stop later batches from
        loading.  In pipelined mode the instrumentation also proves that the
        read for the next batch had already been submitted when the corrupt
        dentry was decoded.
        """
        victim = "file_000090"          # ~6th batch at KEYS_PER_OP=16

        for pipelined in (True, False):
            self._configure(mds_dir_fetch_pipelined=pipelined)
            self._wait_mds_config('mds_dir_fetch_pipelined', pipelined)
            dirname = "decode_fail_%s" % ("pipe" if pipelined else "buf")
            self._create_files(dirname, self.NFILES)
            oid = self._dirfrag_oid(dirname)

            self._offline_setomapval(oid, victim + "_head", "deadbeef" * 10)

            # Resolve the directory inode, but not its dirfrag, before making
            # this fetch the only work visible in the counters.
            self.mount_a.run_shell(["stat", dirname])
            self.fs.rank_asok(["perf", "reset", "all"])
            listing = self._listdir(dirname)
            metrics = self._dir_fetch_metrics()

            self.assertEqual(metrics["dir_fetch_complete"], 1)
            self.assertTrue(self.fs.rank_is_running(rank=0))
            self.assertNotIn(victim, listing)
            self.assertEqual(len(listing), self.NFILES - 1)
            # names before and after the corrupted one both survive
            self.assertIn("file_000000", listing)
            self.assertIn("file_%06d" % (self.NFILES - 1), listing)
            # it really was a multi-batch fetch, so the failure was mid-stream
            self.assertGreater(metrics["dir_fetch_batches"], 1)
            if pipelined:
                self.assertEqual(
                    metrics["dir_fetch_decode_errors_after_next_read"], 1)
            else:
                self.assertEqual(
                    metrics["dir_fetch_decode_errors_after_next_read"], 0)

    def test_snap_versions_span_batch(self):
        """
        Unlink and recreate one name under a snapshot so its old snap version
        and new head version coexist as distinct OMAP keys.  With one key per
        request those versions necessarily land in different batches.
        """
        self._configure(mds_dir_fetch_pipelined=True,
                        mds_dir_keys_per_op=1)
        self._wait_mds_config('mds_dir_fetch_pipelined', True)
        self._wait_mds_config('mds_dir_keys_per_op', 1)

        dirname = "snap_span"
        count = 20
        victim = "file_000009"
        self._create_files(dirname, count)
        self.mount_a.run_python(f"""
with open("{self.mount_a.hostfs_mntpt}/{dirname}/{victim}", "w") as f:
    f.write("old-version")
""")
        expected_head = self._listdir(dirname)
        old_ino = self._path_to_underlying_ino(f"{dirname}/{victim}")

        self.mount_a.run_shell(["mkdir", f"{dirname}/.snap/s1"])
        try:
            self.mount_a.run_shell(["rm", "-f", f"{dirname}/{victim}"])
            self.mount_a.run_shell(["touch", f"{dirname}/{victim}"])
            self.mount_a.run_python(f"""
with open("{self.mount_a.hostfs_mntpt}/{dirname}/{victim}", "w") as f:
    f.write("new-version")
""")
            new_ino = self._path_to_underlying_ino(f"{dirname}/{victim}")
            self.assertNotEqual(new_ino, old_ino)
            head_data = self.mount_a.run_shell(
                ["cat", f"{dirname}/{victim}"]).stdout.getvalue()
            snap_data = self.mount_a.run_shell(
                ["cat", f"{dirname}/.snap/s1/{victim}"]).stdout.getvalue()
            self.assertEqual(head_data, "new-version")
            self.assertEqual(snap_data, "old-version")

            # Flush once to obtain the object id, then unmount and flush again
            # as part of the cold-cache setup.  In v16, releasing client caps
            # during that unmount materializes snapshot COW keys for otherwise
            # unchanged dentries, so the authoritative key set must be sampled
            # after _drop_caches(), not before it.
            self.fs.flush()
            oid = self._dirfrag_oid(dirname)
            self._drop_caches()
            keys = self._dirfrag_keys(oid)

            # Prove that this exact name has both a head and at least one snap
            # key on disk, rather than merely finding an unrelated snap key.
            victim_keys = [k for k in keys
                           if k.startswith(victim + "_")]
            self.assertIn(victim + "_head", victim_keys)
            self.assertTrue(any(not k.endswith("_head")
                                for k in victim_keys))
            self.assertGreaterEqual(len(victim_keys), 2)

            self.mount_a.run_shell(["stat", dirname])
            self.fs.rank_asok(["perf", "reset", "all"])
            self.assertEqual(self._listdir(dirname), expected_head)
            metrics = self._dir_fetch_metrics()
            self.assertEqual(metrics["dir_fetch_complete"], 1)
            self.assertEqual(metrics["dir_fetch_batches"], len(keys))

            snap_listing = self._listdir(f"{dirname}/.snap/s1")
            self.assertEqual(sorted(snap_listing), sorted(expected_head))

            # Listings alone cannot detect the two versions being associated
            # with the wrong inode.  Re-read both paths after the cold fetch
            # and verify the content and inode identity captured above.
            self.assertEqual(
                self.mount_a.run_shell(
                    ["cat", f"{dirname}/{victim}"]).stdout.getvalue(),
                "new-version")
            self.assertEqual(
                self.mount_a.run_shell(
                    ["cat", f"{dirname}/.snap/s1/{victim}"]
                ).stdout.getvalue(),
                "old-version")
            self.assertEqual(
                self._path_to_underlying_ino(f"{dirname}/{victim}"),
                new_ino)
            self.assertEqual(
                self._path_to_underlying_ino(
                    f"{dirname}/.snap/s1/{victim}"),
                old_ino)
        finally:
            self.mount_a.run_shell(["rmdir", f"{dirname}/.snap/s1"])

    # ==================================================================
    # deterministic races: a mutation lands while a fetch is stalled
    # ==================================================================

    def _set_fetch_delay(self, delay_ms):
        """Set the monitor value and an immediate active-rank override."""
        key = 'mds_inject_dir_fetch_batch_delay'
        self.config_set('mds', key, str(delay_ms))
        self.fs.rank_asok(['config', 'set', key, str(delay_ms)])

    def _finish_stalled_fetch(self, process):
        """Release a delayed fetch, wait for it, and remove the override."""
        key = 'mds_inject_dir_fetch_batch_delay'
        self.config_rm('mds', key)
        self.fs.rank_asok(['config', 'set', key, '0'])
        try:
            process.wait()
        finally:
            self.fs.rank_asok(['config', 'unset', key])

    def _start_stalled_fetch(self, dirname, delay_ms=3000,
                             traversal_name=None, expected_batches=1):
        """
        Start a cold fetch and wait until exactly its first effective batch
        has arrived.  The next batch is held by the injected delay, giving the
        caller a verified window in which to mutate configuration or dentries.
        """
        self.fs.rank_asok(["perf", "reset", "all"])
        self._set_fetch_delay(delay_ms)
        if traversal_name is None:
            command = ["ls", "-1", dirname]
        else:
            command = ["stat", f"{dirname}/{traversal_name}"]
        process = self.mount_a.run_shell(command, wait=False)
        try:
            self.wait_until_equal(
                self._dir_fetch_batches, expected_batches, timeout=30,
                reject_fn=lambda value: value > expected_batches,
                period=0.1)
            self.assertFalse(process.finished)
        except Exception:
            self._finish_stalled_fetch(process)
            raise
        return process

    def _assert_fetch_on_first_batch(self, expected_batches=1):
        self.assertEqual(
            self._dir_fetch_batches(), expected_batches,
            "the concurrent operation did not land while the fetch was stalled")

    def _prepare_stalled_mutation(self, dirname, first_batch_name):
        """Cold-fetch dirname and prove first_batch_name precedes the cursor."""
        self._configure(mds_dir_fetch_pipelined=True)
        self._wait_mds_config('mds_dir_fetch_pipelined', True)
        self._wait_mds_config('mds_dir_keys_per_op', self.KEYS_PER_OP)

        self._create_files(dirname, self.NFILES)
        expected = self._listdir(dirname)
        oid = self._dirfrag_oid(dirname)
        self._drop_caches()

        keys = self._dirfrag_keys(oid)
        first_batch = keys[:self.KEYS_PER_OP]
        self.assertIn(first_batch_name + "_head", first_batch)

        # Resolve the directory inode without fetching its dirfrag.  The
        # helper resets counters immediately before starting background ls.
        self.mount_a.run_shell(["stat", dirname])
        process = self._start_stalled_fetch(dirname, delay_ms=5000)
        return process, expected, keys

    def _assert_mutation_fetch_result(self, process, dirname, expected,
                                      present=(), absent=()):
        """Check the in-flight, cached, and refetched views of a mutation."""
        background = sorted(
            name for name in process.stdout.getvalue().split("\n") if name)
        self.assertEqual(background, expected)
        self.assertEqual(self._listdir(dirname), expected)

        metrics = self._dir_fetch_metrics()
        self.assertEqual(metrics['dir_fetch_complete'], 1)
        self.assertEqual(metrics['dir_fetch_version_changed'], 1)
        self.assertEqual(self._damage_ls(), [])

        def assert_targets():
            for name, ino in present:
                path = f"{dirname}/{name}"
                self.mount_a.run_shell(["stat", path])
                self.assertEqual(self.mount_a.path_to_ino(path), ino)
            for name in absent:
                result = self.mount_a.run_shell(
                    ["stat", f"{dirname}/{name}"], check_status=False)
                self.assertNotEqual(result.exitstatus, 0)

        assert_targets()

        # Discard the in-memory result and prove the mutation was persisted,
        # with no stale OMAP entry resurrected by this or a later fetch.
        self._drop_caches()
        self.assertEqual(self._listdir(dirname), expected)
        assert_targets()
        self.assertEqual(self._damage_ls(), [])

    def test_unlink_decoded_name_during_pipelined_fetch(self):
        """Unlinking a batch-one dentry must not let old OMAP data revive it."""
        dirname = "race_unlink"
        victim = "file_000000"
        process, expected, _ = self._prepare_stalled_mutation(
            dirname, victim)
        expected.remove(victim)

        try:
            self.mount_a.run_shell(["rm", "-f", f"{dirname}/{victim}"])
            self.fs.flush()
            self._assert_fetch_on_first_batch()
        finally:
            self._finish_stalled_fetch(process)

        self._assert_mutation_fetch_result(
            process, dirname, expected, absent=(victim,))

    def test_create_behind_cursor_during_pipelined_fetch(self):
        """A new key before the OMAP cursor must survive only in-memory load."""
        dirname = "race_create"
        decoded_name = "file_000000"
        created = "aaa_created"
        process, expected, keys = self._prepare_stalled_mutation(
            dirname, decoded_name)

        cursor = keys[self.KEYS_PER_OP - 1]
        self.assertLess(created + "_head", cursor)
        self.assertNotIn(created + "_head", keys)

        try:
            self.mount_a.run_shell(["touch", f"{dirname}/{created}"])
            created_ino = self.mount_a.path_to_ino(f"{dirname}/{created}")
            self.fs.flush()
            self._assert_fetch_on_first_batch()
        finally:
            self._finish_stalled_fetch(process)

        expected.append(created)
        expected.sort()
        self._assert_mutation_fetch_result(
            process, dirname, expected, present=((created, created_ino),))

    def test_rename_across_cursor_during_pipelined_fetch(self):
        """Rename a decoded name beyond the cursor without losing either side."""
        dirname = "race_rename"
        source = "file_000001"
        target = "zzz_renamed"
        process, expected, keys = self._prepare_stalled_mutation(
            dirname, source)
        source_ino = self.mount_a.path_to_ino(f"{dirname}/{source}")
        self.assertGreater(target + "_head", keys[-1])

        try:
            self.mount_a.run_shell(
                ["mv", f"{dirname}/{source}", f"{dirname}/{target}"])
            self.fs.flush()
            self._assert_fetch_on_first_batch()
        finally:
            self._finish_stalled_fetch(process)

        expected.remove(source)
        expected.append(target)
        expected.sort()
        self._assert_mutation_fetch_result(
            process, dirname, expected,
            present=((target, source_ino),), absent=(source,))

    def test_trim_during_pipelined_fetch(self):
        """Decoded batches must survive an LRU trim until fetch completes."""
        self._configure(mds_dir_fetch_pipelined=True)
        self._wait_mds_config('mds_dir_fetch_pipelined', True)

        dirname = "race_trim"
        self._create_files(dirname, self.NFILES)
        expected = self._listdir(dirname)

        self._drop_caches()
        self.mount_a.run_shell(["stat", dirname])
        p = self._start_stalled_fetch(dirname, delay_ms=5000)
        try:
            # cache drop drives trim(UINT64_MAX), so the clean dentries from
            # batch one are considered for expiry while STATE_FETCHING is
            # still set.  A short recall timeout keeps this operation inside
            # the injected gap even if the client retains unrelated caps.
            result = self.fs.rank_tell(["cache", "drop", "1"])
            self.assertIn("trim_cache", result)
            self._assert_fetch_on_first_batch()
        finally:
            self._finish_stalled_fetch(p)

        background = sorted(
            x for x in p.stdout.getvalue().split("\n") if x)
        self.assertEqual(background, expected)
        self.assertEqual(self._listdir(dirname), expected)
        self.assertEqual(self._dir_fetch_count(), 1)
        self.assertEqual(self._damage_ls(), [])

    # ==================================================================
    # config toggle and failover mid-fetch
    # ==================================================================

    def _run_toggle_mid_fetch(self, start_pipelined):
        dirname = "toggle_%s" % ("on" if start_pipelined else "off")
        self._configure(mds_dir_fetch_pipelined=start_pipelined)
        self._wait_mds_config('mds_dir_fetch_pipelined', start_pipelined)
        self._create_files(dirname, self.NFILES)
        expected = self._listdir(dirname)

        self._drop_caches()
        # Resolve the inode without opening the dirfrag, then reset counters so
        # the background ls is the only complete fetch.
        self.mount_a.run_shell(["stat", dirname])
        # Both modes stall after the first batch.  A cold fnode load must not
        # cause a version-probe restart in the buffered path.
        stalled_batches = 1
        p = self._start_stalled_fetch(
            dirname, expected_batches=stalled_batches)
        try:
            self._configure(mds_dir_fetch_pipelined=not start_pipelined)
            self._wait_mds_config('mds_dir_fetch_pipelined',
                                  not start_pipelined)
            self._assert_fetch_on_first_batch(stalled_batches)
        finally:
            self._finish_stalled_fetch(p)

        background = sorted(x for x in p.stdout.getvalue().split("\n") if x)
        self.assertEqual(background, expected)
        self.assertEqual(self._listdir(dirname), expected)
        metrics = self._dir_fetch_metrics()
        # exactly one fetch completed -- a toggle-induced restart would show up
        # as extra work here.
        self.assertEqual(metrics["dir_fetch_complete"], 1)
        if start_pipelined:
            self.assertLess(metrics["dir_fetch_peak_omap_bytes"],
                            metrics["dir_fetch_omap_bytes"])
        else:
            self.assertEqual(metrics["dir_fetch_peak_omap_bytes"],
                             metrics["dir_fetch_omap_bytes"])

    def test_toggle_pipelined_mid_fetch_on_to_off(self):
        """Turning the pipeline off mid-fetch keeps the in-flight fetch sane."""
        self._run_toggle_mid_fetch(start_pipelined=True)

    def test_toggle_pipelined_mid_fetch_off_to_on(self):
        """Turning the pipeline on mid-fetch keeps the in-flight fetch sane."""
        self._run_toggle_mid_fetch(start_pipelined=False)

    def test_failover_mid_fetch(self):
        """
        Failing the active rank while a fetch is in flight must not lose data:
        the standby takes over and the client re-drives readdir on the new
        rank, which re-fetches from scratch.
        """
        self._configure(mds_dir_fetch_pipelined=True)
        self._wait_mds_config('mds_dir_fetch_pipelined', True)

        dirname = "failover"
        nfiles = 400
        self._create_files(dirname, nfiles)
        expected = self._listdir(dirname)

        self._drop_caches()
        self.mount_a.run_shell(["stat", dirname])
        before = self.fs.status()
        old_name = self.fs.get_rank(rank=0, status=before)['name']
        p = self._start_stalled_fetch(dirname, delay_ms=5000)
        try:
            # `mds fail <fs>:<rank>` alone lets a fast local daemon beacon and
            # reclaim the same rank.  Stop the old holder first so this is a
            # real standby takeover rather than a same-name restart.
            self.fs.mds_stop(old_name)
            self.fs.mds_fail(old_name)
            self.config_rm('mds', 'mds_inject_dir_fetch_batch_delay')
            after = self.fs.wait_for_daemons()
            new_name = self.fs.get_rank(rank=0, status=after)['name']
            self.assertNotEqual(new_name, old_name)
            self.assertTrue(self.fs.status().hadfailover(before))

            # The failed daemon's timer is gone.  Explicitly clear the hook on
            # the replacement before waiting for the client request to replay.
            self.fs.rank_asok(
                ['config', 'set', 'mds_inject_dir_fetch_batch_delay', '0'])
            p.wait()
        finally:
            self.config_rm('mds', 'mds_inject_dir_fetch_batch_delay')
            self.fs.rank_asok(
                ['config', 'unset', 'mds_inject_dir_fetch_batch_delay'])

        background = sorted(x for x in p.stdout.getvalue().split("\n") if x)
        self.assertEqual(background, expected)
        self.assertEqual(self._listdir(dirname), expected)
        self.assertEqual(len(expected), nfiles)
        self.assertEqual(self._damage_ls(), [])

    # ==================================================================
    # fragmentation and OSD-driven truncation
    # ==================================================================

    def test_fragmented_dir_fetch(self):
        """
        A directory split across several dirfrags must read back completely,
        with every fragment fetched.  setUp keeps fragmentation off, so this
        test re-enables it with a small split threshold.
        """
        split_size = 50
        self._configure(mds_dir_fetch_pipelined=True,
                        mds_bal_fragment_dirs=True,
                        mds_bal_split_size=split_size,
                        mds_bal_split_bits=1,
                        mds_bal_merge_size=0)
        self._wait_mds_config('mds_dir_fetch_pipelined', True)
        self._wait_mds_config('mds_bal_fragment_dirs', True)

        dirname = "fragmented"
        nfiles = split_size * 4
        self._create_files(dirname, nfiles)
        expected = self._listdir(dirname)

        def num_dirfrags():
            ino = self.mount_a.path_to_ino(dirname)
            for entry in self.fs.read_cache("/" + dirname, 0):
                if entry['ino'] == ino:
                    return len(entry['dirfrags'])
            return 0

        self.wait_until_true(lambda: num_dirfrags() > 1, timeout=60)
        nfrags = num_dirfrags()

        before = self._dir_fetch_count()
        self._drop_caches()
        self.assertEqual(self._listdir(dirname), expected)
        # every fragment had to be fetched
        self.assertGreaterEqual(self._dir_fetch_count() - before, nfrags)

    def test_osd_driven_truncation(self):
        """
        Batching driven by the OSD's osd_max_omap_entries_per_request, not by
        mds_dir_keys_per_op: leave the MDS limit large and clamp the OSD, and
        the fetch must still span several batches and return everything.
        """
        # Undo setUp's small MDS batch so the MDS is not the limiter.
        self._configure(mds_dir_fetch_pipelined=True,
                        mds_dir_keys_per_op=16384)
        self._wait_mds_config('mds_dir_fetch_pipelined', True)
        self._wait_mds_config('mds_dir_keys_per_op', 16384)

        # injectargs, not `config set`: this option has no runtime flag but is
        # read directly from g_conf() on the OSD.
        original_limit = self.config_get(
            'osd', 'osd_max_omap_entries_per_request')
        manager = self.fs.mon_manager
        manager.raw_cluster_cmd(
            'tell', 'osd.*', 'injectargs',
            '--osd_max_omap_entries_per_request=16')
        try:
            dirname = "osd_trunc"
            self._create_files(dirname, self.NFILES)
            expected = self._listdir(dirname)

            self._drop_caches()
            self.mount_a.run_shell(["stat", dirname])
            self.fs.rank_asok(["perf", "reset", "all"])
            self.assertEqual(self._listdir(dirname), expected)

            metrics = self.fs.rank_asok(["perf", "dump", "mds"])["mds"]
            self.assertEqual(metrics["dir_fetch_complete"], 1)
            # The MDS asked for 16384 keys but the OSD handed back 16 at a
            # time, so the fetch must have taken many batches.
            self.assertGreater(metrics["dir_fetch_batches"], 1)
        finally:
            manager.raw_cluster_cmd(
                'tell', 'osd.*', 'injectargs',
                '--osd_max_omap_entries_per_request=%s' % original_limit)
