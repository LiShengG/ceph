"""
Tests for the batched fetch of a dirfrag from its RADOS object.

Reading a large dirfrag takes several omap_get_vals() round trips: the OSD
clamps each one to osd_max_omap_entries_per_request, so a fragment with more
keys than that arrives in pieces.  These tests drive that multi-batch path and
check that the fragment the MDS ends up with matches what was written,
with the pipelined decode both enabled (the default) and disabled.
"""

import json
from contextlib import contextmanager
from io import BytesIO, StringIO

from tasks.cephfs.cephfs_test_case import CephFSTestCase


class DirfragFetchLogMixin:
    def _dump_dir(self, dirname):
        dirs = self.fs.rank_asok(['dump', 'dir', '/' + dirname, 'true'])
        self.assertEqual(len(dirs), 1, "test directory must have one dirfrag")
        return dirs[0]

    @contextmanager
    def _capture_fetch_log(self, ino):
        """Read only new log messages for this directory's single fragment.

        dir_fetch_complete counts calls to fetch(), not its internal restarts,
        so it cannot distinguish a restart from a single header read.
        """
        mds_id = self.fs.get_rank()['name']
        remote = self.fs.mon_manager.find_remote('mds', mds_id)

        def asok(args):
            return self.fs.mds_asok(args, mds_id=mds_id)

        def gather_level(value):
            # "debug_mds" reads back as "<log>/<gather>"; we need log >= 10.
            try:
                return int(str(value).split('/')[0])
            except ValueError:
                return -1

        saved = {}
        try:
            wanted = [('log_to_file', 'true')]
            # Only ever raise the level. The fs suite already runs at
            # "debug mds = 20"; forcing 10 would thin out the log precisely
            # when one of these tests is failing and needs it.
            if gather_level(asok(['config', 'get', 'debug_mds'])['debug_mds']) < 10:
                wanted.append(('debug_mds', '10'))
            for key, value in wanted:
                saved[key] = asok(['config', 'get', key])[key]
                asok(['config', 'set', key, value])
            log_path = asok(['config', 'get', 'log_file'])['log_file']
            self.assertTrue(log_path, "MDS must have a file log for this test")
            asok(['log', 'flush'])
            offset = int(remote.run(
                args=['sudo', 'stat', '-c', '%s', '--', log_path],
                stdout=StringIO()).stdout.getvalue())
            # dirfrag_t omits the fragment suffix for an unsplit directory,
            # and inodeno_t prints with a "0x" prefix.
            prefix = '.cache.dir(0x{:x}) '.format(ino)

            def read_log():
                asok(['log', 'flush'])
                output = remote.run(
                    args=['sudo', 'tail', '-c', '+{}'.format(offset + 1),
                          '--', log_path], stdout=StringIO()).stdout.getvalue()
                return [line for line in output.splitlines() if prefix in line]

            yield read_log
        finally:
            # config get returns strings ("true", "20/20"), so these restore
            # verbatim.
            for key, value in reversed(list(saved.items())):
                asok(['config', 'set', key, value])


class TestDirfragFetch(DirfragFetchLogMixin, CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    # Small enough that a few hundred files span many batches, so the tests
    # stay quick while still exercising the cursor across batches.
    KEYS_PER_OP = 16
    NFILES = 200

    def _configure(self, **kwargs):
        for k, v in kwargs.items():
            # config_set writes the mon config store, which CephFSTestCase
            # only cleans for keys registered via set_conf(); mds_restart() in
            # setUp does not clear it either. Without this, e.g.
            # mds_dir_prefetch=False leaks into every later test in the job.
            self.addCleanup(self.config_rm, 'mds', str(k))
            self.config_set('mds', str(k), str(v))

    def setUp(self):
        super(TestDirfragFetch, self).setUp()
        # Keep the directory in a single fragment: we want to test batching
        # within one dirfrag, not fragmentation.
        self._configure(mds_bal_fragment_dirs=False,
                        mds_dir_keys_per_op=self.KEYS_PER_OP)

    def _dir_fetch_count(self):
        return self.fs.rank_asok(['perf', 'dump', 'mds'])['mds']['dir_fetch_complete']

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

    def _check_cold_fetch_version(self, pipelined):
        self._configure(mds_dir_fetch_pipelined=pipelined)
        dirname = 'cold_fetch_version'
        self._create_files(dirname, self.NFILES)
        ino = self.mount_a.path_to_ino(dirname)
        self._drop_caches()
        # Resolve ancestors without fetching any entries in the target.
        self.mount_a.run_shell(['stat', dirname])
        cold = self._dump_dir(dirname)
        if cold.get('status') != 'dirfrag not in cache':
            self.assertEqual(int(cold['version']), 0,
                             'must exercise adoption of an on-disk fnode')
            self.assertEqual(int(cold['committed_version']), 0)
            self.assertNotIn('complete', cold['states'])
            self.assertEqual(cold['dentries'], [])

        with self._capture_fetch_log(ino) as read_log:
            listing = self._listdir(dirname)
            lines = read_log()

        self.assertEqual(listing, ['file_%06d' % i for i in range(self.NFILES)])
        loaded = self._dump_dir(dirname)
        self.assertIn('complete', loaded['states'])
        self.assertGreater(int(loaded['committed_version']), 0)
        headers = [line for line in lines if '_fetched header ' in line]
        self.assertEqual(len(headers), 1,
                         'cold fetch reread its header: {}'.format(headers))
        races = [line for line in lines if 'while fetching at v' in line]
        self.assertEqual(races, [],
                         'adopting the disk version was mistaken for a commit')

    def test_cold_fetch_version_pipelined(self):
        """Adopting the fnode must not report a race in any fetch batch."""
        self._check_cold_fetch_version(pipelined=True)

    def test_cold_fetch_version_not_pipelined(self):
        """Adopting the fnode must not cause a redundant buffered fetch."""
        self._check_cold_fetch_version(pipelined=False)

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

    def test_fetch_keys_path(self):
        """
        That the fetch_keys() path -- a single omap_get_vals_by_keys(), never
        batched -- still resolves both present and absent names.  It shares
        the decode with the batched path, so a refactor there can break it.
        """
        self._configure(mds_dir_fetch_pipelined=True,
                        mds_dir_prefetch=False)

        dirname = "fetch_keys"
        self._create_files(dirname, self.NFILES)

        self._drop_caches()

        # A name that exists: resolved via fetch_keys, not a full fetch.
        self.mount_a.run_shell(["stat", f"{dirname}/file_000042"])

        # A name that does not: must come back ENOENT, not hang or assert.
        self.mount_a.run_shell(["stat", f"{dirname}/does_not_exist"],
                               check_status=False)

        # The directory as a whole must still be listable afterwards.
        self.assertEqual(len(self._listdir(dirname)), self.NFILES)

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


class TestDirfragCommitFetchRace(DirfragFetchLogMixin, CephFSTestCase):
    """Commit a target dirfrag between batches of a full fetch.

    Inspect that fragment's cached entries and committed version to prove the
    overlap. Buffered fetches must restart. Pipelined snapshot fetches must
    preserve the on-disk purge watermark until the full scan has completed,
    and persist removals discovered after the racing commit on a later flush.
    """

    CLIENTS_REQUIRED = 2
    MDSS_REQUIRED = 1

    # Small keys-per-op so a few hundred files span several fetch batches.
    KEYS_PER_OP = 16
    NFILES = 200
    # Every Nth file is unlinked while a snapshot pins it, so its snap-dentry
    # lands on disk with a non-head snapid and becomes the thing that leaks.
    VICTIM_STRIDE = 10

    FETCH_DELAY_MS = 10000

    def _configure(self, **kwargs):
        for k, v in kwargs.items():
            # config_set writes the mon config store, which CephFSTestCase
            # only cleans for keys registered via set_conf(); mds_restart() in
            # setUp does not clear it either. Without this, e.g.
            # mds_dir_prefetch=False leaks into every later test in the job.
            self.addCleanup(self.config_rm, 'mds', str(k))
            self.config_set('mds', str(k), str(v))

    def setUp(self):
        super(TestDirfragCommitFetchRace, self).setUp()
        # Single fragment: we are testing one dirfrag's commit/fetch, not splits.
        self._configure(mds_bal_fragment_dirs=False,
                        mds_dir_keys_per_op=self.KEYS_PER_OP,
                        mds_dir_prefetch=False,
                        mds_dir_fetch_pipelined=True)
        self.fs.set_allow_new_snaps(True)

    # --- helpers -----------------------------------------------------------

    def _read_header(self, obj):
        raw = self.fs.radosmo(['getomapheader', obj, '-'], stdout=BytesIO())
        return json.loads(self.fs.dencoder('fnode_t', raw))

    def _snap_server_dump(self):
        return self.fs.rank_asok(["dump", "snaps", "--server"])

    def _dirfrag_obj(self, dirpath):
        ino = self.mount_a.path_to_ino(dirpath)
        return "{0:x}.00000000".format(ino)

    def _omap_keys(self, obj):
        out = self.fs.radosmo(["listomapkeys", obj], stdout=StringIO())
        return [k for k in out.split("\n") if k]

    def _victim_snap_keys(self, obj, victims):
        """
        On-disk snap-dentry keys for the victims.  A dentry key is 'name_head'
        for the head revision and 'name_<hex-snapid>' for a snap revision (see
        dentry_key_t in src/mds/mdstypes.h), so a leaked stale key is one that
        belongs to a victim and does not end in '_head'.
        """
        out = []
        for k in self._omap_keys(obj):
            if k.endswith("_head"):
                continue
            name = k.rsplit("_", 1)[0]
            if name in victims:
                out.append(k)
        return out

    def _drop_a_and_mds_cache(self):
        """
        Force the next access from client A to re-read the dirfrag: flush the
        journal, drop A's client cache (umount/remount) and drop the MDS cache.
        Client B stays mounted throughout, so any file B holds open keeps its
        dentry pinned in the MDS cache -- the dirfrag ends up present but
        incomplete rather than gone.
        """
        self.mount_a.umount_wait()
        self.fs.flush()
        self.fs.rank_tell(['cache', 'drop'])
        self.mount_a.mount_wait()

    # --- the test ----------------------------------------------------------

    def test_buffered_fetch_restarts_after_commit(self):
        """A real commit between batches must still restart a buffered fetch."""
        self._configure(mds_dir_fetch_pipelined=False)
        dirname = 'buffered_commit_race'
        kept = 'keep'
        expected = ['file_%06d' % i for i in range(self.NFILES)] + [kept]
        self.mount_a.run_shell(['mkdir', dirname])
        self.mount_a.run_python(f"""
import os
d = os.path.join({self.mount_a.hostfs_mntpt!r}, {dirname!r})
for name in {expected!r}:
    open(os.path.join(d, name), 'w').close()
""")
        ino = self.mount_a.path_to_ino(dirname)
        bg = self.mount_b.open_background(basename=f'{dirname}/{kept}')
        try:
            self._drop_a_and_mds_cache()
            cold = self._dump_dir(dirname)
            self.assertNotIn('complete', cold['states'])
            committed_before = int(cold['committed_version'])
            self.assertGreater(committed_before, 0)
            self.assertEqual(int(cold['committing_version']), committed_before)

            with self._capture_fetch_log(ino) as read_log:
                self._configure(mds_inject_dir_fetch_delay=self.FETCH_DELAY_MS)
                listing = None
                try:
                    listing = self.mount_a.run_shell(
                        ['ls', '-1', dirname], wait=False)
                    # Buffered mode has no decoded dentries until EOF. The
                    # header log plus a locked dump proves the first callback
                    # ran; the delay holds subsequent callbacks off mds_lock.
                    self.wait_until_true(
                        lambda: any('_fetched header ' in line
                                    for line in read_log()),
                        timeout=60, period=2)
                    before = self._dump_dir(dirname)
                    self.assertNotIn('complete', before['states'])
                    self.assertFalse(listing.finished)
                    self.assertEqual(int(before['committed_version']),
                                     committed_before)
                    self.assertFalse(any('while fetching at v' in line
                                         for line in read_log()))

                    # The pinned inode is already cached, so dirtying it does
                    # not block on the full fetch's WAIT_COMPLETE.
                    self.mount_b.run_python(f"""
import os
fd = os.open(os.path.join({self.mount_b.hostfs_mntpt!r},
                          {dirname!r}, {kept!r}), os.O_RDONLY)
try:
    mode = os.fstat(fd).st_mode & 0o777
    os.fchmod(fd, mode ^ 0o100)
    os.fsync(fd)
finally:
    os.close(fd)
""", timeout=300)
                    self.fs.flush()
                    during = self._dump_dir(dirname)
                    self.assertNotIn('complete', during['states'])
                    self.assertFalse(listing.finished)
                    self.assertGreater(int(during['committed_version']),
                                       committed_before)
                finally:
                    self._configure(mds_inject_dir_fetch_delay=0)
                    if listing is not None:
                        listing.wait()
                lines = read_log()

            self.assertEqual(sorted(listing.stdout.getvalue().splitlines()),
                             sorted(expected))
            self.assertIn('complete', self._dump_dir(dirname)['states'])
            restart = 'while fetching at v{}, restarting'.format(committed_before)
            self.assertTrue(any(restart in line for line in lines),
                            'buffered fetch ignored the racing commit')
            self.assertGreaterEqual(
                sum('_fetched header ' in line for line in lines), 2,
                'restart must actually reread the header')
        finally:
            self.mount_b._kill_background(bg)

    def test_stale_items_not_leaked_on_commit_fetch_race(self):
        dirname = "victimdir"
        kept = "keep_000000"          # non-victim, pinned open by client B

        # Create the kept file plus NFILES ordinary files in one dirfrag.
        self.mount_a.run_shell(["mkdir", "-p", dirname])
        self.mount_a.run_python(f"""
import os
d = os.path.join("{self.mount_a.hostfs_mntpt}", "{dirname}")
open(os.path.join(d, "{kept}"), "w").close()
for i in range({self.NFILES}):
    open(os.path.join(d, "file_%06d" % i), "w").close()
""")

        victims = set("file_%06d" % i
                      for i in range(0, self.NFILES, self.VICTIM_STRIDE))

        # Snapshot, then unlink the victims so only the snapshot holds them.
        self.mount_a.run_shell(["mkdir", f"{dirname}/.snap/s1"])
        for name in sorted(victims):
            self.mount_a.run_shell(["rm", "-f", f"{dirname}/{name}"])

        # Persist the snap-dentries to disk *while the snapshot is still alive*
        # (so they are valid, not yet stale, and are not purged here).
        obj = self._dirfrag_obj(dirname)
        self.fs.flush()
        self.assertGreater(
            len(self._victim_snap_keys(obj, victims)), 0,
            "expected victim snap-dentries on disk after snapshotting+unlink")

        # Client B pins one dentry: this keeps the dirfrag in the MDS cache but
        # incomplete after we drop caches, and lets us dirty the dirfrag during
        # the fetch window without a lookup that would block on WAIT_COMPLETE.
        bg = self.mount_b.open_background(basename=f"{dirname}/{kept}")
        try:
            # Drop the dirfrag out of a completed state, then destroy the
            # snapshot with the dirfrag *not* in cache, so the stale-snap purge
            # is deferred to the fetch path (the racy one) rather than done now
            # by an in-cache commit.
            self._drop_a_and_mds_cache()

            last_destroyed0 = int(self._snap_server_dump()["last_destroyed"])
            self.mount_b.run_shell(["rmdir", f"{dirname}/.snap/s1"])
            self.wait_until_true(
                lambda: len(self._snap_server_dump()["pending_destroy"]) == 0,
                timeout=60)
            self.wait_until_true(
                lambda: int(self._snap_server_dump()["last_destroyed"]) > last_destroyed0,
                timeout=60)

            # The stale keys must still be on disk right before the race,
            # otherwise there is nothing to leak and a pass would be vacuous.
            self.assertGreater(
                len(self._victim_snap_keys(obj, victims)), 0,
                "victim snap-dentries were purged before the race could run")

            purge_before = int(self._read_header(obj)['snap_purged_thru'])
            self._drive_race(dirname, kept, obj, purge_before)

            # On fixed code the dir is left dirty (S1 is not lost), so this
            # flush commits the follow-up that removes the stale keys.  On the
            # buggy code the dir was marked clean and nothing removes them.
            self._configure(mds_inject_dir_fetch_delay=0,
                            mds_inject_dir_commit_ops_delay=0)
            self.fs.flush()

            leaked = self._victim_snap_keys(obj, victims)
            self.assertEqual(
                leaked, [],
                "stale snap-dentries leaked on disk after commit/fetch race: "
                "{0}".format(leaked))
        finally:
            self._configure(mds_inject_dir_fetch_delay=0,
                            mds_inject_dir_commit_ops_delay=0)
            self.mount_b._kill_background(bg)

    def _drive_race(self, dirname, kept, obj, purge_before):
        self._configure(mds_inject_dir_fetch_delay=0,
                        mds_inject_dir_commit_ops_delay=0)
        self._drop_a_and_mds_cache()
        cold = self._dump_dir(dirname)
        self.assertNotIn('complete', cold['states'])
        cached_paths = {dn['path'] for dn in cold.get('dentries', [])}
        self._configure(mds_inject_dir_fetch_delay=self.FETCH_DELAY_MS)

        listing = self.mount_a.run_shell(['ls', '-1', dirname], wait=False)
        try:
            first_batch = {}

            def decoded_partial_batch():
                dump = self._dump_dir(dirname)
                paths = {dn['path'] for dn in dump.get('dentries', [])}
                if 'complete' in dump['states'] or not paths - cached_paths:
                    return False
                first_batch.update(dump)
                return True

            self.wait_until_true(decoded_partial_batch, timeout=60, period=2)
            self.assertFalse(listing.finished)
            committed_before = int(first_batch['committed_version'])

            # Use a cached, pinned inode so this does not wait for readdir.
            # fsync orders the new mode's cap flush before the journal flush.
            self.mount_b.run_python(f"""
import os
fd = os.open(os.path.join({self.mount_b.hostfs_mntpt!r},
                          {dirname!r}, {kept!r}), os.O_RDONLY)
try:
    os.fchmod(fd, 0o600)
    os.fsync(fd)
finally:
    os.close(fd)
""", timeout=300)
            self.fs.flush()

            # Global perf counters cannot prove this ordering: inspect the
            # target itself and reject a commit that only finished after EOF.
            during = self._dump_dir(dirname)
            self.assertNotIn('complete', during['states'])
            self.assertFalse(listing.finished)
            self.assertGreater(int(during['committed_version']), committed_before)
            header = self._read_header(obj)
            self.assertEqual(int(header['snap_purged_thru']), purge_before,
                             'commit published purge progress before fetch EOF')
        finally:
            self._configure(mds_inject_dir_fetch_delay=0)
            listing.wait()

        expected = {"file_%06d" % i for i in range(self.NFILES)
                    if i % self.VICTIM_STRIDE != 0}
        expected.add(kept)
        self.assertEqual(set(listing.stdout.getvalue().splitlines()), expected)
