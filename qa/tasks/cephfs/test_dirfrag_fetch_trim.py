"""Exercise cache trimming between batches of a full dirfrag fetch."""

from tasks.cephfs.cephfs_test_case import CephFSTestCase


class TestDirfragFetchTrim(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    NFILES = 64
    KEYS_PER_OP = 8

    def _config_set(self, key, value):
        """config_set that undoes itself.

        config_set writes the mon config store, which CephFSTestCase only
        cleans for keys registered via set_conf(); mds_restart() in setUp does
        not clear it either, so without this these settings leak into every
        later test in the job.
        """
        self.addCleanup(self.config_rm, 'mds', key)
        self.config_set('mds', key, value)

    def _dump_dir(self, dirname):
        dirs = self.fs.rank_asok(['dump', 'dir', '/' + dirname, 'true'])
        self.assertEqual(len(dirs), 1, "test directory must have one dirfrag")
        return dirs[0]

    def test_trim_between_fetch_batches(self):
        self._config_set('mds_bal_fragment_dirs', False)
        self._config_set('mds_dir_fetch_pipelined', True)
        self._config_set('mds_dir_keys_per_op', self.KEYS_PER_OP)
        # A cache drop must visit the whole small LRU, without stopping at a
        # throttle before it reaches this directory's freshly loaded entries.
        self._config_set('mds_cache_trim_threshold', 1000000)

        dirname = 'fetch_trim'
        expected = ['file_%06d' % i for i in range(self.NFILES)]
        self.mount_a.run_shell(['mkdir', dirname])
        self.mount_a.run_python(f"""
import os
d = os.path.join({self.mount_a.hostfs_mntpt!r}, {dirname!r})
for name in {expected!r}:
    open(os.path.join(d, name), 'w').close()
""")
        self.mount_a.umount_wait()
        self.fs.flush()
        self.fs.rank_tell(['cache', 'drop'])
        self.mount_a.mount_wait()
        # Load the directory inode and its ancestors before arming the delay.
        # This does not read the directory's entries.
        self.mount_a.run_shell(['stat', dirname])
        cold = self._dump_dir(dirname)
        self.assertEqual(cold.get('dentries', []), [])
        self.assertNotIn('complete', cold.get('states', []))

        # Both the initial reply and subsequent replies pause on the finisher
        # outside mds_lock. With eight batches, a cache drop can finish while
        # the readdir is still waiting for the remaining replies.
        self.fs.rank_asok(['config', 'set', 'mds_inject_dir_fetch_delay', '10000'])
        listing = None
        try:
            listing = self.mount_a.run_shell(['ls', '-1', dirname], wait=False)
            first_batch = {}

            def has_decoded_batch():
                dump = self._dump_dir(dirname)
                entries = dump.get('dentries', [])
                if ('complete' in dump.get('states', []) or
                        not 0 < len(entries) < self.NFILES):
                    return False
                # These are actual trim candidates, not entries protected by
                # client caps or dirty metadata. The fetch pins only the dir.
                candidates = [dn for dn in entries
                              if dn['nref'] == 0 and
                              'dirty' not in dn['states'] and
                              not dn['is_null']]
                if not candidates:
                    return False
                first_batch.update(dump)
                return True

            self.wait_until_true(has_decoded_batch, timeout=60, period=0.5)
            self.assertFalse(listing.finished)
            self.assertGreater(first_batch['auth_pins'], 0)
            paths = {dn['path'] for dn in first_batch['dentries']}

            result = self.fs.rank_tell(['cache', 'drop', '1'])
            self.assertEqual(result['flush_journal']['return_code'], 0)
            self.assertIn('trim_cache', result)
            after_trim = self._dump_dir(dirname)
            # Check the target fragment itself, so a drop that only finished
            # after the full fetch cannot make this regression pass vacuously.
            self.assertFalse(listing.finished)
            self.assertNotIn('complete', after_trim['states'])
            self.assertGreater(after_trim['auth_pins'], 0)
            self.assertTrue(paths <= {dn['path']
                                      for dn in after_trim['dentries']},
                            "cache trim evicted a decoded fetch batch")
        finally:
            self.fs.rank_asok(['config', 'set', 'mds_inject_dir_fetch_delay', '0'])
            if listing is not None:
                listing.wait()

        self.assertEqual(sorted(listing.stdout.getvalue().splitlines()), expected)
