"""Header-only dirfrag fetches must complete scrub's gather on damage."""

from tasks.cephfs.cephfs_test_case import CephFSTestCase


class TestDirfragFetchDamage(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    def _config_set(self, key, value):
        """config_set that undoes itself.

        config_set writes the mon config store, which CephFSTestCase only
        cleans for keys registered via set_conf(); mds_restart() in setUp does
        not clear it either, so without this these settings leak into every
        later test in the job.
        """
        self.addCleanup(self.config_rm, 'mds', key)
        self.config_set('mds', key, value)

    def _scrub_damaged_header(self, damage):
        self._config_set('mds_bal_fragment_dirs', False)
        self.mount_a.run_shell(['mkdir', 'bad_header'])
        self.mount_a.run_shell(['touch', 'bad_header/file'])
        ino = self.mount_a.path_to_ino('bad_header')
        oid = '{:x}.00000000'.format(ino)

        self.mount_a.umount_wait()
        self.fs.flush()
        self.fs.fail()
        if damage == 'missing':
            self.fs.radosm(['rm', oid])
        else:
            self.fs.radosm(['setomapheader', oid, damage])
        self.fs.set_joinable()
        self.fs.wait_for_daemons()

        # Do not remount or list the directory: scrub must be the first to
        # open its CDir and call fetch_keys({}, gather.new_sub()) for its
        # still-unloaded fnode. A full fetch uses WAIT_COMPLETE instead.
        before = self.fs.rank_asok(['perf', 'dump', 'mds'])['mds']['dir_fetch_keys']
        result = self.fs.run_scrub(['start', '/bad_header', 'recursive'])
        self.assertEqual(result['return_code'], 0)
        self.assertTrue(self.fs.wait_until_scrub_complete(
            tag=result['scrub_tag'], sleep=1, timeout=60))
        after = self.fs.rank_asok(['perf', 'dump', 'mds'])['mds']['dir_fetch_keys']
        self.assertGreater(after, before)
        self.assertTrue(any(
            entry['damage_type'] == 'dir_frag' and entry['ino'] == ino
            for entry in self.fs.get_damage(rank=0)))

    def test_scrub_missing_dirfrag_completes(self):
        self._scrub_damaged_header('missing')

    def test_scrub_empty_header_completes(self):
        self._scrub_damaged_header('')

    def test_scrub_corrupt_header_completes(self):
        self._scrub_damaged_header('invalid fnode')
