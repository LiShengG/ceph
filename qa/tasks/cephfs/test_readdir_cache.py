import logging

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.fuse_mount import FuseMount

log = logging.getLogger(__name__)


def rjenkins(name):
    """
    ceph_str_hash_rjenkins(), the default mds_default_dir_hash
    """
    M = 0xffffffff

    def mix(a, b, c):
        a = (a - b - c) & M; a ^= c >> 13
        b = (b - c - a) & M; b = (b ^ (a << 8)) & M
        c = (c - a - b) & M; c ^= b >> 13
        a = (a - b - c) & M; a ^= c >> 12
        b = (b - c - a) & M; b = (b ^ (a << 16)) & M
        c = (c - a - b) & M; c ^= b >> 5
        a = (a - b - c) & M; a ^= c >> 3
        b = (b - c - a) & M; b = (b ^ (a << 10)) & M
        c = (c - a - b) & M; c ^= b >> 15
        return a, b, c

    k = name.encode()
    a = b = 0x9e3779b9
    c = 0
    i = 0
    while len(k) - i >= 12:
        a = (a + int.from_bytes(k[i:i+4], 'little')) & M
        b = (b + int.from_bytes(k[i+4:i+8], 'little')) & M
        c = (c + int.from_bytes(k[i+8:i+12], 'little')) & M
        a, b, c = mix(a, b, c)
        i += 12
    c = (c + len(k)) & M
    for j, byte in enumerate(k[i:]):
        if j < 4:
            a = (a + (byte << (8 * j))) & M
        elif j < 8:
            b = (b + (byte << (8 * (j - 4)))) & M
        else:
            # the first byte of c holds the length
            c = (c + (byte << (8 * (j - 7)))) & M
    return mix(a, b, c)[2]


def frag_value(name):
    """
    The 24 bit value that places a dentry in a dirfrag and orders it in
    readdir, see ceph_frag_value() and dentry_key_t.
    """
    return rjenkins(name) & 0xffffff


class TestReaddirCache(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    def _mds_requests(self):
        perf = self.fs.rank_asok(['perf', 'dump', 'mds_server'])['mds_server']
        return (perf['req_readdir_latency']['avgcount'],
                perf['req_getattr_latency']['avgcount'])

    def _configure_split(self, split_size, split_bits):
        conf = {
            'mds_bal_split_size': split_size,
            'mds_bal_split_bits': split_bits,
            'mds_bal_merge_size': 0,
            'mds_bal_fragment_interval': 1,
        }
        for k, v in conf.items():
            self.config_set('mds', k, v)
        # a directory only splits when a dentry is added to it
        for k, v in conf.items():
            self.wait_until_true(
                lambda: self.fs.rank_asok(['config', 'get', k])[k] == str(v),
                timeout=30)

    def _dirfrags(self, path):
        frags = self.fs.rank_asok(['dirfrag', 'ls', path])
        return [f['str'] for f in sorted(frags, key=lambda f: f['value'])]

    def _wait_for_dirfrags(self, path, frags):
        def done():
            current = self._dirfrags(path)
            log.info("dirfrags of %s: %s", path, current)
            return current == frags
        self.wait_until_true(done, timeout=60)

    def test_cached_listing_with_subdir(self):
        """
        A fragmented directory with a subdirectory is listed from the client's
        readdir cache again, with only the subdirectory's rstat refreshed,
        instead of falling back to reading the whole directory from the MDS.
        """
        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("Requires the libcephfs readdir cache (FUSE client)")
        conf = self.mount_a.admin_socket(['config', 'get', 'client_dirsize_rbytes'])
        if conf['client_dirsize_rbytes'] != 'true':
            self.skipTest("Requires client_dirsize_rbytes")

        split_size = 100
        nfiles = 500
        names = ["sub"] + ["file_{0}".format(i) for i in range(nfiles)]
        # one split into 8 frags, none of which gets big enough to split again
        counts = [0] * 8
        for name in names:
            counts[frag_value(name) >> 21] += 1
        self.assertLessEqual(max(counts), split_size)

        self._configure_split(split_size, 3)
        self.mount_a.run_shell(["mkdir", "-p", "dir/sub"])
        self.mount_a.create_n_files("dir/file", nfiles)
        self._wait_for_dirfrags("/dir", ["{0:x}/3".format(i << 21) for i in range(8)])
        expected = sorted(names)
        self.assertEqual(sorted(self.mount_a.ls("dir")), expected)

        # start from a client cache filled by a listing only
        self.mount_a.umount_wait()
        self.mount_a.mount_wait()
        self.assertEqual(sorted(self.mount_a.ls("dir")), expected)

        readdirs, getattrs = self._mds_requests()
        self.assertEqual(sorted(self.mount_a.ls("dir")), expected)
        new_readdirs, new_getattrs = self._mds_requests()
        log.info("second listing: %d readdir, %d getattr requests",
                 new_readdirs - readdirs, new_getattrs - getattrs)

        self.assertEqual(new_readdirs, readdirs)
        # the subdirectory's rstat, plus "." and ".."
        self.assertGreaterEqual(new_getattrs - getattrs, 1)
        self.assertLessEqual(new_getattrs - getattrs, 5)
