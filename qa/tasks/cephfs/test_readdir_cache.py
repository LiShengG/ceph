import json
import logging
from textwrap import dedent

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.fuse_mount import FuseMount

log = logging.getLogger(__name__)

# dir_result_t offsets, see src/client/Client.h
FPOS_SHIFT = 28
FPOS_HASH = 0xff << (FPOS_SHIFT + 24)


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


def hash_order(names):
    return sorted(names, key=lambda n: (frag_value(n), n))


def pick_names(prefix, count, accept):
    """
    The first 'count' names prefix0, prefix1... whose frag_value() is accepted
    """
    names = []
    i = 0
    while len(names) < count:
        name = "{0}{1}".format(prefix, i)
        i += 1
        if accept(frag_value(name)):
            names.append(name)
    return names


class TestReaddirCache(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1
    maxDiff = None

    # A readdir reply carries an inode's xattrs until the client has seen
    # them, and holds at most (512K + mds_max_xattr_pairs_size) bytes: 9
    # files with such an xattr fill one page, a 10th does not fit.
    XATTR_SIZE = 60000
    PAGE_FILES = 9

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

    def _libcephfs(self, script, **args):
        """
        Run a python script against libcephfs, with mount() giving a new
        client instance, and return what it prints as json.
        """
        prelude = dedent("""
            import json
            import os
            import cephfs

            def mount():
                fs = cephfs.LibCephFS(conffile='')
                fs.mount(filesystem_name={fs_name!r})
                return fs

            def entry(fs, dirp):
                de = fs.readdir(dirp)
                if de is None:
                    return None
                return de.d_name.decode(), de.d_off

            def listing(fs, dirp):
                names = []
                while True:
                    de = entry(fs, dirp)
                    if de is None:
                        return names
                    if de[0] not in ('.', '..'):
                        names.append(de[0])
            """).format(fs_name=self.fs.name)
        args_code = "".join("{0} = {1!r}\n".format(k, v) for k, v in args.items())
        out = self.mount_a.run_python(prelude + args_code + dedent(script))
        return json.loads(out.splitlines()[-1])

    def _create(self, path, names, filled=()):
        """
        Create empty files from a client that goes away afterwards, giving
        those in 'filled' an xattr of XATTR_SIZE.
        """
        self._libcephfs("""
            fs = mount()
            fs.mkdir(path, 0o755)
            for name in names:
                fs.close(fs.open(path + "/" + name, os.O_CREAT | os.O_WRONLY, 0o644))
            for name in filled:
                fs.setxattr(path + "/" + name, "user.fill", b"x" * xattr_size, 0)
            fs.unmount()
            fs.shutdown()
            print(json.dumps(None))
            """, path=path, names=list(names), filled=list(filled),
            xattr_size=self.XATTR_SIZE)

    def _readdir_requests_to_list(self, path):
        """
        Readdir requests a new client needs to list a directory.
        """
        readdirs, _ = self._mds_requests()
        self._libcephfs("""
            fs = mount()
            print(json.dumps(listing(fs, fs.opendir(path))))
            """, path=path)
        return self._mds_requests()[0] - readdirs

    def test_cached_listing_with_subdirs(self):
        """
        Listing a cached, fragmented directory again refreshes the rstat of
        its subdirectories by reading on from the MDS, whose replies carry
        many of them each, not by a getattr per subdirectory.
        """
        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("Requires the libcephfs readdir cache (FUSE client)")
        conf = self.mount_a.admin_socket(['config', 'get', 'client_dirsize_rbytes'])
        if conf['client_dirsize_rbytes'] != 'true':
            self.skipTest("Requires client_dirsize_rbytes")

        split_size = 100
        nsubdirs, nfiles = 100, 400
        subdirs = ["sub_{0}".format(i) for i in range(nsubdirs)]
        names = subdirs + ["file_{0}".format(i) for i in range(nfiles)]
        # one split into 8 frags, none of which gets big enough to split again
        counts = [0] * 8
        for name in names:
            counts[frag_value(name) >> 21] += 1
        self.assertLessEqual(max(counts), split_size)

        self._configure_split(split_size, 3)
        self.mount_a.run_shell(["mkdir", "-p"] + ["dir/" + d for d in subdirs])
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

        # at most one reply per frag, and "." and ".." aside no getattr
        self.assertGreaterEqual(new_readdirs - readdirs, 1)
        self.assertLessEqual(new_readdirs - readdirs, 8)
        self.assertLessEqual(new_getattrs - getattrs, 5)

    def test_resume_next_frag_after_hash_collision(self):
        """
        A stream that read a frag up to its end moves on to the next frag
        from its last name, not from the start of that name's hash.  When
        another stream's pass stopped between two dentries with that hash, the
        reply to the first stream must not extend the pass over the second.
        """
        path = "/collision"
        # two names with the same hash, the last dentries of frag 0*
        seen = {}
        i = 0
        while True:
            name = "c{0}".format(i)
            i += 1
            v = frag_value(name)
            if 0x400000 <= v < 0x800000:
                if v in seen:
                    first, last = sorted([seen[v], name])
                    collision = v
                    break
                seen[v] = name

        fillers = pick_names("fill", self.PAGE_FILES, lambda v: v < collision)
        tail = pick_names("tail", self.PAGE_FILES + 2, lambda v: v >= 0x800000)
        names = fillers + [first, last] + tail
        order = hash_order(names)
        self.assertEqual(order[self.PAGE_FILES:self.PAGE_FILES + 2], [first, last])

        # frag 0* holds the fillers and the collision, frag 1* the tail
        self._configure_split(len(names) - 1, 1)
        self._create(path, names, fillers)
        self._wait_for_dirfrags(path, ["0/1", "800000/1"])

        res = self._libcephfs("""
            w = mount()
            order = listing(w, w.opendir(path))

            r = mount()
            r.stat(path)

            # b reads frag 0* from the collision on, up to its end
            b = r.opendir(path)
            r.seekdir(b, hash_bit | (collision << shift) | 2)
            b_names = [entry(r, b)[0], entry(r, b)[0]]

            # make the last collision dentry take up a page again; until the
            # update is journaled, readdir encodes it without the xattrs
            w.setxattr(path + "/" + last, "user.fill", b"y" * xattr_size, 0)
            w.sync_fs()

            # a starts a pass: its first page stops before the last collision
            # dentry
            a = r.opendir(path)
            a_names = [entry(r, a)[0] for _ in range(3)]

            # b moves on to frag 1*, the pass sees the end of the directory
            b_names.append(entry(r, b)[0])

            c = r.opendir(path)
            print(json.dumps({
                "order": order,
                "b": b_names,
                "a": a_names[2:] + listing(r, a),
                "c": listing(r, c),
            }))
            """, path=path, last=last, collision=collision,
            hash_bit=FPOS_HASH, shift=FPOS_SHIFT, xattr_size=self.XATTR_SIZE)

        self.assertEqual(res["order"], order)
        self.assertEqual(res["b"], [first, last, order[self.PAGE_FILES + 2]])
        # frag 0* takes two pages, split between the two collision dentries
        self.assertEqual(self._readdir_requests_to_list(path), 3)
        self.assertEqual({"a": sorted(res["a"]), "c": sorted(res["c"])},
                         {"a": sorted(names), "c": sorted(names)})

    def test_seek_forward_into_later_frag(self):
        """
        A stream seeking forward keeps its last name, which may be in an
        earlier frag than the one it reads next.  The reply lists that frag
        from its start, so it must not extend a pass that has not seen the
        frags in between.
        """
        path = "/seek"
        per_frag = 5
        names = []
        for i in range(4):
            names += pick_names("f{0}_".format(i), per_frag,
                                lambda v, i=i: v >> 22 == i)
        order = hash_order(names)
        self.assertEqual(len(set(frag_value(n) for n in names)), len(names))

        self._configure_split(len(names) - 1, 2)
        self._create(path, names)
        self._wait_for_dirfrags(path, ["0/2", "400000/2", "800000/2", "c00000/2"])

        # the first dentry of frag 2*
        cookie = FPOS_HASH | (frag_value(order[2 * per_frag]) << FPOS_SHIFT) | 2
        res = self._libcephfs("""
            r = mount()
            r.stat(path)

            # d reads frag 0*, the pass reaches the start of frag 1*
            d = r.opendir(path)
            d_names = [entry(r, d)[0] for _ in range(3)][2:]

            # d skips frag 1*
            r.seekdir(d, cookie)
            d_names += listing(r, d)

            c = r.opendir(path)
            print(json.dumps({"d": d_names, "c": listing(r, c)}))
            """, path=path, cookie=cookie)

        self.assertEqual(res["d"], order[:1] + order[2 * per_frag:])
        self.assertEqual(res["c"], order)

    def test_seekdir_into_kept_buffer(self):
        """
        A stream that went on from its buffer to the end of the directory
        through the readdir cache no longer knows where the MDS would continue
        after that buffer.  Seeking back into the buffer must still list
        the rest of the directory.
        """
        path = "/pages"
        names = ["file{0}".format(i) for i in range(self.PAGE_FILES + 3)]
        self._create(path, names, names)
        self.assertEqual(self._dirfrags(path), ["0/0"])
        # a single frag of two pages
        self.assertEqual(self._readdir_requests_to_list(path), 2)

        res = self._libcephfs("""
            w = mount()
            r = mount()
            r.stat(path)

            # h1 keeps the first page
            h1 = r.opendir(path)
            entry(r, h1)
            entry(r, h1)
            h1_names = [entry(r, h1)[0]]

            # h2 lists the directory, which becomes complete
            h2 = r.opendir(path)
            order = listing(r, h2)

            # h1 reads on to the end from the readdir cache
            cookies = {}
            while True:
                de = entry(r, h1)
                if de is None:
                    break
                h1_names.append(de[0])
                cookies[de[0]] = de[1]

            # another client drops the directory cache of r
            w.close(w.open(path + "/tmp", os.O_CREAT | os.O_WRONLY, 0o644))
            w.unlink(path + "/tmp")

            # back into the first page
            r.seekdir(h1, cookies[order[2]])
            print(json.dumps({
                "order": order,
                "h1": h1_names,
                "again": listing(r, h1),
            }))
            """, path=path)

        order = res["order"]
        self.assertEqual(sorted(order), sorted(names))
        self.assertEqual(res["h1"], order)
        self.assertEqual(res["again"], order[3:])
