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


def pick_collision():
    """Two names in the lower half of the hash space, in readdir order."""
    seen = {}
    for i in range(1 << 18):
        name = "c{0}".format(i)
        value = frag_value(name)
        if 0x400000 <= value < 0x800000:
            if value in seen:
                first, last = sorted([seen[value], name])
                return first, last, value
            seen[value] = name
    raise AssertionError("Could not find a directory hash collision")


class ReaddirCacheTestCase(CephFSTestCase):
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
        client instance, and return what it prints as json.  The instances
        are shut down as the script exits, unless it did so: a client that
        just goes away keeps its session, and its caps, until the MDS times
        it out, which stalls the next client that needs them.
        """
        prelude = dedent("""
            import atexit
            import json
            import os
            import cephfs

            def mount(**conf):
                fs = cephfs.LibCephFS(conffile='')
                for key, value in conf.items():
                    fs.conf_set(key, str(value))
                fs.mount(filesystem_name={fs_name!r})
                atexit.register(fs.shutdown)
                return fs

            def mds_requests(fs, rank=0):
                command = json.dumps(dict(prefix='perf dump',
                                          logger='mds_server', format='json'))
                ret, out, err = fs.mds_command(
                    {fs_name!r} + ':' + str(rank), command, b'')
                assert ret == 0, (ret, err)
                perf = json.loads(out)['mds_server']
                return (perf['req_readdir_latency']['avgcount'],
                        perf['req_getattr_latency']['avgcount'])

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

    def _test_rbytes_refresh(self, restart, path="/rbytes", ranks=(0,),
                            readdir_rank=0, create_parent=True):
        """Check the cached size after readdir without refreshing it by stat."""
        names = ["sub_{0}".format(i) for i in range(16)]
        initial_sizes = {name: 4096 * (i + 1) for i, name in enumerate(names)}
        changed_sizes = {name: (1024 if i % 2 else 8192) * (i + 1)
                         for i, name in enumerate(names)}
        res = self._libcephfs("""
            import time

            def resize(fs, sizes):
                for name, size in sizes.items():
                    fd = fs.open(path + '/' + name + '/file',
                                 os.O_CREAT | os.O_WRONLY, 0o644)
                    try:
                        fs.ftruncate(fd, size)
                    finally:
                        fs.close(fd)
                fs.sync_fs()
                # Only the writer polls: refreshing the reader here would
                # hide a failure to refresh rstat during the next listing.
                deadline = time.monotonic() + 90
                while True:
                    observed = {name: int(fs.getxattr(path + '/' + name,
                                                      'ceph.dir.rbytes'))
                                for name in sizes}
                    if observed == sizes:
                        return
                    if time.monotonic() >= deadline:
                        raise AssertionError(('rbytes did not propagate',
                                              observed, sizes))
                    time.sleep(0.1)

            def cached_size(fs, name):
                return fs.statx(path + '/' + name, cephfs.CEPH_STATX_SIZE,
                                cephfs.AT_STATX_DONT_SYNC)['size']

            def sizes_from_listing(fs, handle):
                result = []
                while True:
                    de = entry(fs, handle)
                    if de is None:
                        return result
                    if de[0] not in ('.', '..'):
                        result.append((de[0], cached_size(fs, de[0])))

            w = mount(client_dirsize_rbytes='true')
            r = mount(client_dirsize_rbytes='true')
            handle = None
            try:
                if create_parent:
                    w.mkdir(path, 0o755)
                for name in names:
                    w.mkdir(path + '/' + name, 0o755)
                resize(w, initial_sizes)
                handle = r.opendir(path)
                initial = sizes_from_listing(r, handle)

                resize(w, changed_sizes)
                cached = {name: cached_size(r, name) for name in names}
                before = {rank: mds_requests(w, rank) for rank in ranks}
                if restart == 'opendir':
                    r.closedir(handle)
                    handle = None
                    handle = r.opendir(path)
                elif restart == 'rewinddir':
                    r.rewinddir(handle)
                else:
                    assert restart == 'seekdir'
                    r.seekdir(handle, 0)
                refreshed = sizes_from_listing(r, handle)
                after = {rank: mds_requests(w, rank) for rank in ranks}
                print(json.dumps(dict(initial=initial, cached=cached,
                                      refreshed=refreshed,
                                      before=before, after=after)))
            finally:
                if handle is not None:
                    r.closedir(handle)
                r.shutdown()
                w.shutdown()
            """, path=path, names=names, initial_sizes=initial_sizes,
            changed_sizes=changed_sizes, restart=restart, ranks=list(ranks),
            create_parent=create_parent)

        self.assertEqual(sorted(res['initial']),
                         sorted([name, size] for name, size in initial_sizes.items()))
        self.assertEqual(res['cached'], initial_sizes)
        self.assertEqual(sorted(res['refreshed']),
                         sorted([name, size] for name, size in changed_sizes.items()))
        getattrs = 0
        for rank in ranks:
            before, after = res['before'][str(rank)], res['after'][str(rank)]
            readdir_delta, getattr_delta = [b - a for a, b in zip(before, after)]
            log.info("%s on rank %s: %s readdir, %s getattr requests",
                     restart, rank, readdir_delta, getattr_delta)
            self.assertEqual(readdir_delta, 1 if rank == readdir_rank else 0)
            getattrs += getattr_delta
        # Refresh all 16 directories with one READDIR. The only GETATTRs are
        # those for '.' and '..', none for a child directory.
        self.assertLessEqual(getattrs, 2)


class TestReaddirCache(ReaddirCacheTestCase):
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
        first, last, collision = pick_collision()

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

    def _test_stale_collision_cursor(self, seek_into_buffer):
        path = "/stale_collision"
        first, last, collision = pick_collision()
        fillers = pick_names("fill", self.PAGE_FILES, lambda v: v < collision)
        tail = pick_names("tail", 1, lambda v: v > collision)[0]
        original = hash_order(fillers + [last, tail])
        expected = hash_order(fillers + [first, last, tail])
        # Nine large xattrs and the empty collision dentry fit on the first
        # page. The large tail dentry must be returned by a second reply.
        self._create(path, original, fillers + [tail])
        self.assertEqual(self._dirfrags(path), ["0/0"])
        self.assertEqual(self._readdir_requests_to_list(path), 2)

        res = self._libcephfs("""
            w = mount()
            r = mount()
            handles = []
            try:
                a = r.opendir(path)
                handles.append(a)
                before = mds_requests(w)[0]
                prefix = [entry(r, a) for _ in range(page_files + 3)]
                first_page_requests = mds_requests(w)[0] - before
                cookie = prefix[-1][1]
                assert prefix[-1][0] == last, prefix
                assert cookie == hash_bit | (collision << shift) | 3, prefix[-1]

                original_again = None
                old_tail = None
                if seek_into_buffer:
                    # Let another handle complete the cache; a reads to EOF
                    # from that cache while retaining its original first page.
                    complete = r.opendir(path)
                    handles.append(complete)
                    original_again = listing(r, complete)
                    old_tail = listing(r, a)

                # Invalidate the directory, then make the first page stop
                # between the two colliding names. Refresh the filler xattrs
                # too: r has already seen their old versions. Replace them:
                # otherwise the old value counts against
                # mds_max_xattr_pairs_size and setxattr fails with ENOSPC.
                w.close(w.open(path + '/' + first, os.O_CREAT | os.O_WRONLY, 0o644))
                for name in fillers:
                    w.setxattr(path + '/' + name, 'user.fill', b'y' * xattr_size,
                               os.XATTR_REPLACE)
                w.setxattr(path + '/' + last, 'user.fill', b'y' * xattr_size, 0)
                w.sync_fs()

                b = r.opendir(path)
                handles.append(b)
                before = mds_requests(w)[0]
                new_prefix = [entry(r, b) for _ in range(page_files + 3)]
                new_page_requests = mds_requests(w)[0] - before
                assert new_prefix[-1][0] == first, new_prefix
                assert new_prefix[-1][1] == cookie, new_prefix[-1]

                if seek_into_buffer:
                    r.seekdir(a, cookie)
                before = mds_requests(w)[0]
                resumed = listing(r, a)
                resume_requests = mds_requests(w)[0] - before

                # Read c before allowing b to continue: b could otherwise
                # repair the incomplete cache that a just marked complete.
                c = r.opendir(path)
                handles.append(c)
                fresh = listing(r, c)
                print(json.dumps(dict(prefix=prefix, new_prefix=new_prefix,
                                      original_again=original_again, old_tail=old_tail,
                                      resumed=resumed, fresh=fresh,
                                      requests=[first_page_requests,
                                                new_page_requests, resume_requests])))
            finally:
                for handle in handles:
                    r.closedir(handle)
                r.shutdown()
                w.shutdown()
            """, path=path, first=first, last=last, collision=collision,
            fillers=fillers, page_files=self.PAGE_FILES,
            hash_bit=FPOS_HASH, shift=FPOS_SHIFT, xattr_size=self.XATTR_SIZE,
            seek_into_buffer=seek_into_buffer)

        self.assertEqual([de[0] for de in res['prefix']], ['.', '..'] + original[:-1])
        self.assertEqual([de[0] for de in res['new_prefix']],
                         ['.', '..'] + hash_order(fillers + [first]))
        self.assertEqual(res['requests'], [1, 1, 1])
        if seek_into_buffer:
            self.assertEqual(res['original_again'], original)
            self.assertEqual(res['old_tail'], [tail])
        self.assertEqual(res['resumed'], [tail])
        self.assertEqual(res['fresh'], expected)

    def test_stale_collision_cursor_does_not_complete_new_pass(self):
        """An old ordinal must not bridge a gap in a new directory generation."""
        self._test_stale_collision_cursor(seek_into_buffer=False)

    def test_seekdir_kept_buffer_after_collision_insert(self):
        """Seeking into an old buffer must not make its cursor current again."""
        self._test_stale_collision_cursor(seek_into_buffer=True)

    def test_rbytes_refresh_on_opendir(self):
        self._test_rbytes_refresh('opendir')

    def test_rbytes_refresh_on_rewinddir(self):
        self._test_rbytes_refresh('rewinddir')

    def test_rbytes_refresh_on_seekdir_zero(self):
        self._test_rbytes_refresh('seekdir')

    def test_reopen_per_page_across_invalidation(self):
        """Reopen with an NFS-style cookie after a namespace change."""
        first, last, collision = pick_collision()
        fillers = pick_names("fill", self.PAGE_FILES, lambda v: v < collision)
        tail = pick_names("tail", self.PAGE_FILES + 2, lambda v: v > collision)
        names = hash_order(fillers + [last] + tail)
        for mutation in ('create', 'unlink', 'rename'):
            with self.subTest(mutation=mutation):
                path = "/reopen_" + mutation
                self._create(path, names, fillers + tail)
                self.assertEqual(self._dirfrags(path), ["0/0"])
                res = self._libcephfs("""
                    w = mount()
                    r = mount()
                    try:
                        handle = r.opendir(path)
                        try:
                            prefix = [entry(r, handle) for _ in range(page_files + 3)]
                            cookie = prefix[-1][1]
                            assert prefix[-1][0] == last, prefix
                        finally:
                            r.closedir(handle)

                        if mutation == 'create':
                            w.close(w.open(path + '/' + first,
                                           os.O_CREAT | os.O_WRONLY, 0o644))
                        elif mutation == 'unlink':
                            w.unlink(path + '/' + removed)
                        else:
                            w.rename(path + '/' + removed, path + '/' + first)
                        w.sync_fs()

                        continued = []
                        # Bound the loop so a repeated cookie fails promptly.
                        for _ in range(len(names) + 3):
                            handle = r.opendir(path)
                            eof = False
                            previous_cookie = cookie
                            try:
                                r.seekdir(handle, cookie)
                                for _ in range(3):
                                    de = entry(r, handle)
                                    if de is None:
                                        eof = True
                                        break
                                    continued.append(de[0])
                                    cookie = de[1]
                            finally:
                                r.closedir(handle)
                            if eof:
                                break
                            assert cookie != previous_cookie, (cookie, continued)
                        else:
                            raise AssertionError(('listing did not end', continued))

                        handle = r.opendir(path)
                        try:
                            fresh = listing(r, handle)
                        finally:
                            r.closedir(handle)
                        print(json.dumps(dict(prefix=prefix, continued=continued,
                                              fresh=fresh)))
                    finally:
                        r.shutdown()
                        w.shutdown()
                    """, path=path, names=names, first=first, last=last,
                    removed=fillers[0], page_files=self.PAGE_FILES,
                    mutation=mutation)

                self.assertEqual([de[0] for de in res['prefix']],
                                 ['.', '..'] + names[:self.PAGE_FILES + 1])
                # Cookies in the collision group may change during mutation,
                # so its names may be skipped or returned again. Nothing
                # before it may come back, and the untouched tail must follow
                # it, each name exactly once.
                group = [name for name in res['continued'] if name in (first, last)]
                self.assertEqual(len(group), len(set(group)))
                self.assertEqual(res['continued'], group + hash_order(tail))
                expected = list(names)
                if mutation in ('unlink', 'rename'):
                    expected.remove(fillers[0])
                if mutation in ('create', 'rename'):
                    expected.append(first)
                self.assertEqual(res['fresh'], hash_order(expected))


class TestReaddirCacheMultimds(ReaddirCacheTestCase):
    MDSS_REQUIRED = 2

    def test_rbytes_refresh_with_parent_on_other_rank(self):
        """
        Refresh directory sizes with the listing and its parent on two ranks.
        READDIR is served by the auth of the listed frag, so every child
        directory in its reply comes from its auth MDS. A reply from a
        non-auth MDS is covered by TestClient.ReaddirRstatReplyProvenance.
        """
        self.fs.set_max_mds(2)
        status = self.fs.wait_for_daemons()
        self.mount_a.run_shell(['mkdir', '-p', 'rbytes/entries'])
        self.mount_a.setfattr('rbytes', 'ceph.dir.pin', '1')
        self._wait_subtrees([('/rbytes', 1)], status=status, rank=1)
        # /rbytes's inode belongs to rank 0, while entries and the child
        # inodes returned by READDIR belong to rank 1.
        self._test_rbytes_refresh('rewinddir', path='/rbytes/entries',
                                 ranks=(0, 1), readdir_rank=1, create_parent=False)
