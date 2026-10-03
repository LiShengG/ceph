"""Test-only libcephfs fault barriers; requires gcc on the mirror host."""
import json
import os
import signal
from io import StringIO

from teuthology.exceptions import CommandCrashedError, CommandFailedError
from teuthology.orchestra import run


class MirrorBoundaryFaults:
    def __init__(self, test):
        self.test = test
        daemons = list(test.ctx.daemons.iter_daemons_of_role('cephfs-mirror'))
        test.assertEqual(1, len(daemons))
        self.daemon = daemons[0]
        self.remote = self.daemon.remote
        self.original_args = list(self.daemon.command_kwargs['args'])
        self.configs = {}
        self.directory = None

    def command(self, args, **kwargs):
        kwargs.setdefault('stdout', StringIO())
        kwargs.setdefault('stderr', StringIO())
        kwargs.setdefault('timeout', 30)
        return self.remote.run(args=args, **kwargs)

    def __enter__(self):
        try:
            self.directory = self.command(['mktemp', '-d']).stdout.getvalue().strip()
            source = os.path.join(os.path.dirname(__file__), 'mirror_boundary_faults.c')
            with open(source) as stream:
                self.remote.write_file(f'{self.directory}/faults.c', stream.read())
            # Fail explicitly if the suite's declared compiler dependency is absent.
            self.command(['gcc', '-shared', '-fPIC', '-O2', '-Wall', '-Wextra',
                          '-Werror', '-pthread', '-o', f'{self.directory}/faults.so',
                          f'{self.directory}/faults.c', '-ldl'], timeout=120)
            args = list(self.original_args)
            index = args.index('cephfs-mirror')
            args[index:index] = ['env', f'LD_PRELOAD={self.directory}/faults.so',
                                f'CEPHFS_MIRROR_QA_FAULT_DIR={self.directory}']
            self.daemon.command_kwargs['args'] = args
            self.restart()
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, exc_type, exc, traceback):
        # Kill before releasing gates: a failed assertion must not accidentally
        # let an invalid snapshot commit while cleaning up the test.
        try:
            self.stop()
        finally:
            self.daemon.command_kwargs['args'] = self.original_args
            try:
                for name, old_value in self.configs.items():
                    if old_value is None:
                        self.test.config_rm('client.mirror', name)
                    else:
                        self.test.config_set('client.mirror', name, old_value)
            finally:
                try:
                    if self.directory:
                        self.command(['rm', '-rf', self.directory])
                finally:
                    self.daemon.start()
                    self.wait_ready()

    def set_config(self, name, value):
        if name not in self.configs:
            current = json.loads(self.test.get_ceph_cmd_stdout('config', 'dump',
                                                              '--format=json'))
            self.configs[name] = next((item['value'] for item in current
                                       if item['section'] == 'client.mirror' and
                                       item['name'] == name), None)
        self.test.config_set('client.mirror', name, value)
        self.test.mirror_daemon_command('set boundary test config',
                                        'config', 'set', name, str(value))

    def stop(self):
        if not self.daemon.running():
            return
        pid = self.test.get_mirror_daemon_pid()
        self.daemon.signal(signal.SIGKILL, silent=True)
        self.command(['kill', '-KILL', pid], check_status=False)
        self.test.wait_for_mirror_daemon_stop(pid)
        try:
            run.wait([self.daemon.proc], timeout=30)
        except (CommandCrashedError, CommandFailedError):
            pass
        finally:
            self.daemon.reset()

    def wait_ready(self):
        def ready():
            try:
                self.test.mirror_daemon_command('wait for restarted mirror', 'version')
                return True
            except CommandFailedError:
                return False
        self.test.wait_until_true(ready, timeout=120, period=1)

    def restart(self):
        self.stop()
        self.daemon.start()
        self.wait_ready()

    def arm(self, operation, path, offset=-1, action='gate'):
        self.test.assertIn(action, ('gate', 'error', 'observe'))
        self.test.assertNotIn('\n', path)
        prefix = f'{self.directory}/{operation}'
        self.command(['rm', '-f', prefix + '.claimed', prefix + '.hit',
                      prefix + '.release'])
        self.remote.write_file(prefix + '.next', f'{path}\n{offset}\n{action}\n')
        self.command(['mv', prefix + '.next', prefix + '.rule'])

    def hit(self, operation):
        return self.command(['test', '-f', f'{self.directory}/{operation}.hit'],
                            check_status=False).exitstatus == 0

    def wait_hit(self, operation):
        self.test.wait_until_true(lambda: self.hit(operation), timeout=120, period=1)

    def release(self, operation):
        self.command(['touch', f'{self.directory}/{operation}.release'])

    def disarm(self, operation):
        self.command(['rm', '-f', f'{self.directory}/{operation}.rule'])

    def events(self):
        result = self.command(['cat', f'{self.directory}/events'], check_status=False)
        if result.exitstatus != 0:
            return []
        return [(op, path, int(offset), int(result))
                for op, path, offset, result in
                (line.split('\t') for line in result.stdout.getvalue().splitlines())]

    def clear_events(self):
        self.command(['truncate', '-s', '0', f'{self.directory}/events'])

    def wait_failed_or_snapshot(self, directory, peer_uuid):
        def completed():
            if self.hit('mksnap'):
                return True
            try:
                status = self.test.dir_status_from_asok(
                    self.test.primary_fs_name, self.test.primary_fs_id,
                    directory, peer_uuid)
                return status['state'] == 'failed'
            except (CommandFailedError, KeyError):
                return False
        self.test.wait_until_true(completed, timeout=120, period=1)
        self.test.assertNotIn('gate_timeout', [item[0] for item in self.events()])


def snapshot_info(test, mount, filesystem, directory, snapshot):
    """Read both the source ID and the remote primary_snap_id through libcephfs."""
    script = f'''
import cephfs
import json
client = cephfs.LibCephFS(conffile={mount.config_path!r},
                        auth_id={mount.client_id!r})
client.mount(filesystem_name={filesystem.encode()!r})
try:
    print(json.dumps(client.snap_info({('/' + directory + '/.snap/' + snapshot)!r})))
finally:
    client.shutdown()
'''
    return json.loads(mount.run_python(script))


def tree_manifest(mount, root):
    """Compare paths, inode types, mode bits, sizes and complete file contents."""
    script = f'''
import hashlib
import json
import os
import stat
root = {os.path.join(mount.mountpoint, root)!r}
assert os.path.isdir(root), f'missing manifest root: {{root}}'
entries = []
def failed_walk(error):
    raise error
for current, dirs, files in os.walk(root, followlinks=False, onerror=failed_walk):
    for name in sorted(dirs + files):
        path = os.path.join(current, name)
        st = os.lstat(path)
        kind = stat.S_IFMT(st.st_mode)
        value = None
        if stat.S_ISREG(st.st_mode):
            digest = hashlib.sha256()
            with open(path, 'rb') as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b''):
                    digest.update(block)
            value = [st.st_size, digest.hexdigest()]
        elif stat.S_ISLNK(st.st_mode):
            value = os.readlink(path)
        entries.append([os.path.relpath(path, root), kind, stat.S_IMODE(st.st_mode), value])
print(json.dumps(sorted(entries)))
'''
    return json.loads(mount.run_python(script))
