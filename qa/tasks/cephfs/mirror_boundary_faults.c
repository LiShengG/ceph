/*
 * QA-only LD_PRELOAD interposer for deterministic mirror boundary tests.
 * It calls the real libcephfs functions; no copying or sync logic lives here.
 * Build on the daemon host with gcc -shared -fPIC -pthread ... -ldl.
 * The opaque pointers and public structs below follow libcephfs.h's C ABI.
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

struct ceph_mount_info;
struct snap_metadata { const char *key; const char *value; };
struct snap_info {
  uint64_t id;
  size_t nr_snap_metadata;
  struct snap_metadata *snap_metadata;
};
struct ceph_file_blockdiff_info;
struct cblock { uint64_t offset; uint64_t len; };
struct ceph_file_blockdiff_changedblocks {
  uint64_t num_blocks;
  struct cblock *b;
};

/* mount+fd identifies the two clients even when their fd numbers coincide. */
struct fd_path {
  struct ceph_mount_info *mount;
  int fd;
  char path[4096];
  struct fd_path *next;
};
static struct fd_path *paths;
static pthread_mutex_t paths_lock = PTHREAD_MUTEX_INITIALIZER;

static const char *normalize(const char *path) {
  return strncmp(path, "./", 2) == 0 ? path + 2 : path;
}

static void record_path(struct ceph_mount_info *mount, int fd, const char *path) {
  struct fd_path *entry = calloc(1, sizeof(*entry));
  if (!entry) abort();
  entry->mount = mount;
  entry->fd = fd;
  snprintf(entry->path, sizeof(entry->path), "%s", normalize(path));
  pthread_mutex_lock(&paths_lock);
  entry->next = paths;
  paths = entry;
  pthread_mutex_unlock(&paths_lock);
}

static void forget_path(struct ceph_mount_info *mount, int fd) {
  pthread_mutex_lock(&paths_lock);
  struct fd_path **entry = &paths;
  while (*entry) {
    if ((*entry)->mount == mount && (*entry)->fd == fd) {
      struct fd_path *removed = *entry;
      *entry = removed->next;
      free(removed);
      break;
    }
    entry = &(*entry)->next;
  }
  pthread_mutex_unlock(&paths_lock);
}

static void lookup_path(struct ceph_mount_info *mount, int fd, char *path) {
  path[0] = '\0';
  pthread_mutex_lock(&paths_lock);
  for (struct fd_path *entry = paths; entry; entry = entry->next) {
    if (entry->mount == mount && entry->fd == fd) {
      snprintf(path, 4096, "%s", entry->path);
      break;
    }
  }
  pthread_mutex_unlock(&paths_lock);
}

static void resolve_path(struct ceph_mount_info *mount, int dirfd,
                         const char *path, char *resolved) {
  char parent[4096];
  lookup_path(mount, dirfd, parent);
  int len;
  if (path[0] == '/' || !parent[0]) {
    len = snprintf(resolved, 4096, "%s", normalize(path));
  } else {
    len = snprintf(resolved, 4096, "%s/%s", parent, normalize(path));
  }
  if (len < 0 || len >= 4096) abort();
}

static void event(const char *op, const char *path, int64_t offset, int result) {
  const char *dir = getenv("CEPHFS_MIRROR_QA_FAULT_DIR");
  if (!dir) return;
  char filename[4096], line[8192];
  snprintf(filename, sizeof(filename), "%s/events", dir);
  int fd = open(filename, O_CREAT | O_WRONLY | O_APPEND, 0600);
  if (fd < 0) abort();
  int len = snprintf(line, sizeof(line), "%s\t%s\t%lld\t%d\n", op,
                     normalize(path), (long long)offset, result);
  if (len < 0 || len >= (int)sizeof(line) || write(fd, line, len) != len) abort();
  close(fd);
}

/* Rules are atomically published and claimed exactly once. */
static int boundary(const char *op, const char *path, int64_t offset) {
  const char *dir = getenv("CEPHFS_MIRROR_QA_FAULT_DIR");
  if (!dir) return 0;
  char rule[4096], claimed[4096], hit[4096], release[4096];
  char expected_path[4096], action[16];
  long long expected_offset;
  snprintf(rule, sizeof(rule), "%s/%s.rule", dir, op);
  FILE *fp = fopen(rule, "r");
  if (!fp) return 0;
  int parsed = fscanf(fp, "%4095[^\n]\n%lld\n%15s", expected_path,
                      &expected_offset, action);
  fclose(fp);
  if (parsed != 3 || strcmp(expected_path, normalize(path)) != 0 ||
      (expected_offset != -1 && expected_offset != offset)) return 0;
  snprintf(claimed, sizeof(claimed), "%s/%s.claimed", dir, op);
  if (rename(rule, claimed) != 0) return 0;
  snprintf(hit, sizeof(hit), "%s/%s.hit", dir, op);
  fp = fopen(hit, "w");
  if (!fp) abort();
  fprintf(fp, "%s\n%lld\n%s\n", normalize(path), (long long)offset, action);
  fclose(fp);
  event(op, path, offset, strcmp(action, "error") == 0 ? -EIO : 0);
  if (strcmp(action, "error") == 0) return -EIO;
  if (strcmp(action, "observe") == 0) return 0;
  if (strcmp(action, "gate") != 0) abort();
  snprintf(release, sizeof(release), "%s/%s.release", dir, op);
  struct timespec start, now, tick = {0, 10000000};
  clock_gettime(CLOCK_MONOTONIC, &start);
  while (access(release, F_OK) != 0) {
    clock_gettime(CLOCK_MONOTONIC, &now);
    if (now.tv_sec - start.tv_sec >= 180) {
      event("gate_timeout", op, offset, -ETIMEDOUT);
      return -ETIMEDOUT;
    }
    nanosleep(&tick, NULL);
  }
  return 0;
}

#define REAL(name) __typeof__(&name) real = dlsym(RTLD_NEXT, #name); if (!real) abort()

int ceph_open(struct ceph_mount_info *mount, const char *path, int flags, mode_t mode) {
  REAL(ceph_open);
  int injected = boundary("before_open", path, -1);
  if (injected < 0) return injected;
  int result = real(mount, path, flags, mode);
  if (result >= 0) {
    record_path(mount, result, path);
    injected = boundary("after_open", path, -1);
    if (injected < 0) {
      int (*close_real)(struct ceph_mount_info *, int) = dlsym(RTLD_NEXT, "ceph_close");
      if (!close_real) abort();
      close_real(mount, result);
      forget_path(mount, result);
      return injected;
    }
  }
  return result;
}

int ceph_openat(struct ceph_mount_info *mount, int dirfd, const char *path,
                int flags, mode_t mode) {
  REAL(ceph_openat);
  int result = real(mount, dirfd, path, flags, mode);
  if (result >= 0) {
    char resolved[4096];
    resolve_path(mount, dirfd, path, resolved);
    record_path(mount, result, resolved);
    if ((flags & O_ACCMODE) == O_WRONLY) {
      int injected = boundary("after_write_open", resolved, -1);
      if (injected < 0) {
        int (*close_real)(struct ceph_mount_info *, int) = dlsym(RTLD_NEXT, "ceph_close");
        if (!close_real) abort();
        close_real(mount, result);
        forget_path(mount, result);
        return injected;
      }
    }
  }
  return result;
}

int ceph_close(struct ceph_mount_info *mount, int fd) {
  REAL(ceph_close);
  forget_path(mount, fd);
  return real(mount, fd);
}

int ceph_preadv(struct ceph_mount_info *mount, int fd, const struct iovec *iov,
                int count, int64_t offset) {
  REAL(ceph_preadv);
  char path[4096];
  lookup_path(mount, fd, path);
  int result = boundary("read", path, offset);
  if (result >= 0) result = real(mount, fd, iov, count, offset);
  event("read_result", path, offset, result);
  return result;
}

int ceph_pwritev(struct ceph_mount_info *mount, int fd, const struct iovec *iov,
                 int count, int64_t offset) {
  REAL(ceph_pwritev);
  char path[4096];
  lookup_path(mount, fd, path);
  int result = boundary("write", path, offset);
  if (result >= 0) result = real(mount, fd, iov, count, offset);
  event("write_result", path, offset, result);
  if (result >= 0) {
    int injected = boundary("after_write", path, offset);
    if (injected < 0) return injected;
  }
  return result;
}

int ceph_mkdirat(struct ceph_mount_info *mount, int dirfd, const char *path, mode_t mode) {
  REAL(ceph_mkdirat);
  int result = real(mount, dirfd, path, mode);
  if (result == 0) {
    char resolved[4096];
    resolve_path(mount, dirfd, path, resolved);
    int injected = boundary("after_mkdir", resolved, -1);
    if (injected < 0) return injected;
  }
  return result;
}

int ceph_unlinkat(struct ceph_mount_info *mount, int dirfd, const char *path, int flags) {
  REAL(ceph_unlinkat);
  int result = real(mount, dirfd, path, flags);
  if (result == 0) {
    char resolved[4096];
    resolve_path(mount, dirfd, path, resolved);
    int injected = boundary("after_unlink", resolved, -1);
    if (injected < 0) return injected;
  }
  return result;
}

/* "/root/./a" -> "/root/a", as resolve_path() records relative paths. */
static void collapse_dot_components(const char *path, char *out) {
  size_t len = 0;
  while (*path) {
    if (strncmp(path, "/./", 3) == 0) {
      path += 2;
      continue;
    }
    if (len + 1 >= 4096) abort();
    out[len++] = *path++;
  }
  out[len] = '\0';
}

int ceph_rename(struct ceph_mount_info *mount, const char *from, const char *to) {
  REAL(ceph_rename);
  char path[4096];
  collapse_dot_components(from, path);
  int injected = boundary("before_rename", path, -1);
  if (injected < 0) return injected;
  int result = real(mount, from, to);
  event("rename_result", path, -1, result);
  if (result == 0) {
    injected = boundary("after_rename", path, -1);
    if (injected < 0) return injected;
  }
  return result;
}

int ceph_mksnap(struct ceph_mount_info *mount, const char *path, const char *name,
                mode_t mode, struct snap_metadata *metadata, size_t count) {
  REAL(ceph_mksnap);
  char snap_path[4096];
  snprintf(snap_path, sizeof(snap_path), "%s/.snap/%s", path, name);
  int injected = boundary("mksnap", snap_path, -1);
  if (injected < 0) return injected;
  return real(mount, path, name, mode, metadata, count);
}

int ceph_file_blockdiff(struct ceph_file_blockdiff_info *info,
                        struct ceph_file_blockdiff_changedblocks *blocks) {
  REAL(ceph_file_blockdiff);
  int result = real(info, blocks);
  if (result >= 0) {
    event("block_count", "", blocks->num_blocks, result);
    for (uint64_t i = 0; i < blocks->num_blocks; ++i) {
      event("block", "", blocks->b[i].offset, blocks->b[i].len);
    }
  }
  return result;
}
