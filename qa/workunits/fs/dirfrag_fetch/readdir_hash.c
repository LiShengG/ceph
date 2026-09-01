// SPDX-License-Identifier: LGPL-2.1-or-later
/*
 * Traverse a directory without statting, sorting, or printing its entries.
 * The output is one JSON object suitable for the dirfrag fetch benchmark.
 */

#include <dirent.h>
#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static uint64_t fnv1a(const unsigned char *p, size_t n)
{
  uint64_t h = UINT64_C(14695981039346656037);
  while (n--) {
    h ^= *p++;
    h *= UINT64_C(1099511628211);
  }
  return h;
}

static uint64_t rotl64(uint64_t v, unsigned int shift)
{
  shift &= 63;
  return shift ? (v << shift) | (v >> (64 - shift)) : v;
}

static uint64_t elapsed_ns(const struct timespec *start,
                           const struct timespec *finish)
{
  return (uint64_t)(finish->tv_sec - start->tv_sec) * UINT64_C(1000000000) +
         (uint64_t)(finish->tv_nsec - start->tv_nsec);
}

int main(int argc, char **argv)
{
  if (argc != 2) {
    fprintf(stderr, "usage: %s DIRECTORY\n", argv[0]);
    return 2;
  }

  struct timespec start;
  struct timespec finish;
  if (clock_gettime(CLOCK_MONOTONIC, &start) < 0) {
    perror("clock_gettime");
    return 1;
  }

  DIR *dir = opendir(argv[1]);
  if (!dir) {
    perror("opendir");
    return 1;
  }

  uint64_t entries = 0;
  uint64_t hash_xor = 0;
  uint64_t hash_sum = 0;
  errno = 0;
  for (;;) {
    struct dirent *entry = readdir(dir);
    if (!entry)
      break;
    if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
      continue;

    /* A commutative pair makes the digest independent of readdir order. */
    const uint64_t h = fnv1a((const unsigned char *)entry->d_name,
                             strlen(entry->d_name));
    hash_xor ^= rotl64(h, (unsigned int)(h >> 58));
    hash_sum += h * UINT64_C(0x9e3779b97f4a7c15);
    ++entries;
  }
  const int saved_errno = errno;
  if (closedir(dir) < 0 && !saved_errno) {
    perror("closedir");
    return 1;
  }
  if (saved_errno) {
    errno = saved_errno;
    perror("readdir");
    return 1;
  }
  if (clock_gettime(CLOCK_MONOTONIC, &finish) < 0) {
    perror("clock_gettime");
    return 1;
  }

  printf("{\"elapsed_ns\":%" PRIu64 ",\"entries\":%" PRIu64
         ",\"hash\":\"%016" PRIx64 "%016" PRIx64 "\"}\n",
         elapsed_ns(&start, &finish), entries, hash_xor, hash_sum);
  return 0;
}
