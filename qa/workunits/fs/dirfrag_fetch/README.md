# CephFS dirfrag fetch performance experiment

This directory contains the reproducible experiment for comparing
`mds_dir_fetch_pipelined=false` and `true` on the v16.2.14 pipeline baseline
`5b25946d7e8`. The normal
comparison uses one binary and changes the runtime option between samples.
The parent commit is only an external-metric check of the disabled path.

The runner is intentionally strict. It refuses a topology other than one MON,
three up OSDs, and one active MDS; checks every dataset has one dirfrag; resets
MDS perf counters immediately before each traversal; and requires exactly one
`mds.dir_fetch_complete` afterward. It also verifies entry count, an
order-independent stable name hash, the latched MDS option, and all OSD delay
settings for every sample.

## Build and quick check

Build `ceph-mds`, `ceph`, and `ceph-fuse`, then run a smoke sample:

```console
ninja -C build ceph-mds ceph ceph-fuse
python3 qa/workunits/fs/dirfrag_fetch/run.py \
  --build-dir build --output /tmp/dirfrag-smoke \
  --start-cluster --matrix smoke --only-required-sizes \
  --skip-delay-calibration
```

`--start-cluster` starts `vstart.sh -n -d` with one MON, three OSDs, one MDS,
one MGR and one filesystem, and stops it at the end. Omit it when an appropriate
vstart cluster is already running. The runner temporarily sets `noscrub`,
`nodeep-scrub`, `norecover`, and `nobackfill`, and restores every flag and
runtime option it changed. Use `--no-quiesce` only when those flags are managed
externally.

The dataset creation loop is outside every timed sample. By default the runner
creates and validates 10K, 100K and 1M-file directories once; existing complete
datasets are reused. `--only-required-sizes` is useful for smoke testing.

## Full HEAD experiment

Run the planned core, scale and interference matrices with three warmups and 20
measured fetches per mode and condition:

```console
python3 qa/workunits/fs/dirfrag_fetch/run.py \
  --build-dir build --output /tmp/dirfrag-head \
  --start-cluster --matrix all --warmups 3 --repetitions 20 \
  --profile-match true,100000,1024,5,0 \
  --flamegraph-tools /path/to/FlameGraph
```

The A/B order is randomized in mirrored `false,true,true,false` blocks. Every
cold sample performs this sequence:

1. Unmount the traversal client, synchronously flush the MDS journal twice,
   save the MDS memory counters/status, and respawn the MDS. The second flush expires a
   possible lock-related UPDATE emitted while the first flush completes.
   Respawn the MDS twice, waiting for a new PID, `up:active`, and a working
   admin socket after each restart. Complete cache dumps after both restarts
   are saved; the final dump may contain the target inode metadata but must
   contain no target dirfrag, dentry, or complete-dirfrag state.
2. Reapply and read back the MDS/OSD runtime options lost across `execv`,
   remount, and `stat` only the target directory to resolve its ancestors. A
   second cache dump requires the target dirfrag to have no dentries and not
   be complete before all MDS perf counters are reset.
3. Start 10 ms `VmRSS` sampling and `perf stat` on the MDS, traverse the target
   with `readdir(3)`, then stop sampling.
4. Save the complete perf dump and re-read the MDS/OSD configuration.

The core matrix uses 100K entries, effective batch sizes 128 and 1024, and OSD
dispatch delays 0, 1 and 5 ms. The scale matrix uses 10K, 100K and 1M entries,
batch 1024, and delays 0 and 5 ms. Duplicate conditions are run once. Before
the measured matrix, the runner verifies that 1 and 5 ms injection shift
`dir_fetch_batch_latency`; these are sensitivity tests, not substitutes for
real network RTT.

After stopping a vstart cluster, `--reuse-existing-cluster-data` restarts it
without `vstart.sh -n`. In this mode dataset roots, lookup files, directory
entry counts, hashes, and the single-dirfrag property are validation-only;
missing or incomplete data is an error and the runner never enters its file
creation loop.

The concurrency matrix keeps a second ceph-fuse client mounted. After each MDS
MDS respawn it warms a small lookup target, then issues rate-limited `stat`
operations (1000/s by default) during the 1M-directory traversal. Every lookup
latency is retained, with throughput and p50/p95/p99 added to the sample row.
That client is mounted with a zero-entry metadata cache, and the runner also
requires the MDS request counter to cover at least the number of load
operations; this prevents a client-local cache hit loop from masquerading as
MDS interference.

If `--profile-match` identifies a measured sample, the runner captures
`perf.data` and `perf script` output. Supplying the upstream FlameGraph scripts
also creates folded stacks and an SVG; `_omap_decode_batch` can then be isolated
in the generated profile.

## Parent kill-switch check

Build `238ba602515` in a separate worktree. Invoke this runner from the HEAD
worktree, pointing it at the parent's build directory:

```console
git worktree add ../ceph-dirfrag-parent 238ba602515
cmake -S ../ceph-dirfrag-parent -B ../ceph-dirfrag-parent/build \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo
ninja -C ../ceph-dirfrag-parent/build ceph-mds ceph ceph-fuse

python3 qa/workunits/fs/dirfrag_fetch/run.py \
  --build-dir ../ceph-dirfrag-parent/build \
  --output /tmp/dirfrag-parent --start-cluster \
  --legacy --matrix parent --warmups 3 --repetitions 10
```

The parent does not have the new internal counters, so its rows intentionally
contain only client latency, RSS and `perf stat` metrics. Its original
`mds.dir_fetch` counter is normalized into the CSV's `dir_fetch_complete`
column solely as the one-fetch correctness guard. Compare those with HEAD's
`false` rows. Each worktree's
datasets are created once and excluded from measurement. The runner reads
`CMAKE_HOME_DIRECTORY` from the selected build tree, so the parent commit is
recorded automatically.

## Outputs and analysis

Each output directory contains:

- `environment.json`: commit, CPU, memory, build type, allocator, object store,
  version, PID, exact arguments and cluster topology.
- `datasets.json`: expected count/hash and confirmed fragment count.
- `samples.csv`: one row per calibration, warmup or measured fetch, including
  execution order, configuration, internal counters and process metrics.
- `raw/<sample>/`: full `perf-dump.json`, counters-only pre-respawn cache
  evidence, complete post-respawn/post-stat cache dumps, 10 ms RSS samples,
  `perf stat` CSV, per-sample JSON, optional lookup latencies, and optional
  profiles.
- `analysis/summary.csv`: median, p95, mean, standard deviation and bootstrap
  95% confidence interval for the median.
- `analysis/comparisons.csv`, `parent-comparisons.csv`, and `report.md`:
  enabled-versus-disabled changes, parent checks, and the 5% regression gates.

Re-run or combine analysis without repeating the experiment:

```console
python3 qa/workunits/fs/dirfrag_fetch/analyze.py \
  /tmp/dirfrag-head/samples.csv \
  --output /tmp/dirfrag-head/reanalysis --strict-performance
```

Include the parent CSV to emit `parent-comparisons.csv` and apply the
±5%-or-overlapping-confidence-interval gate:

```console
python3 qa/workunits/fs/dirfrag_fetch/analyze.py \
  /tmp/dirfrag-head/samples.csv /tmp/dirfrag-parent/samples.csv \
  --output /tmp/dirfrag-combined --strict-performance
```

The OMAP byte counters include raw key bytes and encoded `bufferlist` value
bytes only. They do not estimate `std::map`, allocator or other container
overhead. `dir_fetch_peak_omap_bytes` is therefore the primary mechanism
metric; process RSS is auxiliary because allocator caching can outlive a
sample. `dir_fetch_estimated_overlap_ms` is the diagnostic
`sum(batch latency) + sum(decode time) - fetch latency`; compare it with the
fetch-latency change instead of treating injected delay alone as evidence.

If the 0 ms results overlap noise while delayed runs and internal counters show
I/O/decode overlap, report the outcomes separately: no significant local
end-to-end gain, but successful mechanism validation. Do not extrapolate the
injected-delay result to production latency.
