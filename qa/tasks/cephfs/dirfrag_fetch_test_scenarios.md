# CephFS dirfrag pipeline 边界场景测试说明

## 1. 文档目的

本文档说明 `CDir` 分批读取 dirfrag OMAP 时的边界测试，重点覆盖 pipeline
引入后容易发生状态丢失、错误处理异常或测试假阳性的场景。每个场景均说明：

- 要验证的风险；
- 如何稳定构造该状态；
- 测试使用什么断言判定成功；
- 当前工作区中的实验执行结果。

对应实现与测试文件：

- `src/mds/CDir.cc`
- `src/mds/CDir.h`
- `src/mds/MDSRank.cc`
- `src/mds/MDSRank.h`
- `src/common/options.cc`
- `qa/tasks/cephfs/test_dirfrag_fetch.py`

pipeline 实现和测试注入点位于本分支的 MDS 主提交中；本文描述的 functional
测试位于后续独立 QA 提交中。

## 2. 公共测试条件

除 fragmentation 和 OSD 截断专用测试外，测试使用以下公共条件：

| 参数 | 值 | 目的 |
| --- | ---: | --- |
| `NFILES` | 200 | 产生足够多的 dentry |
| `mds_dir_keys_per_op` | 16 | 使一个 dirfrag 稳定跨越约 13 个 batch |
| `mds_bal_fragment_dirs` | `false` | 默认只测试单 dirfrag 内部 batching |
| MDS 数量 | 2 | 为 failover 测试提供一个 standby |

`_drop_caches()` 在每次关键读取前执行以下操作：

1. 卸载客户端；
2. flush MDS journal；
3. 丢弃 MDS cache；
4. 重新挂载客户端。

这保证随后执行的 `readdir` 必须从 RADOS 重新读取 dirfrag，而不是命中客户端或
MDS cache。

### 2.1 可观测性与同步机制

测试使用以下 perf counter：

| counter | 含义 |
| --- | --- |
| `dir_fetch_complete` | 完成的 full dirfrag fetch 数 |
| `dir_fetch_batches` | 已到达 `_omap_fetch_batch()` 的 batch 数；到达时立即递增 |
| `dir_fetch_batch_latency` | 每个 OMAP batch callback 的 read 时延 |
| `dir_fetch_omap_bytes` | fetch 解码的 OMAP key/value 总字节数 |
| `dir_fetch_peak_omap_bytes` | 单次 fetch 同时保留的最大编码 OMAP 字节数 |
| `dir_fetch_version_changed` | pipeline fetch 检测到 committed-version 增长的次数 |
| `dir_fetch_decode_errors_after_next_read` | 下一 OMAP read 已提交后发生的 dentry decode error 数 |

`dir_fetch_batches` 和 `dir_fetch_batch_latency` 在 batch callback 入口同步
递增，而不是等整个 fetch 完成后一次性累加。因此延迟样本会
包括已真实返回但因 version 变化而被丢弃的 read，以及返回错误的 read，
并且始终满足 `dir_fetch_batch_latency.avgcount == dir_fetch_batches`。测试也可以在
fetch 尚未完成时轮询这些 counter。

cold fetch 在 header 初始化时以磁盘 fnode version 建立本次 fetch 的版本基线，
不能把这个初始化误判为 fetch 期间发生的 commit。因此同一目录的 pipeline 和
non-pipelined 模式应读取相同数量的 batch；两种模式仍各自满足 sample 数与
batch 数一致。

配置切换和 failover 测试采用统一的同步协议：

1. reset MDS perf counter；
2. 设置 `mds_inject_dir_fetch_batch_delay=3000`；
3. 后台启动目标目录的 `ls`；
4. 等待 `dir_fetch_batches == 1`，如果直接跳到大于 1 则测试失败；
5. 断言后台 `ls` 尚未结束；
6. 执行配置切换或 failover；
7. 配置切换返回后再次断言 `dir_fetch_batches == 1`；
8. 解除 delay，等待原后台 `ls` 完成。

该协议既证明操作发生在 fetch 中途，也能避免单纯依靠 `sleep()` 产生的假阳性。

## 3. 场景与构造方法

### 3.1 中间 batch 返回 `-EIO`

测试：

- `test_middle_batch_eio_pipelined`
- `test_middle_batch_eio_not_pipelined`

风险：

中间 batch 返回负错误时，如果继续进入正常 finish 路径，可能触发
`_omap_fetch_finish()` 中对返回值的断言，导致 MDS respawn；也可能遗留
`STATE_FETCHING`、auth pin 或半完成的 `fetch_state`。

构造方法：

1. 创建 200 个文件并执行 cold fetch；
2. 设置 `mds_inject_dir_fetch_error_after_batches=3`；
3. 第三个 batch 到达 `_omap_fetch_batch()` 时，将返回值改为 `-EIO`；
4. 分别在 pipeline 开启和关闭时执行。

关键断言：

- `ls` 失败或不能返回完整目录；
- MDS rank 仍然运行；
- pipeline 和 non-pipeline 模式均为 `dir_fetch_batches == 3`；cold fetch
  的 header 初始化只建立 version baseline，不产生额外的 probe/restart；
- `dir_fetch_complete == 0`；
- damage table 中恰好出现一个 damaged dirfrag；
- 清除 injection 和 damage 后，cold fetch 可以恢复完整目录。

限制：

这是 `_omap_fetch_batch()` 层的确定性合成错误，覆盖 MDS batch error handler；
它不覆盖真实 OSD、网络或 Objecter 产生 `-EIO` 时更上游的错误传播。

### 3.2 非最后一个 batch 中出现 corrupt dentry

测试：`test_corrupt_dentry_middle_batch`

风险：

单个 dentry 损坏不应终止后续 batch 的加载，也不应使整个 rank 崩溃。

构造方法：

1. 创建 200 个文件；
2. flush 并停止文件系统；
3. 使用 `rados setomapval` 将 `file_000000_head` 覆盖为非法编码；
4. 重新启动 MDS 并 cold fetch；
5. `file_000000` 位于第一批，因此明确不是最后一个 batch。

关键断言：

- `dir_fetch_batches > 1`，证明损坏项所在 batch 后仍有 batch；
- `dir_fetch_complete == 1`；
- MDS rank 仍然运行；
- 结果中仅缺少损坏的 dentry，总数为 `NFILES - 1`；
- 高排序的最后一个文件仍然存在；
- damage table 中记录了 dentry damage。

### 3.3 下一批已经发出后，当前 batch decode 失败

测试：`test_decode_fail_pipeline_continues`

风险：

pipeline 会先提交下一次 OMAP read，再 decode 当前 batch。当前 batch 中出现损坏
dentry 时，已经发出的下一次 read 必须仍可完成，状态也不能被错误清理。

构造方法：

1. 损坏约位于第六批的 `file_000090_head`；
2. pipeline 模式下 `_omap_fetch_more()` 返回是否已同步提交下一 read；
3. `_omap_decode_batch()` 捕获 malformed dentry 时，如果下一 read 已提交，则递增
   `dir_fetch_decode_errors_after_next_read`；
4. 同时运行 non-pipelined 模式作为对照。

关键断言：

- fetch 跨越多个 batch；
- 损坏项之前和之后的文件都存在；
- 仅损坏项缺失；
- pipeline 模式：`dir_fetch_decode_errors_after_next_read == 1`；
- non-pipelined 模式：该 counter 为 0。

这个 counter 使测试能够发现“未来把 next-read 移到 decode 之后”的时序回归，
而不仅仅验证最终目录内容。

### 3.4 同名多个 snap-version 跨 batch

测试：`test_snap_versions_span_batch`

风险：

同一名字的 snap 版本和 head 版本可能处于不同 OMAP batch。跨 batch 保存的
`last_name`、snapid 和 waiter 游标如果错误，可能导致版本遗漏或错误关联。

构造方法：

1. 设置 `mds_dir_keys_per_op=1`，确保每个 OMAP key 单独占一个 batch；
2. 为 `file_000009` 写入 `old-version`；
3. 创建 snapshot `s1`；
4. unlink 并重新创建同名文件，写入 `new-version`；
5. flush 后 unmount client 并再次 flush，完成 cold-cache 准备；
6. 在这之后读取 dirfrag OMAP key，并筛选所有以
   `file_000009_` 开头的 key。

v16 会在 unmount 释放 client caps 时，为原本未变更的 dentry 实体化
snapshot COW key。因此 batch 数必须与 cold-cache unmount 之后的最终
OMAP key 集合比较，不能使用 unmount 之前的过时采样。

关键断言：

- 新文件 inode 与旧 inode 不同；
- head 内容为 `new-version`，snapshot 内容为 `old-version`；
- 同名 key 中同时存在 `file_000009_head` 和至少一个非 head key；
- 同名 key 数量至少为 2；
- `dir_fetch_batches == OMAP key 总数`，证明一 key 一 batch 的配置实际生效；
- head 和 snapshot listing 均完整；
- cold fetch 后重新读取 head/snapshot 内容，仍分别为 `new-version` 和
  `old-version`；
- cold fetch 后重新读取 inode，仍分别等于 recreate 后的新 inode 和 snapshot
  保存的旧 inode。

### 3.5 buffered restart 保留 snapshot purge 事务状态

测试：`test_buffered_restart_preserves_snap_purge_state`

风险：

buffered fetch 在初始化后、decode 前遇到真实 committed-version 变化时会丢弃
`fetch_state` 并重新读取。如果初始化提前推进 `snap_purged_thru`，第二次 fetch
将不再过滤尚未检查的 stale snapshot key，也可能漏掉最终的 dirty commit。

构造方法：

1. 创建 snapshot，unlink/recreate 一个文件，并确认其 head/non-head OMAP key；
2. 删除 snapshot，保留 non-head key 作为 stale 输入；
3. 关闭 pipeline，将 fetch 停在第一批；
4. 使用 `mds_inject_dir_fetch_mark_dirty_after_batches=1` 标脏 dirfrag；
5. 在第二批延迟期间 flush journal，产生真实的 committed-version 增长；
6. 解除延迟，使 buffered 路径检测 version change、丢弃第一次状态并重启；
7. 完成后再次 flush，检查 stale OMAP key 已删除。

关键断言：

- readdir 结果完整；
- `dir_fetch_batches` 大于无重启时的 batch 数；
- 只有重启后的 fetch 完成，`dir_fetch_complete == 1`；
- 被销毁 snapshot 对应的 non-head OMAP key 最终不存在。

### 3.6 pipeline fetch 中的确定性 mutation

测试：

- `test_unlink_decoded_name_during_pipelined_fetch`
- `test_create_behind_cursor_during_pipelined_fetch`
- `test_rename_across_cursor_during_pipelined_fetch`

风险：

pipeline 在 commit-version 变化后不能丢弃已经 decode 的 batch，而是继续读取。
因此已加载 dentry 必须优先于旧 OMAP 数据，并且游标前后的 mutation 都不能丢失、
复活或错误关联 inode。

构造方法：

1. 创建 200 个文件并读取最终 OMAP key 集合；
2. 证明 mutation 源 name 的 head key 位于前 16 个 key 中；
3. 使用 delay 将 fetch 停在 `dir_fetch_batches == 1`；
4. 分别 unlink 已 decode name、创建排序在 cursor 之前的新 name、将已 decode
   name rename 到全部旧 key 之后；
5. mutation 后 flush journal，使目录 `committed_version` 在下一 batch 前真实推进；
6. 释放 delay，等待原后台 readdir 完成。

关键断言：

- `dir_fetch_version_changed == 1`，证明 continue-on-commit 分支确实执行；
- 原后台 readdir 与 mutation 后预期一致；
- 当前 cache 下第二次 readdir、再次 drop cache 后的 readdir 都一致；
- create/rename 后的目标 inode 与 mutation 时记录的 inode 一致；
- unlink/rename 的旧 name 执行 `stat` 失败；
- damage table 始终为空。

### 3.7 fetch 中 runtime 切换 `mds_dir_fetch_pipelined`

测试：

- `test_toggle_pipelined_mid_fetch_on_to_off`
- `test_toggle_pipelined_mid_fetch_off_to_on`

风险：

一个 fetch 如果在中途重新读取 runtime 配置，可能把前面已经 decode 的 batch 与
`pending` 中的 batch 混合，造成丢项、重复或 stranded fetch。因此模式必须在
第一批到达时锁存到 `fetch_state`。

构造方法：

1. 分别以 pipeline 开启和关闭作为初始模式；
2. 等待 active MDS 确认配置已生效；
3. 使用公共同步协议停在第一个有效批；pipeline 和 non-pipelined 模式的
   callback counter 此时均为 1；
4. 切换到相反模式，并等待 active MDS 确认新配置；
5. 切换完成后确认 callback counter 仍为上述精确值；
6. 释放原 fetch。

关键断言：

- 原后台 `ls` 返回完整目录；
- `dir_fetch_complete == 1`，没有因切换重启 fetch；
- 初始为 pipeline 时：`peak_omap_bytes < omap_bytes`；
- 初始为 non-pipelined 时：`peak_omap_bytes == omap_bytes`。

最后两项证明 in-flight fetch 使用的是启动时锁存的模式，而不是切换后的模式。

### 3.8 pipeline 中途 MDS failover/recovery

测试：`test_failover_mid_fetch`

风险：

active MDS 在 fetch 持有 auth pin、timer 和 `fetch_state` 时失效，standby 必须能够
接管，客户端必须重新驱动未完成的 readdir，且新 rank 上不能出现 damage 或残留
pipeline 状态。

构造方法：

1. 使用两个 MDS，其中一个为 standby；
2. 创建 400 个文件，使 fetch 包含更多 batch；
3. 使用 5000 ms delay，并确认 fetch 停在第一批且后台 `ls` 未结束；
4. 记录原 active MDS 名称和 FSMap；
5. 执行 `rank_fail(0)`；
6. 等待新 active，解除新 rank 上的 delay；
7. 等待原后台 `ls` 完成。

关键断言：

- 新 active MDS 名称与旧 active 不同；
- 新旧 FSMap 比较确认发生 failover；
- 原后台 `ls`（而不只是之后新执行的 `ls`）返回完整 400 项；
- failover 后再次 listing 仍完整；
- damage table 为空。

测试不再调用 `delete_mds_coredump()`。该 helper 用于验证并删除“预期发生”的
coredump，不适合用来断言 clean failover 没有 coredump；意外 coredump 由
teuthology 的 coredump task 统一检测。

### 3.9 fragmented directory

测试：`test_fragmented_dir_fetch`

风险：

默认测试关闭 fragmentation，只能证明单个 dirfrag 的 batching。真实目录可能由
多个 dirfrag 组成，每个 fragment 都需要独立完成 pipeline fetch。

构造方法：

1. 仅在该测试中启用 `mds_bal_fragment_dirs=true`；
2. 设置 `mds_bal_split_size=50`、`mds_bal_split_bits=1`；
3. 创建 200 个文件；
4. 轮询 MDS cache，直到目标目录的 `dirfrags > 1`；
5. 记录 fragment 数量，执行 cold fetch。

关键断言：

- 测试开始前确认实际 fragment 数量大于 1；
- cold listing 与创建的文件集合完全相同；
- `dir_fetch_complete` 增量不少于实际 fragment 数量。

### 3.10 真正由 `osd_max_omap_entries_per_request` 截断

测试：`test_osd_driven_truncation`

风险：

仅把 `mds_dir_keys_per_op` 调小，只能覆盖 MDS 主动请求小 batch；不能证明 OSD 在
MDS 请求较大时返回 truncated OMAP 结果的路径。

构造方法：

1. 设置 `mds_dir_keys_per_op=16384`，使 MDS 不是限制方；
2. 使用 `tell osd.* injectargs` 将
   `osd_max_omap_entries_per_request` 设置为 16；
3. 创建 200 个文件并 cold fetch；
4. 测试结束时恢复实验前读取到的 OSD limit，而不是硬编码默认值。

关键断言：

- active MDS 确认 `mds_dir_keys_per_op == 16384`；
- listing 完整；
- `dir_fetch_complete == 1`；
- `dir_fetch_batches > 1`。

由于 MDS 请求上限大于目录 key 数，而默认 OSD byte limit 远大于本测试的数据量，
多个 batch 证明截断来自注入的 OSD entry limit。

## 4. 覆盖汇总

| 场景 | 测试构造状态 | 排他性证据 |
| --- | --- | --- |
| 中间 batch `-EIO` | 测试代码已实现 | 两种模式 batch 均恰为 3；fetch 未完成、damage 被记录 |
| 非最后 batch corrupt dentry | 测试代码已实现 | batch 大于 1、后续高排序文件存在 |
| next-read 后 decode error | 测试代码已实现 | 专用 counter 在 pipeline 为 1、对照模式为 0 |
| 同名多 snap-version 跨 batch | 测试代码已实现 | 同名 head/non-head key 共存且一 key 一 batch |
| buffered restart snapshot purge | 测试代码已实现 | 真实 commit 触发 restart，stale non-head key 被删除 |
| pipeline concurrent mutation | 测试代码已实现 | 三类 mutation 均命中专用 version-changed counter，并验证三层 cache 视图 |
| runtime toggle | 测试代码已实现 | batch 同步加 peak/total 内存模式判定 |
| MDS failover/recovery | 测试代码已实现 | active 更换、FSMap failover、原 readdir 完整 |
| fragmented directory | 测试代码已实现 | 先确认 fragments 大于 1，再验证 fetch 增量 |
| OSD entry limit 截断 | 测试代码已实现 | MDS limit 16384、OSD limit 16、batch 大于 1 |

## 5. 本轮实验结果

实验日期：2026-08-31。以下结果均来自 v16.2.14 目标 worktree 本轮新执行，
未沿用 main worktree 的结果。

| 检查项 | 命令/范围 | 结果 |
| --- | --- | --- |
| C++ 构建 | `cmake -S . -B build -DENABLE_GIT_VERSION=OFF` 后执行 `ninja -C build ceph-mds -j4` | PASS，`bin/ceph-mds` 链接成功；关闭 git version 是为规避 v16 CMake 对 packed worktree ref 的误解析 |
| Python AST | 对 functional、runner 及 workunit 全部 `*.py` 执行 `ast.parse()` | PASS，6 files |
| diff 格式 | `git diff --check` | PASS |
| benchmark 工具单测 | `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -v test_tools.py` | PASS，43 tests |
| benchmark driver 构建 | 按 Makefile 的严格 flags 编译到 `/tmp` | PASS，未在源码树留下二进制 |
| benchmark smoke | 对 3-entry 临时目录执行 `/tmp/.../readdir_hash` 并校验 JSON | PASS，entries=3、hash 为 32 位十六进制 |
| benchmark 参数自检 | `run.py --help`、`analyze.py --help`、`lookup_load.py --help` | PASS |
| CephFS 高风险用例 | EIO 两种模式、decode、failover、OSD 截断 | PASS，均进入真实集群和测试体 |
| perf counter 专项 | `test_full_fetch_perf_counters` | PASS，42.985 s |
| snap/toggle 修复回归（新增断言前） | snap 一项和 toggle 两项 | PASS，snap 38.257 s；toggle 合计 88.750 s |
| buffered restart snapshot purge | 新增事务状态回归 | 未运行 functional；当前环境没有匹配的 vstart 集群 |
| snap post-fetch 与 mutation 新断言 | 内容/inode 复查及三类并发 mutation | 未运行 functional；2026-09-01 完成 AST、构建及静态检查 |

functional 运行使用 vstart 默认的 3 MON、4 OSD、1 MGR，并按 suite
需求启动 2 MDS 和 2 个 kernel client。所有命令都使用
`--create --teardown --kclient --clear-old-log`，确保每组结果包含真实的
集群创建、mount、测试体和 teardown，而不是 import 或 setup 阶段的假通过。

高风险用例实际结果：

- pipeline/non-pipeline EIO：PASS，40.569 s / 37.973 s；
- decode-after-next-read：PASS，58.357 s；
- active MDS failover：PASS，56.576 s；
- OSD-driven truncation：PASS，44.563 s。

## 6. functional 执行环境与命令

原有未跟踪的 `.venv-teuthology/` 为不完整 sourceless 安装，已原样
保留。实际测试从 worktree 内的离线 wheel bundle 创建隔离环境
`/tmp/teuthology-v16-runner`，teuthology 版本为
`1.1.1.dev612+g3a403c0db`。runner 同时修复了 fork/FD 阻塞、本地
kernel mount Python helper、`LocalCephManager` context 和本地 `rados` 路径。

从 `build/` 目录执行的完整模块命令为：

```bash
env PYTHONPATH=../qa:../src/pybind:lib/cython_modules/lib.3 \
  LD_LIBRARY_PATH=/root/code/ceph-v16.2.14/build/lib/ \
  /tmp/teuthology-v16-runner/bin/python \
  ../qa/tasks/vstart_runner.py \
  --create --teardown --kclient --clear-old-log \
  tasks.cephfs.test_dirfrag_fetch
```

专项用例在同一命令尾部改为完整 unittest 名即可。主验证期间
的 runner 日志为 `build/vstart_runner.log`；`--clear-old-log` 使其保留最后
一次完整模块的原始汇总。
