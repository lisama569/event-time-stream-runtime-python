# 事件时间窗口聚合与断点续算运行时（本地模拟）

一个纯本地的流处理运行时，用**内存事件源**和**本地 JSON 状态存储**模拟真实流的
乱序（out-of-order）、迟到（late data）、故障恢复（crash recovery）与背压
（backpressure）。目标是：**无论从哪个入口（Python API / HTTP / 演示脚本）、是否经历
崩溃与重复投递，最终聚合结论都一致且每一条数据都有可解释的去向。**

## 目录结构

```
etsr/
  model.py       事件 / 窗口结果 / 迟到记录 / 投递报告，SUM|COUNT|MIN|MAX|AVG 聚合
  watermark.py   单调水位线生成器（BoundedOutOfOrderness + 心跳标记）
  windows.py     滚动窗口分配器 + 可更新的窗口聚合状态（含快照/恢复）
  buffer.py      有界 FIFO 缓冲，容量受限，过载抛 OVERLOADED_BUFFER_FULL
  source.py      本地事件源：连续位点、乱序追加、重放视图
  sink.py        结果收集器：最终结果 / 迟到旁路 / 重复位点
  checkpoint.py  检查点原子持久化（JSON）、单调 id、损坏与回退检测
  engine.py      运行时核心：串起水位线、窗口、检查点、背压
main.py          FastAPI 入口（/events、/events/batch、/watermark、/checkpoint …）
scripts/demo.py  无故障运行 vs 崩溃恢复的端到端对比演示
tests/           36 个单测，覆盖乱序、迟到、迟到容忍修正、重启重放、背压、跨入口一致性
```

## 时间语义（event-time，不是处理时间）

- 每条事件携带 `event_time_ms`。滚动窗口由事件时间决定：
  `start = floor(event_time / window_size) * window_size`，窗口为左闭右开
  `[start, start + window_size)`。
- **水位线（watermark）** 是“不会再看到比它更早的事件”的时间下界：
  `wm = max(已观察事件时间) - max_out_of_orderness_ms`，也可由
  `POST /watermark` 的心跳标记显式推进。
- 水位线**单调不减，绝不倒退**：乱序旧事件、显式传入更小的标记、崩溃后重放旧数据，
  都不能拉低它（见 `test_watermark_is_monotonic_and_never_regresses`）。
- 窗口在 `watermark >= window_end` 时关闭。**关闭前**，乱序事件可持续并入，
  `pending_windows()` / `GET /state` 能看到不断更新的中间聚合；**关闭时**产出
  `WindowResult`（含聚合值、计数、纳入的源位点列表、关闭时水位线、
  经历过的检查点 id、结论版本 revision），之后从 `pending` 移除并进入最终结果。
  默认配置下该结论不可再变；开启迟到容忍后，容忍期内可被修正（见下节）。

### 迟到事件：不静默丢弃，也不重复计入

- 当事件到达时其窗口**已经关闭**（或 `event_time < watermark` 且水位线已越过窗口
  结束边界），判定为迟到。
- 默认（`allowed_lateness_ms=0`）时，迟到事件**不会**改动已定稿的结果，而是进入
  **迟到旁路输出** `late_events()`，记录事件位点、所属窗口、当时水位线、迟到毫秒数
  与稳定原因码 `LATE_AFTER_WINDOW_CLOSE`。
- 注意区分：事件时间比当前水位线旧、但其窗口尚未关闭时，仍是**正常乱序事件**，
  会计入并更新窗口（见 `test_event_before_watermark_but_window_open_is_still_accepted`）。

### 迟到容忍与结论修正（`allowed_lateness_ms > 0`，默认关闭）

- 窗口关闭后，从窗口结束时间起算额外保留一段**容忍期**
  `[window_end, window_end + allowed_lateness_ms)`（按事件的事件时间所属窗口判断）。
- 容忍期内到达的迟到事件**修正**该窗口已给出的结论：并入保留的窗口状态，输出
  revision 单调递增的新结论，**以最新 revision 为准**。可从多处观察到修正：
  - `WindowResult.revision` / `corrected`：是否被修正过、当前是第几版；
  - `corrections()` / `GET /state` 的 `corrections`：每次修正的流水（窗口、版本、
    修正后结论、**参与本次修正的事件位点**、当时水位线）；
  - `correctable_windows()` / `GET /state`：当前仍处于容忍期内、可被修正的窗口。
- 超出容忍期的迟到事件仍走旁路，但原因码为 **`LATE_BEYOND_ALLOWED_LATENESS`**，
  与未开启容忍时的 `LATE_AFTER_WINDOW_CLOSE` 明显区分；已给出的修正结论不会被
  超容忍事件污染，也不会被静默丢弃。
- **修正状态有上界**：水位线越过 `window_end + allowed_lateness_ms` 时，对应窗口的
  可修正状态立即释放（日志 `correctable window purged`），历史再长也不会堆积；
  释放之后才到的更晚数据走上面的超容忍旁路，给出明确且可区分的处理结果。
- 修正**不触碰水位线**（迟到事件时间必然更旧），也不影响已关闭窗口的既有语义；
  同一条事件重复投递由位点幂等去重吸收，同一次修正不会被算两遍。
- 可修正窗口状态与修正流水**随检查点一起持久化**，崩溃重启、重放后继续修正，
  最终结论（含 revision 与修正流水）与全程无故障运行完全一致
  （见 `test_restart_replay_correction_matches_failure_free`）。
- **不开启时（默认）表现与原有行为完全一致**：无修正、无可修正状态、迟到原因码不变。

## 位点、去重与状态保留

- 源位点 `offset` 必须**从 0 连续递增**：
  - 追加/投递出现跳号 → `OffsetGapError`（reason=`OFFSET_GAP`），明确拒绝，
    绝不在缺数据的情况下继续推进；
  - 已确认段被再次投递（`offset < next_offset`，at-least-once 重放）→ 幂等跳过，
    记入 `duplicate_offsets`，绝不重复计入，也不报错。
- 窗口聚合状态内部按源位点去重，因此同一条事件无论被投递多少次，聚合值与计数都不变。

### 检查点内容

一次 `Checkpoint` 是 `(位点, 水位线, 全部待关闭窗口状态, 已定稿窗口结果, 重复位点)`
的一致性快照：

| 字段 | 含义 |
| --- | --- |
| `checkpoint_id` | 单调递增，不允许覆盖、不允许恢复更旧 id |
| `next_offset` | 下一条待处理事件的源位点（恢复后从这里重放） |
| `watermark_ms` | 水位线，恢复后重放旧数据不会使其倒退 |
| `windows` | 所有尚未关闭窗口的可更新聚合状态（含已纳入位点） |
| `correctable_windows` | 已关闭但仍在迟到容忍期内的可修正窗口状态（含 revision） |
| `finalized_results` | 已关闭窗口的最新结论（含 revision，修正后以最新版为准） |
| `corrections` | 历次迟到修正流水（窗口、版本、修正后结论、参与事件位点） |
| `duplicate_offsets` | 已识别为重复的位点 |

## 故障恢复方式

- 检查点写入 `state_dir`（默认 `.runtime_state/`），采用“临时文件 + 原子 rename +
  fsync”，崩溃不会留下半个检查点；JSON 损坏在加载时抛 `CheckpointCorruptError`
  （reason=`CHECKPOINT_CORRUPT`）。
- 运行时 `start()` / HTTP 应用启动时自动加载**最新**检查点；事件源用
  `source.replay_from(checkpoint.next_offset)` 从断点重放。
- **位点/检查点回退被明确拒绝**，而不是从中间继续：
  - 恢复一个比当前 id 更旧的检查点 → `CheckpointRegressionError`
    （reason=`STALE_CHECKPOINT`，HTTP 409）；
  - 在活动源上把读取位点 seek 回更早位置 → `OffsetRegressionError`
    （reason=`OFFSET_REGRESSION`）。
- 崩溃边界常见的“已确认段被重发”由 offset 幂等去重吸收，因此
  **恢复后的最终结果与无故障运行逐窗口、逐值、逐计数、逐纳入位点完全一致**
  （见 `test_restart_replay_matches_failure_free_run` 与 `scripts/demo.py`）。

## 背压与有界资源

- 入端与处理解耦：`enqueue*` 只负责入有界缓冲，`process_cycle()` 每个周期最多处理
  `process_budget_per_cycle` 条；`run_source` 用 `source_pull_per_cycle` 控制每周期
  从源拉取的突发大小（输入速率）。
- 缓冲容量 `buffer_capacity` 是内存硬上界。输入速率持续高于处理能力时：
  - `enqueue` 在满时立即抛 `BufferOverloadError`（reason=`OVERLOADED_BUFFER_FULL`，
    HTTP 单条入口返回 503，调用方应退避重试）；
  - `ingest_batch` / `run_source` 会把装不下的**尾部事件**原样保留并在投递报告中逐条
    给出拒绝原因，`run_source` 内部按**原顺序**“处理腾空间 → 重试后缀”，既不丢数据
    也不造成位点缺口；
- 过载（`OVERLOADED_BUFFER_FULL`）、迟到（`LATE_AFTER_WINDOW_CLOSE` /
  超容忍 `LATE_BEYOND_ALLOWED_LATENESS`）、重复（计入 `duplicates`）、
  位点缺口（`OFFSET_GAP`）、检查点回退（`STALE_CHECKPOINT`）
  各自有可区分的字段或 reason 码，失败原因不会混淆。

## 验证方法

```bash
# 安装依赖（Python 3.14，uv）
uv sync

# 运行全部单测（-s 可查看事件时间 / 水位线 / 检查点日志）
uv run pytest -q
uv run pytest -s -k "restart or late or backpressure"
uv run pytest -s tests/test_allowed_lateness.py   # 迟到容忍 + 结论修正专项

# 端到端演示：无故障运行 vs 处理 5 条后崩溃、重启重放（含重复投递）
uv run python scripts/demo.py

# 启动 HTTP 服务
uv run uvicorn main:app --reload
# 然后：
#   POST /events        投递单条事件（过载返回 503，位点缺口返回 409）
#   POST /events/batch  批量投递
#   POST /watermark     心跳推进水位线并关闭到期窗口
#   POST /checkpoint    显式一致性快照
#   POST /restore/{id}  恢复指定检查点（更旧 id 返回 409 STALE_CHECKPOINT）
#   GET  /state         查看水位线、待关闭窗口、最终结果、迟到旁路与重复位点
```

测试日志由 `tests/conftest.py` 配置，引擎在每个关键点打印
**事件时间、窗口、水位线与检查点 id/位点**，例如：

```
event | offset=3 event_time_ms=900 key=a window=[0,1000) watermark_ms=1500
late event | offset=6 ... watermark_ms=2500 reason=LATE_AFTER_WINDOW_CLOSE -> side-output
window closed | key=a window=[0,1000) aggregate=SUM value=3.0 count=2 watermark_ms=3000
window corrected | offset=8 event_time_ms=400 key=a window=[0,1000) revision=1 value=105.0 ...
correctable window purged | key=a window=[0,1000) revision=1 watermark_ms=1500 allowed_lateness_ms=500
checkpoint saved | id=1 next_offset=4 watermark_ms=1500 pending=2 finalized=1 correctable=1 ...
checkpoint restored | id=1 next_offset=4 watermark_ms=1500 pending_windows=2 ...
backpressure | 2 events withheld, draining before retry
```
