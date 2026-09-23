# event-time-stream-runtime-python

本地事件时间窗口聚合与断点续算的流处理运行时。用本地事件源（`LocalEventSource`）
和本地文件状态存储（`CheckpointStore`）模拟真实流的乱序、迟到与故障恢复，
保证各入口（`main.py` 演示、pytest 套件）与各阶段（无故障基线 / 崩溃恢复）
得到的聚合结论一致且可解释。

## 时间语义

- **事件时间窗口**：滚动窗口 `[start, end)`，由 `assign_window(event_time, window_size_ms)`
  按事件时间划分，与到达顺序无关。
- **水位线**：`watermark = max(已观测事件时间) - allowed_lateness_ms`，
  由 `WatermarkTracker` 保证**单调不倒退**（乱序事件只会保持水位线，不会回拨）。
- **乱序事件**：水位线越过窗口右边界之前到达的乱序事件正常计入对应窗口，
  窗口结果在关闭前保持可更新。
- **迟到事件**：窗口关闭（`window_end <= watermark`）后到达的事件**不静默丢弃、
  也不重复计入**已关闭窗口，而是路由到 `runtime.late_events` 侧输出，
  并记录当时的水位线与原因（`event_time <= watermark (window closed)`）。

## 状态保留与恢复

- **检查点内容**（`CheckpointStore`，JSON 原子落盘：写临时文件后 `os.replace`）：
  - `offset`：源位点（下一条待读事件下标）；
  - `watermark`：水位线跟踪器快照；
  - `windows`：未关闭窗口的聚合状态（count/total）；
  - `results` / `late_events`：已输出结果，保证恢复后输出序列与无故障运行完全一致。
- **恢复**（`StreamRuntime.restore()`）：
  - 从最近检查点恢复位点与全部聚合状态，源 `seek` 到检查点位点后重放；
  - **位点回退被明确拒绝**：请求位点 < 检查点位点时抛 `OffsetRegressionError`，
    不会从中间静默继续；请求跳过检查点之后的位点抛 `ValueError`；
  - 检查点文件损坏抛 `CheckpointCorruptionError`，与位点错误可区分。
- **重复投递**：at-least-once 重放检查点之后的事件是安全的——崩溃时未落盘的
  中间状态随检查点回滚，重放后结果与无故障运行一致（exactly-once 效果）。

## 背压与有界资源

- 输入缓冲有界（`buffer_capacity`），`submit()` 在缓冲满时抛 `OverloadError`
  （可区分的过载反馈），不会内存无界增长，也不会以错误结果继续推进。
- `run()` 采用"拉一条、处理一条"的交替循环，源故障时已处理事件的检查点不丢失。

## 验证方法

```bash
uv run pytest          # 全部单测：乱序 / 迟到 / 水位线单调 / 背压 / 重启重放 / 位点回退 / 检查点损坏
uv run pytest -s       # 单测日志中打印事件时间、水位线与检查点信息
uv run python main.py  # 端到端演示：无故障基线 vs 崩溃恢复，断言两条路径结论一致
```

关键一致性断言（`tests/test_recovery.py`）：

- `test_restart_replay_matches_no_failure_run`：注入崩溃 → 恢复重放，
  `results` / `late_events` / `watermark` 与无故障基线逐项相等；
- `test_duplicate_delivery_after_restore_is_consistent`：从检查点位点重放
  同一批事件不产生重复输出；
- `test_offset_regression_rejected`：位点回退抛 `OffsetRegressionError`。

## 代码结构

```
stream_runtime/
  events.py     # Event / WindowResult / LateEvent 类型
  watermark.py  # WatermarkTracker：单调水位线
  windows.py    # 滚动窗口分配与聚合状态
  state.py      # CheckpointStore：原子落盘与位点校验
  source.py     # LocalEventSource：可重放源 + 故障注入
  pipeline.py   # StreamRuntime：背压、迟到路由、检查点编排
  errors.py     # 可区分的错误类型
main.py         # 演示入口
tests/          # pytest 套件
```
