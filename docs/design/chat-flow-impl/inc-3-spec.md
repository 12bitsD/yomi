# P2 增量 3 实施规格：按卡运行控制核心（adapter 接缝 + 调度器 + 停止/恢复）

目标：N6 每卡单一控制者裁定开始/暂停/停止/恢复顺序；C3 执行资格五条件；N2 首次派发前完成绑定；D3 恢复不重试已停止轮；D11 按卡隔离；资源名额统一分配。全部用仿真 adapter（P1 全程原则；真实双 Provider 是 P3/下游 W3）。卡按钮与回调面在增量 4。

## 既有接缝

- `exec/inbox.rs`：受理队列（进程内）。本增量需加 `peek_front`/`pop_front`。
- `exec/store.rs`：`bind_provider_session` 状态机已测；`mark_broken`。
- `Kernel`：持有 `exec_task_store`、`exec_inbox`；新增 `exec_scheduler` + `exec_events`。
- 配置：`crates/kernel/src/config.rs` 现有 `[[channels]]` 与顶层段模式；Justfile check 有 `kernel --no-default-features` 组合——新增代码不得破坏 feature 组合编译。

## 组件 1：Run 模型（`exec/run.rs`）

- `define_id!(RunId => "run_")`（types/mod.rs）。
- `RunStatus { Starting, Running, Stopping, Completed, Failed, Stopped, Unknown }`：
  - Starting=已取资格、adapter start 未确认；Running=adapter 已确认开始；
  - Stopping=停止已受理、原生终态未确认（**停止中≠已停止**，C6）；
  - Completed/Failed=已确认自然终态（业务失败不自动暂停，N3）；Stopped=取消已确认；
  - Unknown=派发或终态确认丢失——**阻止下一轮**（N3 未知不当普通失败跳过）。
- `RunRecord { run_id, task_id, input_seq, text, image_keys, status, started_at, ended_at }`。

## 组件 2：ExecAdapter trait + SimAdapter（`exec/adapter.rs`）

```rust
#[async_trait]
pub trait ExecAdapter: Send + Sync {
    async fn create_session(&self, task: &ExecTask) -> Result<String>;        // 原生身份
    async fn start_run(&self, native_session_id: &str, input: &AcceptedInput) -> Result<()>;
    async fn cancel(&self, native_session_id: &str) -> Result<()>;
}
```

- 终态经回调进入调度器：`ExecAdapterSink::terminal(native_session_id, TerminalKind)`（`Completed|Failed|Cancelled`）。trait 对象持有 sink 句柄（构造时注入）。
- `SimAdapter`（feature 无关、默认装配，标注 P3 由真实 adapter 替换）：`create_session` 返回 `sim-{ulid}`；行为旋钮（测试可注）：`complete_after: Option<Duration>`（None=挂起直到测试显式 terminal）、`fail_next_start`、`cancel_never_confirms`。start_run 成功即视为「原生已确认开始」（同步 ack）；complete_after 到点经 sink 报 Completed。
- 生产装配：Kernel::new 用 `SimAdapter::default()`（挂起模式——不会自己完成，避免无真实 Provider 时伪造进展）。

## 组件 3：ExecScheduler（`exec/scheduler.rs`）

- 结构：`lanes: DashMap<ExecTaskId, Arc<Mutex<TaskLane>>>` + `slots: Semaphore`（`exec.max_concurrent_runs`，默认 2）+ store/adapter/inbox/event_tx/config。
- `TaskLane { paused: bool, current: Option<RunRecord>, stop_requested_at: Option<Instant>, blocked_unknown: bool }`。** lane Mutex 是 N6 唯一裁定者**：取队列、暂停、开始、停止、恢复的全部顺序都在锁内决定；网络等待（adapter 调用）在锁外，回来重核状态。
- `try_dispatch(task_id)`（accept/resume/terminal 后调用）：
  1. 锁内检查 C3 五条件：!paused、!blocked_unknown、current.is_none()、inbox 非空、slots 有许可（try_acquire，无则出锁返回——名额到时由别的 terminal 触发重试）。
  2. binding==Uninitialized → 锁外 `adapter.create_session` → `store.bind_provider_session`（Err → 锁内标 `blocked_unknown`、任务如实显示；Broken → 同阻断，不自动重建）；已 Bound 直接用原 id。
  3. 锁外 `adapter.start_run` 成功 → 锁内 `pop_front` → current=Running，发 `RunStarted` 事件；start 失败 → current=Unknown/ blocked_unknown=true（不 pop，不跳过该输入）。
- `terminal(native_session_id, kind)`：按 native id 找 lane/run → 锁内：current=终态（Completed/Failed/Stopped(cancelled)），`ended_at`，释放 slot，发 `RunTerminal`；Stopping 中收到 Completed → **保存真实自然终态 Completed**（N6 竞态），队列保持暂停；收到 Cancelled → Stopped。终态后若 !paused && !blocked → `try_dispatch`。
- `stop_and_pause(task_id, expected_run: Option<RunId>) -> StopOutcome`：
  1. 锁内先 `paused=true`（关闭后续派发资格，先于任何网络动作）。
  2. current=None → `NoCurrentRun{paused:true}`。
  3. current 在且 expected_run 不匹配 → `RunMismatch{actual_run, actual_status}`（旧按钮不得套到新 Run，C6/R7）。
  4. 匹配 → current.status=Stopping、stop_requested_at=now、锁外 adapter.cancel → `Accepted{run}`；超时由 `sweep_unconfirmed`（超时阈值 `exec.stop_confirm_timeout_secs` 默认 30s）标记并保留 Stopping（未确认，继续禁写；**不 detach 续跑**）。
- `resume(task_id) -> ResumeOutcome`：锁内——Stopping 未确认 → `BlockedStopUnconfirmed`（保持暂停，**不预约自动恢复**，N6 候选 1）；否则 paused=false → `try_dispatch` → `Resumed{dispatched}`；空队列 → `Resumed{dispatched:false}`（不调用 adapter）；重复 resume 幂等。已停止的 A **不回队**（D3）。
- `snapshot(task_id) -> LaneSnapshot { paused, current, queued: usize, blocked_unknown }`（卡面/查询用，增量 4 接）。
- 事件（tokio broadcast）：`ExecEvent::{RunStarted{task_id,run_id}, RunTerminal{task_id,run_id,kind}, Paused{task_id}, Resumed{task_id}, StopUnconfirmed{task_id,run_id}}`；kernel  accessor `exec_events() -> broadcast::Receiver`（订阅者模式，增量 4 通道侧刷新用）。

## 组件 4：配置与开关（R7）

- 顶层 `[exec]`：`max_concurrent_runs: usize = 2`、`stop_confirm_timeout_secs: u64 = 30`（config.rs，缺省值按现有 Default 模式）。
- 通道级：`[[channels]]` 加 `exec_tasks: bool`（**默认 false**——未启用场景保持原行为，R7）。gate 点：`/task` 臂（关闭 → 明确拒绝文本）、分流臂（关闭 → 落回原 chat 路径）、`task_create` 工具 exec 时按路由到的通道配置检查（无通道路由的本地会话：允许）。增量 2 测试相应补 flag。

## 组件 5：Kernel 装配

`Kernel::new`：`ExecScheduler::new(store, Arc::new(SimAdapter::default()), inbox, exec_config)` + `exec_events` broadcast(256)；accessor `exec_scheduler()`、`exec_events()`。handlers.rs accept 成功后追加 `scheduler.try_dispatch(task_id)`（暂停/无名额时自然不派发）。

## 测试（`exec/scheduler_test.rs`，全部用可控 SimAdapter + 内存 store）

1. **A/B/C/D 全场景**：A running、B/C queued → stop_and_pause → A Stopped、B/C 保留有序；暂停中收 D → 排尾；resume → B→C→D 依次单 writer 执行；A 不重试。
2. **交接竞态**：stop 与 A 自然完成同时 → A=Completed 如实、队列保持暂停、B 不启动。
3. **旧按钮**：stop 带 run A 的 expected、B 已开始 → RunMismatch、B 不受扰。
4. **慢停止**：cancel_never_confirms → 超阈值后 StopUnconfirmed、resume=BlockedStopUnconfirmed、无新派发；之后 terminal(Cancelled) 到达 → Stopped、仍 paused，直到显式 resume。
5. **业务失败不自动暂停**：Failed 终态且未暂停 → 自动派下一项；start 失败 → blocked_unknown、后续不派（未知不跳过）。
6. **按卡隔离**：X 停止暂停不影响 Y；`max_concurrent_runs=1` 时 X 跑 Y 等，X 终态 Y 起。
7. **resume 幂等**：连点两次 → 只派一个；空队列 resume → adapter 零调用。
8. **绑定时机**：Uninitialized 首次派发才 create_session+bind；bind 冲突（不同 id）→ blocked_unknown 不重建。
9. **开关**：通道 `exec_tasks=false` → /task 拒绝、分流关闭落回 chat。

## 检查门槛

`cargo check -p kernel`（含 `--no-default-features` 各组合）、`cargo clippy -p kernel --all-features` 零新增、`cargo fmt`、`cargo test -p kernel` 全绿（存量 4 个沙箱进程组失败为已知基线）。

## 明确不做

卡按钮/回调（增量 4）、WaitingRequest/当前轮问答（P3/N5）、真实 Provider adapter（P3）、Run 事实持久化（P4/W2）、重启恢复语义（P6）、wire/RPC 查询面。
