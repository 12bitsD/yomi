# P2 增量 4 实施规格：卡面控制（按钮 + 回调 + 事件驱动刷新）

目标：把增量 3 的运行控制接到占位主卡上——用户可停止并暂停/恢复队列；回调按 C9 重核身份/权限/代次/Run 版本；卡面按事件提示从快照重渲染（N7：事件是提示，快照是事实）。P2 就此收口。

## 既有接缝

- 回调路由：`channels/hub/mod.rs` 按钮命名空间分发（`ns.starts_with(...)` 臂序列）——新增 `exec_` 前缀臂。user gate 已在分发前统一施加；停止类动作与 `/stop` 同档不叠加 admin（面板契约 §5）。
- 调度器：`Kernel::exec_scheduler()`（stop_and_pause/resume/snapshot/terminal）、`Kernel::exec_events()`（broadcast 订阅）。
- 卡渲染：`channels/cards/taskcard.rs`（现签名 `task_card(task, accepted)`，改为快照渲染）。
- 卡刷新：inc-2 分流臂已做整卡 PATCH；本增量统一改走「快照渲染 + 串行 PATCH」。
- SimAdapter：测试需要注入终态——加 `SimControl` 共享控制柄（见组件 3）。

## 组件 1：卡渲染改快照驱动（channels/cards/taskcard.rs）

`task_card(task: &ExecTask, snap: &LaneSnapshot, card_generation: i64) -> String`：
- 状态区（如实映射增量 3 语义）：
  - current Running → `执行中 · 第 {seq} 轮（{started 相对时间}）`
  - Stopping → `停止中 · 第 {seq} 轮（未确认前不启动后续）`
  - Stopped → `已停止 · 第 {seq} 轮`
  - Completed/Failed → `第 {seq} 轮已结束（{完成/失败}）`（业务失败不冒充任务成功，R4）
  - Unknown/blocked_unknown → `⚠️ 状态待核对（已阻断后续派发）`
  - 无 current → binding 三态（沿用增量 2 文案）
- 队列区：`已受理待执行 {queued} 条`（queued>0 且 paused → 标注 `已暂停`；D2 标签保留「进程内·重启不保留」）。
- 控制区（按钮，`behaviors` callback，value 携带 `action/task/run/gen`）：
  - Running → `⏹ 停止并暂停`（value run=当前 run_id）
  - paused（无论有无 current）→ `▶ 恢复队列`
  - Stopping / blocked_unknown / archived → 不出按钮（无可合法操作）
- value 形态：`{"action":"exec_stop"|"exec_resume","task":"<id>","run":"<id|null>","gen":<i64>}`。

## 组件 2：回调处理（channels/taskcard/mod.rs + hub/mod.rs 臂）

hub/mod.rs：`ns.starts_with("exec_")` → `crate::channels::taskcard::handle_exec_action(&name, &config, &kernel, &adapter, action)`。

`handle_exec_action`（C9 重核全部维度）：
1. 解析 value：action/task/run/gen；字段缺 → 拒绝 toast（保守方向）。
2. 通道 `exec_tasks` 关闭 → 拒绝 toast。
3. `store.get(task)`：不存在或 `gen != task.card_generation`（旧代卡）→ toast「卡片已过期，操作未生效」，不动任何状态。
4. 任务 Archived → toast「任务已归档」。
5. `exec_stop`：`scheduler.stop_and_pause(task, Some(run))` →
   - Accepted → toast「已受理：停止中，队列已暂停」+ 刷新卡；
   - NoCurrentRun → toast「当前无执行中的轮次，队列已暂停」+ 刷新卡；
   - RunMismatch{actual} → toast「目标轮次已变化（当前第 N 轮状态 X），未执行停止」+ 刷新卡（不取消任何 Run）。
6. `exec_resume`：`scheduler.resume(task)` →
   - Resumed{dispatched} → toast（dispatched?「已恢复，继续执行」:「已恢复，队列空」）+ 刷新卡；
   - BlockedStopUnconfirmed → toast「停止尚未确认，保持暂停；确认后请再次恢复」。
7. toast 用平台延迟更新/回调应答现有机制（参照 obs/approval 的 action 应答方式；3 秒契约）。

## 组件 3：事件驱动刷新（channels/taskcard/relay.rs）

- `spawn_exec_relay(hub: &ChannelHub, kernel: &Arc<Kernel>)`：订阅 `kernel.exec_events()`；`ChannelHub::start_all` 末尾 spawn（仅当存在启用了 exec_tasks 的通道实例——没有也要能起：任务可能由 RPC 创建后经事件刷新？不，卡只在通道侧发。仅在有实例时 spawn，warn 记录跳过原因）。
- 每个事件：按 task_id 读 store（任务仍在、有 card_msg_id、通道实例在）→ `scheduler.snapshot(task_id)` → 渲染 → 串行 PATCH：
  - per-task `patch_lock: DashMap<ExecTaskId, Arc<tokio::Mutex<()>>>`（同一卡任意时刻只有一个 PATCH 在飞；事件只是提示，锁内重读快照再渲染——合并高频事件，N7/C9「只向前更新」）。
  - PATCH 失败：warn（呈现待同步，不反向改任务状态，C9）；连续失败不扩散（无重试风暴——下一事件自然带来新快照）。
- `RunTerminal` 后若任务无卡（CardPending）：跳过不补卡（C12/普通投递失败不擅自补卡）。

## 组件 4：SimControl（测试驱动柄）

- `SimAdapter` 加 `control: SimControl`（Arc 共享，clone  cheap）：方法 `complete(native_id)`/`fail(native_id)`/`cancel_confirms(native_id)`（经 sink 报对应终态）；`set_auto_complete(Option<Duration>)`。
- `Kernel` 装配时若 adapter 是 Sim 则持有 `exec_sim_control: Option<SimControl>`，accessor `exec_sim_control()`（prod 默认 hang；注释标明 P3 真实 adapter 替换后恒 None，仅供测试与本地驱动）。
- 分流臂 accept 后的整卡 PATCH 改走「snapshot 渲染」（与 relay 同一渲染函数）。

## 测试（channels/hub/taskflow_test.rs 扩展 + cards/taskcard.rs 单测）

1. 按钮全流程（flag on）：/task 建卡 → Thread 输入受理+派发（sim hang → Running）→ 点 `exec_stop` → toast 受理、卡面「停止中」、inbox 保留；重复点 → 幂等不重复 cancel。
2. Stopping 中点 `exec_resume` → toast 受阻、仍 paused；`SimControl.cancel_confirms` → 卡面「已停止」、仍 paused；再 resume → 派发下一条。
3. 旧回调：`gen+1` 伪造/任务不存在 → 拒绝 toast 零副作用；`run` 不匹配的 exec_stop → RunMismatch toast、当前 Run 不受扰。
4. 事件驱动：SimControl.complete → relay 刷新卡面为「第 N 轮已结束（完成）」且 queued>0 未暂停时自动派发下一条（卡面跟进）。
5. archived 任务回调 → toast 归档；exec_tasks=false 通道回调 → 拒绝。
6. 渲染单测：Running/Stopping/Stopped/Completed/Failed/Unknown/暂停队列/空队列各形态文案与按钮有无；value 含 task/run/gen 三字段。

## 检查门槛

同增量 3（check 三组合、clippy 零新增、fmt、全量绿——存量 4 沙箱失败除外）。

## 明确不做

当前轮问答/授权按钮（P3/N5）、结果区与历史入口（P4/P5）、CardKit 局部更新与展开态（P5）、L1 换代（P5/§8）、重启恢复（P6）、真实 adapter（P3）。
