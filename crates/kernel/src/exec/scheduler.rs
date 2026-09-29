//! ExecScheduler（chat-flow 增量 3）：按卡运行控制核心——派发、
//! 停止/暂停、恢复、终态收口。
//!
//! 设计依据 docs/design/chat-flow-technical-design.md N2/N3/N6/C3/C6/D3/D11：
//! - N6：每卡单一控制者——lane `Mutex` 是开始/暂停/停止/恢复顺序的
//!   唯一裁定者：决定都在锁内做出，adapter 网络调用在锁外，回来
//!   重核状态再提交；
//! - C3：执行资格五条件（`!paused`、`!blocked_unknown`、无在飞
//!   Run、队列非空、名额可用）在锁内一次判定；队首失败保留顺序
//!   位置，状态未知/停止未确认阻止新运行；
//! - N2：原生 Session 绑定在首次派发前完成；绑定失败/损坏如实
//!   阻断（`blocked_unknown`），绝不静默换绑/重建；
//! - C6/N6：停止中≠已停止；Stopping 中收到自然终态保存真实终态；
//!   停止未确认时恢复返回受阻，不预约自动恢复（候选 1）；
//! - D3：恢复不重试已停止轮（已停止输入不回队）；
//! - D11/N3：按卡隔离，名额统一分配（`Semaphore`）、轮次间公平、
//!   不抢占当前 Run；业务失败不自动暂停。
//!
//! 事件（`ExecEvent`，tokio broadcast）只是提示（hint）——状态唯
//! 一事实源是 lane 锁内字段（增量 4 卡面据此刷新，不据事件推断）。

use crate::exec::adapter::{ExecAdapter, TerminalKind, TerminalNotice};
use crate::exec::run::{RunRecord, RunStatus};
use crate::exec::{BindingState, ExecInbox, ExecTaskStore};
use crate::types::{ExecTaskId, KernelError, Result, RunId};
use chrono::Utc;
use dashmap::DashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

/// 调度事件（broadcast 提示；订阅者模式，增量 4 通道侧刷新用）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecEvent {
    RunStarted {
        task_id: ExecTaskId,
        run_id: RunId,
    },
    RunTerminal {
        task_id: ExecTaskId,
        run_id: RunId,
        kind: TerminalKind,
    },
    Paused {
        task_id: ExecTaskId,
    },
    Resumed {
        task_id: ExecTaskId,
    },
    StopUnconfirmed {
        task_id: ExecTaskId,
        run_id: RunId,
    },
}

impl ExecEvent {
    /// 事件所属任务（增量 4 relay 按此定位卡；事件只是提示，卡面
    /// 内容以锁内重读的快照为准——N7）。
    pub fn task_id(&self) -> &ExecTaskId {
        match self {
            Self::RunStarted { task_id, .. }
            | Self::RunTerminal { task_id, .. }
            | Self::Paused { task_id }
            | Self::Resumed { task_id }
            | Self::StopUnconfirmed { task_id, .. } => task_id,
        }
    }
}

/// `stop_and_pause` 的结果（三态如实，C6）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopOutcome {
    /// 停止已受理：paused 已置、Run 转 Stopping、cancel 已发出
    /// （或绑定在飞待补发）。受理≠已停止。
    Accepted { run: RunRecord },
    /// 无在飞 Run 可停（无 current 或 current 已终态）；暂停仍生效。
    NoCurrentRun { paused: bool },
    /// 期望 Run 与实际不符（旧按钮不得套到新 Run，C6/R7）：未
    /// 取消任何 Run；暂停已生效（stop 语义先于匹配核对）。
    RunMismatch {
        actual_run: RunId,
        actual_status: RunStatus,
    },
}

/// `resume` 的结果（C6：已恢复 / 仍受阻）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeOutcome {
    /// 已解除暂停并尝试派发（dispatched=false = 空队列等无可派；
    /// 重复 resume 幂等，不重复启动）。
    Resumed { dispatched: bool },
    /// 停止未确认：保持暂停，不预约自动恢复（N6 候选 1）。
    BlockedStopUnconfirmed,
}

/// 卡面/查询用的 lane 快照（增量 4 接卡面）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaneSnapshot {
    pub paused: bool,
    pub current: Option<RunRecord>,
    pub queued: usize,
    pub blocked_unknown: bool,
}

/// 每卡控制 lane（N6 唯一裁定者）。`std::sync::Mutex`：全部判定
/// 在锁内完成且锁绝不跨 `.await`（adapter 调用一律锁外）。
#[allow(clippy::struct_excessive_bools)] // lane 状态位天然是 bool 集合
#[derive(Default)]
struct TaskLane {
    paused: bool,
    /// 当前/最后一轮 Run（终态记录保留供卡面显示；在飞判定走
    /// `RunStatus::is_live`）
    current: Option<RunRecord>,
    /// 停止受理时刻（sweep 超时判据）
    stop_requested_at: Option<Instant>,
    /// 未知阻断（绑定失败/派发确认丢失）：阻止一切新派发，不跳过
    blocked_unknown: bool,
    /// 本卡绑定的原生身份（terminal 反查 lane 用；N2 一卡一绑定）
    native_session_id: Option<String>,
    /// 全局名额许可（持有到终态释放；未知状态保留不释放——原生
    /// 侧可能仍在运行，释放会对可能存活的作业超发）
    permit: Option<OwnedSemaphorePermit>,
    /// cancel 是否已发出（Starting 在飞时停止的补发判据）
    cancel_issued: bool,
    /// `StopUnconfirmed` 是否已标记（一次性，避免每次 sweep 重报）
    unconfirmed_notified: bool,
}

/// 执行调度器（每卡 lane + 全局名额）。
pub struct ExecScheduler {
    lanes: DashMap<ExecTaskId, Arc<Mutex<TaskLane>>>,
    /// 全局并发名额（`exec.max_concurrent_runs`，统一分配）
    slots: Arc<Semaphore>,
    store: Arc<dyn ExecTaskStore>,
    adapter: Arc<dyn ExecAdapter>,
    inbox: ExecInbox,
    event_tx: broadcast::Sender<ExecEvent>,
    /// 停止确认超时（`exec.stop_confirm_timeout_secs`）
    stop_confirm_timeout: Duration,
}

impl ExecScheduler {
    #[allow(clippy::needless_pass_by_value)] // 配置对象按值持有是本意（所有权清晰）
    pub fn new(
        store: Arc<dyn ExecTaskStore>,
        adapter: Arc<dyn ExecAdapter>,
        inbox: ExecInbox,
        event_tx: broadcast::Sender<ExecEvent>,
        config: crate::config::ExecConfig,
    ) -> Self {
        Self {
            lanes: DashMap::new(),
            slots: Arc::new(Semaphore::new(config.max_concurrent_runs)),
            store,
            adapter,
            inbox,
            event_tx,
            stop_confirm_timeout: Duration::from_secs(config.stop_confirm_timeout_secs),
        }
    }

    /// 事件订阅口（`Kernel::exec_events` 经此暴露）。
    pub fn subscribe(&self) -> broadcast::Receiver<ExecEvent> {
        self.event_tx.subscribe()
    }

    /// 周期 sweep 的建议间隔（超时的一半，钳 1–30s；Kernel 装配用）。
    pub fn sweep_interval(&self) -> Duration {
        Duration::from_secs((self.stop_confirm_timeout.as_secs() / 2).clamp(1, 30))
    }

    /// adapter 终态回调泵：sink 上报逐条转 `terminal`（C4 回调
    /// 入口；`Kernel::new` 装配时 spawn）。
    pub async fn terminal_pump(
        self: Arc<Self>,
        mut rx: mpsc::UnboundedReceiver<TerminalNotice>,
        cancel: CancellationToken,
    ) {
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                notice = rx.recv() => {
                    match notice {
                        Some((native, kind)) => self.terminal(&native, kind).await,
                        None => break,
                    }
                }
            }
        }
    }

    /// 取 lane（无则建默认 lane；lanes 表锁永远先于 lane 锁获取，
    /// 全模块统一锁序，无嵌套反向）。
    fn lane(&self, task_id: &ExecTaskId) -> Arc<Mutex<TaskLane>> {
        Arc::clone(
            self.lanes
                .entry(task_id.clone())
                .or_insert_with(|| Arc::new(Mutex::new(TaskLane::default())))
                .value(),
        )
    }

    /// 尝试派发（accept/resume/terminal 后调用）。返回是否新起了
    /// Run；Err 仅来自登记读取失败（已回滚 `Starting`，lane 可重试）。
    ///
    /// 顺序纪律（N6）：C3 五条件与 `Starting` 占位在锁内一次完成
    /// （占位即关死并发派发——第二调用见在飞即退）；绑定与
    /// `start_run` 在锁外；回锁重核状态后才提交 Running/Unknown。
    pub async fn try_dispatch(&self, task_id: &ExecTaskId) -> Result<bool> {
        let lane = self.lane(task_id);
        // ── 锁内：C3 资格五条件 + Starting 占位（唯一裁定者）──
        let (input, run) = {
            let mut g = lane.lock().unwrap();
            if g.paused || g.blocked_unknown {
                return Ok(false);
            }
            if g.current.as_ref().is_some_and(|r| r.status.is_live()) {
                return Ok(false);
            }
            let Some(input) = self.inbox.peek_front(task_id) else {
                return Ok(false);
            };
            // 无名额：出锁返回——名额释放时由 terminal 触发重试。
            let Ok(permit) = Arc::clone(&self.slots).try_acquire_owned() else {
                return Ok(false);
            };
            let run = RunRecord {
                run_id: RunId::new(),
                task_id: task_id.clone(),
                input_seq: input.seq,
                text: input.text.clone(),
                image_keys: input.image_keys.clone(),
                status: RunStatus::Starting,
                started_at: Utc::now(),
                ended_at: None,
            };
            g.current = Some(run.clone());
            g.permit = Some(permit);
            g.cancel_issued = false;
            g.unconfirmed_notified = false;
            (input, run)
        };

        // ── 锁外：登记读取（失败回滚 Starting，可重试——尚未产生
        // 任何原生副作用，不算未知态）──
        let task = match self.store.get(task_id).await {
            Ok(Some(task)) => task,
            Ok(None) => {
                Self::rollback_starting(&lane, &run.run_id);
                return Err(KernelError::task(format!(
                    "exec task {task_id} not found on dispatch"
                )));
            }
            Err(e) => {
                Self::rollback_starting(&lane, &run.run_id);
                return Err(e);
            }
        };

        // ── 锁外：绑定（N2：首次派发前完成；失败/损坏如实阻断，
        // 绝不静默换绑/重建）──
        let native_id = match task.binding {
            BindingState::Bound => {
                let id = task.provider_session_id.clone().unwrap_or_default();
                lane.lock().unwrap().native_session_id = Some(id.clone());
                id
            }
            BindingState::Uninitialized => match self.adapter.create_session(&task).await {
                Ok(id) => match self.store.bind_provider_session(task_id, &id).await {
                    Ok(_) => {
                        lane.lock().unwrap().native_session_id = Some(id.clone());
                        id
                    }
                    Err(e) => {
                        Self::mark_blocked(&lane, &run.run_id, "bind", &e);
                        return Ok(false);
                    }
                },
                Err(e) => {
                    Self::mark_blocked(&lane, &run.run_id, "create_session", &e);
                    return Ok(false);
                }
            },
            BindingState::Broken => {
                tracing::warn!(
                    task_id = %task_id,
                    "exec dispatch blocked: binding is broken (N2: never auto-recreate)"
                );
                Self::block_lane(&lane, &run.run_id);
                return Ok(false);
            }
        };

        // ── 锁外：派发（start_run 同步 ack = 原生已确认开始）──
        match self.adapter.start_run(&native_id, &input).await {
            Ok(()) => {
                // 回锁重核（锁外等待期间停止/终态可能已发生）。
                enum Post {
                    /// 正常确认 Running
                    Acked,
                    /// 启动中收到停止：原生已开跑 → 锁外补发 cancel
                    CancelAfter,
                    /// 终态经 sink 抢先到达并已收口：如实不再补记
                    AlreadyTerminaled,
                }
                let post = {
                    let mut g = lane.lock().unwrap();
                    match g.current.as_mut().filter(|r| r.run_id == run.run_id) {
                        None => Post::AlreadyTerminaled,
                        Some(cur) => match cur.status {
                            RunStatus::Starting => {
                                cur.status = RunStatus::Running;
                                self.inbox.pop_front(task_id);
                                Post::Acked
                            }
                            RunStatus::Stopping => {
                                self.inbox.pop_front(task_id);
                                if g.cancel_issued {
                                    Post::Acked
                                } else {
                                    g.cancel_issued = true;
                                    Post::CancelAfter
                                }
                            }
                            _ => Post::AlreadyTerminaled,
                        },
                    }
                };
                if !matches!(post, Post::AlreadyTerminaled) {
                    let _ = self.event_tx.send(ExecEvent::RunStarted {
                        task_id: task_id.clone(),
                        run_id: run.run_id.clone(),
                    });
                }
                if matches!(post, Post::CancelAfter) {
                    // C6/N6 竞态收口：Starting 在飞时受理的停止，等
                    // 原生确认开始后补发 cancel。
                    if let Err(e) = self.adapter.cancel(&native_id).await {
                        tracing::warn!(
                            task_id = %task_id,
                            error = %e,
                            "exec cancel after late start-ack failed; stop stays unconfirmed"
                        );
                    }
                }
                Ok(true)
            }
            Err(e) => {
                // 派发确认丢失（C4：可能已发送也可能未发送）→ Unknown
                // + blocked_unknown：不 pop、不跳过该输入（N3）；名额
                // 保留不释放（原生侧可能仍在运行，释放即超发）。
                tracing::warn!(
                    task_id = %task_id,
                    error = %e,
                    "exec start_run unconfirmed; lane blocked (input kept at queue front)"
                );
                let mut g = lane.lock().unwrap();
                if let Some(cur) = g.current.as_mut().filter(|r| r.run_id == run.run_id) {
                    cur.status = RunStatus::Unknown;
                    // ended_at 留 None：终态未知，不伪造结束时刻。
                }
                g.blocked_unknown = true;
                Ok(false)
            }
        }
    }

    /// 原生终态收口（sink 泵与测试的直达入口）。按 native id 反查
    /// lane（N2 一卡一绑定）；竞态如实：Stopping 中收到自然终态
    /// （Completed/Failed）保存真实终态，取消确认才记 Stopped。
    pub async fn terminal(&self, native_session_id: &str, kind: TerminalKind) {
        // 反查 lane（ lanes 表锁先行、取到 Arc 即放，锁序单向）。
        let mut found = None;
        for e in &self.lanes {
            let lane = Arc::clone(e.value());
            if lane.lock().unwrap().native_session_id.as_deref() == Some(native_session_id) {
                found = Some((e.key().clone(), lane));
                break;
            }
        }
        let Some((task_id, lane)) = found else {
            tracing::warn!(
                native_session_id,
                "exec terminal for unknown native session; ignored"
            );
            return;
        };
        {
            let mut g = lane.lock().unwrap();
            let Some(cur) = g.current.as_mut() else {
                tracing::warn!(
                    native_session_id,
                    "exec terminal without a current run; ignored"
                );
                return;
            };
            if !matches!(
                cur.status,
                RunStatus::Starting | RunStatus::Running | RunStatus::Stopping
            ) {
                tracing::warn!(
                    native_session_id,
                    status = ?cur.status,
                    "exec terminal for a non-live run; ignored"
                );
                return;
            }
            // Starting 中到达的终态同样消费输入（adapter 契约：先
            // ack 后报终态——ack 即输入已交原生侧）。
            if cur.status == RunStatus::Starting {
                self.inbox.pop_front(&task_id);
            }
            cur.status = match kind {
                TerminalKind::Completed => RunStatus::Completed,
                TerminalKind::Failed => RunStatus::Failed,
                TerminalKind::Cancelled => RunStatus::Stopped,
            };
            cur.ended_at = Some(Utc::now());
            let run_id = cur.run_id.clone();
            g.permit = None; // 释放名额
            g.stop_requested_at = None;
            let _ = self.event_tx.send(ExecEvent::RunTerminal {
                task_id: task_id.clone(),
                run_id,
                kind,
            });
        }
        // 终态后重试派发：本卡（!paused && !blocked 时业务失败也
        // 继续，N3）与可能因名额等待的他卡（名额刚释放；各 lane
        // 的 C3 条件由 try_dispatch 锁内自查，不合格即 no-op）。
        let candidates: Vec<ExecTaskId> = self.lanes.iter().map(|e| e.key().clone()).collect();
        for id in candidates {
            if let Err(e) = self.try_dispatch(&id).await {
                tracing::warn!(task_id = %id, error = %e, "exec post-terminal redispatch failed");
            }
        }
    }

    /// 停止并暂停（C6/N6）。锁内先 `paused=true`——关闭后续派发
    /// 资格，先于任何网络动作；再核对 Run 目标。
    pub async fn stop_and_pause(
        &self,
        task_id: &ExecTaskId,
        expected_run: Option<RunId>,
    ) -> StopOutcome {
        enum Act {
            NoCurrent,
            /// 已在 Stopping（幂等：重复停止不重复 cancel）
            Already(RunRecord),
            Cancel {
                native_id: Option<String>,
                run: RunRecord,
            },
        }
        let lane = self.lane(task_id);
        let act = {
            let mut g = lane.lock().unwrap();
            // ① 先暂停（先于任何网络动作；重复停止幂等不重复发事件）。
            if !g.paused {
                g.paused = true;
                let _ = self.event_tx.send(ExecEvent::Paused {
                    task_id: task_id.clone(),
                });
            }
            match g.current.as_ref().filter(|r| r.status.is_live()) {
                None => Act::NoCurrent,
                Some(cur) => {
                    if let Some(expected) = &expected_run {
                        if *expected != cur.run_id {
                            // 旧按钮不得套到新 Run（C6/R7）：不取消
                            // 任何 Run；暂停已生效（stop 语义在先）。
                            return StopOutcome::RunMismatch {
                                actual_run: cur.run_id.clone(),
                                actual_status: cur.status,
                            };
                        }
                    }
                    if cur.status == RunStatus::Stopping {
                        Act::Already(cur.clone())
                    } else {
                        let native_id = g.native_session_id.clone();
                        let cur = g.current.as_mut().unwrap();
                        cur.status = RunStatus::Stopping;
                        g.stop_requested_at = Some(Instant::now());
                        g.unconfirmed_notified = false;
                        // 绑定在飞（Starting 且尚无原生身份）：无法发
                        // cancel，待 start ack 回锁时补发（try_dispatch）。
                        g.cancel_issued = native_id.is_some();
                        Act::Cancel {
                            native_id,
                            run: g.current.clone().unwrap(),
                        }
                    }
                }
            }
        };
        match act {
            Act::NoCurrent => StopOutcome::NoCurrentRun { paused: true },
            Act::Already(run) => StopOutcome::Accepted { run },
            Act::Cancel { native_id, run } => {
                if let Some(native_id) = native_id {
                    // 锁外发取消；cancel 失败不撤销 Stopping——未确认
                    // 状态由 sweep 超时如实标记（C6：超时不报全停）。
                    if let Err(e) = self.adapter.cancel(&native_id).await {
                        tracing::warn!(
                            task_id = %task_id,
                            error = %e,
                            "exec cancel delivery failed; stop stays unconfirmed"
                        );
                    }
                }
                StopOutcome::Accepted { run }
            }
        }
    }

    /// 恢复队列（C6/N6 候选 1）：停止未确认 → 受阻且保持暂停，
    /// 不预约自动恢复；否则解除暂停并尝试派发。已停止的 Run 不
    /// 回队（D3：恢复不是重试）。
    pub async fn resume(&self, task_id: &ExecTaskId) -> ResumeOutcome {
        let lane = self.lane(task_id);
        {
            let mut g = lane.lock().unwrap();
            if g.current
                .as_ref()
                .is_some_and(|r| r.status == RunStatus::Stopping)
            {
                return ResumeOutcome::BlockedStopUnconfirmed;
            }
            if g.paused {
                g.paused = false;
                let _ = self.event_tx.send(ExecEvent::Resumed {
                    task_id: task_id.clone(),
                });
            }
        }
        let dispatched = match self.try_dispatch(task_id).await {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!(task_id = %task_id, error = %e, "exec dispatch after resume failed");
                false
            }
        };
        ResumeOutcome::Resumed { dispatched }
    }

    /// 停止确认清扫：Stopping 超阈值未报原生终态 → 一次性标记
    /// `StopUnconfirmed` 并保留 Stopping（未确认，继续禁写；不
    /// detach 续跑——N6/C6）。
    pub fn sweep_unconfirmed(&self) {
        for e in &self.lanes {
            let task_id = e.key().clone();
            let mut g = e.value().lock().unwrap();
            let Some((run_id, overdue)) = g.current.as_ref().and_then(|cur| {
                (cur.status == RunStatus::Stopping && !g.unconfirmed_notified).then(|| {
                    let overdue = g
                        .stop_requested_at
                        .is_some_and(|at| at.elapsed() >= self.stop_confirm_timeout);
                    (cur.run_id.clone(), overdue)
                })
            }) else {
                continue;
            };
            if overdue {
                g.unconfirmed_notified = true;
                tracing::warn!(
                    task_id = %task_id,
                    run_id = %run_id,
                    "exec stop unconfirmed past timeout; lane stays Stopping"
                );
                let _ = self
                    .event_tx
                    .send(ExecEvent::StopUnconfirmed { task_id, run_id });
            }
        }
    }

    /// lane 快照（卡面/查询用；只读，不创建 Run、不恢复队列——C7）。
    pub fn snapshot(&self, task_id: &ExecTaskId) -> LaneSnapshot {
        let lane = self.lane(task_id);
        let g = lane.lock().unwrap();
        LaneSnapshot {
            paused: g.paused,
            current: g.current.clone(),
            queued: self.inbox.len(task_id),
            blocked_unknown: g.blocked_unknown,
        }
    }

    /// 回滚 Starting 占位（登记读取失败：未产生原生副作用，
    /// lane 恢复可重试）。
    fn rollback_starting(lane: &Mutex<TaskLane>, run_id: &RunId) {
        let mut g = lane.lock().unwrap();
        if g.current
            .as_ref()
            .is_some_and(|r| r.run_id == *run_id && r.status == RunStatus::Starting)
        {
            g.current = None;
            g.permit = None;
        }
    }

    /// 标 `blocked_unknown` 并清理 `Starting` 占位（绑定类失败发生
    /// 在原生派发之前：`Starting` 记录不代表任何原生事实；输入保留
    /// 在队首不跳过；`blocked_unknown` 阻断后续一切派发——N2/N3）。
    fn mark_blocked(lane: &Mutex<TaskLane>, run_id: &RunId, stage: &str, err: &KernelError) {
        tracing::warn!(
            stage,
            error = %err,
            "exec dispatch blocked: lane marked blocked_unknown"
        );
        Self::block_lane(lane, run_id);
    }

    fn block_lane(lane: &Mutex<TaskLane>, run_id: &RunId) {
        let mut g = lane.lock().unwrap();
        if g.current.as_ref().is_some_and(|r| r.run_id == *run_id) {
            g.current = None;
        }
        g.permit = None;
        g.blocked_unknown = true;
    }
}

#[cfg(test)]
#[path = "scheduler_test.rs"]
mod tests;
