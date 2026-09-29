//! ExecScheduler（chat-flow 增量 3）：按卡运行控制核心——派发、
//! 停止/暂停、恢复、终态收口。增量 5 起同时是 Run 事实与结果正
//! 文的唯一写入方（N7/N9）。
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
//! 增量 5 事实纪律（N7/N9/C7）：
//! - 事实写入在锁内状态提交之后、锁外执行；写入失败只 warn——不
//!   阻断已确认的运行，查询面如实反映缺失（快照仍以 lane 为准）；
//! - 结果上报按 native id 反查 lane/Run；反查不到（迟到、未知）
//!   → warn 忽略，绝不归属最新轮；
//! - `save_result` 成功（首次保存）才发 `ResultPublished` 并触发
//!   卡面刷新——先保存再公布（N9）。
//!
//! 增量 6 追加（N11/N12/C8/N10/D9）：
//! - 启动核对 `boot_sweep`（N11/D9）：上一进程生命周期未闭合的
//!   Run 如实标 `interrupted`——不伪造终态、不凭旧 running 显示
//!   正常、绝不自动重跑；
//! - 受理「是否开始」（N12）：原生确认开始（含 Starting 中终态
//!   消费）即 `mark_started`——重启后重送据此区分「曾等待」与
//!   「已派发」，两种都绝不重新执行；
//! - 空闲释放（C8/N10）：终态事实写定且无在飞 Run → per-lane 释
//!   放计时；暂停且有等待项同样释放（yomi 队列/暂停不动）；计
//!   时被任何 lane 状态变化打断；释放不消费队列、不解暂停、不
//!   发事件伪造状态——恢复是下次派发用原 native id（D9）。
//!
//! 增量 9 追加（N12/C1）：受理/凭据/派发微窗口硬化——「进程内
//! 查重 → 受理凭据 → 入队」收敛为 `accept_input` 单一方法，与
//! `try_dispatch` 的取队段共用 per-task 异步受理锁
//! （`accept_locks`）；锁序全局统一：lanes 表锁永远最先，
//! `accept_lock` 先于 lane 锁，持锁段内零 await。跨进程重送凭
//! 据核对不通过时根本不入队（增量 6「先入队再撤回」的窗口不
//! 复存在）——重送项对派发永不可见，绝不重复执行。
//!
//! 事件（`ExecEvent`，tokio broadcast）只是提示（hint）——状态唯
//! 一事实源是 lane 锁内字段（增量 4 卡面据此刷新，不据事件推断）。

use crate::exec::adapter::{AdapterNotice, ExecAdapter, TerminalKind, TerminalNotice};
use crate::exec::run::{RunRecord, RunStatus};
use crate::exec::{AcceptOutcome, BindingState, ExecFactStore, ExecInbox, ExecTask, ExecTaskStore};
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
    /// 一轮结果正文已保存（增量 5，N9：先保存再公布——本事件即
    /// 「公布」，卡面据此刷新出结果行）。
    ResultPublished {
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
            | Self::StopUnconfirmed { task_id, .. }
            | Self::ResultPublished { task_id, .. } => task_id,
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

/// `accept_input` 的受理判定（增量 9，N12/C1）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcceptVerdict {
    /// 新受理（任务内序号，先到先得）。
    Accepted { seq: u64 },
    /// 进程内重送：静默——受理是一次性事实（C1），重复投递零
    /// 可见动作。
    Duplicate,
    /// 跨进程重送：受理凭据先于本进程存在（上一进程生命周期受
    /// 理过）——不入队、不派发；`started` 如实区分「曾等待」与
    /// 「已派发」（两者都绝不重新执行），回复文案由调用方给出。
    NotRecovered { started: bool },
}

/// 卡面/查询用的 lane 快照（增量 4 接卡面）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaneSnapshot {
    pub paused: bool,
    pub current: Option<RunRecord>,
    pub queued: usize,
    pub blocked_unknown: bool,
    /// 运行实例已释放（增量 6，C8/N10）：历史/队列/暂停不动，下
    /// 次输入用原 Session 恢复——卡面「已释放」行的凭据。
    pub released: bool,
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
    /// lane 活动代次（增量 6，C8/N10 空闲释放的打断凭据）：任何
    /// lane 状态变化（Starting 占位/终态提交/暂停/恢复）+1；释放
    /// 计时到点重核代次，不匹配即放弃——不释放有活动的 lane。
    activity_epoch: u64,
    /// 运行实例已释放（增量 6）：释放不删历史、不动队列/暂停；
    /// `native_session_id` 保留——恢复是下次派发用原 id（sim 即
    /// 同 id `start_run`）。
    released: bool,
}

impl TaskLane {
    /// 记录一次 lane 状态变化（打断在飞的空闲释放计时）。
    fn note_activity(&mut self) {
        self.activity_epoch = self.activity_epoch.wrapping_add(1);
    }
}

/// 执行调度器（每卡 lane + 全局名额）。
pub struct ExecScheduler {
    lanes: DashMap<ExecTaskId, Arc<Mutex<TaskLane>>>,
    /// 受理锁（增量 9，N12/C1）：`accept_input` 的「查重→凭据→
    /// 入队」与 `try_dispatch` 的 C3 判定 + peek 段共用同一把
    /// per-task 异步锁。锁序全局统一：lanes 表锁永远最先，
    /// `accept_lock` 先于 lane 锁，无反向嵌套；lane 锁（std）
    /// 不跨 `.await` 的纪律不变。
    accept_locks: DashMap<ExecTaskId, Arc<tokio::sync::Mutex<()>>>,
    /// 全局并发名额（`exec.max_concurrent_runs`，统一分配）
    slots: Arc<Semaphore>,
    store: Arc<dyn ExecTaskStore>,
    adapter: Arc<dyn ExecAdapter>,
    inbox: ExecInbox,
    event_tx: broadcast::Sender<ExecEvent>,
    /// 停止确认超时（`exec.stop_confirm_timeout_secs`）
    stop_confirm_timeout: Duration,
    /// 空闲释放阈值（`exec.idle_release_secs`，增量 6，C8/N10）
    idle_release: Duration,
    /// Run 事实 + 结果正文持久化（增量 5，N7/N9；None = 纯内存测
    /// 试台——事实写入整体关闭，行为与增量 3/4 一致）
    facts: Option<Arc<dyn ExecFactStore>>,
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
            accept_locks: DashMap::new(),
            slots: Arc::new(Semaphore::new(config.max_concurrent_runs)),
            store,
            adapter,
            inbox,
            event_tx,
            stop_confirm_timeout: Duration::from_secs(config.stop_confirm_timeout_secs),
            idle_release: Duration::from_secs(config.idle_release_secs),
            facts: None,
        }
    }

    /// 接 Run 事实 store（增量 5；`Kernel::new` 装配时注入）。
    #[must_use]
    pub fn with_facts(mut self, facts: Arc<dyn ExecFactStore>) -> Self {
        self.facts = Some(facts);
        self
    }

    /// 事件订阅口（`Kernel::exec_events` 经此暴露）。
    pub fn subscribe(&self) -> broadcast::Receiver<ExecEvent> {
        self.event_tx.subscribe()
    }

    /// 周期 sweep 的建议间隔（超时的一半，钳 1–30s；Kernel 装配用）。
    pub fn sweep_interval(&self) -> Duration {
        Duration::from_secs((self.stop_confirm_timeout.as_secs() / 2).clamp(1, 30))
    }

    /// 启动核对（增量 6，N11/D9）：上一进程生命周期未闭合的 Run
    /// （starting/running/stopping）如实标 `interrupted`——中断是
    /// 事实状态，不伪造终态（`terminal_kind`/`ended_at` 留
    /// NULL），不凭旧 running 显示正常，绝不自动重跑。
    /// `Kernel::new` 装配后调用一次；返回标记行数（无 facts 的纯
    /// 内存台恒 0）。
    pub async fn boot_sweep(&self) -> Result<u64> {
        let Some(facts) = &self.facts else {
            return Ok(0);
        };
        let n = facts.mark_interrupted_open_runs().await?;
        if n > 0 {
            tracing::warn!(
                n,
                "exec boot sweep: open runs from a previous process lifetime marked interrupted (never re-executed)"
            );
        } else {
            tracing::info!("exec boot sweep: no open runs from previous lifetimes");
        }
        Ok(n)
    }

    /// 空闲释放计时（增量 6，C8/N10）：terminal 落定且 lane 无在
    /// 飞 Run 后武装。到点重核活动代次——期间任何 lane 状态变化
    /// （新派发/停止/恢复/终态）都已 bump 代次，不匹配即放弃；
    /// 释放只关闭运行实例：不消费队列、不解暂停、不发事件伪造
    /// 状态；`native_session_id` 保留（恢复用原 id）。
    fn arm_idle_release(&self, task_id: &ExecTaskId, lane: &Arc<Mutex<TaskLane>>) {
        let epoch = lane.lock().unwrap().activity_epoch;
        let adapter = Arc::clone(&self.adapter);
        let lane = Arc::clone(lane);
        let task_id = task_id.clone();
        let delay = self.idle_release;
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let native = {
                let g = lane.lock().unwrap();
                let idle = g.activity_epoch == epoch
                    && !g.released
                    && !g.current.as_ref().is_some_and(|r| r.status.is_live());
                if idle {
                    g.native_session_id.clone()
                } else {
                    None
                }
            };
            let Some(native) = native else {
                return;
            };
            if let Err(e) = adapter.release(&native).await {
                // 释放失败只 warn：实例原样保留（不谎称已释放）；下
                // 次终态自然再武装。
                tracing::warn!(
                    task_id = %task_id,
                    error = %e,
                    "exec idle release failed; instance left as-is"
                );
                return;
            }
            // 标记前再核代次：释放等待期间起跑的新 Run 拥有会话，
            // 不标 released（释放与起跑竞态按「恢复」语义兼容）。
            let mut g = lane.lock().unwrap();
            if g.activity_epoch == epoch {
                g.released = true;
                tracing::info!(
                    task_id = %task_id,
                    "exec idle release: native instance released (history kept; next input resumes the same session)"
                );
            }
        });
    }

    /// adapter 回调泵：sink 上报逐条转 `terminal` / `result_reported`
    ///（C4/N9 回调入口；`Kernel::new` 装配时 spawn）。
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
                        Some((native, AdapterNotice::Terminal(kind))) => {
                            self.terminal(&native, kind).await;
                        }
                        Some((native, AdapterNotice::Result(body))) => {
                            self.result_reported(&native, body).await;
                        }
                        None => break,
                    }
                }
            }
        }
    }

    /// 按 native id 反查 lane（N2 一卡一绑定；`terminal` 与
    /// `result_reported` 共用。lanes 表锁先行、取到 Arc 即放，锁
    /// 序单向）。
    fn find_lane_by_native(
        &self,
        native_session_id: &str,
    ) -> Option<(ExecTaskId, Arc<Mutex<TaskLane>>)> {
        for e in &self.lanes {
            let lane = Arc::clone(e.value());
            if lane.lock().unwrap().native_session_id.as_deref() == Some(native_session_id) {
                return Some((e.key().clone(), lane));
            }
        }
        None
    }

    /// 结果上报收口（增量 5，N9）：按 native id 反查 lane——反查
    /// 不到（未知会话）→ warn 忽略。归属规则：上报通道只带 native
    /// 身份不带 run 身份，而一卡单 writer 顺序执行（Provider 完成
    /// 第 N 轮才开第 N+1 轮，正文按轮序发布），故归属本任务**最
    /// 早一份尚无权威正文的 Run**——A 的迟到正文在 B 已成为
    /// current 后到达仍归 A，绝不污染 B，也绝不猜「最新一轮」。
    /// 全部 Run 已有正文 → 迟到重报，warn 忽略。保存 insert-once
    /// 成功才发 `ResultPublished`（先保存再公布）；结果保存不改
    /// lane 任何状态（只经 lane 定位任务归属）。
    pub async fn result_reported(&self, native_session_id: &str, body: impl Into<String>) {
        let Some(facts) = &self.facts else {
            return;
        };
        let body = body.into();
        let Some((task_id, _lane)) = self.find_lane_by_native(native_session_id) else {
            tracing::warn!(
                native_session_id,
                "exec result for unknown native session; ignored (never attached to the latest run)"
            );
            return;
        };
        // 最早无正文的 Run（按 input_seq 升序第一份）。
        let target = match facts.runs_for(&task_id).await {
            Ok(runs) => {
                let mut found = None;
                for r in runs {
                    match facts.result_for(&r.run_id).await {
                        Ok(None) => {
                            found = Some(r);
                            break;
                        }
                        Ok(Some(_)) => {}
                        Err(e) => {
                            tracing::warn!(
                                task_id = %task_id,
                                run_id = %r.run_id,
                                error = %e,
                                "exec result attribution lookup failed; not published"
                            );
                            return;
                        }
                    }
                }
                found
            }
            Err(e) => {
                tracing::warn!(
                    task_id = %task_id,
                    error = %e,
                    "exec result attribution failed; not published"
                );
                return;
            }
        };
        let Some(run) = target else {
            tracing::warn!(
                native_session_id,
                task_id = %task_id,
                "exec result but every run already has an authoritative body; late duplicate ignored"
            );
            return;
        };
        let meta = serde_json::json!({
            "native_session_id": native_session_id,
        });
        match facts
            .save_result(&run.run_id, &task_id, run.input_seq, &body, &meta)
            .await
        {
            Ok(true) => {
                let _ = self.event_tx.send(ExecEvent::ResultPublished {
                    task_id: task_id.clone(),
                    run_id: run.run_id.clone(),
                });
            }
            Ok(false) => {
                // 归属判定与保存之间的并发重报：INSERT OR IGNORE
                // 兜底——先保存者为准，不覆盖不重发事件。
                tracing::warn!(
                    task_id = %task_id,
                    run_id = %run.run_id,
                    "exec result already saved for this run; duplicate ignored (authoritative body kept)"
                );
            }
            Err(e) => {
                tracing::warn!(
                    task_id = %task_id,
                    run_id = %run.run_id,
                    error = %e,
                    "exec result save failed; not published"
                );
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

    /// 取受理锁（增量 9；与 lanes 表同律：表锁先行、取到 Arc 即
    /// 放，锁序单向——`accept_lock` 永远先于 lane 锁获取）。
    fn accept_lock(&self, task_id: &ExecTaskId) -> Arc<tokio::sync::Mutex<()>> {
        Arc::clone(
            self.accept_locks
                .entry(task_id.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .value(),
        )
    }

    /// 受理一条任务 Thread 输入（增量 9，N12/C1）：「进程内查重
    /// → 受理凭据 → 入队」收敛为持受理锁的单一方法，与
    /// `try_dispatch` 的取队段互斥——跨重启重送在凭据核对不通
    /// 过时**根本不入队**（增量 6「先入队再撤回」的窗口不复存
    /// 在：重送项对派发永不可见）。锁内不碰 lane 锁；凭据读写
    /// 是仅有的 await。锁内顺序（同一次持锁完成）：
    /// 1. inbox 查重 → `Duplicate`（不触碰凭据）；
    /// 2. 凭据写入 Ok(false)（上一进程受理过）→ 只登记进程内
    ///    去重记忆（受理是一次性事实，后续重送静默）后返回
    ///    `NotRecovered`——不入队、不派发，无需撤回；
    /// 3. Ok(true) → `inbox.accept` 入队 → `Accepted`；
    /// 4. 凭据写入 Err → warn 后仍按 `Accepted`（增量 6 既有纪
    ///    律：不阻断受理；代价是「DB 故障 + 重启 + 平台重送」组
    ///    合下凭据缺失会按新输入再受理——如实记录，不伪造拒收）。
    pub async fn accept_input(
        &self,
        task: &ExecTask,
        msg_id: String,
        sender_open_id: String,
        text: String,
        image_keys: Vec<String>,
    ) -> AcceptVerdict {
        let _guard = self.accept_lock(&task.id).lock_owned().await;
        if self.inbox.is_seen(&task.id, &msg_id) {
            return AcceptVerdict::Duplicate;
        }
        if let Some(facts) = &self.facts {
            match facts
                .record_acceptance(&task.channel_name, &msg_id, &task.id)
                .await
            {
                Ok(true) => {}
                Ok(false) => {
                    // 「是否开始」（N12）：曾等待与已派发如实区分——
                    // 两者都绝不重新执行。
                    let started = facts
                        .acceptance_for(&task.channel_name, &msg_id)
                        .await
                        .ok()
                        .flatten()
                        .is_some_and(|a| a.started);
                    self.inbox.note_seen(&task.id, &msg_id);
                    return AcceptVerdict::NotRecovered { started };
                }
                Err(e) => {
                    tracing::warn!(
                        task_id = %task.id,
                        error = %e,
                        "exec acceptance record failed; proceeding as accepted"
                    );
                }
            }
        }
        match self
            .inbox
            .accept(&task.id, msg_id, sender_open_id, text, image_keys)
        {
            AcceptOutcome::Accepted { seq } => AcceptVerdict::Accepted { seq },
            // 受理锁内查重后不可能撞重；防御性如实映射，不 panic。
            AcceptOutcome::Duplicate => AcceptVerdict::Duplicate,
        }
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
        // 增量 9（N12/C1）：受理锁先于 lane 锁（全局唯一锁序，与
        // accept_input 的「查重→凭据→入队」互斥——跨进程重送项
        // 对派发永不可见）；持锁段内零 await（peek/占位全是同步
        // 操作），guard 随块结束即放。
        let (input, run) = {
            let _accept_guard = self.accept_lock(task_id).lock_owned().await;
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
            // 取得资格 = lane 状态变化（打断空闲释放计时）；新 Run
            // 拥有原生会话——released 标志随起跑清除（恢复已发生）。
            g.released = false;
            g.note_activity();
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
                    // 增量 5（N7）：Running 提交后写 Run 开始事实，
                    // 状态取 lane 内实际提交值（Acked 亦可能是
                    // Stopping——Starting 在飞时停止已受理）。失败
                    // 只 warn——不阻断已确认的运行，查询面如实反映
                    // 缺失（lane 快照仍是状态唯一事实源）。
                    if let Some(facts) = &self.facts {
                        // 增量 6（N12）：原生确认开始 = 受理「已开
                        // 始」——重启后重送据此如实回答「已开始执
                        // 行，不重复执行」而非「未恢复」。
                        if let Err(e) = facts.mark_started(&task.channel_name, &input.msg_id).await
                        {
                            tracing::warn!(
                                task_id = %task_id,
                                error = %e,
                                "exec acceptance mark_started failed; run continues"
                            );
                        }
                        let mut rec = run.clone();
                        rec.status = lane
                            .lock()
                            .unwrap()
                            .current
                            .as_ref()
                            .filter(|r| r.run_id == run.run_id)
                            .map_or(RunStatus::Running, |r| r.status);
                        if let Err(e) = facts.run_started(&rec).await {
                            tracing::warn!(
                                task_id = %task_id,
                                run_id = %rec.run_id,
                                error = %e,
                                "exec run start fact write failed; run continues"
                            );
                        }
                    }
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
        let Some((task_id, lane)) = self.find_lane_by_native(native_session_id) else {
            tracing::warn!(
                native_session_id,
                "exec terminal for unknown native session; ignored"
            );
            return;
        };
        // 终态事实写入载荷（锁内提交时捕获，锁外写 store）。
        let (terminal_fact, starting_consumed) = {
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
            let consumed = (cur.status == RunStatus::Starting)
                .then(|| self.inbox.pop_front(&task_id))
                .flatten();
            cur.status = match kind {
                TerminalKind::Completed => RunStatus::Completed,
                TerminalKind::Failed => RunStatus::Failed,
                TerminalKind::Cancelled => RunStatus::Stopped,
            };
            cur.ended_at = Some(Utc::now());
            let run_id = cur.run_id.clone();
            let fact = cur.clone();
            g.permit = None; // 释放名额
            g.stop_requested_at = None;
            // 终态提交 = lane 状态变化（打断在飞的空闲释放计时）。
            g.note_activity();
            let _ = self.event_tx.send(ExecEvent::RunTerminal {
                task_id: task_id.clone(),
                run_id,
                kind,
            });
            (fact, consumed)
        };
        // 增量 5（N7）：终态事实持久化。先 run_started（INSERT OR
        // IGNORE 幂等）再 run_terminal——泵与 try_dispatch 并发时
        // 保证行存在，terminal UPDATE 不落空；单向守卫在 store 侧
        // （`terminal_kind IS NULL` 才写，重复/迟到上报不改写）。
        // 失败只 warn：不阻断已收口的终态，查询面如实反映缺失。
        if let Some(facts) = &self.facts {
            let rec = terminal_fact;
            if let Err(e) = facts.run_started(&rec).await {
                tracing::warn!(
                    task_id = %task_id,
                    run_id = %rec.run_id,
                    error = %e,
                    "exec run fact backfill on terminal failed"
                );
            }
            if let Err(e) = facts
                .run_terminal(&rec.run_id, rec.status, kind, rec.ended_at.unwrap())
                .await
            {
                tracing::warn!(
                    task_id = %task_id,
                    run_id = %rec.run_id,
                    error = %e,
                    "exec run terminal fact write failed; scheduler state unaffected"
                );
            }
            // 增量 6（N12）：Starting 中终态消费的输入同样「已开
            // 始」——与 try_dispatch 的标记同一语义（ack 即输入已
            // 交原生侧）。lane 不存通道名，经任务登记取（罕见路
            // 径，读一次）。
            if let Some(input) = starting_consumed {
                match self.store.get(&task_id).await {
                    Ok(Some(task)) => {
                        if let Err(e) = facts.mark_started(&task.channel_name, &input.msg_id).await
                        {
                            tracing::warn!(
                                task_id = %task_id,
                                error = %e,
                                "exec acceptance mark_started on terminal failed"
                            );
                        }
                    }
                    Ok(None) => {
                        tracing::warn!(
                            task_id = %task_id,
                            "exec acceptance mark_started: task gone; skipped"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            task_id = %task_id,
                            error = %e,
                            "exec acceptance mark_started: task lookup failed"
                        );
                    }
                }
            }
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
        // 增量 6（C8/N10）：重试后 lane 仍无在飞 Run → 武装空闲释
        // 放计时。暂停且有 B/C 等待同样释放（yomi 队列/暂停不
        // 动）；`blocked_unknown` 不释放——原生侧可能仍在运行
        // （与名额保留同一道理）；计时被任何 lane 状态变化打断。
        let arm = {
            let g = lane.lock().unwrap();
            !g.blocked_unknown
                && !g.released
                && !g.current.as_ref().is_some_and(|r| r.status.is_live())
                && g.native_session_id.is_some()
        };
        if arm {
            self.arm_idle_release(&task_id, &lane);
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
            // 停止动作 = lane 状态变化（打断空闲释放计时——停止动
            // 作期间不释放，C8/N10）。
            g.note_activity();
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
            // 恢复动作 = lane 状态变化（打断空闲释放计时）。
            g.note_activity();
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
            released: g.released,
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
