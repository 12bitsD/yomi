//! chat-flow 增量 3 规格测试（调度器）：全部用可控 SimAdapter +
//! 内存 sqlite store。设计依据 docs/design/chat-flow-technical-design.md
//! N2/N3/N6/C3/C6/D3/D11。
//!
//! 确定性原则：终态确认一律经 `scheduler.terminal(...)` 直达注入
//! （不依赖挂钟）；仅「sink 泵通路」一条用短 `complete_after` +
//! 轮询（带 5s 上限）。

use super::*;
use crate::exec::adapter::{ExecAdapterSink, SimAdapter};
use crate::exec::{
    AcceptOutcome, CreateExecTask, ExecProvider, ExecTask, ExecTaskSource, SqliteExecFactStore,
    SqliteExecTaskStore,
};
use crate::storage::migrations::run_migrations;
use sqlx::sqlite::SqlitePoolOptions;

struct Harness {
    sched: Arc<ExecScheduler>,
    store: Arc<dyn ExecTaskStore>,
    adapter: Arc<SimAdapter>,
    inbox: ExecInbox,
    event_rx: broadcast::Receiver<ExecEvent>,
    /// Run 事实 store（增量 5 测试台装配；None = 纯内存台）
    facts: Option<Arc<SqliteExecFactStore>>,
}

async fn harness_with(
    max_runs: usize,
    stop_timeout_secs: u64,
    adapter: SimAdapter,
    store: Option<Arc<dyn ExecTaskStore>>,
) -> Harness {
    let store = match store {
        Some(s) => s,
        None => {
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect("sqlite::memory:")
                .await
                .unwrap();
            run_migrations(&pool).await.unwrap();
            Arc::new(SqliteExecTaskStore::new(pool))
        }
    };
    let inbox = ExecInbox::new();
    let (event_tx, event_rx) = broadcast::channel(256);
    let adapter = Arc::new(adapter);
    let sched = Arc::new(ExecScheduler::new(
        Arc::clone(&store),
        adapter.clone(),
        inbox.clone(),
        event_tx,
        crate::config::ExecConfig {
            max_concurrent_runs: max_runs,
            stop_confirm_timeout_secs: stop_timeout_secs,
        },
    ));
    Harness {
        sched,
        store,
        adapter,
        inbox,
        event_rx,
        facts: None,
    }
}

/// 默认测试台：2 名额、30s 停止确认超时、挂起 SimAdapter（一切
/// 终态由测试显式注入）。
async fn harness(adapter: SimAdapter) -> Harness {
    harness_with(2, 30, adapter, None).await
}

/// 增量 5 测试台：任务 store 与事实 store 同库（镜像生产装配），
/// 调度器 `.with_facts` 接事实写入。
async fn harness_facts(adapter: SimAdapter) -> Harness {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    run_migrations(&pool).await.unwrap();
    let store: Arc<dyn ExecTaskStore> = Arc::new(SqliteExecTaskStore::new(pool.clone()));
    let facts = Arc::new(SqliteExecFactStore::new(pool));
    let inbox = ExecInbox::new();
    let (event_tx, event_rx) = broadcast::channel(256);
    let adapter = Arc::new(adapter);
    let sched = Arc::new(
        ExecScheduler::new(
            Arc::clone(&store),
            adapter.clone(),
            inbox.clone(),
            event_tx,
            crate::config::ExecConfig {
                max_concurrent_runs: 2,
                stop_confirm_timeout_secs: 30,
            },
        )
        .with_facts(facts.clone()),
    );
    Harness {
        sched,
        store,
        adapter,
        inbox,
        event_rx,
        facts: Some(facts),
    }
}

/// 登记任务并按序入队输入。
async fn make_task(h: &Harness, dedup: &str, inputs: &[&str]) -> ExecTask {
    let (task, created) = h
        .store
        .create(&CreateExecTask {
            channel_name: "test".into(),
            provider: ExecProvider::Kimi,
            goal: format!("goal-{dedup}"),
            working_dir: None,
            created_by: "ou_t".into(),
            source: ExecTaskSource::Skill,
            dedup_key: dedup.into(),
        })
        .await
        .unwrap();
    assert!(created);
    for (i, text) in inputs.iter().enumerate() {
        let outcome = h
            .inbox
            .accept(&task.id, format!("{dedup}-m{i}"), "ou_t", *text, vec![]);
        assert!(matches!(outcome, AcceptOutcome::Accepted { .. }));
    }
    task
}

fn queue_texts(h: &Harness, task_id: &ExecTaskId) -> Vec<String> {
    h.inbox
        .snapshot(task_id)
        .iter()
        .map(|i| i.text.clone())
        .collect()
}

fn started_texts(h: &Harness) -> Vec<String> {
    h.adapter
        .started_runs()
        .iter()
        .map(|(_, i)| i.text.clone())
        .collect()
}

fn drain_events(rx: &mut broadcast::Receiver<ExecEvent>) -> Vec<ExecEvent> {
    let mut out = Vec::new();
    while let Ok(e) = rx.try_recv() {
        out.push(e);
    }
    out
}

// ── 场景 1：A/B/C/D 全场景 ─────────────────────────────────────

#[tokio::test]
async fn abcd_stop_pause_resume_fifo_and_no_retry_of_stopped() {
    let mut h = harness(SimAdapter::default()).await;
    let task = make_task(&h, "s1", &["A", "B", "C"]).await;

    // A 开跑（首次派发完成绑定，N2），B/C 排队。
    assert!(h.sched.try_dispatch(&task.id).await.unwrap());
    let native = h.adapter.created_sessions()[0].clone();
    let run_a = h.sched.snapshot(&task.id).current.expect("A dispatched");
    assert_eq!(run_a.status, RunStatus::Running);
    assert_eq!(run_a.text, "A");
    assert_eq!(queue_texts(&h, &task.id), vec!["B", "C"]);

    // 停止并暂停（expected 匹配）→ A Stopping、cancel 已发、paused。
    let outcome = h
        .sched
        .stop_and_pause(&task.id, Some(run_a.run_id.clone()))
        .await;
    let StopOutcome::Accepted { run } = outcome else {
        panic!("expected Accepted, got {outcome:?}")
    };
    assert_eq!(run.status, RunStatus::Stopping, "stopping != stopped (C6)");
    assert_eq!(h.adapter.cancelled_sessions(), vec![native.clone()]);
    assert!(h.sched.snapshot(&task.id).paused);

    // 取消确认 → A=Stopped（D3：不回队）；仍 paused；B/C 有序保留。
    h.sched.terminal(&native, TerminalKind::Cancelled).await;
    let snap = h.sched.snapshot(&task.id);
    assert!(snap.paused);
    assert_eq!(snap.current.unwrap().status, RunStatus::Stopped);
    assert_eq!(queue_texts(&h, &task.id), vec!["B", "C"]);
    assert_eq!(
        h.adapter.started_runs().len(),
        1,
        "paused: no auto dispatch"
    );

    // 暂停中收 D → 排尾，不派发。
    h.inbox.accept(&task.id, "s1-m3", "ou_t", "D", vec![]);
    assert!(!h.sched.try_dispatch(&task.id).await.unwrap());
    assert_eq!(queue_texts(&h, &task.id), vec!["B", "C", "D"]);
    assert_eq!(h.adapter.started_runs().len(), 1);

    // resume → B→C→D 依次单 writer 执行；已停止的 A 不重试（D3）。
    assert_eq!(
        h.sched.resume(&task.id).await,
        ResumeOutcome::Resumed { dispatched: true }
    );
    assert_eq!(started_texts(&h), vec!["A", "B"]);
    assert_eq!(
        h.sched.snapshot(&task.id).current.unwrap().status,
        RunStatus::Running
    );

    h.sched.terminal(&native, TerminalKind::Completed).await; // B 完成 → 自动派 C
    assert_eq!(started_texts(&h), vec!["A", "B", "C"]);
    h.sched.terminal(&native, TerminalKind::Completed).await; // C 完成 → 自动派 D
    assert_eq!(started_texts(&h), vec!["A", "B", "C", "D"]);
    h.sched.terminal(&native, TerminalKind::Completed).await; // D 完成 → 队列空
    let snap = h.sched.snapshot(&task.id);
    assert_eq!(snap.current.unwrap().status, RunStatus::Completed);
    assert_eq!(snap.queued, 0);
    assert_eq!(
        started_texts(&h).iter().filter(|t| *t == "A").count(),
        1,
        "stopped A never retried"
    );

    // 事件提示（hint）流水：4 次开始、4 次终态、暂停/恢复各一。
    let events = drain_events(&mut h.event_rx);
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, ExecEvent::RunStarted { .. }))
            .count(),
        4,
        "{events:?}"
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, ExecEvent::RunTerminal { .. }))
            .count(),
        4,
        "{events:?}"
    );
    assert!(events.contains(&ExecEvent::Paused {
        task_id: task.id.clone()
    }));
    assert!(events.contains(&ExecEvent::Resumed {
        task_id: task.id.clone()
    }));
}

// ── 场景 2：交接竞态 ───────────────────────────────────────────

#[tokio::test]
async fn stop_racing_natural_completion_keeps_real_terminal_and_pause() {
    let h = harness(SimAdapter::default()).await;
    let task = make_task(&h, "s2", &["A", "B"]).await;
    assert!(h.sched.try_dispatch(&task.id).await.unwrap());
    let native = h.adapter.created_sessions()[0].clone();
    let run_a = h.sched.snapshot(&task.id).current.unwrap();

    // 停止先受理（Stopping、paused），自然终态随后到达 → 保存真实
    // 终态 Completed（N6 竞态如实），队列保持暂停，B 不启动。
    let outcome = h
        .sched
        .stop_and_pause(&task.id, Some(run_a.run_id.clone()))
        .await;
    assert!(matches!(outcome, StopOutcome::Accepted { .. }));
    h.sched.terminal(&native, TerminalKind::Completed).await;

    let snap = h.sched.snapshot(&task.id);
    assert_eq!(
        snap.current.unwrap().status,
        RunStatus::Completed,
        "real natural terminal preserved"
    );
    assert!(snap.paused, "queue stays paused");
    assert_eq!(queue_texts(&h, &task.id), vec!["B"]);
    assert_eq!(h.adapter.started_runs().len(), 1, "B must not start");
}

// ── 场景 3：旧按钮 ─────────────────────────────────────────────

#[tokio::test]
async fn stale_stop_button_mismatches_and_leaves_new_run_untouched() {
    let h = harness(SimAdapter::default()).await;
    let task = make_task(&h, "s3", &["A", "B"]).await;
    assert!(h.sched.try_dispatch(&task.id).await.unwrap());
    let native = h.adapter.created_sessions()[0].clone();
    let run_a = h.sched.snapshot(&task.id).current.unwrap();

    // A 自然完成 → 未暂停 → 自动派 B（B 开跑）。
    h.sched.terminal(&native, TerminalKind::Completed).await;
    let run_b = h.sched.snapshot(&task.id).current.unwrap();
    assert_eq!(run_b.text, "B");
    assert_eq!(run_b.status, RunStatus::Running);

    // 旧按钮带 A 的 expected 到达 → RunMismatch（旧请求不得套到
    // 新 Run，C6/R7）：B 不受扰（未取消、仍 Running）。
    let outcome = h
        .sched
        .stop_and_pause(&task.id, Some(run_a.run_id.clone()))
        .await;
    assert_eq!(
        outcome,
        StopOutcome::RunMismatch {
            actual_run: run_b.run_id.clone(),
            actual_status: RunStatus::Running,
        }
    );
    assert!(
        h.adapter.cancelled_sessions().is_empty(),
        "no cancel issued for the stale button"
    );
    let snap = h.sched.snapshot(&task.id);
    assert_eq!(
        snap.current.unwrap().status,
        RunStatus::Running,
        "B undisturbed"
    );
    assert!(snap.paused, "stop semantics took effect before the check");
}

// ── 场景 4：慢停止 ─────────────────────────────────────────────

#[tokio::test]
async fn slow_stop_unconfirmed_blocks_resume_until_terminal_then_explicit_resume() {
    // 超时 0s：第一次 sweep 即满足超时判据（确定性，不等挂钟）。
    let mut h = harness_with(2, 0, SimAdapter::default(), None).await;
    let task = make_task(&h, "s4", &["A", "B"]).await;
    assert!(h.sched.try_dispatch(&task.id).await.unwrap());
    let native = h.adapter.created_sessions()[0].clone();
    let run_a = h.sched.snapshot(&task.id).current.unwrap();

    // cancel_never_confirms（Sim 默认挂起）：受理后终态不到达 →
    // sweep 标记 StopUnconfirmed，保留 Stopping（不 detach 续跑）。
    h.sched
        .stop_and_pause(&task.id, Some(run_a.run_id.clone()))
        .await;
    h.sched.sweep_unconfirmed();
    let events = drain_events(&mut h.event_rx);
    assert!(
        events.contains(&ExecEvent::StopUnconfirmed {
            task_id: task.id.clone(),
            run_id: run_a.run_id.clone(),
        }),
        "{events:?}"
    );

    // 停止未确认 → resume 受阻、保持暂停、无新派发（N6 候选 1）。
    assert_eq!(
        h.sched.resume(&task.id).await,
        ResumeOutcome::BlockedStopUnconfirmed
    );
    let snap = h.sched.snapshot(&task.id);
    assert!(snap.paused);
    assert_eq!(
        snap.current.unwrap().status,
        RunStatus::Stopping,
        "kept Stopping, never detached"
    );
    assert_eq!(
        h.adapter.started_runs().len(),
        1,
        "no dispatch while stop unconfirmed"
    );

    // 迟到的 Cancelled 到达 → Stopped；仍 paused，直到显式 resume
    // （后台收到停止确认本身不解除暂停）。
    h.sched.terminal(&native, TerminalKind::Cancelled).await;
    let snap = h.sched.snapshot(&task.id);
    assert_eq!(snap.current.unwrap().status, RunStatus::Stopped);
    assert!(snap.paused, "terminal alone never unpauses");
    assert_eq!(h.adapter.started_runs().len(), 1);

    assert_eq!(
        h.sched.resume(&task.id).await,
        ResumeOutcome::Resumed { dispatched: true }
    );
    assert_eq!(started_texts(&h), vec!["A", "B"]);
}

// ── 场景 5：业务失败不自动暂停 / start 失败阻断 ────────────────

#[tokio::test]
async fn failed_terminal_dispatches_next_but_start_failure_blocks_lane() {
    let h = harness(SimAdapter::default()).await;

    // 业务失败不自动暂停（N3）：Failed 终态且未暂停 → 自动派下一项。
    let t1 = make_task(&h, "s5a", &["A", "B"]).await;
    assert!(h.sched.try_dispatch(&t1.id).await.unwrap());
    let native1 = h.adapter.created_sessions()[0].clone();
    h.sched.terminal(&native1, TerminalKind::Failed).await;
    let snap = h.sched.snapshot(&t1.id);
    assert!(!snap.paused, "business failure never auto-pauses");
    let cur = snap.current.unwrap();
    assert_eq!(cur.text, "B");
    assert_eq!(cur.status, RunStatus::Running);

    // start 失败 → 派发确认丢失（C4 可能已发送）：current=Unknown、
    // blocked_unknown、输入不 pop 不跳过（N3 未知不当普通失败）。
    let t2 = make_task(&h, "s5b", &["X", "Y"]).await;
    h.adapter.fail_next_start();
    assert!(!h.sched.try_dispatch(&t2.id).await.unwrap());
    let snap = h.sched.snapshot(&t2.id);
    assert!(snap.blocked_unknown);
    assert_eq!(snap.current.unwrap().status, RunStatus::Unknown);
    assert_eq!(
        queue_texts(&h, &t2.id),
        vec!["X", "Y"],
        "failed input kept at queue front"
    );
    // 阻断后不自动重试、不越过 X 派 Y。
    assert!(!h.sched.try_dispatch(&t2.id).await.unwrap());
    assert_eq!(
        h.adapter.started_runs().len(),
        2,
        "no silent retry, no skipping X"
    );
}

// ── 场景 6：按卡隔离 + 名额统一分配 ────────────────────────────

#[tokio::test]
async fn per_card_isolation_and_global_slot_fairness() {
    // 按卡隔离（D11）：X 停止暂停不影响 Y。
    let h = harness(SimAdapter::default()).await; // 2 名额
    let x = make_task(&h, "s6x", &["x1", "x2"]).await;
    let y = make_task(&h, "s6y", &["y1"]).await;
    assert!(h.sched.try_dispatch(&x.id).await.unwrap());
    assert!(h.sched.try_dispatch(&y.id).await.unwrap());
    let native_x = h.adapter.created_sessions()[0].clone();
    let run_x = h.sched.snapshot(&x.id).current.unwrap();

    h.sched
        .stop_and_pause(&x.id, Some(run_x.run_id.clone()))
        .await;
    h.sched.terminal(&native_x, TerminalKind::Cancelled).await;
    let snap_x = h.sched.snapshot(&x.id);
    let snap_y = h.sched.snapshot(&y.id);
    assert!(snap_x.paused);
    assert_eq!(snap_x.current.unwrap().status, RunStatus::Stopped);
    assert!(!snap_y.paused, "Y untouched by X's stop");
    assert_eq!(snap_y.current.unwrap().status, RunStatus::Running);

    // 名额统一分配（N3）：max=1 时 X 跑 Y 等，X 终态 Y 起。
    let h = harness_with(1, 30, SimAdapter::default(), None).await;
    let x = make_task(&h, "s6x2", &["x1"]).await;
    let y = make_task(&h, "s6y2", &["y1"]).await;
    assert!(h.sched.try_dispatch(&x.id).await.unwrap());
    assert!(
        !h.sched.try_dispatch(&y.id).await.unwrap(),
        "slots full: Y waits"
    );
    assert_eq!(
        h.adapter.created_sessions().len(),
        1,
        "no session created before a slot"
    );
    let native_x = h.adapter.created_sessions()[0].clone();
    h.sched.terminal(&native_x, TerminalKind::Completed).await;
    assert_eq!(
        h.adapter.started_runs().len(),
        2,
        "X terminal frees the slot → Y starts"
    );
    assert_eq!(
        h.sched.snapshot(&y.id).current.unwrap().status,
        RunStatus::Running
    );
    assert_eq!(
        h.sched.snapshot(&x.id).current.unwrap().status,
        RunStatus::Completed
    );
}

// ── 场景 7：resume 幂等 ────────────────────────────────────────

#[tokio::test]
async fn resume_is_idempotent_and_empty_queue_calls_no_adapter() {
    let h = harness(SimAdapter::default()).await;
    let t = make_task(&h, "s7", &["B"]).await;
    // 无在飞 Run 的停止 → NoCurrentRun，但暂停生效。
    let outcome = h.sched.stop_and_pause(&t.id, None).await;
    assert_eq!(outcome, StopOutcome::NoCurrentRun { paused: true });

    // 连点两次 resume：只派一个（重复操作不重复启动，C6）。
    assert_eq!(
        h.sched.resume(&t.id).await,
        ResumeOutcome::Resumed { dispatched: true }
    );
    assert_eq!(
        h.sched.resume(&t.id).await,
        ResumeOutcome::Resumed { dispatched: false }
    );
    assert_eq!(h.adapter.started_runs().len(), 1);

    // 空队列 resume → adapter 零调用。
    let t2 = make_task(&h, "s7b", &[]).await;
    assert_eq!(
        h.sched.resume(&t2.id).await,
        ResumeOutcome::Resumed { dispatched: false }
    );
    assert_eq!(
        h.adapter.created_sessions().len(),
        1,
        "empty queue: zero adapter calls"
    );
    assert_eq!(h.adapter.started_runs().len(), 1);
}

// ── 场景 8：绑定时机 + 冲突/损坏阻断 ───────────────────────────

/// 绑定冲突注入（模拟 create→bind 窗口内的并发双绑，N2）：get 如
/// 实返回 Uninitialized，`bind_provider_session` 永远报「已绑定到
/// 不同原生身份」。
struct ConflictingBindStore {
    inner: SqliteExecTaskStore,
}

#[async_trait::async_trait]
impl ExecTaskStore for ConflictingBindStore {
    async fn create(&self, input: &CreateExecTask) -> Result<(ExecTask, bool)> {
        self.inner.create(input).await
    }
    async fn get(&self, id: &ExecTaskId) -> Result<Option<ExecTask>> {
        self.inner.get(id).await
    }
    async fn find_by_dedup(&self, channel_name: &str, dedup_key: &str) -> Result<Option<ExecTask>> {
        self.inner.find_by_dedup(channel_name, dedup_key).await
    }
    async fn find_by_thread_root(
        &self,
        channel_name: &str,
        root_msg_id: &str,
    ) -> Result<Option<ExecTask>> {
        self.inner
            .find_by_thread_root(channel_name, root_msg_id)
            .await
    }
    async fn bind_provider_session(
        &self,
        id: &ExecTaskId,
        _native_session_id: &str,
    ) -> Result<ExecTask> {
        Err(KernelError::task(format!(
            "exec task {id} already bound to a different provider session; refusing to rebind"
        )))
    }
    async fn mark_broken(&self, id: &ExecTaskId, reason: &str) -> Result<ExecTask> {
        self.inner.mark_broken(id, reason).await
    }
    async fn set_thread_and_card(
        &self,
        id: &ExecTaskId,
        thread_root_msg_id: &str,
        card_msg_id: &str,
    ) -> Result<ExecTask> {
        self.inner
            .set_thread_and_card(id, thread_root_msg_id, card_msg_id)
            .await
    }
    async fn archive(&self, id: &ExecTaskId) -> Result<ExecTask> {
        self.inner.archive(id).await
    }
}

#[tokio::test]
async fn binding_happens_on_first_dispatch_and_conflict_blocks_without_rebind() {
    let h = harness(SimAdapter::default()).await;
    let t = make_task(&h, "s8", &["A", "B"]).await;

    // Uninitialized：受理不建原生身份，首次派发才 create_session +
    // bind（N2 首次派发前完成绑定）。
    assert!(h.adapter.created_sessions().is_empty());
    assert!(h.sched.try_dispatch(&t.id).await.unwrap());
    assert_eq!(h.adapter.created_sessions().len(), 1);
    let native = h.adapter.created_sessions()[0].clone();
    let bound = h.store.get(&t.id).await.unwrap().unwrap();
    assert_eq!(bound.binding, BindingState::Bound);
    assert_eq!(bound.provider_session_id.as_deref(), Some(native.as_str()));

    // 已 Bound：下一轮直接用原 id，不再 create_session。
    h.sched.terminal(&native, TerminalKind::Completed).await;
    assert_eq!(h.adapter.started_runs().len(), 2, "B dispatched");
    assert_eq!(
        h.adapter.created_sessions().len(),
        1,
        "bound identity reused"
    );

    // bind 冲突（不同 id）→ blocked_unknown、不重建、不跳过输入。
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    run_migrations(&pool).await.unwrap();
    let conflict_store: Arc<dyn ExecTaskStore> = Arc::new(ConflictingBindStore {
        inner: SqliteExecTaskStore::new(pool),
    });
    let h2 = harness_with(2, 30, SimAdapter::default(), Some(conflict_store)).await;
    let t2 = make_task(&h2, "s8c", &["X"]).await;
    assert!(!h2.sched.try_dispatch(&t2.id).await.unwrap());
    assert_eq!(
        h2.adapter.created_sessions().len(),
        1,
        "one create attempt, no auto-recreate"
    );
    let snap = h2.sched.snapshot(&t2.id);
    assert!(snap.blocked_unknown);
    assert_eq!(
        snap.current, None,
        "pre-dispatch failure: no native fact to show"
    );
    assert_eq!(queue_texts(&h2, &t2.id), vec!["X"], "input not skipped");
    assert!(
        !h2.sched.try_dispatch(&t2.id).await.unwrap(),
        "blocked lane never retries"
    );
    assert_eq!(h2.adapter.created_sessions().len(), 1);

    // Broken → 同阻断，不自动重建（N2/D6）。
    let t3 = make_task(&h, "s8b", &["Z"]).await;
    h.store
        .bind_provider_session(&t3.id, "native-manual")
        .await
        .unwrap();
    h.store.mark_broken(&t3.id, "test").await.unwrap();
    assert!(!h.sched.try_dispatch(&t3.id).await.unwrap());
    assert!(h.sched.snapshot(&t3.id).blocked_unknown);
    assert_eq!(
        h.adapter.created_sessions().len(),
        1,
        "broken binding: no recreate"
    );
}

// ── 附：sink 泵通路（complete_after / cancel 自动确认）─────────

#[tokio::test]
async fn sink_reports_reach_the_scheduler_via_the_pump() {
    let (sink, rx) = ExecAdapterSink::channel();
    let adapter = SimAdapter::default()
        .with_sink(sink)
        .with_cancel_never_confirms(false)
        .with_complete_after(Duration::from_millis(10));
    let h = harness(adapter).await;
    let cancel = CancellationToken::new();
    tokio::spawn(h.sched.clone().terminal_pump(rx, cancel.clone()));

    // complete_after 到点经 sink 报 Completed → 泵转 terminal →
    // 自动派下一项（B 开跑）。
    let t = make_task(&h, "sink", &["A", "B"]).await;
    assert!(h.sched.try_dispatch(&t.id).await.unwrap());
    wait_until("B dispatched via the pump", || {
        let snap = h.sched.snapshot(&t.id);
        snap.current
            .as_ref()
            .is_some_and(|r| r.text == "B" && r.status == RunStatus::Running)
    })
    .await;

    // cancel 自动确认（cancel_never_confirms=false）：停止 B →
    // Cancelled 经 sink 到达 → Stopped、保持暂停。
    let run_b = h.sched.snapshot(&t.id).current.unwrap();
    h.sched
        .stop_and_pause(&t.id, Some(run_b.run_id.clone()))
        .await;
    wait_until("B stop confirmed via the pump", || {
        h.sched
            .snapshot(&t.id)
            .current
            .as_ref()
            .is_some_and(|r| r.status == RunStatus::Stopped)
    })
    .await;
    assert!(h.sched.snapshot(&t.id).paused);

    cancel.cancel();
}

/// 轮询等待（仅 sink 泵通路用；5s 上限，10ms 步进）。
async fn wait_until(desc: &str, mut pred: impl FnMut() -> bool) {
    for _ in 0..500 {
        if pred() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {desc}");
}

// ── 增量 5 场景 1：事实流（N7/N9）──────────────────────────────

#[tokio::test]
async fn facts_flow_terminal_one_way_result_insert_once_and_published_event() {
    let mut h = harness_facts(SimAdapter::default()).await;
    let facts = h.facts.clone().unwrap();
    let task = make_task(&h, "f1", &["A"]).await;
    assert!(h.sched.try_dispatch(&task.id).await.unwrap());
    let native = h.adapter.created_sessions()[0].clone();
    let run_a = h.sched.snapshot(&task.id).current.unwrap();

    // accept→dispatch：run_started 行在（Running 提交后写入）。
    let runs = facts.runs_for(&task.id).await.unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].run_id, run_a.run_id);
    assert_eq!(runs[0].input_seq, 1);
    assert_eq!(runs[0].text, "A");
    assert_eq!(runs[0].status, "running");
    assert!(runs[0].terminal_kind.is_none());
    assert!(runs[0].ended_at.is_none());

    // publish_result → 正文保存 + ResultPublished 事件（先保存再
    // 公布——事件即公布，relay 据此刷新卡面结果行）。
    h.sched.result_reported(&native, "A 轮权威正文").await;
    let saved = facts.result_for(&run_a.run_id).await.unwrap().unwrap();
    assert_eq!(saved.body, "A 轮权威正文");
    assert_eq!(saved.body_bytes, "A 轮权威正文".len() as u64);
    assert_eq!(saved.input_seq, 1);
    let events = drain_events(&mut h.event_rx);
    assert!(
        events.contains(&ExecEvent::ResultPublished {
            task_id: task.id.clone(),
            run_id: run_a.run_id.clone(),
        }),
        "{events:?}"
    );

    // 重复上报不覆盖权威正文、不重发事件（insert-once，N9）。
    h.sched.result_reported(&native, "被篡改的正文").await;
    let saved = facts.result_for(&run_a.run_id).await.unwrap().unwrap();
    assert_eq!(saved.body, "A 轮权威正文", "duplicate never overwrites");
    assert!(
        drain_events(&mut h.event_rx)
            .iter()
            .all(|e| !matches!(e, ExecEvent::ResultPublished { .. })),
        "duplicate never re-publishes"
    );

    // 停止暂停（阻断终态后重派，隔离后续 terminal 断言）→ 取消
    // 确认 → 终态行单向写入。
    let outcome = h
        .sched
        .stop_and_pause(&task.id, Some(run_a.run_id.clone()))
        .await;
    assert!(matches!(outcome, StopOutcome::Accepted { .. }));
    h.sched.terminal(&native, TerminalKind::Cancelled).await;
    let runs = facts.runs_for(&task.id).await.unwrap();
    assert_eq!(runs[0].status, "stopped");
    assert_eq!(runs[0].terminal_kind.as_deref(), Some("cancelled"));
    assert!(runs[0].ended_at.is_some());
    let ended_at = runs[0].ended_at;

    // 重复 terminal（旧事件）：lane 已非在飞 → 忽略；终态行不改
    // kind（单向——旧事件不得覆盖新事实）。
    h.sched.terminal(&native, TerminalKind::Completed).await;
    let runs = facts.runs_for(&task.id).await.unwrap();
    assert_eq!(runs[0].terminal_kind.as_deref(), Some("cancelled"));
    assert_eq!(runs[0].status, "stopped");
    assert_eq!(runs[0].ended_at, ended_at, "terminal fact is one-way");
}

// ── 增量 5 场景 2：隔离（迟到结果不污染新 Run）─────────────────

#[tokio::test]
async fn late_result_belongs_to_its_own_run_and_never_pollutes_current() {
    let mut h = harness_facts(SimAdapter::default()).await;
    let facts = h.facts.clone().unwrap();
    let task = make_task(&h, "f2", &["A", "B"]).await;
    assert!(h.sched.try_dispatch(&task.id).await.unwrap());
    let native = h.adapter.created_sessions()[0].clone();
    let run_a = h.sched.snapshot(&task.id).current.unwrap();

    // A 终态但正文迟到 → 未暂停，B 自动开跑（current 已是 B）。
    h.sched.terminal(&native, TerminalKind::Completed).await;
    let run_b = h.sched.snapshot(&task.id).current.unwrap();
    assert_eq!(run_b.text, "B");
    assert_eq!(run_b.status, RunStatus::Running);

    // 迟到的 A 正文到达：归 A（最早无权威正文的 Run），不污染
    // current B（N9——迟到结果绝不猜最新轮）。
    h.sched.result_reported(&native, "A 的迟到正文").await;
    assert_eq!(
        facts.result_for(&run_a.run_id).await.unwrap().unwrap().body,
        "A 的迟到正文"
    );
    assert!(
        facts.result_for(&run_b.run_id).await.unwrap().is_none(),
        "late result must not pollute the current run"
    );
    let events = drain_events(&mut h.event_rx);
    assert!(
        events.contains(&ExecEvent::ResultPublished {
            task_id: task.id.clone(),
            run_id: run_a.run_id.clone(),
        }),
        "published for run A: {events:?}"
    );

    // B 正文到达 → 归 B；两轮各自独立。
    h.sched.result_reported(&native, "B 的正文").await;
    assert_eq!(
        facts.result_for(&run_b.run_id).await.unwrap().unwrap().body,
        "B 的正文"
    );
    assert_eq!(
        facts.result_for(&run_a.run_id).await.unwrap().unwrap().body,
        "A 的迟到正文",
        "bodies stay independent per run"
    );

    // 全部轮次已有正文后再到 → 迟到重报，忽略不覆盖。
    h.sched.result_reported(&native, "多余的重报").await;
    assert_eq!(
        facts.result_for(&run_a.run_id).await.unwrap().unwrap().body,
        "A 的迟到正文"
    );
    assert_eq!(
        facts.result_for(&run_b.run_id).await.unwrap().unwrap().body,
        "B 的正文"
    );

    // 未知 native：warn 忽略，零写入（绝不归属最新轮）。
    h.sched.result_reported("native-ghost", "幽灵正文").await;
    let runs = facts.runs_for(&task.id).await.unwrap();
    assert_eq!(runs.len(), 2);
    assert_eq!(
        facts.result_for(&run_b.run_id).await.unwrap().unwrap().body,
        "B 的正文"
    );
}
