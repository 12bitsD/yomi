//! ACP 契约套件（chat-flow 增量 7 组件 3）：真实 `kimi acp` + 真
//! 实 `ExecScheduler`（`SqliteExecFactStore` 内存库 + `terminal_pump`）
//! 驱动四场景——证明调度/绑定/事实/取消/恢复语义对真实双向协议
//! 成立（缺下游验证手段，性质见 `acp_harness` 模块文档）。
//!
//! 全部 `#[ignore]` + env 门 `YOMI_ACP_E2E=1`：无 env 或无 kimi
//! 二进制 → 打印原因并返回（跳过而非失败）。每场景硬超时 180s，
//! 超时即失败并清理进程。模型调用保持极小（reply-exactly 型）。

use super::*;
use crate::exec::adapter::ExecAdapterSink;
use crate::exec::{
    AcceptOutcome, AnswerOutcome, BindingState, CreateExecTask, ExecEvent, ExecFactStore,
    ExecOptionKind, ExecProvider, ExecRequestKind, ExecRequestStatus, ExecScheduler, ExecTask,
    ExecTaskSource, ExecTaskStore, RequestOutcome, ResumeOutcome, RunStatus, SqliteExecFactStore,
    SqliteExecTaskStore, StopOutcome,
};
use crate::storage::migrations::run_migrations;
use crate::types::{ExecRequestId, RunId};
use sqlx::sqlite::SqlitePoolOptions;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

/// 场景硬超时（spec：超时即失败并留进程清理）。
const SCENARIO_TIMEOUT: Duration = Duration::from_secs(180);
/// 单次状态/事实轮询上限。
const WAIT_TIMEOUT: Duration = Duration::from_secs(60);

/// 统一门：无 env 或无 kimi 二进制 → 打印原因并跳过（不失败）。
fn e2e_enabled() -> bool {
    if std::env::var("YOMI_ACP_E2E").ok().as_deref() != Some("1") {
        eprintln!("acp_harness e2e: YOMI_ACP_E2E!=1，跳过（设 1 后以 --ignored 运行）");
        return false;
    }
    match std::process::Command::new("kimi").arg("--version").output() {
        Ok(out) if out.status.success() => true,
        _ => {
            eprintln!("acp_harness e2e: PATH 无可用 kimi 二进制，跳过");
            false
        }
    }
}

/// 契约套件测试台：真实调度器 + 内存 sqlite（任务/事实同库，镜
/// 像生产装配）+ ACP adapter + sink 泵。
struct Rig {
    sched: Arc<ExecScheduler>,
    store: Arc<dyn ExecTaskStore>,
    adapter: Arc<AcpHarnessAdapter>,
    inbox: crate::exec::ExecInbox,
    event_rx: broadcast::Receiver<ExecEvent>,
    facts: Arc<SqliteExecFactStore>,
    pump_cancel: CancellationToken,
}

impl Rig {
    async fn new() -> Self {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        run_migrations(&pool).await.unwrap();
        let store: Arc<dyn ExecTaskStore> = Arc::new(SqliteExecTaskStore::new(pool.clone()));
        let facts = Arc::new(SqliteExecFactStore::new(pool));
        let inbox = crate::exec::ExecInbox::new();
        let (event_tx, event_rx) = broadcast::channel(256);
        let (sink, sink_rx) = ExecAdapterSink::channel();
        let adapter = Arc::new(AcpHarnessAdapter::new(sink));
        let sched = Arc::new(
            ExecScheduler::new(
                Arc::clone(&store),
                adapter.clone(),
                inbox.clone(),
                event_tx,
                crate::config::ExecConfig {
                    max_concurrent_runs: 2,
                    stop_confirm_timeout_secs: 60,
                    // 套件内不触发空闲释放（场景 3 手动 release）。
                    idle_release_secs: 3600,
                    card_renew_margin_secs: 129_600,
                    card_renew_sweep_secs: 1800,
                },
            )
            .with_facts(facts.clone()),
        );
        let pump_cancel = CancellationToken::new();
        tokio::spawn(sched.clone().terminal_pump(sink_rx, pump_cancel.clone()));
        Self {
            sched,
            store,
            adapter,
            inbox,
            event_rx,
            facts,
            pump_cancel,
        }
    }

    /// 收尾：终止全部 ACP 进程 + 停泵（含超时路径——先清理再失败）。
    async fn teardown(&self) {
        self.adapter.shutdown_all().await;
        self.pump_cancel.cancel();
    }
}

/// 登记任务（Uninitialized；provider=kimi）。
async fn make_task(rig: &Rig, dedup: &str) -> ExecTask {
    let (task, created) = rig
        .store
        .create(&CreateExecTask {
            channel_name: "acp-e2e".into(),
            provider: ExecProvider::Kimi,
            goal: format!("acp-e2e-{dedup}"),
            working_dir: None,
            created_by: "ou_acp".into(),
            source: ExecTaskSource::Skill,
            dedup_key: dedup.into(),
        })
        .await
        .unwrap();
    assert!(created);
    task
}

/// 受理一条输入（镜像生产装配：受理凭据先于 inbox，C1/N12）。
async fn accept(rig: &Rig, task_id: &ExecTaskId, msg_id: &str, text: &str) {
    assert!(
        rig.facts
            .record_acceptance("acp-e2e", msg_id, task_id)
            .await
            .unwrap(),
        "acceptance must be recorded once"
    );
    let outcome = rig.inbox.accept(task_id, msg_id, "ou_acp", text, vec![]);
    assert!(matches!(outcome, AcceptOutcome::Accepted { .. }));
}

/// 取任务当前绑定的原生身份（断言 Bound 且非空）。
async fn bound_native(rig: &Rig, task_id: &ExecTaskId) -> String {
    let task = rig.store.get(task_id).await.unwrap().unwrap();
    assert_eq!(task.binding, BindingState::Bound, "bind must persist (N2)");
    task.provider_session_id.expect("native id persisted")
}

/// 同步谓词轮询（lane 快照等进程内状态；100ms 步进）。
async fn wait_until_sync(desc: &str, mut pred: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        if pred() {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "等待超时：{desc}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// 异步谓词轮询（事实库查询；谓词须自持数据——Arc clone 进闭包）。
async fn wait_until<F, Fut>(desc: &str, mut pred: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        if pred().await {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "等待超时：{desc}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// lane 当前 Run 状态谓词（`wait_until_sync` 适配）。
fn current_status_is<'a>(
    rig: &'a Rig,
    task_id: &'a ExecTaskId,
    status: RunStatus,
) -> impl FnMut() -> bool + 'a {
    move || {
        rig.sched
            .snapshot(task_id)
            .current
            .as_ref()
            .is_some_and(|r| r.status == status)
    }
}

fn drain_events(rx: &mut broadcast::Receiver<ExecEvent>) -> Vec<ExecEvent> {
    let mut out = Vec::new();
    while let Ok(e) = rx.try_recv() {
        out.push(e);
    }
    out
}

// ── 场景 1：创建→绑定→运行→事实 ───────────────────────────────

#[tokio::test]
#[ignore = "acp e2e：需 YOMI_ACP_E2E=1 与 kimi 二进制"]
async fn scenario_1_create_bind_run_and_facts() {
    if !e2e_enabled() {
        return;
    }
    let started = std::time::Instant::now();
    let mut rig = Rig::new().await;
    let result = tokio::time::timeout(SCENARIO_TIMEOUT, async {
        let task = make_task(&rig, "s1").await;
        accept(
            &rig,
            &task.id,
            "s1-m0",
            "Reply with exactly the single token: HARNESS_OK. Nothing else.",
        )
        .await;

        // Uninitialized → 首次派发完成绑定（N2：派发前绑定并持久化）。
        assert!(rig.sched.try_dispatch(&task.id).await.unwrap());
        let native = bound_native(&rig, &task.id).await;
        eprintln!("[s1] bound native session: {native}");

        // 运行 → 终态 Completed（真实 prompt 经 sink 泵收口）。
        wait_until_sync(
            "run completed",
            current_status_is(&rig, &task.id, RunStatus::Completed),
        )
        .await;

        // Run 事实：一行、单向终态 completed（事实写定在 lane 提交
        // 之后，轮询取齐）。
        let facts = rig.facts.clone();
        let task_id = task.id.clone();
        wait_until("run fact completed", move || {
            let facts = facts.clone();
            let task_id = task_id.clone();
            async move {
                facts.runs_for(&task_id).await.is_ok_and(|runs| {
                    runs.len() == 1 && runs[0].terminal_kind.as_deref() == Some("completed")
                })
            }
        })
        .await;
        let runs = rig.facts.runs_for(&task.id).await.unwrap();
        assert_eq!(runs[0].status, "completed");

        // 结果正文：已保存且归属该 Run；先保存再公布（ResultPublished）。
        let row = rig
            .facts
            .result_for(&runs[0].run_id)
            .await
            .unwrap()
            .expect("authoritative body saved");
        assert!(row.body.contains("HARNESS_OK"), "body: {}", row.body);
        assert_eq!(row.input_seq, 1);
        let events = drain_events(&mut rig.event_rx);
        assert!(
            events.contains(&ExecEvent::ResultPublished {
                task_id: task.id.clone(),
                run_id: runs[0].run_id.clone(),
            }),
            "ResultPublished missing: {events:?}"
        );

        // 受理凭据已标记开始（N12）。
        let acc = rig
            .facts
            .acceptance_for("acp-e2e", "s1-m0")
            .await
            .unwrap()
            .expect("acceptance row");
        assert!(acc.started, "acceptance marked started on dispatch");
        eprintln!(
            "[s1] terminal=completed, body saved ({} bytes): {:?}",
            row.body.len(),
            row.body
        );
    })
    .await;
    rig.teardown().await;
    assert!(result.is_ok(), "场景 1 超时（{SCENARIO_TIMEOUT:?}）");
    eprintln!("[s1] scenario done in {:?}", started.elapsed());
}

// ── 场景 2：真实取消 ──────────────────────────────────────────

#[tokio::test]
#[ignore = "acp e2e：需 YOMI_ACP_E2E=1 与 kimi 二进制"]
async fn scenario_2_real_cancel_then_resume() {
    if !e2e_enabled() {
        return;
    }
    let started = std::time::Instant::now();
    let mut rig = Rig::new().await;
    let result = tokio::time::timeout(SCENARIO_TIMEOUT, async {
        let task = make_task(&rig, "s2").await;
        accept(
            &rig,
            &task.id,
            "s2-m0",
            "Run this exact shell command: sleep 60 && echo hi. \
             You MUST actually execute it and wait for it to finish before replying.",
        )
        .await;
        accept(
            &rig,
            &task.id,
            "s2-m1",
            "Reply with exactly the single token: RESUMED_OK. Nothing else.",
        )
        .await;

        assert!(rig.sched.try_dispatch(&task.id).await.unwrap());
        let native = bound_native(&rig, &task.id).await;
        let run_a = rig.sched.snapshot(&task.id).current.expect("A dispatched");
        assert_eq!(run_a.status, RunStatus::Running);
        eprintln!("[s2] A running on {native}; stopping");

        // Running 后停止并暂停 → Stopping（受理≠已停止，C6）。
        let outcome = rig
            .sched
            .stop_and_pause(&task.id, Some(run_a.run_id.clone()))
            .await;
        let StopOutcome::Accepted { run } = outcome else {
            panic!("expected Accepted, got {outcome:?}")
        };
        assert_eq!(run.status, RunStatus::Stopping, "stopping != stopped");

        // session/cancel 通知 → prompt 以 cancelled 收口 → Stopped。
        wait_until_sync(
            "cancel confirmed",
            current_status_is(&rig, &task.id, RunStatus::Stopped),
        )
        .await;
        let snap = rig.sched.snapshot(&task.id);
        assert!(snap.paused, "暂停保持");
        let facts = rig.facts.clone();
        let task_id = task.id.clone();
        wait_until("run fact cancelled", move || {
            let facts = facts.clone();
            let task_id = task_id.clone();
            async move {
                facts.runs_for(&task_id).await.is_ok_and(|runs| {
                    runs.len() == 1 && runs[0].terminal_kind.as_deref() == Some("cancelled")
                })
            }
        })
        .await;
        eprintln!("[s2] cancel confirmed: run=Stopped, terminal_kind=cancelled");

        // resume：停止已确认 → 不出现 BlockedStopUnconfirmed → 实
        // 际恢复派发下一条（D3：已停止的 A 不回队）。
        assert_eq!(
            rig.sched.resume(&task.id).await,
            ResumeOutcome::Resumed { dispatched: true }
        );
        wait_until_sync(
            "second run completed",
            current_status_is(&rig, &task.id, RunStatus::Completed),
        )
        .await;
        let runs = rig.facts.runs_for(&task.id).await.unwrap();
        assert_eq!(runs.len(), 2);
        assert_eq!(runs[0].status, "stopped");
        assert_eq!(runs[1].status, "completed");
        // B 的正文确已到达并保存（先保存再公布）。归属注意：上报通
        // 道只带 native 身份，调度器归属「最早无权威正文的 Run」—
        // 被取消的 A 无正文，归属指针停在 A，B 的正文落在 A 行
        // （row.input_seq=1 即此现象）。已知限制：trait 的 run 身份
        // 回显扩展随首个生产 adapter 落地（inc-7 spec 明确不做，见
        // chat-flow-downstream-integration.md §1.2）；归属修复后本
        // 断言（按任务取最新正文，不钉死行）仍然成立。
        let row = rig
            .facts
            .latest_result(&task.id)
            .await
            .unwrap()
            .expect("resumed round body saved");
        assert!(row.body.contains("RESUMED_OK"), "body: {}", row.body);
        eprintln!(
            "[s2] resumed round body saved (attributed to input_seq={} — cancelled run has no body; known attribution caveat)",
            row.input_seq
        );

        // 同一原生 Session 承接两轮（恢复非新建；已停止轮不重试）。
        let started_runs = rig.adapter.started_texts();
        assert_eq!(started_runs.len(), 2, "{started_runs:?}");
        assert!(
            started_runs.iter().all(|(id, _)| *id == native),
            "both rounds on the same native session: {started_runs:?}"
        );
        let events = drain_events(&mut rig.event_rx);
        let a_starts = events
            .iter()
            .filter(|e| matches!(e, ExecEvent::RunStarted { run_id, .. } if *run_id == run_a.run_id))
            .count();
        assert_eq!(a_starts, 1, "A started exactly once (never retried)");
        eprintln!("[s2] resumed on same session; B body: {:?}", row.body);
    })
    .await;
    rig.teardown().await;
    assert!(result.is_ok(), "场景 2 超时（{SCENARIO_TIMEOUT:?}）");
    eprintln!("[s2] scenario done in {:?}", started.elapsed());
}

// ── 场景 3：释放→恢复原 Session ───────────────────────────────

#[tokio::test]
#[ignore = "acp e2e：需 YOMI_ACP_E2E=1 与 kimi 二进制"]
async fn scenario_3_release_and_resume_same_session() {
    if !e2e_enabled() {
        return;
    }
    let started = std::time::Instant::now();
    let mut rig = Rig::new().await;
    let result = tokio::time::timeout(SCENARIO_TIMEOUT, async {
        let code = format!("GIRAFFE-{:08X}", rand::random::<u32>());
        let task = make_task(&rig, "s3").await;
        accept(
            &rig,
            &task.id,
            "s3-m0",
            &format!("Remember this codeword: {code}. Reply with exactly: STORED. Nothing else."),
        )
        .await;

        // 轮 1：教暗号 → 完成。
        assert!(rig.sched.try_dispatch(&task.id).await.unwrap());
        let native = bound_native(&rig, &task.id).await;
        wait_until_sync(
            "round 1 completed",
            current_status_is(&rig, &task.id, RunStatus::Completed),
        )
        .await;
        eprintln!("[s3] round 1 done on {native}; codeword={code}");

        // 手动释放（等价空闲释放效果）：进程终止、句柄移出表；历
        // 史由 kimi 数据目录保留（C8）。
        rig.adapter.release(&native).await.unwrap();
        assert_eq!(rig.adapter.process_count(), 0, "process released");

        // 轮 2：句柄表无 → 新起进程 + session/load 恢复原 Session
        // （D9 证据：跨进程恢复），再 prompt。
        accept(
            &rig,
            &task.id,
            "s3-m1",
            "What was the codeword I asked you to remember? \
             Reply with just the codeword, nothing else.",
        )
        .await;
        assert!(rig.sched.try_dispatch(&task.id).await.unwrap());
        assert_eq!(
            rig.adapter.process_count(),
            1,
            "process respawned for resume"
        );
        assert_eq!(
            bound_native(&rig, &task.id).await,
            native,
            "same native id after resume (N2: never re-bind)"
        );

        wait_until_sync(
            "round 2 completed",
            current_status_is(&rig, &task.id, RunStatus::Completed),
        )
        .await;
        let runs = rig.facts.runs_for(&task.id).await.unwrap();
        assert_eq!(runs.len(), 2);
        let row = rig
            .facts
            .result_for(&runs[1].run_id)
            .await
            .unwrap()
            .expect("round 2 body saved");
        assert!(
            row.body.contains(&code),
            "round 2 must recall the codeword across processes.\ncode: {code}\nbody: {}",
            row.body
        );
        // 两轮同一原生身份；轮 2 经恢复路径（started 观察口两条）。
        let started_runs = rig.adapter.started_texts();
        assert_eq!(started_runs.len(), 2);
        assert!(started_runs.iter().all(|(id, _)| *id == native));
        let events = drain_events(&mut rig.event_rx);
        assert!(
            events
                .iter()
                .filter(|e| matches!(e, ExecEvent::ResultPublished { .. }))
                .count()
                == 2,
            "both rounds published results"
        );
        eprintln!(
            "[s3] round 2 recalled codeword across processes: {:?}",
            row.body
        );
    })
    .await;
    rig.teardown().await;
    assert!(result.is_ok(), "场景 3 超时（{SCENARIO_TIMEOUT:?}）");
    eprintln!("[s3] scenario done in {:?}", started.elapsed());
}

// ── 场景 4：两卡隔离 ──────────────────────────────────────────

#[tokio::test]
#[ignore = "acp e2e：需 YOMI_ACP_E2E=1 与 kimi 二进制"]
async fn scenario_4_two_cards_isolation() {
    if !e2e_enabled() {
        return;
    }
    let started = std::time::Instant::now();
    let mut rig = Rig::new().await;
    let result = tokio::time::timeout(SCENARIO_TIMEOUT, async {
        let x = make_task(&rig, "s4x").await;
        let y = make_task(&rig, "s4y").await;
        accept(
            &rig,
            &x.id,
            "s4x-m0",
            "Run this exact shell command: sleep 60 && echo x. \
             You MUST actually execute it and wait for it to finish before replying.",
        )
        .await;
        accept(
            &rig,
            &y.id,
            "s4y-m0",
            "Run this exact shell command: sleep 60 && echo y. \
             You MUST actually execute it and wait for it to finish before replying.",
        )
        .await;

        // 两卡同 provider 各跑长 prompt（2 名额，互不抢占）。
        assert!(rig.sched.try_dispatch(&x.id).await.unwrap());
        assert!(rig.sched.try_dispatch(&y.id).await.unwrap());
        let run_x = rig.sched.snapshot(&x.id).current.expect("X dispatched");
        let run_y = rig.sched.snapshot(&y.id).current.expect("Y dispatched");
        assert_eq!(run_x.status, RunStatus::Running);
        assert_eq!(run_y.status, RunStatus::Running);
        let native_x = bound_native(&rig, &x.id).await;
        let native_y = bound_native(&rig, &y.id).await;
        assert_ne!(native_x, native_y, "per-card native sessions (D11)");
        eprintln!("[s4] X={native_x} Y={native_y} both running; stopping X");

        // 停 X：session/cancel 只带 X 的 sessionId。
        let outcome = rig
            .sched
            .stop_and_pause(&x.id, Some(run_x.run_id.clone()))
            .await;
        assert!(matches!(outcome, StopOutcome::Accepted { .. }));
        wait_until_sync(
            "X stopped",
            current_status_is(&rig, &x.id, RunStatus::Stopped),
        )
        .await;
        assert!(rig.sched.snapshot(&x.id).paused, "X 暂停保持");

        // Y 未被误停：X 收口后 5s 仍 Running。
        tokio::time::sleep(Duration::from_secs(5)).await;
        let y_snap = rig.sched.snapshot(&y.id);
        assert_eq!(
            y_snap.current.as_ref().map(|r| r.status),
            Some(RunStatus::Running),
            "Y must survive X's cancel (per-card isolation)"
        );
        assert!(!y_snap.paused, "Y never paused");
        eprintln!("[s4] X stopped; Y still running 5s later (isolation holds)");

        // 清理：停 Y（留证据：两卡终态各自如实）。
        let outcome = rig
            .sched
            .stop_and_pause(&y.id, Some(run_y.run_id.clone()))
            .await;
        assert!(matches!(outcome, StopOutcome::Accepted { .. }));
        wait_until_sync(
            "Y stopped",
            current_status_is(&rig, &y.id, RunStatus::Stopped),
        )
        .await;
        let facts = rig.facts.clone();
        let (x_id, y_id) = (x.id.clone(), y.id.clone());
        wait_until("both facts cancelled", move || {
            let facts = facts.clone();
            let (x_id, y_id) = (x_id.clone(), y_id.clone());
            async move {
                let cancelled = |runs: Vec<crate::exec::ExecRunRow>| {
                    runs.len() == 1 && runs[0].terminal_kind.as_deref() == Some("cancelled")
                };
                facts.runs_for(&x_id).await.is_ok_and(cancelled)
                    && facts.runs_for(&y_id).await.is_ok_and(cancelled)
            }
        })
        .await;
        let events = drain_events(&mut rig.event_rx);
        assert!(
            events.contains(&ExecEvent::RunTerminal {
                task_id: x.id.clone(),
                run_id: run_x.run_id.clone(),
                kind: TerminalKind::Cancelled,
            }),
            "X terminal event"
        );
        eprintln!("[s4] Y stopped on its own cancel; both facts terminal_kind=cancelled");
    })
    .await;
    rig.teardown().await;
    assert!(result.is_ok(), "场景 4 超时（{SCENARIO_TIMEOUT:?}）");
    eprintln!("[s4] scenario done in {:?}", started.elapsed());
}

// ── 场景 5：真实授权请求的回答生命周期（N5/C5）──────────────────
//
// permission_mode 置 manual-approval 的择路记录（spec 组件 4）：
// 实测 kimi 2.1.0 ACP 有 `session/set_mode`，但 modeId 集合是
// default/plan/auto/yolo——**无 "ask"**；`default` 即 Manual
// approvals（session/new 的 configOptions 自述）。故采
// `session/set_mode {modeId:"default"}`（`enable_permission_ask`
// 在 session/new 后调用），不改全局 config、不开独立
// KIMI_CONFIG_HOME。

#[tokio::test]
#[ignore = "acp e2e：需 YOMI_ACP_E2E=1 与 kimi 二进制"]
async fn scenario_5_permission_request_answer_lifecycle() {
    if !e2e_enabled() {
        return;
    }
    let started = std::time::Instant::now();
    let mut rig = Rig::new().await;
    let result = tokio::time::timeout(SCENARIO_TIMEOUT, async {
        // 建 session 前开 ask：此后该进程 request_permission 转发
        // 调度器等人工回答（不自动应答）。
        rig.adapter.enable_permission_ask();
        let task = make_task(&rig, "s5").await;
        accept(
            &rig,
            &task.id,
            "s5-m0",
            "Run this exact shell command: echo PERM_S5_OK. \
             You MUST actually execute it with your shell tool before replying. \
             Then reply with exactly the single token: S5_DONE. Nothing else.",
        )
        .await;

        // ① prompt 触发 shell 工具 → 收到 Request notice、run 转
        // WaitingRequest、快照（卡面凭据）有待回应区。
        assert!(rig.sched.try_dispatch(&task.id).await.unwrap());
        let native = bound_native(&rig, &task.id).await;
        wait_until_sync(
            "run waiting for answer",
            current_status_is(&rig, &task.id, RunStatus::WaitingRequest),
        )
        .await;
        let snap = rig.sched.snapshot(&task.id);
        let run = snap.current.expect("run current");
        let req = snap
            .pending_request
            .expect("pending request in snapshot (卡面待回应区凭据)");
        assert_eq!(req.kind, ExecRequestKind::Permission);
        assert_eq!(req.status, ExecRequestStatus::Pending);
        assert!(
            req.options
                .iter()
                .any(|o| o.kind == ExecOptionKind::AllowOnce),
            "native allow option mapped: {:?}",
            req.options
        );
        assert!(
            req.options
                .iter()
                .any(|o| o.kind == ExecOptionKind::RejectOnce),
            "native reject option kept unbeautified: {:?}",
            req.options
        );
        let events = drain_events(&mut rig.event_rx);
        assert!(
            events.contains(&ExecEvent::RequestPending {
                task_id: task.id.clone(),
                run_id: run.run_id.clone(),
            }),
            "RequestPending missing: {events:?}"
        );
        eprintln!(
            "[s5] request pending on {native}: {} opts={:?}",
            req.prompt_text,
            req.options
                .iter()
                .map(|o| (&o.option_id, o.kind))
                .collect::<Vec<_>>()
        );

        // ② 错误 req / 错误 run 回答 → Mismatch，零状态变更。
        let outcome = || RequestOutcome::Selected {
            option_id: "approve_once".into(),
        };
        assert_eq!(
            rig.sched
                .answer_request(&task.id, &run.run_id, &ExecRequestId::new(), outcome())
                .await
                .unwrap(),
            AnswerOutcome::Mismatch,
            "wrong req → Mismatch"
        );
        assert_eq!(
            rig.sched
                .answer_request(&task.id, &RunId::new(), &req.request_id, outcome())
                .await
                .unwrap(),
            AnswerOutcome::Mismatch,
            "wrong run → Mismatch"
        );
        assert_eq!(
            rig.sched.snapshot(&task.id).pending_request.unwrap().status,
            ExecRequestStatus::Pending,
            "错配后请求仍待答"
        );

        // ③ 正确回答 allow_once → adapter 确认 Resolved → 退出等
        // 待态 → prompt 继续 → Terminal(Completed)、正文保存。
        let allow = req
            .options
            .iter()
            .find(|o| o.kind == ExecOptionKind::AllowOnce)
            .unwrap();
        assert_eq!(
            rig.sched
                .answer_request(
                    &task.id,
                    &run.run_id,
                    &req.request_id,
                    RequestOutcome::Selected {
                        option_id: allow.option_id.clone(),
                    },
                )
                .await
                .unwrap(),
            AnswerOutcome::Resolved,
            "adapter ack decides Resolved"
        );
        assert!(
            rig.sched.snapshot(&task.id).pending_request.is_none(),
            "Resolved 后待回应区消失"
        );
        assert_eq!(
            rig.sched.snapshot(&task.id).current.unwrap().status,
            RunStatus::Running,
            "无 Pending 剩余 → Provider 重新在执行"
        );
        wait_until_sync(
            "run completed after answer",
            current_status_is(&rig, &task.id, RunStatus::Completed),
        )
        .await;
        let runs = rig.facts.runs_for(&task.id).await.unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].terminal_kind.as_deref(), Some("completed"));
        let row = rig
            .facts
            .result_for(&runs[0].run_id)
            .await
            .unwrap()
            .expect("authoritative body saved");
        assert!(
            row.body.contains("S5_DONE") || row.body.contains("PERM_S5_OK"),
            "prompt continued after approval; body: {}",
            row.body
        );
        eprintln!(
            "[s5] approved → prompt completed, body saved: {:?}",
            row.body
        );

        // ④ 二次回答同 req → Already{Resolved}，不重复放行（C5：
        // 一次请求只一个最终回应）。
        assert_eq!(
            rig.sched
                .answer_request(&task.id, &run.run_id, &req.request_id, outcome())
                .await
                .unwrap(),
            AnswerOutcome::Already {
                status: ExecRequestStatus::Resolved,
            },
            "duplicate answer not re-delivered"
        );
    })
    .await;
    rig.teardown().await;
    assert!(result.is_ok(), "场景 5 超时（{SCENARIO_TIMEOUT:?}）");
    eprintln!("[s5] scenario done in {:?}", started.elapsed());
}
