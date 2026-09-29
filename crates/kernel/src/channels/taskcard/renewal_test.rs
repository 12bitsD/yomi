//! L1 换代测试（chat-flow 增量 8）：纯函数决策单测 + MockAdapter
//! 执行测试。规格场景映射：
//! 1. 活跃任务临期 → `RenewSoon` → 新卡发原 Thread、gen+1、旧
//!    gen 回调被拒 → `renew_posts_new_card_in_original_thread_and_
//!    switches_generation` / `old_generation_callback_rejected_after_
//!    renewal` / `sweep_renews_due_active_task_and_skips_idle_expired`；
//! 2. 空闲未过期 → Noop；空闲已过期 → 无自动换代，用户输入时
//!    RenewOnReturn 先换再受理 → `return_path_*` 两条与决策单测；
//! 3. 发送失败 → SendUncertain、映射不动、无第二张卡 →
//!    `send_failure_keeps_mapping_and_sends_no_second_card`；
//! 4. 换代前后 inbox/lane/binding/current Run 逐项相等 →
//!    `renewal_leaves_inbox_lane_binding_runs_untouched`。

use super::*;
use crate::channels::hub::ChannelInstance;
use crate::channels::{CardAction, ChannelConfig, ContentBlock, PlatformAdapter, PlatformConfig};
use crate::exec::run::RunStatus;
use crate::exec::{
    AcceptOutcome, BindingState, CreateExecTask, ExecProvider, ExecTaskSource, RunRecord,
};
use crate::types::RunId;
use chrono::Duration as CDuration;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;

// ── 纯函数决策单测（无 adapter/store）────────────────────────────

fn task_with_card(sent_at: Option<DateTime<Utc>>, created_at: DateTime<Utc>) -> ExecTask {
    ExecTask {
        id: ExecTaskId::new(),
        channel_name: "mock".into(),
        provider: ExecProvider::Kimi,
        status: ExecTaskStatus::Active,
        binding: BindingState::Bound,
        provider_session_id: Some("native-1".into()),
        thread_root_msg_id: Some("card-1".into()),
        card_msg_id: Some("card-1".into()),
        card_generation: 0,
        card_sent_at: sent_at,
        card_entity_created_at: None,
        goal: "g".into(),
        working_dir: None,
        created_by: "ou_t".into(),
        source: ExecTaskSource::Skill,
        dedup_key: "d".into(),
        created_at,
        updated_at: created_at,
    }
}

fn lane(current: Option<RunStatus>, queued: usize, paused: bool) -> LaneSnapshot {
    LaneSnapshot {
        paused,
        current: current.map(|status| RunRecord {
            run_id: RunId::new(),
            task_id: ExecTaskId::new(),
            input_seq: 1,
            text: "输入".into(),
            image_keys: vec![],
            status,
            started_at: Utc::now(),
            ended_at: None,
        }),
        queued,
        blocked_unknown: false,
        released: false,
    }
}

const MARGIN: Duration = Duration::from_secs(129_600); // 36h 默认

#[test]
fn decision_no_card_is_noop_even_when_expired_and_active() {
    let now = Utc::now();
    let mut task = task_with_card(Some(now - CDuration::days(30)), now - CDuration::days(30));
    task.card_msg_id = None; // CardPending 半完成态
    task.thread_root_msg_id = None;
    // 普通投递失败不擅自补卡（C9）——即使已过期且有活跃 Run。
    let d = renewal_decision(
        &task,
        &lane(Some(RunStatus::Running), 0, false),
        now,
        MARGIN,
    );
    assert_eq!(d, RenewalDecision::Noop);
}

#[test]
fn decision_archived_is_noop() {
    let now = Utc::now();
    let mut task = task_with_card(Some(now - CDuration::days(30)), now - CDuration::days(30));
    task.status = ExecTaskStatus::Archived;
    let d = renewal_decision(&task, &lane(None, 0, false), now, MARGIN);
    assert_eq!(d, RenewalDecision::Noop);
}

#[test]
fn decision_active_near_deadline_renews_soon() {
    let now = Utc::now();
    // 发卡 13 天前：距消息期限 1 天 < 36h 余量。
    let task = task_with_card(Some(now - CDuration::days(13)), now - CDuration::days(13));
    // Running / Stopping / paused+queued 三种活跃形态都换代。
    for snap in [
        lane(Some(RunStatus::Running), 0, false),
        lane(Some(RunStatus::Stopping), 0, true),
        lane(None, 2, true),
    ] {
        let d = renewal_decision(&task, &snap, now, MARGIN);
        assert_eq!(
            d,
            RenewalDecision::RenewSoon {
                reason: RenewReason::MessageDeadline
            },
            "snap {snap:?}"
        );
    }
    // 已过期（活跃任务不允许拖到过期才换——距离为负同样 < 余量）。
    let expired = task_with_card(Some(now - CDuration::days(15)), now - CDuration::days(15));
    let d = renewal_decision(
        &expired,
        &lane(Some(RunStatus::Running), 0, false),
        now,
        MARGIN,
    );
    assert!(matches!(d, RenewalDecision::RenewSoon { .. }));
    // 未临期（发卡 1 天前）→ Noop。
    let fresh = task_with_card(Some(now - CDuration::days(1)), now - CDuration::days(1));
    let d = renewal_decision(
        &fresh,
        &lane(Some(RunStatus::Running), 0, false),
        now,
        MARGIN,
    );
    assert_eq!(d, RenewalDecision::Noop);
}

#[test]
fn decision_picks_the_earlier_applicable_deadline() {
    let now = Utc::now();
    // 消息期限还远（发卡 1 天前），实体 13.5 天前创建 → 实体先到
    // 期（0.5 天 < 36h）→ 按实体期限 RenewSoon。
    let mut task = task_with_card(Some(now - CDuration::days(1)), now - CDuration::days(1));
    task.card_entity_created_at = Some(now - CDuration::days(13) - CDuration::hours(12));
    let d = renewal_decision(
        &task,
        &lane(Some(RunStatus::Running), 0, false),
        now,
        MARGIN,
    );
    assert_eq!(
        d,
        RenewalDecision::RenewSoon {
            reason: RenewReason::EntityDeadline
        }
    );
    // 实体比消息晚创建（消息期限更早）→ 仍按消息期限。
    let mut task = task_with_card(Some(now - CDuration::days(13)), now - CDuration::days(13));
    task.card_entity_created_at = Some(now - CDuration::days(1));
    let d = renewal_decision(
        &task,
        &lane(Some(RunStatus::Running), 0, false),
        now,
        MARGIN,
    );
    assert_eq!(
        d,
        RenewalDecision::RenewSoon {
            reason: RenewReason::MessageDeadline
        }
    );
}

#[test]
fn decision_idle_expired_renews_on_return_idle_fresh_is_noop() {
    let now = Utc::now();
    // 空闲 = 无在飞 Run 且队列空（paused 但队列空同样空闲）。
    let expired = task_with_card(Some(now - CDuration::days(15)), now - CDuration::days(15));
    for snap in [lane(None, 0, false), lane(None, 0, true)] {
        let d = renewal_decision(&expired, &snap, now, MARGIN);
        assert_eq!(d, RenewalDecision::RenewOnReturn, "snap {snap:?}");
    }
    // 空闲未过期（即使距期限 < 余量）→ Noop：不对空闲任务自动
    // 发无人看的换代消息。
    let fresh = task_with_card(Some(now - CDuration::days(13)), now - CDuration::days(13));
    let d = renewal_decision(&fresh, &lane(None, 0, false), now, MARGIN);
    assert_eq!(d, RenewalDecision::Noop);
    // 空闲 + 终态 current（Completed 非在飞）= 空闲语义。
    let done = lane(Some(RunStatus::Completed), 0, false);
    let d = renewal_decision(&expired, &done, now, MARGIN);
    assert_eq!(d, RenewalDecision::RenewOnReturn);
}

#[test]
fn decision_falls_back_to_created_at_when_sent_at_is_null() {
    let now = Utc::now();
    // v29 前存量行：card_sent_at NULL —— 卡即创建时所发（增量 2
    // 发卡紧接登记），回退 created_at。创建 15 天前 → 已过期。
    let task = task_with_card(None, now - CDuration::days(15));
    let d = renewal_decision(&task, &lane(None, 0, false), now, MARGIN);
    assert_eq!(d, RenewalDecision::RenewOnReturn);
    // 创建 1 天前的存量行 → 未过期。
    let task = task_with_card(None, now - CDuration::days(1));
    let d = renewal_decision(&task, &lane(None, 0, false), now, MARGIN);
    assert_eq!(d, RenewalDecision::Noop);
}

// ── 执行测试台（kernel + MockAdapter）────────────────────────────

/// 换代 MockAdapter：send_card 逐次发号（new-card-N）可注入失败/
/// 无 id；update_card 记录；文字消息记录（toast 断言用）。
struct RenewalMock {
    sent: tokio::sync::Mutex<Vec<(String, String, Option<String>)>>,
    updated: tokio::sync::Mutex<Vec<(String, String)>>,
    outgoing: tokio::sync::Mutex<Vec<String>>,
    fail_sends: AtomicBool,
    no_id: AtomicBool,
    counter: AtomicUsize,
}

impl RenewalMock {
    fn new() -> Self {
        Self {
            sent: tokio::sync::Mutex::new(Vec::new()),
            updated: tokio::sync::Mutex::new(Vec::new()),
            outgoing: tokio::sync::Mutex::new(Vec::new()),
            fail_sends: AtomicBool::new(false),
            no_id: AtomicBool::new(false),
            counter: AtomicUsize::new(0),
        }
    }
}

#[async_trait::async_trait]
impl PlatformAdapter for RenewalMock {
    async fn run_receiver(
        &self,
        _incoming: tokio::sync::mpsc::Sender<crate::channels::ChannelEvent>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> std::result::Result<(), crate::channels::ChannelError> {
        cancel.cancelled().await;
        Ok(())
    }

    async fn send_message(
        &self,
        _external_chat_id: &str,
        blocks: Vec<ContentBlock>,
        _reply_msg_id: Option<&str>,
    ) -> std::result::Result<Option<String>, crate::channels::ChannelError> {
        for b in &blocks {
            if let ContentBlock::Text { text } = b {
                self.outgoing.lock().await.push(text.clone());
            }
        }
        Ok(Some("msg-1".to_string()))
    }

    async fn send_card(
        &self,
        external_chat_id: &str,
        card_json: &str,
        reply_msg_id: Option<&str>,
    ) -> std::result::Result<Option<String>, crate::channels::ChannelError> {
        if self.fail_sends.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(crate::channels::ChannelError::Platform(
                "mock send_card failure".into(),
            ));
        }
        self.sent.lock().await.push((
            external_chat_id.to_string(),
            card_json.to_string(),
            reply_msg_id.map(str::to_string),
        ));
        if self.no_id.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(None);
        }
        let n = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        Ok(Some(format!("new-card-{n}")))
    }

    async fn update_card(
        &self,
        message_id: &str,
        card_json: &str,
    ) -> std::result::Result<(), crate::channels::ChannelError> {
        self.updated
            .lock()
            .await
            .push((message_id.to_string(), card_json.to_string()));
        Ok(())
    }

    fn supports_status_card(&self) -> bool {
        true
    }
}

/// 测试台：kernel（Sim 执行适配器）+ 注入 RenewalMock 的通道实例
/// + per-task 锁注册表。返回 (kernel, mock, instances, patches, _tmp)。
async fn renewal_harness() -> (
    Arc<Kernel>,
    Arc<RenewalMock>,
    Arc<DashMap<String, ChannelInstance>>,
    ExecCardPatches,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let storage = crate::storage::StorageSet::open(dir.path().join("data"))
        .await
        .unwrap();
    let kernel = Kernel::new(
        &storage,
        crate::agent::AgentConfig::default(),
        None,
        None,
        vec![],
        false,
        None,
        vec![],
        crate::config::TasksConfig::default(),
        crate::config::GcConfig::default(),
        crate::config::ExecConfig::default(),
        false,
        crate::permission::Level::default(),
    )
    .unwrap();
    let mock = Arc::new(RenewalMock::new());
    let adapter: Arc<dyn PlatformAdapter> = mock.clone();
    let instances = Arc::new(DashMap::new());
    let config = ChannelConfig {
        name: "mock".to_string(),
        enabled: true,
        platform: PlatformConfig::Feishu {
            app_id: "fake".into(),
            app_secret: "fake".into(),
        },
        exec_tasks: true,
        ..Default::default()
    };
    instances.insert(
        "mock".to_string(),
        ChannelInstance::test_instance(config, adapter),
    );
    (kernel, mock, instances, ExecCardPatches::default(), dir)
}

/// 建任务并回填卡（卡即 Thread 锚：root == card）。
async fn task_with_card_row(kernel: &Arc<Kernel>, dedup: &str) -> ExecTask {
    let (task, created) = kernel
        .create_exec_task(CreateExecTask {
            channel_name: "mock".into(),
            provider: ExecProvider::Kimi,
            goal: format!("目标 {dedup}"),
            working_dir: None,
            created_by: "ou_t".into(),
            source: ExecTaskSource::Skill,
            dedup_key: dedup.into(),
        })
        .await
        .unwrap();
    assert!(created);
    kernel
        .exec_task_store()
        .set_thread_and_card(&task.id, "card-1", "card-1")
        .await
        .unwrap()
}

/// 接受一条输入并派发（Sim 挂起 → Running）；再排一条等待。
async fn start_running_with_queue(kernel: &Arc<Kernel>, task_id: &ExecTaskId) {
    for (id, text) in [("m1", "第一步"), ("m2", "第二步排队")] {
        let outcome =
            kernel
                .exec_inbox()
                .accept(task_id, id.to_string(), "ou_t", text.to_string(), vec![]);
        assert!(matches!(outcome, AcceptOutcome::Accepted { .. }));
    }
    assert!(kernel.exec_scheduler().try_dispatch(task_id).await.unwrap());
    let snap = kernel.exec_scheduler().snapshot(task_id);
    assert_eq!(
        snap.current.as_ref().map(|r| r.status),
        Some(RunStatus::Running)
    );
    assert_eq!(kernel.exec_inbox().len(task_id), 1);
}

fn exec_action(chat: &str, user: &str, value: serde_json::Value) -> CardAction {
    CardAction {
        operator_open_id: user.to_string(),
        operator_union_id: None,
        chat_id: Some(chat.to_string()),
        message_id: Some("card-1".to_string()),
        token: None,
        value,
    }
}

async fn last_toast(mock: &RenewalMock) -> String {
    mock.outgoing
        .lock()
        .await
        .last()
        .cloned()
        .unwrap_or_default()
}

// ── 规格场景 1：活跃任务临期 → 新卡发原 Thread、gen+1 ───────────

#[tokio::test]
async fn renew_posts_new_card_in_original_thread_and_switches_generation() {
    let (kernel, mock, instances, patches, _tmp) = renewal_harness().await;
    let task = task_with_card_row(&kernel, "r1").await;
    start_running_with_queue(&kernel, &task.id).await;

    let outcome = renew_master_card(&kernel, &instances, &patches, &task.id).await;

    let RenewOutcome::Renewed { new_msg_id, gen } = outcome else {
        panic!("expected Renewed, got {outcome:?}");
    };
    assert_eq!(new_msg_id, "new-card-1");
    assert_eq!(gen, 1, "代次 +1");
    // 新卡发原 Thread：锚 = 旧 Thread 根（卡即锚，根仍是旧卡）。
    {
        let sent = mock.sent.lock().await;
        assert_eq!(sent.len(), 1, "只发一张新卡（C9）");
        assert_eq!(sent[0].2.as_deref(), Some("card-1"), "reply 到旧 Thread 根");
        // 新卡按钮带新代次（旧代回调被重核拒绝的凭据）。
        assert!(sent[0].1.contains("\"gen\":1"), "{}", sent[0].1);
        // 新卡渲染自当前快照：Running + 队列 1 条如实呈现。
        assert!(sent[0].1.contains("执行中 · 第 1 轮"), "{}", sent[0].1);
        assert!(sent[0].1.contains("已受理待执行 1 条"), "{}", sent[0].1);
    }
    // 映射已切换：card_msg_id 新、thread_root 不变、gen 1、发卡
    // 时刻回填。
    let after = kernel
        .exec_task_store()
        .get(&task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.card_msg_id.as_deref(), Some("new-card-1"));
    assert_eq!(after.thread_root_msg_id.as_deref(), Some("card-1"));
    assert_eq!(after.card_generation, 1);
    let sent_at = after.card_sent_at.expect("card_sent_at backfilled");
    assert!((Utc::now() - sent_at) < CDuration::minutes(1), "{sent_at}");
    assert_eq!(after.card_entity_created_at, None, "CardKit 未启用不回填");
    // 旧卡标注新入口（尽力而为——mock 成功，内容指向换代）。
    {
        let updated = mock.updated.lock().await;
        assert_eq!(updated.len(), 1);
        assert_eq!(updated[0].0, "card-1", "标注打在旧卡上");
        assert!(updated[0].1.contains("已换代"), "{}", updated[0].1);
        assert!(!updated[0].1.contains("behaviors"), "旧卡不再出按钮");
    }
    kernel.close_tokens();
}

#[tokio::test]
async fn old_generation_callback_rejected_after_renewal() {
    let (kernel, mock, instances, patches, _tmp) = renewal_harness().await;
    let task = task_with_card_row(&kernel, "r2").await;
    start_running_with_queue(&kernel, &task.id).await;
    let outcome = renew_master_card(&kernel, &instances, &patches, &task.id).await;
    assert!(matches!(outcome, RenewOutcome::Renewed { gen: 1, .. }));
    let config = ChannelConfig {
        name: "mock".into(),
        enabled: true,
        platform: PlatformConfig::Feishu {
            app_id: "fake".into(),
            app_secret: "fake".into(),
        },
        exec_tasks: true,
        ..Default::default()
    };
    let adapter: Arc<dyn PlatformAdapter> = mock.clone();

    // 旧代（gen=0）回调 → toast 拒绝，零状态变更（增量 4 gen 重
    // 核覆盖旧卡控制失效，L1 换代的另一半）。
    let before = kernel.exec_scheduler().snapshot(&task.id);
    crate::channels::taskcard::handle_exec_action(
        "mock",
        &config,
        &kernel,
        &adapter,
        exec_action(
            "oc_1",
            "ou_1",
            serde_json::json!({
                "action": "exec_resume", "task": task.id.as_str(), "run": null, "gen": 0,
            }),
        ),
    )
    .await;
    let toast = last_toast(&mock).await;
    assert!(toast.contains("卡片已过期，操作未生效"), "{toast}");
    assert_eq!(
        kernel.exec_scheduler().snapshot(&task.id),
        before,
        "旧代回调零状态变更"
    );

    // 新代（gen=1）回调正常受理（代次重核通过）。
    crate::channels::taskcard::handle_exec_action(
        "mock",
        &config,
        &kernel,
        &adapter,
        exec_action(
            "oc_1",
            "ou_1",
            serde_json::json!({
                "action": "exec_resume", "task": task.id.as_str(), "run": null, "gen": 1,
            }),
        ),
    )
    .await;
    let toast = last_toast(&mock).await;
    assert!(toast.contains("已恢复"), "{toast}");
    kernel.close_tokens();
}

// ── 规格场景 1 的 sweep 接线：活跃临期换代、空闲过期不自动换 ────

#[tokio::test]
async fn sweep_renews_due_active_task_and_skips_idle_expired() {
    let (kernel, mock, instances, patches, _tmp) = renewal_harness().await;
    let active = task_with_card_row(&kernel, "s1").await;
    start_running_with_queue(&kernel, &active.id).await;
    let idle = task_with_card_row(&kernel, "s2").await;

    // now 推到 15 天后：活跃任务过期（距期限负值 < 余量）→
    // RenewSoon 由 sweep 执行；空闲任务过期 → RenewOnReturn，
    // sweep 不自动换（无自动换代，等用户返回）。
    let future = Utc::now() + CDuration::days(15);
    sweep_once(&kernel, &instances, &patches, MARGIN, future).await;

    let active_after = kernel
        .exec_task_store()
        .get(&active.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(active_after.card_generation, 1, "活跃任务已换代");
    assert_eq!(active_after.card_msg_id.as_deref(), Some("new-card-1"));
    let idle_after = kernel
        .exec_task_store()
        .get(&idle.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(idle_after.card_generation, 0, "空闲任务 sweep 不自动换代");
    assert_eq!(idle_after.card_msg_id.as_deref(), Some("card-1"));
    assert_eq!(mock.sent.lock().await.len(), 1, "只换活跃任务的卡");
    kernel.close_tokens();
}

// ── 规格场景 2：空闲已过期 → 用户输入时 RenewOnReturn 先换再受理 ─

#[tokio::test]
async fn return_path_renews_expired_idle_task_before_accept() {
    let (kernel, _mock, instances, patches, _tmp) = renewal_harness().await;
    let task = task_with_card_row(&kernel, "t1").await;

    // 用户返回（now 推到 15 天后，卡已过期）：先换再受理。
    let future = Utc::now() + CDuration::days(15);
    let outcome =
        renew_on_return_if_due(&kernel, &instances, &patches, &task, MARGIN, future).await;
    assert!(
        matches!(outcome, RenewOutcome::Renewed { gen: 1, .. }),
        "{outcome:?}"
    );
    let after = kernel
        .exec_task_store()
        .get(&task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.card_msg_id.as_deref(), Some("new-card-1"));

    // 换代完成后受理照常（先换再受理——受理不被呈现阻断）。
    let outcome = kernel.exec_inbox().accept(
        &task.id,
        "m1".to_string(),
        "ou_t",
        "回来了，继续".to_string(),
        vec![],
    );
    assert!(matches!(outcome, AcceptOutcome::Accepted { .. }));
    kernel.close_tokens();
}

#[tokio::test]
async fn return_path_leaves_fresh_idle_task_alone() {
    let (kernel, mock, instances, patches, _tmp) = renewal_harness().await;
    let task = task_with_card_row(&kernel, "t2").await;

    // 空闲未过期 → Noop，不发卡；受理照常。
    let outcome =
        renew_on_return_if_due(&kernel, &instances, &patches, &task, MARGIN, Utc::now()).await;
    assert_eq!(outcome, RenewOutcome::Noop);
    assert!(mock.sent.lock().await.is_empty(), "未过期不发换代卡");
    let outcome = kernel.exec_inbox().accept(
        &task.id,
        "m1".to_string(),
        "ou_t",
        "新输入".to_string(),
        vec![],
    );
    assert!(matches!(outcome, AcceptOutcome::Accepted { .. }));
    kernel.close_tokens();
}

// ── 规格场景 3：发送失败 → SendUncertain、映射不动、无第二张卡 ────

#[tokio::test]
async fn send_failure_keeps_mapping_and_sends_no_second_card() {
    let (kernel, mock, instances, patches, _tmp) = renewal_harness().await;
    let task = task_with_card_row(&kernel, "f1").await;
    start_running_with_queue(&kernel, &task.id).await;
    let before = kernel
        .exec_task_store()
        .get(&task.id)
        .await
        .unwrap()
        .unwrap();

    // ① 发送失败。
    mock.fail_sends
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let outcome = renew_master_card(&kernel, &instances, &patches, &task.id).await;
    let RenewOutcome::SendUncertain { error } = outcome else {
        panic!("expected SendUncertain, got {outcome:?}");
    };
    assert!(error.contains("mock send_card failure"), "{error}");
    mock.fail_sends
        .store(false, std::sync::atomic::Ordering::Relaxed);

    // ② 平台未回 id（无法确认可定位，等同失败）。
    mock.no_id.store(true, std::sync::atomic::Ordering::Relaxed);
    let outcome = renew_master_card(&kernel, &instances, &patches, &task.id).await;
    assert!(matches!(outcome, RenewOutcome::SendUncertain { .. }));
    mock.no_id
        .store(false, std::sync::atomic::Ordering::Relaxed);

    // ③ 通道实例不在（关停中）——同档不确定。
    let empty = Arc::new(DashMap::new());
    let outcome = renew_master_card(&kernel, &empty, &patches, &task.id).await;
    let RenewOutcome::SendUncertain { error } = outcome else {
        panic!("expected SendUncertain, got {outcome:?}");
    };
    assert!(error.contains("not available"), "{error}");

    // 映射逐项不动（含代次与发卡时刻）；无第二张卡生效（gen 仍 0，
    // 旧卡仍是当前卡）；未给旧卡打标注（换代未发生）。
    let after = kernel
        .exec_task_store()
        .get(&task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after, before, "发送不确定：映射原样保留（C9）");
    assert_eq!(
        mock.sent.lock().await.len(),
        1,
        "仅②一次成功发送尝试，未重试"
    );
    assert!(mock.updated.lock().await.is_empty(), "未标注旧卡");
    kernel.close_tokens();
}

// ── 规格场景 4：换代前后 inbox/lane/binding/current Run 逐项相等 ──

#[tokio::test]
async fn renewal_leaves_inbox_lane_binding_runs_untouched() {
    let (kernel, mock, instances, patches, _tmp) = renewal_harness().await;
    let task = task_with_card_row(&kernel, "u1").await;
    start_running_with_queue(&kernel, &task.id).await;
    // 暂停（有在飞 Run + 排队输入的完整形态）。
    kernel.exec_scheduler().stop_and_pause(&task.id, None).await;

    let snap_before = kernel.exec_scheduler().snapshot(&task.id);
    let inbox_len_before = kernel.exec_inbox().len(&task.id);
    let task_before = kernel
        .exec_task_store()
        .get(&task.id)
        .await
        .unwrap()
        .unwrap();

    let outcome = renew_master_card(&kernel, &instances, &patches, &task.id).await;
    assert!(matches!(outcome, RenewOutcome::Renewed { .. }));

    // lane 快照逐项相等（current Run 记录、paused、queued、
    // blocked_unknown、released）；inbox 长度不变；执行身份列
    // （binding/provider_session_id/status/goal/dedup）不变。
    let snap_after = kernel.exec_scheduler().snapshot(&task.id);
    assert_eq!(snap_after, snap_before, "换代不动 lane（D12）");
    assert_eq!(
        kernel.exec_inbox().len(&task.id),
        inbox_len_before,
        "换代不动 inbox"
    );
    let task_after = kernel
        .exec_task_store()
        .get(&task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(task_after.binding, task_before.binding);
    assert_eq!(
        task_after.provider_session_id,
        task_before.provider_session_id
    );
    assert_eq!(task_after.status, task_before.status);
    assert_eq!(task_after.goal, task_before.goal);
    assert_eq!(task_after.dedup_key, task_before.dedup_key);
    assert_eq!(
        snap_after.current.as_ref().map(|r| r.status),
        Some(RunStatus::Stopping),
        "停止中不被换代打断（不解暂停、不触发执行）"
    );
    // 新卡如实渲染停止中 + 已暂停（呈现与事实一致）。
    let sent = mock.sent.lock().await;
    assert!(sent[0].1.contains("停止中 · 第 1 轮"), "{}", sent[0].1);
    assert!(
        sent[0].1.contains("已暂停 · 进程内 · 重启不保留"),
        "{}",
        sent[0].1
    );
    drop(sent);
    kernel.close_tokens();
}
