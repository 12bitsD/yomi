//! chat-flow 增量 2 集成测试：N1 双入口创建契约（slash `/task` +
//! 内建工具 `task_create`）、任务 Thread 确定性分流与进程内受理登
//! 记。设计依据 docs/design/chat-flow-technical-design.md N1/C1/C2/D2。
//!
//! 复用 hub_test 的 `MockAdapter`（同模块树兄弟节点），hub 实例经
//! `ChannelInstance::test_instance` 注入——工具侧
//! `get_routing_for_session` 因此能解析出路由与 adapter。

use super::tests::MockAdapter;
use super::*;

use crate::channels::hub_handlers::handle_incoming_message;
use crate::channels::{ChannelConfig, ChannelMessage, ChannelStore, MappingKind, PlatformConfig};
use crate::exec::SqliteExecTaskStore;
use crate::storage::migrations::run_migrations;
use crate::tools::{TaskCreateTool, Tool, ToolExecCtx, TASK_CREATE_TOOL_NAME};

/// 测试台：kernel（带 "mock" 通道配置 → channel_hub 存在）+ 注入
/// mock adapter 的通道实例 + hub 自带的 channel store。
async fn task_harness() -> (
    Arc<Kernel>,
    Arc<MockAdapter>,
    Arc<dyn ChannelStore>,
    ChannelConfig,
    tempfile::TempDir,
) {
    task_harness_with_exec_tasks(true).await
}

/// 带开关的测试台（增量 3 场景 9：`exec_tasks=false` 回归用）。
async fn task_harness_with_exec_tasks(
    exec_tasks: bool,
) -> (
    Arc<Kernel>,
    Arc<MockAdapter>,
    Arc<dyn ChannelStore>,
    ChannelConfig,
    tempfile::TempDir,
) {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = ChannelConfig {
        name: "mock".to_string(),
        enabled: true,
        platform: PlatformConfig::Feishu {
            app_id: "fake".into(),
            app_secret: "fake".into(),
        },
        require_mention: false,
        // R7 开关（增量 3）：本测试台演练任务功能，显式启用。
        exec_tasks,
        ..Default::default()
    };
    let mut kconfig = crate::config::Config {
        data_dir: tmp.path().to_path_buf(),
        channels: vec![config.clone()],
        ..crate::config::Config::default()
    };
    kconfig.finalize();
    let kernel = crate::build_kernel(&kconfig, false).await.unwrap();
    let mock = Arc::new(MockAdapter::new("mock"));
    let adapter: Arc<dyn PlatformAdapter> = mock.clone();
    let hub = kernel.channel_manager().expect("channel hub configured");
    hub.instances.insert(
        "mock".to_string(),
        ChannelInstance::test_instance(config.clone(), adapter),
    );
    let store = hub.store.clone();
    (kernel, mock, store, config, tmp)
}

fn chan_msg(
    chat: &str,
    user: &str,
    id: &str,
    text: &str,
    thread: Option<&str>,
    root: Option<&str>,
) -> ChannelMessage {
    ChannelMessage {
        external_chat_id: chat.to_string(),
        external_user_id: user.to_string(),
        external_message_id: Some(id.to_string()),
        is_mention: true,
        raw_text: Some(text.to_string()),
        content: vec![ContentBlock::Text {
            text: text.to_string(),
        }],
        image_keys: vec![],
        thread_id: thread.map(str::to_string),
        root_id: root.map(str::to_string),
        parent_id: None,
        is_group: true,
        create_time: None,
        doc_comment: None,
    }
}

async fn session_count(kernel: &Kernel) -> usize {
    kernel
        .list_sessions(
            None,
            crate::storage::session::SessionListScope::All,
            None,
            100,
        )
        .await
        .unwrap()
        .sessions
        .len()
}

fn output_text(out: &crate::types::ToolOutput) -> String {
    out.contents.iter().filter_map(|b| b.as_text()).collect()
}

// ── 解析（组件 3a）─────────────────────────────────────────────

#[test]
fn parse_task_command() {
    use crate::channels::hub_command::{parse_channel_command, ChannelCommand};

    assert!(matches!(
        parse_channel_command(Some("/task")),
        ChannelCommand::InvalidTaskCommand
    ));
    // 缺省 provider = kimi；goal 原文保留。
    match parse_channel_command(Some("/task 修复 登录页 校验")) {
        ChannelCommand::Task { provider, goal } => {
            assert_eq!(provider, crate::exec::ExecProvider::Kimi);
            assert_eq!(goal, "修复 登录页 校验");
        }
        _ => panic!("expected Task"),
    }
    // 显式 codex 前缀。
    match parse_channel_command(Some("/task codex 跑全量测试")) {
        ChannelCommand::Task { provider, goal } => {
            assert_eq!(provider, crate::exec::ExecProvider::Codex);
            assert_eq!(goal, "跑全量测试");
        }
        _ => panic!("expected Task"),
    }
    // 只有前缀没有 goal → usage。
    assert!(matches!(
        parse_channel_command(Some("/task kimi")),
        ChannelCommand::InvalidTaskCommand
    ));
    // 命令表/help 收录。
    assert!(crate::channels::hub_command::HELP_TEXT.contains("/task"));
}

// ── 规格测试 1：双入口同 dedup_key → 同一 task，第二张卡不发出 ──

#[tokio::test]
async fn dual_entry_same_dedup_key_converges_to_one_task() {
    let (kernel, mock, store, config, tmp) = task_harness().await;
    let adapter: Arc<dyn PlatformAdapter> = mock.clone();
    let obs = Arc::new(ObsTracker::new());

    // 入口 A：slash `/task`（dedup 凭据 = 触发消息 id "m1"）。
    let reply = handle_incoming_message(
        "mock",
        &config,
        &store,
        Arc::clone(&kernel),
        chan_msg("oc_1", "ou_1", "m1", "/task 修复登录页校验", None, None),
        &obs,
        &adapter,
    )
    .await
    .unwrap()
    .unwrap();
    assert!(reply.contains("任务已创建"), "{reply}");
    assert_eq!(
        mock.cards.lock().await.len(),
        1,
        "slash entry posts one card"
    );

    let slash_task = kernel
        .exec_task_store()
        .find_by_dedup("mock", "m1")
        .await
        .unwrap()
        .expect("task registered by the slash entry");

    // 入口 B：`task_create` 工具（同一创建意图跨入口传递 → 同 dedup_key）。
    let sid = kernel
        .create_session(crate::kernel::CreateSessionInput {
            project_id: None,
            working_dir: None,
            auto_approve_level: None,
            tool_blocklist: vec![],
            model_key: None,
            context_window: None,
        })
        .await
        .unwrap();
    store
        .save_mapping("mock", "oc_1", &sid, "oc_1", None, MappingKind::Normal)
        .await
        .unwrap();
    let tool = TaskCreateTool::new(kernel.channel_manager(), Arc::downgrade(&kernel));
    let out = tool
        .exec(
            serde_json::json!({"goal": "修复登录页校验", "dedup_key": "m1"}),
            ToolExecCtx::new("tc-1", tmp.path(), sid.0.clone()),
        )
        .await
        .unwrap();
    let out: serde_json::Value = serde_json::from_str(&output_text(&out)).unwrap();
    assert_eq!(out["state"], "existing", "{out}");
    assert_eq!(out["created"], false, "{out}");
    assert_eq!(out["task_id"], slash_task.id.as_str(), "{out}");

    // 第二张卡未发出（C1：重送不重复发卡）。
    assert_eq!(mock.cards.lock().await.len(), 1, "no second card");

    kernel.stop().await;
}

// ── 规格测试 2：/task 空 goal → usage；任务 Thread 内 /task → 拒绝 ──

#[tokio::test]
async fn task_command_usage_and_nesting_refusal() {
    let (kernel, mock, store, config, _tmp) = task_harness().await;
    let adapter: Arc<dyn PlatformAdapter> = mock.clone();
    let obs = Arc::new(ObsTracker::new());

    // 空 goal → usage。
    let reply = handle_incoming_message(
        "mock",
        &config,
        &store,
        Arc::clone(&kernel),
        chan_msg("oc_1", "ou_1", "m0", "/task", None, None),
        &obs,
        &adapter,
    )
    .await
    .unwrap()
    .unwrap();
    assert!(reply.contains("Usage: `/task"), "{reply}");
    assert!(kernel
        .exec_task_store()
        .find_by_dedup("mock", "m0")
        .await
        .unwrap()
        .is_none());

    // 顶层建任务（卡 = "card-1" = Thread 锚）。
    handle_incoming_message(
        "mock",
        &config,
        &store,
        Arc::clone(&kernel),
        chan_msg("oc_1", "ou_1", "m1", "/task 做 A", None, None),
        &obs,
        &adapter,
    )
    .await
    .unwrap();

    // 任务 Thread 内 /task → 拒绝嵌套，且不新建任务。
    let reply = handle_incoming_message(
        "mock",
        &config,
        &store,
        Arc::clone(&kernel),
        chan_msg(
            "oc_1",
            "ou_1",
            "t1",
            "/task 嵌套一个",
            Some("omt_1"),
            Some("card-1"),
        ),
        &obs,
        &adapter,
    )
    .await
    .unwrap()
    .unwrap();
    assert!(reply.contains("不支持在任务 Thread 内再建任务"), "{reply}");
    assert!(
        kernel
            .exec_task_store()
            .find_by_dedup("mock", "t1")
            .await
            .unwrap()
            .is_none(),
        "nested /task must not register a task"
    );
    assert_eq!(
        mock.cards.lock().await.len(),
        1,
        "no card for the nested attempt"
    );

    kernel.stop().await;
}

// ── 规格测试 3：工具无通道会话 → 仅登记 no_channel，store 可读 ──

#[tokio::test]
async fn tool_without_channel_registers_no_channel() {
    let (kernel, _mock, _store, _config, tmp) = task_harness().await;
    let tool = TaskCreateTool::new(kernel.channel_manager(), Arc::downgrade(&kernel));

    let out = tool
        .exec(
            serde_json::json!({"goal": "无通道会话里的交办", "provider": "codex"}),
            ToolExecCtx::new("tc-2", tmp.path(), "sess-unrouted".to_string()),
        )
        .await
        .unwrap();
    let out: serde_json::Value = serde_json::from_str(&output_text(&out)).unwrap();
    assert_eq!(out["state"], "no_channel", "{out}");
    assert_eq!(out["created"], true, "{out}");
    assert_eq!(out["provider"], "codex", "{out}");

    // 仅登记：store 可读，thread/card 字段为空（C2 半完成态如实）。
    let task_id = out["task_id"].as_str().unwrap();
    let task = kernel
        .exec_task_store()
        .get(&crate::types::ExecTaskId::from(task_id.to_string()))
        .await
        .unwrap()
        .expect("task readable from the store");
    assert_eq!(task.channel_name, "skill");
    assert_eq!(task.thread_root_msg_id, None);
    assert_eq!(task.card_msg_id, None);

    // goal 空 → 明确报错（Kernel::create_exec_task 的入口校验）。
    let err = tool
        .exec(
            serde_json::json!({"goal": "   "}),
            ToolExecCtx::new("tc-3", tmp.path(), "sess-unrouted".to_string()),
        )
        .await;
    assert!(err.is_err(), "empty goal rejected");

    kernel.stop().await;
}

// ── 规格测试 4：任务 Thread 分流 + 受理登记（含归档拒收）──────────

#[tokio::test]
async fn task_thread_diversion_accepts_in_order_and_skips_chat() {
    let (kernel, mock, store, config, _tmp) = task_harness().await;
    let adapter: Arc<dyn PlatformAdapter> = mock.clone();
    let obs = Arc::new(ObsTracker::new());

    // 顶层 /task 建任务：卡 "card-1" 即 Thread 锚。
    handle_incoming_message(
        "mock",
        &config,
        &store,
        Arc::clone(&kernel),
        chan_msg("oc_1", "ou_1", "m1", "/task 做 A", None, None),
        &obs,
        &adapter,
    )
    .await
    .unwrap();
    let task = kernel
        .exec_task_store()
        .find_by_dedup("mock", "m1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(task.thread_root_msg_id.as_deref(), Some("card-1"));
    let sessions_before = session_count(&kernel).await;

    // Thread 输入 1：分流受理，不进 chat。增量 3 起受理即派发：
    // 本输入立即取得资格开跑（Sim 挂起模式 → Running 不动），
    // 受理计数以受理时刻为准记 1，随后队首被消费出队。
    let reply = handle_incoming_message(
        "mock",
        &config,
        &store,
        Arc::clone(&kernel),
        chan_msg(
            "oc_1",
            "ou_1",
            "t1",
            "先做第一步",
            Some("omt_1"),
            Some("card-1"),
        ),
        &obs,
        &adapter,
    )
    .await
    .unwrap();
    assert_eq!(reply, None, "diverted messages produce no chat reply");
    {
        let updated = mock.updated_cards.lock().await;
        assert_eq!(updated.len(), 1, "accept refreshes the card via PATCH");
        assert_eq!(updated[0].0, "card-1");
        assert!(updated[0].1.contains("1 条"), "{}", updated[0].1);
        assert!(updated[0].1.contains("重启不保留"), "{}", updated[0].1);
    }
    // 受理→派发（增量 3）：t1 已开跑，原生身份完成绑定（N2）。
    let snap = kernel.exec_scheduler().snapshot(&task.id);
    assert_eq!(
        snap.current.as_ref().map(|r| (r.status, r.text.as_str())),
        Some((crate::exec::RunStatus::Running, "先做第一步"))
    );
    let bound = kernel
        .exec_task_store()
        .get(&task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bound.binding, crate::exec::BindingState::Bound);
    assert_eq!(
        kernel.exec_inbox().len(&task.id),
        0,
        "dispatched input consumed"
    );

    // Thread 输入 2：有序受理；A 在飞（挂起）→ 排队不派发。
    handle_incoming_message(
        "mock",
        &config,
        &store,
        Arc::clone(&kernel),
        chan_msg(
            "oc_1",
            "ou_2",
            "t2",
            "再补个背景",
            Some("omt_1"),
            Some("card-1"),
        ),
        &obs,
        &adapter,
    )
    .await
    .unwrap();
    let snapshot = kernel.exec_inbox().snapshot(&task.id);
    assert_eq!(snapshot.len(), 1, "t1 dispatched, only t2 queued");
    assert_eq!(snapshot[0].seq, 2);
    assert_eq!(snapshot[0].msg_id, "t2");
    assert_eq!(snapshot[0].text, "再补个背景");
    assert_eq!(snapshot[0].sender_open_id, "ou_2");
    {
        let updated = mock.updated_cards.lock().await;
        assert_eq!(updated.len(), 2);
        assert!(updated[1].1.contains("1 条"), "{}", updated[1].1);
    }

    // 同 msg_id 重送 → Duplicate：inbox 不动，卡不重复 PATCH。
    let reply = handle_incoming_message(
        "mock",
        &config,
        &store,
        Arc::clone(&kernel),
        chan_msg(
            "oc_1",
            "ou_1",
            "t1",
            "先做第一步",
            Some("omt_1"),
            Some("card-1"),
        ),
        &obs,
        &adapter,
    )
    .await
    .unwrap();
    assert_eq!(reply, None, "duplicate resend is silent");
    assert_eq!(
        kernel.exec_inbox().len(&task.id),
        1,
        "resend not re-accepted"
    );
    assert_eq!(
        mock.updated_cards.lock().await.len(),
        2,
        "no PATCH on duplicate"
    );

    // 全程未建 chat session / mapping（分流绝不进 chat 路径）。
    assert_eq!(
        session_count(&kernel).await,
        sessions_before,
        "no chat session created for task-thread messages"
    );
    assert!(
        store.list_mappings("mock").await.unwrap().is_empty(),
        "no session mapping created"
    );

    // 归档 → 线程内拒收（不删历史）。
    kernel.exec_task_store().archive(&task.id).await.unwrap();
    let reply = handle_incoming_message(
        "mock",
        &config,
        &store,
        Arc::clone(&kernel),
        chan_msg(
            "oc_1",
            "ou_1",
            "t3",
            "归档后再来",
            Some("omt_1"),
            Some("card-1"),
        ),
        &obs,
        &adapter,
    )
    .await
    .unwrap()
    .unwrap();
    assert!(reply.contains("任务已归档"), "{reply}");
    assert_eq!(
        kernel.exec_inbox().len(&task.id),
        1,
        "archived task accepts nothing"
    );

    kernel.stop().await;
}

// ── 规格测试 5：未命中任务 root 的 Thread 消息 → 原 chat 路径 ──────

#[tokio::test]
async fn non_task_thread_message_takes_the_chat_path() {
    let (kernel, mock, store, config, _tmp) = task_harness().await;
    let adapter: Arc<dyn PlatformAdapter> = mock.clone();
    let obs = Arc::new(ObsTracker::new());

    // 先建一个任务（其 Thread 锚是 "card-1"），再造一条无关 Thread。
    handle_incoming_message(
        "mock",
        &config,
        &store,
        Arc::clone(&kernel),
        chan_msg("oc_1", "ou_1", "m1", "/task 做 A", None, None),
        &obs,
        &adapter,
    )
    .await
    .unwrap();
    let task = kernel
        .exec_task_store()
        .find_by_dedup("mock", "m1")
        .await
        .unwrap()
        .unwrap();

    // 普通 Thread 消息（root 不命中任何任务）→ 走原 chat 路径：
    // 为该 Thread 建出 session + mapping（冒烟：chat 行为原样）。
    let sessions_before = session_count(&kernel).await;
    handle_incoming_message(
        "mock",
        &config,
        &store,
        Arc::clone(&kernel),
        chan_msg(
            "oc_1",
            "ou_1",
            "u1",
            "随便聊聊",
            Some("omt_2"),
            Some("om_other"),
        ),
        &obs,
        &adapter,
    )
    .await
    .unwrap();
    assert_eq!(
        session_count(&kernel).await,
        sessions_before + 1,
        "chat path creates a session for the plain thread"
    );
    assert!(
        store.find_mapping("mock", "omt_2").await.unwrap().is_some(),
        "chat path saves the thread mapping"
    );
    // 与任务互不影响：任务 inbox 仍为空，任务卡未被 PATCH。
    assert_eq!(kernel.exec_inbox().len(&task.id), 0);
    assert!(mock.updated_cards.lock().await.is_empty());

    kernel.stop().await;
}

// ── 规格测试 3b 附：exec_task_store 配好才注册 task_create ─────────

#[tokio::test]
async fn task_create_tool_registered_only_with_exec_task_store() {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    run_migrations(&pool).await.unwrap();
    let store: Arc<dyn crate::exec::ExecTaskStore> = Arc::new(SqliteExecTaskStore::new(pool));

    let shared = |with_store: bool| {
        let shared = crate::agent::AgentShared::with_data_dir(
            Arc::new(std::collections::BTreeMap::new()),
            "m".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            vec![],
            None,
            None,
            std::path::PathBuf::from("/tmp"),
        );
        Arc::new(if with_store {
            shared.with_exec_task_store(Some(Arc::clone(&store)))
        } else {
            shared
        })
    };
    let bus = crate::comms::EventBus::new();
    let handle = bus.handle(SessionId::new());
    let registry = |shared: &Arc<crate::agent::AgentShared>| {
        crate::tools::ToolRegistry::new().with_standard_tools(crate::tools::ToolRegistryConfig {
            shared,
            event_bus: &handle,
            session_id: "s1",
            input_bus: None,
            file_state_store: None,
            tool_blocklist: vec![],
            flags: crate::tools::ToolFlags::default(),
        })
    };

    assert!(registry(&shared(true)).has(TASK_CREATE_TOOL_NAME));
    assert!(!registry(&shared(false)).has(TASK_CREATE_TOOL_NAME));
}

// ── 增量 3 场景 9：功能开关（R7）──────────────────────────────

/// `exec_tasks=false`：`/task` 明确拒绝（不登记、不发卡）；用法错误
/// 的 `/task` 同样拒绝（不展示已关闭功能的用法）。
#[tokio::test]
async fn exec_tasks_off_task_command_refused() {
    let (kernel, mock, store, config, _tmp) = task_harness_with_exec_tasks(false).await;
    let adapter: Arc<dyn PlatformAdapter> = mock.clone();
    let obs = Arc::new(ObsTracker::new());

    let reply = handle_incoming_message(
        "mock",
        &config,
        &store,
        Arc::clone(&kernel),
        chan_msg("oc_1", "ou_1", "m1", "/task 做 A", None, None),
        &obs,
        &adapter,
    )
    .await
    .unwrap()
    .unwrap();
    assert!(reply.contains("未启用执行任务功能"), "{reply}");
    assert!(
        kernel
            .exec_task_store()
            .find_by_dedup("mock", "m1")
            .await
            .unwrap()
            .is_none(),
        "refused /task must not register a task"
    );
    assert!(mock.cards.lock().await.is_empty(), "no card posted");

    let reply = handle_incoming_message(
        "mock",
        &config,
        &store,
        Arc::clone(&kernel),
        chan_msg("oc_1", "ou_1", "m2", "/task", None, None),
        &obs,
        &adapter,
    )
    .await
    .unwrap()
    .unwrap();
    assert!(reply.contains("未启用执行任务功能"), "{reply}");

    kernel.stop().await;
}

/// `exec_tasks=false`：任务 Thread 分流关闭——即使登记里存在命中
/// 的 Thread 锚（比如开关被关掉前建的任务），消息也落回原 chat
/// 路径（建 chat session + mapping，不受理进 inbox）。
#[tokio::test]
async fn exec_tasks_off_diversion_falls_back_to_chat() {
    let (kernel, mock, store, config, _tmp) = task_harness_with_exec_tasks(false).await;
    let adapter: Arc<dyn PlatformAdapter> = mock.clone();
    let obs = Arc::new(ObsTracker::new());

    // 直接在登记里造一个带 Thread 锚的任务（绕过已关闭的 /task）。
    let (task, created) = kernel
        .exec_task_store()
        .create(&crate::exec::CreateExecTask {
            channel_name: "mock".to_string(),
            provider: crate::exec::ExecProvider::Kimi,
            goal: "存量任务".to_string(),
            working_dir: None,
            created_by: "ou_1".to_string(),
            source: crate::exec::ExecTaskSource::Entry,
            dedup_key: "legacy".to_string(),
        })
        .await
        .unwrap();
    assert!(created);
    kernel
        .exec_task_store()
        .set_thread_and_card(&task.id, "card-x", "card-x")
        .await
        .unwrap();

    // 命中锚的 Thread 消息 → 落回 chat 路径：建 session + mapping，
    // inbox 不受理，卡不 PATCH。
    let sessions_before = session_count(&kernel).await;
    handle_incoming_message(
        "mock",
        &config,
        &store,
        Arc::clone(&kernel),
        chan_msg(
            "oc_1",
            "ou_1",
            "u1",
            "开关关了之后再来",
            Some("omt_1"),
            Some("card-x"),
        ),
        &obs,
        &adapter,
    )
    .await
    .unwrap();
    assert_eq!(
        session_count(&kernel).await,
        sessions_before + 1,
        "diversion off: plain chat path creates a session"
    );
    assert!(
        store.find_mapping("mock", "omt_1").await.unwrap().is_some(),
        "diversion off: chat mapping saved"
    );
    assert_eq!(
        kernel.exec_inbox().len(&task.id),
        0,
        "diversion off: nothing accepted into the inbox"
    );
    assert!(mock.updated_cards.lock().await.is_empty());

    kernel.stop().await;
}

/// `exec_tasks=false`：`task_create` 工具按路由到的通道配置拒绝
/// （state=disabled，未登记）；无通道路由的本地会话仍允许（仅登记）。
#[tokio::test]
async fn exec_tasks_off_task_create_tool_refused() {
    let (kernel, _mock, store, _config, tmp) = task_harness_with_exec_tasks(false).await;
    let tool = TaskCreateTool::new(kernel.channel_manager(), Arc::downgrade(&kernel));

    // 有通道路由（mock 通道 exec_tasks=false）→ 拒绝，不登记。
    let sid = kernel
        .create_session(crate::kernel::CreateSessionInput {
            project_id: None,
            working_dir: None,
            auto_approve_level: None,
            tool_blocklist: vec![],
            model_key: None,
            context_window: None,
        })
        .await
        .unwrap();
    store
        .save_mapping("mock", "oc_1", &sid, "oc_1", None, MappingKind::Normal)
        .await
        .unwrap();
    let out = tool
        .exec(
            serde_json::json!({"goal": "通道关闭时的交办", "dedup_key": "k-off"}),
            ToolExecCtx::new("tc-off", tmp.path(), sid.0.clone()),
        )
        .await
        .unwrap();
    let out: serde_json::Value = serde_json::from_str(&output_text(&out)).unwrap();
    assert_eq!(out["state"], "disabled", "{out}");
    assert!(
        kernel
            .exec_task_store()
            .find_by_dedup("mock", "k-off")
            .await
            .unwrap()
            .is_none(),
        "disabled channel: task must not be registered"
    );

    // 无通道路由的本地会话 → 允许（仅登记 no_channel）。
    let out = tool
        .exec(
            serde_json::json!({"goal": "本地会话的交办"}),
            ToolExecCtx::new("tc-local", tmp.path(), "sess-unrouted".to_string()),
        )
        .await
        .unwrap();
    let out: serde_json::Value = serde_json::from_str(&output_text(&out)).unwrap();
    assert_eq!(out["state"], "no_channel", "{out}");
    assert_eq!(out["created"], true, "{out}");

    kernel.stop().await;
}
