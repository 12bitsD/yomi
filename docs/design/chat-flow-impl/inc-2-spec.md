# P1 增量 2 实施规格：双入口创建 + Thread 分流 + 受理登记

目标：N1 双入口（Skill 工具 + 专用 slash 入口）共用同一创建契约；任务 Thread 消息确定性分流（不走 chat/steer）；原文/附件受理登记（进程内有序，P2 才加暂停/派发）。占位主卡用现有整卡 send_card/PATCH，CardKit 是 P5 的事。

## 既有接缝（已核实坐标）

- 命令注册：`channels/hub/command.rs`（CMD_* const、alias、ChannelCommand、parse_channel_command）+ `handlers.rs` match 臂 + HELP_TEXT。
- 工具注册：`tools/mod.rs` `with_standard_tools`（ToolRegistryConfig.shared = AgentShared）；`AgentShared` 在 `agent/types.rs`，已有 `channel_hub: Option<Arc<ChannelHub>>`，照 `with_cron` 模式加 `with_exec_task_store`。
- 会话→通道解析：`ChannelHub::get_routing_for_session(session_id) -> Option<(SessionRouting, Arc<dyn PlatformAdapter>)>`（hub/mod.rs:938）。
- 卡发送：`PlatformAdapter::send_card(chat_id, card_json, None)`；更新 `update_card(msg_id, card_json)`；能力探测 `supports_status_card()`。
- 路由解析：`handlers.rs:42` `effective_mapping_key` 之后；`msg.root_id`/`thread_root_id` 即 Thread 根 om_ id。
- 卡拼装参照：`channels/cards/welcome.rs`。

## 组件 1：kernel 创建契约服务（共用核心）

`Kernel::create_exec_task(input: CreateExecTask) -> Result<(ExecTask, bool)>`：薄封装 store.create（校验 goal 非空、provider 合法），两入口都经它，不各自直连 store。

store 增方法（补进 `exec/store.rs` + 测试）：`find_by_thread_root(channel_name, root_msg_id) -> Result<Option<ExecTask>>`（走 idx_exec_tasks_thread 索引）。

## 组件 2：共享创建助手（channels 侧）

`channels/taskcard/mod.rs`：`create_and_announce(kernel, adapter, channel_name, chat_id, input) -> Result<AnnounceOutcome>`：

1. `kernel.create_exec_task(input)` → (task, created)。created=false → 返回既有任务与现状（入口各自呈现"已是同一任务"，不重复发卡）。
2. created=true → 渲染占位卡（`channels/cards/taskcard.rs`，见组件 4）→ `adapter.send_card(chat_id, card, None)`。
3. 发卡成功 → `store.set_thread_and_card(task.id, card_msg_id, card_msg_id)`（卡即 Thread 锚）→ outcome = Ready{task, card_msg_id}。
4. 发卡失败 → 任务保持已登记、card 字段空（C2 半完成态如实），outcome = CardPending{task, error}；入口如实告知"任务已登记，卡片投递失败"，**不**重试建任务、**不**自动补卡。

`AnnounceOutcome { Ready{..}, Existing{task}, CardPending{task, error} }`。

## 组件 3：两个入口

### 3a. slash `/task`（Entry）

- command.rs：`CMD_TASK`、`ChannelCommand::Task`、parse 臂（`/task`）；HELP_TEXT 加一行。
- 语法：`/task <goal>`，可选前缀 `kimi|codex`（缺省 kimi）；空 goal → usage 文本。
- handlers.rs 臂：在任务 Thread 内调用 → 拒绝（不嵌套任务）；否则 `create_and_announce(..., source=Entry, dedup_key=msg.external_message_id, created_by=msg.external_user_id, channel_name, chat_id)` → 按 outcome 文字回执（Ready: 任务已创建+卡链接 `adapter.message_link`；Existing: 返回原任务+原卡链接；CardPending: 如实说明）。
- 权限：消息闸已限 allowed_users，本臂不叠加 admin（与 /stop 同档原则——创建不是管理配置）。

### 3b. 内建工具 `task_create`（Skill 路径）

- `tools/task_create.rs`：schema {goal: string 必填, provider: "kimi"|"codex" 可选缺省 kimi, working_dir: string 可选, dedup_key: string 可选}。desc 中文写清：用户明确交办执行（改代码/跑测试等实际工作）时调用；goal 保留用户原文；同一用户消息重复调用用同一 dedup_key（建议=触发消息 id，不知则省略）；普通讨论/方案不调用。
- 注册：`with_standard_tools` 中，`shared.exec_task_store.is_some()` 时注册（不需要 channel_hub——无通道会话也可创建，CardPending 变体为 NoChannel）。
- exec：`create_and_announce`（adapter/chat 经 `channel_hub.get_routing_for_session(ctx.session_id)` 解析；解析不到 → 仅登记，返回 task 身份 + 说明无通道卡）。输出 JSON：{task_id, created, state: ready/existing/card_pending/no_channel, card_msg_id?, provider, binding}。
- AgentShared：加 `exec_task_store: Option<Arc<dyn ExecTaskStore>>` + `with_exec_task_store`；Kernel::new 中 `storage.exec_task_store()` 装入 agent_shared。

## 组件 4：占位主卡（channels/cards/taskcard.rs）

`task_card(task: &ExecTask, accepted: usize) -> String`（整卡 JSON，现有 schema 1.0 风格，参照 welcome/obs 卡）：
- header：🧩 任务卡 · {goal 前 30 字}（turquoise）。
- 字段：Task `{id 短码}`、Provider、状态（已登记·待初始化 / 绑定损坏 / 已绑定——binding 三态如实）、**本进程已受理输入 n 条**（标注"重启不保留"，D2 诚实）、创建者、创建时间。
- note：在本卡 Thread 回复即向本任务交办；执行能力接入中（后续阶段提供停止/恢复）。
- 无按钮（回调面 P2 才接）。

## 组件 5：Thread 分流 + 受理登记

kernel 新增 `ExecInbox`（`exec/inbox.rs`）：`DashMap<ExecTaskId, VecDeque<AcceptedInput>>`；`AcceptedInput { seq: u64, msg_id, sender_open_id, text: String, image_keys: Vec<String>, accepted_at }`。方法：
- `accept(task_id, msg_id, sender, text, image_keys) -> AcceptOutcome`：msg_id 已在列 → Duplicate；否则定序 push → Accepted{seq}。**进程内语义，重启清空**（D2）。
- `len(task_id) -> usize`。
Kernel 持有 + accessor。

handlers.rs 分流（`effective_mapping_key` 之后、`match cmd` 之前）：
- 条件：`cmd == ChannelCommand::None` 且 `msg.thread_id.is_some()` 且 root（`msg.root_id` 或 `adapter.thread_root_id`）命中 `store.find_by_thread_root(channel_name, root)`。
- 命中 → 不进入 chat 路径（不 prepare_trigger、不 steer、不建 chat session）：任务 archived → 线程内文字回"任务已归档，不接受新输入"（不删历史）；否则 `inbox.accept` → Duplicate 静默（ reaction 也不重复）；Accepted → 用占位卡整卡 PATCH 更新（`task_card(task, inbox.len())`，adapter.update_card）。
- 未命中 → 原逻辑不变。
- slash 命令在任务 Thread 内不分流（保持旧语义，/task 除外——见 3a 拒绝）。

## 测试（integration，参照现有 channels 测试与 init_test 建池/建 Kernel 模式）

1. 双入口同 dedup_key → 同一 task id，第二张卡不发出。
2. /task 空 goal → usage；任务 Thread 内 /task → 拒绝文本。
3. 工具无通道会话 → 仅登记 no_channel，store 可读。
4. 分流：构造 root 命中任务的 ChannelMessage → 不进 chat（session store 无新 chat session）、inbox 有序两条、同 msg_id 重送 Duplicate；archived → 拒收。
5. 未命中 root 的普通消息 → 走原 chat 路径（冒烟：mock adapter 收到 steer 侧行为——参照 handlers 现有测试套路）。
6. 卡 JSON 含 binding 三态与受理数；update_card 在 accept 后被调用（mock adapter 断言）。

Mock adapter：参照 `channels/platform/feishu_test.rs` 与现有 hub/handlers 测试的 adapter mock。

## 检查门槛

`cargo check -p kernel`、`cargo clippy -p kernel --all-features`（零新增警告）、`cargo fmt`、`cargo test -p kernel`（新增测试+存量全过）。中文注释；模块 doc 注明设计依据 N1/C1/C2/D2。

## 明确不做

CardKit/局部更新、停止/暂停/恢复按钮与回调、真实 Provider 创建（bind 路径已在增量1测试）、队列暂停与派发（P2）、重启去重凭据持久化（P6/N12）、wire/RPC 新方法。
