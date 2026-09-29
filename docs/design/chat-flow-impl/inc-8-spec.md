# 增量 8 实施规格：L1 换代状态机 + Markdown 导出 + CardKit 请求构造（P5 通用内核）

目标：在真卡凭据缺失下，只建 P5 中**可验证、不依赖平台行为假设**的部分。明确遵守技术方案 §7.2「P0 平台门槛未闭合时不提前投入完整单卡产品化」——本增量不含渲染 v2/局部更新/展开态（那些由平台行为主导，凭据到位后再做）。

## 组件 1：L1 换代状态机（`channels/taskcard/renewal.rs`）

依据 technical-design §8/L1 与 D12/C2/C9：每任务一张当前有效主卡；活跃/待回答任务在最早适用更新期限前换代；空闲任务在用户返回时换代；新卡确认可定位后才切换当前映射；发送不确定不制造两张有效卡；换代不触发执行、不解暂停、不换 Task/Session。

- 纯函数策略 `renewal_decision(task, lane: &LaneSnapshot, now) -> RenewalDecision`：
  - 期限常量：消息更新 14 天、CardKit 实体 14 天（取较早者；实体创建时间存于 task meta？——exec_tasks 表无该列：用 `card_sent_at`。本增量在 store 加列？不加表列——把发卡时间存进 `exec_tasks` 现有结构需迁移。**决定**：migration v29 加 `card_sent_at DATETIME`、`card_entity_created_at DATETIME NULL`（CardKit 实体启用后回填）。
  - `RenewalDecision::{Noop, RenewSoon{reason}, RenewOnReturn}`：
    - 无卡（CardPending）→ Noop（普通投递失败不擅自补卡，C9）。
    - 有活跃 Run（Running/Stopping/WaitingRequest 预留）或 paused 且队列非空 → 距期限 < `exec.card_renew_margin_secs`（默认 36h）→ RenewSoon。
    - 空闲（无活跃 Run、队列空）→ 已过期 → RenewOnReturn（用户下次输入时执行）；未过期 → Noop。
- 执行 `renew_master_card(kernel, instances, patches, task_id) -> RenewOutcome`：
  1. 用当前快照渲染新卡 → **优先在原 Thread 发布**（`send_card` 到 chat，锚=旧 thread_root？飞书 Thread 内发卡用 reply 到 thread_root——按 adapter 现有 send 能力实现：reply 到旧卡所在 thread）；
  2. 平台回执拿到新 msg id（确认可定位）→ `store.set_thread_and_card` 更新 + `card_generation+1`（store 加 `bump_card_generation(id, thread_root, card_msg_id, sent_at)`——**先确认后切换**，C2）；
  3. 发送失败/无 id → 保留原映射如实返回 `SendUncertain`，不重试不发第二张（C9）；
  4. 换代只换呈现：不动 inbox/lane/binding/Run（D12 明确）；`RenewOutcome::{Renewed{new_msg_id,gen}, SendUncertain{error}, Noop}`。
  - 旧卡控制失效已由回调 gen 重核覆盖（增量 4）；旧卡标注新入口：尝试 `update_card(旧卡, 指向新卡的提示)`，失败不依赖（过期后不可写属预期，L1）。
- 周期驱动：hub relay 同进程加低频 sweep（每 30 分钟，`exec.card_renew_sweep_secs`）对 exec_tasks 通道的活动任务跑 renewal_decision + 执行 RenewSoon；RenewOnReturn 在分流臂 accept 前检查执行。
- Mock 测试（纯函数+MockAdapter）：
  1. 活跃任务临期 → RenewSoon → 新卡发原 Thread、gen+1、旧 gen 回调被拒；
  2. 空闲未过期 → Noop；空闲已过期 → 无自动换代，用户输入时 RenewOnReturn 先换再受理；
  3. 发送失败 → SendUncertain、映射不动、无第二张卡；
  4. 换代前后 inbox/lane/binding/current Run 逐项相等（不触发执行不解暂停）。

## 组件 2：超限 Markdown 导出（`exec/export.rs`，N9 首版交付点）

- `export_result_markdown(facts, run_id, dir) -> Result<PathBuf>`：读权威正文 → 写 `<data_dir>/exec-results/<task_id>/<run_id>.md`（归属头：task/run/seq/provider/native_session_id/时间 + 分隔线 + **原始正文一字不改**）；已存在同路径不覆盖（先写临时名再 rename，幂等）。
- 超限时机的首版接线：`task_result` 工具输出超 `max_tool_output_length` 时自动导出并在输出中给路径（正文仍按截断约定给头部）——工具用户立即可读全文，不依赖飞书附件（附件上传属真卡链路，凭据到位后接同一导出产物）。
- 导出失败与正文保存分开报错（N9：附件未成功不得显示可访问；本增量无卡面入口，工具输出如实）。
- 测试：超长中文/代码块正文导出字节级一致；重复导出不覆盖；正文缺失明确报错；task_result 超限路径含导出路径与头部截断。

## 组件 3：CardKit 请求构造（`channels/platform/feishu_cardkit.rs`，纯构造+mock）

- 为 feishu adapter 加方法（仅构造+发送经既有 request 助手，**真实调用未验证、逐方法标注**）：
  - `cardkit_create(card_json) -> Request`（POST `/open-apis/cardkit/v1/cards`，body 构造）；
  - `cardkit_batch_update(card_id, updates, sequence, uuid)`（PUT `.../cards/{id}/batch_update`，sequence 递增与 uuid 幂等字段构造）；
  - `cardkit_element_patch(card_id, element_id, content, sequence)`（PATCH `.../cards/{id}/elements/{element_id}/content`）。
- 单测只验证请求构造（URL/method/body/字段），用既有 feishu_test mock 套路捕获请求；**不发真实请求**。
- 本增量不接线任何调用方（渲染 v2 凭据到位后再接）；模块 doc 写明「API 存在性有官方文档，行为未实测（P0 门槛）」。

## 组件 4：迁移与配置

- migration v29：`exec_tasks` 加 `card_sent_at DATETIME`、`card_entity_created_at DATETIME`（NULL）；发卡/换代时回填 `card_sent_at`（create_and_announce、renew）。
- `[exec]` 加 `card_renew_margin_secs=129600`（36h）、`card_renew_sweep_secs=1800`（30min）；CONFIG.md 同步。

## 检查门槛

同前（check 三组合、clippy 零新增、fmt、全量绿除 4 沙箱存量）。

## 明确不做

渲染 v2/稳定区/局部更新/展开态（平台行为主导，凭据后）、CardKit 真实调用与实体期限实测、飞书附件上传（真卡链路）、原 Thread 换代真机路由实测、换代提醒卡以外的通知面。
