# chat-flow 实施状态记录

接手：2026-09-29，云端环境（nika pod）。基线：`feat/chat-flow-impl` 自交接提交 `924b05a8`（= `a8566269` 源码 + 交接文档）创建。本文按 P0–P7 持续记录：对应需求/节点/契约、实际改动、检查命令与结果、证据位置、仍欠验收。权威需求与方案见 [requirements](chat-flow-requirements.md) / [technical-design](chat-flow-technical-design.md)，本文不覆盖其中已确认选择。

## 环境核查（2026-09-29）

| 项 | 结果 | 证据/说明 |
|---|---|---|
| 仓库与分支 | ✅ | upstream `Crescent617/yomi`；交接提交 `924b05a8` 经 upstream `refs/pull/8/head` 取得；实施分支 `feat/chat-flow-impl` 本地已建 |
| fork push | ⛔ 阻塞 | 需 `12bitsD/yomi` 推送凭据；pod 无 GitHub token，SSH 22 端口不通，HTTPS 可克隆 |
| Rust 工具链 | ✅ | rustup stable 1.98.1 + just 1.42.4（rsproxy.cn 镜像；官方源 ~25KB/s 不可用），crates 走 rsproxy sparse |
| 基线构建 | 🔶 进行中 | 首次 `cargo check` 拉全 workspace 依赖中 |
| Kimi Code | ✅ 2.1.0 | `kimi acp` 可用；free-tokens provider 已配置，真实模型调用验证通过（见 P0） |
| Codex CLI | ⛔ 缺失 | 未安装；App Server 接入属 W3（下游/nika 侧），版本固定待下游决定 |
| 飞书测试凭据 | ⛔ 阻塞 | P0 真卡实验需用户授权的测试应用与目标群；pod 内 nika 生产 bot 凭据不作为默认测试目标（handoff 验证门槛） |
| 下游（nika）接入仓库 | ⛔ 无 checkout | nika 仓仅部署骨架（Dockerfile/chart），无 Adapter 代码；W3 按「缺下游访问」条款推进 |

## 代码核查（对照 evidence 文档，基线 `a8566269`）

逐项复核研究证据与当前代码一致性：

| 证据声明 | 复核结果 | 位置 |
|---|---|---|
| 普通飞书消息经 steer 入口（含手动 Thread） | ✅ 一致 | `handlers.rs:159/193/845` send_steer |
| mailbox steer/normal 为优先级双队列，Streaming 前批量注入 steer | ✅ 一致 | `comms/mailbox.rs:9-17` 模块 doc 明示 |
| conductor 逐条 spawn、含异步图片准备 | ✅ 一致 | `kernel/conductor.rs` intake 循环 |
| 现有 stop 先关 intake、停活跃 run、再 shutdown；旧 cancel 语义保留必要 | ✅ 一致 | `kernel/mod.rs:652` stop() 三段式 |
| 停止按钮回调只传 Session | ✅ 一致 | `render/obs.rs` act_stop 回调值构造 |
| 映射指向已删 Session 时删除映射并自动新建 | ✅ 一致 | `hub/routing.rs:398-414` dangling-mapping guard（任务恢复路径必须绕开） |
| event Envelope 仅 session_id+event_id，无 Run 身份 | ✅ 一致 | `event/mod.rs:6-20` |
| 事件订阅有界、Stopped 后清 replay buffer | ✅ 一致 | `server/dispatcher.rs`、`comms/bus.rs` |
| send_message 30,000 字节截断 | ✅ 一致 | `platform/feishu.rs:848` |
| reply.rs 28,000 字节结果预算 | ✅ 一致 | `render/reply.rs:30` |
| 卡片正文提取跳过折叠面板 | ✅ 一致 | `platform/feishu_text.rs:141` extract_card_text |
| Justfile 存在（大写 J），ci = check+lint+test+fmt-check | ✅ 一致 | `Justfile` |

结论：交接文档的源码研究证据与实施基线完全一致，无漂移。

## P0：契约与平台验证

状态：🔶 进行中。

- [x] C1–C9 与术语基线：随交接文档冻结（technical-design §6）。
- [ ] CardKit 展开态/同代多轮/L1 换代/原 Thread 路由真卡实验 —— **阻塞**：需授权测试应用与群。
- [x] Kimi ACP 最小实验（2.1.0，本机真实模型调用）—— 六项全过，证据 `/tmp/acp_probe*.py` + `/tmp/acp-probe-results.json`：
  - `initialize`：protocolVersion 1；能力=loadSession✓、session list/resume/close/delete/fork/additionalDirectories✓、prompt image✓/audio✗/embeddedContext✓、mcp http+sse✓。
  - `session/new`：返回原生 sessionId 与 configOptions（当前模型）。
  - `session/prompt`：真实模型应答，`stopReason=end_turn`。
  - `session/load`：原 sessionId 成功载入（恢复路径存在）。
  - `session/request_permission`（ask 模式）：工具调用触发授权请求，options 含 allow_once/allow_always 类，客户端回 selected 后执行继续。
  - `session/cancel`（**通知**，非请求——probe2 误作请求发出不生效即为证据）：sleep 60 中途取消，0.1s 内返回 `stopReason=cancelled`。
  - **跨进程恢复（内容级）**：进程 A teach 暗号 AMETHYST-7391 → 杀进程 → 进程 B `session/load` 原 sessionId → 召回返回原文暗号。D6/N4/N10 的「释放进程、后续恢复原 Session」在 2.1.0 实证成立（证据 probe4/5）。
  - **并发 prompt**：2.1.0 协议层**不拒绝**同 Session 并发 prompt（两个 prompt 均返回 end_turn；2.0.2 研究记录为 631 行起拒绝——版本差异，行为倒退或语义改变）。**单写入保证必须由 yomi 队列侧强制**，不能依赖 Provider 拒绝。
  - 待补：question form 交互、load 时 replay 行为（本次 load 未观察到 replay 通知轰炸，与「resume 不 replay」一致但未严格取证）、close/delete 语义、外部写入检测。版本注意：研究基线固定 2.0.2，本机 2.1.0 能力集有增（resume/close/fork）且并发拒绝行为不同，下游固定版本时需重新核对。
- [ ] Codex App Server 最小实验 —— **阻塞**：无 CLI，属 W3（下游/nika 侧），版本固定待下游决定。
- [ ] 能力矩阵与失败证据成文（Kimi 行已具雏形，Codex/飞书行待 unblock）。

## P1：身份与直达输入

### 增量 1：执行任务登记核心（exec registry）—— ✅ 已验收（commit `e70e6229`）

- 改动：`crates/kernel/src/exec/`（mod 254 行 + store 289 行 + 测试 232 行）；migration v26 `add_exec_tasks`；装配经 `StorageSet`，`Kernel::new` 签名不变；`define_id!(ExecTaskId => "task_")`。
- 对应契约：C2（分步创建、dedup 不扩大副作用）、N2（Uninitialized/Bound/Broken 三态，Broken 拒绝绑定不自动重建）、D8（归档不删行）。
- 验收证据（主 agent 独立复跑）：`cargo test -p kernel exec::` 11/11 过（含并发绑定的状态守卫 UPDATE、dedup 竞争 ON CONFLICT）；`cargo clippy -p kernel --all-features` 新文件零警告（65 个为存量，未变）；`cargo fmt --check` 过。
- 规格：`docs/design/chat-flow-impl/inc-1-spec.md`；偏差记录：mark_broken/archive 对已终态幂等无操作（规格未定义，选可重试安全）；时间戳显式 `Utc::now()` 绑定。
- 备注：实施 agent 为编译补装了容器系统包 `pkg-config`/`libssl-dev`/`protobuf-compiler`（构建前置，非代码改动）。

### 增量 2：通道接线（双入口 + Thread 分流 + 受理登记 + 占位卡）—— ✅ 已验收（commit `57aa047c`）

- 测试：新增 13 全过（taskflow 7 / inbox 3 / store 1 / card 2）；全量 1747 过、4 失败（`kill_tree_reaps_whole_group` 等进程组语义）经主 agent 在 stash 纯净基线独立复现——存量沙箱问题，与本增量无关；clippy 65=基线零新增；fmt 过。
- 偏差（实施方记录，主 agent 复核认可）：①`create_and_announce` 去掉冗余 `channel_name` 参数；②工具→Kernel 通路按 `cron_scheduler` slot 先例加 `Weak<Kernel>` 回指；③slash 无消息 id 时 dedup 用一次性键、工具 `created_by="session:{id}"`、无通道占位 `channel_name="skill"`。

## P2：按卡运行与控制

### 增量 3：运行控制核心（scheduler + adapter 接缝）—— ✅ 已验收（commit `cd03012b`）

- 改动：`exec/scheduler.rs`（615 行，lane Mutex 唯一裁定 + Semaphore 名额 + terminal_pump/sweep）、`exec/adapter.rs`（ExecAdapter trait + SimAdapter 默认挂起）、`exec/run.rs`；`ExecInbox` 改 `TaskInbox{queue,seen,last_seq}` 修 pop 破坏去重；通道 `exec_tasks` 开关默认 false gate 双臂+工具；`[exec]` 配置段（max_concurrent_runs=2、stop_confirm_timeout_secs=30）。
- 对应契约：N6（锁内裁定/暂停先于网络/受阻不预约续跑）、C3（五条件）、C6（Stopping≠Stopped、Mismatch 不取消新 Run、自然终态如实保存）、N2（首派发前绑定）、D3（不回队）、N3（业务失败不自动暂停、未知阻断）。
- 验收（主 agent 独立复跑）：`cargo test -p kernel exec::` 25/25 过（9 场景映射见 inc-3-spec 与 scheduler_test）；feature 三组合 check 过（`--no-default-features` 1 个 dead_code 警告为基线存量 ef1ec657，非本次）；clippy 零新增；fmt 过。
- 偏差（实施方记录，主 agent 复核认可）：场景 9（开关）置 taskflow_test（需 hub MockAdapter 测试台）；start 失败留 Unknown 且名额不释放（防超发）；RunMismatch 保持 paused（先暂停后核对）。

### 增量 4：卡面控制（按钮/回调/事件刷新）—— 设计中

## 未验证项与所需条件（滚动清单）

| 项 | 影响 | 所需条件 |
|---|---|---|
| fork 推送 | 无法交付可 review PR | GitHub token（12bitsD/yomi 写权限） |
| 飞书真卡实验 | P0 平台门槛、P5 验收 | 授权测试应用凭据 + 测试群 |
| Kimi 真实模型调用 | ~~ACP 契约实证~~ **已解**（本机 2.1.0 六项 probe 全过，见 P0） | — |
| Codex App Server | 双 Provider 之一全程 | 安装固定版本 + 凭据（下游 W3） |
| 下游 nika 接入 | W3/W5、Skill 与专用入口业务接线 | 下游仓库 checkout/权限 |
