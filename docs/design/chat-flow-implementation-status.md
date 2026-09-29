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

### 增量 4：卡面控制（按钮/回调/事件刷新）—— ✅ 已验收（commit `c4011b32`），**P2 收口**

- 过程备注：实施 agent 中途被主人取消（两次），代码已基本写完；主 agent 直接接手验收——独立跑全量（1772 过/4 已知沙箱失败）、taskflow 16/16（含新增 6 场景）、clippy 零新增、fmt 过，逐文件审 diff 后提交。
- 改动：`cards/taskcard.rs` 改快照渲染（各状态如实区分，无合法操作不出按钮）；`taskcard/mod.rs` 加 `handle_exec_action`（C9 逐维重核：字段→开关→任务→代次→Run）；`taskcard/relay.rs` 事件驱动刷新（per-task 串行锁内重读快照渲染，事件只是提示；PATCH 失败只 warn；无卡不补）；`SimControl` 测试柄；hub `exec_` 前缀臂；分流臂受理后统一走快照刷新。
- 主人交代事项：git stash 已查——`git stash list` 为空（增量 2 验收时的 stash 当场 pop 干净）；fsck 的 dangling commits 是 stash/pop 残留，无害，随 gc 清理。

## P4：事实、结果与只读查询

### 增量 5：Run 事实/结果/只读查询（W2 内核）—— ✅ 已验收（commit `934e6a71`）

- 改动：migration v27（`exec_runs`/`exec_results`）、`exec/facts.rs`（503 行，insert-once/终态单向）、scheduler 事实钩子 + 结果上报收口（`AdapterNotice::Terminal/Result` 枚举泵）、卡面结果行经 relay 锁内读取、`tools/task_status.rs`（665 行，task_status 双形态 + task_result 按轮读，严格只读）。
- 验收（主 agent 独立复跑）：exec 29/29、task_status 3/3、facts_flow 1/1；全量 1779–1780 过、4 已知沙箱失败（hook 测试负载下偶发 flake 为第 5 个波动源，单跑过、基线同现）；clippy 65=基线；fmt 过。
- **已知待硬化（P3 协议层，记入下游集成要求）**：结果上报通道只带 native 身份，当前归属=「单 writer 轮序下最早无正文 Run」。某轮合法无正文（如无报告的失败轮）时，下一轮正文会被错配到该轮。正解=真实 adapter 上报必须回显 start_run 授予的 run 身份（C4「上报属于该轮的正文」的协议化）。
- 偏差（实施方记录，主 agent 复核认可）：`list_tasks_with_activity` 取 `Option<&str>`；`terminal()` 先补 `run_started`（INSERT OR IGNORE）消泵/派发写竞态；测试环境缺 libssl 开发符号链接，实施方用 `~/.local/lib/ssl-dev-shim` 用户级 shim 未动系统。

## P6：恢复与故障闭环

### 增量 6：重启核对/重送去重/空闲释放/开关收尾 —— ✅ 已验收（commit `67f269e6`）

- 改动：migration v28 `exec_acceptance`（受理凭据：是否开始/何时受理）；分流臂 C1 完整语义（进程内重送静默、跨进程重送撤回+「未恢复、未重新执行」、曾等待/已派发区分）；`boot_sweep`（未闭合 Run 标 interrupted 不伪造终态）+ hub 启动刷新活动卡；`ExecAdapter.release` + per-lane 代次计时释放（暂停有等待项同样释放、队列/暂停不动、恢复用原 native id）；开关语义修正（命中任务+flag off 明确拒收不掉回 chat）；SimControl.resume_fails。
- 验收（主 agent 独立复跑）：exec:: 40/40、taskflow 18/18；全量 1795 过、仅 4 已知沙箱失败；clippy 65=基线；fmt 过。
- **已知待硬化（P7）**：分流臂 受理→凭据写入→派发 之间存在微窗口——同任务终态竞态恰落窗口时，跨重启重送项理论上有被派发可能（需同消息并发+终态同微秒落窗，现实概率极低）。正解=受理/凭据/派发检查纳入同一 per-task 异步锁（lane 锁 tokio 化或独立接受锁），随真实 adapter 落地时一并做。
- 偏差（实施方记录，主 agent 复核认可）：facts 加 `latest_run()`（中断卡面需要读取 API）；重送回复按 acceptance_for 区分 started/waiting 两文案；boot_sweep 在 build_kernel 后置接线。

### 增量 7：Kimi ACP 真实契约测试接入 —— ✅ 已验收（commit `bca2a403`）

- 性质：通用测试接入（handoff 缺下游条款允许）；`exec/acp_harness.rs`（568 行，#[cfg(test)] ACP stdio 客户端 + AcpHarnessAdapter）+ 4 场景契约套件（#[ignore]+`YOMI_ACP_E2E=1` 门）。
- **主 agent 亲跑取证（kimi 2.1.0 真实调用，12.45s 4/4 过）**：
  - s1 创建→绑定→运行→事实：原生 session 绑定落库，正文 HARNESS_OK 保存归属该 run。
  - s2 真实取消：长 prompt 中途 stop_and_pause → `terminal_kind=cancelled`、run=Stopped、暂停保持；恢复后同 Session 续跑（RESUMED_OK）。
  - s3 释放→恢复：release 后新输入经 `session/load` 跨进程恢复原 Session，暗号 GIRAFFE-00532BC8 原样召回（D9 全链路实证）。
  - s4 两卡隔离：X 停止时 Y 继续运行 5s 无扰；Y 各自 cancel 生效（D11 实证）。
- **实证补记（强化下游要求）**：s2 中被取消轮无正文 → 下一轮结果按轮序规则错配到该轮 seq——chat-flow-downstream-integration §1.2 run 身份回显硬化要求的真实佐证，不再只是推断。
- 结论：**ExecAdapter 契约对真实双向 Provider 成立**（Kimi 路径）；Codex 路径待下游。

### 增量 8：L1 换代状态机/Markdown 导出/CardKit 构造 —— ✅ 已验收（commit `02825993`）

- 范围纪律：遵守 §7.2「P0 门槛未闭合不提前投入完整单卡产品化」——只建可验证部分；渲染 v2/局部更新/展开态待凭据。
- 改动：`channels/taskcard/renewal.rs`（382 行，纯函数决策 + 执行 + sweep/返回两驱动）；migration v29（card_sent_at/card_entity_created_at）；`exec/export.rs`（256 行，正文字节级一致、rename 幂等）；`feishu_cardkit.rs`（113 行，三方法构造、逐方法标未实测）。
- 验收（主 agent 独立复跑）：renewal 13/13、export 5/5、cardkit 3/3；全量 1817 过、仅 4 已知沙箱失败；clippy 65=基线；fmt 过。
- 偏差（实施方记录 + 主 agent 当日官方文档复核确认）：`batch_update`=POST、`element content`=PUT（原规格 PUT/PATCH 组合不存在）；v29 前存量行 `card_sent_at` NULL 回退 `created_at`（保守方向）。主 agent 追加修正：content 方法正名 `cardkit_element_content_update`（原名 patch 与官方 PATCH /elements 端点混淆）。

### 增量 9：受理/凭据/派发微窗口硬化 —— ✅ 已验收（commit `0a86d906`）

- 改动：`accept_input`（scheduler）收敛「查重→凭据→入队」为持 per-task `accept_lock` 单一方法，与 `try_dispatch` 取队段互斥；锁序 lanes 表锁→accept_lock→lane 锁，持锁段内零 await；`remove_queued` 删。
- 验证（实施方跑无锁矩阵、主 agent 复核）：锁-only revert→并发同 msg 重送重复判 NotRecovered 确定性失败；inc-6 窗口变体 revert→重送项被 terminal 重派并 mark_started（N12 违反复现）；恢复后全绿。场景 2 形状修正为「A 在飞+B 注入∥A 终态」（C3 单在飞下原形状打不到窗口，如实记录）。
- 主 agent 复跑：exec:: 47/47、taskflow 18/18、clippy 65=基线、fmt 过。
- 结论：增量 6 记录的 P7 硬化项关闭。

### 后续：增量 10（N5/C5 当前轮问答生命周期 + ACP 真实授权验证）—— 进行中
## harness-e2e 回归（工具表变更后，AGENTS.md 要求）—— ✅ 15/17，两失败均非回归

- 环境建设（容器 apt 源受限全录）：sqlite3 CLI 无包→`/root/.local/bin/sqlite3` python shim；libssl-dev/protobuf-compiler 无包→镜像站抽 `libssl-dev_3.5.7` 到 `/root/.local/ssl-dev`（arch 头文件合并）、`protoc 25.8` 到 `/root/.local/protoc`（均用户级，未动系统）；链接经 `OPENSSL_DYNAMIC=1`+shim 目录。**关键坑**：容器全局 env 有 `YOMI_EXTRA_SOCKET=ws://0.0.0.0:57231`（生产占用）——测试 daemon 必须 `env -u YOMI_EXTRA_SOCKET`，否则 extra 绑定失败触发「清主 socket 文件后退出」路径（daemon 假死后连接 ENOENT、重绑 EADDRINUSE 的根因）。
- 结果（隔离三件套 + `YOMI_DB`，debug 构建 `target/debug/yomi`）：15 过 2 失败。①verifier 未出 `VERDICT: ` 锚——jsonl 取证：子 agent 流程完整、结论正确但改写为「结论：**通过**」，模型格式 compliance flake，非代码回归；②kanban 建卡——`kb.py` 在本 pod 未安装（kanban skill 缺），纯环境缺口。
- 另注：增量 2 实施 agent 当时声称安装的 `pkg-config/libssl-dev/protobuf-compiler` 实际均未装上（apt 源不可达）——其门禁结果依赖 cargo 缓存，已在本轮全部补齐并复核。

## 未验证项与所需条件（滚动清单）

| 项 | 影响 | 所需条件 |
|---|---|---|
| fork 推送 | 无法交付可 review PR | GitHub token（12bitsD/yomi 写权限） |
| 飞书真卡实验 | P0 平台门槛、P5 验收 | 授权测试应用凭据 + 测试群 |
| Kimi 真实模型调用 | ~~ACP 契约实证~~ **已解**（本机 2.1.0 六项 probe 全过，见 P0） | — |
| Codex App Server | 双 Provider 之一全程 | 安装固定版本 + 凭据（下游 W3） |
| 下游 nika 接入 | W3/W5、Skill 与专用入口业务接线 | 下游仓库 checkout/权限 |
