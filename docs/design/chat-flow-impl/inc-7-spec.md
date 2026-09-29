# 增量 7 实施规格：Kimi ACP 契约测试接入（真实 Provider 验证）

目标：在缺下游的条件下，用真实 `kimi acp` 验证 ExecAdapter 契约——证明调度器/绑定/事实/取消/恢复语义对真实双向协议成立，而不是只对 SimAdapter 成立。性质=通用测试接入（handoff 允许：公共仓可承载不含业务配置的测试工具）；生产 ACP adapter 仍属下游 W3。

## 组件 1：ACP stdio 最小客户端（`exec/acp_harness.rs`，`#[cfg(test)]`）

-  spawn `kimi acp`（子进程 stdin/stdout 行分隔 JSON-RPC 2.0）；请求 id 递增、响应按 id 路由、agent→client 请求（request_permission）自动回 allow_once、通知收集。
- 方法封装：`initialize` / `session_new(cwd)` / `prompt(session_id, text)` / `cancel(session_id)`（**通知**，无 id）/ `load(session_id, cwd)` / 进程终止。
- 环境要求：PATH 有 `kimi`；无则测试跳过（见组件 3 guard）。

## 组件 2：`AcpHarnessAdapter`（同文件，实现 ExecAdapter）

- `create_session`：`initialize`（每进程一次）+ `session/new` → 返回原生 sessionId；进程句柄存入 `DashMap<native_id, AcpProcess>`。
- `start_run(native_id, input)`：同步校验会话在 → spawn 后台任务跑 `prompt`，完成后经 sink 依次报 `Result(正文)`（有正文时）与 `Terminal(Completed)`；prompt 返回错误/进程死 → `Terminal(Failed)`。同步 ack 立即返回 Ok（原生已受理）。
- `cancel(native_id)`：发 `session/cancel` 通知 → Ok。prompt 任务收到 `stopReason=cancelled` → 报 `Terminal(Cancelled)`。
- `release(native_id)`：终止 ACP 进程并移出句柄表（历史由 kimi 自身数据目录保留）；Ok。
- resume：`start_run` 时若句柄表无此 native_id → 新起 ACP 进程 + `session/load(native_id)` 恢复（证据=D9 跨进程恢复），再 prompt。
- 工作目录：每任务 `tempdir()/acp-harness/<task>`；kimi 配置用全局 `~/.kimi-code/config.toml`（free-tokens 已配好，permission_mode=auto 不触发授权）。

## 组件 3：契约套件（`exec/acp_harness_test.rs`，`#[ignore]` + env 门 `YOMI_ACP_E2E=1`）

统一 guard：无 env 或无 `kimi` 二进制 → 打印原因并返回（视为跳过而非失败）。用**真实 ExecScheduler**（SqliteExecFactStore 内存库 + AcpHarnessAdapter + terminal_pump）驱动：

1. **创建→绑定→运行→事实**：建任务（Uninitialized）→ inbox accept → try_dispatch → 断言 bind 持久化（原生 id 非空）、facts run_started 行、最终 Terminal(Completed)、结果正文已保存且归属该 run（ResultPublished）。
2. **真实取消**：长 prompt（`sleep 60 && echo hi`）→ Running 后 stop_and_pause → 断言 Terminal(Cancelled)、run=Stopped、暂停保持；resume → BlockedStopUnconfirmed 不出现（已确认）→ 实际恢复派发下一条。
3. **释放→恢复原 Session**：跑轮 1（教暗号）→ terminal → 手动 `release` → 新输入 accept+dispatch → 断言同一 native id、轮 2 prompt 召回暗号内容（正文含暗号）。
4. **两卡隔离**：X/Y 两任务同 provider 各跑长 prompt → stop X → X Stopped；Y 在 5s 后仍 Running（未被误停）→ cancel Y 清理。

模型调用保持极小（reply-exactly 型）；每场景硬超时 180s，超时即失败并留进程清理。

## 组件 4：默认 CI 不受影响

- `#[ignore]` 测试不进 `cargo test` 默认集；guard 保证无 env 时 `--ignored` 也只打跳过信息。
- clippy/fmt/check 三组合照常过（harness 在 `#[cfg(test)]` 下也要过 clippy）。

## 验收方式

主 agent 亲自跑：`YOMI_ACP_E2E=1 cargo test -p kernel acp_harness -- --ignored --nocapture`，四场景逐个看断言与耗时；证据记入状态文档（输出摘录）。

## 明确不做

生产 ACP adapter（下游 W3）、Codex 任何接入、当前轮问答（N5）、trait 的 run 身份回显扩展（随首个生产 adapter 落地，见 chat-flow-downstream-integration.md §1.2）、`kimi` 进程池/复用优化。
