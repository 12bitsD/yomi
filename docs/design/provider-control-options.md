# 任务卡接入 coding agent 的协议取舍

> 阅读定位：本文件保留截至 **2026-09-28** 的研究证据与候选比较；**2026-09-29 已完成方案收敛**，最终选择和开发契约以 [已确认技术方案](chat-flow-technical-design.md#final-review-decisions) 为准。本文历史候选不是新的待决策项；路线获批仍不等于能力或端到端效果已验证。

日期：2026-09-28。状态：用户已选择双向协议；Kimi 使用 ACP、Codex 使用 App Server，先固定版本验证再接卡片。尚未实现或做模型端到端验证。既有约束见 [需求基线](chat-flow-requirements.md) 与 [Session 生命周期](provider-session-lifecycle.md)。

完整流程、节点双方案和行为契约统一读 [技术评审稿](chat-flow-technical-design.md)；本文件保留协议选择的背景。新增首方核查见 [Provider 研究](chat-flow-evidence/provider-recovery.md)。

## 要解决的问题

卡片绑定固定 Provider 会话，原始 prompt 直接送入；后续多轮使用原历史，需要读取进度、停止当前执行、按需恢复。如果 coding agent 本身要求用户补充信息或授权，卡片必须能显式表示能力与当前状态，不能静默改成自动批准。

## 候选

| 路线 | 实现思路 | 收益 | 代价与效果边界 |
|---|---|---|---|
| CLI 非交互（未选） | 每个 Run 启动 CLI，解析 JSONL；保存精确会话 ID，下一轮用 resume/--session | 封装较小，进程生命周期直接；可验证生成、事件与恢复 | 面向预设权限的批处理；不能假设有完整双向审批/问答；停止依赖进程树管理及真实退出验收 |
| 各 Provider 原生双向协议（已选） | Kimi 使用 ACP，Codex 使用 App Server stdio；adapter 统一上层输入/事件/控制契约，保留能力差异 | 能明确关联会话、执行轮、进度和当前轮的待用户响应；适合卡片内持续互动 | 多做请求关联、断线、版本适配、能力探测及回复幂等；两端能力不完全等价，Codex 存在实验性限制 |

这里的 Provider 指 Kimi Code/Codex 执行引擎，不是模型供应商。统一 adapter 不意味着把两者伪装成同一个协议，也不引入卡内切换 Provider。

## 工作目录职责边界（用户纠正）

工作目录、Git 分支及是否使用 worktree，由用户/接入配置通过 Codex、Kimi Code 已有机制指定。yomi 不负责创建、合并、清理 worktree，也不需要先决定代码隔离策略才能实现任务卡接入。adapter 只保存和使用 Provider 恢复会话所需参数，例如原生会话 ID 以及必要的 cwd；不另建环境管理系统。

跨重启恢复仍要保留 Provider 历史、卡片绑定与原目录，这由接入/部署满足。目录或历史缺失时明确报错，不静默换目录或新建空会话。独立 Session 不自动保证文件隔离，原生协议支持指定 cwd 也不等于自动创建 worktree。

此前 Agent 建议每卡独立 worktree，是未采纳的候选；现撤回该必选架构问题。来源：[用户范围纠正](chat-flow-context.md#decisions-before-review)。

## 证据与限制

- Codex 官方 [App Server](https://developers.openai.com/codex/app-server/) 支持 `thread/read`、`thread/resume`、`turn/start`、`turn/interrupt`、命令/文件审批以及用户输入回调。`thread/read` 不启动模型；resume 后仍需 turn/start 才执行。应持久绑定原生 `threadId`，不要混用 nika 自有 Session/Run ID 或其他 session 字段。
- Codex 官方 [Non-interactive mode](https://developers.openai.com/codex/noninteractive/) 支持 `exec --json` / `exec resume <ID>`；本轮资料没有提供与 App Server 等价的双向审批/问答协议。两种路线均需保留历史，禁止 ephemeral，不用 `--last` 关联任务卡。
- 本轮读取的 App Server 文档含“app-server command and WebSocket transport ... experimental ... not supported for production workloads”说明。首版建议先用本地 stdio 固定版本做接入验证；生成匹配版本的 schema、逐项确认实验字段，不能当成无风险稳定生产接口。
- App Server 当前文档描述最后订阅者 unsubscribe 后，无 thread 活动 30 分钟才卸载；共享服务继续存在。因此“空闲可释放资源”不等于“每轮结束立即清空全部内存”。最终 N4 已选择每个活动 Session 独立进程，N10 按短空闲期释放；共享服务是未选历史候选，不把其原生卸载时延混作已选方案效果。
- Codex `turn/interrupt` 的响应只是请求受理；需等该 turn 的终态事件，并核对关联工具实际停止范围。点击停止不能直接显示全停。
- Kimi Code 2.0.2 对应的 [官方 ACP 文档](https://github.com/MoonshotAI/kimi-code/blob/%40moonshot-ai/kimi-code%402.0.2/docs/en/reference/kimi-acp.md) 声明支持 new/load/resume/close；load 带历史 replay，resume 不 replay；close 取消 in-flight 并释放 live resources，delete 才删除历史。接入需按版本和能力声明验证，不能假设任意 ACP client/server 均提供这些方法。
- Kimi ACP 支持 `session/request_permission`；多问题/多选交互依赖客户端 elicitation.form 能力，否则会降级。2.0.2 问答桥接尚不支持 Other 自由文本，不能承诺任意表单输入。print 路径会强制 auto 权限，见 [版本源码](https://github.com/MoonshotAI/kimi-code/blob/9d07f634be94ebeb1deba2f55d247807cf729315/apps/kimi-code/src/cli/v2/run-v2-print.ts#L407)，不能把 stream-json 单向输出等同于完整卡片交互。
- Kimi `session/cancel` 是 notification，不能视为已停；应等原 `session/prompt` 返回 cancelled，且委派/后台进程停止仍需 E2E 核验。其 ACP 同 Session 忙时拒绝第二个 prompt，所以上层必须串行投递；见 [session 实现](https://github.com/MoonshotAI/kimi-code/blob/9d07f634be94ebeb1deba2f55d247807cf729315/packages/acp-server/src/session.ts#L621)。

## 双向接入的输入语义

已确认 D7 约束用户主动追加的 prompt：排为后续 Run。双向接入须区分 coding agent 自己提出、当前 Run 正在等待的回答或授权：通过 pending request 的 ID 原样回复当前 Run，而不是进入后续队列。否则当前 Run 等待回答、回答又等待当前 Run 结束，会形成死锁。该区分属于已选协议回调的正确接入，不是改变默认追加排队行为。

卡片交互应携带对应 task/run/request ID；过期回答不能被当成新 prompt 或转给下一轮。停止、超时和重启后旧请求不应自动批准，须以 Provider 的最新状态判断是否仍可回答。这是正确关联响应的设计要求，不额外引入业务审批流程。

## 验证范围与下一步

按已选路线先做固定版本的最小 adapter 验证：创建/恢复同一会话、一次新输入、实时事件、当前轮提问/授权响应、停止确认、断线和重启后恢复。历史 replay 不重复投递结果或控制命令；事件重送及投递重试不得制造冲突结果、让旧事件覆盖新状态或额外创建结果卡。通过后才承诺卡片端到端效果；不支持时明确能力与替代方案，禁止默默降级到自动批准或新建空 Session。

确认来源：[用户选择](chat-flow-context.md#decisions-before-review)，对应需求 D10。执行次序：先设计协议请求/事件契约与可检查的验证步骤，后做最小接入验证，再接任务卡。整轮方案已于 2026-09-29 完成对齐；开发和验收按主方案 P0–P7 推进，本研究记录不构成额外的确认门槛。

任务卡的队列及停止/暂停/恢复范围已按需求 D11 确定为本卡：控制请求与事件必须关联目标 card/session/run，不能影响另一张卡；即使使用同一 Provider 或共享服务也成立。该产品约束本身不规定进程模型；最终 N4 另已选择活动 Session 独立进程，仍不承诺文件隔离或无限并发。

结果呈现最终按修订后的需求 D12 采用每任务一张当前有效主卡：同代持续更新状态、结果及控制，跨期限按 L1 换代，并验证同代刷新保留展开态；不按每轮新增独立结果卡。跨轮保持同一任务和 Provider Session 关联，完整结果回读和超限入口仍需实现、验收。此前多结果卡兜底已成为历史备选，不自动启用；单卡无法满足时先报告具体限制。

最终技术方案已给出资源并发限制与跨卡调度、活动 Session 独立进程及短空闲释放、卡片问答入口、状态及回读的实现逻辑、取舍和验证方式。产品语义及方案选择已收束；实施时只有新证据表明无法满足已定行为，才需报告具体影响并重新评审。工作目录与 worktree 策略交给接入方及 Provider；资源调度不改变按卡控制范围，也不宣称提供文件或外部资源隔离。
