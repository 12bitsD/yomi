# 任务卡绑定 Session 的保留与恢复

> 阅读定位：本文件保留截至 **2026-09-28** 的研究证据与候选比较；**2026-09-29 已完成方案收敛**，最终选择和开发契约以 [已确认技术方案](chat-flow-technical-design.md#final-review-decisions) 为准。本文历史候选不是新的待决策项；路线获批仍不等于能力或端到端效果已验证。

日期：2026-09-28。状态：生命周期及保留/恢复边界已确认，包括不自动到期删除、保留原数据卷和目录时跨普通重启/部署升级主动恢复。可行性已由官方文档、CLI help 和版本源码核对，未实现、未做真实 Provider 端到端验收。

具体开发流程与行为契约统一读 [技术评审稿](chat-flow-technical-design.md)。本文的原生 Session 保留目标不证明旧飞书卡片可永久更新；平台期限已通过 L1 产品选择处理：每任务一张当前有效主卡，有效期内跨轮复用、跨期限换代，旧卡保留历史；具体契约见该方案 §8，平台效果仍待验证。

## 用户意图与已定边界

- 一张任务卡绑定 Kimi Code 或 Codex 的一个 Session；首版不切换 Provider。
- 任务 Thread 内的原始 prompt 直接送到绑定 Session，普通聊天只读任务进度。
- 用户已确认：本轮执行完成、离开卡片后，再回到同一卡片发 prompt，恢复原 Session；“结束 Run”“释放运行实例”和“删除历史”是不同动作。
- 旧表述“任务完成，卡片及 Session 一起结束”已由本次生命周期选择取代：卡片显示“本轮已完成，可继续”，归档只收起入口；历史/工作区不自动过期删除，普通重启和部署升级后可从原任务入口主动恢复；入口过期按 L1 换代。归档与清理按最终 N10 开发。

## 已核实的能力

| 后端 | 恢复能力 | 证据与限制 |
|---|---|---|
| Codex，本机 CLI 0.158.0-alpha.2.1 | `codex exec resume <SESSION_ID> <PROMPT>` 可追加新一轮；App Server 的 `thread/resume` 后可 `turn/start`，`thread/read` 可不恢复就读历史 | 官方 noninteractive/app-server 文档及本机 help；不使用 `--ephemeral`。生产目标版本需固定并复验；本轮未启动模型 |
| Kimi Code，本机 2.0.2 | 在原工作目录用 `kimi --session <SESSION_ID> -p <PROMPT> --output-format stream-json` 续接 | 版本源码确认恢复后提交 prompt；cwd 必须与保存值一致。print 模式会设置 auto 权限，该命令只证明恢复能力，不能据此确定生产交互审批接入 |

Codex 官方：https://developers.openai.com/codex/noninteractive/ 、https://developers.openai.com/codex/app-server/ 。

Kimi 官方：https://moonshotai.github.io/kimi-code/en/guides/sessions 、https://moonshotai.github.io/kimi-code/en/configuration/data-locations 。

Kimi 精确版本源码：[run-v2-print.ts](https://github.com/MoonshotAI/kimi-code/blob/9d07f634be94ebeb1deba2f55d247807cf729315/apps/kimi-code/src/cli/v2/run-v2-print.ts#L420)（420–445 恢复/cwd，213–240 收尾/flush，565–582 新 prompt）；[prompt-render.ts](https://github.com/MoonshotAI/kimi-code/blob/9d07f634be94ebeb1deba2f55d247807cf729315/apps/kimi-code/src/cli/prompt-render.ts#L390)（正常结束的 session.resume_hint）。首次运行异常退出前能否稳定取得 Session ID 待验证。

## 已确认的生命周期

1. 先登记任务、卡片与 Thread；首次取得执行资格后创建 Provider Session，持久保存 Provider、Session ID、执行主机/数据位置和原目录的关联，再发送第一条 prompt。
2. 每个活动 Session 使用独立 Provider 进程。Run 完成并确认历史/结果保存后，相邻且有执行资格的下一项可复用；否则按短空闲期释放实例。暂停队列仍由同一 yomi 进程保留，释放不解除暂停。按修订 D12，同代任务卡跨轮复用，期限到达时按 L1 换代，不为每轮结束另发独立结果卡。
3. 再次打开/查看卡片只读取状态，不启动模型。用户发新 prompt 后，按既有调度门禁受理；获得执行资格才恢复同一 Session 并建立新 Run。
4. 当前 Run 仍执行时，追加 prompt 排为后续 Run，不隐式打断当前执行；同一个 Provider Session 同时只有一个写入者，不并发调用 resume。
5. 本卡队列处于暂停时，发给本卡的新 prompt 仍排队，不能借 resume 绕过暂停；恢复本卡队列仍不自动重试已停止的 A。停止、暂停和恢复不改变其他卡的运行或暂停状态，普通聊天继续；对应 D11。
6. 归档仅收起入口；删除历史/工作区才可能破坏恢复能力。若历史不可用，应明确失败，不能静默新建空 Session 冒充恢复成功。

## 代价与可承诺效果

| 保留对象 | 代价 | 效果边界 |
|---|---|---|
| 会话历史与卡片映射 | 磁盘、元数据维护；日志可能随任务增长 | 空闲保留历史本身不发模型请求；不需要每张卡常驻一个 CLI |
| 工作目录与产物 | 仓库、依赖、构建产物或 worktree 的磁盘成本 | Kimi 2.0.2 print resume 要求原 cwd；历史不会恢复已删除代码或旧文件快照 |
| 下次恢复 | 启动/载入延迟，下一轮上下文 token；可能涉及历史压缩 | 保留 Provider 支持的对话上下文，不保证无限逐字上下文、零延迟或零成本 |
| 常驻执行实例（备选） | 每个实例持续占内存、进程及连接资源 | 可少一些启动成本，但本产品无需依赖常驻实例来保留 Session |

Resume 不恢复被终止的 shell、测试进程或模型生成现场；它利用已保存的对话记录开始新一轮。停止状态仍须以实际委派执行退出为依据。

## 与 D2 的分工及恢复承诺

需求 D2 仅限制待执行队列及暂停状态跨 yomi 队列所属进程的保留承诺，不禁止 Provider 自身保存历史。已确认的 D9 要求部署升级后仍能从旧卡继续，因此必须分别持久化卡片映射、Provider 历史及 workspace；只保存 Session ID 不足以保证恢复。恢复对话不意味着重启后自动执行旧队列。

下游接入方负责为绑定、结果、Provider 历史和原工作目录提供跨重启保留条件，并固定受支持的 Provider 版本。已有存储条件不证明恢复功能已完成；仍需补持久绑定、原目录/历史校验、两个 Provider 的运行时与凭据接线，以及真实重建/升级恢复验收。公开仓库仅维护这些责任与能力缺口，见 [下游接入边界](chat-flow-context.md#downstream-integration)，不依赖私有部署模板作为云端开发输入。

用户已确认：首版不自动到期删除历史/关联工作区，由明确清理操作处理；支持在保留原持久卷和工作目录的普通重建/部署升级后主动续接旧卡，但不恢复旧内存队列或暂停状态，不自动继续中断的 Run。持久卷损毁/删除与任意不兼容 Provider 升级不属于该恢复承诺；所支持的升级路径须做兼容与恢复验收。

生命周期和保留分支已对齐；Provider 协议已选择 Kimi ACP / Codex App Server 双向接入。归档/清理、真实取消、审批、附件能力、首次异常的 ID 恢复以及资源并发与调度均按最终技术方案开发和验收；队列及停止/暂停/恢复范围按 D11 已确定为本任务。用户已纠正工作区职责：目录/分支/worktree 在 Provider 与接入配置中指定，yomi 不管理 Git 工作区；adapter 只保存恢复所需参数。既有 D8/D9 要求接入/部署保留原目录，不因此扩展为 yomi 的文件隔离或清理功能。

已选协议及证据见 [Provider 接入协议取舍](provider-control-options.md)。最终 N4 已选活动 Session 独立进程，N10 已选记录保存后短空闲释放；不保证每轮结束立即清空全部内存，也不依赖每张历史卡常驻。

本次确认来源：[用户确认记录](chat-flow-context.md#decisions-before-review)。需求映射：D5 固定 Provider，D6 空闲保留与恢复，D7 执行中追加排队；D1–D3 继续约束暂停与恢复行为。

保留/恢复确认来源：[用户两项选择](chat-flow-context.md#decisions-before-review)，映射需求 D8–D9。

建议验收：同卡两轮返回同一 Session ID 和不同 Run ID；关闭运行实例后第二轮能使用第一轮上下文；单纯打开卡片没有模型请求；本卡暂停时发给本卡的新 prompt 不启动；停止/暂停/恢复 X 不改变 Y 的执行和暂停状态，即使两卡使用同一 Provider；历史缺失时给出恢复失败；每个 Provider 单独验收。

新增恢复验收：保存绑定/历史/工作区后重建容器或升级目标版本，原卡新 prompt 仍进入相同 Session、不同 Run；旧等待项和中断 Run 不自动重跑；归档/空闲不触发删除。运行中重启须核对旧执行状态，未排除冲突执行时不启动同 Session 的新写入者。

结果呈现验收按修订 D12：有效期内采用同一代任务卡跨轮更新，验证用户展开选择不会因刷新被重置；期限换代执行已批准的 L1，仍不按每轮发结果卡，也不预先承诺把个人展开偏好迁到新物理卡。无法满足已定行为时报告限制。引用旧轮次结果仍读取对应正文，在原 Thread 追加 prompt 仍恢复同一 Session；Session 绑定、队列及控制范围不变。
