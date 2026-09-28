# 任务卡开发：项目上下文与已确认决定

整理日期：2026-09-29。本文是此次任务相关讨论和只读研究的可公开交接摘要；使开发者无需访问原聊天、本机 Wiki 或下游私有仓库即可理解设计依据。用户选择与研究事实分别标明。完整当前行为以 [需求](chat-flow-requirements.md) 和 [技术方案](chat-flow-technical-design.md) 为准；候选研究不覆盖已确认决定。

## 文档权威顺序

1. [需求 R1–R7、D1–D12](chat-flow-requirements.md)：用户要得到的行为与范围。
2. [技术方案](chat-flow-technical-design.md)：最终决定表、N1–N12、C1–C9、P0–P7 和验收矩阵，全部方案选择已闭合，能力尚未实测。
3. 本文：这些选择的上下文、日期及被取代的旧含义。
4. [运行流程研究](chat-flow-evidence/runtime-flow.md)、[Provider 研究](chat-flow-evidence/provider-recovery.md)、[单卡研究](chat-flow-evidence/feishu-single-card.md) 及两个 Provider 背景文档：研究时点的证据与备选比较。实施前针对所用版本复核，不能把其中的“推荐”当成新的待定需求。

开发入口与交付要求见 [云端开发 handoff](chat-flow-handoff.md)。实现状态和新验证结果应由接手者另行记录；文档“已确认”不表示代码已存在。

<a id="repository-relationship"></a>

## 仓库关系与职责

- 开发 fork：[12bitsD/yomi](https://github.com/12bitsD/yomi)；上游：[Crescent617/yomi](https://github.com/Crescent617/yomi)。2026-09-29 使用 GitHub API 核实 fork 关系与默认分支 `main`。
- 本轮文档分支：`feat/chat-flow-requirements`。源码研究基线为 [`a8566269311cb10a870f2c706d5488f1a76dbbc1`](https://github.com/12bitsD/yomi/tree/a8566269311cb10a870f2c706d5488f1a76dbbc1)。这不是功能已实现的提交。
- **nika 是 yomi 的下游产品和部署 harness。** 2026-09-28 对下游的只读检查确认：其部署消费带版本与校验的 yomi release 二进制，当前集成形式不是直接维护 yomi 源码 fork。下游内部源码、凭据和部署拓扑不属于本公开交接包。
- **yomi 负责通用能力**：执行会话和 Run、输入/控制/查询契约、任务内队列、暂停/停止/恢复、权威状态、结果保存、飞书路由与呈现。
- **nika 负责业务接入**：何时交办 Task、创建 Skill 与专用入口的业务配置、Kimi ACP/Codex App Server Adapter、目标版本与运行配置，以及集成部署。接入方不能另建一套与 yomi 竞争的权威队列，也不能用 Chat 模型代转任务 Thread 的每轮原始 Prompt。
- 长期改动路径是 yomi fork → upstream PR → release → 下游固定版本与校验。集成验证可先消费可定位的测试构建，不要求先正式发布；这条维护路径不是自动发布 release 或生产部署的授权。

<a id="downstream-integration"></a>

## 下游接入上下文与交付边界

下游历史模板已提供持久存储基础，但仅有数据卷不能证明卡片到 Session 的绑定、Run 核对或恢复已实现。历史核查还发现 Provider 安装/版本固定与 Codex 接线需要补齐。这里保存的是研究时点的能力缺口；接手者有下游访问权限时应在当前版本核实，不能将旧缺口直接视为现状。

| 下游必须提供/开发 | yomi 侧如何配合 | 何时才算完成 |
|---|---|---|
| 固定 Kimi/Codex 版本及运行环境 | 能力握手、版本与限制声明，受控外部执行契约 | 两种 Provider 分别有真实契约验证结果；一个通过不能替另一个通过 |
| 双入口业务接入 | Skill 与专用入口共用创建能力 | 两入口实际调用相同创建、权限、去重与上下文交接规则，不能仅写 Skill 文本 |
| Provider Adapter | 新输入、当前轮回答、控制和事实事件的归属契约 | 原生创建、绑定、提问/授权、停止、释放及同 Session 恢复均经过真实验证 |
| 持久原生历史、原目录和配置 | 保存任务/卡片/Session 绑定、关键 Run 事实、原始结果 | 普通重启和目标升级后主动续接；旧等待队列不重放，旧运行不自动重跑 |
| 授权测试账号与飞书目标 | 单卡/Thread 路由、回调校验与版本投递 | 桌面、手机及双读者展开态，L1 换代、旧回调和原 Thread 延续均有证据 |

本公开仓库可以承载通用契约、控制、状态、通道能力以及不含业务配置的测试工具。生产 Adapter 的维护归属仍是接入侧，不能为方便云端 checkout 把下游私有部署配置混进 runtime。若接手环境没有下游仓库或测试凭据，继续完成可独立验证的 yomi 工作，并明确列出待接入的 Adapter、配置与真实验收项；**这仍是完整目标的未完成部分，不能用仿真 Adapter 把首版标记完成**。涉及另一仓库的实际修改需要对应 checkout 和访问权限。

<a id="decisions-before-review"></a>

## 逐节点评审前的用户共识

以下为 2026-09-25～09-28 的相关选择摘要，短引文来自用户当时的选择；这里只保留实现本任务所需的上下文。

| 问题 | 用户选择及确定含义 |
|---|---|
| 普通回复是否也卡片化 | 普通交流保留现有文字风格；coding 等执行任务使用任务卡，不凭长度、工具使用或耗时自动转换 |
| 暂停队列时能否聊天 | “普通聊天继续，执行队列暂停”；暂停范围最终限定为本 Task，其他任务也不受影响 |
| 等待队列跨重启保留 | “首版只保证本次运行期间保留”；指 yomi 队列所属进程，Provider 单独释放/重启不清掉 yomi 中的等待项 |
| 停止 A 后恢复 | “继续 B/C；A 另行重试”；保留改动，恢复不是自动重试或恢复旧执行现场 |
| 后续 Prompt 发往哪里 | 任务专属 Thread 直接进入绑定的 Execution Session；用户不希望经普通 Chat 改写再间接影响执行 |
| 任务卡与 Provider | 首版固定一个 Kimi Code 或 Codex 原生 Session；不做卡内 Provider 切换，保留未来显式 handoff 的扩展边界 |
| 一轮结束是否销毁 Session | 后续澄清为 Run 结束、空闲释放实例，用户新 Prompt 可恢复原 Session；“任务结束就永久关闭会话”的早期解释已被取代 |
| 保留多久 | “不自动到期删除，明确清理时再处理”；归档也保留，接受磁盘增长，工作目录清理由接入/用户负责 |
| 普通重启与升级 | “支持普通重启和升级后的主动恢复”；保留原数据与目录、验证版本兼容，不恢复旧等待队列，不自动重跑 |
| 接入协议 | “用双向协议吧”；Kimi ACP、Codex App Server，本地 stdio 起步并固定版本验证，不能改成单向输出或自动授权 |
| worktree 管理 | “yomi 应该不需要考虑到这一层……直接在 codex/kimi code 里指定好了”；yomi 不建/合并/清理 worktree，不提供文件隔离 |
| 单卡与结果卡 | “按照单卡的方案来吧”；同任务主入口跨 Run 延续，不按每轮另发结果卡；平台期限的后续修订见 L1 |

<a id="review-n1"></a>

## 2026-09-29 N1：两入口共用契约

用户明确：“可以共用一个契约，然后我们合并 AB”，同时要求 nika 能用 Skill 创建任务，并有专门声明执行任务的入口。**A+B 均进入首版**；不是只留扩展口。早期“A 先做、B 延后”的建议 state=superseded。细节见 [N1](chat-flow-technical-design.md#n1-review-decision)。

<a id="review-n2"></a>

## N2：绑定顺序

针对“先给可追踪入口，再于实际执行前完成绑定”的 A，用户答“认可”。登记 Task/Provider → 卡片/Thread → 执行资格 → 创建原生 Session → 持久保存绑定 → 首轮 Prompt。初次尚未初始化与旧 Session 丢失是两种状态，后者不得静默新建。见 [N2](chat-flow-technical-design.md#n2-review-decision)。

<a id="review-n3"></a>

## N3：队列与业务处理分工

用户指出 Git commit/push 和冲突处理由 Agent 自行完成；随后同意 A：每任务队列/暂停，统一资源名额。**业务失败本身不自动暂停队列**；上一轮是否仍在执行未知、停止未确认或恢复失败才禁止下一轮。早期“整轮业务失败自动暂停”建议 state=superseded，未成为需求。见 [N3](chat-flow-technical-design.md#n3-review-decision)。

<a id="review-n4"></a>

## N4：活动 Session 独立进程

用户答“A 同意”：活动 Session 各有 Provider 进程，相邻轮次可复用，空闲保存后释放，后续恢复原 Session。接受启动与并发资源成本；首版不支持其他客户端同时写同一 Session。实际外部写入检测、保存与停止边界待验证。见 [N4](chat-flow-technical-design.md#n4-review-decision)。

<a id="review-n5"></a>

## N5：当前轮回答的双入口

用户认可 A 主卡控件+B 固定问题消息引用回复，共用同一原生待处理请求契约；后者补充长文字。新 Prompt 排下一轮，原生待处理请求的回答直接回当前轮。普通最终正文提问后的回复仍为新 Prompt；不凭模型猜测制造授权请求。见 [N5](chat-flow-technical-design.md#n5-review-decision)。

<a id="review-n6"></a>

## N6：停止与明确恢复

用户选 A，并对“停止尚未确认时保持暂停，确认后再点恢复”答“可以的”。先阻止后续派发，再取消当前 Run；取消受理不等于已经停止。提前恢复返回受阻，**不记住意图后自动续跑**。见 [N6](chat-flow-technical-design.md#n6-review-decision)。

<a id="review-n7"></a>

## N7：事实与查询

用户选“A 吧看着好像好一些”：持久关键事实+当前快照+按需历史，不保证逐条补齐通知。普通 Chat 可用自己的模型解释所读事实，查询不创建 Execution Run 或 Prompt。原始指令不算已完成工作，Run 终态不代表业务验收通过。见 [N7](chat-flow-technical-design.md#n7-review-decision)。

<a id="review-n8"></a>

## N8：局部更新与展开态

用户对 A 原型验证路线答“同意”：稳定折叠结构+局部更新，保留同代多轮阅读选择及多读者独立性的验收。服务端保存共享展开状态的 B 不作为自动兜底。此次批准只是路线，未证明真实客户端行为。见 [N8](chat-flow-technical-design.md#n8-review-decision)。

<a id="review-l1"></a>

## L1：按平台期限换代

用户随后明确选“L1”：每任务始终只有一张**当前有效**主卡，同代多轮；因平台期限更换物理卡，旧卡留历史。活跃/待回答任务在期限前换代，空闲过期任务在用户返回时换代。保持 Task、Session、Run、请求与本进程队列/暂停，换代不触发执行。

旧解释“永久同一物理卡消息”仅在永久性部分 state=superseded。N8-A 同代展开态要求继续有效；跨物理卡不预先承诺客户端个人偏好迁移。原 Thread 延续及换代可靠性待真实验证。见 [L1](chat-flow-technical-design.md#l1-review-decision)。

<a id="historical-result-fallback"></a>

## 已被取代的每轮结果卡兜底

用户曾接受“主卡更新进度，每轮另发可折叠结果卡”，随后改为单卡方案；该每轮结果卡兜底 state=superseded。L1 是期限换代而非每轮发卡，不重新启用旧兜底。旧卡引用可能有结果轮次歧义，按 N9 明确定位，不能默认选最新正文。

<a id="review-final"></a>

## N9–N12 与交付路线的最终收敛

在 N9 推荐“按轮保存 Agent 原始正文，超长全文首版使用 Markdown 附件”之后，用户答：“同意，之后的都按照你的建议收敛吧”。因此 N9-A 明确同意，N10-A、N11-A、N12-A 与交付路线 A 按此授权闭合；保留前面已确认的组合选择。

- N9：规范化仅归属元数据，保留原始回复文字；保存后发布固定 Run/版本引用，导出失败不重跑任务。
- N10：终态确认和可靠保存后，短空闲期释放 Provider；暂停队列即使有 B/C 也可释放，yomi 内等待项和暂停保持。
- N11：启动核对未闭合 Run，新输入才主动恢复原 Session。yomi 重启不恢复旧等待队列，不重放旧任务。
- N12：最小持久受理身份/事实，显示重试最新版本；未知 Provider 派发先核对、不盲重发。重启后旧输入重送返回“未恢复、未重新执行”，不回报旧“仍排队”。期限换代按 L1，普通投递失败不能擅自补卡。
- 交付路线：先验证关键风险，再贯通一个完整用户流程，随后补异常、多卡及双 Provider 独立验收。

这是技术选择闭合，不是实现或测试完成。完整效果、限制和验收以 [最终技术方案](chat-flow-technical-design.md#final-review-decisions) 为准。
