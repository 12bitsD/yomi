# Runtime、路由与控制研究证据

> 阅读定位：本文件保留截至 **2026-09-28** 的研究证据与候选比较；**2026-09-29 已完成方案收敛**，最终选择和开发契约以 [已确认技术方案](../chat-flow-technical-design.md#final-review-decisions) 为准。本文历史候选不是新的待决策项；路线获批仍不等于能力或端到端效果已验证。

日期：2026-09-28。代码基线：yomi `a8566269`。范围：任务创建、确定性路由、按卡排队、停止/恢复、只读状态、事件真值及四端口边界。仅静态阅读，未运行新功能、真实 Provider 或飞书端到端实验。本文为技术方案证据和候选研究；节点字母是本文件的研究编号，不对应最终方案 N1–N12 的选项字母。

## 1. 最影响方案的事实

| 静态核实事实 | 对开发的含义 | 代码来源 |
|---|---|---|
| 现有普通飞书消息完成上下文准备及图片下载后，使用 steer 入口；手动开 Thread 也是 steer | 不能只把新卡绑定到一个旧 Session 就宣称满足排队。任务 Thread 必须先于普通聊天路径分流，原文及附件进入新的串行输入契约 | [handlers.rs](https://github.com/12bitsD/yomi/blob/a8566269311cb10a870f2c706d5488f1a76dbbc1/crates/kernel/src/channels/hub/handlers.rs#L817)、[handlers.rs](https://github.com/12bitsD/yomi/blob/a8566269311cb10a870f2c706d5488f1a76dbbc1/crates/kernel/src/channels/hub/handlers.rs#L162) |
| 现有 mailbox 的 steer 在 Streaming 前批量注入，normal 在 Idle 逐条取出；它们是优先级队列 | steer/normal 不是独立 Chat/Execution lanes。任务 prompt 不能隐式走 steer；当前轮问题回复又必须绕过等待队列 | [mailbox.rs](https://github.com/12bitsD/yomi/blob/a8566269311cb10a870f2c706d5488f1a76dbbc1/crates/kernel/src/comms/mailbox.rs#L9)、[agent.rs](https://github.com/12bitsD/yomi/blob/a8566269311cb10a870f2c706d5488f1a76dbbc1/crates/kernel/src/agent/agent.rs#L306) |
| conductor 从输入总线收消息后逐条 spawn 处理；处理里有异步图片准备 | 总线到达有顺序，不代表最终入队有顺序。每卡需要一个明确的受理与控制所有者，先固定次序，再完成可能较慢的准备 | [conductor.rs](https://github.com/12bitsD/yomi/blob/a8566269311cb10a870f2c706d5488f1a76dbbc1/crates/kernel/src/kernel/conductor.rs#L139)、[conductor.rs](https://github.com/12bitsD/yomi/blob/a8566269311cb10a870f2c706d5488f1a76dbbc1/crates/kernel/src/kernel/conductor.rs#L465) |
| 现有 cancel 清空双队列；取消超时存在 detach，随后可能唤醒新输入 | 新 stop_and_pause 不能套用旧 cancel。新控制需要保留队列、暂停门、目标 Run 校验和真实停止栅栏；旧 cancel 保持兼容 | [conductor.rs](https://github.com/12bitsD/yomi/blob/a8566269311cb10a870f2c706d5488f1a76dbbc1/crates/kernel/src/kernel/conductor.rs#L380)、[kernel/mod.rs](https://github.com/12bitsD/yomi/blob/a8566269311cb10a870f2c706d5488f1a76dbbc1/crates/kernel/src/kernel/mod.rs#L1744) |
| 当前停止按钮只传 Session，直接执行 cancel | 卡片按钮须增加目标 Run、命令去重与失效判定；不能让旧 A 的按钮取消已开始的 B | [obs.rs](https://github.com/12bitsD/yomi/blob/a8566269311cb10a870f2c706d5488f1a76dbbc1/crates/kernel/src/channels/render/obs.rs#L1239) |
| Thread 路由及并发创建锁已经存在；已有映射指向已删除 Session 时会删除映射并自动新建 | 可复用 Thread 身份与创建串行机制；任务恢复必须绕开自动新建兜底，恢复失败应保持原卡并说明原因 | [routing.rs](https://github.com/12bitsD/yomi/blob/a8566269311cb10a870f2c706d5488f1a76dbbc1/crates/kernel/src/channels/hub/routing.rs#L194)、[routing.rs](https://github.com/12bitsD/yomi/blob/a8566269311cb10a870f2c706d5488f1a76dbbc1/crates/kernel/src/channels/hub/routing.rs#L354) |
| 当前 event envelope 只有 Session 和事件身份，无统一 Run 身份 | 同卡多轮必须新增 Run 关联，否则旧轮迟到事件可能污染当前状态 | [event/mod.rs](https://github.com/12bitsD/yomi/blob/a8566269311cb10a870f2c706d5488f1a76dbbc1/crates/kernel/src/event/mod.rs#L5) |
| 当前事件订阅是有界的；客户端慢时可能丢件；消息落盘及 Stopped 会清除 replay buffer | 不能把重连订阅作为可靠历史/当前状态来源。需要版本化状态读取、独立结果保存及漏件后重新读取 | [server/mod.rs](https://github.com/12bitsD/yomi/blob/a8566269311cb10a870f2c706d5488f1a76dbbc1/crates/kernel/src/server/mod.rs#L14)、[dispatcher.rs](https://github.com/12bitsD/yomi/blob/a8566269311cb10a870f2c706d5488f1a76dbbc1/crates/kernel/src/server/dispatcher.rs#L820)、[bus.rs](https://github.com/12bitsD/yomi/blob/a8566269311cb10a870f2c706d5488f1a76dbbc1/crates/kernel/src/comms/bus.rs#L9) |
| 现有 Stopped 已被延后到 turn 收尾之后发出；Completed 只表示一个 step 正常结束 | 可复用“收尾完成才给终态”的原则；不能把 Completed 映射成整项工作验证成功 | [agent.rs](https://github.com/12bitsD/yomi/blob/a8566269311cb10a870f2c706d5488f1a76dbbc1/crates/kernel/src/agent/agent.rs#L383)、[event/mod.rs](https://github.com/12bitsD/yomi/blob/a8566269311cb10a870f2c706d5488f1a76dbbc1/crates/kernel/src/event/mod.rs#L200) |
| 当前 get_session、mailbox、todos、messages、events 分散；get_session 没有活跃 agent 时返回 Idle | 新状态读取应聚合为同一版本；进程重启不能因内存为空而把旧运行视为正常空闲 | [kernel/mod.rs](https://github.com/12bitsD/yomi/blob/a8566269311cb10a870f2c706d5488f1a76dbbc1/crates/kernel/src/kernel/mod.rs#L1458)、[kernel/mod.rs](https://github.com/12bitsD/yomi/blob/a8566269311cb10a870f2c706d5488f1a76dbbc1/crates/kernel/src/kernel/mod.rs#L1510)、[dispatcher.rs](https://github.com/12bitsD/yomi/blob/a8566269311cb10a870f2c706d5488f1a76dbbc1/crates/kernel/src/server/dispatcher.rs#L311) |
| 四端口是架构约束；现在 tools 扩展每次单独 spawn，上限 600 秒，结果回流还会截断；旧 ext_register/pull/result 已删除 | 不能声称持久双向 adapter 已是现成接口。需要在既有四端口内增加受控的执行会话行为，不能简单包装一次 shell 工具调用 | [AGENTS.md](https://github.com/12bitsD/yomi/blob/a8566269311cb10a870f2c706d5488f1a76dbbc1/AGENTS.md#L13)、[ext.rs](https://github.com/12bitsD/yomi/blob/a8566269311cb10a870f2c706d5488f1a76dbbc1/crates/kernel/src/tools/ext.rs#L1)、[ext.md](https://github.com/12bitsD/yomi/blob/a8566269311cb10a870f2c706d5488f1a76dbbc1/docs/design/ext.md#L9) |

## 2. 应对外提供的最小行为契约

目的：让飞书、普通 Chat、nika 业务入口只描述“要做什么”，不参与操作 mailbox、猜测进程是否结束、拼接 Session 历史。

必须有的行为是：创建并绑定任务；追加原始输入；回答当前请求；停止指定 Run 并暂停本卡；恢复本卡等待队列；读取状态；读取某轮结果。这里是行为集合，不要求逐个变成独立 API 或新概念。

每次变更都携带来源和操作者，能关联到明确任务；涉及正在执行的操作必须关联本次 Run；当前轮回复还关联到原问题/授权请求。外部消息或按钮重试能被识别为同一次操作。结果应区分“拒绝”“已受理”“目标已变化”“已经如此”；已受理不等于 Provider 已完成动作。

任务创建响应至少让调用方知道：创建是否完成，主卡与 Thread 的入口，固定的 Provider 绑定，初始输入是等待、运行还是创建失败。受理队列输入之后才能显示已排队；不得仅因命令已写入总线就回报已排队。

读取结果必须同时包含可信运行事实与业务结论的证据边界，例如“本轮已结束；代码已修改；测试失败”。未知进度、无法确认的停止、陈旧状态应明确显示，不用模型推断填满。

## 3. 节点 A：任务创建与输入分流

**发散。** 考察先建立可见任务入口再准备 Provider、先准备 Provider 再发卡、把创建过程交给普通 Chat 通过多次工具调用编排。第三项会把部分失败、重复创建和路由接管责任泄漏给模型，不作为首版候选。

**方案 A（推荐）：一个创建行为，内部先记录任务意图，再建立主卡和 Thread，最后完成 Provider 绑定并开始首轮。** 用户较早看见“准备中”，全部创建重试围绕同一任务身份；原始首条指令只允许进入执行一次。要开发创建阶段记录、唯一来源键、卡片与 Thread 回填、失败阶段展示和同一任务继续准备。代价是需要表达“卡已存在但暂时不能执行”；Provider 尚未准备成功时不能显示已运行。

**方案 B：一个创建行为，内部先准备并记录 Provider Session，再发布主卡，入口完整后开始首轮。** 用户看到的卡天然已具备运行绑定；Provider 初始化失败时不会出现半成品卡。代价是首次反馈更晚，发卡失败会留下没有可见入口的已建 Session，需要保留未完成创建记录并重试同一卡投递；不能盲目再建 Session。

两者均需要阶段记录和消息去重。任何跨平台动作存在“远端成功、本地没收到确认”的窗口，不能宣传全链路 exactly-once；处理方式是用可用身份核对，核对不清时显示创建待确认，停止自动重放有副作用的步骤。

绑定完成后的路由不再走模型：任务 Thread → 原绑定；带请求关联的回复 → 原请求；普通聊天 → Chat Session。对“开始按方案改”的业务理解可以由接入方提供，但必须变成明确的任务创建声明；yomi 不以长回复/工具调用猜执行意图，也不让 Chat 改写任务 Thread 原文。

**不变量与失败语义。** 未知 Thread 不自动绑到“最近任务”；任务绑定损坏不自动建空 Session；当前轮回复的关联失效时不降级成新 prompt。重复初始消息返回同一任务。附件先确认已完整接收或引用可读取，再声明该输入已受理，避免只保留文本摘要。

## 4. 节点 B：按卡队列与执行准入

**发散。** 考察扩展旧 mailbox、新建受控任务执行分支、把等待项留在飞书或 Provider 内。第三项无法统一暂停/读状态/去重，不作为候选。

**方案 A（推荐）：在 yomi 内形成一个按卡的执行控制单元，集中拥有等待项、暂停状态、当前 Run 与控制顺序。** 普通聊天继续原路径；新任务只在这个单元中排队一次，adapter 接到的永远是已经取得执行资格的一轮。要开发统一受理、原始内容保留、先进先出、准入检查、可查询状态及资源占用释放。价值是把复杂性藏在一个行为入口内，旧 cancel/steer 不会渗入任务路径。代价是增加一条通用执行分支，须明确避免产生“yomi 一份队列、adapter 又一份队列”。该单元仍属于 yomi 内部，不是第五扩展端口。

**方案 B：将现有 conductor/mailbox 重构成可复用的执行所有者，普通 agent 与外部 Provider 都通过它运行。** 共用排队与控制模型，长期概念更少；可以复用既有输入块保存和 mailbox 管理。代价是必须同时梳理 steer、continue、compact 等旧语义，变更面扩大，普通对话回归更重。不能仅添加 paused 布尔值：旧取消、唤醒、取队列、空闲清理与并发输入均需一并改造。

**合同。** 每张卡只有一个实际写入 Provider 的 Run；消息按系统受理顺序排队，而不是按附件下载完成顺序排队。相同原始消息重送不会新增等待项。同一 yomi 队列所属进程保存完整内容、附件、来源和顺序；该进程重启后不恢复这些等待项。仅关闭 Provider 进程不丢弃 yomi 中的等待项或暂停状态。新 Run 开始前必须同时满足：本卡未暂停、无尚未结束或无法确认停止的前序 Run、会话恢复成功、资源额度可用。

“已受理”和“开始执行”必须分开。受理后的输入若因附件准备或恢复失败不能运行，保留并显示阻塞原因，不能跳过它执行后面的指令。系统没有能力保存的新输入应明确拒收，不能先回复排队成功再丢弃。

去重凭据与等待队列要分开：可以持久记录一条外部消息已经被受理、关联哪项任务及是否已开始，以避免飞书重送在重启后变成重复执行；这不等于持久保存并恢复待执行输入。进程重启后，曾受理但未开始的旧输入应显示“等待项未恢复”，不能因消息重送重新入队。用户主动重试应有新的明确交办身份，即使正文与旧消息相同也允许新建尝试。若向 Provider 发起执行后失去确认，应先核对原生状态，不自动重发可能已开始的有副作用 prompt；只读核对和卡片重试可以自动重试，开始执行不能套用相同策略。

跨卡并发额度不是暂停范围。可选共同公平额度（推荐首版，配置简单）或按 Provider 分开额度（阻止一类 Provider 独占资源，但配置更多）。等待资源、等待用户、用户暂停、恢复失败应表达不同原因；不能都叫“暂停”。

## 5. 节点 C：停止、暂停与显式恢复

**发散。** 考察原生中断并等待真实终态、中断后有边界地升级终止独占执行容器、立即回报停止并后台尽力取消。第三项违背可信停止和单写入，不作为候选。

**最终方案衔接：** N4 已选活动 Session 独立进程；N6 已选每任务统一控制顺序，停止未确认时拒绝恢复且不预约自动恢复。下述 A/B 只比较研究时的故障收尾策略；仅明确归属本任务的进程树可按最终契约强制收尾，无法证明范围时仍保持停止未确认。

**方案 A（推荐首版）：原生协议中断，超时保留“停止未确认”，阻止同 Session 下一轮。** 受理时先关闭本卡后续启动许可，再核对目标 Run，将有效中断发送到 adapter。只有本轮及已纳入管理的委派结束、收尾完成后确认停止。代价是 Provider 卡死时需要显示阻塞并人工处理；收益是不会为停 X 误杀共享服务中的 Y。

**方案 B：原生中断优先，超时后终止经过验证只属于本次任务的进程树/执行容器。** 可提高故障下的停止成功率；代价是必须先证明进程归属和后代收尾、处理历史未落盘等情况。共享 App Server、未隔离子进程或远程工作不能套用该升级步骤。若不能证明仅影响 X，仍必须回到停止未确认。该方案不是 worktree 管理，也不是停止后回滚文件。

**顺序合同。** 停止回执表示“本卡已暂停，针对 Run A 的停止已受理”；并不表示 A 已退出。A 已自然结束且 B 未启动，则只暂停等待项；A 已结束、B 已在点击抵达前启动，则暂停未来项，报告 A 已结束及 B 当前状态，绝不默默用旧操作取消 B。停止和出队共享同一决定顺序，不能先发网络中断再暂停队列。

恢复只解除本卡等待队列的用户暂停，不把 A 放回等待队列。重复恢复不重复取队列；空队列恢复不会调用模型。最终 N6 已确认停止未确认时拒绝恢复并说明需等待核对，不把点击悄悄记成未来自动恢复意图。已确定结束但业务失败的一轮，可以按队列规则继续用户已排好的下一条；若会话状态未知，保持阻塞。

取消传播属于 adapter 的可验证承诺：Provider 回了 interrupt ACK、进程退出、子进程全部收尾是三件事。只对系统掌握生命周期的委派宣称停止；无法管理的外部工作要明确范围。超时/掉线不可证明已停止，迟到的 A 事件也不可结束或恢复 B。

## 6. 节点 D：状态、事件与真实终态

**发散。** 考察快照主导并以事件通知刷新、可靠事件日志重建状态、界面仅监听现有实时事件。第三项因现有丢件与 buffer 清除不能满足重连和终态正确性。

**方案 A（推荐）：版本化状态读取是对外依据，事件提示状态有变化。** 重要变化及每轮结果先记录，再发布变化通知；界面和 Chat 首次读取、重连、发现缺口时重新读同一版本快照。实时文字可流式显示，但不承担终态保证。要开发稳定任务/Run 关联、变化版本、当前运行及队列的统一视图、重要状态落盘与结果索引。代价是多次读取；收益是无需把所有 token 流事件永久保存才能正确恢复。

**方案 B：任务关键事件持久记录，状态由这些事件重建并提供可续接订阅。** 只记录接收、开始、请求、停止受理、终止及结果等业务关键事件，不要求存每个 token。顺序、审计与重建清晰；代价是事件版本迁移、投影重放、订阅游标与回放幂等实现更多。待执行输入即使有审计记录也不得在进程重启后自动重放为待执行队列。

两者都遵循仓库“状态可由记录恢复”的原则；区别在于对外主要通过快照核对，还是把可靠关键事件订阅也纳入契约。现成的临时事件总线不能单独提供两者所需的保证。

**合同。** 一条进展属于明确 Task 和 Run；重复事件不再应用，旧 Run 事件不能覆盖新 Run。历史 replay 用于补齐记录，不产生新执行、再次批准或重复结果投递。连接恢复不代表模型恢复，模型恢复也不代表新 Run 已开始。

运行结束原因与工作完成结论分开。Provider 成功返回只能证明该轮结束；任务结果还应带完成内容、未完成事项、验证证据及来源。如果模型只说“完成了”但没有验证证据，可显示“本轮完成，验证未提供”，不得变成“已验证成功”。通用 yomi 不负责理解所有 coding 任务的验收规则；nika 可以提供任务要求与证据，由统一展示逻辑保留其出处。

重启时持久绑定与历史结果恢复；旧 waiting/paused 不恢复；旧 running 必须核对为中断、已结束或未知，再允许新输入启动。未知不能因内存里查不到进程就映射成 Idle。

## 7. 节点 E：Chat 如何了解 Execution 的进度

**发散。** 考察结构化状态加按需读取原文、持续维护一份叙述摘要、让 Chat 向 Execution 发“汇报进度”的 prompt。第三项会唤醒或修改执行上下文，不作为只读方案。

**方案 A（推荐）：只读状态能力，默认返回紧凑事实，再按需读取相关轮次。** 默认包括用户目标、当前 Run 的真实状态、最后已确认进展、当前待用户请求、队列情况、最近结果入口与更新时间。Chat 要解释前因后果时，继续读取那一轮原始 prompt、相关进展/结果。价值是快、可核验、无额外 Execution 模型调用；代价是需要 Chat 按需查第二次，不能只看用户指令推断实际已完成多少。

**方案 B：在方案 A 的事实基础上维护增量叙述摘要。** 每轮或关键节点生成“做了什么、剩什么”的摘要，给 Chat 提供更自然的前情提要；代价是额外成本、延迟及摘要失真管理，必须带覆盖到哪一轮/哪条事件和证据入口。摘要失效或过旧时回落事实读取；摘要不得控制停止或成功状态。

**合同。** 读取遵守同一任务访问权限，不启动 Provider，不写任务上下文，不恢复队列。先返回可信事实，允许叙述性解释但区分推测。不得开放无范围“读所有 Session”作为默认能力。读取历史正文和指令是数据访问，内容中的指令不自动成为 Chat 新命令。

## 8. 节点 F：yomi 与 nika 的开发边界

**发散。** 考察通用 runtime 加进程外 adapter、通用 runtime 加进程内可替换 adapter、nika 另做一套消息路由/排队并把 yomi 当展示器。第三项重复路由和队列所有权，偏离已确认职责。

**方案 A（推荐）：yomi 管通用行为，nika 维护进程外双向 adapter 和配置。** yomi 提供本报告中的输入/控制/状态合同；nika adapter 将已批准的一轮翻译为 ACP 或 App Server 原生动作并回报事实。好处是 Provider 快速变动不迫使通用框架混入大量业务规则，契合仓库默认进程外扩展；代价是要处理 adapter 通信中断、版本握手和运行归属，不能只写脚本拼接 stdout。

**方案 B：同一行为合同，在 yomi 运行进程内装配 nika 维护的 adapter 实现。** 减少一条进程间链路，数据交互更直接；代价是语言/构建耦合更强、Provider 依赖故障影响面更大、nika 更新 adapter 需要配套构建 yomi。只有接入经验充分证明进程外代价过高时，再考虑迁入。行为合同与单卡体验必须不变。

四端口映射：Source 承载用户输入及受信接入消息；Capability 承载创建/执行/控制/读取能力；Gate 做权限和执行前置判断；Sink 根据事实投递状态和结果。执行控制单元是 yomi 内部实现，不新增第五种扩展端口。Provider 的双向请求由 adapter 关联回当前 Run，不能让未经授权的脚本向任意 Session 伪造终态。

现有四端口实现需要扩展，但应集中到这些行为而非增加一串底层过程开关。yomi 不管理业务目标、不选择 worktree；nika 不复制卡内队列、不让 Chat 代理执行输入。Provider 原生历史由 Provider 保存，绑定和运行事实由 yomi 权威记录，nika 部署保留必要卷与目录；公开可执行的接入要求见 [下游接入边界](../chat-flow-context.md#downstream-integration)。

## 9. 可交付顺序与验收证据

1. 固定语义及行为合同：以上受理、Run 身份、控制范围、重复请求和未知状态可以用模拟 adapter 验证。先验证控制所有权只有一处。
2. 建立真实输入路径：同卡新 prompt 串行，当前轮回答回原请求，普通聊天不被任务 pause 影响；验证原文/附件保真及乱序下载。
3. 打通真实 Provider 生命周期：创建、原 Session 恢复、长运行、原生中断、问答/授权回路。两 Provider 各自提供证据，不能互相替代。
4. 完成停止栅栏和异常恢复：停止同时发生自然结束、新 prompt、旧事件、重复点击、adapter 掉线、进程重启。每一项核对“实际是否还在运行”，不能只验证按钮文案。
5. 接入单卡呈现及只读查询：它们读取同一事实。渲染失败不重启任务，查询不启动模型，卡片重试不重复执行。
6. 回归旧行为与启用：旧 cancel/steer/普通聊天语义保持；新能力未支持时明确拒绝。通道改动完成真实链路验证和仓库要求检查后再发布固定版本。

在上述验证前，本文所有候选的效果是设计目标；代码阅读只能说明改造点及现有风险，不证明双向协议、真实停止或单卡体验已达成。
