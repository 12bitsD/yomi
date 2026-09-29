# 增量 10 实施规格：当前轮问答生命周期（N5/C5）+ ACP 真实授权验证

目标：把「Provider 当前轮提问/授权请求」建模为一等契约（C5），调度器管理请求生命周期；主卡显示待回应区，回答经回调回原请求；用真实 Kimi ACP `session/request_permission` 做端到端验证（harness 测试接入）。这是 yomi 侧最后一块可独立实施的契约。

## 契约依据（technical-design N5/C5/D10）

- 只登记 Provider 明确发出、当前 Run 仍在等待的原生请求；模型普通正文提问不产生伪请求。
- 回答只在 task/run/request 三者匹配且请求仍有效时被接收；一次请求只形成一个最终回应（跨入口重复给既有处置，不重复放行）；「已提交回答」≠「Provider 已接收并继续」。
- 拒绝走原生拒绝路径，不变成同意；过期/终态/停止/重启后请求失效——不批准新操作、不转成新 Prompt；断线不自动批准。
- 等回答的 Run 仍占有本 Session：不能开始下一轮（既有 C3 单在飞纪律已保证）。

## 组件 1：请求模型与生命周期（exec/request.rs + scheduler）

- `define_id!(ExecRequestId => "ereq_")`（types/mod.rs）。
- `ExecRequest { request_id, task_id, run_id, native_ref: String, kind: Permission|Question, prompt_text, options: Vec<{option_id, label, kind(allow_once/allow_always/reject_once/other)}>, status: Pending|Submitted|Resolved|Invalidated, created_at }`。
- adapter 上报：`AdapterNotice::Request { native_ref, kind, prompt_text, options }`；adapter 回答方法：`answer(native_session_id, native_ref, outcome: RequestOutcome) -> Result<()>`（`RequestOutcome::{Selected{option_id}, Rejected, FreeText{text}——能力不支持时 adapter 返回错误，不伪装}`）。
- scheduler：
  - 收到 Request（native 反查 lane/run 同 terminal 纪律）→ 登记 Pending、current.status=`WaitingRequest`（RunStatus 新值；is_live()=true——等回答仍占有 Session）、发 `ExecEvent::RequestPending{task_id, run_id}`；卡面待回应区（组件 2）。
  - `answer_request(task_id, run_id, request_id, outcome) -> AnswerOutcome`：逐维核对（任务/run 匹配/请求 Pending/操作者权限由调用方闸）→ 标 Submitted（**已提交≠已接收**）→ 锁外 adapter.answer → Ok 标 Resolved（以 adapter 确认为准）；Err → 回滚 Pending 并报错。重复回答（已 Submitted/Resolved）→ `AnswerOutcome::Already{status}` 不重复放行。
  - 失效路径：run 终态（含 Stopped/Failed）/run 被替换/boot_sweep → 该 run 全部 Pending 请求 → Invalidated（一次性，后续回答返回 `Invalid`）；不批准任何新操作、不转新 Prompt（C5）。
  - `LaneSnapshot` 加 `pending_request: Option<ExecRequest>`。
- `AnswerOutcome::{Submitted, Resolved, Already{status}, Invalid, Mismatch}`。

## 组件 2：卡面待回应区 + 回答回调（channels）

- 卡渲染：`pending_request` 在时加待回应区：问题/授权摘要 + 选项按钮（每选项一按钮，value `{action:"exec_answer", task, run, req, gen, opt}`；`reject` 类选项按原生语义呈现不美化）。无 Pending 不出该区。
- 回调 `exec_answer`：重核（同 exec_stop 全维度 + req 存在且 Pending）→ `scheduler.answer_request` → toast 映射（Submitted=「已提交，等待执行方确认」/Already/Invalid/Mismatch）→ 刷新卡。
- 卡面在 Submitted 后显示「已提交，待确认」（不显示「已生效」）；Resolved 后待回应区消失（下一事件刷新）。

## 组件 3：Sim 支持 + 单测

- SimAdapter：`SimControl.raise_request(native_id, kind, text, options)`（经 sink 上报）；`answer` 记录调用；`auto_resolve_on_answer`（answer 后报 terminal Completed）。
- 单测：登记/WaitingRequest 占位（派发不启动）/回答核对（错 run/错 req/重复）/Submitted→Resolved/adapter 错误回滚/终态失效/失效后回答 Invalid 不批准。

## 组件 4：ACP harness 真实验证（acp_harness_test.rs 追加场景 5）

- harness adapter 接 `session/request_permission`（agent→client 请求）：转 `AdapterNotice::Request`（options 映射 allow_once/allow_always/reject_once）；`answer` → 回 JSON-RPC result；等 prompt 终态报 Terminal。
- 场景 5（真实 kimi 2.1.0，permission_mode 临时 ask——测试内用 `session/set_mode`？若无该 ACP 方法，则 harness 进程启动前写临时 config 或用带 ask 的独立 KIMI_CONFIG_HOME；实现时择可行者并记录）：
  1. prompt 触发 shell 工具 → 收到 Request notice、run=WaitingRequest、卡面（快照）有待回应区；
  2. 错误 req 回答 → Mismatch/Invalid；
  3. 正确回答 allow_once → adapter 确认 Resolved → prompt 继续 → Terminal(Completed)、正文保存；
  4. 二次回答同 req → Already 不重复放行。
- `#[ignore]`+`YOMI_ACP_E2E=1` 门，同既有场景。

## 检查门槛

同前（check 三组合、clippy 零新增、fmt、全量绿除 4 沙箱存量；ACP 场景由主 agent 亲跑）。

## 明确不做

N5-B 固定问题消息引用回复（长文字入口，P5 通道面）、Kimi form 多题多选/Other 自由文本（按 probe 能力矩阵：不支持则 options 不含 free-text）、Codex 侧、请求持久化（请求随进程生命周期，boot_sweep 全失效——重启后由 Provider 重附着核实，P7 范围；卡片重启刷新经 latest_run/快照自然显示无待回应）。
