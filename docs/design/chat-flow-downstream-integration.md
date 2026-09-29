# 下游集成说明：Provider Adapter 与部署验收要求（W3/W5）

日期：2026-09-30。作者：云端实施（chat-flow loop）。本文是 handoff「缺下游访问时」条款要求的集成说明：列出 yomi 侧已冻结的接入契约、下游必须提供的改动、固定版本要求与验收依赖。yomi 侧实现状态见 [实施状态](chat-flow-implementation-status.md)；行为语义权威在 [技术方案](chat-flow-technical-design.md)（本文不重复其论证，只落到接口）。

## 1. yomi 侧已冻结的接入面

### 1.1 ExecAdapter trait（`crates/kernel/src/exec/adapter.rs`）

```rust
#[async_trait]
pub trait ExecAdapter: Send + Sync {
    async fn create_session(&self, task: &ExecTask) -> Result<String>;
    async fn start_run(&self, native_session_id: &str, input: &AcceptedInput) -> Result<()>;
    async fn cancel(&self, native_session_id: &str) -> Result<()>;
    async fn release(&self, native_session_id: &str) -> Result<()>;
}
```

上报通道（构造时注入的 sink，mpsc）：`AdapterNotice::Terminal(TerminalKind)` / `AdapterNotice::Result(body)`，均携带 native_session_id。

调度语义（yomi 已实现，adapter 无需也不能重复实现）：每卡队列/暂停/停止/恢复、单 writer、名额、绑定状态机、事实与结果持久化、卡面。adapter 不接触这些状态，只做协议翻译与事实上报（C4）。

### 1.2 adapter 必须遵守的契约点

| 点 | 要求 | 依据 |
|---|---|---|
| 能力声明 | 启动时声明实际版本与支持项（创建/恢复/提问/授权/附件/取消范围/历史读取/释放）；缺必需能力拒绝启用该任务，不静默降级、不自动批准 | C4/N4/P0 |
| 绑定优先 | 原生身份返回后 yomi 先持久化绑定再发首个 prompt；恢复用原身份，失败不替换、不新建空 Session | N2/D6 |
| start_run 语义 | 只接受已获执行资格的完整输入；同步 ack = 原生已确认开始；**回执丢失必须返回错误**（yomi 据此标 Unknown 阻断，不盲重发） | C4/N3 |
| **run 身份回显（硬化要求）** | 上报 Terminal/Result 时必须能归属到 yomi 授予的本次 run——当前 sink 只带 native 身份，yomi 按「单 writer 轮序最早无正文 Run」推断，**某轮合法无正文时会错配**。真实 adapter 应在 start_run 获得 yomi run 标识并在上报中回显（trait 需随首个真实 adapter 落地时扩展 `start_run(run_id, ...)` 与 notice 载荷） | C4/N9，增量 5 记录 |
| 终态 | Completed/Failed/Cancelled 如实区分；取消回执≠已停止，原生终态才报 Cancelled；超时/失联报错误不猜 | C4/C6 |
| 当前轮请求 | 原生提问/授权请求以独立 notice 上报（**首版 trait 尚未建模**，P3 落地时新增 `AdapterNotice::Request{...}` 与 `answer(request_id, ...)` 方法）；不支持自由输入等能力差异如实声明 | N5/C5/D10 |
| release | 只释放运行实例，不删除历史；暂停有等待项时 yomi 也会调用——不得连带清队列语义 | C8/N10 |
| 重放 | 协议历史 replay 只补视图，不得触发新执行/重复投递/重复批准 | C4/D10 |
| 单 writer 外部面 | yomi 保证自身串行；adapter 需声明能否探测同一原生 Session 被其他客户端写入，能探测则报冲突 | N4/P0 |

### 1.3 进程与资源模型

每活动 Session 独立 Provider 进程（N4）；stdio 本地起步；相邻轮次可复用进程；终态+保存后按 `exec.idle_release_secs`（默认 60s）释放；暂停有等待项同样释放。yomi 不管理工作目录/worktree，只保存与传递恢复参数（cwd 等，经 `ExecTask.working_dir` 与创建配置）。

## 2. Provider 能力矩阵（实证状态）

### Kimi Code ACP（本机 2.1.0，2026-09-29 真实调用验证；探针 `/tmp/acp_probe*.py`）

| 能力 | 状态 | 证据/备注 |
|---|---|---|
| initialize 握手 | ✅ | protocolVersion 1；authMethods terminal login |
| session/new | ✅ | 返回原生 sessionId + configOptions |
| session/prompt | ✅ | 真实模型调用 stopReason=end_turn |
| session/load（跨进程恢复） | ✅ **内容级** | 进程 A 教暗号→杀→进程 B load→原样召回 |
| session/request_permission | ✅ | ask 模式下触发，options 含 allow_once/allow_always 类 |
| session/cancel | ✅ | **通知语义**（误作请求发出不生效）；sleep 60 中途 0.1s 内 cancelled |
| 同 Session 并发 prompt | ⚠️ **不拒绝**（2.1.0） | 两个 prompt 均 end_turn；2.0.2 研究记录为拒绝——版本差异，单 writer 必须由 yomi 强制（已强制） |
| sessionCapabilities | list/resume/close/delete/fork/additionalDirectories | initialize 声明；loadSession=true |
| prompt 能力 | image ✅ / audio ❌ / embeddedContext ✅ | initialize 声明 |
| 未验证 | question form 交互、load 时 replay 行为、close/delete 语义、外部写入检测、首次异常退出时 sessionId 取得时点 | P3 落地时逐项补 |

**版本注意**：研究基线固定 2.0.2，本机 2.1.0 能力集与并发行为均有差异；下游固定版本时须重新核对本矩阵。

### Codex App Server（未验证：无 CLI、无凭据）

仅有 2026-09-28 文档研究（见 provider-recovery.md）：stdio 双向、thread.id 恢复、read 与 resume/start 分开、turn/interrupt 受理≠终态、最后订阅者退出后 30 分钟卸载、experimental 限制。**全部待装固定版本后按 §3 契约套件验证；一个 Provider 通过不代表另一个通过。**

## 3. 下游必须交付（W3）

1. **Kimi ACP adapter 与 Codex App Server adapter**：实现 §1.1 trait + §1.2 全部契约点（含 run 身份回显的 trait 扩展）；各自通过同一套契约测试（创建/绑定/运行/结果归属/取消/停止范围/释放/恢复/请求失效/未知派发核对）。
2. **固定版本与安装**：两 Provider 的精确版本、安装方式、凭据接线；版本写入能力声明；yomi 消费时校验。
3. **业务接线（nika 侧）**：Skill 与专用入口的业务配置（何时交办、Provider 选择、工作目录）；不靠普通 Chat 代转。
4. **当前轮问答/授权接入**：N5 主卡控件+B 引用回复的用户面（yomi 卡面交互区待 P5；adapter 侧 Request/Answer 契约先行）。

## 4. 部署与保留（W5）

1. 持久卷保留：yomi.db（exec_* 表）、Provider 原生历史、原工作目录；三者缺一恢复能力即退化，须分别如实报告。
2. 普通重启与目标升级验证：重启后 boot_sweep 标中断、用户新输入恢复原 Session（yomi 侧已实现并有测试）；升级仅对验证过的版本路径承诺，保留可恢复备份。
3. 队列语义声明：yomi 队列/暂停只保同一进程生命周期（D2），用户可见处（卡面已标注）与运维文档都不得暗示跨重启保留。
4. 资源参数：`exec.max_concurrent_runs`（默认 2）、`stop_confirm_timeout_secs`（30）、`idle_release_secs`（60）按实测调整；通道级 `exec_tasks` 开关默认 false，逐通道启用。

## 5. 验收依赖（未满足前不得声称端到端完成）

- 双 Provider 各自真实通过 §3.1 契约套件（含真实取消与关联执行停止范围）；
- 飞书桌面/手机真卡链路（CardKit 展开态、同代多轮、L1 换代、原 Thread 路由）——需授权测试应用与群；
- 升级路径一次实测（普通重启 + 一次目标版本升级）；
- 普通聊天与旧命令回归（harness-e2e + 通道真链路）。
