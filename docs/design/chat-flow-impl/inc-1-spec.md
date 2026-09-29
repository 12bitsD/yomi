# P1 增量 1 实施规格：执行任务登记核心（exec registry）

目标：落地 N1/N2 的身份与契约内核——任务登记、去重、绑定状态机。本增量**不接**通道、工具、卡片；只交付 kernel 内核心 + 测试。

## 背景约束（来自设计文档，必须遵守）

- C2 任务与绑定契约：创建分步——登记任务与固定 Provider → 卡片/Thread（增量 2）→ 原生 Session 绑定持久化 → 才允许执行。
- 必须区分「合法新任务尚未初始化」与「旧绑定不存在/损坏」：后者明确失败，绝不自动新建 Session（N2、D6）。
- 同一创建意图重送返回同一任务（dedup）；独立的明确新建可产生新任务（C2）。
- 队列/暂停/Run 不在本增量（P2）。原生 Session 创建用仿真（P1 全程）。

## 命名（避免与既有 `crate::tools::task`（todo）冲突）

- 新模块：`crates/kernel/src/exec/`（执行任务域）。
- ID：`define_id!(ExecTaskId => "task_")`，加进 `crates/kernel/src/types/mod.rs` 的 define_id 列表。
- 表名前缀 `exec_`，migration version 26（当前 CURRENT_SCHEMA_VERSION=25， bump 到 26）。

## 类型（`crates/kernel/src/exec/mod.rs`，serde snake_case，遵循仓库现有风格）

- `ExecProvider { Kimi, Codex }` — Display + FromStr + serde。
- `BindingState { Uninitialized, Bound, Broken }` — 三态语义见上；DB 存 snake_case 字符串。
- `ExecTaskStatus { Active, Archived }`。
- `ExecTaskSource { Skill, Entry }` — Skill=nika Skill 调用；Entry=专用执行入口。
- `ExecTask` 结构：
  - `id: ExecTaskId`
  - `channel_name: String`（创建来源通道；RPC/Skill 场景可为 "rpc" 类占位，本增量不强约束）
  - `provider: ExecProvider`
  - `status: ExecTaskStatus`
  - `binding: BindingState`
  - `provider_session_id: Option<String>`（原生身份，绑定持久化后才有值）
  - `thread_root_msg_id: Option<String>`（增量 2 回填）
  - `card_msg_id: Option<String>`（增量 2 回填）
  - `card_generation: i64`（默认 0=尚无卡；L1 换代递增）
  - `goal: String`（任务目标/首轮原文摘要）
  - `working_dir: Option<String>`
  - `created_by: String`（操作者 open_id 或 rpc 身份）
  - `source: ExecTaskSource`
  - `dedup_key: String`
  - `created_at / updated_at: chrono::DateTime<Utc>`
- `CreateExecTask { channel_name, provider, goal, working_dir, created_by, source, dedup_key }`。

## SQLite（migration v26，`add_exec_tasks`）

```sql
CREATE TABLE exec_tasks (
    id TEXT PRIMARY KEY,
    channel_name TEXT NOT NULL,
    provider TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    binding TEXT NOT NULL DEFAULT 'uninitialized',
    provider_session_id TEXT,
    thread_root_msg_id TEXT,
    card_msg_id TEXT,
    card_generation INTEGER NOT NULL DEFAULT 0,
    goal TEXT NOT NULL,
    working_dir TEXT,
    created_by TEXT NOT NULL,
    source TEXT NOT NULL,
    dedup_key TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX idx_exec_tasks_dedup ON exec_tasks(channel_name, dedup_key);
CREATE INDEX idx_exec_tasks_thread ON exec_tasks(thread_root_msg_id);
```

## Store（`crates/kernel/src/exec/store.rs`，镜像 `channels/store.rs` 模式）

`trait ExecTaskStore`（async_trait）：
- `create(input: &CreateExecTask) -> Result<(ExecTask, bool)>` — bool=是否新建；dedup 命中（同 channel_name+dedup_key）返回既有任务+false，**不更新**既有行（重送不得扩大副作用）。
- `get(id: &ExecTaskId) -> Result<Option<ExecTask>>`
- `find_by_dedup(channel_name: &str, dedup_key: &str) -> Result<Option<ExecTask>>`
- `bind_provider_session(id: &ExecTaskId, native_session_id: &str) -> Result<ExecTask>` — 状态机：仅 `Uninitialized -> Bound` 合法；`Bound` 且同 id → 幂等返回；`Bound` 且不同 id 或 `Broken` → 返回明确错误（不得静默换绑/重建）。
- `mark_broken(id: &ExecTaskId, reason: &str) -> Result<ExecTask>` — Bound/Uninitialized -> Broken；reason 进 tracing（表不加列）。
- `set_thread_and_card(id, thread_root_msg_id, card_msg_id) -> Result<ExecTask>` — 增量 2 用，本增量实现+测。
- `archive(id: &ExecTaskId) -> Result<ExecTask>` — Active -> Archived；归档不删行（D8）。
- 所有写操作刷新 updated_at。

`SqliteExecTaskStore { pool }` + `new(pool)`。行映射用 `sqlx::FromRow` 的 DbRow + into 转换（镜像 PermRequestDbRow 模式）。枚举↔字符串转换失败要 warn 并给保守默认（binding 未知值视为 Broken——保守方向）。

## 装配

- `crates/kernel/src/storage/init.rs`：照 channel_store 模式建 `Arc<dyn ExecTaskStore>`，挂进 `StorageSet`（新增字段+getter，照 existing store 的挂法）。
- `crates/kernel/src/kernel/mod.rs`：`Kernel::new` **不改签名**——从 `storage: &StorageSet` 取 `storage.exec_task_store()` 存入 Kernel 字段；加 `pub fn exec_task_store(&self) -> Arc<dyn ExecTaskStore>`。
- `crates/kernel/src/lib.rs`（或 mod 声明处）：`pub mod exec;`

## 测试（`crates/kernel/src/exec/store_test.rs` 或模块内 #[cfg(test)]，用内存 sqlite，参照 `storage/migrations_test.rs` / init_test.rs 的建池方式）

1. create 新建 → (task, true)，默认 Uninitialized/Active/card_generation=0。
2. 同 channel+dedup_key 再 create（goal 不同）→ 返回原 task、(false)，goal 未被改。
3. 不同 dedup_key → 新任务。
4. bind：Uninitialized->Bound 成功；同 id 再 bind 幂等；不同 id bind → Err；Broken 后 bind → Err。
5. mark_broken 后 get 可读、状态 Broken；Uninitialized 也可 mark_broken。
6. set_thread_and_card 回填可读；find_by_dedup 命中。
7. archive 后 status=Archived，行仍在（get 可读）。
8. 迁移幂等：run_migrations 跑两遍不炸（照 migrations_test 现有模式）。

## 检查门槛

- `cargo check`、`cargo clippy --all-features`（仓库 .cargo/config.toml 开了 pedantic，警告即失败级别对待，逐条处理或按现有 allow 风格处理）、`cargo fmt`、`cargo test exec` 全过。
- 注释用中文（仓库现有风格），模块 doc 写明：本模块是 chat-flow W1 的执行任务登记，设计依据 docs/design/chat-flow-technical-design.md N1/N2/C2。

## 明确不做（本增量）

- 通道 slash 命令、agent 工具、卡片发送、Thread 路由、Run/队列/暂停、真实 Provider adapter、wire/RPC 方法。
- 不改 `tools::task`（todo）任何代码。
