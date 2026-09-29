//! Run 事实与结果正文的持久化（chat-flow W2 增量 5）。
//!
//! 设计依据 docs/design/chat-flow-technical-design.md N7/N9/C7：
//! - N7（方案 A）：关键事实持久化，快照由其派生——每轮 Run 一行
//!   （开始/终态/起止时刻），查询读事实而非推断事件流；
//! - N9（方案 A）：按 Run 保存 Agent 原始完整正文与归属元数据，
//!   先保存再公布——一 Run 一份权威正文（`exec_results.run_id` 主
//!   键），重复上报 INSERT OR IGNORE 不覆盖；
//! - 单向纪律：终态一次性写入（`terminal_kind IS NULL` 才写），旧
//!   事件不得覆盖新 Run；迟到结果按 native id 反查归属（调度器
//!   侧），反查不到绝不猜最新轮。
//!
//! 增量 6 追加（设计依据 N11/N12/C1/D9）：
//! - 受理凭据（`exec_acceptance`）：记录「收到什么、是否开始」的
//!   最小去重账本——重启后旧输入重送据此区分「上一进程生命周期
//!   受理过」，明确回复未恢复、未重新执行；不是待执行队列持久
//!   化，绝不用于重放旧输入（D2）；
//! - 启动核对（N11）：上一进程生命周期未闭合的 Run 如实标
//!   `interrupted`——不伪造终态（`terminal_kind`/`ended_at` 留
//!   NULL），不凭旧 running 显示正常。
//!
//! 本模块只有 store：写入方是调度器（`exec/scheduler.rs`），查询
//! 方是只读工具（`tools/task_status.rs`）与卡面渲染。

use crate::exec::adapter::TerminalKind;
use crate::exec::run::{RunRecord, RunStatus};
use crate::storage::storage_err;
use crate::types::{ExecTaskId, Result, RunId};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::sqlite::SqlitePool;

/// `exec_runs` 的一行（N7 事实视图；`status`/`terminal_kind` 以字
/// 符串原样存取——行是写时刻的如实快照，不做枚举回译）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecRunRow {
    pub run_id: RunId,
    pub task_id: ExecTaskId,
    pub input_seq: u64,
    pub status: String,
    pub text: String,
    pub image_keys: Vec<String>,
    /// 已确认的终态种类（completed/failed/cancelled；None = 未终态）。
    /// 单向：一旦写入不再改变。
    pub terminal_kind: Option<String>,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// `exec_results` 的一行（N9 权威正文；`body` 是 Agent 原始完整正
/// 文，不改写）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecResultRow {
    pub run_id: RunId,
    pub task_id: ExecTaskId,
    pub input_seq: u64,
    pub body: String,
    pub body_bytes: u64,
    /// 归属元数据 JSON（`provider`/`native_session_id`/来源；只规
    /// 范化归属，正文不动）。
    pub meta: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

/// `exec_acceptance` 的一行（增量 6，C1/N12 受理凭据：最小去重
/// 账本——「收到什么、是否开始」，不是可重放的执行队列）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecAcceptanceRow {
    pub channel_name: String,
    pub msg_id: String,
    pub task_id: ExecTaskId,
    pub accepted_at: DateTime<Utc>,
    /// 是否已被派发过（N12「是否开始」）：原生确认开始或 Starting
    /// 中终态消费输入时由调度器标记。
    pub started: bool,
}

/// Run 事实 + 结果正文 store。
#[async_trait]
pub trait ExecFactStore: Send + Sync {
    /// Run 开始事实（INSERT；同 `run_id` 重入 IGNORE——重放不重
    /// 复建行）。
    async fn run_started(&self, run: &RunRecord) -> Result<()>;

    /// 终态事实（单向：仅当 `terminal_kind IS NULL` 才写——旧事件
    /// 不得覆盖已确认终态）。`status` 同步到 Run 行；重复终态上报
    /// 不改正文外的任何终态字段。
    async fn run_terminal(
        &self,
        run_id: &RunId,
        status: RunStatus,
        kind: TerminalKind,
        ended_at: DateTime<Utc>,
    ) -> Result<()>;

    /// 保存一轮的权威正文（INSERT OR IGNORE；返回 false = 该 Run
    /// 已有权威正文——重复上报不覆盖，先保存者为准）。
    async fn save_result(
        &self,
        run_id: &RunId,
        task_id: &ExecTaskId,
        input_seq: u64,
        body: &str,
        meta: &serde_json::Value,
    ) -> Result<bool>;

    /// 某任务的全部 Run 行（按 `input_seq` 升序；查询工具用）。
    async fn runs_for(&self, task_id: &ExecTaskId) -> Result<Vec<ExecRunRow>>;

    /// 按 `run_id` 取权威正文（`task_result` 工具用）。
    async fn result_for(&self, run_id: &RunId) -> Result<Option<ExecResultRow>>;

    /// 任务最新一份已保存正文（按 `input_seq` 取大；卡面结果行
    /// 与 `task_result` 缺省轮用）。
    async fn latest_result(&self, task_id: &ExecTaskId) -> Result<Option<ExecResultRow>>;

    /// 有活动事实的任务候选（有 Run 事实的任务 id 集合；`None` =
    /// 不限通道——无通道路由会话的「全部」形态）。无 id 查询返回
    /// 候选——范围不明确不挑最近猜（N7）。
    async fn list_tasks_with_activity(&self, channel_name: Option<&str>)
        -> Result<Vec<ExecTaskId>>;

    // ── 增量 6：受理凭据（C1/N12）与启动核对（N11）──────────────

    /// 记录受理凭据（INSERT OR IGNORE；返回 false = 该消息已受理
    /// 过——凭据先于本进程存在时，调用方按「上一进程生命周期受理
    /// 过」处理：不重新入队、不派发）。
    async fn record_acceptance(
        &self,
        channel_name: &str,
        msg_id: &str,
        task_id: &ExecTaskId,
    ) -> Result<bool>;

    /// 标记该受理已开始（N12「是否开始」；幂等——原生确认开始
    /// 与 Starting 中终态消费两处都可能到达）。
    async fn mark_started(&self, channel_name: &str, msg_id: &str) -> Result<()>;

    /// 读取受理凭据（重启后重送的「当前处理状态」查询用）。
    async fn acceptance_for(
        &self,
        channel_name: &str,
        msg_id: &str,
    ) -> Result<Option<ExecAcceptanceRow>>;

    /// 启动核对（N11）：上一进程生命周期未闭合的 Run
    /// （starting/running/stopping）如实标 `interrupted`——中断是
    /// 事实状态，`terminal_kind`/`ended_at` 留 NULL（不伪造终
    /// 态）。返回标记行数（调度器记 tracing）。
    async fn mark_interrupted_open_runs(&self) -> Result<u64>;

    /// 本通道有卡的活动任务（重启后逐一刷新卡面用——受理数归
    /// 零、中断轮可见、旧队列不显示仍可恢复，N11/D9）。
    async fn active_tasks_with_card(&self, channel_name: &str) -> Result<Vec<ExecTaskId>>;

    /// 任务最近一轮 Run 事实（按 `input_seq` 取大；卡面「已中断」
    /// 行的数据源——重启后 lane 无 current，不凭旧 binding 显示
    /// 一切正常）。
    async fn latest_run(&self, task_id: &ExecTaskId) -> Result<Option<ExecRunRow>>;
}

/// `SQLite` 实现（镜像 `SqliteExecTaskStore` 模式）。
pub struct SqliteExecFactStore {
    pool: SqlitePool,
}

impl SqliteExecFactStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[derive(sqlx::FromRow)]
struct ExecRunDbRow {
    run_id: String,
    task_id: String,
    input_seq: i64,
    status: String,
    text: String,
    image_keys: String,
    terminal_kind: Option<String>,
    started_at: DateTime<Utc>,
    ended_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
}

impl ExecRunDbRow {
    fn into_row(self) -> ExecRunRow {
        ExecRunRow {
            run_id: RunId::from(self.run_id),
            task_id: ExecTaskId::from(self.task_id),
            input_seq: u64::try_from(self.input_seq).unwrap_or_default(),
            status: self.status,
            text: self.text,
            image_keys: serde_json::from_str(&self.image_keys).unwrap_or_else(|e| {
                tracing::warn!(error = %e, "exec_runs.image_keys not JSON; treating as empty");
                Vec::new()
            }),
            terminal_kind: self.terminal_kind,
            started_at: self.started_at,
            ended_at: self.ended_at,
            created_at: self.created_at,
        }
    }
}

#[derive(sqlx::FromRow)]
struct ExecResultDbRow {
    run_id: String,
    task_id: String,
    input_seq: i64,
    body: String,
    body_bytes: i64,
    meta: String,
    created_at: DateTime<Utc>,
}

impl ExecResultDbRow {
    fn into_row(self) -> ExecResultRow {
        ExecResultRow {
            run_id: RunId::from(self.run_id),
            task_id: ExecTaskId::from(self.task_id),
            input_seq: u64::try_from(self.input_seq).unwrap_or_default(),
            body: self.body,
            body_bytes: u64::try_from(self.body_bytes).unwrap_or_default(),
            meta: serde_json::from_str(&self.meta).unwrap_or_else(|e| {
                tracing::warn!(error = %e, "exec_results.meta not JSON; treating as {{}}");
                serde_json::json!({})
            }),
            created_at: self.created_at,
        }
    }
}

#[derive(sqlx::FromRow)]
struct ExecAcceptanceDbRow {
    channel_name: String,
    msg_id: String,
    task_id: String,
    accepted_at: DateTime<Utc>,
    started: i64,
}

impl ExecAcceptanceDbRow {
    fn into_row(self) -> ExecAcceptanceRow {
        ExecAcceptanceRow {
            channel_name: self.channel_name,
            msg_id: self.msg_id,
            task_id: ExecTaskId::from(self.task_id),
            accepted_at: self.accepted_at,
            started: self.started != 0,
        }
    }
}

/// `RunStatus` 的 DB 字符串形态（与 serde `snake_case` 一致；
/// 手写而不用 serde：行语义是「写时刻的如实快照」，不引序列化
/// 依赖进 SQL 层）。
fn status_str(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Starting => "starting",
        RunStatus::Running => "running",
        RunStatus::WaitingRequest => "waiting_request",
        RunStatus::Stopping => "stopping",
        RunStatus::Completed => "completed",
        RunStatus::Failed => "failed",
        RunStatus::Stopped => "stopped",
        RunStatus::Unknown => "unknown",
    }
}

/// `TerminalKind` 的 DB 字符串形态（`terminal_kind` 列取值
/// completed/failed/cancelled）。
fn terminal_kind_str(kind: TerminalKind) -> &'static str {
    match kind {
        TerminalKind::Completed => "completed",
        TerminalKind::Failed => "failed",
        TerminalKind::Cancelled => "cancelled",
    }
}

#[async_trait]
impl ExecFactStore for SqliteExecFactStore {
    async fn run_started(&self, run: &RunRecord) -> Result<()> {
        let image_keys = serde_json::to_string(&run.image_keys)
            .map_err(|e| storage_err(format!("serialize run image_keys: {e}")))?;
        // INSERT OR IGNORE：同 run_id 重入（重放/重试路径）不重复
        // 建行，也不改写既有行——首次写入为准。
        sqlx::query(
            r"INSERT OR IGNORE INTO exec_runs
               (run_id, task_id, input_seq, status, text, image_keys, started_at, ended_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(run.run_id.as_str())
        .bind(run.task_id.as_str())
        .bind(i64::try_from(run.input_seq).unwrap_or(i64::MAX))
        .bind(status_str(run.status))
        .bind(&run.text)
        .bind(image_keys)
        .bind(run.started_at)
        .bind(run.ended_at)
        .execute(&self.pool)
        .await
        .map_err(|e| storage_err(format!("Failed to record exec run start: {e}")))?;
        Ok(())
    }

    async fn run_terminal(
        &self,
        run_id: &RunId,
        status: RunStatus,
        kind: TerminalKind,
        ended_at: DateTime<Utc>,
    ) -> Result<()> {
        // 终态单向：`terminal_kind IS NULL` 守卫——已确认终态后旧
        // 事件（重复/迟到上报）不得改写 kind/ended_at（N7）。
        sqlx::query(
            r"UPDATE exec_runs
               SET status = ?, terminal_kind = ?, ended_at = ?
               WHERE run_id = ? AND terminal_kind IS NULL",
        )
        .bind(status_str(status))
        .bind(terminal_kind_str(kind))
        .bind(ended_at)
        .bind(run_id.as_str())
        .execute(&self.pool)
        .await
        .map_err(|e| storage_err(format!("Failed to record exec run terminal: {e}")))?;
        Ok(())
    }

    async fn save_result(
        &self,
        run_id: &RunId,
        task_id: &ExecTaskId,
        input_seq: u64,
        body: &str,
        meta: &serde_json::Value,
    ) -> Result<bool> {
        let meta_str = serde_json::to_string(meta)
            .map_err(|e| storage_err(format!("serialize result meta: {e}")))?;
        // INSERT OR IGNORE：一 Run 一份权威正文（N9）——重复上报
        // 返回 false，绝不覆盖先保存的正文。
        let inserted = sqlx::query(
            r"INSERT OR IGNORE INTO exec_results
               (run_id, task_id, input_seq, body, body_bytes, meta)
               VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(run_id.as_str())
        .bind(task_id.as_str())
        .bind(i64::try_from(input_seq).unwrap_or(i64::MAX))
        .bind(body)
        .bind(i64::try_from(body.len()).unwrap_or(i64::MAX))
        .bind(meta_str)
        .execute(&self.pool)
        .await
        .map_err(|e| storage_err(format!("Failed to save exec result: {e}")))?
        .rows_affected();
        Ok(inserted == 1)
    }

    async fn runs_for(&self, task_id: &ExecTaskId) -> Result<Vec<ExecRunRow>> {
        let rows = sqlx::query_as::<_, ExecRunDbRow>(
            r"SELECT run_id, task_id, input_seq, status, text, image_keys,
                     terminal_kind, started_at, ended_at, created_at
              FROM exec_runs WHERE task_id = ? ORDER BY input_seq ASC",
        )
        .bind(task_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| storage_err(format!("Failed to list exec runs: {e}")))?;
        Ok(rows.into_iter().map(ExecRunDbRow::into_row).collect())
    }

    async fn result_for(&self, run_id: &RunId) -> Result<Option<ExecResultRow>> {
        let row = sqlx::query_as::<_, ExecResultDbRow>(
            r"SELECT run_id, task_id, input_seq, body, body_bytes, meta, created_at
              FROM exec_results WHERE run_id = ?",
        )
        .bind(run_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| storage_err(format!("Failed to get exec result: {e}")))?;
        Ok(row.map(ExecResultDbRow::into_row))
    }

    async fn latest_result(&self, task_id: &ExecTaskId) -> Result<Option<ExecResultRow>> {
        // 「最新」= input_seq 最大（序号是受理定序的唯一事实源）。
        let row = sqlx::query_as::<_, ExecResultDbRow>(
            r"SELECT run_id, task_id, input_seq, body, body_bytes, meta, created_at
              FROM exec_results WHERE task_id = ?
              ORDER BY input_seq DESC LIMIT 1",
        )
        .bind(task_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| storage_err(format!("Failed to get latest exec result: {e}")))?;
        Ok(row.map(ExecResultDbRow::into_row))
    }

    async fn list_tasks_with_activity(
        &self,
        channel_name: Option<&str>,
    ) -> Result<Vec<ExecTaskId>> {
        let rows: Vec<(String,)> = match channel_name {
            Some(channel) => {
                sqlx::query_as(
                    r"SELECT DISTINCT r.task_id FROM exec_runs r
                  JOIN exec_tasks t ON t.id = r.task_id
                  WHERE t.channel_name = ?
                  ORDER BY r.task_id ASC",
                )
                .bind(channel)
                .fetch_all(&self.pool)
                .await
            }
            None => {
                sqlx::query_as(r"SELECT DISTINCT task_id FROM exec_runs ORDER BY task_id ASC")
                    .fetch_all(&self.pool)
                    .await
            }
        }
        .map_err(|e| storage_err(format!("Failed to list exec tasks with activity: {e}")))?;
        Ok(rows.into_iter().map(|(id,)| ExecTaskId::from(id)).collect())
    }

    async fn record_acceptance(
        &self,
        channel_name: &str,
        msg_id: &str,
        task_id: &ExecTaskId,
    ) -> Result<bool> {
        // INSERT OR IGNORE：受理是一次性事实（C1）——同一平台消息
        // 重复记录不改写既有凭据；返回 false 即「凭据先于本次存
        // 在」，由调用方区分进程内重送与跨进程重送。
        let inserted = sqlx::query(
            r"INSERT OR IGNORE INTO exec_acceptance (channel_name, msg_id, task_id)
               VALUES (?, ?, ?)",
        )
        .bind(channel_name)
        .bind(msg_id)
        .bind(task_id.as_str())
        .execute(&self.pool)
        .await
        .map_err(|e| storage_err(format!("Failed to record exec acceptance: {e}")))?
        .rows_affected();
        Ok(inserted == 1)
    }

    async fn mark_started(&self, channel_name: &str, msg_id: &str) -> Result<()> {
        // 幂等：无 started 守卫——重复标记同义，先写者为准即可。
        sqlx::query(
            r"UPDATE exec_acceptance SET started = 1
               WHERE channel_name = ? AND msg_id = ?",
        )
        .bind(channel_name)
        .bind(msg_id)
        .execute(&self.pool)
        .await
        .map_err(|e| storage_err(format!("Failed to mark exec acceptance started: {e}")))?;
        Ok(())
    }

    async fn acceptance_for(
        &self,
        channel_name: &str,
        msg_id: &str,
    ) -> Result<Option<ExecAcceptanceRow>> {
        let row = sqlx::query_as::<_, ExecAcceptanceDbRow>(
            r"SELECT channel_name, msg_id, task_id, accepted_at, started
              FROM exec_acceptance WHERE channel_name = ? AND msg_id = ?",
        )
        .bind(channel_name)
        .bind(msg_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| storage_err(format!("Failed to get exec acceptance: {e}")))?;
        Ok(row.map(ExecAcceptanceDbRow::into_row))
    }

    async fn mark_interrupted_open_runs(&self) -> Result<u64> {
        // N11：只动未闭合行（starting/running/stopping）；终态行不
        // 动，`terminal_kind`/`ended_at` 留 NULL——中断是事实状
        // 态，不伪造终态种类或结束时刻。
        let marked = sqlx::query(
            r"UPDATE exec_runs SET status = 'interrupted'
               WHERE status IN ('starting', 'running', 'waiting_request', 'stopping')",
        )
        .execute(&self.pool)
        .await
        .map_err(|e| storage_err(format!("Failed to mark interrupted exec runs: {e}")))?
        .rows_affected();
        Ok(marked)
    }

    async fn active_tasks_with_card(&self, channel_name: &str) -> Result<Vec<ExecTaskId>> {
        let rows: Vec<(String,)> = sqlx::query_as(
            r"SELECT id FROM exec_tasks
               WHERE channel_name = ? AND status = 'active' AND card_msg_id IS NOT NULL
               ORDER BY id ASC",
        )
        .bind(channel_name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| storage_err(format!("Failed to list active exec tasks with card: {e}")))?;
        Ok(rows.into_iter().map(|(id,)| ExecTaskId::from(id)).collect())
    }

    async fn latest_run(&self, task_id: &ExecTaskId) -> Result<Option<ExecRunRow>> {
        // 「最近」= input_seq 最大（序号是受理定序的唯一事实源）。
        let row = sqlx::query_as::<_, ExecRunDbRow>(
            r"SELECT run_id, task_id, input_seq, status, text, image_keys,
                     terminal_kind, started_at, ended_at, created_at
              FROM exec_runs WHERE task_id = ?
              ORDER BY input_seq DESC LIMIT 1",
        )
        .bind(task_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| storage_err(format!("Failed to get latest exec run: {e}")))?;
        Ok(row.map(ExecRunDbRow::into_row))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::{
        CreateExecTask, ExecProvider, ExecTaskSource, ExecTaskStore, SqliteExecTaskStore,
    };
    use crate::storage::migrations::run_migrations;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn setup() -> (SqliteExecFactStore, ExecTaskId) {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        run_migrations(&pool).await.unwrap();
        let task_store = SqliteExecTaskStore::new(pool.clone());
        let (task, created) = task_store
            .create(&CreateExecTask {
                channel_name: "test".into(),
                provider: ExecProvider::Kimi,
                goal: "g".into(),
                working_dir: None,
                created_by: "ou_t".into(),
                source: ExecTaskSource::Skill,
                dedup_key: "d".into(),
            })
            .await
            .unwrap();
        assert!(created);
        (SqliteExecFactStore::new(pool), task.id)
    }

    fn run(task_id: &ExecTaskId, seq: u64, status: RunStatus) -> RunRecord {
        RunRecord {
            run_id: RunId::new(),
            task_id: task_id.clone(),
            input_seq: seq,
            text: format!("输入 {seq}"),
            image_keys: vec!["k1".into()],
            status,
            started_at: Utc::now(),
            ended_at: None,
        }
    }

    #[tokio::test]
    async fn run_started_is_insert_once_and_terminal_is_one_way() {
        let (store, task_id) = setup().await;
        let r = run(&task_id, 1, RunStatus::Running);

        store.run_started(&r).await.unwrap();
        // 同 run_id 重入 IGNORE：不改写既有行（状态保持首次写入）。
        let mut replayed = r.clone();
        replayed.status = RunStatus::Completed;
        store.run_started(&replayed).await.unwrap();
        let rows = store.runs_for(&task_id).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].status, "running");
        assert_eq!(rows[0].terminal_kind, None);

        // 终态写入后重复终态不改 kind（单向）。
        let ended = Utc::now();
        store
            .run_terminal(
                &r.run_id,
                RunStatus::Completed,
                TerminalKind::Completed,
                ended,
            )
            .await
            .unwrap();
        store
            .run_terminal(
                &r.run_id,
                RunStatus::Failed,
                TerminalKind::Failed,
                Utc::now(),
            )
            .await
            .unwrap();
        let rows = store.runs_for(&task_id).await.unwrap();
        assert_eq!(rows[0].terminal_kind.as_deref(), Some("completed"));
        assert_eq!(rows[0].status, "completed");
        assert_eq!(rows[0].ended_at, Some(ended));
    }

    #[tokio::test]
    async fn save_result_is_insert_once_and_latest_picks_max_seq() {
        let (store, task_id) = setup().await;
        let r1 = run(&task_id, 1, RunStatus::Completed);
        let r2 = run(&task_id, 2, RunStatus::Completed);
        store.run_started(&r1).await.unwrap();
        store.run_started(&r2).await.unwrap();

        // 先保存为准：重复上报返回 false 且不覆盖。
        assert!(store
            .save_result(
                &r1.run_id,
                &task_id,
                1,
                "第一轮正文",
                &serde_json::json!({})
            )
            .await
            .unwrap());
        assert!(!store
            .save_result(
                &r1.run_id,
                &task_id,
                1,
                "被改写的正文",
                &serde_json::json!({})
            )
            .await
            .unwrap());
        assert!(store
            .save_result(
                &r2.run_id,
                &task_id,
                2,
                "第二轮正文",
                &serde_json::json!({"provider": "kimi"}),
            )
            .await
            .unwrap());

        let first = store.result_for(&r1.run_id).await.unwrap().unwrap();
        assert_eq!(first.body, "第一轮正文", "insert-once authoritative");
        assert_eq!(first.body_bytes, "第一轮正文".len() as u64);
        assert_eq!(first.input_seq, 1);
        let second = store.result_for(&r2.run_id).await.unwrap().unwrap();
        assert_eq!(second.meta["provider"], "kimi");

        // latest = input_seq 最大者。
        let latest = store.latest_result(&task_id).await.unwrap().unwrap();
        assert_eq!(latest.input_seq, 2);
        assert_eq!(latest.body, "第二轮正文");
        assert!(store
            .latest_result(&ExecTaskId::new())
            .await
            .unwrap()
            .is_none());

        let ids = store.list_tasks_with_activity(Some("test")).await.unwrap();
        assert_eq!(ids, vec![task_id.clone()]);
        assert!(store
            .list_tasks_with_activity(Some("other"))
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            store.list_tasks_with_activity(None).await.unwrap(),
            vec![task_id]
        );
    }

    // ── 增量 6：受理凭据（C1/N12）───────────────────────────────

    #[tokio::test]
    async fn acceptance_is_insert_once_and_started_marks_idempotently() {
        let (store, task_id) = setup().await;

        // 首次记录 true；同 (channel, msg_id) 重入 false 且不改写
        // 既有凭据（任务归属保持首次写入）。
        assert!(store
            .record_acceptance("test", "m1", &task_id)
            .await
            .unwrap());
        let other_task = ExecTaskId::new();
        assert!(!store
            .record_acceptance("test", "m1", &other_task)
            .await
            .unwrap());
        let row = store
            .acceptance_for("test", "m1")
            .await
            .unwrap()
            .expect("acceptance persisted");
        assert_eq!(row.task_id, task_id, "first writer wins");
        assert!(!row.started);

        // 通道命名空间隔离：同 msg_id 不同通道各自受理。
        assert!(store
            .record_acceptance("other", "m1", &task_id)
            .await
            .unwrap());

        // 标记开始幂等；按通道定位不错标。
        store.mark_started("test", "m1").await.unwrap();
        store.mark_started("test", "m1").await.unwrap();
        let row = store.acceptance_for("test", "m1").await.unwrap().unwrap();
        assert!(row.started);
        let namespaced = store.acceptance_for("other", "m1").await.unwrap().unwrap();
        assert!(!namespaced.started, "other channel untouched");

        assert!(store
            .acceptance_for("test", "ghost")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn interrupted_sweep_marks_only_open_rows() {
        let (store, task_id) = setup().await;
        let r1 = run(&task_id, 1, RunStatus::Running);
        let r2 = run(&task_id, 2, RunStatus::Stopping);
        let r3 = run(&task_id, 3, RunStatus::Completed);
        store.run_started(&r1).await.unwrap();
        store.run_started(&r2).await.unwrap();
        store.run_started(&r3).await.unwrap();
        let ended = Utc::now();
        store
            .run_terminal(
                &r3.run_id,
                RunStatus::Completed,
                TerminalKind::Completed,
                ended,
            )
            .await
            .unwrap();

        let marked = store.mark_interrupted_open_runs().await.unwrap();
        assert_eq!(marked, 2, "only open rows marked");
        // 幂等：再扫一遍无行可标。
        assert_eq!(store.mark_interrupted_open_runs().await.unwrap(), 0);

        let rows = store.runs_for(&task_id).await.unwrap();
        assert_eq!(rows[0].status, "interrupted");
        assert_eq!(rows[1].status, "interrupted");
        for r in &rows[..2] {
            // 不伪造终态：kind/ended_at 留 NULL。
            assert_eq!(r.terminal_kind, None);
            assert_eq!(r.ended_at, None);
        }
        assert_eq!(rows[2].status, "completed", "terminal row untouched");
        assert_eq!(rows[2].terminal_kind.as_deref(), Some("completed"));
        assert_eq!(rows[2].ended_at, Some(ended));

        // latest_run = input_seq 最大者。
        let latest = store.latest_run(&task_id).await.unwrap().unwrap();
        assert_eq!(latest.run_id, r3.run_id);
        assert!(store
            .latest_run(&ExecTaskId::new())
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn active_tasks_with_card_filters_status_and_card() {
        let (store, task_id) = setup().await;
        let task_store = SqliteExecTaskStore::new(store.pool.clone());

        // 无卡活动任务不在列。
        assert!(store
            .active_tasks_with_card("test")
            .await
            .unwrap()
            .is_empty());

        // 有卡活动任务在列。
        let task = task_store
            .set_thread_and_card(&task_id, "card-1", "card-1")
            .await
            .unwrap();
        assert_eq!(task.card_msg_id.as_deref(), Some("card-1"));
        assert_eq!(
            store.active_tasks_with_card("test").await.unwrap(),
            vec![task_id.clone()]
        );
        assert!(store
            .active_tasks_with_card("other")
            .await
            .unwrap()
            .is_empty());

        // 归档任务（不删行）不在列。
        task_store.archive(&task_id).await.unwrap();
        assert!(store
            .active_tasks_with_card("test")
            .await
            .unwrap()
            .is_empty());
    }
}
