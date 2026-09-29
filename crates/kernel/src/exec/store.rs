use crate::exec::{
    BindingState, CreateExecTask, ExecProvider, ExecTask, ExecTaskSource, ExecTaskStatus,
    ExecTaskStore,
};
use crate::storage::storage_err;
use crate::types::{ExecTaskId, KernelError, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::sqlite::SqlitePool;

pub struct SqliteExecTaskStore {
    pool: SqlitePool,
}

impl SqliteExecTaskStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// 按 id 读取，不存在 → 明确报错（调用方均为「任务应已存在」的
    /// 写路径；读路径请用 `get`）。
    async fn get_required(&self, id: &ExecTaskId) -> Result<ExecTask> {
        self.get(id)
            .await?
            .ok_or_else(|| KernelError::task(format!("exec task {id} not found")))
    }
}

#[derive(sqlx::FromRow)]
struct ExecTaskDbRow {
    id: String,
    channel_name: String,
    provider: String,
    status: String,
    binding: String,
    provider_session_id: Option<String>,
    thread_root_msg_id: Option<String>,
    card_msg_id: Option<String>,
    card_generation: i64,
    goal: String,
    working_dir: Option<String>,
    created_by: String,
    source: String,
    dedup_key: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl ExecTaskDbRow {
    /// 枚举列↔字符串转换失败走 warn + 保守默认（binding 未知值视为
    /// `Broken`——保守方向，见 `BindingState::from_str_lossy`）。
    fn into_task(self) -> ExecTask {
        ExecTask {
            id: ExecTaskId::from(self.id),
            channel_name: self.channel_name,
            provider: ExecProvider::from_str_lossy(&self.provider),
            status: ExecTaskStatus::from_str_lossy(&self.status),
            binding: BindingState::from_str_lossy(&self.binding),
            provider_session_id: self.provider_session_id,
            thread_root_msg_id: self.thread_root_msg_id,
            card_msg_id: self.card_msg_id,
            card_generation: self.card_generation,
            goal: self.goal,
            working_dir: self.working_dir,
            created_by: self.created_by,
            source: ExecTaskSource::from_str_lossy(&self.source),
            dedup_key: self.dedup_key,
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

#[async_trait]
impl ExecTaskStore for SqliteExecTaskStore {
    async fn create(&self, input: &CreateExecTask) -> Result<(ExecTask, bool)> {
        let now = Utc::now();
        let task = ExecTask {
            id: ExecTaskId::new(),
            channel_name: input.channel_name.clone(),
            provider: input.provider,
            status: ExecTaskStatus::Active,
            binding: BindingState::Uninitialized,
            provider_session_id: None,
            thread_root_msg_id: None,
            card_msg_id: None,
            card_generation: 0,
            goal: input.goal.clone(),
            working_dir: input.working_dir.clone(),
            created_by: input.created_by.clone(),
            source: input.source,
            dedup_key: input.dedup_key.clone(),
            created_at: now,
            updated_at: now,
        };
        // dedup 命中（同 channel_name+dedup_key）→ 返回既有任务且不
        // 改既有行：重送不得扩大副作用。ON CONFLICT DO NOTHING 同时
        // 兜住并发重送的唯一索引竞争。
        let inserted = sqlx::query(
            r"INSERT INTO exec_tasks
               (id, channel_name, provider, status, binding,
                provider_session_id, thread_root_msg_id, card_msg_id, card_generation,
                goal, working_dir, created_by, source, dedup_key, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, 0, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(channel_name, dedup_key) DO NOTHING",
        )
        .bind(task.id.as_str())
        .bind(&task.channel_name)
        .bind(task.provider.as_str())
        .bind(task.status.as_str())
        .bind(task.binding.as_str())
        .bind(&task.goal)
        .bind(&task.working_dir)
        .bind(&task.created_by)
        .bind(task.source.as_str())
        .bind(&task.dedup_key)
        .bind(task.created_at)
        .bind(task.updated_at)
        .execute(&self.pool)
        .await
        .map_err(|e| storage_err(format!("Failed to create exec task: {e}")))?
        .rows_affected();

        if inserted == 1 {
            return Ok((task, true));
        }
        // 命中既有行：以 DB 为准读回（不携带本次输入的任何变更）。
        let existing = self
            .find_by_dedup(&input.channel_name, &input.dedup_key)
            .await?
            .ok_or_else(|| storage_err("exec task dedup conflict but row missing on re-read"))?;
        Ok((existing, false))
    }

    async fn get(&self, id: &ExecTaskId) -> Result<Option<ExecTask>> {
        let row = sqlx::query_as::<_, ExecTaskDbRow>(
            r"SELECT id, channel_name, provider, status, binding,
                     provider_session_id, thread_root_msg_id, card_msg_id, card_generation,
                     goal, working_dir, created_by, source, dedup_key, created_at, updated_at
              FROM exec_tasks WHERE id = ?",
        )
        .bind(id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| storage_err(format!("Failed to get exec task: {e}")))?;

        Ok(row.map(ExecTaskDbRow::into_task))
    }

    async fn find_by_dedup(&self, channel_name: &str, dedup_key: &str) -> Result<Option<ExecTask>> {
        let row = sqlx::query_as::<_, ExecTaskDbRow>(
            r"SELECT id, channel_name, provider, status, binding,
                     provider_session_id, thread_root_msg_id, card_msg_id, card_generation,
                     goal, working_dir, created_by, source, dedup_key, created_at, updated_at
              FROM exec_tasks WHERE channel_name = ? AND dedup_key = ?",
        )
        .bind(channel_name)
        .bind(dedup_key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| storage_err(format!("Failed to find exec task by dedup key: {e}")))?;

        Ok(row.map(ExecTaskDbRow::into_task))
    }

    async fn find_by_thread_root(
        &self,
        channel_name: &str,
        root_msg_id: &str,
    ) -> Result<Option<ExecTask>> {
        let row = sqlx::query_as::<_, ExecTaskDbRow>(
            r"SELECT id, channel_name, provider, status, binding,
                     provider_session_id, thread_root_msg_id, card_msg_id, card_generation,
                     goal, working_dir, created_by, source, dedup_key, created_at, updated_at
              FROM exec_tasks WHERE channel_name = ? AND thread_root_msg_id = ?",
        )
        .bind(channel_name)
        .bind(root_msg_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| storage_err(format!("Failed to find exec task by thread root: {e}")))?;

        Ok(row.map(ExecTaskDbRow::into_task))
    }

    async fn bind_provider_session(
        &self,
        id: &ExecTaskId,
        native_session_id: &str,
    ) -> Result<ExecTask> {
        let current = self.get_required(id).await?;
        match current.binding {
            BindingState::Uninitialized => {
                // 仅 Uninitialized -> Bound 合法；WHERE 带状态守卫防
                // 并发双绑（宁可报错，不得静默换绑）。
                let updated = sqlx::query(
                    r"UPDATE exec_tasks
                       SET binding = ?, provider_session_id = ?, updated_at = ?
                       WHERE id = ? AND binding = ?",
                )
                .bind(BindingState::Bound.as_str())
                .bind(native_session_id)
                .bind(Utc::now())
                .bind(id.as_str())
                .bind(BindingState::Uninitialized.as_str())
                .execute(&self.pool)
                .await
                .map_err(|e| storage_err(format!("Failed to bind provider session: {e}")))?
                .rows_affected();
                if updated == 0 {
                    return Err(KernelError::task(format!(
                        "exec task {id} binding changed concurrently; bind aborted"
                    )));
                }
                self.get_required(id).await
            }
            BindingState::Bound
                if current.provider_session_id.as_deref() == Some(native_session_id) =>
            {
                // 同 id 重绑：幂等返回（无写，不刷 updated_at）。
                Ok(current)
            }
            BindingState::Bound => Err(KernelError::task(format!(
                "exec task {id} already bound to a different provider session; \
                 refusing to rebind"
            ))),
            BindingState::Broken => Err(KernelError::task(format!(
                "exec task {id} binding is broken; refusing to bind \
                 (N2/D6: never auto-recreate)"
            ))),
        }
    }

    async fn mark_broken(&self, id: &ExecTaskId, reason: &str) -> Result<ExecTask> {
        let current = self.get_required(id).await?;
        match current.binding {
            BindingState::Uninitialized | BindingState::Bound => {
                tracing::warn!(
                    task_id = %id,
                    from = current.binding.as_str(),
                    reason,
                    "exec task binding marked broken"
                );
                sqlx::query("UPDATE exec_tasks SET binding = ?, updated_at = ? WHERE id = ?")
                    .bind(BindingState::Broken.as_str())
                    .bind(Utc::now())
                    .bind(id.as_str())
                    .execute(&self.pool)
                    .await
                    .map_err(|e| storage_err(format!("Failed to mark exec task broken: {e}")))?;
                self.get_required(id).await
            }
            BindingState::Broken => {
                // 已 Broken：幂等返回（重复上报损坏不得再产生写）。
                tracing::warn!(
                    task_id = %id,
                    reason,
                    "mark_broken on already-broken exec task (no-op)"
                );
                Ok(current)
            }
        }
    }

    async fn set_thread_and_card(
        &self,
        id: &ExecTaskId,
        thread_root_msg_id: &str,
        card_msg_id: &str,
    ) -> Result<ExecTask> {
        let updated = sqlx::query(
            r"UPDATE exec_tasks
               SET thread_root_msg_id = ?, card_msg_id = ?, updated_at = ?
               WHERE id = ?",
        )
        .bind(thread_root_msg_id)
        .bind(card_msg_id)
        .bind(Utc::now())
        .bind(id.as_str())
        .execute(&self.pool)
        .await
        .map_err(|e| storage_err(format!("Failed to set exec task thread/card: {e}")))?
        .rows_affected();
        if updated == 0 {
            return Err(KernelError::task(format!("exec task {id} not found")));
        }
        self.get_required(id).await
    }

    async fn archive(&self, id: &ExecTaskId) -> Result<ExecTask> {
        let current = self.get_required(id).await?;
        // 已归档：幂等返回（归档不删行，D8）。
        if current.status == ExecTaskStatus::Archived {
            return Ok(current);
        }
        sqlx::query("UPDATE exec_tasks SET status = ?, updated_at = ? WHERE id = ?")
            .bind(ExecTaskStatus::Archived.as_str())
            .bind(Utc::now())
            .bind(id.as_str())
            .execute(&self.pool)
            .await
            .map_err(|e| storage_err(format!("Failed to archive exec task: {e}")))?;
        self.get_required(id).await
    }
}

#[cfg(test)]
#[path = "store_test.rs"]
mod tests;
