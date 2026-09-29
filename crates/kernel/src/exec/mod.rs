//! 执行任务域（exec registry）：chat-flow W1 的执行任务登记核心。
//!
//! 设计依据 docs/design/chat-flow-technical-design.md N1/N2/C2：
//! - N1：执行任务是一等身份载体——登记、去重、归档都落在本模块；
//! - N2：原生 Session 绑定必须显式持久化；「合法新任务尚未初始化」
//!   （`BindingState::Uninitialized`）与「旧绑定不存在/损坏」
//!   （`BindingState::Broken`）严格区分，后者明确失败，绝不自动新建；
//! - C2：创建分步——登记任务并固定 Provider（本模块）→ 卡片/Thread
//!   （增量 2 回填 `thread_root_msg_id` / `card_msg_id`）→ 原生
//!   Session 绑定持久化 → 才允许执行。
//!
//! 增量 3 起本域同时承载按卡运行控制（`run`/`adapter`/`scheduler`），
//! 设计依据 N2/N3/N6/C3/C6/D3/D11（详见各子模块文档）：lane Mutex
//! 是每卡开始/暂停/停止/恢复顺序的唯一裁定者，C3 五条件裁定派发，
//! 停止中≠已停止，恢复不重试已停止轮，名额统一分配、按卡隔离。
//!
//! 增量 5 起承载 Run 事实与结果正文持久化（`facts`，设计依据
//! N7/N9/C7）：终态单向、正文一 Run 一份、迟到结果不猜最新轮。
//!
//! 命名刻意避开 `crate::tools::task`（todo 工具域），两者互不相关。

use crate::types::{ExecTaskId, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[cfg(test)]
pub mod acp_harness;
pub mod adapter;
pub mod facts;
pub mod inbox;
pub mod run;
pub mod scheduler;
pub mod store;
pub use adapter::{
    AdapterNotice, ExecAdapter, ExecAdapterSink, SimAdapter, SimControl, TerminalKind,
    TerminalNotice,
};
pub use facts::{ExecFactStore, ExecResultRow, ExecRunRow, SqliteExecFactStore};
pub use inbox::{AcceptOutcome, AcceptedInput, ExecInbox};
pub use run::{RunRecord, RunStatus};
pub use scheduler::{ExecEvent, ExecScheduler, LaneSnapshot, ResumeOutcome, StopOutcome};
pub use store::SqliteExecTaskStore;

/// 执行 Provider（原生 agent 后端；P1 全程用仿真创建原生 Session）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecProvider {
    Kimi,
    Codex,
}

impl ExecProvider {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Kimi => "kimi",
            Self::Codex => "codex",
        }
    }

    /// DB 解析：未知值 warn 后保守回退 `Kimi`（仅防御损坏行，正常
    /// 路径不会出现）。
    fn from_str_lossy(s: &str) -> Self {
        match s {
            "kimi" => Self::Kimi,
            "codex" => Self::Codex,
            other => {
                tracing::warn!(
                    other,
                    "unknown exec provider in db row, falling back to kimi"
                );
                Self::Kimi
            }
        }
    }
}

impl std::fmt::Display for ExecProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for ExecProvider {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "kimi" => Ok(Self::Kimi),
            "codex" => Ok(Self::Codex),
            _ => Err(format!("Invalid exec provider: {s}")),
        }
    }
}

/// 原生 Session 绑定状态（三态机）
///
/// `Uninitialized`：合法新任务尚未初始化（C2 分步创建的中间态）；
/// `Bound`：原生 Session 已持久化绑定，才允许执行；
/// `Broken`：旧绑定不存在/损坏——明确失败态，绝不自动重建（N2/D6）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BindingState {
    Uninitialized,
    Bound,
    Broken,
}

impl BindingState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Uninitialized => "uninitialized",
            Self::Bound => "bound",
            Self::Broken => "broken",
        }
    }

    /// DB 解析：未知值 warn 后按保守方向视为 `Broken`——宁可拒绝
    /// 执行，也不能把无法识别的绑定当作「尚未初始化」放行（N2）。
    fn from_str_lossy(s: &str) -> Self {
        match s {
            "uninitialized" => Self::Uninitialized,
            "bound" => Self::Bound,
            "broken" => Self::Broken,
            other => {
                tracing::warn!(other, "unknown binding state in db row, treating as broken");
                Self::Broken
            }
        }
    }
}

/// 任务生命周期状态（归档不删行，D8）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecTaskStatus {
    Active,
    Archived,
}

impl ExecTaskStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Archived => "archived",
        }
    }

    /// DB 解析：未知值 warn 后回退 `Active`（与列默认值一致）。
    fn from_str_lossy(s: &str) -> Self {
        match s {
            "active" => Self::Active,
            "archived" => Self::Archived,
            other => {
                tracing::warn!(
                    other,
                    "unknown exec task status in db row, falling back to active"
                );
                Self::Active
            }
        }
    }
}

/// 任务来源：`Skill` = nika Skill 调用；`Entry` = 专用执行入口
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecTaskSource {
    Skill,
    Entry,
}

impl ExecTaskSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Skill => "skill",
            Self::Entry => "entry",
        }
    }

    /// DB 解析：未知值 warn 后回退 `Skill`。
    fn from_str_lossy(s: &str) -> Self {
        match s {
            "skill" => Self::Skill,
            "entry" => Self::Entry,
            other => {
                tracing::warn!(
                    other,
                    "unknown exec task source in db row, falling back to skill"
                );
                Self::Skill
            }
        }
    }
}

/// 执行任务登记记录（C2 身份与绑定契约的持久化形态）
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ExecTask {
    pub id: ExecTaskId,
    /// 创建来源通道；RPC/Skill 场景可为 "rpc" 类占位
    pub channel_name: String,
    pub provider: ExecProvider,
    pub status: ExecTaskStatus,
    pub binding: BindingState,
    /// 原生身份（绑定持久化后才有值）
    pub provider_session_id: Option<String>,
    /// 增量 2 回填
    pub thread_root_msg_id: Option<String>,
    /// 增量 2 回填
    pub card_msg_id: Option<String>,
    /// 0 = 尚无卡；L1 换代递增
    pub card_generation: i64,
    /// 任务目标/首轮原文摘要
    pub goal: String,
    pub working_dir: Option<String>,
    /// 操作者 `open_id` 或 rpc 身份
    pub created_by: String,
    pub source: ExecTaskSource,
    pub dedup_key: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// 创建执行任务的输入（同一创建意图重送经 `dedup_key` 收敛到同一任务）
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CreateExecTask {
    pub channel_name: String,
    pub provider: ExecProvider,
    pub goal: String,
    pub working_dir: Option<String>,
    pub created_by: String,
    pub source: ExecTaskSource,
    pub dedup_key: String,
}

/// 执行任务登记 store
#[async_trait]
pub trait ExecTaskStore: Send + Sync {
    /// 登记任务。返回 `(task, created)`：dedup 命中（同
    /// `channel_name` + `dedup_key`）返回既有任务 + `false`，且**不更新**
    /// 既有行（重送不得扩大副作用）；独立的明确新建（不同 `dedup_key`）
    /// 可产生新任务（C2）。
    async fn create(&self, input: &CreateExecTask) -> Result<(ExecTask, bool)>;

    /// 按 id 读取任务（归档后仍可读，归档不删行）。
    async fn get(&self, id: &ExecTaskId) -> Result<Option<ExecTask>>;

    /// 按 dedup 键查找。
    async fn find_by_dedup(&self, channel_name: &str, dedup_key: &str) -> Result<Option<ExecTask>>;

    /// 按 Thread 根消息查找（增量 2 的任务 Thread 分流入口）：任务
    /// 卡即 Thread 锚，`thread_root_msg_id` 命中即「本 Thread 属于
    /// 该任务」，消息确定性分流、不进普通 chat 路径（N1/N2）。
    async fn find_by_thread_root(
        &self,
        channel_name: &str,
        root_msg_id: &str,
    ) -> Result<Option<ExecTask>>;

    /// 持久化原生 Session 绑定。状态机：仅 `Uninitialized -> Bound`
    /// 合法；`Bound` 且同 id → 幂等返回；`Bound` 且不同 id 或
    /// `Broken` → 明确报错（不得静默换绑/重建，N2/D6）。
    async fn bind_provider_session(
        &self,
        id: &ExecTaskId,
        native_session_id: &str,
    ) -> Result<ExecTask>;

    /// 标记绑定损坏（`Bound`/`Uninitialized -> Broken`）；reason 进
    /// tracing（表不加列）。
    async fn mark_broken(&self, id: &ExecTaskId, reason: &str) -> Result<ExecTask>;

    /// 回填 Thread 根消息与卡片消息（增量 2 用，本增量实现+测）。
    async fn set_thread_and_card(
        &self,
        id: &ExecTaskId,
        thread_root_msg_id: &str,
        card_msg_id: &str,
    ) -> Result<ExecTask>;

    /// 归档（`Active -> Archived`）；归档不删行（D8）。
    async fn archive(&self, id: &ExecTaskId) -> Result<ExecTask>;
}
