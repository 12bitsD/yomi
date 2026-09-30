//! Run 模型（chat-flow 增量 3）：一次「已获得执行资格的输入」的
//! 生命周期记录。
//!
//! 设计依据 docs/design/chat-flow-technical-design.md N3/N6/C6：
//! - N3：状态未知（`Unknown`）不当普通失败跳过——阻止下一轮；
//! - N6/C6：停止中（`Stopping`）≠ 已停止（`Stopped`）——受理取消
//!   与原生终态确认是两个独立事实，竞态以真实自然终态为准。

use crate::types::{ExecTaskId, RunId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Run 状态机。
///
/// `Starting`：已取资格、adapter start 未确认；`Running`：adapter
/// 已确认开始；`WaitingRequest`：执行中且 Provider 有未结问答请
/// 求（增量 10，N5/C5——等回答仍占有本 Session，不能开始下一
/// 轮）；`Stopping`：停止已受理、原生终态未确认（停止中≠已停
/// 止，C6）；`Completed`/`Failed`：已确认自然终态（业务失败不自
/// 动暂停，N3）；`Stopped`：取消已确认；`Unknown`：派发或终态确认
/// 丢失——阻止下一轮（N3）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Starting,
    Running,
    WaitingRequest,
    Stopping,
    Completed,
    Failed,
    Stopped,
    Unknown,
}

impl RunStatus {
    /// 在飞（占用当前 Run 槽位，阻止新派发）：Starting/Running/
    /// WaitingRequest/Stopping。Unknown 虽非在飞，经
    /// `blocked_unknown` 单独阻断。
    pub fn is_live(&self) -> bool {
        matches!(
            self,
            Self::Starting | Self::Running | Self::WaitingRequest | Self::Stopping
        )
    }
}

/// 一次 Run 的事实记录（进程内语义；Run 事实持久化是 P4/W2，
/// 本增量不保存历史 Run——lane 只持有当前/最后一轮）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RunRecord {
    pub run_id: RunId,
    pub task_id: ExecTaskId,
    /// 所消费输入的受理序号（`AcceptedInput::seq`）
    pub input_seq: u64,
    pub text: String,
    pub image_keys: Vec<String>,
    pub status: RunStatus,
    /// 取得执行资格（进入 `Starting`）的时刻
    pub started_at: DateTime<Utc>,
    /// 已确认终态的时刻；Starting/Running/Stopping/Unknown 为 None
    pub ended_at: Option<DateTime<Utc>>,
}
