//! 当前轮问答请求模型（chat-flow 增量 10）：Provider 在当前 Run 内
//! 明确发出的原生提问/授权请求的一等契约。
//!
//! 设计依据 docs/design/chat-flow-technical-design.md N5/C5/D10：
//! - N5：只登记 Provider 明确发出、当前 Run 仍在等待的原生请求；
//!   模型普通正文提问不产生伪请求；
//! - C5：回答只在 task/run/request 三者匹配且请求仍有效时被接收；
//!   一次请求只形成一个最终回应；「已提交回答」≠「Provider 已接收
//!   并继续」（Submitted ≠ Resolved，以 adapter 确认为准）；拒绝
//!   走原生拒绝路径，不变成同意；过期/终态/停止/重启后请求失效
//!   ——不批准新操作、不转成新 Prompt；断线不自动批准；
//! - D10：请求是进程内语义（随进程生命周期），不做持久化——重启
//!   后全部失效，由 Provider 重附着核实（P7 范围）。

use crate::types::{ExecRequestId, ExecTaskId, RunId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 请求种类（N5：授权与自由提问同契约，区分呈现）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecRequestKind {
    /// 工具/操作授权（如 ACP `session/request_permission`）
    Permission,
    /// Provider 结构化提问
    Question,
}

/// 选项种类（原生语义直映射——reject 类按原生呈现，不美化）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecOptionKind {
    AllowOnce,
    AllowAlways,
    RejectOnce,
    /// 原生侧的其他种类（含 `reject_always` 等）：如实保留，不猜测语义。
    Other,
}

impl ExecOptionKind {
    /// 原生 kind 字符串 → 选项种类（未知值落 `Other`，不丢弃选项）。
    pub fn from_native(kind: &str) -> Self {
        match kind {
            "allow_once" => Self::AllowOnce,
            "allow_always" => Self::AllowAlways,
            "reject_once" => Self::RejectOnce,
            _ => Self::Other,
        }
    }
}

/// 一个可选项（授权/提问的候答）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ExecRequestOption {
    /// 原生选项身份（回答时原样回传）
    pub option_id: String,
    /// 原生选项文案（不美化）
    pub label: String,
    pub kind: ExecOptionKind,
}

/// 请求状态机（C5）。
///
/// `Pending`：已登记、等待用户回答；`Submitted`：回答已提交给
/// adapter、执行方确认未回（已提交≠已接收）；`Resolved`：adapter
/// 确认回答已交付（最终回应，一次请求只此一份）；`Invalidated`：
/// run 终态/被替换后失效——后续回答返回 `Invalid`，不批准任何新
/// 操作、不转成新 Prompt。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecRequestStatus {
    Pending,
    Submitted,
    Resolved,
    Invalidated,
}

impl ExecRequestStatus {
    /// toast 如实告知用的中文短标签。
    pub fn label(self) -> &'static str {
        match self {
            Self::Pending => "待回应",
            Self::Submitted => "已提交",
            Self::Resolved => "已确认",
            Self::Invalidated => "已失效",
        }
    }
}

/// 一条当前轮问答请求（进程内事实，D10：不持久化）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ExecRequest {
    pub request_id: ExecRequestId,
    pub task_id: ExecTaskId,
    pub run_id: RunId,
    /// 原生请求身份（回答时经 adapter 原样回传；ACP 即 JSON-RPC
    /// 请求 id 的序列化形态）
    pub native_ref: String,
    pub kind: ExecRequestKind,
    /// 问题/授权摘要（原生原文，不改写）
    pub prompt_text: String,
    pub options: Vec<ExecRequestOption>,
    pub status: ExecRequestStatus,
    pub created_at: DateTime<Utc>,
}

/// 回答载荷（调度器 → adapter）。adapter 能力不支持某形态时如实
/// 返回错误，不伪装（C5：拒绝走原生拒绝路径，自由文本不支持即
/// 报错，不转成别的）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestOutcome {
    /// 选定一个原生选项（授权通过/拒绝选项都走这里——拒绝是原生
    /// 拒绝选项，不变成同意）
    Selected { option_id: String },
    /// 原生整体拒绝（无选项可表达时）
    Rejected,
    /// 自由文本回答（能力矩阵不支持时 adapter 返回错误）
    FreeText { text: String },
}
