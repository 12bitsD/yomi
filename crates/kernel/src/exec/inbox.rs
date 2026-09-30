//! 执行任务受理登记（exec inbox）：任务 Thread 输入的进程内有序受理。
//!
//! 设计依据 docs/design/chat-flow-technical-design.md C1/D2：
//! - C1：同一平台消息只受理一次，重送查询当前状态；同卡顺序在耗时
//!   准备前确定——`accept` 同步定序，先到先得；
//! - D2：受理只保证同一运行进程——`ExecInbox` 是纯内存结构，重启
//!   清空，卡片不得把旧输入显示为仍已排队（见
//!   `crate::channels::cards::taskcard` 的如实标注）。
//!
//! P2 才在本结构之上加暂停/派发；本增量只受理、定序、去重。

use crate::types::ExecTaskId;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use std::collections::VecDeque;
use std::sync::Arc;

/// 一条已受理输入（原文 + 附件键，不做任何改写）
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptedInput {
    /// 任务内受理序号（从 1 起，先到先得）
    pub seq: u64,
    /// 平台消息 id（去重凭据：同 `msg_id` 只受理一次，C1）
    pub msg_id: String,
    pub sender_open_id: String,
    pub text: String,
    pub image_keys: Vec<String>,
    pub accepted_at: DateTime<Utc>,
}

/// `accept` 的结果：新受理（带序号）或重复送达
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcceptOutcome {
    Accepted { seq: u64 },
    Duplicate,
}

/// 任务输入受理表（进程内语义，重启清空——D2，不做持久化）。
///
/// `DashMap` entry 提供 per-task 互斥：去重检查与定序 push 是一次原子
/// 操作，并发重送与同卡并发新输入都不会乱序或双收。
///
/// `Arc` 内核 + `Clone`：Kernel 与 `ExecScheduler` 共享同一实例
/// （增量 3 调度器直接消费本队列），克隆句柄指向同一受理表。
#[derive(Debug, Default, Clone)]
pub struct ExecInbox {
    inner: Arc<DashMap<ExecTaskId, TaskInbox>>,
}

/// 单卡受理状态：队列 + 去重记忆 + 单调序号高水位。后两者在
/// `pop_front` 后保留（进程内语义，D2）——受理是一次性事实（C1），
/// 已消费输入的重送仍是 Duplicate，序号不因队空而重置。去重记忆
/// 也含「仅登记未入队」的跨进程重送项（增量 9 `note_seen`）。
#[derive(Debug, Default)]
struct TaskInbox {
    queue: VecDeque<AcceptedInput>,
    /// 已受理过的 `msg_id`（含已弹出）
    seen: std::collections::HashSet<String>,
    /// 最近一次受理序号（0 = 尚无输入；下一条 = `last_seq` + 1）
    last_seq: u64,
}

impl ExecInbox {
    pub fn new() -> Self {
        Self::default()
    }

    /// 受理一条任务 Thread 输入。`msg_id` 已在列（含已弹出的历史，
    /// C1：受理是一次性事实）→ `Duplicate`（重送不扩大副作用）；
    /// 否则定序 push 并返回受理序号（进程内单调，弹出不清零）。
    pub fn accept(
        &self,
        task_id: &ExecTaskId,
        msg_id: impl Into<String>,
        sender_open_id: impl Into<String>,
        text: impl Into<String>,
        image_keys: Vec<String>,
    ) -> AcceptOutcome {
        let msg_id = msg_id.into();
        let mut entry = entry_or_default(&self.inner, task_id);
        if entry.seen.contains(&msg_id) {
            return AcceptOutcome::Duplicate;
        }
        entry.seen.insert(msg_id.clone());
        let seq = entry.last_seq + 1;
        entry.last_seq = seq;
        entry.queue.push_back(AcceptedInput {
            seq,
            msg_id,
            sender_open_id: sender_open_id.into(),
            text: text.into(),
            image_keys,
            accepted_at: Utc::now(),
        });
        AcceptOutcome::Accepted { seq }
    }

    /// 当前已受理条数（卡片「本进程已受理输入 n 条」的数据源）。
    pub fn len(&self, task_id: &ExecTaskId) -> usize {
        self.inner.get(task_id).map_or(0, |e| e.queue.len())
    }

    /// 队首输入（不取出——C3 资格检查与派发准备用；队首失败保留
    /// 顺序位置，明确重试或撤回后才让后项继续）。
    pub fn peek_front(&self, task_id: &ExecTaskId) -> Option<AcceptedInput> {
        self.inner
            .get(task_id)
            .and_then(|e| e.queue.front().cloned())
    }

    /// 取出队首。唯一调用方是调度器在原生确认开始之后（增量 3）：
    /// 确认前绝不 pop，未确认失败的输入不跳过（N3/C3）。去重记忆
    /// 与序号水位不随弹出清除。
    pub fn pop_front(&self, task_id: &ExecTaskId) -> Option<AcceptedInput> {
        self.inner
            .get_mut(task_id)
            .and_then(|mut e| e.queue.pop_front())
    }

    /// 进程内去重记忆查询（C1：受理是一次性事实——含已弹出项
    /// 与仅登记未入队的跨进程重送项）。增量 9 起受理入口在凭据
    /// 核对前先查此项（`ExecScheduler::accept_input`）。
    pub fn is_seen(&self, task_id: &ExecTaskId, msg_id: &str) -> bool {
        self.inner
            .get(task_id)
            .is_some_and(|e| e.seen.contains(msg_id))
    }

    /// 仅登记去重记忆、不入队（增量 9，C1/N12：受理凭据核对判
    /// 定「上一进程生命周期受理过」的收口——该消息不入队、不
    /// 派发，但受理是一次性事实：登记后本进程内后续重送仍是
    /// Duplicate，静默不再产生任何可见动作）。序号水位不动。
    pub fn note_seen(&self, task_id: &ExecTaskId, msg_id: &str) {
        let mut entry = entry_or_default(&self.inner, task_id);
        entry.seen.insert(msg_id.to_string());
    }

    #[cfg(test)]
    pub fn is_empty(&self, task_id: &ExecTaskId) -> bool {
        self.len(task_id) == 0
    }

    /// 测试用：取出某任务当前已受理输入的有序快照。
    #[cfg(test)]
    pub fn snapshot(&self, task_id: &ExecTaskId) -> Vec<AcceptedInput> {
        self.inner
            .get(task_id)
            .map_or_else(Vec::new, |e| e.queue.iter().cloned().collect())
    }
}

/// `DashMap::entry` 的 `ExecTaskId` 键版本（单独成函数只为让
/// `accept` 的借用链一目了然）。
fn entry_or_default<'a>(
    map: &'a DashMap<ExecTaskId, TaskInbox>,
    task_id: &ExecTaskId,
) -> dashmap::mapref::one::RefMut<'a, ExecTaskId, TaskInbox> {
    map.entry(task_id.clone()).or_default()
}

#[cfg(test)]
#[path = "inbox_test.rs"]
mod tests;
