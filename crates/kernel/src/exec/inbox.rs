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
#[derive(Debug, Default)]
pub struct ExecInbox {
    inner: DashMap<ExecTaskId, VecDeque<AcceptedInput>>,
}

impl ExecInbox {
    pub fn new() -> Self {
        Self::default()
    }

    /// 受理一条任务 Thread 输入。`msg_id` 已在列 → `Duplicate`（重送
    /// 不扩大副作用）；否则定序 push 并返回受理序号。
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
        if entry.iter().any(|i| i.msg_id == msg_id) {
            return AcceptOutcome::Duplicate;
        }
        let seq = entry.back().map_or(1, |i| i.seq + 1);
        entry.push_back(AcceptedInput {
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
        self.inner.get(task_id).map_or(0, |q| q.len())
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
            .map_or_else(Vec::new, |q| q.iter().cloned().collect())
    }
}

/// `DashMap::entry` 的 `ExecTaskId` 键版本（单独成函数只为让
/// `accept` 的借用链一目了然）。
fn entry_or_default<'a>(
    map: &'a DashMap<ExecTaskId, VecDeque<AcceptedInput>>,
    task_id: &ExecTaskId,
) -> dashmap::mapref::one::RefMut<'a, ExecTaskId, VecDeque<AcceptedInput>> {
    map.entry(task_id.clone()).or_default()
}

#[cfg(test)]
#[path = "inbox_test.rs"]
mod tests;
