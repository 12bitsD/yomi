//! `ExecAdapter` 接缝与仿真实现（chat-flow 增量 3）。
//!
//! 设计依据 docs/design/chat-flow-technical-design.md C4/N2：
//! - C4：准备会话返回可持久绑定的原生身份；提交新一轮区分未发送/
//!   可能已发送/原生已确认开始；停止先报受理、再报原生终态——
//!   终态经 [`ExecAdapterSink`] 回调进入调度器，不在调用返回里
//!   冒充；
//! - N2：原生身份必须可持久绑定，恢复时使用原身份，失败不替换。
//!
//! P1 全程只有 [`SimAdapter`]（标注：P3 由真实双 Provider adapter
//! 替换）；生产装配默认挂起模式——无真实 Provider 时不伪造进展。

use crate::exec::{AcceptedInput, ExecTask};
use crate::types::Result;
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::mpsc;

/// 原生终态种类（sink 回调载荷）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalKind {
    /// 自然完成
    Completed,
    /// 业务失败（不自动暂停队列，N3）
    Failed,
    /// 取消已确认
    Cancelled,
}

/// 一条终态上报（原生身份 + 终态种类）。
pub type TerminalNotice = (String, TerminalKind);

/// adapter → 调度器的终态回调口（C4）：构造时注入 adapter 持有；
/// 调度器侧泵循环把每条上报转为 `ExecScheduler::terminal` 调用。
/// 事件只是提示（hint），状态以 lane 锁内事实为准。
#[derive(Clone)]
pub struct ExecAdapterSink(mpsc::UnboundedSender<TerminalNotice>);

impl ExecAdapterSink {
    /// 建一对回调口：sink 给 adapter，receiver 给调度器泵。
    pub fn channel() -> (Self, mpsc::UnboundedReceiver<TerminalNotice>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self(tx), rx)
    }

    /// 上报原生终态（非阻塞；丢不了——unbounded）。
    pub fn terminal(&self, native_session_id: &str, kind: TerminalKind) {
        // 接收方已拆（调度器关停中）时静默丢弃：终态本来就只是提示。
        let _ = self.0.send((native_session_id.to_string(), kind));
    }
}

/// 执行 adapter（原生 agent 后端接缝）。
#[async_trait]
pub trait ExecAdapter: Send + Sync {
    /// 建原生会话，返回可持久绑定的原生身份（N2）。
    async fn create_session(&self, task: &ExecTask) -> Result<String>;

    /// 提交新一轮。返回 Ok = 原生已确认开始（同步 ack）；Err = 未
    /// 确认——可能已发送也可能未发送（C4），调用方按未知处理。
    async fn start_run(&self, native_session_id: &str, input: &AcceptedInput) -> Result<()>;

    /// 请求取消。返回 Ok = 停止请求已受理（≠已停止）；原生终态
    /// 经 sink 异步上报（C6）。
    async fn cancel(&self, native_session_id: &str) -> Result<()>;
}

/// 仿真 adapter（P1 全程使用；P3 由真实 adapter 替换）。
///
/// `Default` = 全挂起模式：不报完成、不报取消确认——生产装配
/// （`Kernel::new`）就用它，无真实 Provider 时不伪造任何进展。
/// 测试旋钮（可注）：`complete_after`（到点经 sink 报 Completed）、
/// `fail_next_start`（下一次 `start_run` 失败一次）、
/// `cancel_never_confirms`（cancel 受理后永不报终态）。
pub struct SimAdapter {
    /// 终态回调口（构造时注入；None = 无从上报，全部挂起）
    sink: Option<ExecAdapterSink>,
    /// Some(d)：`start_run` ack 后 d 经 sink 报 Completed；None =
    /// 挂起直到测试显式 terminal
    complete_after: Option<Duration>,
    /// true：cancel 受理后永不报 Cancelled（慢停止/停止丢失场景）
    cancel_never_confirms: bool,
    /// 下一次 `start_run` 返回 Err 一次（随后自动复位）
    fail_next_start: AtomicBool,
    /// 测试观察口：`create_session` 发出的原生身份（按序）
    created: Mutex<Vec<String>>,
    /// 测试观察口：`start_run` 收到的（原生身份, 输入）（按序）
    started: Mutex<Vec<(String, AcceptedInput)>>,
    /// 测试观察口：cancel 收到的原生身份（按序）
    cancelled: Mutex<Vec<String>>,
}

impl Default for SimAdapter {
    /// 挂起模式（生产默认）：不自己完成、不自己确认取消。
    fn default() -> Self {
        Self {
            sink: None,
            complete_after: None,
            cancel_never_confirms: true,
            fail_next_start: AtomicBool::new(false),
            created: Mutex::new(Vec::new()),
            started: Mutex::new(Vec::new()),
            cancelled: Mutex::new(Vec::new()),
        }
    }
}

impl SimAdapter {
    /// 注入终态回调口（构造时注入的 builder 形态）。
    #[must_use]
    pub fn with_sink(mut self, sink: ExecAdapterSink) -> Self {
        self.sink = Some(sink);
        self
    }

    /// `start_run` ack 后 d 自动报 Completed（需同时注入 sink）。
    #[must_use]
    pub fn with_complete_after(mut self, d: Duration) -> Self {
        self.complete_after = Some(d);
        self
    }

    /// false = cancel 受理后立即经 sink 报 Cancelled（默认 true =
    /// 永不确认，挂起模式）。
    #[must_use]
    pub fn with_cancel_never_confirms(mut self, never: bool) -> Self {
        self.cancel_never_confirms = never;
        self
    }

    /// 让下一次 `start_run` 失败一次（可在调度器装配后经共享引用
    /// 随时武装）。
    pub fn fail_next_start(&self) {
        self.fail_next_start.store(true, Ordering::Release);
    }

    /// 测试观察口：已发出的原生身份。
    pub fn created_sessions(&self) -> Vec<String> {
        self.created.lock().unwrap().clone()
    }

    /// 测试观察口：已收到的 `start_run`（原生身份, 输入）。
    pub fn started_runs(&self) -> Vec<(String, AcceptedInput)> {
        self.started.lock().unwrap().clone()
    }

    /// 测试观察口：已收到的 cancel。
    pub fn cancelled_sessions(&self) -> Vec<String> {
        self.cancelled.lock().unwrap().clone()
    }
}

#[async_trait]
impl ExecAdapter for SimAdapter {
    async fn create_session(&self, _task: &ExecTask) -> Result<String> {
        let id = format!("sim-{}", ulid::Ulid::new());
        self.created.lock().unwrap().push(id.clone());
        Ok(id)
    }

    async fn start_run(&self, native_session_id: &str, input: &AcceptedInput) -> Result<()> {
        if self.fail_next_start.swap(false, Ordering::AcqRel) {
            return Err(crate::types::KernelError::task(
                "sim adapter: start_run failed (fail_next_start armed)",
            ));
        }
        self.started
            .lock()
            .unwrap()
            .push((native_session_id.to_string(), input.clone()));
        if let (Some(d), Some(sink)) = (self.complete_after, &self.sink) {
            let native = native_session_id.to_string();
            let sink = sink.clone();
            // 同步 ack 先到（上面已记录），终态到点经 sink 异步上报。
            tokio::spawn(async move {
                tokio::time::sleep(d).await;
                sink.terminal(&native, TerminalKind::Completed);
            });
        }
        Ok(())
    }

    async fn cancel(&self, native_session_id: &str) -> Result<()> {
        self.cancelled
            .lock()
            .unwrap()
            .push(native_session_id.to_string());
        if !self.cancel_never_confirms {
            if let Some(sink) = &self.sink {
                sink.terminal(native_session_id, TerminalKind::Cancelled);
            }
        }
        Ok(())
    }
}
