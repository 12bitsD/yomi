//! `ExecAdapter` 接缝与仿真实现（chat-flow 增量 3）。
//!
//! 设计依据 docs/design/chat-flow-technical-design.md C4/N2：
//! - C4：准备会话返回可持久绑定的原生身份；提交新一轮区分未发送/
//!   可能已发送/原生已确认开始；停止先报受理、再报原生终态——
//!   终态经 [`ExecAdapterSink`] 回调进入调度器，不在调用返回里
//!   冒充；释放按固定版本核实持久化边界，关闭资源不删除历史；
//! - N2：原生身份必须可持久绑定，恢复时使用原身份，失败不替换。
//!
//! 增量 6 追加（C8/N10/D6）：
//! - `release`：空闲释放运行实例——不删历史、不影响别的任务；
//!   恢复是调用方用原身份 `start_run`（sim 即同 id 恢复）；
//! - Sim `resume_fails` 旋钮：Bound 原生身份历史不可用的如实阻
//!   断场景（D6——恢复失败不静默新建）。
//!
//! P1 全程只有 [`SimAdapter`]（标注：P3 由真实双 Provider adapter
//! 替换）；生产装配默认挂起模式——无真实 Provider 时不伪造进展。

use crate::exec::{AcceptedInput, ExecTask};
use crate::types::Result;
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
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

/// adapter → 调度器的上报（增量 5 枚举化：终态 + 结果正文）。
/// 经同一 mpsc 到达调度器泵，按 native id 反查归属。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdapterNotice {
    /// 原生终态（C4）。
    Terminal(TerminalKind),
    /// 一轮的 Agent 原始完整正文（N9：先保存再公布；body 不改写）。
    Result(String),
}

/// 一条上报（原生身份 + 载荷）。`TerminalNotice` 是兼容别名——增
/// 量 5 起回调通道同时承载终态与结果。
pub type AdapterNoticeMsg = (String, AdapterNotice);
pub type TerminalNotice = AdapterNoticeMsg;

/// adapter → 调度器的回调口（C4/N9）：构造时注入 adapter 持有；
/// 调度器侧泵循环把每条上报转为 `ExecScheduler::terminal` /
/// `ExecScheduler::result_reported` 调用。
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
        let _ = self
            .0
            .send((native_session_id.to_string(), AdapterNotice::Terminal(kind)));
    }

    /// 上报一轮结果正文（N9：原始完整正文；调度器先保存再公布）。
    pub fn result(&self, native_session_id: &str, body: impl Into<String>) {
        let _ = self.0.send((
            native_session_id.to_string(),
            AdapterNotice::Result(body.into()),
        ));
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

    /// 空闲释放运行实例（增量 6，C8/N10）：关闭资源不删除历史、
    /// 不影响别的任务；恢复由调用方用原身份 `start_run`（原生侧
    /// load/resume，sim 即同 id `start_run`）。
    async fn release(&self, native_session_id: &str) -> Result<()>;
}

/// `SimAdapter` 的共享控制柄（chat-flow 增量 4，Arc 共享、clone
/// cheap）：测试与本地驱动经它注入原生终态、动态改自动完成旋钮。
/// 生产语义不变——prod 装配是挂起模式，本柄不显式调用即无任何
/// 行为；P3 真实 adapter 替换后本柄随 Sim 一并消失。
#[derive(Clone, Default)]
pub struct SimControl {
    /// 终态上报口（`with_sink` 装配时与 adapter 同口；None = 无从
    /// 上报，`complete`/`fail`/`cancel_confirms` 静默无操作）
    sink: Option<ExecAdapterSink>,
    /// 自动完成旋钮（单一事实源：构造期 `with_complete_after` 与运
    /// 行期 `set_auto_complete` 都写这里，`start_run` 只读这里）：
    /// Some(d) = `start_run` ack 后 d 经 sink 报 Completed；None =
    /// 挂起直到显式注入终态
    auto_complete: Arc<Mutex<Option<Duration>>>,
    /// cancel 观察口（与 adapter 共享；停止幂等性断言用）
    cancelled: Arc<Mutex<Vec<String>>>,
    /// 恢复失败旋钮（增量 6，D6 历史缺失场景）：集合内的原生身份
    /// `start_run` 一律报错——模拟「Bound 的原生 Session 历史不可
    /// 用」，走 Unknown + `blocked_unknown` 如实阻断路径（不静默新
    /// 建 Session）。
    resume_fails: Arc<Mutex<std::collections::HashSet<String>>>,
}

impl SimControl {
    /// 注入「自然完成」终态（经 sink 上报，C4：不在调用返回里冒充）。
    pub fn complete(&self, native_session_id: &str) {
        self.report(native_session_id, TerminalKind::Completed);
    }

    /// 注入「业务失败」终态。
    pub fn fail(&self, native_session_id: &str) {
        self.report(native_session_id, TerminalKind::Failed);
    }

    /// 注入「取消已确认」终态（停止未确认场景的收口驱动）。
    pub fn cancel_confirms(&self, native_session_id: &str) {
        self.report(native_session_id, TerminalKind::Cancelled);
    }

    /// 注入一轮结果正文（增量 5，N9）：经 sink 上报，调度器按
    /// native id 反查归属 Run 后先保存再公布。
    pub fn publish_result(&self, native_session_id: &str, body: impl Into<String>) {
        if let Some(sink) = &self.sink {
            sink.result(native_session_id, body);
        }
    }

    /// 动态改自动完成旋钮；None 回到挂起模式。
    pub fn set_auto_complete(&self, d: Option<Duration>) {
        *self.auto_complete.lock().unwrap() = d;
    }

    /// 测试观察口：adapter 已收到的 cancel（按序）。
    pub fn cancelled_sessions(&self) -> Vec<String> {
        self.cancelled.lock().unwrap().clone()
    }

    /// 让某原生身份的 `start_run` 失败（增量 6：Bound 历史缺失的
    /// 恢复失败场景，D6）。武装后一直生效（与真实「历史不可用」
    /// 同义——不是一次性抖动）。
    pub fn fail_resume(&self, native_session_id: &str) {
        self.resume_fails
            .lock()
            .unwrap()
            .insert(native_session_id.to_string());
    }

    fn report(&self, native_session_id: &str, kind: TerminalKind) {
        if let Some(sink) = &self.sink {
            sink.terminal(native_session_id, kind);
        }
    }
}

/// 仿真 adapter（P1 全程使用；P3 由真实 adapter 替换）。
///
/// `Default` = 全挂起模式：不报完成、不报取消确认——生产装配
/// （`Kernel::new`）就用它，无真实 Provider 时不伪造任何进展。
/// 测试旋钮（可注）：`with_complete_after`/`SimControl::set_auto_complete`
/// （到点经 sink 报 Completed）、`fail_next_start`（下一次 `start_run`
/// 失败一次）、`cancel_never_confirms`（cancel 受理后永不报终态）。
pub struct SimAdapter {
    /// 终态回调口（构造时注入；None = 无从上报，全部挂起）
    sink: Option<ExecAdapterSink>,
    /// true：cancel 受理后永不报 Cancelled（慢停止/停止丢失场景）
    cancel_never_confirms: bool,
    /// 下一次 `start_run` 返回 Err 一次（随后自动复位）
    fail_next_start: AtomicBool,
    /// 测试观察口：`create_session` 发出的原生身份（按序）
    created: Mutex<Vec<String>>,
    /// 测试观察口：`start_run` 收到的（原生身份, 输入）（按序）
    started: Mutex<Vec<(String, AcceptedInput)>>,
    /// 测试观察口：`release` 收到的原生身份（按序；增量 6 空闲释
    /// 放——记录调用即释放，历史不动）
    released: Mutex<Vec<String>>,
    /// 增量 4：共享控制柄（自动完成旋钮 + cancel 观察口 + 终态注
    /// 入口的共享副本——见 [`SimControl`]）
    control: SimControl,
}

impl Default for SimAdapter {
    /// 挂起模式（生产默认）：不自己完成、不自己确认取消。
    fn default() -> Self {
        Self {
            sink: None,
            cancel_never_confirms: true,
            fail_next_start: AtomicBool::new(false),
            created: Mutex::new(Vec::new()),
            started: Mutex::new(Vec::new()),
            released: Mutex::new(Vec::new()),
            control: SimControl::default(),
        }
    }
}

impl SimAdapter {
    /// 注入终态回调口（构造时注入的 builder 形态；控制柄同口）。
    #[must_use]
    pub fn with_sink(mut self, sink: ExecAdapterSink) -> Self {
        self.control.sink = Some(sink.clone());
        self.sink = Some(sink);
        self
    }

    /// `start_run` ack 后 d 自动报 Completed（需同时注入 sink；等
    /// 价于 `SimControl::set_auto_complete(Some(d))` 的构造期形态）。
    #[must_use]
    pub fn with_complete_after(self, d: Duration) -> Self {
        self.control.set_auto_complete(Some(d));
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

    /// 测试观察口：已收到的 `release`（原生身份, 按序）。
    pub fn released_sessions(&self) -> Vec<String> {
        self.released.lock().unwrap().clone()
    }

    /// 测试观察口：已收到的 cancel。
    pub fn cancelled_sessions(&self) -> Vec<String> {
        self.control.cancelled_sessions()
    }

    /// 共享控制柄（chat-flow 增量 4）：测试/本地驱动注入终态、动
    /// 态改自动完成旋钮。clone cheap（Arc 共享同一内核）。
    pub fn control(&self) -> SimControl {
        self.control.clone()
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
        // D6 历史缺失旋钮：Bound 身份恢复失败如实报错（不替换身份、
        // 不静默新建）。
        if self
            .control
            .resume_fails
            .lock()
            .unwrap()
            .contains(native_session_id)
        {
            return Err(crate::types::KernelError::task(
                "sim adapter: native session history unavailable (resume_fails armed)",
            ));
        }
        self.started
            .lock()
            .unwrap()
            .push((native_session_id.to_string(), input.clone()));
        let auto_complete = *self.control.auto_complete.lock().unwrap();
        if let (Some(d), Some(sink)) = (auto_complete, &self.sink) {
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
        self.control
            .cancelled
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

    async fn release(&self, native_session_id: &str) -> Result<()> {
        // sim：记录调用即释放（C8：关闭资源不删除历史——恢复是同
        // 一身份的 start_run，不走 create_session）。
        self.released
            .lock()
            .unwrap()
            .push(native_session_id.to_string());
        Ok(())
    }
}
