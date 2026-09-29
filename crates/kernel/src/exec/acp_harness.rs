//! Kimi ACP 契约测试接入（chat-flow 增量 7）：用真实 `kimi acp`
//! 子进程验证 `ExecAdapter` 契约——调度/绑定/事实/取消/恢复语义
//! 对真实双向协议成立，而非只对 `SimAdapter` 成立。
//!
//! 性质 = **通用测试接入**（handoff 缺下游条款：公共仓可承载不含
//! 业务配置的测试工具）。本模块只服务 `#[cfg(test)]` 契约套件，
//! **不是生产 ACP adapter**——生产 adapter 属下游 W3（见
//! docs/design/chat-flow-downstream-integration.md），此处明确不做。
//!
//! 协议要点（2026-09-29 对 kimi 2.1.0 实测）：
//! - stdio 行分隔 JSON-RPC 2.0；`initialize`（protocolVersion=1）→
//!   `session/new {cwd, mcpServers:[]}` → `result.sessionId`；
//! - `session/prompt {sessionId, prompt:[{type:"text",text}]}` → 期
//!   间经 `session/update` 通知流式推 `agent_message_chunk`（正文
//!   增量，`content.text` 拼接为全文），响应 `result.stopReason`：
//!   `end_turn` 完成、`cancelled` 取消已确认；
//! - `session/cancel` 是**通知**（无 id、无响应）——在飞 prompt
//!   的响应以 `stopReason=cancelled` 收口（实测立即返回）；
//! - `session/load {cwd, mcpServers:[], sessionId}` 在新进程恢复原
//!   Session 历史（实测跨进程召回暗号，D9 证据）；
//! - agent→client 请求（method+id，如 `session/request_permission`）
//!   自动回 allow 选项（free-tokens 配置 `permission_mode=auto`，正
//!   常不触发，按 spec 兜底处理）。
//!
//! 已知简化（测试接入可承载，生产 adapter 不得照抄）：
//! - 正文 = prompt 期间收到的 `agent_message_chunk` 拼接；上一轮
//!   迟到的 chunk 可能混入下一轮正文（套件断言一律 contains 语
//!   义，不精确比对）；
//! - 一 native Session 一进程，无池化/复用（spec 明确不做）；
//! - `release` 直接杀进程——历史由 kimi 自身数据目录保留，恢复
//!   走 `session/load`（C8：关闭资源不删除历史）。

use crate::exec::adapter::{ExecAdapter, ExecAdapterSink, TerminalKind};
use crate::exec::{AcceptedInput, ExecTask};
use crate::types::{ExecTaskId, KernelError, Result};
use async_trait::async_trait;
use dashmap::DashMap;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{broadcast, oneshot, Mutex as AsyncMutex};
use tokio::task::JoinHandle;

/// 控制面请求（`initialize`/`session_new`/`session_load`）响应超时。
const REQ_TIMEOUT: Duration = Duration::from_secs(90);
/// 通知广播容量（chunk 通知高频；滞后即跳过——套件断言是
/// contains 语义，不逐字比对）。
const NOTIFY_CAPACITY: usize = 1024;

/// `kimi acp` 子进程句柄：stdin 串行写、stdout 读循环路由响应/
/// 通知/agent 请求。`Weak` 自引用避免读循环与句柄互相续命——
/// 句柄表移除后读循环随 stdout 关闭退出。
struct AcpProcess {
    child: AsyncMutex<Child>,
    stdin: AsyncMutex<ChildStdin>,
    /// 请求 id 递增器（响应按 id 路由回 oneshot 等待方）
    next_id: AtomicU64,
    /// 在飞请求：id → 响应通道
    pending: DashMap<u64, oneshot::Sender<Value>>,
    /// 通知广播口（`session/update` 等；订阅者按 sessionId 自取）
    notify_tx: broadcast::Sender<Value>,
    /// stdout 读循环
    reader: JoinHandle<()>,
}

impl AcpProcess {
    /// 起 `kimi acp` 子进程并挂读循环（`kill_on_drop` 兜底防泄漏；
    /// 进程工作目录即 Session cwd）。
    fn spawn(cwd: &Path) -> Result<Arc<Self>> {
        let mut child = Command::new("kimi")
            .arg("acp")
            .current_dir(cwd)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| KernelError::task(format!("acp harness: spawn `kimi acp`: {e}")))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| KernelError::task("acp harness: child stdin missing"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| KernelError::task("acp harness: child stdout missing"))?;
        let (notify_tx, _) = broadcast::channel(NOTIFY_CAPACITY);
        Ok(Arc::new_cyclic(|weak| {
            let reader = tokio::spawn(reader_loop(stdout, weak.clone()));
            Self {
                child: AsyncMutex::new(child),
                stdin: AsyncMutex::new(stdin),
                next_id: AtomicU64::new(1),
                pending: DashMap::new(),
                notify_tx,
                reader,
            }
        }))
    }

    /// 写一行 JSON-RPC（stdin 串行：读循环应答 agent 请求与控制
    /// 面请求共用同一写口）。
    async fn write_line(&self, msg: &Value) -> Result<()> {
        let mut s = serde_json::to_string(msg)
            .map_err(|e| KernelError::task(format!("acp harness: serialize request: {e}")))?;
        s.push('\n');
        let mut stdin = self.stdin.lock().await;
        stdin.write_all(s.as_bytes()).await.map_err(|e| {
            KernelError::task(format!("acp harness: stdin write (process dead?): {e}"))
        })?;
        stdin
            .flush()
            .await
            .map_err(|e| KernelError::task(format!("acp harness: stdin flush: {e}")))
    }

    /// 投递请求（同步落管即返回；响应对端经 oneshot 回）。写失败
    /// = 未确认（可能已发送也可能未发送，C4）——如实 Err。
    async fn request_send(
        &self,
        method: &str,
        params: Value,
    ) -> Result<(u64, oneshot::Receiver<Value>)> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.insert(id, tx);
        let msg = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        if let Err(e) = self.write_line(&msg).await {
            self.pending.remove(&id);
            return Err(e);
        }
        Ok((id, rx))
    }

    /// 控制面请求-响应往返（固定超时；rpc 错误/进程死如实 Err）。
    async fn request(&self, method: &str, params: Value) -> Result<Value> {
        let (_id, rx) = self.request_send(method, params).await?;
        let msg = match tokio::time::timeout(REQ_TIMEOUT, rx).await {
            Ok(Ok(msg)) => msg,
            Ok(Err(_)) => {
                return Err(KernelError::task(format!(
                    "acp {method}: response channel dropped (process gone)"
                )));
            }
            Err(_) => {
                return Err(KernelError::task(format!(
                    "acp {method}: response timeout ({REQ_TIMEOUT:?})"
                )));
            }
        };
        if let Some(err) = msg.get("error") {
            return Err(KernelError::task(format!("acp {method} rpc error: {err}")));
        }
        Ok(msg.get("result").cloned().unwrap_or(Value::Null))
    }

    /// 发通知（无 id、无响应；`session/cancel` 用）。
    async fn notify(&self, method: &str, params: Value) -> Result<()> {
        let msg = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        });
        self.write_line(&msg).await
    }

    /// ACP 握手（每进程一次，spawn 后立即调用）。
    async fn initialize(&self) -> Result<()> {
        self.request(
            "initialize",
            serde_json::json!({
                "protocolVersion": 1,
                "clientCapabilities": {},
                "clientInfo": {"name": "yomi-acp-harness", "version": env!("CARGO_PKG_VERSION")},
            }),
        )
        .await?;
        Ok(())
    }

    /// agent→client 请求自动应答：`session/request_permission` 回
    /// allow 选项（优先 `allow_once`）；未实现的方法如实回
    /// method-not-found（不冒充支持）。
    async fn answer_agent_request(&self, msg: &Value) {
        let Some(id) = msg.get("id").cloned() else {
            return;
        };
        let method = msg.get("method").and_then(Value::as_str).unwrap_or("");
        let resp = if method == "session/request_permission" {
            let options = msg["params"]["options"].as_array();
            let chosen = options.and_then(|opts| {
                opts.iter()
                    .find(|o| o["kind"].as_str() == Some("allow_once"))
                    .or_else(|| {
                        opts.iter()
                            .find(|o| o["kind"].as_str().is_some_and(|k| k.contains("allow")))
                    })
                    .or_else(|| opts.first())
            });
            match chosen.and_then(|o| o["optionId"].as_str()) {
                Some(option_id) => serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {"outcome": {"outcome": "selected", "optionId": option_id}},
                }),
                // 无选项可选：如实回 cancelled（不编造 optionId）。
                None => serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {"outcome": {"outcome": "cancelled"}},
                }),
            }
        } else {
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {"code": -32601, "message": format!("harness does not implement {method}")},
            })
        };
        if let Err(e) = self.write_line(&resp).await {
            tracing::warn!(method, error = %e, "acp harness: answer to agent request lost");
        }
    }

    /// 失败全部在飞请求（进程退出/release 时）：等待方如实收到
    /// 错误响应——不悬挂、不冒充成功。
    fn fail_all_pending(&self, reason: &str) {
        let ids: Vec<u64> = self.pending.iter().map(|e| *e.key()).collect();
        for id in ids {
            if let Some((_, tx)) = self.pending.remove(&id) {
                let _ = tx.send(serde_json::json!({
                    "error": {"code": -32000, "message": reason},
                }));
            }
        }
    }

    /// 终止进程（release/套件清理用）。历史由 kimi 数据目录保留，
    /// 不随进程删除（C8）。
    async fn kill(&self) {
        self.fail_all_pending("acp process killed (release)");
        self.reader.abort();
        let _ = self.child.lock().await.kill().await;
    }
}

/// stdout 读循环：行分隔 JSON 分三路——响应按 id 路由回等待方；
/// agent→client 请求自动应答；纯通知进广播。stdout 关闭 = 进程
/// 退出 → 失败全部在飞请求（等待方如实报 Failed，不悬挂）。
async fn reader_loop(stdout: ChildStdout, proc: Weak<AcpProcess>) {
    let mut lines = BufReader::new(stdout).lines();
    loop {
        let line = match lines.next_line().await {
            Ok(Some(line)) => line,
            Ok(None) | Err(_) => break,
        };
        let Ok(msg) = serde_json::from_str::<Value>(&line) else {
            tracing::warn!(line = %line, "acp harness: non-JSON stdout line ignored");
            continue;
        };
        if msg.get("method").is_some() {
            if msg.get("id").is_some() {
                // agent→client 请求（带 id 必须应答，否则 agent 悬挂）。
                if let Some(p) = proc.upgrade() {
                    p.answer_agent_request(&msg).await;
                }
            } else if let Some(p) = proc.upgrade() {
                let _ = p.notify_tx.send(msg);
            }
        } else if let Some(id) = msg.get("id").and_then(Value::as_u64) {
            if let Some(p) = proc.upgrade() {
                if let Some((_, tx)) = p.pending.remove(&id) {
                    let _ = tx.send(msg);
                }
            }
        }
    }
    if let Some(p) = proc.upgrade() {
        p.fail_all_pending("acp process exited");
    }
}

/// prompt 收口任务：等响应、沿途收集正文 chunk，按 stopReason 如
/// 实上报。有正文先报 `Result` 再报 `Terminal`（sink 通道保序——
/// 调度器先保存再公布，N9）。
async fn prompt_waiter(
    session: String,
    mut rx: oneshot::Receiver<Value>,
    mut notif: broadcast::Receiver<Value>,
    sink: ExecAdapterSink,
) {
    let mut body = String::new();
    let mut notif_open = true;
    let resp = loop {
        tokio::select! {
            r = &mut rx => break r,
            n = notif.recv(), if notif_open => {
                match n {
                    Ok(msg) => collect_chunk(&session, &msg, &mut body),
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(
                            skipped,
                            "acp harness: chunk notifications lagged; body may have gaps"
                        );
                    }
                    Err(broadcast::error::RecvError::Closed) => notif_open = false,
                }
            }
        }
    };
    match resp {
        Ok(msg) => {
            if let Some(err) = msg.get("error") {
                tracing::warn!(error = %err, "acp harness: prompt rpc error / process gone");
                sink.terminal(&session, TerminalKind::Failed);
            } else if msg["result"]["stopReason"].as_str() == Some("cancelled") {
                // 取消已确认（C6：原生终态经 sink 异步上报）。
                sink.terminal(&session, TerminalKind::Cancelled);
            } else {
                // 滞后 chunk 宽限排空：实测 kimi 可在 prompt 响应之
                // 后再吐正文 chunk（增量 7 探针 2 证据）——响应到达
                // 后继续收集，连续安静即停（上限 2s 兜底）。
                drain_late_chunks(&session, &mut notif, &mut body).await;
                if !body.is_empty() {
                    sink.result(&session, body);
                }
                sink.terminal(&session, TerminalKind::Completed);
            }
        }
        Err(_) => {
            tracing::warn!("acp harness: prompt response channel dropped (process gone)");
            sink.terminal(&session, TerminalKind::Failed);
        }
    }
}

/// 响应到达后的滞后 chunk 排空（实测 kimi 可在 prompt 响应之后再
/// 吐正文 chunk——增量 7 探针 2）：连续 `DRAIN_QUIET` 无新通知即
/// 停，`DRAIN_CAP` 硬上限兜底。取消/错误路径不排空（无正文可收）。
async fn drain_late_chunks(
    session: &str,
    notif: &mut broadcast::Receiver<Value>,
    body: &mut String,
) {
    const DRAIN_QUIET: Duration = Duration::from_millis(500);
    const DRAIN_CAP: Duration = Duration::from_secs(2);
    let cap = tokio::time::sleep(DRAIN_CAP);
    tokio::pin!(cap);
    loop {
        tokio::select! {
            () = &mut cap => break,
            n = tokio::time::timeout(DRAIN_QUIET, notif.recv()) => {
                match n {
                    Ok(Ok(msg)) => collect_chunk(session, &msg, body),
                    Ok(Err(broadcast::error::RecvError::Lagged(_))) => {}
                    Ok(Err(broadcast::error::RecvError::Closed)) | Err(_) => break,
                }
            }
        }
    }
}

/// 从 `session/update` 通知拾取本 Session 的正文增量（只收
/// `agent_message_chunk`；thought chunk 不入正文）。
fn collect_chunk(session: &str, msg: &Value, body: &mut String) {
    let params = &msg["params"];
    if params["sessionId"].as_str() != Some(session) {
        return;
    }
    let update = &params["update"];
    if update["sessionUpdate"].as_str() != Some("agent_message_chunk") {
        return;
    }
    if let Some(text) = update["content"]["text"].as_str() {
        body.push_str(text);
    }
}

/// 每任务工作目录：`tempdir()/acp-harness/<task id>`（spec 组件 2；
/// kimi 配置用全局 `~/.kimi-code/config.toml`，不随任务改写）。
fn harness_dir(task_id: &ExecTaskId) -> PathBuf {
    std::env::temp_dir()
        .join("acp-harness")
        .join(task_id.as_str())
}

/// ACP 契约测试 adapter（性质见模块文档：通用测试接入，非生产
/// adapter）。一 native Session 一 `kimi acp` 进程；句柄表是恢复
/// 判据——`release` 移出句柄后，下次 `start_run` 新起进程并经
/// `session/load` 恢复原 Session（D9），绝不静默新建（N2/D6）。
pub struct AcpHarnessAdapter {
    sink: ExecAdapterSink,
    /// native id → 进程句柄（release 移出；恢复时重起）
    procs: DashMap<String, Arc<AcpProcess>>,
    /// native id → 工作目录（`create_session` 登记；`release` 保留——
    /// 恢复要回原目录）
    cwds: DashMap<String, PathBuf>,
    /// 测试观察口：(native id, 输入原文) 按序
    started: Mutex<Vec<(String, String)>>,
}

impl AcpHarnessAdapter {
    pub fn new(sink: ExecAdapterSink) -> Self {
        Self {
            sink,
            procs: DashMap::new(),
            cwds: DashMap::new(),
            started: Mutex::new(Vec::new()),
        }
    }

    /// 取进程句柄；句柄表无此 native id = 已释放 → 新起进程 +
    /// `session/load` 恢复（证据=D9 跨进程恢复；load 失败如实
    /// Err，绝不静默新建 Session）。
    async fn ensure_process(&self, native_id: &str) -> Result<Arc<AcpProcess>> {
        if let Some(p) = self.procs.get(native_id) {
            return Ok(Arc::clone(&p));
        }
        let cwd = self
            .cwds
            .get(native_id)
            .map(|e| e.value().clone())
            .ok_or_else(|| {
                KernelError::task(format!("acp harness: no cwd recorded for {native_id}"))
            })?;
        tracing::info!(
            native_id,
            "acp harness: respawning process + session/load (resume after release)"
        );
        let proc = AcpProcess::spawn(&cwd)?;
        proc.initialize().await?;
        proc.request(
            "session/load",
            serde_json::json!({
                "cwd": cwd,
                "mcpServers": [],
                "sessionId": native_id,
            }),
        )
        .await?;
        self.procs.insert(native_id.to_string(), Arc::clone(&proc));
        Ok(proc)
    }

    /// 测试观察口：已收到的 `start_run`（native id, 输入原文）。
    pub fn started_texts(&self) -> Vec<(String, String)> {
        self.started.lock().unwrap().clone()
    }

    /// 测试观察口：当前在表进程数（释放/恢复断言用）。
    pub fn process_count(&self) -> usize {
        self.procs.len()
    }

    /// 套件收尾：终止全部在表进程（超时路径同样先走这里再失败）。
    pub async fn shutdown_all(&self) {
        let ids: Vec<String> = self.procs.iter().map(|e| e.key().clone()).collect();
        for id in ids {
            if let Some((_, proc)) = self.procs.remove(&id) {
                proc.kill().await;
            }
        }
    }
}

#[async_trait]
impl ExecAdapter for AcpHarnessAdapter {
    async fn create_session(&self, task: &ExecTask) -> Result<String> {
        let cwd = harness_dir(&task.id);
        tokio::fs::create_dir_all(&cwd).await.map_err(|e| {
            KernelError::task(format!("acp harness: create cwd {}: {e}", cwd.display()))
        })?;
        let proc = AcpProcess::spawn(&cwd)?;
        proc.initialize().await?;
        let result = proc
            .request(
                "session/new",
                serde_json::json!({"cwd": cwd, "mcpServers": []}),
            )
            .await?;
        let session = result["sessionId"]
            .as_str()
            .ok_or_else(|| KernelError::task("acp harness: session/new without sessionId"))?
            .to_string();
        self.cwds.insert(session.clone(), cwd);
        self.procs.insert(session.clone(), proc);
        Ok(session)
    }

    async fn start_run(&self, native_session_id: &str, input: &AcceptedInput) -> Result<()> {
        // 同步校验会话在（无句柄 = 已释放 → 原身份恢复，D9）。
        let proc = self.ensure_process(native_session_id).await?;
        self.started
            .lock()
            .unwrap()
            .push((native_session_id.to_string(), input.text.clone()));
        // 先订阅再投递（chunk 通知不丢）；请求同步落管成功 = 原生
        // 已受理（同步 ack）。
        let notif_rx = proc.notify_tx.subscribe();
        let (_id, rx) = proc
            .request_send(
                "session/prompt",
                serde_json::json!({
                    "sessionId": native_session_id,
                    "prompt": [{"type": "text", "text": input.text}],
                }),
            )
            .await?;
        tokio::spawn(prompt_waiter(
            native_session_id.to_string(),
            rx,
            notif_rx,
            self.sink.clone(),
        ));
        Ok(())
    }

    async fn cancel(&self, native_session_id: &str) -> Result<()> {
        let proc = self.procs.get(native_session_id).map(|p| Arc::clone(&p));
        let Some(proc) = proc else {
            // 进程不在表（已释放）：无可取消——受理语义下如实 Ok
            // （防御路径；套件场景不会走到）。
            tracing::warn!(
                native_session_id,
                "acp harness: cancel for a released session; nothing to cancel"
            );
            return Ok(());
        };
        // session/cancel 是通知（无 id 无响应）；写失败只 warn——
        // 在飞 prompt 会随进程死经 waiter 如实报终态。
        if let Err(e) = proc
            .notify(
                "session/cancel",
                serde_json::json!({"sessionId": native_session_id}),
            )
            .await
        {
            tracing::warn!(
                native_session_id,
                error = %e,
                "acp harness: cancel notification write failed"
            );
        }
        Ok(())
    }

    async fn release(&self, native_session_id: &str) -> Result<()> {
        // 终止进程并移出句柄表；历史由 kimi 数据目录保留（恢复走
        // session/load，C8/N10）。
        if let Some((_, proc)) = self.procs.remove(native_session_id) {
            proc.kill().await;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "acp_harness_test.rs"]
mod tests;
