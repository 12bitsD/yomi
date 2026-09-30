//! `task_status` / `task_result` 内建工具（chat-flow W2 增量 5）：
//! 普通 Chat 查询执行任务进展与结果正文的只读视图。
//!
//! 设计依据 docs/design/chat-flow-technical-design.md N7/N9/C7：
//! - N7：查询读关键事实（登记 + lane 快照 + Run 事实行），不从
//!   「模型说完成了」推断执行状态；范围不明确返回候选任务，不挑
//!   最近任务猜答案；
//! - N9：结果正文按 Run 归属保存（先保存再公布），`task_result`
//!   按轮次读取权威正文——读旧轮不会读到新轮；
//! - C7：**查询是只读操作**——本模块两个工具不调调度器任何写方
//!   法、不联系 adapter、不创建 Run、不向执行 Session 发新
//!   Prompt（唯一调度器接触面是只读 `snapshot`）。本模块 doc 与
//!   `queries_are_strictly_read_only` 测试双重锁定这条红线。
//!
//! 读取的是事实快照：解释时须区分事实与推测（「已保存第 N 轮正
//! 文」是事实，「任务快完成了」是推测）；工具不得用于发起、恢
//! 复或停止任务。

use crate::exec::{ExecResultRow, ExecRunRow, ExecTask};
use crate::kernel::Kernel;
use crate::tools::{Tool, ToolExecCtx};
use crate::types::{ExecTaskId, KernelError, Result, SessionId, ToolOutput};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::{Arc, Weak};

pub const TASK_STATUS_TOOL_NAME: &str = "task_status";
pub const TASK_RESULT_TOOL_NAME: &str = "task_result";

/// 无 id 查询的候选上限（防刷屏；超出如实标注截断）。
const MAX_CANDIDATES: usize = 20;
/// 带 id 查询返回的最近 Run 行数。
const RECENT_RUNS: usize = 5;

/// 执行任务进度查询工具（只读）。
pub struct TaskStatusTool {
    /// 会话→通道路由解析（无 id 形态的范围圈定；`None` = 无通道
    /// 环境，候选覆盖全部通道）。
    channel_hub: Option<Arc<crate::channels::hub::ChannelHub>>,
    /// 登记/快照/事实所在（Weak：断 Kernel→Conductor→Registry→
    /// Tool 引用环，同 `task_create`）。
    kernel: Weak<Kernel>,
}

impl TaskStatusTool {
    pub fn new(
        channel_hub: Option<Arc<crate::channels::hub::ChannelHub>>,
        kernel: Weak<Kernel>,
    ) -> Self {
        Self {
            channel_hub,
            kernel,
        }
    }
}

/// 执行任务结果正文读取工具（只读）。
pub struct TaskResultTool {
    kernel: Weak<Kernel>,
}

impl TaskResultTool {
    pub fn new(kernel: Weak<Kernel>) -> Self {
        Self { kernel }
    }
}

fn upgrade(kernel: &Weak<Kernel>, tool: &str) -> Result<Arc<Kernel>> {
    kernel
        .upgrade()
        .ok_or_else(|| KernelError::tool(format!("{tool}: kernel is not available")))
}

/// 任务 id 参数解析（trim 后非空才算给出；给出则必须是完整任务
/// id——短码不足以唯一定位，拒绝而非猜测，N7/N9）。
fn task_id_arg(args: &Value, tool: &str) -> Result<Option<ExecTaskId>> {
    let Some(raw) = args["task_id"]
        .as_str()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    else {
        return Ok(None);
    };
    if !raw.starts_with(ExecTaskId::PREFIX) || raw.len() < ExecTaskId::PREFIX.len() + 20 {
        return Err(KernelError::tool(format!(
            "{tool}: `task_id` 无效（须为 task_create 返回的完整任务 id，不接受短码）"
        )));
    }
    Ok(Some(ExecTaskId::from(raw)))
}

/// goal 摘录（候选列表一行用：前 30 字，换行压平）。
fn goal_excerpt(goal: &str) -> String {
    let flat: String = goal.split_whitespace().collect::<Vec<_>>().join(" ");
    let truncated: String = flat.chars().take(30).collect();
    if flat.chars().count() > 30 {
        format!("{truncated}…")
    } else {
        truncated
    }
}

/// Run 事实行的 JSON 视图（`status`/`terminal_kind` 原样呈现——
/// 行是写时刻的如实快照）。
fn run_json(run: &ExecRunRow, result: Option<&ExecResultRow>) -> Value {
    json!({
        "run_id": run.run_id.as_str(),
        "seq": run.input_seq,
        "status": run.status,
        "terminal_kind": run.terminal_kind,
        "started_at": run.started_at.to_rfc3339(),
        "ended_at": run.ended_at.map(|t| t.to_rfc3339()),
        "result_saved": result.is_some(),
        "result_bytes": result.map(|r| r.body_bytes),
        "result_saved_at": result.map(|r| r.created_at.to_rfc3339()),
    })
}

#[async_trait]
impl Tool for TaskStatusTool {
    fn name(&self) -> &'static str {
        TASK_STATUS_TOOL_NAME
    }

    fn desc(&self) -> &'static str {
        "查询执行任务进展（只读事实快照）。用于回答「任务 X 进展如何」「那个任务跑到哪了」：返回登记状态、队列快照、最近轮次与结果保存情况，不含结果正文（正文用 task_result 按轮读取）。读到的是事实快照而非实时推断：解释时区分事实与推测，不得把「用户交办的内容」说成「已完成的工作」。本工具不发起、不恢复、不停止任何任务；不带 task_id 时返回当前通道下的活动任务候选，不替你猜是哪个任务。"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "task_id": {
                    "type": "string",
                    "description": "任务 id（可选，task_create 返回的完整 id）。给出 = 该任务详细快照；省略 = 当前通道路向下的活动任务候选列表",
                },
            },
        })
    }

    async fn exec(&self, args: Value, ctx: ToolExecCtx<'_>) -> Result<ToolOutput> {
        let kernel = upgrade(&self.kernel, TASK_STATUS_TOOL_NAME)?;
        match task_id_arg(&args, TASK_STATUS_TOOL_NAME)? {
            Some(task_id) => {
                let task = kernel
                    .exec_task_store()
                    .get(&task_id)
                    .await?
                    .ok_or_else(|| {
                        KernelError::tool(format!(
                            "task_status: 未找到执行任务 {task_id}（id 以 task_create 返回为准）"
                        ))
                    })?;
                self.task_detail(&kernel, &task).await
            }
            None => self.candidates(&kernel, &ctx).await,
        }
    }
}

impl TaskStatusTool {
    /// 带 id 形态：登记 + lane 快照 + 最近 Run 行 + 结果有无。
    async fn task_detail(&self, kernel: &Arc<Kernel>, task: &ExecTask) -> Result<ToolOutput> {
        let facts = kernel.exec_fact_store();
        let snap = kernel.exec_scheduler().snapshot(&task.id);
        let runs = facts.runs_for(&task.id).await?;
        let latest_result = facts.latest_result(&task.id).await?;
        let mut recent = Vec::new();
        for run in runs.iter().rev().take(RECENT_RUNS) {
            let result = facts.result_for(&run.run_id).await?;
            recent.push(run_json(run, result.as_ref()));
        }
        let current = snap.current.as_ref().map(|r| {
            json!({
                "run_id": r.run_id.as_str(),
                "seq": r.input_seq,
                "status": r.status,
                "started_at": r.started_at.to_rfc3339(),
                "ended_at": r.ended_at.map(|t| t.to_rfc3339()),
            })
        });
        let out = json!({
            "task": {
                "id": task.id.as_str(),
                "id_short": &task.id.as_str()[..12.min(task.id.as_str().len())],
                "channel": task.channel_name,
                "provider": task.provider.as_str(),
                "status": task.status.as_str(),
                "binding": task.binding.as_str(),
                "goal": task.goal,
                "created_by": task.created_by,
                "created_at": task.created_at.to_rfc3339(),
            },
            "lane": {
                "paused": snap.paused,
                "queued": snap.queued,
                "blocked_unknown": snap.blocked_unknown,
                "current": current,
            },
            "recent_runs": recent,
            "latest_result": latest_result.as_ref().map(|r| json!({
                "seq": r.input_seq,
                "run_id": r.run_id.as_str(),
                "bytes": r.body_bytes,
                "saved_at": r.created_at.to_rfc3339(),
            })),
            "note": "只读事实快照：lane 是进程内状态（重启清空），Run 行与结果行是持久事实。结果正文用 task_result 按轮读取。",
        });
        Ok(ToolOutput::text(out.to_string()))
    }

    /// 无 id 形态：当前通道路由下的活动任务候选（范围不明确返回
    /// 候选，不挑最近猜——N7）。
    async fn candidates(&self, kernel: &Arc<Kernel>, ctx: &ToolExecCtx<'_>) -> Result<ToolOutput> {
        let routing = match &self.channel_hub {
            Some(hub) => {
                hub.get_routing_for_session(&SessionId::from(ctx.session_id.clone()))
                    .await?
            }
            None => None,
        };
        let channel = routing.map(|(r, _)| r.channel_name);
        let ids = kernel
            .exec_fact_store()
            .list_tasks_with_activity(channel.as_deref())
            .await?;
        let total = ids.len();
        let mut candidates = Vec::new();
        for id in ids.iter().take(MAX_CANDIDATES) {
            let Some(task) = kernel.exec_task_store().get(id).await? else {
                continue;
            };
            let snap = kernel.exec_scheduler().snapshot(id);
            let runs = kernel.exec_fact_store().runs_for(id).await?;
            let last = runs.last();
            let status_line = last.map_or_else(
                || "尚无已记录的轮次".to_string(),
                |r| {
                    format!(
                        "第 {} 轮 {}{}",
                        r.input_seq,
                        r.status,
                        r.terminal_kind
                            .as_ref()
                            .map_or(String::new(), |k| format!("（{k}）"))
                    )
                },
            );
            candidates.push(json!({
                "id": task.id.as_str(),
                "id_short": &task.id.as_str()[..12.min(task.id.as_str().len())],
                "goal": goal_excerpt(&task.goal),
                "task_status": task.status.as_str(),
                "channel": task.channel_name,
                "latest": status_line,
                "queued": snap.queued,
                "paused": snap.paused,
            }));
        }
        let out = json!({
            "scope": {
                "channel": channel,
                "note": "范围不明确：返回有活动事实的任务候选，不替你挑任务。带 task_id 重新查询看详情。",
            },
            "candidates": candidates,
            "total": total,
            "truncated": total > MAX_CANDIDATES,
        });
        Ok(ToolOutput::text(out.to_string()))
    }
}

#[async_trait]
impl Tool for TaskResultTool {
    fn name(&self) -> &'static str {
        TASK_RESULT_TOOL_NAME
    }

    fn desc(&self) -> &'static str {
        "读取执行任务某一轮的权威结果正文（Agent 原始完整正文，不改写）。seq 缺省 = 最新已保存轮；明确给出 seq 读对应轮——读旧轮不会读到新轮。读到的是已保存事实，解释时区分事实与推测。本工具只读：不发起、不恢复、不停止任何任务。超长正文按工具输出上限截断，同时自动导出完整 Markdown 并在输出中给出路径（导出失败会如实说明，不会显示可访问）。"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "task_id": {
                    "type": "string",
                    "description": "任务 id（task_create 返回的完整 id）",
                },
                "seq": {
                    "type": "integer",
                    "description": "轮次序号（可选，从 1 起；缺省 = 最新已保存轮）",
                    "minimum": 1,
                },
            },
            "required": ["task_id"],
        })
    }

    async fn exec(&self, args: Value, ctx: ToolExecCtx<'_>) -> Result<ToolOutput> {
        let kernel = upgrade(&self.kernel, TASK_RESULT_TOOL_NAME)?;
        let task_id = task_id_arg(&args, TASK_RESULT_TOOL_NAME)?
            .ok_or_else(|| KernelError::tool("task_result: `task_id` is required"))?;
        let task = kernel
            .exec_task_store()
            .get(&task_id)
            .await?
            .ok_or_else(|| {
                KernelError::tool(format!(
                    "task_result: 未找到执行任务 {task_id}（id 以 task_create 返回为准）"
                ))
            })?;
        let facts = kernel.exec_fact_store();
        let seq = args["seq"].as_u64();
        let row = match seq {
            Some(seq) => {
                let runs = facts.runs_for(&task_id).await?;
                let run = runs.iter().find(|r| r.input_seq == seq).ok_or_else(|| {
                    let known: Vec<u64> = runs.iter().map(|r| r.input_seq).collect();
                    KernelError::tool(format!(
                        "task_result: 任务 {} 没有第 {seq} 轮 Run（已记录轮次：{known:?}）",
                        &task.id.as_str()[..12.min(task.id.as_str().len())],
                    ))
                })?;
                facts.result_for(&run.run_id).await?
            }
            None => facts.latest_result(&task_id).await?,
        };
        let Some(row) = row else {
            let note = match seq {
                Some(seq) => format!(
                    "第 {seq} 轮尚无已保存正文（该轮可能仍在执行或结果未上报；已保存的正文不会迟到覆盖）"
                ),
                None => "该任务尚无已保存的结果正文".to_string(),
            };
            let out = json!({
                "task_id": task.id.as_str(),
                "seq": seq,
                "result": Value::Null,
                "note": note,
            });
            return Ok(ToolOutput::text(out.to_string()));
        };
        // 权威正文原样输出（N9：不改写）；超限按工具输出截断约定
        // 给头部，同时自动导出完整 Markdown（增量 8，N9 首版交付
        // 点）——导出成功给路径；导出失败与正文保存分开报错：未
        // 成功不得显示可访问，如实说明（正文仍已安全保存）。
        let header = format!(
            "任务 `{}` · 第 {} 轮结果正文（{} 字节，保存于 {}）\n\n",
            &task.id.as_str()[..12.min(task.id.as_str().len())],
            row.input_seq,
            row.body_bytes,
            crate::storage::format_age(row.created_at),
        );
        let full = format!("{header}{}", row.body);
        let text = if full.len() > ctx.max_tool_output_length {
            let export = crate::exec::export::export_result_markdown(
                &kernel.exec_fact_store(),
                &row.run_id,
                &kernel.data_dir().await,
            )
            .await;
            let suffix = match export {
                Ok(path) => format!(
                    "\n\n[正文超出工具输出上限已截断；完整 Markdown 已导出：{}]",
                    path.display()
                ),
                Err(e) => format!(
                    "\n\n[正文超出工具输出上限已截断；Markdown 导出失败（{e}）——正文仍已安全保存，可稍后重试]"
                ),
            };
            crate::tools::helper::truncate_output(&full, ctx.max_tool_output_length, &suffix)
        } else {
            full
        };
        Ok(ToolOutput::text(text))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::{AcceptOutcome, CreateExecTask, ExecProvider, ExecTaskSource};
    use std::sync::Arc;
    use std::time::Duration;

    /// 装配最小 Kernel（无通道、挂起 Sim + sink 泵；事实 store 经
    /// `Kernel::new` 接调度器）。
    async fn test_kernel() -> (tempfile::TempDir, Arc<Kernel>) {
        let dir = tempfile::tempdir().unwrap();
        let storage = crate::storage::StorageSet::open(dir.path().join("data"))
            .await
            .unwrap();
        let kernel = Kernel::new(
            &storage,
            crate::agent::AgentConfig::default(),
            None,
            None,
            vec![],
            false,
            None,
            vec![],
            crate::config::TasksConfig::default(),
            crate::config::GcConfig::default(),
            crate::config::ExecConfig::default(),
            false,
            crate::permission::Level::default(),
        )
        .unwrap();
        (dir, kernel)
    }

    async fn make_task(kernel: &Arc<Kernel>, dedup: &str, goal: &str) -> ExecTask {
        let (task, created) = kernel
            .create_exec_task(CreateExecTask {
                channel_name: "test".into(),
                provider: ExecProvider::Kimi,
                goal: goal.into(),
                working_dir: None,
                created_by: "ou_t".into(),
                source: ExecTaskSource::Skill,
                dedup_key: dedup.into(),
            })
            .await
            .unwrap();
        assert!(created);
        task
    }

    fn ctx() -> ToolExecCtx<'static> {
        ToolExecCtx::new("call-1", "/tmp", "sess-1")
    }

    /// 异步轮询（泵是异步通路；5s 上限，10ms 步进）。
    async fn wait_until<Fut>(desc: &str, mut pred: impl FnMut() -> Fut)
    where
        Fut: std::future::Future<Output = bool>,
    {
        for _ in 0..500 {
            if pred().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for {desc}");
    }

    /// 建任务 + 跑完一轮并保存正文（终态与结果事实落库后返回）。
    async fn run_one_round(kernel: &Arc<Kernel>, dedup: &str, goal: &str, body: &str) -> ExecTask {
        let task = make_task(kernel, dedup, goal).await;
        let outcome = kernel.exec_inbox().accept(
            &task.id,
            format!("{dedup}-m1"),
            "ou_t",
            format!("{goal} 的输入"),
            vec![],
        );
        assert!(matches!(outcome, AcceptOutcome::Accepted { .. }));
        assert!(kernel
            .exec_scheduler()
            .try_dispatch(&task.id)
            .await
            .unwrap());
        let native = kernel
            .exec_task_store()
            .get(&task.id)
            .await
            .unwrap()
            .unwrap()
            .provider_session_id
            .unwrap();
        let control = kernel.exec_sim_control().unwrap();
        control.publish_result(&native, body);
        control.complete(&native);
        let facts = kernel.exec_fact_store();
        let id = task.id.clone();
        wait_until("result + terminal facts saved", || {
            let facts = facts.clone();
            let id = id.clone();
            async move {
                facts
                    .latest_result(&id)
                    .await
                    .unwrap()
                    .is_some_and(|r| r.body == body)
                    && facts
                        .runs_for(&id)
                        .await
                        .unwrap()
                        .last()
                        .is_some_and(|r| r.terminal_kind.as_deref() == Some("completed"))
            }
        })
        .await;
        task
    }

    /// 场景 3：task_status 带/不带 id 两形态；数值与 store/snapshot
    /// 一致。场景 4：只读锁定（查询全程调度器状态不变、零派发、
    /// 零 adapter 接触——工具根本拿不到 adapter，Kernel 不暴露）。
    #[tokio::test]
    async fn task_status_both_forms_match_facts_and_stay_read_only() {
        let (_dir, kernel) = test_kernel().await;
        let task = run_one_round(
            &kernel,
            "ts1",
            "把校驗邏輯抽出來獨立成模塊",
            "第一轮权威正文",
        )
        .await;
        let tool = TaskStatusTool::new(None, Arc::downgrade(&kernel));

        // 只读锁定：查询前后快照逐字段相等（含队列、当前轮）。
        let before = kernel.exec_scheduler().snapshot(&task.id);
        let out = tool
            .exec(json!({ "task_id": task.id.as_str() }), ctx())
            .await
            .unwrap();
        let after = kernel.exec_scheduler().snapshot(&task.id);
        assert_eq!(before, after, "查询不得改变调度器任何状态（C7）");
        assert_eq!(
            kernel.exec_inbox().len(&task.id),
            0,
            "查询不得受理/消费输入"
        );

        let v: Value = serde_json::from_str(&out.text_content()).unwrap();
        assert_eq!(v["task"]["id"], task.id.as_str());
        assert_eq!(v["task"]["goal"], "把校驗邏輯抽出來獨立成模塊");
        assert_eq!(v["task"]["status"], "active");
        assert_eq!(v["task"]["binding"], "bound");
        assert_eq!(v["lane"]["paused"], false);
        assert_eq!(v["lane"]["queued"], 0);
        assert_eq!(v["lane"]["blocked_unknown"], false);
        assert_eq!(v["lane"]["current"]["status"], "completed");
        // 最近 Run 行与 store 事实一致（status/terminal_kind/结果有无）。
        let runs = kernel.exec_fact_store().runs_for(&task.id).await.unwrap();
        assert_eq!(v["recent_runs"].as_array().unwrap().len(), runs.len());
        assert_eq!(v["recent_runs"][0]["seq"], 1);
        assert_eq!(v["recent_runs"][0]["status"], "completed");
        assert_eq!(v["recent_runs"][0]["terminal_kind"], "completed");
        assert_eq!(v["recent_runs"][0]["result_saved"], true);
        assert_eq!(
            v["recent_runs"][0]["result_bytes"],
            "第一轮权威正文".len() as u64
        );
        assert_eq!(v["latest_result"]["seq"], 1);
        // 快照与 store 数值一致（started/ended 如实）。
        assert_eq!(
            v["recent_runs"][0]["started_at"],
            runs[0].started_at.to_rfc3339()
        );

        // 无 id 形态：无通道路由 → 全部通道候选（不挑最近猜，N7）。
        let out = tool.exec(json!({}), ctx()).await.unwrap();
        let after = kernel.exec_scheduler().snapshot(&task.id);
        assert_eq!(before, after, "无 id 查询同样只读");
        let v: Value = serde_json::from_str(&out.text_content()).unwrap();
        assert_eq!(v["scope"]["channel"], Value::Null);
        assert_eq!(v["total"], 1);
        let c = &v["candidates"][0];
        assert_eq!(c["id"], task.id.as_str());
        assert!(c["goal"].as_str().unwrap().contains("把校驗邏輯"), "{c}");
        assert!(
            c["latest"].as_str().unwrap().contains("第 1 轮 completed"),
            "{c}"
        );

        // 未知 id：明确报错，不猜不兜底。
        let err = tool
            .exec(json!({ "task_id": ExecTaskId::new().as_str() }), ctx())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("未找到执行任务"), "{err}");

        kernel.close_tokens();
    }

    /// 场景 5：task_result 按 seq 取正确轮；缺省取最新；未知轮明
    /// 确报错；只读锁定同场景 4。
    #[tokio::test]
    async fn task_result_reads_the_right_round_and_stays_read_only() {
        let (_dir, kernel) = test_kernel().await;
        let task = run_one_round(&kernel, "tr1", "目标一", "第一轮正文").await;
        // 第二轮：不同正文，各自独立（N9 轮次归属）。
        let outcome = kernel
            .exec_inbox()
            .accept(&task.id, "tr1-m2", "ou_t", "第二轮输入", vec![]);
        assert!(matches!(outcome, AcceptOutcome::Accepted { .. }));
        assert!(kernel
            .exec_scheduler()
            .try_dispatch(&task.id)
            .await
            .unwrap());
        let native = kernel
            .exec_task_store()
            .get(&task.id)
            .await
            .unwrap()
            .unwrap()
            .provider_session_id
            .unwrap();
        let control = kernel.exec_sim_control().unwrap();
        control.publish_result(&native, "第二轮正文");
        control.complete(&native);
        let facts = kernel.exec_fact_store();
        let id = task.id.clone();
        wait_until("round 2 result saved", || {
            let facts = facts.clone();
            let id = id.clone();
            async move {
                facts
                    .latest_result(&id)
                    .await
                    .unwrap()
                    .is_some_and(|r| r.input_seq == 2 && r.body == "第二轮正文")
            }
        })
        .await;

        let tool = TaskResultTool::new(Arc::downgrade(&kernel));
        let before = kernel.exec_scheduler().snapshot(&task.id);

        // 按 seq 取正确轮（读旧轮不会读到新轮，N9）。
        let out = tool
            .exec(json!({ "task_id": task.id.as_str(), "seq": 1 }), ctx())
            .await
            .unwrap();
        let text = out.text_content();
        assert!(text.contains("第 1 轮结果正文"), "{text}");
        assert!(text.contains("第一轮正文"), "{text}");
        assert!(!text.contains("第二轮正文"), "{text}");

        // 缺省 = 最新已保存轮。
        let out = tool
            .exec(json!({ "task_id": task.id.as_str() }), ctx())
            .await
            .unwrap();
        let text = out.text_content();
        assert!(text.contains("第 2 轮结果正文"), "{text}");
        assert!(text.contains("第二轮正文"), "{text}");

        // 未知轮：明确报错（列出已记录轮次）。
        let err = tool
            .exec(json!({ "task_id": task.id.as_str(), "seq": 9 }), ctx())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("没有第 9 轮"), "{err}");

        // 只读锁定：全程调度器状态不变。
        let after = kernel.exec_scheduler().snapshot(&task.id);
        assert_eq!(before, after, "结果读取不得改变调度器状态（C7）");

        // 无结果的任务：如实「尚无」而非编造（N9 不拼接冒充）。
        let empty = make_task(&kernel, "tr2", "目标二").await;
        let out = tool
            .exec(json!({ "task_id": empty.id.as_str() }), ctx())
            .await
            .unwrap();
        let v: Value = serde_json::from_str(&out.text_content()).unwrap();
        assert_eq!(v["result"], Value::Null);
        assert!(v["note"].as_str().unwrap().contains("尚无已保存"), "{v}");

        kernel.close_tokens();
    }

    /// 只读红线的构造性保证：Weak 为空（kernel 不可用）时两工具
    /// 明确报错而非 panic——工具除 Weak 升级外无任何写通道可触达。
    #[tokio::test]
    async fn tools_fail_honestly_without_kernel() {
        let status = TaskStatusTool::new(None, Weak::new());
        let err = status.exec(json!({}), ctx()).await.unwrap_err();
        assert!(err.to_string().contains("kernel is not available"), "{err}");
        let result = TaskResultTool::new(Weak::new());
        let err = result
            .exec(json!({ "task_id": ExecTaskId::new().as_str() }), ctx())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("kernel is not available"), "{err}");
    }

    /// 增量 8（N9 首版交付点）：task_result 超限 → 自动导出完整
    /// Markdown 并在输出中给路径；正文仍按截断约定给头部；导出
    /// 文件正文字节级一致。
    #[tokio::test]
    async fn task_result_overflow_exports_markdown_and_reports_path() {
        let (dir, kernel) = test_kernel().await;
        // 超长中文正文（>> 2KB 上限）。
        let body = "超长中文正文「」——逐字填充。".repeat(500);
        let task = run_one_round(&kernel, "ex1", "目标导出", &body).await;
        let tool = TaskResultTool::new(Arc::downgrade(&kernel));
        let mut c = ctx();
        c.max_tool_output_length = 2_000;

        let out = tool
            .exec(json!({ "task_id": task.id.as_str() }), c)
            .await
            .unwrap();
        let text = out.text_content();
        assert!(text.contains("已截断"), "{text}");
        assert!(text.contains("完整 Markdown 已导出"), "{text}");
        assert!(text.len() <= 2_000, "截断约定不变：{}", text.len());
        // 截断给的是头部（原文开头在，结尾不在）。
        assert!(text.contains("超长中文正文"), "{text}");

        // 导出产物：`<data_dir>/exec-results/<task_id>/<run_id>.md`，
        // 分隔线以下与权威正文逐字节一致。
        let export_dir = dir
            .path()
            .join("data")
            .join("exec-results")
            .join(task.id.as_str());
        let mut entries = tokio::fs::read_dir(&export_dir).await.unwrap();
        let entry = entries.next_entry().await.unwrap().expect("export file");
        assert!(
            entries.next_entry().await.unwrap().is_none(),
            "一轮一份导出"
        );
        assert!(entry.file_name().to_string_lossy().ends_with(".md"));
        assert!(
            text.contains(&entry.file_name().to_string_lossy().into_owned()),
            "{text}"
        );
        let bytes = tokio::fs::read(entry.path()).await.unwrap();
        let sep = b"\n\n---\n\n";
        let at = bytes
            .windows(sep.len())
            .position(|w| w == sep)
            .expect("attribution separator");
        assert_eq!(&bytes[at + sep.len()..], body.as_bytes(), "正文字节级一致");

        // 幂等：再次超限查询不覆盖（导出直接命中既存路径）。
        let before = tokio::fs::metadata(entry.path()).await.unwrap();
        let mut c = ctx();
        c.max_tool_output_length = 2_000;
        let out = tool
            .exec(json!({ "task_id": task.id.as_str() }), c)
            .await
            .unwrap();
        assert!(out.text_content().contains("完整 Markdown 已导出"));
        let after = tokio::fs::metadata(entry.path()).await.unwrap();
        assert_eq!(
            before.modified().unwrap(),
            after.modified().unwrap(),
            "重复导出不改写既存文件"
        );
        kernel.close_tokens();
    }

    /// 增量 8：导出失败与正文保存分开报错——不显示可访问路径，
    /// 如实说明失败原因与正文保存状态（N9）。
    #[tokio::test]
    async fn task_result_overflow_export_failure_is_reported_without_path() {
        let (dir, kernel) = test_kernel().await;
        let body = "超长中文正文「」——逐字填充。".repeat(500);
        let task = run_one_round(&kernel, "ex2", "目标导出失败", &body).await;
        // 让导出失败：`<data_dir>/exec-results` 被占位为普通文件，
        // create_dir_all 必失败。
        tokio::fs::write(dir.path().join("data").join("exec-results"), "blocked")
            .await
            .unwrap();
        let tool = TaskResultTool::new(Arc::downgrade(&kernel));
        let mut c = ctx();
        c.max_tool_output_length = 2_000;

        let out = tool
            .exec(json!({ "task_id": task.id.as_str() }), c)
            .await
            .unwrap();
        let text = out.text_content();
        assert!(text.contains("Markdown 导出失败"), "{text}");
        assert!(text.contains("正文仍已安全保存"), "{text}");
        assert!(!text.contains("已导出"), "{text}");
        assert!(!text.contains("exec-results/"), "{text}");
        kernel.close_tokens();
    }
}
