//! 执行任务创建助手 + 主卡按钮回调（chat-flow 增量 2/4）：slash
//! `/task` 与内建工具 `task_create` 两入口共用的「登记 + 发卡」流
//! 程，以及主卡 `exec_stop`/`exec_resume` 回调的权威重核处理。
//!
//! 设计依据 docs/design/chat-flow-technical-design.md N1/C1/C2/C9/N7：
//! - N1：双入口共用同一创建契约——两入口都经
//!   `Kernel::create_exec_task` 登记，不各自直连 store；
//! - C1：同一创建意图重送（同 `dedup_key`）收敛到同一任务，不重复
//!   发卡（`Existing`）；
//! - C2：创建分步——登记（本模块第一步）→ 卡片/Thread（第二步，
//!   卡即 Thread 锚）。发卡失败保留「任务已登记、卡未投递」的半完
//!   成态（`CardPending`），如实告知，不重试建任务、不自动补卡；
//! - C9：控制回调即使来自旧显示，也要在权威状态重新核对——字段、
//!   通道开关、任务存在、卡片代次、Run 版本逐维重核，任一不符：
//!   toast 拒绝，零状态变更；
//! - N7：回调后的卡面刷新走 relay 同一「快照渲染 + 串行 PATCH」
//!   路径（事件/点击只是提示，快照是事实）。

pub(crate) mod relay;
pub(crate) mod renewal;

use std::sync::Arc;

use tracing::warn;

use crate::channels::{cards::taskcard::task_card, CardAction, ChannelConfig, PlatformAdapter};
use crate::exec::{
    AnswerOutcome, CreateExecTask, ExecRequestStatus, ExecTask, ExecTaskStatus, RequestOutcome,
    ResumeOutcome, RunStatus, StopOutcome,
};
use crate::kernel::Kernel;
use crate::types::{ExecRequestId, ExecTaskId, Result, RunId};

/// 「登记 + 发卡」的结果（三态如实，入口各自呈现）。
pub(crate) enum AnnounceOutcome {
    /// 新任务且占位卡已投递：卡即 Thread 锚，已回填登记。
    Ready { task: ExecTask, card_msg_id: String },
    /// dedup 命中的既有任务（未重复发卡；卡状态以登记为准）。
    Existing { task: ExecTask },
    /// 任务已登记但卡片投递失败（C2 半完成态如实）：任务的
    /// thread/card 字段保持空，不自动补卡。
    CardPending { task: ExecTask, error: String },
}

/// 登记任务并投递占位主卡。`input.channel_name`/`chat_id` 由入口按
/// 自身上下文填好（slash 用消息所在通道与群；工具用会话路由解析结
/// 果）。
pub(crate) async fn create_and_announce(
    kernel: &Arc<Kernel>,
    adapter: &Arc<dyn PlatformAdapter>,
    chat_id: &str,
    input: CreateExecTask,
) -> Result<AnnounceOutcome> {
    let (task, created) = kernel.create_exec_task(input).await?;
    if !created {
        // 同一创建意图重送：返回既有任务，不重复发卡（C1）。
        return Ok(AnnounceOutcome::Existing { task });
    }

    // 增量 5：新建任务正常无结果行；读取失败不阻断发卡（warn
    // + 按无结果渲染，下一事件刷新自然补齐）。新建任务亦无 Run
    // 事实（增量 6 中断轮凭据）——首发卡恒 None。
    let latest = latest_result_or_none(kernel, &task.id).await;
    let card = task_card(
        &task,
        &kernel.exec_scheduler().snapshot(&task.id),
        task.card_generation,
        latest.as_ref(),
        None,
    );
    let card_msg_id = match adapter.send_card(chat_id, &card, None).await {
        Ok(Some(id)) => id,
        // 发卡失败或平台未回卡消息 id（无法做 Thread 锚，等同失败）：
        // 任务保持已登记、card 字段空——半完成态如实（C2），不重试
        // 建任务、不自动补卡。
        Ok(None) => {
            return Ok(AnnounceOutcome::CardPending {
                task,
                error: "platform returned no card message id".to_string(),
            });
        }
        Err(e) => {
            return Ok(AnnounceOutcome::CardPending {
                task,
                error: e.to_string(),
            });
        }
    };

    // 卡即 Thread 锚：thread_root_msg_id == card_msg_id。任务 Thread
    // 分流（handlers.rs）按 thread_root_msg_id 反查本任务。
    let task = kernel
        .exec_task_store()
        .set_thread_and_card(&task.id, &card_msg_id, &card_msg_id)
        .await?;
    Ok(AnnounceOutcome::Ready { task, card_msg_id })
}

/// 主卡按钮回调（chat-flow 增量 4）：`exec_stop`（停止并暂停）与
/// `exec_resume`（恢复队列）。C9 逐维重核——字段齐 → 通道
/// `exec_tasks` 开 → 任务在 → 代次匹配 →（stop）Run 匹配；任何一
/// 项不符：toast 拒绝，零状态变更。
///
/// 反馈走既有回调应答助手（`approval::send_action_denial`，与
/// act_/mb_ 臂同一机制：ws 回调由 SDK 即时 ack 满足 3 秒契约，文
/// 字反馈落到点击所在群）。停止类动作与 `/stop` 同档——用户闸已
/// 在 hub 分发前统一施加，本函数不叠加 admin（面板契约 §5）。
pub(crate) async fn handle_exec_action(
    channel_name: &str,
    config: &ChannelConfig,
    kernel: &Arc<Kernel>,
    adapter: &Arc<dyn PlatformAdapter>,
    action: CardAction,
) {
    let toast = |text: &str| {
        crate::channels::approval::send_action_denial(adapter, &action, text.to_string())
    };

    // ① 字段解析（缺字段一律保守拒绝，C9）。
    let op = action.value["action"].as_str().unwrap_or_default();
    let task_str = action.value["task"].as_str().unwrap_or_default();
    let gen = action.value["gen"].as_i64();
    let run_str = action.value["run"].as_str();
    let req_str = action.value["req"].as_str().unwrap_or_default();
    let opt_str = action.value["opt"].as_str().unwrap_or_default();
    if !matches!(op, "exec_stop" | "exec_resume" | "exec_answer")
        || task_str.is_empty()
        || gen.is_none()
        || action.value.get("run").is_none()
        || (matches!(op, "exec_stop" | "exec_answer") && run_str.unwrap_or_default().is_empty())
        || (op == "exec_answer" && (req_str.is_empty() || opt_str.is_empty()))
    {
        warn!(
            channel = channel_name,
            value = %action.value,
            "exec card action with missing or unrecognized fields"
        );
        toast("⚠️ 无法识别的任务卡操作，未生效").await;
        return;
    }

    // ② 通道功能开关（R7）。
    if !config.exec_tasks {
        toast("⛔ 本通道未启用执行任务功能（exec_tasks=false）").await;
        return;
    }

    // ③ 权威状态重核：任务在 + 代次匹配（旧代卡不再接受控制）。
    let task_id = ExecTaskId::from(task_str);
    let task = match kernel.exec_task_store().get(&task_id).await {
        Ok(Some(task)) => task,
        Ok(None) => {
            toast("卡片已过期，操作未生效").await;
            return;
        }
        Err(e) => {
            warn!(channel = channel_name, task_id = %task_id, error = %e,
                "exec card action: task lookup failed");
            toast("⚠️ 状态核对失败，操作未生效").await;
            return;
        }
    };
    if gen != Some(task.card_generation) {
        toast("卡片已过期，操作未生效").await;
        return;
    }

    // ④ 归档不删行，也不再接受控制（D8）。
    if task.status == ExecTaskStatus::Archived {
        toast("任务已归档").await;
        return;
    }

    // ⑤/⑥ 按动作分派；调度器三态如实映射为 toast，状态有变化的
    // 分支顺手刷新卡面（relay 同一路径）。
    match op {
        "exec_stop" => {
            let expected = RunId::from(run_str.unwrap_or_default());
            match kernel
                .exec_scheduler()
                .stop_and_pause(&task_id, Some(expected))
                .await
            {
                StopOutcome::Accepted { .. } => {
                    toast("已受理：停止中，队列已暂停").await;
                    refresh_card(kernel, &task_id).await;
                }
                StopOutcome::NoCurrentRun { .. } => {
                    toast("当前无执行中的轮次，队列已暂停").await;
                    refresh_card(kernel, &task_id).await;
                }
                // 旧按钮不得套到新 Run（C6/R7）：不取消任何 Run；
                // 当前轮如实告知（快照重读）。
                StopOutcome::RunMismatch { actual_status, .. } => {
                    let snap = kernel.exec_scheduler().snapshot(&task_id);
                    let seq = snap.current.as_ref().map_or(0, |r| r.input_seq);
                    toast(&format!(
                        "目标轮次已变化（当前第 {seq} 轮状态 {}），未执行停止",
                        run_status_label(actual_status),
                    ))
                    .await;
                    refresh_card(kernel, &task_id).await;
                }
            }
        }
        "exec_resume" => match kernel.exec_scheduler().resume(&task_id).await {
            ResumeOutcome::Resumed { dispatched } => {
                toast(if dispatched {
                    "已恢复，继续执行"
                } else {
                    "已恢复，队列空"
                })
                .await;
                refresh_card(kernel, &task_id).await;
            }
            // 停止未确认：保持暂停，不预约自动恢复（N6 候选 1）。
            ResumeOutcome::BlockedStopUnconfirmed => {
                toast("停止尚未确认，保持暂停；确认后请再次恢复").await;
            }
        },
        // 增量 10（N5/C5）：待回应区选项回答。重核与 exec_stop 同
        // 全维度（字段/开关/任务/代次/归档在上方已过；run/req/
        // Pending 核对在 `answer_request` 锁内）；toast 如实映射—
        // —「已提交」≠「已生效」，重复不重复放行，失效不批准任何
        // 操作。
        "exec_answer" => {
            let expected = RunId::from(run_str.unwrap_or_default());
            let request_id = ExecRequestId::from(req_str);
            let outcome = RequestOutcome::Selected {
                option_id: opt_str.to_string(),
            };
            match kernel
                .exec_scheduler()
                .answer_request(&task_id, &expected, &request_id, outcome)
                .await
            {
                Ok(AnswerOutcome::Resolved) => {
                    toast("已提交，执行方已确认").await;
                    refresh_card(kernel, &task_id).await;
                }
                Ok(AnswerOutcome::Submitted) => {
                    toast("已提交，等待执行方确认").await;
                    refresh_card(kernel, &task_id).await;
                }
                Ok(AnswerOutcome::Already { status }) => {
                    toast(&format!(
                        "该请求{}，不重复放行",
                        match status {
                            ExecRequestStatus::Submitted => "已提交，等待执行方确认",
                            ExecRequestStatus::Resolved => "已回答并经执行方确认",
                            other => other.label(),
                        },
                    ))
                    .await;
                }
                // C5：失效请求不批准任何新操作、不转成新 Prompt。
                Ok(AnswerOutcome::Invalid) => {
                    toast("该请求已失效（轮次已结束/被替换），未执行任何操作").await;
                    refresh_card(kernel, &task_id).await;
                }
                Ok(AnswerOutcome::Mismatch) => {
                    toast("目标轮次或请求已变化，操作未生效").await;
                    refresh_card(kernel, &task_id).await;
                }
                // 交付未确认：调度器已回滚待答状态，可重答。
                Err(e) => {
                    warn!(channel = channel_name, task_id = %task_id, error = %e,
                        "exec answer delivery failed; request rolled back to pending");
                    toast("⚠️ 回答提交失败（未确认），请重试").await;
                    refresh_card(kernel, &task_id).await;
                }
            }
        }
        _ => unreachable!("op 已在 ① 白名单校验"),
    }
}

/// 回调后的卡面刷新：走 hub 的「快照渲染 + 串行 PATCH」路径（与
/// 事件 relay 同一渲染函数，N7）。无 hub（非通道形态）时跳过——
/// 卡本来就只在通道侧发。
async fn refresh_card(kernel: &Arc<Kernel>, task_id: &ExecTaskId) {
    if let Some(hub) = kernel.channel_manager() {
        hub.refresh_exec_task_card(kernel, task_id).await;
    }
}

/// 读取任务最新已保存结果（增量 5 卡面结果行；读取失败只 warn，
/// 按无结果渲染——呈现缺失不阻断卡面主流程）。
pub(crate) async fn latest_result_or_none(
    kernel: &Arc<Kernel>,
    task_id: &ExecTaskId,
) -> Option<crate::exec::ExecResultRow> {
    match kernel.exec_fact_store().latest_result(task_id).await {
        Ok(row) => row,
        Err(e) => {
            warn!(task_id = %task_id, error = %e, "exec card: latest result lookup failed");
            None
        }
    }
}

/// 读取任务最近一轮 Run 事实（增量 6 卡面中断行的凭据，N11/D9；
/// 读取失败只 warn，按无 Run 事实渲染——呈现缺失不阻断卡面主
/// 流程，下一事件刷新自然补齐）。
pub(crate) async fn latest_run_or_none(
    kernel: &Arc<Kernel>,
    task_id: &ExecTaskId,
) -> Option<crate::exec::ExecRunRow> {
    match kernel.exec_fact_store().latest_run(task_id).await {
        Ok(row) => row,
        Err(e) => {
            warn!(task_id = %task_id, error = %e, "exec card: latest run lookup failed");
            None
        }
    }
}

/// Run 状态的中文短标签（toast 如实告知用）。
fn run_status_label(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Starting => "启动中",
        RunStatus::Running => "执行中",
        RunStatus::WaitingRequest => "等待回应",
        RunStatus::Stopping => "停止中",
        RunStatus::Stopped => "已停止",
        RunStatus::Completed => "已完成",
        RunStatus::Failed => "失败",
        RunStatus::Unknown => "待核对",
    }
}
