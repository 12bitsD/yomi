//! `task_create` 内建工具（chat-flow N1 Skill 路径）：模型在用户明
//! 确交办执行时调用，登记执行任务并投递占位主卡。
//!
//! 设计依据 docs/design/chat-flow-technical-design.md N1/C1/C2：
//! - N1：与 slash `/task` 共用同一创建契约——登记只经
//!   `Kernel::create_exec_task`（工具经 `AgentShared` 的 kernel
//!   回指调用，slot 模式同 `cron_scheduler`）；
//! - C1：同一创建意图重送（同 `dedup_key`）收敛到同一任务，不重
//!   复发卡；
//! - C2：创建分步——登记 → 卡片/Thread。会话无通道路由时仅登记
//!   （`no_channel`），如实返回，不伪造卡已投递。

use crate::exec::{CreateExecTask, ExecProvider, ExecTaskSource};
use crate::kernel::Kernel;
use crate::tools::{Tool, ToolExecCtx};
use crate::types::{KernelError, Result, SessionId, ToolOutput};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::{Arc, Weak};

pub const TASK_CREATE_TOOL_NAME: &str = "task_create";

/// 执行任务创建工具（Skill 路径入口）。
pub struct TaskCreateTool {
    /// 会话→通道路由解析（`None` = 无通道环境，全部走 `no_channel` 仅登记）。
    channel_hub: Option<Arc<crate::channels::hub::ChannelHub>>,
    /// 创建契约所在（Weak：断 Kernel→Conductor→Registry→Tool 引用环）。
    kernel: Weak<Kernel>,
}

impl TaskCreateTool {
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

#[async_trait]
impl Tool for TaskCreateTool {
    fn name(&self) -> &'static str {
        TASK_CREATE_TOOL_NAME
    }

    fn desc(&self) -> &'static str {
        "创建执行任务（登记 + 任务卡 Thread）。仅当用户明确交办实际执行工作时调用（改代码、跑测试、构建等）；普通讨论、方案设计、问答不调用。goal 必须保留用户交办原文，不改写不总结。同一用户消息导致的重复调用必须传同一 dedup_key（建议取触发消息 id；不知道则省略，省略视为独立新建）。创建成功后用户在任务卡 Thread 回复即继续交办，不再经本会话代转。"
    }

    fn schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "goal": {
                    "type": "string",
                    "description": "任务目标：用户交办原文（不改写、不总结）",
                },
                "provider": {
                    "type": "string",
                    "enum": ["kimi", "codex"],
                    "description": "执行 Provider（可选，缺省 kimi）；创建后固定，不可更换",
                },
                "working_dir": {
                    "type": "string",
                    "description": "任务工作目录（可选）",
                },
                "dedup_key": {
                    "type": "string",
                    "description": "去重凭据（可选）：同一用户消息的重复调用传同一值（建议=触发消息 id）；省略则每次调用都是独立新建",
                },
            },
            "required": ["goal"],
        })
    }

    async fn exec(&self, args: Value, ctx: ToolExecCtx<'_>) -> Result<ToolOutput> {
        let goal = args["goal"]
            .as_str()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| KernelError::tool("task_create: `goal` must be a non-empty string"))?
            .to_string();
        let provider = match args["provider"].as_str() {
            None => ExecProvider::Kimi,
            Some(s) => s
                .parse::<ExecProvider>()
                .map_err(|e: String| KernelError::tool(format!("task_create: {e}")))?,
        };
        let working_dir = args["working_dir"]
            .as_str()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        // dedup 凭据省略 = 独立新建意图：生成一次性键——宁可多建，
        // 不可把两次独立调用误并（C2：相同文字不作为去重依据）。
        let dedup_key = args["dedup_key"]
            .as_str()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map_or_else(
                || format!("skill-auto-{}", crate::types::ExecTaskId::new().as_str()),
                str::to_string,
            );

        let kernel = self
            .kernel
            .upgrade()
            .ok_or_else(|| KernelError::tool("task_create: kernel is not available"))?;

        let input = CreateExecTask {
            // 有通道路由时用路由通道名；无路由用占位（与既有
            // 「RPC/Skill 场景可为占位」约定一致）。
            channel_name: "skill".to_string(),
            provider,
            goal,
            working_dir,
            // Skill 调用方的可追溯身份 = 发起会话。
            created_by: format!("session:{}", ctx.session_id),
            source: ExecTaskSource::Skill,
            dedup_key,
        };

        // 通道解析：路由命中 → 走完整「登记 + 发卡」；未命中 → 仅登
        // 记（no_channel），如实返回，不伪造卡已投递（C2）。
        let routing = match &self.channel_hub {
            Some(hub) => {
                hub.get_routing_for_session(&SessionId::from(ctx.session_id.clone()))
                    .await?
            }
            None => None,
        };
        let Some((routing, adapter)) = routing else {
            let (task, created) = kernel.create_exec_task(input).await?;
            let out = json!({
                "task_id": task.id.as_str(),
                "created": created,
                "state": "no_channel",
                "provider": task.provider.as_str(),
                "binding": task.binding.as_str(),
                "note": "本会话无通道路由：任务已登记，未投递任务卡。用户无法经任务卡 Thread 交办。",
            });
            return Ok(ToolOutput::text(out.to_string()));
        };

        // R7 开关（增量 3）：按路由到的通道配置检查；无通道路由的
        // 本地会话已在上面 no_channel 分支放行。
        if let Some(hub) = &self.channel_hub {
            if let Some(cfg) = hub.channel_config(&routing.channel_name) {
                if !cfg.exec_tasks {
                    let out = json!({
                        "created": false,
                        "state": "disabled",
                        "channel": routing.channel_name,
                        "note": "路由到的通道未启用执行任务功能（exec_tasks=false）：未登记任务。",
                    });
                    return Ok(ToolOutput::text(out.to_string()));
                }
            }
        }

        let input = CreateExecTask {
            channel_name: routing.channel_name.clone(),
            ..input
        };
        let outcome = crate::channels::taskcard::create_and_announce(
            &kernel,
            &adapter,
            &routing.external_chat_id,
            input,
        )
        .await?;
        let out = match outcome {
            crate::channels::taskcard::AnnounceOutcome::Ready { task, card_msg_id } => json!({
                "task_id": task.id.as_str(),
                "created": true,
                "state": "ready",
                "card_msg_id": card_msg_id,
                "provider": task.provider.as_str(),
                "binding": task.binding.as_str(),
            }),
            crate::channels::taskcard::AnnounceOutcome::Existing { task } => json!({
                "task_id": task.id.as_str(),
                "created": false,
                "state": "existing",
                "card_msg_id": task.card_msg_id,
                "provider": task.provider.as_str(),
                "binding": task.binding.as_str(),
                "note": "同一 dedup_key 的任务已存在：返回原任务，未重复发卡。",
            }),
            crate::channels::taskcard::AnnounceOutcome::CardPending { task, error } => json!({
                "task_id": task.id.as_str(),
                "created": true,
                "state": "card_pending",
                "provider": task.provider.as_str(),
                "binding": task.binding.as_str(),
                "error": error,
                "note": "任务已登记但占位卡投递失败（半完成态如实）；暂不自动补卡。",
            }),
        };
        Ok(ToolOutput::text(out.to_string()))
    }
}
