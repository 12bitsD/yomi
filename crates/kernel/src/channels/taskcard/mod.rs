//! 执行任务创建助手（chat-flow 增量 2）：slash `/task` 与内建工具
//! `task_create` 两入口共用的「登记 + 发卡」流程。
//!
//! 设计依据 docs/design/chat-flow-technical-design.md N1/C1/C2：
//! - N1：双入口共用同一创建契约——两入口都经
//!   `Kernel::create_exec_task` 登记，不各自直连 store；
//! - C1：同一创建意图重送（同 `dedup_key`）收敛到同一任务，不重复
//!   发卡（`Existing`）；
//! - C2：创建分步——登记（本模块第一步）→ 卡片/Thread（第二步，
//!   卡即 Thread 锚）。发卡失败保留「任务已登记、卡未投递」的半完
//!   成态（`CardPending`），如实告知，不重试建任务、不自动补卡。

use std::sync::Arc;

use crate::channels::{cards::taskcard::task_card, PlatformAdapter};
use crate::exec::{CreateExecTask, ExecTask};
use crate::kernel::Kernel;
use crate::types::Result;

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

    let card = task_card(&task, kernel.exec_inbox().len(&task.id));
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
