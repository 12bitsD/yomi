//! 执行任务主卡的 L1 期限换代（chat-flow 增量 8）：每任务一张
//! 当前有效主卡，跨平台期限换当前代次。
//!
//! 设计依据 docs/design/chat-flow-technical-design.md §8-L1 与
//! D12/C2/C9：
//! - D12（已修订）：每任务一张当前有效主卡；有效期内同卡多轮，
//!   跨期限允许换代，旧卡留作历史。换代保持同一任务、Session 与
//!   执行状态——**换代只换呈现**：不触发执行、不解暂停、不换
//!   Task/Session、不动 inbox/lane/binding/Run；
//! - §8-L1：活跃/待回答任务在最早适用更新期限（已发卡 14 天更
//!   新期限 / `CardKit` 实体创建起 14 天，取较早者）前换代；空闲
//!   任务在用户返回时换代；优先在原 Thread 发布新卡；
//! - C2：**先确认后切换**——平台回执拿到新卡 msg id（新卡确认
//!   可定位）才切换当前映射（`bump_card_generation` 原子 +1）；
//! - C9：发送失败/结果不确定保留原映射、如实返回，不重试、不制
//!   造第二张有效卡；普通投递失败（CardPending）不擅自补卡；旧
//!   代控制失效由回调 gen 重核覆盖（增量 4）。
//!
//! 平台行为边界（§7.2 P0 门槛未闭合）：原 Thread 内换代的路由
//! 连续性、旧卡过期后不可写，均属官方文档承诺、真机未实测——
//! 换代如实暴露发送结果，不承诺控制入口绝不中断。
//!
//! 本模块分两层：`renewal_decision` 是 (task, snapshot, now) 的
//! 纯函数（无 adapter/store 接触，单测直给）；`renew_master_card`
//! 是执行（渲染 → 原 Thread 发卡 → 确认后切换 → 旧卡标注），调
//! 用方（sweep / 分流臂）只做「决策 → 执行」的接线。

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde_json::json;
use tracing::{info, warn};

use crate::channels::cards::taskcard::task_card;
use crate::channels::hub::ChannelInstance;
use crate::exec::{ExecTask, ExecTaskStatus, LaneSnapshot};
use crate::kernel::Kernel;
use crate::types::ExecTaskId;

use super::relay::ExecCardPatches;

/// 已发卡片更新期限（§8.1：IM 更新仅支持 14 天内发送的消息）。
const CARD_UPDATE_TTL: chrono::Duration = chrono::Duration::days(14);
/// `CardKit` 实体期限（§8.1：实体自创建起 14 天有效）。
const CARDKIT_ENTITY_TTL: chrono::Duration = chrono::Duration::days(14);

/// 换代决策（纯函数输出；执行见 `renew_master_card`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RenewalDecision {
    /// 不动作（无卡 / 未到期 / 归档）。
    Noop,
    /// 活跃/待回答任务临近更新期限：尽快换代（sweep 执行）。
    RenewSoon { reason: RenewReason },
    /// 空闲任务已过期：不自动换代，用户下次输入时先换再受理
    ///（分流臂 accept 前执行）。
    RenewOnReturn,
}

/// `RenewSoon` 的期限来源（如实记录哪条期限在约束）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RenewReason {
    /// 已发卡片 14 天更新期限（`card_sent_at` 起计）。
    MessageDeadline,
    /// `CardKit` 实体 14 天期限（`card_entity_created_at` 起计）。
    EntityDeadline,
}

impl RenewReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::MessageDeadline => "message_update_deadline",
            Self::EntityDeadline => "cardkit_entity_deadline",
        }
    }
}

/// 换代执行结果（三态如实）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RenewOutcome {
    /// 新卡已确认可定位，当前映射已切换（gen 已 +1）。
    Renewed { new_msg_id: String, gen: i64 },
    /// 发送失败/平台未回 id/切换未确认：原映射原样保留，未制造
    /// 第二张有效卡（C9）。
    SendUncertain { error: String },
    /// 未执行（无卡、归档、或决策为不换代）。
    Noop,
}

/// 换代决策（纯函数）：只读任务登记与 lane 快照，不接触任何
/// adapter/store，输出三态。`margin` = `exec.card_renew_margin_
/// secs`（活跃任务临期判据；重试与 sweep 周期的缓冲）。
///
/// 活跃 = 有在飞 Run（Starting/Running/Stopping；WaitingRequest
/// 预留——出现即归入在飞侧）或 paused 且队列非空；空闲 = 无在
/// 飞 Run 且队列空。活跃临期 → `RenewSoon`；空闲已过期 →
/// `RenewOnReturn`；其余 → `Noop`。
pub(crate) fn renewal_decision(
    task: &ExecTask,
    lane: &LaneSnapshot,
    now: DateTime<Utc>,
    margin: Duration,
) -> RenewalDecision {
    // 无卡（CardPending 半完成态）→ Noop：普通投递失败不擅自补
    // 卡（C9）。归档不删行也不再维护呈现（D8）。
    if task.card_msg_id.is_none() || task.status == ExecTaskStatus::Archived {
        return RenewalDecision::Noop;
    }
    // 发卡时刻：v29 前存量行 NULL——卡即创建时所发（增量 2 发
    // 卡紧接登记），回退 `created_at`。保守方向：宁可算老不提
    // 前豁免，不得把未知年龄当年轻。
    let sent_at = task.card_sent_at.unwrap_or(task.created_at);
    let msg_deadline = sent_at + CARD_UPDATE_TTL;
    // 取较早的适用更新期限（实体列未回填时只用消息期限）。
    let (deadline, reason) = match task.card_entity_created_at {
        Some(entity_at) if entity_at + CARDKIT_ENTITY_TTL < msg_deadline => {
            (entity_at + CARDKIT_ENTITY_TTL, RenewReason::EntityDeadline)
        }
        _ => (msg_deadline, RenewReason::MessageDeadline),
    };
    let margin = chrono::Duration::from_std(margin).unwrap_or(chrono::Duration::MAX);

    let active = lane.current.as_ref().is_some_and(|r| r.status.is_live())
        || (lane.paused && lane.queued > 0);
    if active {
        // 距期限 < 余量（含已过期）→ 尽快换代。
        if deadline - now < margin {
            return RenewalDecision::RenewSoon { reason };
        }
        return RenewalDecision::Noop;
    }
    // 空闲：已过期 → 用户返回时换代；未过期 → 不动作。
    if now >= deadline {
        return RenewalDecision::RenewOnReturn;
    }
    RenewalDecision::Noop
}

/// 换代执行：渲染当前快照 → 新卡发原 Thread → 确认可定位后切
/// 换当前映射 → 旧卡标注新入口。与卡面刷新共用同一把 per-task
/// 串行锁（换代与 PATCH 不得在同一卡上并发交错）；锁内重读登
/// 记（快照是事实，调用方的决策只是提示——N7 同一纪律）。
///
/// 纪律（§8-L1/C2/C9/D12）：
/// - 优先在原 Thread 发布：reply 到旧 Thread 根（卡即锚——根仍
///   是旧卡，新卡落在同一 Thread 内；飞书 reply 路径只凭 msg
///   id，不使用 `chat_id`）；
/// - **先确认后切换**：平台回执拿到新 msg id 才 `bump_card_
///   generation`；发送失败/无 id → 保留原映射如实返回
///   `SendUncertain`，不重试、不发第二张；
/// - 只换呈现：不触碰 inbox/lane/binding/Run，不解暂停，不换
///   Task/Session（本函数没有任何执行侧调用面）；
/// - 旧卡控制失效已由回调 gen 重核覆盖（增量 4）；旧卡标注新
///   入口尽力而为——过期后不可写属预期（§8-L1），失败不依赖。
pub(crate) async fn renew_master_card(
    kernel: &Arc<Kernel>,
    instances: &Arc<DashMap<String, ChannelInstance>>,
    patches: &ExecCardPatches,
    task_id: &ExecTaskId,
) -> RenewOutcome {
    let serial = patches.lock(task_id);
    let _guard = serial.lock().await;
    // 锁内重读（调用方的决策只是提示；登记才是事实）。
    let task = match kernel.exec_task_store().get(task_id).await {
        Ok(Some(task)) => task,
        Ok(None) => return RenewOutcome::Noop,
        Err(e) => {
            warn!(task_id = %task_id, error = %e, "exec card renewal: task lookup failed");
            return RenewOutcome::SendUncertain {
                error: format!("task lookup failed: {e}"),
            };
        }
    };
    // 无卡不换（C9：CardPending 保持原样）；归档不再维护呈现。
    let (Some(old_card), Some(old_root)) =
        (task.card_msg_id.clone(), task.thread_root_msg_id.clone())
    else {
        return RenewOutcome::Noop;
    };
    if task.status == ExecTaskStatus::Archived {
        return RenewOutcome::Noop;
    }
    let adapter = match instances.get(&task.channel_name) {
        Some(instance) => Arc::clone(&instance.adapter),
        // 通道实例不在（关停中）：未能确认新卡可定位——与发送失
        // 败同档，原映射保留。
        None => {
            return RenewOutcome::SendUncertain {
                error: format!("channel instance {} not available", task.channel_name),
            };
        }
    };

    // 用当前快照渲染新卡（与 relay 同一渲染函数与事实来源）；
    // 按钮 gen 先取旧代 +1——切换成功后与权威代次一致，旧代回
    // 调被重核拒绝（C9）。
    let snap = kernel.exec_scheduler().snapshot(task_id);
    let latest = super::latest_result_or_none(kernel, task_id).await;
    let latest_run = super::latest_run_or_none(kernel, task_id).await;
    let card = task_card(
        &task,
        &snap,
        task.card_generation + 1,
        latest.as_ref(),
        latest_run.as_ref(),
    );
    let new_msg_id = match adapter.send_card("", &card, Some(&old_root)).await {
        Ok(Some(id)) => id,
        // 发送失败或平台未回 id（无法确认新卡可定位）：保留原映
        // 射如实返回，不重试、不发第二张（C9）。
        Ok(None) => {
            return RenewOutcome::SendUncertain {
                error: "platform returned no card message id".to_string(),
            };
        }
        Err(e) => {
            return RenewOutcome::SendUncertain {
                error: e.to_string(),
            };
        }
    };

    // 新卡确认可定位 → 切换当前映射（先确认后切换，C2）。原
    // Thread 换代根不变：新卡在同一 Thread 内，锚仍是旧卡。
    let sent_at = Utc::now();
    let updated = match kernel
        .exec_task_store()
        .bump_card_generation(task_id, &old_root, &new_msg_id, sent_at)
        .await
    {
        Ok(task) => task,
        Err(e) => {
            // 切换失败：新卡已发出但映射未切——如实按不确定处理
            // （不重试不发第二张；新卡按钮 gen 与权威代次不符，
            // 控制被重核拒绝，是一张无映射的游离呈现）。
            warn!(
                task_id = %task_id,
                new_msg_id = %new_msg_id,
                error = %e,
                "exec card renewal: mapping switch failed after send confirmed; \
                 orphan card left unreferenced (its controls are rejected by gen recheck)"
            );
            return RenewOutcome::SendUncertain {
                error: format!("mapping switch failed after send confirmed: {e}"),
            };
        }
    };
    info!(
        task_id = %task_id,
        gen = updated.card_generation,
        new_msg_id = %new_msg_id,
        "exec master card renewed in the original thread (same task/session/runs; presentation only)"
    );

    // 旧卡标注新入口（§8-L1：过期后不可写属预期——失败只
    // warn，不依赖；旧卡控制失效本就由回调 gen 重核覆盖）。
    if let Err(e) = adapter.update_card(&old_card, &renewed_notice_card()).await {
        warn!(
            task_id = %task_id,
            old_card = %old_card,
            error = %e,
            "old card annotation failed; tolerated (expired cards are not writable)"
        );
    }
    RenewOutcome::Renewed {
        new_msg_id,
        gen: updated.card_generation,
    }
}

/// 换代 sweep 的一轮（hub relay 同进程的低频清扫；relay 按
/// `exec.card_renew_sweep_secs` 周期调用）。对启用 `exec_tasks`
/// 通道的活动有卡任务跑决策，只执行 `RenewSoon`；`RenewOnReturn`
/// 留给分流臂在用户返回时执行（不对空闲任务自动发无人看的换
/// 代消息）。单任务失败只 warn——sweep 是呈现维护，不阻断其他
/// 任务。
pub(crate) async fn sweep_once(
    kernel: &Arc<Kernel>,
    instances: &Arc<DashMap<String, ChannelInstance>>,
    patches: &ExecCardPatches,
    margin: Duration,
    now: DateTime<Utc>,
) {
    for instance in instances.iter() {
        if !instance.config.exec_tasks {
            continue;
        }
        let channel = instance.key().clone();
        // dashmap 哨兵不跨 await（锁序：instances 表锁先取先放）。
        drop(instance);
        let task_ids = match kernel
            .exec_fact_store()
            .active_tasks_with_card(&channel)
            .await
        {
            Ok(ids) => ids,
            Err(e) => {
                warn!(channel = %channel, error = %e, "exec renewal sweep: task lookup failed");
                continue;
            }
        };
        for task_id in task_ids {
            let task = match kernel.exec_task_store().get(&task_id).await {
                Ok(Some(task)) => task,
                Ok(None) => continue,
                Err(e) => {
                    warn!(task_id = %task_id, error = %e, "exec renewal sweep: task read failed");
                    continue;
                }
            };
            let snap = kernel.exec_scheduler().snapshot(&task_id);
            let RenewalDecision::RenewSoon { reason } = renewal_decision(&task, &snap, now, margin)
            else {
                continue;
            };
            let outcome = renew_master_card(kernel, instances, patches, &task_id).await;
            match outcome {
                RenewOutcome::Renewed { gen, .. } => {
                    info!(
                        task_id = %task_id,
                        reason = reason.as_str(),
                        gen,
                        "exec renewal sweep: card renewed ahead of deadline"
                    );
                }
                RenewOutcome::SendUncertain { error } => {
                    warn!(
                        task_id = %task_id,
                        reason = reason.as_str(),
                        error,
                        "exec renewal sweep: renewal uncertain; mapping kept, no second card"
                    );
                }
                RenewOutcome::Noop => {}
            }
        }
    }
}

/// 「用户返回即换代」（§8-L1 空闲过期场景）：分流臂 accept 前
/// 调用——决策为 `RenewOnReturn` 才执行（先换再受理）。换代失
/// 败不阻断受理：受理是执行侧语义，呈现缺失由刷新路径如实反
/// 映（C9）。
pub(crate) async fn renew_on_return_if_due(
    kernel: &Arc<Kernel>,
    instances: &Arc<DashMap<String, ChannelInstance>>,
    patches: &ExecCardPatches,
    task: &ExecTask,
    margin: Duration,
    now: DateTime<Utc>,
) -> RenewOutcome {
    let snap = kernel.exec_scheduler().snapshot(&task.id);
    if !matches!(
        renewal_decision(task, &snap, now, margin),
        RenewalDecision::RenewOnReturn
    ) {
        return RenewOutcome::Noop;
    }
    renew_master_card(kernel, instances, patches, &task.id).await
}

/// 旧卡的「已换代」标注（无按钮——旧代控制已失效；指向 Thread
/// 内最新任务卡）。
fn renewed_notice_card() -> String {
    json!({
        "schema": "2.0",
        "header": {
            "template": "grey",
            "title": { "tag": "plain_text", "content": "🧩 任务卡 · 已换代" },
        },
        "body": { "elements": [{
            "tag": "markdown",
            "content": "本卡已因平台期限换代：请以本 Thread 内最新任务卡为准。本卡按钮不再生效；任务、Session 与执行状态不受影响。",
        }] },
    })
    .to_string()
}

#[cfg(test)]
#[path = "renewal_test.rs"]
mod tests;
