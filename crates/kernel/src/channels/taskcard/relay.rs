//! 执行任务卡的事件驱动刷新（chat-flow 增量 4）。
//!
//! 设计依据 docs/design/chat-flow-technical-design.md N7/C9：
//! - N7：调度事件（`ExecEvent`）只是提示——任何刷新都在 per-task
//!   串行锁内重读登记 + lane 快照再渲染，绝不据事件载荷出图；
//! - C9：同一卡任意时刻只有一个 PATCH 在飞、版本只向前（高频事
//!   件在锁上自然合并）；PATCH 失败只 warn——呈现待同步，不反向
//!   改任务状态，不重试（下一事件自然带来新快照，无重试风暴）；
//! - 无卡任务（CardPending 半完成态）跳过不补卡（普通投递失败不
//!   擅自补卡）。

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use dashmap::DashMap;
use tokio::sync::broadcast::error::RecvError;
use tracing::warn;

use crate::channels::cards::taskcard::task_card;
use crate::channels::hub::{ChannelHub, ChannelInstance};
use crate::kernel::Kernel;
use crate::types::ExecTaskId;

/// per-task 串行 PATCH 锁注册表（hub 持有，relay 与回调/分流刷新
/// 共用）。每任务一条目，随任务数自然有界（进程内语义，重启清空）。
#[derive(Default)]
pub(crate) struct ExecCardPatches {
    locks: DashMap<ExecTaskId, Arc<tokio::sync::Mutex<()>>>,
}

impl ExecCardPatches {
    /// 取某任务的串行锁（无则建；同一卡任意时刻只有一个 PATCH 在
    /// 飞——等待者在锁上排队，拿到锁后重读快照，内容只向前）。
    /// `pub(super)`：增量 8 换代执行（renewal）与刷新共用同一把
    /// per-task 锁——换代与 PATCH 不得在同一卡上并发交错。
    pub(super) fn lock(&self, task_id: &ExecTaskId) -> Arc<tokio::sync::Mutex<()>> {
        Arc::clone(
            self.locks
                .entry(task_id.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .value(),
        )
    }
}

/// 卡面刷新的唯一入口（relay / 回调 / 分流受理共用）：per-task 锁
/// 内重读 store + 快照 → 渲染 → 整卡 PATCH。调用方的一切上下文
/// （事件载荷、点击时刻的状态）都只是提示，锁内快照才是事实（N7）。
pub(crate) async fn refresh_task_card(
    kernel: &Arc<Kernel>,
    instances: &Arc<DashMap<String, ChannelInstance>>,
    patches: &ExecCardPatches,
    task_id: &ExecTaskId,
) {
    let serial = patches.lock(task_id);
    let _guard = serial.lock().await;
    // 锁内重读（N7：快照是事实，事件是提示）。
    let task = match kernel.exec_task_store().get(task_id).await {
        Ok(Some(task)) => task,
        Ok(None) => return,
        Err(e) => {
            warn!(task_id = %task_id, error = %e, "exec card refresh: task lookup failed");
            return;
        }
    };
    // 无卡（CardPending 半完成态）不补卡；通道实例已不在（关停
    // 中）同样跳过。
    let Some(card_msg_id) = task.card_msg_id.clone() else {
        return;
    };
    let adapter = match instances.get(&task.channel_name) {
        Some(instance) => Arc::clone(&instance.adapter),
        None => return,
    };
    let snap = kernel.exec_scheduler().snapshot(task_id);
    // 增量 5（N9）：结果行事实在同一 per-task 锁内读取——快照语
    // 义不变，渲染依据仍是「锁内重读的事实」，事件只是提示。
    let latest = super::latest_result_or_none(kernel, task_id).await;
    // 增量 6（N11/D9）：中断轮凭据同锁内读取——重启后 lane 无
    // current，卡面凭最近一轮 Run 事实如实显示「已中断 · 待核
    // 对」，不凭旧 binding 显示正常。
    let latest_run = super::latest_run_or_none(kernel, task_id).await;
    let card = task_card(
        &task,
        &snap,
        task.card_generation,
        latest.as_ref(),
        latest_run.as_ref(),
    );
    if let Err(e) = adapter.update_card(&card_msg_id, &card).await {
        // PATCH 失败只 warn：呈现待同步，不反向改任务状态，不重试
        // （下一事件自然带来新快照——无重试风暴，C9）。
        warn!(
            task_id = %task_id,
            card_msg_id = %card_msg_id,
            error = %e,
            "exec task card patch failed; display stays stale until the next hint"
        );
    }
}

/// 订阅调度事件，逐事件刷新对应任务卡（`ChannelHub::start_all` 末
/// 尾调用）。仅当存在启用 `exec_tasks` 的通道实例时 spawn——卡只
/// 在通道侧发，无实例即无卡可刷（warn 记录跳过原因）。
pub(crate) fn spawn_exec_relay(hub: &ChannelHub, kernel: &Arc<Kernel>) {
    let instances = hub.instances_handle();
    if !instances.iter().any(|i| i.config.exec_tasks) {
        warn!("no exec_tasks-enabled channel instance; exec card relay skipped");
        return;
    }
    let patches = hub.exec_patches();
    let mut rx = kernel.exec_events();
    // relay 只持 Weak：kernel 拆毁（关停）即随 broadcast 关闭退出。
    let kernel_weak = Arc::downgrade(kernel);
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(event) => {
                    let task_id = event.task_id().clone();
                    let Some(kernel) = kernel_weak.upgrade() else {
                        break;
                    };
                    refresh_task_card(&kernel, &instances, &patches, &task_id).await;
                }
                // 丢提示不丢事实：下一事件带来新快照，warn 即可。
                Err(RecvError::Lagged(n)) => {
                    warn!(
                        n,
                        "exec event relay lagged; hints dropped, snapshots stay authoritative"
                    );
                }
                Err(RecvError::Closed) => break,
            }
        }
    });
}

/// L1 换代 sweep（chat-flow 增量 8，§8-L1）：hub relay 同进程的
/// 低频清扫（`exec.card_renew_sweep_secs`，默认 30 分钟），对
/// `exec_tasks` 通道的活动有卡任务跑换代决策并执行临期换代
/// （`RenewSoon`）；空闲过期任务不在这里换——`RenewOnReturn`
/// 由分流臂在用户返回时执行。首 tick 延迟一个周期：启动时刻的
/// 呈现一致性由 `start_all` 的 boot 刷新负责，sweep 只管周期换
/// 代。与事件 relay 同一纪律：只持 Weak，kernel 拆毁即退出。
pub(crate) fn spawn_renewal_sweep(hub: &ChannelHub, kernel: &Arc<Kernel>) {
    let instances = hub.instances_handle();
    if !instances.iter().any(|i| i.config.exec_tasks) {
        warn!("no exec_tasks-enabled channel instance; exec renewal sweep skipped");
        return;
    }
    let patches = hub.exec_patches();
    let margin = Duration::from_secs(kernel.exec_config().card_renew_margin_secs);
    let period = Duration::from_secs(kernel.exec_config().card_renew_sweep_secs);
    let kernel_weak = Arc::downgrade(kernel);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            let Some(kernel) = kernel_weak.upgrade() else {
                break;
            };
            super::renewal::sweep_once(&kernel, &instances, &patches, margin, Utc::now()).await;
        }
    });
}
