//! 执行任务主卡（chat-flow 增量 4：增量 2 占位卡 → 快照驱动控制
//! 卡）：任务身份的通道呈现、Thread 锚点与运行控制面——用户在本卡
//! Thread 回复即向任务交办输入，经卡面按钮停止并暂停/恢复队列。
//!
//! 设计依据 docs/design/chat-flow-technical-design.md N2/C2/D2/C9/N7/R4：
//! - N2/C2：卡即 Thread 锚（`thread_root_msg_id == card_msg_id`），
//!   绑定三态如实呈现——「已登记·待初始化」（合法新任务，不伪造
//!   Session 已存在）与「绑定损坏」（明确失败态）严格区分；
//! - D2：受理队列是进程内事实，卡面必须标注「重启不保留」，不得
//!   把旧输入显示为仍已排队；
//! - N7/C9：卡面唯一渲染依据是任务登记 + lane 快照（事件只是提
//!   示，快照是事实）；按钮 value 携带 `task`/`run`/`gen`，旧代
//!   与失配回调在权威状态重核后一律不生效；
//! - R4：业务失败不冒充任务成功——失败轮如实标「失败」。
//!
//! 整卡 send/PATCH（schema 2.0，与 info 卡同信封风格）；CardKit
//! 局部更新是 P5 的事。

use serde_json::json;

use crate::exec::{BindingState, ExecTask, ExecTaskStatus, LaneSnapshot, RunStatus};

/// 主卡整卡 JSON（快照渲染）。`card_generation` 只进按钮 value
/// （C9 旧代重核凭据），不参与卡面内容。
pub(crate) fn task_card(task: &ExecTask, snap: &LaneSnapshot, card_generation: i64) -> String {
    let id_short = &task.id.as_str()[..12.min(task.id.as_str().len())];
    let mut elements = vec![json!({
        "tag": "markdown",
        "content": format!(
            "- **Task**: `{id_short}` · **Provider**: `{}`\n\
             - **状态**: {}\n\
             - **队列**: {}\n\
             - **创建者**: `{}` · **创建**: {}",
            task.provider,
            status_line(task, snap),
            queue_line(snap),
            task.created_by,
            crate::storage::format_age(task.created_at),
        ),
    })];
    let buttons = control_buttons(task, snap, card_generation);
    if !buttons.is_empty() {
        elements.push(json!({ "tag": "hr" }));
        elements.push(json!({ "tag": "column_set", "columns": buttons }));
    }
    elements.push(json!({ "tag": "hr" }));
    elements.push(json!({
        "tag": "markdown",
        "text_size": "notation",
        "content": "在本卡 Thread 回复即向本任务交办；按钮操作按最新登记状态核对，过期操作不生效。",
    }));
    json!({
        "schema": "2.0",
        "header": {
            "template": "turquoise",
            "title": { "tag": "plain_text", "content": format!("🧩 任务卡 · {}", goal_excerpt(&task.goal)) },
        },
        "body": { "elements": elements },
    })
    .to_string()
}

/// 状态区一行（如实映射增量 3 lane 语义）。
fn status_line(task: &ExecTask, snap: &LaneSnapshot) -> String {
    if task.status == ExecTaskStatus::Archived {
        return "⏹ 任务已归档".to_string();
    }
    // blocked_unknown 与 Run Unknown 同义呈现：派发/终态确认丢失
    // ——如实阻断后续派发，不猜不跳（N3）。
    if snap.blocked_unknown {
        return "⚠️ 状态待核对（已阻断后续派发）".to_string();
    }
    match snap.current.as_ref() {
        Some(run) => {
            let seq = run.input_seq;
            match run.status {
                // Starting：尚无 Provider 开始证据，不显示已开始（C9）。
                RunStatus::Starting => format!("启动中 · 第 {seq} 轮（开始未确认）"),
                RunStatus::Running => format!(
                    "执行中 · 第 {seq} 轮（{}）",
                    crate::storage::format_age(run.started_at)
                ),
                // 停止中 ≠ 已停止（C6）：未确认前不启动后续。
                RunStatus::Stopping => format!("停止中 · 第 {seq} 轮（未确认前不启动后续）"),
                RunStatus::Stopped => format!("已停止 · 第 {seq} 轮"),
                RunStatus::Completed => format!("第 {seq} 轮已结束（完成）"),
                // R4：业务失败不冒充任务成功。
                RunStatus::Failed => format!("第 {seq} 轮已结束（失败）"),
                RunStatus::Unknown => "⚠️ 状态待核对（已阻断后续派发）".to_string(),
            }
        }
        // 无 current：绑定三态（增量 2 文案）——「待初始化」不是损
        // 坏，损坏是明确失败态（N2）。
        None => match task.binding {
            BindingState::Uninitialized => "已登记 · 待初始化".to_string(),
            BindingState::Bound => "已绑定".to_string(),
            BindingState::Broken => "⚠️ 绑定损坏".to_string(),
        },
    }
}

/// 队列区一行（D2 标签保留：进程内受理，重启不保留）。
fn queue_line(snap: &LaneSnapshot) -> String {
    let paused = if snap.queued > 0 && snap.paused {
        "已暂停 · "
    } else {
        ""
    };
    format!(
        "已受理待执行 {} 条（{paused}进程内 · 重启不保留）",
        snap.queued
    )
}

/// 控制区按钮列（`column_set` 的 columns；无可合法操作时为空——
/// Stopping / 状态待核对 / 已归档一律不出按钮）。
fn control_buttons(task: &ExecTask, snap: &LaneSnapshot, gen: i64) -> Vec<serde_json::Value> {
    if task.status == ExecTaskStatus::Archived
        || snap.blocked_unknown
        || snap
            .current
            .as_ref()
            .is_some_and(|r| r.status == RunStatus::Stopping)
    {
        return Vec::new();
    }
    let mut columns = Vec::new();
    // Running → ⏹ 停止并暂停（value 锁定当前 run——旧按钮套不到
    // 新 Run，C6/R7）。
    if let Some(run) = snap
        .current
        .as_ref()
        .filter(|r| r.status == RunStatus::Running)
    {
        columns.push(button_column(
            "⏹ 停止并暂停",
            "danger",
            &json!({
                "action": "exec_stop",
                "task": task.id.as_str(),
                "run": run.run_id.as_str(),
                "gen": gen,
            }),
        ));
    }
    // paused（无论有无 current）→ ▶ 恢复队列。
    if snap.paused {
        columns.push(button_column(
            "▶ 恢复队列",
            "primary",
            &json!({
                "action": "exec_resume",
                "task": task.id.as_str(),
                "run": null,
                "gen": gen,
            }),
        ));
    }
    columns
}

/// 单个按钮列（与 obs/approval 卡同一 button 形态）。
fn button_column(text: &str, kind: &str, value: &serde_json::Value) -> serde_json::Value {
    json!({
        "tag": "column", "width": "auto",
        "elements": [{
            "tag": "button",
            "size": "small",
            "text": { "tag": "plain_text", "content": text },
            "type": kind,
            "behaviors": [{ "type": "callback", "value": value }],
        }],
    })
}

/// 卡标题里的 goal 摘录：前 30 字，换行压平（标题是单行
/// `plain_text`，用户原文可能带换行）。
fn goal_excerpt(goal: &str) -> String {
    let flat: String = goal.split_whitespace().collect::<Vec<_>>().join(" ");
    let truncated: String = flat.chars().take(30).collect();
    if flat.chars().count() > 30 {
        format!("{truncated}…")
    } else {
        truncated
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::{ExecProvider, ExecTaskSource, RunRecord};
    use crate::types::{ExecTaskId, RunId};
    use chrono::Utc;

    fn task(binding: BindingState) -> ExecTask {
        ExecTask {
            id: ExecTaskId::new(),
            channel_name: "feishu".into(),
            provider: ExecProvider::Kimi,
            status: ExecTaskStatus::Active,
            binding,
            provider_session_id: None,
            thread_root_msg_id: None,
            card_msg_id: None,
            card_generation: 0,
            goal: "把登錄頁的校驗邏輯抽出來獨立成模塊，跑通全量測試，再補一條用例覆蓋空輸入與超時"
                .into(),
            working_dir: None,
            created_by: "ou_user".into(),
            source: ExecTaskSource::Entry,
            dedup_key: "k1".into(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn run(seq: u64, status: RunStatus) -> RunRecord {
        RunRecord {
            run_id: RunId::new(),
            task_id: ExecTaskId::new(),
            input_seq: seq,
            text: "输入原文".into(),
            image_keys: vec![],
            status,
            started_at: Utc::now(),
            ended_at: None,
        }
    }

    fn snap(
        current: Option<RunRecord>,
        queued: usize,
        paused: bool,
        blocked_unknown: bool,
    ) -> LaneSnapshot {
        LaneSnapshot {
            paused,
            current,
            queued,
            blocked_unknown,
        }
    }

    /// 卡面全部按钮的 callback value（解析整卡 JSON）。
    fn button_values(card: &str) -> Vec<serde_json::Value> {
        let parsed: serde_json::Value = serde_json::from_str(card).unwrap();
        let mut values = Vec::new();
        for el in parsed["body"]["elements"].as_array().unwrap() {
            if el["tag"] == "column_set" {
                for col in el["columns"].as_array().unwrap() {
                    values.push(col["elements"][0]["behaviors"][0]["value"].clone());
                }
            }
        }
        values
    }

    // ── 增量 4 场景 6：渲染单测（各形态文案与按钮有无）──────────

    #[test]
    fn idle_card_renders_binding_states_and_queue_honestly() {
        // 三态文案严格区分（N2：待初始化 ≠ 损坏 ≠ 已绑定）。
        let idle = snap(None, 0, false, false);
        let uninit = task_card(&task(BindingState::Uninitialized), &idle, 0);
        assert!(uninit.contains("已登记 · 待初始化"), "{uninit}");
        assert!(!uninit.contains("绑定损坏"), "{uninit}");
        let broken = task_card(&task(BindingState::Broken), &idle, 0);
        assert!(broken.contains("绑定损坏"), "{broken}");
        let bound = task_card(&task(BindingState::Bound), &idle, 0);
        assert!(bound.contains("已绑定"), "{bound}");

        // 队列区如实标注进程内语义（D2：重启不保留）。
        let card = task_card(
            &task(BindingState::Uninitialized),
            &snap(None, 3, false, false),
            0,
        );
        assert!(card.contains("已受理待执行 3 条"), "{card}");
        assert!(card.contains("进程内 · 重启不保留"), "{card}");
        // 未暂停不标注「已暂停」。
        assert!(!card.contains("已暂停"), "{card}");

        // 空队列 + 无 current + 未暂停：无可合法操作，无按钮回调。
        assert!(button_values(&uninit).is_empty(), "{uninit}");

        // 结构：turquoise 头、goal 摘录（30 字截断）、合法 JSON 整卡。
        assert!(card.contains("\"template\":\"turquoise\""), "{card}");
        assert!(card.contains("🧩 任务卡 · "), "{card}");
        assert!(card.contains("…"), "{card}");
        let parsed: serde_json::Value = serde_json::from_str(&card).unwrap();
        assert_eq!(parsed["schema"], "2.0");
    }

    #[test]
    fn running_card_shows_stop_button_with_full_value() {
        let t = task(BindingState::Bound);
        let r = run(3, RunStatus::Running);
        let run_id = r.run_id.as_str().to_string();
        let card = task_card(&t, &snap(Some(r), 1, false, false), 7);
        assert!(card.contains("执行中 · 第 3 轮"), "{card}");
        // 未暂停：只有 ⏹，value 含 task/run/gen 三字段（C9 重核凭据）。
        let values = button_values(&card);
        assert_eq!(values.len(), 1, "{card}");
        assert_eq!(
            values[0],
            serde_json::json!({
                "action": "exec_stop",
                "task": t.id.as_str(),
                "run": run_id,
                "gen": 7,
            })
        );
    }

    #[test]
    fn stopping_card_blocks_all_buttons() {
        let card = task_card(
            &task(BindingState::Bound),
            &snap(Some(run(2, RunStatus::Stopping)), 1, true, false),
            0,
        );
        assert!(card.contains("停止中 · 第 2 轮"), "{card}");
        assert!(card.contains("未确认前不启动后续"), "{card}");
        assert!(button_values(&card).is_empty(), "Stopping 不出任何按钮");
    }

    #[test]
    fn terminal_cards_report_truthfully() {
        let stopped = task_card(
            &task(BindingState::Bound),
            &snap(Some(run(4, RunStatus::Stopped)), 0, true, false),
            0,
        );
        assert!(stopped.contains("已停止 · 第 4 轮"), "{stopped}");
        let completed = task_card(
            &task(BindingState::Bound),
            &snap(Some(run(4, RunStatus::Completed)), 0, false, false),
            0,
        );
        assert!(completed.contains("第 4 轮已结束（完成）"), "{completed}");
        let failed = task_card(
            &task(BindingState::Bound),
            &snap(Some(run(4, RunStatus::Failed)), 0, false, false),
            0,
        );
        // R4：业务失败不冒充任务成功。
        assert!(failed.contains("第 4 轮已结束（失败）"), "{failed}");
        assert!(!failed.contains("（完成）"), "{failed}");
    }

    #[test]
    fn unknown_card_marks_recheck_and_blocks_buttons() {
        let card = task_card(&task(BindingState::Bound), &snap(None, 2, false, true), 0);
        assert!(card.contains("⚠️ 状态待核对（已阻断后续派发）"), "{card}");
        assert!(button_values(&card).is_empty(), "blocked 不出按钮");
    }

    #[test]
    fn paused_card_marks_queue_and_offers_resume() {
        // queued>0 + paused → 队列标注「已暂停」+ ▶（run 为 null）。
        let card = task_card(&task(BindingState::Bound), &snap(None, 2, true, false), 5);
        assert!(card.contains("已暂停 · 进程内 · 重启不保留"), "{card}");
        let values = button_values(&card);
        assert_eq!(values.len(), 1, "{card}");
        assert_eq!(
            values[0],
            serde_json::json!({
                "action": "exec_resume",
                "task": values[0]["task"].clone(),
                "run": null,
                "gen": 5,
            })
        );
        assert!(!values[0]["task"].as_str().unwrap().is_empty());
        // 空队列 + paused：不标注「已暂停」，▶ 仍在（无论有无 current）。
        let empty = task_card(&task(BindingState::Bound), &snap(None, 0, true, false), 5);
        assert!(!empty.contains("已暂停"), "{empty}");
        assert_eq!(button_values(&empty).len(), 1, "{empty}");
    }

    #[test]
    fn archived_card_shows_no_buttons() {
        let mut t = task(BindingState::Bound);
        t.status = ExecTaskStatus::Archived;
        let card = task_card(
            &t,
            &snap(Some(run(1, RunStatus::Running)), 1, true, false),
            0,
        );
        assert!(card.contains("任务已归档"), "{card}");
        assert!(button_values(&card).is_empty(), "archived 不出按钮");
    }

    #[test]
    fn goal_excerpt_flattens_and_truncates() {
        assert_eq!(goal_excerpt("短目标"), "短目标");
        assert_eq!(goal_excerpt("  多行\n目标\t带空白  "), "多行 目标 带空白");
        let long = "一".repeat(40);
        let excerpt = goal_excerpt(&long);
        assert_eq!(excerpt.chars().count(), 31, "30 字 + 省略号");
        assert!(excerpt.ends_with('…'));
    }
}
