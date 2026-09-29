//! 执行任务占位主卡（chat-flow 增量 2）：任务身份的通道呈现与
//! Thread 锚点——用户在本卡 Thread 回复即向任务交办输入。
//!
//! 设计依据 docs/design/chat-flow-technical-design.md N2/C2/D2：
//! - N2/C2：卡即 Thread 锚（`thread_root_msg_id == card_msg_id`），
//!   绑定三态如实呈现——「已登记·待初始化」（合法新任务，不伪造
//!   Session 已存在）与「绑定损坏」（明确失败态）严格区分；
//! - D2：受理计数是进程内事实，卡面必须标注「重启不保留」，不得把
//!   旧输入显示为仍已排队。
//!
//! 整卡 send/PATCH（schema 2.0，与 info 卡同信封风格）；CardKit
//! 局部更新是 P5 的事。本卡无按钮——回调面 P2 才接。

use serde_json::json;

use crate::exec::{BindingState, ExecTask};

/// 占位主卡整卡 JSON。`accepted` = 本进程已受理输入条数
/// （`ExecInbox::len`）。
pub(crate) fn task_card(task: &ExecTask, accepted: usize) -> String {
    // 绑定三态如实：合法新任务的「待初始化」不是损坏，损坏是明确失
    // 败态（N2）——文案分开，互不冒充。
    let binding_text = match task.binding {
        BindingState::Uninitialized => "已登记 · 待初始化",
        BindingState::Bound => "已绑定",
        BindingState::Broken => "⚠️ 绑定损坏",
    };
    let id_short = &task.id.as_str()[..12.min(task.id.as_str().len())];
    let elements = vec![
        json!({
            "tag": "markdown",
            "content": format!(
                "- **Task**: `{id_short}` · **Provider**: `{}`\n\
                 - **状态**: {binding_text}\n\
                 - **本进程已受理输入**: {accepted} 条（重启不保留）\n\
                 - **创建者**: `{}` · **创建**: {}",
                task.provider,
                task.created_by,
                crate::storage::format_age(task.created_at),
            ),
        }),
        json!({ "tag": "hr" }),
        json!({
            "tag": "markdown",
            "text_size": "notation",
            "content": "在本卡 Thread 回复即向本任务交办；执行能力接入中（后续阶段提供停止/恢复）。",
        }),
    ];
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
    use crate::exec::{ExecProvider, ExecTaskSource, ExecTaskStatus};
    use crate::types::ExecTaskId;
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

    #[test]
    fn card_renders_binding_states_and_accepted_count_honestly() {
        // 三态文案严格区分（N2：待初始化 ≠ 损坏 ≠ 已绑定）。
        let uninit = task_card(&task(BindingState::Uninitialized), 0);
        assert!(uninit.contains("已登记 · 待初始化"), "{uninit}");
        assert!(!uninit.contains("绑定损坏"), "{uninit}");
        let broken = task_card(&task(BindingState::Broken), 0);
        assert!(broken.contains("绑定损坏"), "{broken}");
        let bound = task_card(&task(BindingState::Bound), 0);
        assert!(bound.contains("已绑定"), "{bound}");

        // 受理计数如实标注进程内语义（D2：重启不保留）。
        let card = task_card(&task(BindingState::Uninitialized), 3);
        assert!(card.contains("本进程已受理输入"), "{card}");
        assert!(card.contains("3 条"), "{card}");
        assert!(card.contains("重启不保留"), "{card}");

        // 结构：turquoise 头、goal 摘录（30 字截断）、无按钮回调。
        assert!(card.contains("\"template\":\"turquoise\""), "{card}");
        assert!(card.contains("🧩 任务卡 · "), "{card}");
        assert!(card.contains("…"), "{card}");
        assert!(!card.contains("behaviors"), "{card}");
        // 合法 JSON 整卡。
        let parsed: serde_json::Value = serde_json::from_str(&card).unwrap();
        assert_eq!(parsed["schema"], "2.0");
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
