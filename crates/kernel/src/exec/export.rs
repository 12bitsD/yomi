//! 超限结果正文的 Markdown 导出（chat-flow 增量 8）：N9「完整
//! Markdown 导出」的首版交付点。
//!
//! 设计依据 docs/design/chat-flow-technical-design.md N9：
//! - 导出读**权威正文**（`exec_results`，先保存再公布的那一份）
//!   ——归属头与正文分隔，原始正文一字不改（字节级一致）；
//! - 一 Run 一份导出：同路径已存在不覆盖（先写临时名再 rename，
//!   幂等；正文 insert-once 不变，重导出不改写既存文件）；
//! - 导出失败与正文保存分开报错——附件未成功不得显示可访问
//!   （本增量无卡面入口；`task_result` 工具输出如实区分两种结
//!   果，飞书附件上传属真卡链路，凭据到位后接同一导出产物）。

use crate::exec::ExecFactStore;
use crate::types::{KernelError, Result, RunId};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// 导出一轮 Run 的权威正文为 Markdown 文件：
/// `<dir>/exec-results/<task_id>/<run_id>.md`。
///
/// 返回文件路径。正文缺失（该 Run 无权威正文）→ 明确报错，不导
/// 出空文件冒充（N9）。已存在同路径 → 直接返回不覆盖（幂等：
/// 一 Run 一份权威正文，重导出不改写）。写盘经临时名 + rename：
/// 崩溃不留半截文件在目标路径上。
pub async fn export_result_markdown(
    facts: &Arc<dyn ExecFactStore>,
    run_id: &RunId,
    dir: &Path,
) -> Result<PathBuf> {
    let row = facts.result_for(run_id).await?.ok_or_else(|| {
        KernelError::storage(format!(
            "exec result body missing for run {run_id}; export refused (no empty stand-in)"
        ))
    })?;

    let out_dir = dir.join("exec-results").join(row.task_id.as_str());
    tokio::fs::create_dir_all(&out_dir)
        .await
        .map_err(|e| KernelError::storage(format!("create exec-results dir: {e}")))?;
    let target = out_dir.join(format!("{}.md", row.run_id.as_str()));
    // 幂等不覆盖：正文 insert-once 权威不变，已存在即导出已完成。
    if target.exists() {
        return Ok(target);
    }

    // 归属头（task/run/seq/provider/native_session_id/时间）+ 分
    // 隔线；归属缺失如实写「未知」（meta 只规范化归属，正文不动）。
    let provider = row.meta["provider"].as_str().unwrap_or("未知");
    let native = row.meta["native_session_id"].as_str().unwrap_or("未知");
    let header = format!(
        "# 执行任务结果导出\n\n\
         - **Task**: `{}`\n\
         - **Run**: `{}`\n\
         - **轮次**: 第 {} 轮\n\
         - **Provider**: `{}`\n\
         - **原生 Session**: `{}`\n\
         - **保存时间**: {}\n\n\
         ---\n\n",
        row.task_id.as_str(),
        row.run_id.as_str(),
        row.input_seq,
        provider,
        native,
        row.created_at.to_rfc3339(),
    );
    // 原始正文一字不改：header 字节 + body 字节顺序拼接，不追加
    // 尾部换行、不做任何规范化。
    let mut bytes = header.into_bytes();
    bytes.extend_from_slice(row.body.as_bytes());

    // 先写临时名（同目录，rename 原子）再 rename——崩溃只留临时
    // 文件，目标路径要么不存在要么是完整文件。临时名带进程与纳秒
    // 防并发重名；并发双导出内容相同，rename 结果等价。
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    let tmp = out_dir.join(format!(
        ".{}.md.tmp-{}-{nonce}",
        row.run_id.as_str(),
        std::process::id(),
    ));
    if let Err(e) = tokio::fs::write(&tmp, &bytes).await {
        return Err(KernelError::storage(format!("write export temp file: {e}")));
    }
    if let Err(e) = tokio::fs::rename(&tmp, &target).await {
        let _ = tokio::fs::remove_file(&tmp).await;
        return Err(KernelError::storage(format!(
            "rename export into place: {e}"
        )));
    }
    Ok(target)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::run::RunStatus;
    use crate::exec::{
        CreateExecTask, ExecProvider, ExecTaskSource, ExecTaskStore, RunRecord,
        SqliteExecFactStore, SqliteExecTaskStore,
    };
    use crate::storage::migrations::run_migrations;
    use crate::types::ExecTaskId;
    use chrono::Utc;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn setup() -> (Arc<dyn ExecFactStore>, ExecTaskId, RunId) {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        run_migrations(&pool).await.unwrap();
        let task_store = SqliteExecTaskStore::new(pool.clone());
        let (task, created) = task_store
            .create(&CreateExecTask {
                channel_name: "test".into(),
                provider: ExecProvider::Kimi,
                goal: "g".into(),
                working_dir: None,
                created_by: "ou_t".into(),
                source: ExecTaskSource::Skill,
                dedup_key: "d".into(),
            })
            .await
            .unwrap();
        assert!(created);
        (
            Arc::new(SqliteExecFactStore::new(pool)),
            task.id,
            RunId::new(),
        )
    }

    async fn save_body(
        facts: &Arc<dyn ExecFactStore>,
        task_id: &ExecTaskId,
        run_id: &RunId,
        body: &str,
    ) {
        facts
            .run_started(&RunRecord {
                run_id: run_id.clone(),
                task_id: task_id.clone(),
                input_seq: 1,
                text: "输入".into(),
                image_keys: vec![],
                status: RunStatus::Completed,
                started_at: Utc::now(),
                ended_at: None,
            })
            .await
            .unwrap();
        assert!(facts
            .save_result(
                run_id,
                task_id,
                1,
                body,
                &serde_json::json!({"provider": "kimi", "native_session_id": "native-1"}),
            )
            .await
            .unwrap());
    }

    /// 文件正文字节（归属头分隔线之后的一切，原样）。
    async fn body_bytes_of(path: &Path) -> Vec<u8> {
        let bytes = tokio::fs::read(path).await.unwrap();
        let sep = b"\n\n---\n\n";
        let at = bytes
            .windows(sep.len())
            .position(|w| w == sep)
            .expect("attribution separator present");
        bytes[at + sep.len()..].to_vec()
    }

    #[tokio::test]
    async fn export_preserves_body_byte_for_byte() {
        let (facts, task_id, run_id) = setup().await;
        // 超长中文 + 代码块 + 奇异字节组合（结尾无换行）。
        let body = format!(
            "{}\n\n```rust\nfn main() {{ println!(\"嵌套 ``` 反引号\"); }}\n```\n\n\
             结尾没有换行",
            "超长中文正文「」『』——重复填充。".repeat(2000)
        );
        save_body(&facts, &task_id, &run_id, &body).await;
        let dir = tempfile::tempdir().unwrap();

        let path = export_result_markdown(&facts, &run_id, dir.path())
            .await
            .unwrap();

        assert_eq!(
            path,
            dir.path()
                .join("exec-results")
                .join(task_id.as_str())
                .join(format!("{}.md", run_id.as_str()))
        );
        // 字节级一致：分隔线以下与权威正文逐字节相等。
        assert_eq!(body_bytes_of(&path).await, body.as_bytes());
        // 归属头字段齐（task/run/seq/provider/native_session_id/时间）。
        let text = tokio::fs::read_to_string(&path).await.unwrap();
        for needle in [
            task_id.as_str(),
            run_id.as_str(),
            "第 1 轮",
            "`kimi`",
            "`native-1`",
            "**保存时间**",
        ] {
            assert!(text.contains(needle), "header missing {needle}");
        }
    }

    #[tokio::test]
    async fn export_is_idempotent_and_never_overwrites() {
        let (facts, task_id, run_id) = setup().await;
        save_body(&facts, &task_id, &run_id, "权威正文").await;
        let dir = tempfile::tempdir().unwrap();

        let first = export_result_markdown(&facts, &run_id, dir.path())
            .await
            .unwrap();
        // 人为改动已导出文件后再导出：不覆盖（幂等——已存在即返回）。
        tokio::fs::write(&first, "被人工改动过的内容")
            .await
            .unwrap();
        let second = export_result_markdown(&facts, &run_id, dir.path())
            .await
            .unwrap();
        assert_eq!(first, second);
        let after = tokio::fs::read_to_string(&second).await.unwrap();
        assert_eq!(after, "被人工改动过的内容", "重复导出不得覆盖既有文件");
        // 目录里不留临时文件。
        let mut entries = tokio::fs::read_dir(first.parent().unwrap()).await.unwrap();
        let mut names = Vec::new();
        while let Some(e) = entries.next_entry().await.unwrap() {
            names.push(e.file_name().to_string_lossy().to_string());
        }
        assert_eq!(names, vec![format!("{}.md", run_id.as_str())], "{names:?}");
    }

    #[tokio::test]
    async fn export_missing_body_fails_honestly() {
        let (facts, _task_id, _run_id) = setup().await;
        let dir = tempfile::tempdir().unwrap();
        let err = export_result_markdown(&facts, &RunId::new(), dir.path())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("body missing"), "{err}");
        // 不产出任何文件冒充（N9：未成功不得显示可访问）。
        assert!(!dir.path().join("exec-results").exists());
    }
}
