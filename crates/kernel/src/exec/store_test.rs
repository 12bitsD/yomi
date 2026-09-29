use super::*;

use sqlx::sqlite::SqlitePoolOptions;

async fn create_test_pool() -> SqlitePool {
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap()
}

async fn create_test_store() -> SqliteExecTaskStore {
    let pool = create_test_pool().await;
    crate::storage::migrations::run_migrations(&pool)
        .await
        .unwrap();
    SqliteExecTaskStore::new(pool)
}

fn input(channel: &str, dedup: &str, goal: &str) -> CreateExecTask {
    CreateExecTask {
        channel_name: channel.into(),
        provider: ExecProvider::Kimi,
        goal: goal.into(),
        working_dir: None,
        created_by: "ou_test".into(),
        source: ExecTaskSource::Skill,
        dedup_key: dedup.into(),
    }
}

#[tokio::test]
async fn test_create_new_task() {
    let store = create_test_store().await;

    let (task, created) = store
        .create(&input("feishu", "k1", "做 W1 登记"))
        .await
        .unwrap();
    assert!(created);
    assert!(task.id.as_str().starts_with("task_"));
    assert_eq!(task.binding, BindingState::Uninitialized);
    assert_eq!(task.status, ExecTaskStatus::Active);
    assert_eq!(task.card_generation, 0);
    assert_eq!(task.provider_session_id, None);
    assert_eq!(task.thread_root_msg_id, None);
    assert_eq!(task.card_msg_id, None);
    assert_eq!(task.provider, ExecProvider::Kimi);
    assert_eq!(task.source, ExecTaskSource::Skill);
    assert_eq!(task.created_at, task.updated_at);

    // get 读回与内存一致
    let loaded = store.get(&task.id).await.unwrap().unwrap();
    assert_eq!(loaded, task);
}

#[tokio::test]
async fn test_create_dedup_hit_returns_existing_unchanged() {
    let store = create_test_store().await;

    let (first, created) = store
        .create(&input("feishu", "k1", "原始 goal"))
        .await
        .unwrap();
    assert!(created);

    // 同 channel+dedup_key 重送（goal 不同）→ 返回原任务，不改既有行
    let (second, created) = store
        .create(&input("feishu", "k1", "改写后的 goal"))
        .await
        .unwrap();
    assert!(!created);
    assert_eq!(second.id, first.id);
    assert_eq!(second.goal, "原始 goal");

    // DB 里的行也未被重送修改
    let loaded = store.get(&first.id).await.unwrap().unwrap();
    assert_eq!(loaded.goal, "原始 goal");
    assert_eq!(loaded.updated_at, first.updated_at);
}

#[tokio::test]
async fn test_create_different_dedup_key_creates_new_task() {
    let store = create_test_store().await;

    let (first, _) = store.create(&input("feishu", "k1", "g1")).await.unwrap();
    let (second, created) = store.create(&input("feishu", "k2", "g1")).await.unwrap();
    assert!(created);
    assert_ne!(first.id, second.id);

    // 不同通道同 dedup_key 也是独立任务
    let (third, created) = store.create(&input("rpc", "k1", "g1")).await.unwrap();
    assert!(created);
    assert_ne!(first.id, third.id);
}

#[tokio::test]
async fn test_bind_state_machine() {
    let store = create_test_store().await;
    let (task, _) = store.create(&input("feishu", "k1", "g")).await.unwrap();

    // Uninitialized -> Bound 成功
    let bound = store
        .bind_provider_session(&task.id, "native-sess-1")
        .await
        .unwrap();
    assert_eq!(bound.binding, BindingState::Bound);
    assert_eq!(bound.provider_session_id.as_deref(), Some("native-sess-1"));

    // 同 id 再 bind：幂等（不刷 updated_at）
    let again = store
        .bind_provider_session(&task.id, "native-sess-1")
        .await
        .unwrap();
    assert_eq!(again, bound);

    // 不同 id bind → Err（不得静默换绑）
    let rebind = store.bind_provider_session(&task.id, "native-sess-2").await;
    assert!(rebind.is_err());
    let loaded = store.get(&task.id).await.unwrap().unwrap();
    assert_eq!(loaded.provider_session_id.as_deref(), Some("native-sess-1"));

    // Broken 后 bind → Err（绝不自动重建）
    let broken = store
        .mark_broken(&task.id, "native session gone")
        .await
        .unwrap();
    assert_eq!(broken.binding, BindingState::Broken);
    let bind_after_broken = store.bind_provider_session(&task.id, "native-sess-1").await;
    assert!(bind_after_broken.is_err());
}

#[tokio::test]
async fn test_mark_broken_readable_and_from_uninitialized() {
    let store = create_test_store().await;
    let (task, _) = store.create(&input("feishu", "k1", "g")).await.unwrap();

    // Bound -> Broken
    let bound = store
        .bind_provider_session(&task.id, "native-sess-1")
        .await
        .unwrap();
    let broken = store.mark_broken(&bound.id, "bind lost").await.unwrap();
    assert_eq!(broken.binding, BindingState::Broken);
    // 损坏后 get 仍可读，且原绑定信息保留（便于排查）
    let loaded = store.get(&task.id).await.unwrap().unwrap();
    assert_eq!(loaded.binding, BindingState::Broken);
    assert_eq!(loaded.provider_session_id.as_deref(), Some("native-sess-1"));

    // Uninitialized 也可 mark_broken（绑定尚未建立就失败的情形）
    let (fresh, _) = store.create(&input("feishu", "k2", "g")).await.unwrap();
    let broken = store
        .mark_broken(&fresh.id, "simulated failure")
        .await
        .unwrap();
    assert_eq!(broken.binding, BindingState::Broken);
}

#[tokio::test]
async fn test_set_thread_and_card_and_find_by_dedup() {
    let store = create_test_store().await;
    let (task, _) = store.create(&input("feishu", "k1", "g")).await.unwrap();
    // 未发卡时期限事实列为 NULL（v29 增量 8）。
    assert_eq!(task.card_sent_at, None);
    assert_eq!(task.card_entity_created_at, None);

    let updated = store
        .set_thread_and_card(&task.id, "om_thread_root", "om_card")
        .await
        .unwrap();
    assert_eq!(
        updated.thread_root_msg_id.as_deref(),
        Some("om_thread_root")
    );
    assert_eq!(updated.card_msg_id.as_deref(), Some("om_card"));
    // 发卡时刻随回填写入（增量 8，§8-L1 期限判据）。
    let sent_at = updated
        .card_sent_at
        .expect("card_sent_at backfilled on announce");
    assert!(
        (chrono::Utc::now() - sent_at) < chrono::Duration::minutes(1),
        "{sent_at}"
    );

    // find_by_dedup 命中且读得到回填值
    let found = store.find_by_dedup("feishu", "k1").await.unwrap().unwrap();
    assert_eq!(found.id, task.id);
    assert_eq!(found.thread_root_msg_id.as_deref(), Some("om_thread_root"));
    assert_eq!(found.card_sent_at, Some(sent_at));

    // 不存在 id → Err
    let missing = store
        .set_thread_and_card(&ExecTaskId::new(), "t", "c")
        .await;
    assert!(missing.is_err());
}

/// 增量 8（§8-L1/C2）：换代切换——代次原子 +1、映射更新、发卡时
/// 刻回填；执行身份列不动；不存在 id 报错。
#[tokio::test]
async fn test_bump_card_generation_switches_mapping_atomically() {
    let store = create_test_store().await;
    let (task, _) = store.create(&input("feishu", "k1", "g")).await.unwrap();
    let announced = store
        .set_thread_and_card(&task.id, "om_root", "om_card_1")
        .await
        .unwrap();
    let first_sent_at = announced.card_sent_at.unwrap();

    let sent_at = chrono::Utc::now();
    let bumped = store
        .bump_card_generation(&task.id, "om_root", "om_card_2", sent_at)
        .await
        .unwrap();
    assert_eq!(bumped.card_generation, announced.card_generation + 1);
    assert_eq!(bumped.card_msg_id.as_deref(), Some("om_card_2"));
    // 原 Thread 换代根不变（新卡落在同一 Thread 内）。
    assert_eq!(bumped.thread_root_msg_id.as_deref(), Some("om_root"));
    assert_eq!(bumped.card_sent_at, Some(sent_at));
    assert_ne!(bumped.card_sent_at, Some(first_sent_at));
    // 只换呈现：执行身份列逐项不动（D12）。
    assert_eq!(bumped.binding, announced.binding);
    assert_eq!(bumped.provider_session_id, announced.provider_session_id);
    assert_eq!(bumped.status, announced.status);
    assert_eq!(bumped.goal, announced.goal);
    assert_eq!(bumped.dedup_key, announced.dedup_key);

    // 再换一代：代次继续向前。
    let bumped2 = store
        .bump_card_generation(&task.id, "om_root", "om_card_3", chrono::Utc::now())
        .await
        .unwrap();
    assert_eq!(bumped2.card_generation, bumped.card_generation + 1);

    // 不存在 id → Err。
    let missing = store
        .bump_card_generation(&ExecTaskId::new(), "t", "c", chrono::Utc::now())
        .await;
    assert!(missing.is_err());
}

#[tokio::test]
async fn test_find_by_thread_root() {
    let store = create_test_store().await;
    let (task, _) = store.create(&input("feishu", "k1", "g")).await.unwrap();

    // 未回填前查不到
    assert!(store
        .find_by_thread_root("feishu", "om_thread_root")
        .await
        .unwrap()
        .is_none());

    store
        .set_thread_and_card(&task.id, "om_thread_root", "om_card")
        .await
        .unwrap();

    // 命中：channel + root 双条件（走 idx_exec_tasks_thread 索引）
    let found = store
        .find_by_thread_root("feishu", "om_thread_root")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.id, task.id);
    assert_eq!(found.card_msg_id.as_deref(), Some("om_card"));

    // 同 root 不同通道不命中；归档后仍命中（归档任务的 Thread 输入
    // 要分流到「已归档拒收」而不是落回普通 chat）。
    assert!(store
        .find_by_thread_root("telegram", "om_thread_root")
        .await
        .unwrap()
        .is_none());
    store.archive(&task.id).await.unwrap();
    let archived = store
        .find_by_thread_root("feishu", "om_thread_root")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(archived.status, ExecTaskStatus::Archived);
}

#[tokio::test]
async fn test_archive_keeps_row() {
    let store = create_test_store().await;
    let (task, _) = store.create(&input("feishu", "k1", "g")).await.unwrap();

    let archived = store.archive(&task.id).await.unwrap();
    assert_eq!(archived.status, ExecTaskStatus::Archived);

    // 归档不删行：get 可读
    let loaded = store.get(&task.id).await.unwrap().unwrap();
    assert_eq!(loaded.status, ExecTaskStatus::Archived);

    // 重复归档幂等
    let again = store.archive(&task.id).await.unwrap();
    assert_eq!(again.status, ExecTaskStatus::Archived);
}

#[tokio::test]
async fn test_migrations_idempotent() {
    let pool = create_test_pool().await;

    // 跑两遍不炸（照 migrations_test 现有模式）
    crate::storage::migrations::run_migrations(&pool)
        .await
        .unwrap();
    crate::storage::migrations::run_migrations(&pool)
        .await
        .unwrap();

    // exec_tasks 表与索引已建立
    let table: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='exec_tasks'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(table, 1, "exec_tasks table should exist");

    let index: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_exec_tasks_dedup'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(index, 1, "idx_exec_tasks_dedup should exist");
}
