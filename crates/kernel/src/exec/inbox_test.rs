use super::*;

fn task() -> ExecTaskId {
    ExecTaskId::new()
}

#[test]
fn accept_orders_and_dedups_by_msg_id() {
    let inbox = ExecInbox::new();
    let id = task();

    let first = inbox.accept(&id, "m1", "ou_a", "第一条", vec![]);
    assert_eq!(first, AcceptOutcome::Accepted { seq: 1 });
    let second = inbox.accept(&id, "m2", "ou_b", "第二条", vec!["img_1".into()]);
    assert_eq!(second, AcceptOutcome::Accepted { seq: 2 });
    assert_eq!(inbox.len(&id), 2);

    // 同 msg_id 重送 → Duplicate，序号与条数都不动（C1：重送查询
    // 当前状态，不扩大副作用）。
    let dup = inbox.accept(&id, "m1", "ou_a", "第一条", vec![]);
    assert_eq!(dup, AcceptOutcome::Duplicate);
    assert_eq!(inbox.len(&id), 2);

    // 重复之后新消息继续定序（不留空洞）。
    let third = inbox.accept(&id, "m3", "ou_a", "第三条", vec![]);
    assert_eq!(third, AcceptOutcome::Accepted { seq: 3 });
    assert_eq!(inbox.len(&id), 3);
}

#[test]
fn inbox_is_scoped_per_task() {
    let inbox = ExecInbox::new();
    let a = task();
    let b = task();

    inbox.accept(&a, "m1", "ou_a", "给 a", vec![]);
    assert_eq!(inbox.len(&a), 1);
    assert!(inbox.is_empty(&b));

    // 同一 msg_id 落在不同任务互不冲突。
    assert_eq!(
        inbox.accept(&b, "m1", "ou_a", "给 b", vec![]),
        AcceptOutcome::Accepted { seq: 1 }
    );
    assert_eq!(inbox.len(&b), 1);
}

#[tokio::test]
async fn concurrent_resend_accepts_once() {
    let inbox = std::sync::Arc::new(ExecInbox::new());
    let id = task();

    // 并发重送同一 msg_id：entry 互斥保证恰好受理一次。
    let mut handles = Vec::new();
    for _ in 0..8 {
        let inbox = std::sync::Arc::clone(&inbox);
        let id = id.clone();
        handles.push(tokio::spawn(async move {
            inbox.accept(&id, "m1", "ou_a", "同一条", vec![])
        }));
    }
    let mut accepted = 0;
    for h in handles {
        if let AcceptOutcome::Accepted { seq } = h.await.unwrap() {
            accepted += 1;
            assert_eq!(seq, 1);
        }
    }
    assert_eq!(accepted, 1);
    assert_eq!(inbox.len(&id), 1);
}

#[test]
fn pop_keeps_dedup_memory_and_seq_watermark() {
    let inbox = ExecInbox::new();
    let id = task();

    // 增量 3 弹出消费后：去重记忆与序号水位不清除（C1 受理是一次
    // 性事实；序号进程内单调）。
    assert_eq!(
        inbox.accept(&id, "m1", "ou_a", "第一条", vec![]),
        AcceptOutcome::Accepted { seq: 1 }
    );
    assert_eq!(inbox.pop_front(&id).map(|i| i.seq), Some(1));
    assert_eq!(inbox.len(&id), 0);

    // 队空后新输入序号接续（不重置回 1）。
    assert_eq!(
        inbox.accept(&id, "m2", "ou_a", "第二条", vec![]),
        AcceptOutcome::Accepted { seq: 2 }
    );
    // 已弹出消息的重送仍是 Duplicate。
    assert_eq!(
        inbox.accept(&id, "m1", "ou_a", "第一条", vec![]),
        AcceptOutcome::Duplicate
    );
    assert_eq!(inbox.len(&id), 1);

    // peek 不消费；pop 出队队首。
    assert_eq!(inbox.peek_front(&id).map(|i| i.seq), Some(2));
    assert_eq!(inbox.len(&id), 1);
    assert_eq!(inbox.pop_front(&id).map(|i| i.seq), Some(2));
    assert!(inbox.is_empty(&id));
}
