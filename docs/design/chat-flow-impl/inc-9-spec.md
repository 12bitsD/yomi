# 增量 9 实施规格：受理/凭据/派发微窗口硬化

目标：消除增量 6 记录的微窗口——分流臂 受理→凭据写入→（重送判定→撤回）序列与并发派发（terminal 重派/resume）之间存在窗口，跨重启重送项理论上可在窗口内被派发（N12 违反，概率极低但契约关键）。

## 现状（inc-6 实现，handlers.rs 分流臂）

1. `inbox.accept`（进程内去重）
2. Accepted → `record_acceptance` → false（上一进程受理过）→ `remove_queued` 撤回 → 回复「未恢复」
3. 步骤 1–2 之间，重送项在 inbox 中可见；terminal 重派/resume 恰在此窗口 `try_dispatch` 即可把它派出去。

## 设计

把「进程内查重 → 凭据 → 入队」收敛为 scheduler 单一方法，与 try_dispatch 的取队段共用同一把 per-task 异步锁：

- `ExecScheduler` 加 `accept_locks: DashMap<ExecTaskId, Arc<tokio::Mutex<()>>>` + `accept_lock(task_id)`（与 lane 表同锁序：表锁先行）。
- 新方法 `accept_input(task: &ExecTask, msg_id, sender, text, image_keys) -> AcceptVerdict`：
  ```rust
  pub enum AcceptVerdict {
      Accepted { seq: u64 },
      Duplicate,                          // 进程内重送：静默
      NotRecovered { started: bool },     // 跨进程重送：撤回 + 调用方回复
  }
  ```
  锁内顺序（全部在同一次持锁中完成）：
  1. inbox 查重 → Duplicate 即返回（不触碰凭据）；
  2. `facts.record_acceptance` → Ok(false) → `NotRecovered{started}`（读 acceptance_for 得 started；**不入队**，无需撤回——顺序调换后窗口不存在）；
  3. Ok(true) → `inbox.accept` → `Accepted{seq}`；
  4. 凭据写入 Err → warn 后仍按 Accepted（inc-6 既有纪律：不阻断受理，如实记录组合风险）。
- `try_dispatch` 的「锁内 C3 判定 + peek」段改为：先取 `accept_lock`（tokio，持锁段内**无 await**——peek/pop 都是同步操作）再取 lane 锁。锁序全局统一：`accept_lock → lane 锁 → lanes 表锁` 的禁止反向；lanes 表锁永远最先（现状已是）。
- handlers.rs 分流臂改为调 `scheduler.accept_input`；`NotRecovered{started}` 两文案保持 inc-6 原文。
- `record_acceptance` 与 `remove_queued` 的调用方同步清理（remove_queued 不再有调用方可删——保留 API 还是删？删，死代码不进仓）。

## 测试

1. 既有重送场景回归（inc-6 测试套件全绿：进程内静默/跨进程「未恢复」两文案/不派发）。
2. 强制交错回归：构造测试——inbox 有 A 等待；持 accept_lock 注入「跨进程重送 B」的同时另一任务并发 try_dispatch；断言 B 永不进入 inbox、永不被派发（无锁版本可复现失败，有锁版本稳定过；用 `tokio::sync::Barrier` 或 yield 控制交错）。
3. accept_input 与 stop_and_pause/resume 并发：Accepted 后立即可被 resume 派发（锁序正确无死锁——`tokio::time::timeout` 兜底断言 5s 内完成）。

## 检查门槛

同前（check 三组合、clippy 零新增、fmt、全量绿除 4 沙箱存量）。重点盯死锁：全量测试 + scheduler 套件各跑两遍。

## 明确不做

lane 锁 tokio 化（更大重构，无必要）、凭据与 inbox 合并存储（D2 进程内语义不变）。
