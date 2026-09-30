# P6 增量 6 实施规格：重启核对、重送去重、释放次序与开关收尾

目标：N11 启动核对未闭合 Run（不凭旧 running 显示正常）；N12/C1 受理凭据持久化——重启后旧输入重送返回「未恢复、未重新执行」，不显示仍排队、不重复执行；C8/N10 终态保存后短空闲释放 Provider，暂停队列可释放且 yomi 侧队列/暂停不丢；D9 绑定持久化支持重启后用户主动新输入恢复原 Session；开关关闭后旧任务安全收尾（不掉回普通 Chat）。仿真 adapter 全程。

## 组件 1：受理凭据持久化（migration v28 `add_exec_acceptance`）

```sql
CREATE TABLE exec_acceptance (
    channel_name TEXT NOT NULL,
    msg_id TEXT NOT NULL,
    task_id TEXT NOT NULL REFERENCES exec_tasks(id),
    accepted_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started INTEGER NOT NULL DEFAULT 0,    -- 是否已被派发过（N12「是否开始」）
    PRIMARY KEY (channel_name, msg_id)
);
```

- `ExecFactStore` 加：`record_acceptance(channel, msg_id, task_id) -> Result<bool>`（INSERT OR IGNORE，false=已受理过）；`mark_started(channel, msg_id)`；`acceptance_for(channel, msg_id)`。
- handlers.rs 分流臂改造（C1 完整语义）：
  1. inbox.accept 判进程内重复（现状）；
  2. 新受理 → `record_acceptance`；**false（凭据已在但 inbox 无此条）= 上一进程生命周期受理过** → 线程内明确回复「该输入在重启前已受理；等待项未恢复、未重新执行」（N12），**不**重新入队、**不**派发；
  3. 进程内 Duplicate → 静默（现状）。
- dispatch pop 时 `mark_started`。

## 组件 2：启动核对（N11）

- `ExecScheduler::boot_sweep()`（Kernel::new 装配后调用一次）：`exec_runs` 中 status 属于 starting/running/stopping 的行 → `status='interrupted'`（terminal_kind 留 NULL、ended_at 留 NULL——中断是事实状态，不伪造终态）。facts 加 `mark_interrupted_open_runs() -> Result<u64>`（返回行数记 tracing）。
- 卡面渲染加 interrupted 文案：`⚠️ 第 {seq} 轮已中断（进程重启）· 待核对`——不凭旧 running 显示正常（D9/N11）。
- hub `start_all`：relay spawn 后，对每个通道实例 `exec_tasks=true` 的，逐一刷新「本通道有卡的活动任务」卡面一次（重启后卡面如实——受理数归零、中断可见、队列不显示仍可恢复）。facts 加 `active_tasks_with_card(channel_name)`。

## 组件 3：空闲释放（C8/N10）

- `ExecAdapter` 加 `release(native_session_id) -> Result<()>`（释放运行实例，不删历史；sim 记录调用）。
- 配置 `[exec] idle_release_secs`（默认 60）。
- scheduler：terminal（且事实已写）后若无立即可派项 → 启动 per-lane 释放计时；到点且期间无新派发/停止动作 → `adapter.release(native_id)`，lane 标 `released=true`（保留 native_session_id——恢复是 adapter 用原 id load/resume，sim 即同 id start_run）。**暂停且有 B/C 等待同样释放**（yomi 队列/暂停不动，C8）。释放不消费队列、不解暂停、不发事件伪造状态；卡面增加 `运行实例已释放（下次输入恢复原 Session）` 一行（有 current 终态 + released 时）。
- 计时被任何 lane 状态变化打断（新 accept→dispatch、stop、resume）。

## 组件 4：恢复与历史缺失（D9/D6）

- 现状已具：Bound 任务重启后新输入 → dispatch 走 Bound 分支用原 native id（sim=恢复）。补测试固化。
- 历史缺失：SimAdapter 加 `resume_fails: HashSet<String>` 旋钮——Bound id start_run 报错 → 现有 Unknown+blocked_unknown 路径；补断言：binding 不变、不自动重建、卡面显示待核对。mark_broken 不在本增量自动做（恢复失败原因需可区分，P7 前人工/运维核对——如实阻断即可）。

## 组件 5：开关关闭的旧任务收尾（N12/P6）

- 分流臂 gate 语义修正：**先查任务命中，再看开关**。命中任务但 `exec_tasks=false` → 回复「本通道已关闭执行任务功能；任务保留，输入未受理」（查看与安全收尾，**不掉回普通 Chat**——这是与增量 3 行为的语义修正，原「落回 chat 路径」仅适用于未命中任务的消息）。新任务创建（/task、task_create）仍拒绝。
- 同步改增量 3 场景 9 测试：原 `diversion_falls_back_to_chat` 断言收窄为「未命中任务的消息落回 chat」；新增「命中任务+开关关=明确拒收不执行」。

## 测试（exec/ 与 taskflow）

1. 受理凭据：accept 后重启（新 inbox+scheduler 同 sqlite）→ 同 msg 重送 → 明确「未恢复」回复、inbox 仍空、零派发；进程内重送仍静默。
2. boot_sweep：造 running/stopping 行 → sweep 后 interrupted、u64 计数对、终态行不动。
3. D9 主动恢复：Bound 任务 + 新 scheduler → 新输入 accept → dispatch → native id 不变、新 run 行、binding 不动。
4. 空闲释放：terminal 后无派项 → 到点 release 调用一次；暂停+B/C 等待同样 release 且 inbox/paused 不变；释放后新输入 → 同 native id 恢复派发；释放计时被 stop 打断不释放。
5. 历史缺失：Bound id 在 resume_fails → blocked_unknown、binding 不变、无新 native id。
6. 开关收尾：命中任务+flag off → 拒收文本、无 chat session 创建；未命中+flag off → 原 chat 路径（兼容）。

## 检查门槛

同前（check 三组合、clippy 零新增、fmt、全量绿除 4 沙箱存量）。

## 明确不做

L1 换代（P5）、真实 adapter 释放语义核实（P3/C4「固定版本核实持久化边界」）、wire/RPC 恢复面、版本升级/回滚实测（P7）、卡面历史轮次入口（P5）。
