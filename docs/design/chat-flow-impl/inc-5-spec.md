# P4 增量 5 实施规格：Run 事实、结果保存与只读查询（W2 内核）

目标：N7 关键事实持久化 + 快照；N9 按 Run 保存 Agent 原始完整正文与归属元数据（先保存再公布）；只读进度工具供普通 Chat 查询（不唤醒执行、不创建 Run）。旧事件不污染新 Run。

## 既有接缝

- 调度器事件点：`exec/scheduler.rs` `try_dispatch`（Running 提交处）、`terminal`（终态收口处）。
- adapter 回调：`exec/adapter.rs` `TerminalNotice` mpsc；本增量加结果上报通道。
- 工具注册：`tools/mod.rs with_standard_tools`（照 task_create 的 Weak<Kernel> 模式）。
- 迁移：CURRENT_SCHEMA_VERSION=26 → v27。

## 组件 1：持久化（migration v27 `add_exec_run_facts`）

```sql
CREATE TABLE exec_runs (
    run_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES exec_tasks(id),
    input_seq INTEGER NOT NULL,
    status TEXT NOT NULL,              -- starting/running/stopping/completed/failed/stopped/unknown
    text TEXT NOT NULL,
    image_keys TEXT NOT NULL DEFAULT '[]',
    terminal_kind TEXT,                -- completed/failed/cancelled（终态一次性写入，之后拒改）
    started_at DATETIME NOT NULL,
    ended_at DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_exec_runs_task ON exec_runs(task_id, input_seq);
CREATE TABLE exec_results (
    run_id TEXT PRIMARY KEY,           -- 一 Run 一份权威正文（N9）
    task_id TEXT NOT NULL REFERENCES exec_tasks(id),
    input_seq INTEGER NOT NULL,
    body TEXT NOT NULL,                -- Agent 原始完整正文，不改写
    body_bytes INTEGER NOT NULL,
    meta TEXT NOT NULL DEFAULT '{}',   -- 归属元数据 JSON（provider/native_session_id/来源）
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_exec_results_task ON exec_results(task_id, input_seq);
```

`ExecFactStore` trait + `SqliteExecFactStore`（`exec/facts.rs`，镜像既有 store 模式）：
- `run_started(rec: &RunRecord) -> Result<()>`（INSERT；同 run_id 重入 IGNORE——重放不重复建行）
- `run_terminal(run_id, status, kind, ended_at) -> Result<()>`（仅当 terminal_kind IS NULL 才写——终态单向，旧事件不得覆盖）
- `save_result(run_id, task_id, seq, body, meta) -> Result<bool>`（INSERT OR IGNORE；false=已有权威正文——重复上报不覆盖）
- `runs_for(task_id) -> Result<Vec<ExecRunRow>>`、`result_for(run_id)`、`latest_result(task_id)`、`list_tasks_with_activity(channel_name)`（查询工具用）
- 装配：`StorageSet` 加 `exec_fact_store`（照 inc-1 模式），Kernel accessor。

## 组件 2：调度器接事实写入

- `try_dispatch` 提交 Running 后：`facts.run_started(&run)`（失败只 warn——事实写入失败不阻断已确认的运行；但标注在 RunRecord？不，保持简单：warn + 继续，查询面如实反映缺失）。
- `terminal` 状态提交后：`facts.run_terminal(...)`。
- 结果上报：`ExecAdapterSink` 加 `result(native_session_id, body: String)` 通道（与 terminal 同一 mpsc，`TerminalNotice` 枚举化 `Terminal{kind}`/`Result{body}`）。scheduler 收到：按 native id 反查 lane/run（不存在 → warn 忽略——迟到结果不归属任何 Run 时绝不猜最新轮，N9）；`facts.save_result` → 成功发 `ExecEvent::ResultPublished{task_id, run_id}`（relay 刷新卡面「第 N 轮结果已保存」）。
- `SimControl` 加 `publish_result(native_id, body)`（测试注入）。

## 组件 3：卡面结果行（cards/taskcard.rs 小改）

状态区下方加一行：latest_result 存在时显示 `📄 第 {seq} 轮结果已保存（{body_bytes} 字节）`。完整结果区/附件入口/历史轮次是 P5，本增量只让「已保存」可见（ refresh_task_card 渲染时读 `facts.latest_result`，锁内快照语义不变）。

## 组件 4：只读查询工具 `task_status`（tools/task_status.rs）

- 注册：与 task_create 同条件（exec_task_store 在）。
- 输入：`task_id`（可选）。输出 JSON：
  - 带 id：task 登记信息 + lane 快照（paused/current/queued/blocked_unknown）+ 最近 run 行（status/terminal_kind/起止）+ 结果有无（seq/bytes/时间）。**不含正文**——正文读取走 `task_result`（见下）。
  - 不带 id：当前通道路由下（经 `routing.channel_name`，无路由则全部）活动任务候选列表（id 短码/goal 摘录/状态一行）——范围不明确返回候选，不挑最近猜（N7）。
- 工具 `task_result`：输入 `task_id` + `seq`（可选，缺省=最新已保存轮）。输出该轮权威正文（超限按工具输出截断约定处理，注明完整导出在 P5）。两工具都**只读**：不调 scheduler 任何写方法、不联系 adapter、不创建 Run——模块 doc 与测试双重锁定。
- desc 写清：用于回答「任务 X 进展如何」；读到的是事实快照，解释时区分事实与推测；不得用来发起/恢复/停止任务。

## 测试

1. 事实流：accept→dispatch（sim）→ run_started 行在；SimControl.complete → terminal 行终态单向（重复 terminal 不改 kind）；publish_result → 正文保存、重复上报不覆盖、ResultPublished 事件触发卡刷新显示结果行。
2. 隔离：run A 的迟到 result（native 反查时 A 已终态）仍归 A 不污染 current B；不同 run 各自正文独立。
3. task_status：带/不带 id 两形态；无 id 返回候选；数值与 store/snapshot 一致。
4. 只读锁定：查询全程 SimControl/adapter 零调用、scheduler 状态不变（前后 snapshot 相等）。
5. task_result：按 seq 取正确轮；缺省取最新；未知轮明确报错。

## 检查门槛

同前（check 三组合、clippy 零新增、fmt、全量绿除 4 沙箱存量）。

## 明确不做

Markdown 附件上传通道（P5/N9 首版交付点）、历史轮次卡面入口与引用回读（P5）、Chat 摘要生成（N7 候选 B 未选）、wire/RPC 查询面、L1 换代。
