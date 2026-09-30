#!/usr/bin/env bash
# 本地部署验证：隔离 daemon 真实驱动 exec 任务生命周期
# 前置：target/debug/yomi 已构建。自含三件套（防串生产）。
set -uo pipefail
export PATH="/root/.local/bin:$PATH"
export YOMI_CONFIG=/root/.yomi-e2e/config.toml
export YOMI_DATA_DIR=/root/.yomi-e2e
export YOMI_SOCKET=unix:///tmp/yomi-e2e.sock
unset YOMI_EXTRA_SOCKET
YOMI=/root/.yomi/workspace/yomi/target/debug/yomi
DB=/root/.yomi-e2e/yomi.db
PASS=0; FAIL=0
ok()  { PASS=$((PASS+1)); echo "PASS  $1"; }
bad() { FAIL=$((FAIL+1)); echo "FAIL  $1 — $2"; }

echo "═══ 1. task_create 创建任务（工具真实调用）"
out=$("$YOMI" run --yolo --timeout 120 "调用 task_create 工具，参数：goal='修复登录页的越权漏洞并补一条回归测试'，provider='kimi'，dedup_key='deploy-test-1'。然后把工具返回的 JSON 原样输出，不要加任何其他文字。" 2>/dev/null | tail -5)
echo "$out" | head -3
task_id=$(echo "$out" | grep -o '"task_id":"task_[A-Za-z0-9]*"' | head -1 | cut -d'"' -f4)
# 兜底：宽松匹配但跳过 "task_id" 键名本身
[ -z "$task_id" ] && task_id=$(echo "$out" | grep -o 'task_[A-Za-z0-9]\{20,\}' | head -1)
[ -n "$task_id" ] && ok "task_create 返回任务身份 ($task_id)" || bad "task_create 返回任务身份" "$out"
row=$(sqlite3 "$DB" "SELECT provider, binding, status, source, dedup_key FROM exec_tasks WHERE id='$task_id'")
[ "$row" = "kimi|uninitialized|active|skill|deploy-test-1" ] && ok "exec_tasks 落库正确：$row" || bad "exec_tasks 落库" "$row"

echo "═══ 2. 同 dedup_key 重试 → 同一任务（不重复建）"
out2=$("$YOMI" run --yolo --timeout 120 "调用 task_create 工具，参数：goal='修复登录页的越权漏洞并补一条回归测试'，provider='kimi'，dedup_key='deploy-test-1'。把工具返回的 JSON 原样输出。" 2>/dev/null | tail -5)
task_id2=$(echo "$out2" | grep -o '"task_id":"task_[A-Za-z0-9]*"' | head -1 | cut -d'"' -f4)
[ -z "$task_id2" ] && task_id2=$(echo "$out2" | grep -o 'task_[A-Za-z0-9]\{20,\}' | head -1)
[ "$task_id2" = "$task_id" ] && ok "同 dedup 收敛同一任务" || bad "同 dedup 收敛" "$task_id2 vs $task_id"
cnt=$(sqlite3 "$DB" "SELECT COUNT(*) FROM exec_tasks")
[ "$cnt" = "1" ] && ok "仍只有 1 个任务" || bad "任务数" "$cnt"

echo "═══ 3. task_status 只读查询"
out3=$("$YOMI" run --yolo --timeout 120 "调用 task_status 工具，参数 task_id='$task_id'。把返回的 JSON 原样输出。" 2>/dev/null | tail -5)
echo "$out3" | grep -q "$task_id" && ok "task_status 读到任务" || bad "task_status" "$out3"

echo "═══ 4. task_result 无结果时如实报错"
out4=$("$YOMI" run --yolo --timeout 120 "调用 task_result 工具，参数 task_id='$task_id'。把返回原样输出。" 2>/dev/null | tail -5)
echo "$out4" | grep -qiE "无|没有|暂无|not|error|尚未" && ok "task_result 如实反馈无结果" || bad "task_result" "$out4"

echo "═══ 5. 普通讨论不创建任务（R1 模型侧）"
before=$(sqlite3 "$DB" "SELECT COUNT(*) FROM exec_tasks")
"$YOMI" run --yolo --timeout 120 "我们讨论一下：登录页越权漏洞一般有哪些成因？只讨论，不要创建任何执行任务。" >/dev/null 2>&1
after=$(sqlite3 "$DB" "SELECT COUNT(*) FROM exec_tasks")
[ "$before" = "$after" ] && ok "普通讨论未新建任务" || bad "普通讨论新建了任务" "$before→$after"

echo "═══ 6. daemon 日志无 panic/ERROR 级错误（WARN 级工具拒绝属预期行为）"
if grep -E "panic| ERROR" /root/.yomi-e2e/logs/daemon.$(date +%Y-%m-%d).log | grep -v "Send frame error" | grep -q .; then
  bad "daemon 日志有 panic/ERROR" "$(grep -iE 'panic| ERROR' /root/.yomi-e2e/logs/daemon.$(date +%Y-%m-%d).log | head -2)"
else
  ok "daemon 日志无 panic/ERROR"
fi

echo "== $PASS passed, $FAIL failed =="
