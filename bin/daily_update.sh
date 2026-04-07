#!/bin/bash
set -euo pipefail

# ReportClaw 定时更新脚本
# 作用：
# 1) 先跑 main.py 抓取/解析当天增量数据
# 2) 再跑 daily_report.py；如果有新数据则生成文档并发邮件，没有新数据则自动跳过
# 3) 不改写 conf/config.ini，避免影响你平时调试配置
# 4) 运行 daily_report.py 时临时传入 [email] enabled=true 的覆盖配置
#
# 建议由 launchd / cron 在每天 08:00 和 21:00 调用本脚本。

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_PY="$PROJECT_ROOT/venv/bin/python"
CONFIG_PATH="$PROJECT_ROOT/conf/config.ini"
LOG_DIR="$PROJECT_ROOT/logs"
LOCK_DIR="$PROJECT_ROOT/bin/.runlocks"
mkdir -p "$LOG_DIR" "$LOCK_DIR"

DATE_TAG="$(date '+%Y-%m')"
TIME_TAG="$(date '+%H%M%S')"
LOG_FILE="$LOG_DIR/daily_update_${DATE_TAG}.log"
# 只保留按月汇总日志，避免每天一个文件
find "$LOG_DIR" -type f -name 'daily_update_*.log' ! -name "daily_update_${DATE_TAG}.log" -mtime +7 -delete 2>/dev/null || true
LOCK_FILE="$LOCK_DIR/daily_update.lock"

log() {
  echo "[$(date '+%F %T')] $*" | tee -a "$LOG_FILE"
}

cleanup() {
  rm -f "$LOCK_FILE"
}
trap cleanup EXIT

if [ -e "$LOCK_FILE" ]; then
  log "检测到锁文件，说明可能已有任务在运行：$LOCK_FILE"
  exit 1
fi

touch "$LOCK_FILE"

if [ ! -x "$VENV_PY" ]; then
  log "未找到虚拟环境 Python：$VENV_PY"
  exit 1
fi

if [ ! -f "$CONFIG_PATH" ]; then
  log "未找到配置文件：$CONFIG_PATH"
  exit 1
fi

cd "$PROJECT_ROOT"

log "开始执行定时更新任务"
log "PROJECT_ROOT=$PROJECT_ROOT"
log "LOG_FILE=$LOG_FILE"

# 1) 跑 main.py，抓取/解析当天增量
log "开始运行 main.py"
if ! PYTHONPATH="$PROJECT_ROOT/src" "$VENV_PY" -m reportclaw.main >> "$LOG_FILE" 2>&1; then
  log "main.py 执行失败，任务终止"
  exit 1
fi
log "main.py 执行完成"

# 2) 跑 daily_report.py
#    - 不加 --no-email，因为我们就是要发邮件
#    - 通过命令行临时覆盖 [email] enabled=true，不污染 config.ini
#    - 使用模块方式启动，避免 reportclaw 包导入失败
#    - daily_report.py 自己会判断是否有新增记录；如果没有，会输出提示并直接结束
log "开始运行 daily_report.py（临时覆盖 [email] enabled=true）"
if ! PYTHONPATH="$PROJECT_ROOT/src" "$VENV_PY" -m reportclaw.daily_report --config "$CONFIG_PATH" --email-enabled true >> "$LOG_FILE" 2>&1; then
  log "daily_report.py 执行失败，任务终止"
  exit 1
fi
log "daily_report.py 执行完成"

log "本次定时更新任务结束"