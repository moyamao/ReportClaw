#!/bin/bash
set -euo pipefail

# ReportClaw 定时更新脚本
# 作用：
# 1) 先跑 main.py 抓取/解析当天增量数据
# 2) 再跑 report_scoring.py，默认不传 since-days，由程序自行决定起始时间（优先取状态文件）
# 3) 再跑 daily_report.py；如果有新数据则生成文档并发邮件，没有新数据则自动跳过
# 4) 不改写 conf/config.ini，避免影响你平时调试配置
# 5) 运行 daily_report.py 时临时传入 [email] enabled=true 的覆盖配置
# 6) 每次执行时自动预设下一次系统唤醒时间（07:55 / 20:55），减少长期开机需求
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

schedule_next_wake() {
  # 目标：让机器在下一次任务前 5 分钟唤醒，配合 launchd 的 08:00 / 21:00 任务执行。
  # 规则：
  #   - 当前时间早于 07:55 -> 设为今天 07:55
  #   - 当前时间早于 20:55 -> 设为今天 20:55
  #   - 否则 -> 设为明天 07:55
  # 注意：
  #   - 这里使用 `sudo -n`，要求本机已配置对 pmset 的免密码 sudo；否则只记录日志，不中断主任务。
  #   - 对于已登录用户的“睡眠”场景最有效；若机器已关机且未自动登录，LaunchAgent 不会自行进入用户会话。
  local next_wake
  next_wake="$($VENV_PY - <<'PY'
from datetime import datetime, timedelta

now = datetime.now()
slots = [
    now.replace(hour=7, minute=55, second=0, microsecond=0),
    now.replace(hour=20, minute=55, second=0, microsecond=0),
]
future = [x for x in slots if x > now]
if future:
    target = min(future)
else:
    target = (now + timedelta(days=1)).replace(hour=7, minute=55, second=0, microsecond=0)
print(target.strftime('%m/%d/%y %H:%M:%S'))
PY
)"

  if [ -z "$next_wake" ]; then
    log "未能计算下一次唤醒时间，跳过 pmset 唤醒设置"
    return 0
  fi

  if command -v pmset >/dev/null 2>&1; then
    if sudo -n pmset schedule wakeorpoweron "$next_wake" >> "$LOG_FILE" 2>&1; then
      log "已设置下一次系统唤醒时间：$next_wake"
    else
      log "设置系统唤醒时间失败（可能尚未配置 sudo 免密码执行 pmset）：$next_wake"
    fi
  else
    log "系统未找到 pmset，跳过唤醒设置"
  fi
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

# 2) 跑 report_scoring.py，不显式传 since-days，默认由程序优先读取状态文件中的起始时间
log "开始运行 report_scoring.py（默认模式：优先取状态文件起始时间）"
if ! PYTHONPATH="$PROJECT_ROOT/src" "$VENV_PY" -m reportclaw.report_scoring >> "$LOG_FILE" 2>&1; then
  log "report_scoring.py 执行失败，任务终止"
  exit 1
fi
log "report_scoring.py 执行完成"

# 3) 跑 daily_report.py
#    - 不加 --no-email，因为我们就是要发邮件
#    - 通过命令行临时覆盖 [email] enabled=true，不污染 config.ini
#    - 使用模块方式启动，避免 reportclaw 包导入失败
#    - daily_report.py 自己会判断是否有新增记录；如果没有，会输出提示并直接结束
#    - 更新 Google Sheet 等外网访问走本机代理
export HTTPS_PROXY=http://127.0.0.1:1092
export HTTP_PROXY=http://127.0.0.1:1092
export https_proxy=http://127.0.0.1:1092
export http_proxy=http://127.0.0.1:1092
log "开始运行 daily_report.py（临时覆盖 [email] enabled=true）"
if ! PYTHONPATH="$PROJECT_ROOT/src" "$VENV_PY" -m reportclaw.daily_report --config "$CONFIG_PATH" --email-enabled true >> "$LOG_FILE" 2>&1; then
  log "daily_report.py 执行失败，任务终止"
  exit 1
fi
log "daily_report.py 执行完成"

log "本次定时更新任务结束"
schedule_next_wake