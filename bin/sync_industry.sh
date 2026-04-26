#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_PY="$PROJECT_ROOT/venv/bin/python"

usage() {
  echo "用法: $0 [sync_industry_info 参数...]"
  echo "示例: $0"
  echo "示例: $0 --provider jqdata"
  echo "示例: $0 --provider tushare --limit 50"
  echo "示例: $0 --stock-code 300750 002594"
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  usage
  exit 0
fi

if [ ! -x "$VENV_PY" ]; then
  echo "未找到虚拟环境 Python：$VENV_PY"
  exit 1
fi

cd "$PROJECT_ROOT"
export PYTHONPATH=src

echo "==> 开始同步行业信息"
"$VENV_PY" -m reportclaw.sync_industry_info "$@"
