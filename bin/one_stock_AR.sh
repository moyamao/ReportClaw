#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "用法: $0 <股票代码[,股票代码...]|股票代码1 股票代码2 ...> [-y|--years 年数]"
  echo "示例: $0 300750 002594 --years 5"
  echo "示例: $0 300750,002594 -y 5"
}

if [ "$#" -lt 1 ]; then
  usage
  exit 1
fi

YEARS=10
RAW_ARGS=("$@")
ARGS=()

while [ "$#" -gt 0 ]; do
  case "$1" in
    -y|--years)
      if [ "$#" -lt 2 ] || [[ ! "$2" =~ ^[0-9]+$ ]]; then
        echo "参数错误: $1 需要跟一个数字年数"
        exit 1
      fi
      YEARS="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      ARGS+=("$1")
      shift
      ;;
  esac
done

# 兼容旧写法: 只有最后一个参数是 1-2 位数字时，才当作 years。
if [ "${#ARGS[@]}" -ge 2 ]; then
  LAST_INDEX=$((${#ARGS[@]} - 1))
  LAST_ARG="${ARGS[$LAST_INDEX]}"
  if [[ "$LAST_ARG" =~ ^[0-9]{1,2}$ ]]; then
    YEARS="$LAST_ARG"
    unset 'ARGS[$LAST_INDEX]'
  fi
fi

STOCK_CODES=()
for arg in "${ARGS[@]}"; do
  IFS=',' read -r -a parts <<< "$arg"
  for part in "${parts[@]}"; do
    code="$(echo "$part" | tr -d '[:space:]')"
    if [ -n "$code" ]; then
      STOCK_CODES+=("$code")
    fi
  done
done

if [ "${#STOCK_CODES[@]}" -eq 0 ]; then
  echo "未解析到有效股票代码"
  usage
  exit 1
fi

cd /Users/mhy/python/ReportClaw
export PYTHONPATH=src

for STOCK_CODE in "${STOCK_CODES[@]}"; do
  echo "==> 生成单公司报告: $STOCK_CODE (years=$YEARS)"
  ./venv/bin/python -m reportclaw.main --single-company --stock-code "$STOCK_CODE" --years "$YEARS"
  ./venv/bin/python -m reportclaw.daily_report --stock-code "$STOCK_CODE" --no-email
done
