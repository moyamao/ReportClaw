#!/usr/bin/env bash
set -euo pipefail

STOCK_CODE="${1:?请传入股票代码}"
YEARS="${2:-10}"

cd /Users/mhy/python/ReportClaw

export PYTHONPATH=src

./venv/bin/python -m reportclaw.main --single-company --stock-code "$STOCK_CODE" --years "$YEARS"
./venv/bin/python -m reportclaw.daily_report --stock-code "$STOCK_CODE" --no-email