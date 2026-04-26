"""
ReportClaw - 行业信息独立同步脚本

作用
- 从本地数据库的 `stock_master_cn` + `annual_reports` 汇总股票池。
- 使用行业源（jqdata / tushare / auto）拉取并刷新行业信息。
- 只更新行业缓存字段，不做任何财报抓取、下载、解析操作。

写入范围
- stock_master_cn
- annual_reports（按 stock_code 批量回写该股票的所有历史年报）

默认行为
- provider=auto 时，按 jqdata -> tushare 顺序尝试远端行业源。
- 不使用本地行业缓存做“命中即返回”，避免把旧行业信息再次抄回去。

示例
    PYTHONPATH=src ./venv/bin/python -m reportclaw.sync_industry_info
    PYTHONPATH=src ./venv/bin/python -m reportclaw.sync_industry_info --provider tushare --limit 50
    PYTHONPATH=src ./venv/bin/python -m reportclaw.sync_industry_info --stock-code 300750 002594
    PYTHONPATH=src ./venv/bin/python -m reportclaw.sync_industry_info --dry-run
"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from reportclaw.main import (
    CONF_DIR,
    JoinQuantIndustryClient,
    TushareIndustryClient,
)
from reportclaw.repository import MySQLClient


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Sync industry info only, without annual report operations.")
    ap.add_argument("--provider", choices=["auto", "jqdata", "tushare"], default="auto",
                    help="industry provider order; default=auto (jqdata -> tushare)")
    ap.add_argument("--stock-code", nargs="*", default=None,
                    help="only sync specified stock codes, e.g. --stock-code 300750 002594")
    ap.add_argument("--limit", type=int, default=0, help="debug limit N stocks")
    ap.add_argument("--dry-run", action="store_true", help="do not write database")
    return ap.parse_args()


class RemoteIndustryClient:
    """Remote-only industry client chain for refresh jobs."""

    def __init__(self, provider: str):
        provider = str(provider or "auto").strip().lower()
        self.provider = provider
        self.jq_client = JoinQuantIndustryClient()
        self.ts_client = TushareIndustryClient()

        if provider == "jqdata":
            self.clients = [self.jq_client]
        elif provider == "tushare":
            self.clients = [self.ts_client]
        else:
            self.clients = [self.jq_client, self.ts_client]

    def get_sw_industry(self, stock_code: str, date: str | None = None) -> dict:
        last = {
            "sw_l1_code": None,
            "sw_l1_name": None,
            "sw_l2_code": None,
            "sw_l2_name": None,
            "sw_l3_code": None,
            "sw_l3_name": None,
            "industry_source": None,
            "industry_lookup_date": str(date or "").strip()[:10] or None,
        }
        for client in self.clients:
            try:
                result = client.get_sw_industry(stock_code, date)
            except Exception as e:
                print(f"[industry_sync] provider failed: {type(client).__name__} stock={stock_code} err={e}")
                continue
            if isinstance(result, dict):
                last = result
                if any(result.get(k) for k in ("sw_l1_name", "sw_l2_name", "sw_l3_name")):
                    return result
        return dict(last)


def _pick_lookup_date(repo: MySQLClient, stock_code: str) -> str | None:
    info = repo.get_cached_industry_info(stock_code)
    raw = info.get("industry_lookup_date")
    s = str(raw).strip() if raw is not None else ""
    if s:
        return s[:10]
    # Important: pass a concrete date so jqdata permission-cutoff fallback can trigger.
    return date.today().strftime("%Y-%m-%d")


def _filter_universe(rows: list[dict], stock_codes: list[str] | None, limit: int) -> list[dict]:
    out = list(rows or [])
    if stock_codes:
        wanted = {str(x).strip() for x in stock_codes if str(x).strip()}
        out = [r for r in out if str(r.get("stock_code") or "").strip() in wanted]
    if limit and limit > 0:
        out = out[:limit]
    return out


def main() -> None:
    args = parse_args()

    repo = MySQLClient(conf_dir=Path(CONF_DIR))
    try:
        repo.ensure_stock_master_industry_schema()
        universe = repo.list_stock_universe_for_industry_sync()
        universe = _filter_universe(universe, args.stock_code, args.limit)

        if args.provider == "jqdata":
            print("[industry_sync] provider=jqdata")
        elif args.provider == "tushare":
            print("[industry_sync] provider=tushare")
        else:
            print("[industry_sync] provider=auto (jqdata -> tushare)")

        print(f"[industry_sync] stock_universe={len(universe)}")
        if not universe:
            print("[industry_sync] no stocks found in stock_master_cn / annual_reports")
            return

        client = RemoteIndustryClient(args.provider)

        touched = 0
        miss = 0
        miss_rows: list[tuple[str, str]] = []
        stock_master_updates = 0
        annual_report_updates = 0

        for idx, row in enumerate(universe, start=1):
            code = str(row.get("stock_code") or "").strip()
            name = str(row.get("stock_name") or "").strip()
            if not code:
                continue

            lookup_date = _pick_lookup_date(repo, code)
            industry_info = client.get_sw_industry(code, lookup_date)
            if not any(industry_info.get(k) for k in ("sw_l1_name", "sw_l2_name", "sw_l3_name")):
                miss += 1
                miss_rows.append((code, name))
                print(f"[industry_sync] miss {idx}/{len(universe)} stock={code} name={name}")
                continue

            touched += 1
            source = str(industry_info.get("industry_source") or "").strip() or "unknown"
            sw_line = " / ".join(
                str(industry_info.get(k) or "").strip()
                for k in ("sw_l1_name", "sw_l2_name", "sw_l3_name")
                if str(industry_info.get(k) or "").strip()
            )

            if args.dry_run:
                print(f"[industry_sync][dry-run] {idx}/{len(universe)} stock={code} source={source} industry={sw_line}")
                continue

            repo.upsert_stock_master_industry(code, name, industry_info)
            stock_master_updates += 1
            updated_rows = repo.update_annual_reports_industry_by_stock(code, industry_info)
            annual_report_updates += updated_rows
            print(
                f"[industry_sync] {idx}/{len(universe)} stock={code} source={source} "
                f"annual_reports_updated={updated_rows} industry={sw_line}"
            )

        print(
            f"[industry_sync] done touched={touched} miss={miss} "
            f"stock_master_updates={stock_master_updates} annual_report_updates={annual_report_updates}"
        )
        if miss_rows:
            miss_summary = ", ".join(
                f"{code}({name})" if name else code
                for code, name in miss_rows
            )
            print(f"[industry_sync] missed stocks: {miss_summary}")
    finally:
        repo.close()


if __name__ == "__main__":
    main()
