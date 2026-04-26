"""
ReportClaw - cninfo 年报抓取 + 第三节（管理层讨论与分析）解析入库脚本

作用
- 从 cninfo（巨潮资讯）按“全市场（深交所 + 上交所）”拉取最近 N 天披露的“年度报告全文”公告（排除摘要/短PDF）。
- 下载 PDF 到本地 data/downloads/（若已存在则不重复下载）。
- 解析 PDF 的“第三节 管理层讨论与分析”正文：
    - 提取“管理层综述”（第三节开头到“核心竞争力分析/主营业务分析/公司治理/重要事项/未来展望/风险提示/经营情况讨论与分析”等大章之前）
    - 可选提取“未来展望”（如“十一、公司未来发展的展望”）
- 将结果写入 MySQL：
    - annual_reports：股票代码/名称/报告年度/披露日期/pdf路径
    - annual_report_mda：management_overview（存入 main_business_section）、future_section、full_mda

配置
- conf/config.ini 必须包含：
    [mysql]
    host=...
    port=3306
    user=...
    pass=...
    db=stock

- 可选：
    [crawler]
    days_back = 30   # 默认 30，表示仅拉取最近 N 天披露的年报公告
    use_last_crawl = true           # 默认 true：使用 data/state/last_sent.json 的 last_crawl_end_iso 记录上次抓取截止时间，避免重复爬取
    last_crawl_state_file = data/state/last_sent.json   # 可选：自定义状态文件路径（与 daily_report 共用一个 json）

运行
- 建议使用项目 venv：
    ./venv/bin/python src/reportclaw/main.py

输出目录
- PDF 下载目录：data/downloads/
-（日报 PDF 生成与发送由 daily_report.py 负责，输出到 data/report/）

注意
- 本脚本使用 (stock_code, report_year) 做“去重”。默认会对已入库记录重新解析并覆盖（便于你迭代抽取逻辑）；如需旧行为，在 conf/config.ini 的 [crawler] 中设置 reparse_existing=false。
- 若需要支持“更正/修订版覆盖更新”，应改为按 announcementId/adjunctUrl 做版本化或 update 逻辑。

删除某条条年报
DELETE m
FROM annual_report_mda m
JOIN annual_reports r ON r.id=m.report_id
WHERE r.stock_code='000975' AND r.report_year=2025;

DELETE FROM annual_reports
WHERE stock_code='000975' AND report_year=2025;

清库重新来
DELETE FROM annual_report_mda;
DELETE FROM annual_reports;

文件结构速览
- 参数/路径：parse_args / 路径常量
- 状态与运行配置：reportclaw.state / reportclaw.runtime_config
- 外部服务适配：JoinQuantIndustryClient
- 数据库访问：MySQLClient
- PDF 解析核心：AnnualReportParser
- 编排入口：main()

推荐拆分方向
- cli.py：命令行参数、路径常量、运行模式选择
- state.py：增量状态读写
- industry.py：聚宽行业适配
- repository.py：MySQL 读写
- parser.py：AnnualReportParser 及其文本规则
- pipeline.py：候选公告收集、下载、并行解析、落库编排

当前 main.py 更像“单文件应用”，这一版先通过文档和分段注释把模块边界标清，
方便后续按职责拆文件，而不是在拆分时同时改业务逻辑。
"""
import argparse
import os
import re
import sys
import time
import requests
import pdfplumber
from pdfminer.high_level import extract_text as pdfminer_extract_text
import configparser
import mysql.connector
from datetime import datetime, timedelta
import traceback
import json
import concurrent.futures
import subprocess
from typing import Any

from pathlib import Path
import signal
from reportclaw.chairman_letter import extract_chairman_letter as extract_chairman_letter_impl
from reportclaw.chairman_letter import normalize_for_letter as normalize_for_letter_impl
from reportclaw.mda_support import (
    build_fallback_mda as build_fallback_mda_impl,
    extract_alt_sections as extract_alt_sections_impl,
    extract_between_markers as extract_between_markers_impl,
    truncate_text as truncate_text_impl,
)
from reportclaw.parse_pipeline import build_parse_jobs, run_parse_jobs
from reportclaw.parser_sections import (
    extract_section as extract_section_impl,
    extract_section_by_keywords as extract_section_by_keywords_impl,
    extract_section_by_ordinal as extract_section_by_ordinal_impl,
    next_ordinal_candidates as next_ordinal_candidates_impl,
    slice_to_next_bracket_heading as slice_to_next_bracket_heading_impl,
    slice_to_next_heading_with_title_keywords as slice_to_next_heading_with_title_keywords_impl,
    slice_to_next_major_heading as slice_to_next_major_heading_impl,
    slice_to_next_ordinal as slice_to_next_ordinal_impl,
)
from reportclaw.pipeline import fetch_candidate_announcements, dedupe_candidates, download_missing_pdfs
from reportclaw.repository import MySQLClient
from reportclaw.runtime_config import load_main_runtime_config
from reportclaw.state import load_last_crawl_ts, save_last_crawl_ts
try:
    from jqdatasdk import auth as jq_auth, get_industry as jq_get_industry
    JQDATA_AVAILABLE = True
except Exception:
    jq_auth = None
    jq_get_industry = None
    JQDATA_AVAILABLE = False

try:
    import tushare as ts
    TUSHARE_AVAILABLE = True
except Exception:
    ts = None
    TUSHARE_AVAILABLE = False

# main.py 位于 src/reportclaw/ 下，所以项目根目录是再向上两级
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONF_DIR = PROJECT_ROOT / "conf"
DATA_DIR = PROJECT_ROOT / "data"
CACHE_DIR = DATA_DIR / "cache"
DOWNLOADS_DIR = DATA_DIR / "downloads"
DAILY_DIR = DATA_DIR / "report"
STATE_DIR = DATA_DIR / "state"

def parse_args():
    """
    命令行参数说明

    用法一：全市场增量抓取（默认行为）
        PYTHONPATH=src ./venv/bin/python -m reportclaw.mainM

        说明：
        - 不加 --single-company 时，按原有逻辑抓取全市场最近 N 天年报。
        - N 由 config.ini 里的 [crawler].days_back 控制。
        - 是否使用上次抓取时间做增量窗口，由 [crawler].use_last_crawl 控制。

    用法二：单公司历史年报模式
        PYTHONPATH=src ./venv/bin/python -m reportclaw.mainM --single-company --stock-code 000559 --years 10

        说明：
        - 加了 --single-company 后，只抓取单个公司的历史年报。
        - --stock-code 可传 6 位股票代码；如果不传，默认使用 600519。
        - --years 表示抓取最近多少个报告年度，默认 10。
        - 单公司模式下，会强制关闭 use_last_crawl，并强制开启 reparse_existing。
        - 单公司模式下，解析入库完成后，会自动调用 report_scoring.py 对本次写入的 report_id 逐个打分。

    示例：
        1) 全市场增量：
           PYTHONPATH=src ./venv/bin/python -m reportclaw.mainM

        2) 单公司（指定股票）：
           PYTHONPATH=src ./venv/bin/python -m reportclaw.mainM --single-company --stock-code 000559 --years 10

        3) 单公司（默认 600519）：
           PYTHONPATH=src ./venv/bin/python -m reportclaw.mainM --single-company --years 10
    """
    p = argparse.ArgumentParser(description="抓取A股年报并解析MD&A入库")
    p.add_argument("--single-company", action="store_true", help="启用单公司历史年报模式")
    p.add_argument("--stock-code", default=None, help="指定单个A股公司代码（6位），单公司模式下默认 600519")
    p.add_argument("--years", type=int, default=10, help="单公司模式下抓取最近多少个报告年度，默认10")
    return p.parse_args()


def _score_reports_by_ids(report_ids: list[int]) -> None:
    ids = []
    seen = set()
    for rid in report_ids:
        try:
            rid_i = int(rid)
        except Exception:
            continue
        if rid_i in seen:
            continue
        seen.add(rid_i)
        ids.append(rid_i)

    if not ids:
        return

    scoring_script = PROJECT_ROOT / "src" / "reportclaw" / "report_scoring.py"
    if not scoring_script.exists():
        raise RuntimeError(f"未找到打分脚本: {scoring_script}")

    env = os.environ.copy()
    src_dir = str(PROJECT_ROOT / "src")
    old_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = src_dir if not old_pythonpath else src_dir + os.pathsep + old_pythonpath

    for rid in ids:
        cmd = [sys.executable, str(scoring_script), "--report-id", str(rid)]
        print(f"[score] start report_id={rid}")
        subprocess.run(cmd, cwd=str(PROJECT_ROOT), env=env, check=True)
        print(f"[score] done report_id={rid}")

# ===============================
# 增量抓取状态（避免重复爬取）
# ===============================

LAST_CRAWL_STATE_FILE = STATE_DIR / "last_sent.json"

def _parse_pdf_task(args: tuple[str, int | None, str | None, int | None]) -> dict[str, Any]:
    """Parse one PDF in a worker process.

    Args:
      args: (file_path, page_count, stock_code, report_year)

    Returns:
      dict with keys:
        - ok(bool)
        - mda(dict|None)
        - reason(str)
        - page_count(int|None)
        - elapsed_sec(float)
        - tag(str)
        - pdf_name(str)
    """
    file_path, page_count, stock_code, report_year = args
    pdf_name = os.path.basename(file_path)
    tag = f"{stock_code or 'UNKNOWN'}-{report_year or 'UNKNOWN'} | {pdf_name}"

    parser = AnnualReportParser()
    # Attach a log tag so parser-side logs can be attributed to a specific PDF
    try:
        setattr(parser, "log_tag", tag)
    except Exception:
        pass

    t0 = time.perf_counter()
    try:
        mda = parser.extract_mda(file_path)
        elapsed = time.perf_counter() - t0
        if mda:
            return {
                "ok": True,
                "mda": mda,
                "reason": "",
                "page_count": page_count,
                "elapsed_sec": elapsed,
                "tag": tag,
                "pdf_name": pdf_name,
            }
        fb = parser.build_fallback_mda(file_path, reason="mda_not_found", page_count=int(page_count or 0))
        return {
            "ok": False,
            "mda": fb,
            "reason": "mda_not_found",
            "page_count": page_count,
            "elapsed_sec": elapsed,
            "tag": tag,
            "pdf_name": pdf_name,
        }
    except Exception as e:
        elapsed = time.perf_counter() - t0
        msg = str(e) or ""
        if "IMAGE_HEAVY_SKIP" in msg:
            reason = "image_heavy_skip"
        else:
            reason = f"exception:{type(e).__name__}"

        fb = parser.build_fallback_mda(file_path, reason=reason, page_count=int(page_count or 0))
        return {
            "ok": False,
            "mda": fb,
            "reason": reason,
            "page_count": page_count,
            "elapsed_sec": elapsed,
            "tag": tag,
            "pdf_name": pdf_name,
        }


def _build_worker_fallback_result(c: dict[str, Any], e: Exception) -> dict[str, Any]:
    """Build a placeholder parse result when a worker crashes before returning a normal payload."""
    parser_local = AnnualReportParser()
    fb = parser_local.build_fallback_mda(
        c["file_path"],
        reason=f"exception:{type(e).__name__}",
        page_count=int(c.get("page_count") or 0),
    )
    return {
        "ok": False,
        "mda": fb,
        "reason": f"exception:{type(e).__name__}",
        "page_count": c.get("page_count"),
    }


class JoinQuantIndustryClient:
    """
    行业信息补充适配层。

    职责很单一：
    - 读取 jqdata 配置并完成认证
    - 以 (stock_code, date) 为键查询申万行业
    - 做最小缓存和权限失败降级

    后续拆分建议：
    - 可整体迁到 `industry.py`
    - 对外只保留 `get_sw_industry()` 一个主接口
    """
    def __init__(self):
        cfg = configparser.ConfigParser()
        cfg.read(CONF_DIR / "config.ini", encoding="utf-8")

        self.enabled = False
        self._cache = {}
        self._logged_disabled_reason = False
        self._permission_cutoff_date = None

        if not JQDATA_AVAILABLE:
            self._log_disabled_once("[jqdata] jqdatasdk 未安装，跳过聚宽行业同步")
            return

        enabled = cfg.getboolean("jqdata", "enabled", fallback=False)
        if not enabled:
            self._log_disabled_once("[jqdata] enabled=false，跳过聚宽行业同步")
            return

        username = cfg.get("jqdata", "username", fallback="").strip()
        password = cfg.get("jqdata", "password", fallback="").strip()
        if not username or not password:
            self._log_disabled_once("[jqdata] 缺少 username/password，跳过聚宽行业同步")
            return

        try:
            jq_auth(username, password)
            self.enabled = True
            print("[jqdata] auth ok")
        except Exception as e:
            self._log_disabled_once(f"[jqdata] auth failed: {e}")

    def _log_disabled_once(self, msg: str) -> None:
        if not self._logged_disabled_reason:
            print(msg)
            self._logged_disabled_reason = True

    @staticmethod
    def normalize_stock_code(stock_code: str) -> str:
        code = str(stock_code).strip()
        if code.startswith(("600", "601", "603", "605", "688", "689", "900")):
            return f"{code}.XSHG"
        return f"{code}.XSHE"

    @staticmethod
    def _pick_value(item, *keys):
        if item is None:
            return None
        if isinstance(item, dict):
            for k in keys:
                if item.get(k) not in (None, ""):
                    return item.get(k)
            return None
        for k in keys:
            if hasattr(item, k):
                v = getattr(item, k)
                if v not in (None, ""):
                    return v
        return None

    @staticmethod
    def _normalize_date_str(date_value: str | None) -> str | None:
        if not date_value:
            return None
        s = str(date_value).strip()
        if not s:
            return None
        return s[:10]

    @staticmethod
    def _extract_permission_end_date(error_text: str) -> str | None:
        if not error_text:
            return None
        m = re.search(r"仅能获取\s*(\d{4}-\d{2}-\d{2})\s*至\s*(\d{4}-\d{2}-\d{2})\s*的数据", error_text)
        if m:
            return m.group(2)
        return None

    def _fetch_raw_industry(self, jq_code: str, lookup_date: str | None):
        return jq_get_industry(jq_code, date=lookup_date)

    def get_sw_industry(self, stock_code: str, date: str | None = None) -> dict:
        requested_date = self._normalize_date_str(date)
        base = {
            "sw_l1_code": None,
            "sw_l1_name": None,
            "sw_l2_code": None,
            "sw_l2_name": None,
            "sw_l3_code": None,
            "sw_l3_name": None,
            "industry_source": "joinquant",
            "industry_lookup_date": requested_date,
        }

        if not self.enabled:
            return dict(base)

        cache_key = (str(stock_code), requested_date)
        if cache_key in self._cache:
            return dict(self._cache[cache_key])

        jq_code = self.normalize_stock_code(stock_code)

        raw = None
        used_date = requested_date
        fallback_used = False
        fallback_from = None
        permission_cutoff = self._normalize_date_str(self._permission_cutoff_date)
        if requested_date and permission_cutoff and permission_cutoff < requested_date:
            used_date = permission_cutoff
            fallback_used = True
            fallback_from = requested_date

        try:
            raw = self._fetch_raw_industry(jq_code, used_date)
        except Exception as e:
            err_text = str(e)
            fallback_date = self._extract_permission_end_date(err_text)
            if fallback_date and requested_date and fallback_date < requested_date:
                self._permission_cutoff_date = fallback_date
                try:
                    raw = self._fetch_raw_industry(jq_code, fallback_date)
                    used_date = fallback_date
                    fallback_used = True
                    fallback_from = requested_date
                    print(f"[jqdata] permission cutoff cached for this run: requested={requested_date} cutoff={fallback_date}")
                except Exception as e2:
                    print(f"[jqdata] get_industry failed: {stock_code} date={requested_date} fallback_date={fallback_date} err={e2}")
                    self._cache[cache_key] = dict(base)
                    return dict(base)
            else:
                print(f"[jqdata] get_industry failed: {stock_code} date={requested_date} err={e}")
                self._cache[cache_key] = dict(base)
                return dict(base)

        payload = raw
        if isinstance(raw, dict) and jq_code in raw:
            payload = raw.get(jq_code) or {}

        if not isinstance(payload, dict):
            self._cache[cache_key] = dict(base)
            return dict(base)

        result = dict(base)
        result["industry_lookup_date"] = used_date
        for level in ("sw_l1", "sw_l2", "sw_l3"):
            item = payload.get(level)
            result[f"{level}_code"] = self._pick_value(item, "industry_code", "code", "index")
            result[f"{level}_name"] = self._pick_value(item, "industry_name", "name")

        if fallback_used:
            print(f"[jqdata] fallback date used: {stock_code} requested={fallback_from} actual={used_date}")

        self._cache[cache_key] = dict(result)
        return dict(result)


class TushareIndustryClient:
    """Tushare industry enrichment based on stock_basic.industry."""

    def __init__(self):
        cfg = configparser.ConfigParser()
        cfg.read(CONF_DIR / "config.ini", encoding="utf-8")

        self.enabled = False
        self._cache = {}
        self._logged_disabled_reason = False
        self._pro = None
        self._stock_basic_map = None
        self._stock_basic_cache_path = CACHE_DIR / "tushare_stock_basic_industry.json"
        self._stock_basic_refresh_attempted = False

        if not TUSHARE_AVAILABLE:
            self._log_disabled_once("[tushare] tushare 未安装，跳过行业同步")
            return

        enabled = cfg.getboolean("tushare", "enabled", fallback=False)
        if not enabled:
            self._log_disabled_once("[tushare] enabled=false，跳过行业同步")
            return

        token = cfg.get("tushare", "token", fallback="").strip()
        if not token:
            self._log_disabled_once("[tushare] 缺少 token，跳过行业同步")
            return

        try:
            self._pro = ts.pro_api(token)
            self.enabled = True
            print("[tushare] auth ok")
        except Exception as e:
            self._log_disabled_once(f"[tushare] auth failed: {e}")

    def _log_disabled_once(self, msg: str) -> None:
        if not self._logged_disabled_reason:
            print(msg)
            self._logged_disabled_reason = True

    def _load_stock_basic_map_from_disk(self) -> dict[str, str] | None:
        path = self._stock_basic_cache_path
        if not path.exists():
            return None
        try:
            obj = json.loads(path.read_text(encoding="utf-8"))
            data = obj.get("data") if isinstance(obj, dict) else None
            if not isinstance(data, dict):
                return None
            out = {}
            for k, v in data.items():
                key = str(k).strip()
                val = str(v).strip()
                if key and val:
                    out[key] = val
            if out:
                updated_at = str(obj.get("updated_at") or "").strip() if isinstance(obj, dict) else ""
                if updated_at:
                    print(f"[tushare] stock_basic map loaded from disk: {len(out)} updated_at={updated_at}")
                else:
                    print(f"[tushare] stock_basic map loaded from disk: {len(out)}")
                return out
        except Exception as e:
            print(f"[tushare] stock_basic disk cache read failed: err={e}")
        return None

    def _save_stock_basic_map_to_disk(self, stock_basic_map: dict[str, str]) -> None:
        if not stock_basic_map:
            return
        path = self._stock_basic_cache_path
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "source": "tushare.stock_basic",
                "count": len(stock_basic_map),
                "data": stock_basic_map,
            }
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"[tushare] stock_basic map saved to disk: {len(stock_basic_map)} path={path}")
        except Exception as e:
            print(f"[tushare] stock_basic disk cache write failed: err={e}")

    def _ensure_stock_basic_map(self) -> dict[str, str]:
        if self._stock_basic_map is not None:
            return self._stock_basic_map

        # Desired strategy:
        # 1) try refreshing from Tushare once per run
        # 2) if refresh fails, fall back to local disk cache
        if not self._stock_basic_refresh_attempted:
            self._stock_basic_refresh_attempted = True
            try:
                df = self._pro.stock_basic(
                    exchange="",
                    list_status="L",
                    fields="ts_code,symbol,name,industry",
                )
                stock_basic_map = {}
                if df is not None and not getattr(df, "empty", True):
                    for _, row in df.iterrows():
                        symbol = str(row.get("symbol") or "").strip()
                        industry_name = str(row.get("industry") or "").strip()
                        if symbol and industry_name:
                            stock_basic_map[symbol] = industry_name
                self._stock_basic_map = stock_basic_map
                print(f"[tushare] stock_basic map loaded: {len(self._stock_basic_map)}")
                self._save_stock_basic_map_to_disk(self._stock_basic_map)
                return self._stock_basic_map
            except Exception as e:
                print(f"[tushare] stock_basic preload failed: err={e}")

        disk_map = self._load_stock_basic_map_from_disk()
        if disk_map:
            self._stock_basic_map = disk_map
            return self._stock_basic_map

        self._stock_basic_map = {}
        return self._stock_basic_map

    @staticmethod
    def normalize_stock_code(stock_code: str) -> str:
        code = str(stock_code).strip()
        if code.startswith(("600", "601", "603", "605", "688", "689", "900")):
            return f"{code}.SH"
        if code.startswith(("8", "4", "9")):
            return f"{code}.BJ"
        return f"{code}.SZ"

    def get_sw_industry(self, stock_code: str, date: str | None = None) -> dict:
        requested_date = str(date or "").strip()[:10] or None
        base = {
            "sw_l1_code": None,
            "sw_l1_name": None,
            "sw_l2_code": None,
            "sw_l2_name": None,
            "sw_l3_code": None,
            "sw_l3_name": None,
            "industry_source": "tushare",
            "industry_lookup_date": requested_date,
        }

        if not self.enabled or self._pro is None:
            return dict(base)

        cache_key = str(stock_code).strip()
        if cache_key in self._cache:
            return dict(self._cache[cache_key])

        stock_basic_map = self._ensure_stock_basic_map()
        industry_name = str((stock_basic_map or {}).get(cache_key) or "").strip()
        if not industry_name:
            self._cache[cache_key] = dict(base)
            return dict(base)

        result = dict(base)
        if industry_name:
            # Tushare stock_basic.industry is not a Shenwan hierarchy, but we map it
            # into sw_l1_name so downstream storage/reporting can keep one display field.
            result["sw_l1_name"] = industry_name
        self._cache[cache_key] = dict(result)
        return dict(result)


class LocalAnnualReportsIndustryClient:
    """Reuse previously stored industry info from annual_reports as a local cache."""

    def __init__(self):
        cfg = configparser.ConfigParser()
        cfg.read(CONF_DIR / "config.ini", encoding="utf-8")

        self.enabled = True
        self._cache = {}
        self._logged_disabled_reason = False
        self._conn = None

        try:
            self._conn = mysql.connector.connect(
                host=cfg.get("mysql", "host"),
                port=cfg.getint("mysql", "port", fallback=3306),
                user=cfg.get("mysql", "user"),
                password=cfg.get("mysql", "pass"),
                database=cfg.get("mysql", "db"),
                charset="utf8mb4",
            )
            print("[industry] local annual_reports cache ready")
        except Exception as e:
            self.enabled = False
            self._log_disabled_once(f"[industry] local annual_reports cache unavailable: {e}")

    def _log_disabled_once(self, msg: str) -> None:
        if not self._logged_disabled_reason:
            print(msg)
            self._logged_disabled_reason = True

    @staticmethod
    def _has_full_sw_levels(row: dict | None, *, use_stock_master_names: bool = False) -> bool:
        if not isinstance(row, dict):
            return False
        if use_stock_master_names:
            return all(
                str(row.get(k) or "").strip()
                for k in ("sw_l1_code", "sw_l1", "sw_l2_code", "sw_l2", "sw_l3_code", "sw_l3")
            )
        return all(
            str(row.get(k) or "").strip()
            for k in ("sw_l1_code", "sw_l1_name", "sw_l2_code", "sw_l2_name", "sw_l3_code", "sw_l3_name")
        )

    def get_sw_industry(self, stock_code: str, date: str | None = None) -> dict:
        requested_date = str(date or "").strip()[:10] or None
        base = {
            "sw_l1_code": None,
            "sw_l1_name": None,
            "sw_l2_code": None,
            "sw_l2_name": None,
            "sw_l3_code": None,
            "sw_l3_name": None,
            "industry_source": "local_annual_reports",
            "industry_lookup_date": requested_date,
        }
        if not self.enabled or self._conn is None:
            return dict(base)

        code = str(stock_code).strip()
        if not code:
            return dict(base)
        if code in self._cache:
            return dict(self._cache[code])

        stock_master_sql = """
            SELECT
              sw_l1_code, sw_l1,
              sw_l2_code, sw_l2,
              sw_l3_code, sw_l3,
              industry_source, industry_lookup_date
            FROM stock_master_cn
            WHERE stock_code=%s
            LIMIT 1
        """
        sql = """
            SELECT
              sw_l1_code, sw_l1_name,
              sw_l2_code, sw_l2_name,
              sw_l3_code, sw_l3_name,
              industry_source, industry_lookup_date
            FROM annual_reports
            WHERE stock_code=%s
              AND COALESCE(sw_l1_code, '') <> ''
              AND COALESCE(sw_l1_name, '') <> ''
              AND COALESCE(sw_l2_code, '') <> ''
              AND COALESCE(sw_l2_name, '') <> ''
              AND COALESCE(sw_l3_code, '') <> ''
              AND COALESCE(sw_l3_name, '') <> ''
            ORDER BY
              CASE WHEN publish_date IS NULL THEN 1 ELSE 0 END,
              publish_date DESC,
              id DESC
            LIMIT 1
        """
        try:
            cur = self._conn.cursor(dictionary=True)
            cur.execute(stock_master_sql, (code,))
            row = cur.fetchone()
            if self._has_full_sw_levels(row, use_stock_master_names=True):
                result = dict(base)
                result["sw_l1_code"] = row.get("sw_l1_code")
                result["sw_l1_name"] = row.get("sw_l1")
                result["sw_l2_code"] = row.get("sw_l2_code")
                result["sw_l2_name"] = row.get("sw_l2")
                result["sw_l3_code"] = row.get("sw_l3_code")
                result["sw_l3_name"] = row.get("sw_l3")
                result["industry_source"] = str(row.get("industry_source") or "").strip() or "stock_master_cn"
                lookup_date = row.get("industry_lookup_date")
                if lookup_date:
                    result["industry_lookup_date"] = str(lookup_date)[:10]
                self._cache[code] = dict(result)
                cur.close()
                return dict(result)

            cur.execute(sql, (code,))
            row = cur.fetchone()
            cur.close()
        except Exception as e:
            print(f"[industry] local annual_reports lookup failed: stock={code} err={e}")
            self._cache[code] = dict(base)
            return dict(base)

        if not self._has_full_sw_levels(row):
            self._cache[code] = dict(base)
            return dict(base)

        result = dict(base)
        for key in ("sw_l1_code", "sw_l1_name", "sw_l2_code", "sw_l2_name", "sw_l3_code", "sw_l3_name"):
            val = row.get(key)
            if val not in (None, ""):
                result[key] = val
        source = str(row.get("industry_source") or "").strip()
        if source:
            result["industry_source"] = source
        lookup_date = row.get("industry_lookup_date")
        if lookup_date:
            result["industry_lookup_date"] = str(lookup_date)[:10]
        self._cache[code] = dict(result)
        return dict(result)


class CompositeIndustryClient:
    def __init__(self, clients: list[Any]):
        self.clients = [c for c in clients if c is not None]

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
                print(f"[industry] provider failed: {type(client).__name__} stock={stock_code} err={e}")
                continue
            if isinstance(result, dict):
                last = result
                if any(result.get(k) for k in ("sw_l1_name", "sw_l2_name", "sw_l3_name")):
                    return result
        return dict(last)


def build_industry_client() -> Any:
    cfg = configparser.ConfigParser()
    cfg.read(CONF_DIR / "config.ini", encoding="utf-8")
    provider = cfg.get("industry", "provider", fallback="auto").strip().lower()

    local_client = LocalAnnualReportsIndustryClient()
    jq_client = JoinQuantIndustryClient()
    ts_client = TushareIndustryClient()

    if provider == "local":
        print("[industry] provider=local")
        return local_client
    if provider == "jqdata":
        print("[industry] provider=jqdata")
        return jq_client
    if provider == "tushare":
        print("[industry] provider=tushare")
        return ts_client

    print("[industry] provider=auto (local -> jqdata -> tushare)")
    return CompositeIndustryClient([local_client, jq_client, ts_client])

# ===============================
# PDF解析器
# ===============================
class AnnualReportParser:
    """
    年报 PDF 文本解析器。

    这是当前文件里最“值得单独拆包”的部分，内部其实已经包含几层不同职责：
    - 文本预处理：normalize / _preprocess_text_for_extract / 表格压缩
    - PDF 体检：_is_image_heavy_pdf
    - 特定段落抽取：extract_chairman_letter / extract_alt_sections
    - MDA 主流程：extract_mda
    - 标题/序号切片工具：extract_section_by_keywords 等

    后续如果拆文件，建议优先从这里开始，把“纯文本规则函数”和“PDF IO”分开。
    """

    def _compress_tables_keep_head_tail(
        self,
        text: str,
        *,
        min_run: int = 10,
        head_keep: int = 3,
        tail_keep: int = 3,
    ) -> str:
        """Compress long table-like blocks to keep readability and avoid false end-markers.

        Heuristic:
        - Detect consecutive runs of "table-like" short lines (>= min_run lines).
        - Keep the first `head_keep` and last `tail_keep` lines.
        - Replace the middle with an ellipsis + hint.

        This is intentionally conservative: it only triggers on long runs.
        """
        if not text:
            return ""

        lines = text.split("\n")
        out: list[str] = []

        def is_table_like_line(ln: str) -> bool:
            s = (ln or "").strip()
            if not s:
                return False

            # If a line looks like normal prose (Chinese punctuation), treat it as NOT a table.
            if any(p in s for p in ("。", "！", "？")):
                return False

            compact = re.sub(r"\s+", "", s)
            # Very short lines are unlikely to be meaningful paragraphs.
            if len(compact) <= 2:
                return False

            # Common table separators
            if any(ch in s for ch in ("|", "│", "┆", "┊", "—", "-")) and re.search(r"\d", s):
                return True

            # "Short" lines with lots of digits/percent/commas are likely table rows.
            if len(compact) <= 40:
                digit_cnt = sum(c.isdigit() for c in compact)
                if digit_cnt >= 6:
                    return True
                if digit_cnt >= 3 and ("," in compact or "%" in compact or "‰" in compact):
                    return True

            # Dense numeric tokens without sentence punctuation
            if len(compact) <= 50 and re.fullmatch(r"[0-9,\.\-+%‰/（）()\u4e00-\u9fffA-Za-z]{10,}", compact):
                # Still require at least some digits to avoid matching random headings.
                if re.search(r"\d", compact):
                    return True

            return False

        i = 0
        n = len(lines)
        while i < n:
            if not is_table_like_line(lines[i]):
                out.append(lines[i])
                i += 1
                continue

            # Start a run
            j = i
            while j < n and is_table_like_line(lines[j]):
                j += 1
            run_len = j - i

            if run_len >= min_run:
                head = lines[i : i + head_keep]
                tail = lines[max(i, j - tail_keep) : j]
                out.extend(head)
                out.append("…（此处为表格，已省略中间内容；建议查看报告原文PDF）…")
                out.extend(tail)
            else:
                out.extend(lines[i:j])

            i = j

        # Keep line breaks mostly as-is, but avoid huge blank runs
        res = "\n".join(out)
        res = re.sub(r"\n{4,}", "\n\n\n", res)
        return res

    def _preprocess_text_for_extract(self, text: str) -> str:
        """Preprocess extracted text while preserving layout as much as possible.

        - Normalize newlines and page breaks
        - Compress long table blocks
        """
        if not text:
            return ""
        t = text.replace("\r\n", "\n").replace("\r", "\n").replace("\x0c", "\n")
        t = self._compress_tables_keep_head_tail(t)
        return t
    def normalize_for_letter(self, text: str) -> str:
        return normalize_for_letter_impl(
            text,
            preprocess_text=self._preprocess_text_for_extract,
        )
    MAJOR_HEADING_KEYWORDS = [
        "核心竞争力", "核心竞争力分析",
        "主营业务分析", "主营业务",
        "非主营业务分析", "非主营业务",
        "资产及负债状况分析", "资产及负债", "资产负债",
        "投资状况分析", "投资状况",
        "公司治理", "重要事项",
        "公司未来发展的展望", "未来发展的展望",
        "行业情况", "行业状况", "所属行业", "所处行业", "行业概况",
        "从事的主要业务", "主要业务", "主营业务",
        "公司未来发展的展望"
    ]

    def _log(self, msg: str) -> None:
        tag = getattr(self, "log_tag", None)
        if tag:
            print(f"[{tag}] {msg}")
        else:
            print(msg)

    def _is_image_heavy_pdf(
            self,
            pdf_path: str,
            *,
            sample_pages: int = 8,
            min_images: int = 20,
            max_total_text_chars: int = 800,
            min_avg_text_chars_per_page: int = 90,
            max_file_mb: int = 30,
    ) -> tuple[bool, dict[str, Any]]:
        """Heuristic preflight to detect image-heavy PDFs that are very slow to parse by pdfminer.

        A PDF is considered image-heavy only when BOTH:
          - images in sampled pages are high
          - extracted text is very low
        """
        stats: dict[str, Any] = {
            "file_mb": None,
            "sample_pages": sample_pages,
            "images": 0,
            "text_chars": 0,
            "avg_text_chars": 0,
        }

        try:
            sz = os.path.getsize(pdf_path)
            stats["file_mb"] = round(sz / (1024 * 1024), 2)
        except Exception:
            stats["file_mb"] = None

        large_file = isinstance(stats["file_mb"], (int, float)) and stats["file_mb"] >= max_file_mb

        try:
            with pdfplumber.open(pdf_path) as pdf:
                n = min(sample_pages, len(pdf.pages))
                img_cnt = 0
                txt_cnt = 0
                for i in range(n):
                    p = pdf.pages[i]

                    # image count (cheap)
                    try:
                        img_cnt += len(getattr(p, "images", []) or [])
                    except Exception:
                        pass

                    # text probe (cheap)
                    try:
                        s = p.extract_text() or ""
                        txt_cnt += len(re.sub(r"\s+", "", s))
                    except Exception:
                        pass

                stats["images"] = img_cnt
                stats["text_chars"] = txt_cnt
                stats["avg_text_chars"] = int(txt_cnt / max(n, 1))
        except Exception as e:
            stats["open_error"] = f"{type(e).__name__}: {e}"
            return False, stats

        img_heavy = stats["images"] >= min_images
        low_text = (stats["text_chars"] <= max_total_text_chars) or (
                stats["avg_text_chars"] <= min_avg_text_chars_per_page
        )

        # Primary rule: many images + very low text in sampled pages
        if img_heavy and low_text:
            return True, stats

        # Secondary rule: very large file + still image-leaning + lowish text
        if large_file and (stats["images"] >= max(8, min_images // 2)) and (
                stats["avg_text_chars"] <= (min_avg_text_chars_per_page * 2)
        ):
            return True, stats

        return False, stats


    def _extract_between_markers(self, text: str, start_pat: str, end_pats: list[str], *, flags=re.MULTILINE):
        return extract_between_markers_impl(text, start_pat, end_pats, flags=flags)


    def _truncate(self, s: str | None, max_len: int) -> str | None:
        return truncate_text_impl(s, max_len)


    def extract_alt_sections(self, pdf_path: str, *, max_pages: int = 260) -> dict | None:
        return extract_alt_sections_impl(
            pdf_path,
            max_pages=max_pages,
            extract_text_fn=self.extract_text,
            extract_between_markers_fn=self._extract_between_markers,
            truncate_text_fn=self._truncate,
            extract_chairman_letter_fn=self.extract_chairman_letter,
        )

    def extract_chairman_letter(self, pdf_path: str, *, max_pages: int = 25) -> str | None:
        return extract_chairman_letter_impl(
            pdf_path,
            max_pages=max_pages,
            normalize_for_letter_fn=self.normalize_for_letter,
            is_image_heavy_pdf_fn=self._is_image_heavy_pdf,
            log_fn=self._log,
        )

    def extract_text(self, pdf_path, page_numbers=None):
        # 使用 pdfminer 按页提取（支持只读指定页）
        text = pdfminer_extract_text(pdf_path, page_numbers=page_numbers)
        if not text:
            return ""
        return self.normalize(text)

    def build_fallback_mda(self, pdf_path: str, reason: str, page_count: int) -> dict:
        return build_fallback_mda_impl(
            pdf_path,
            reason=reason,
            page_count=page_count,
            extract_text_fn=self.extract_text,
            extract_between_markers_fn=self._extract_between_markers,
            extract_chairman_letter_fn=self.extract_chairman_letter,
        )

    def _slice_to_next_bracket_heading(self, text: str, start_idx: int):
        return slice_to_next_bracket_heading_impl(text, start_idx)

    def normalize(self, text):
        # 统一换行/去除不可见分页符
        text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\x0c", "\n")
        text = self._compress_tables_keep_head_tail(text)

        # 先做一次基本清理
        text = re.sub(r"\n+", "\n", text)

        # 按行过滤页眉/页脚/页码等噪声
        lines = []
        for raw in text.split("\n"):
            line = raw.strip()
            if not line:
                continue

            # 1) 纯页码行（如：11）
            if re.fullmatch(r"\d{1,4}", line):
                continue

            # 1.1) 形如 14/248 的页码
            if re.fullmatch(r"\d{1,4}\s*/\s*\d{1,4}", line):
                continue

            # 1.2) 常见页眉（公司名 + 年度报告）
            if ("年度报告" in line) and ("股份有限公司" in line):
                continue

            # 1.3) “公司代码/公司简称”页眉
            if line.startswith("公司代码：") or line.startswith("公司简称：") or ("公司代码：" in line):
                continue

            # 1.4) 仅公司名一行的页眉（常见：XXX股份有限公司 / XXX有限公司）
            if (line.endswith("股份有限公司") or line.endswith("有限公司")) and len(line) <= 30:
                continue

            # 1.5) 仅“XXXX年年度报告/年度报告全文/年度报告”一行的页眉（常与公司名分成两行）
            if ("年度报告" in line) and len(line) <= 20:
                continue

            # 2) 表格边框/分隔符（如：---+、|、+--- 等）
            if re.fullmatch(r"[-+|]{3,}", line):
                continue

            # 3) 常见年报页眉（包含“年度报告全文”）
            if "年度报告全文" in line:
                continue

            lines.append(line)

        # 最后再做你原先的处理：去掉所有空格，并压缩多余空行
        text = "\n".join(lines)
        text = text.replace(" ", "")
        text = re.sub(r"\n+", "\n", text)
        return text

    def extract_mda(self, pdf_path):

        # Preflight: skip extremely image-heavy PDFs early to avoid multi-minute pdfminer stalls.
        is_heavy, st = self._is_image_heavy_pdf(pdf_path)
        if is_heavy:
            self._log(
                f"[perf] image-heavy PDF detected, skip parse. "
                f"file_mb={st.get('file_mb')} sample_pages={st.get('sample_pages')} "
                f"images={st.get('images')} text_chars={st.get('text_chars')} avg_text_chars={st.get('avg_text_chars')}"
            )
            raise RuntimeError("IMAGE_HEAVY_SKIP")

        # 1) 直接正文扫描定位：不要依赖目录页码（目录/重要提示/释义常导致误判）
        # 目标：找到“管理层讨论与分析/经营情况讨论与分析”的真实正文起点页。
        start_page = None

        # 性能优化（关键）：不要逐页调用 pdfplumber.extract_text（某些复杂 PDF 会极慢）。
        # 改为：一次性用 pdfminer 抽取前 N 页文本，然后按分页符切分成 pages cache。
        _pages: list[str] = []
        _n_pages = 0

        def _load_pages(max_pages: int) -> None:
            nonlocal _pages, _n_pages
            if _pages:
                return

            # PERF GUARD:
            # pdfminer 对“复杂矢量/图文混排/超长表格/扫描图”的 PDF 可能分钟级卡顿。
            # 用分批抽取 + 总耗时预算，避免单个 PDF 拖垮整批任务。
            batch_size = 20
            max_total_seconds = 60.0  # 单个PDF在pages-cache阶段的总预算（你可以调小到30）
            max_batch_seconds = 20.0  # 单个batch预算（超过就认为该PDF极慢）

            raw_pages_all: list[str] = []
            t0 = time.perf_counter()

            for start in range(0, max_pages, batch_size):
                # 总预算
                if (time.perf_counter() - t0) > max_total_seconds:
                    raise RuntimeError("PARSE_TOO_SLOW")

                end = min(max_pages, start + batch_size)
                page_nums = list(range(start, end))

                tb = time.perf_counter()
                try:
                    raw_part = pdfminer_extract_text(pdf_path, page_numbers=page_nums) or ""
                except Exception:
                    raw_part = ""
                batch_elapsed = time.perf_counter() - tb

                # 单batch都很慢：直接熔断
                if batch_elapsed > max_batch_seconds:
                    raise RuntimeError("PARSE_TOO_SLOW")

                if raw_part:
                    raw_pages_all.extend(re.split(r"\x0c|\f", raw_part))

            if not raw_pages_all:
                _pages = []
                _n_pages = 0
                return

            _pages = [self.normalize(p) for p in raw_pages_all]
            _n_pages = len(_pages)

        def _get_page_text(p: int) -> str:
            if p < 0:
                return ""
            if not _pages:
                _load_pages(260)
            if p >= _n_pages:
                return ""
            return _pages[p] or ""

        def _close_pdf() -> None:
            # 兼容旧调用点：现在不需要 close 资源，但保持接口不变。
            return

        # 经验：多数年报前 6 页为“重要提示/目录/释义”等前置信息，默认跳过。
        front_matter_max_pages = 5

        def _is_front_matter_page(t: str, page_index: int | None = None) -> bool:
            if not t:
                return True

            # 1) 前 6 页：无条件视为前置页（只用于 start_page 探测阶段的跳过）
            if page_index is not None and page_index <= front_matter_max_pages:
                return True

            # 2) 6 页之后：只过滤“目录/释义/词汇表/名词解释/备查文件目录”等清单型内容（强信号）
            if re.search(r"(目\s*录|释\s*义|词\s*汇\s*表|名词解释|备查文件目录)", t):
                return True

            # 3) 目录页特征：大量点线 + 页码（强信号）
            dot_lines = len(re.findall(r"[\.·…]{6,}.*\d{1,4}\s*$", t, flags=re.MULTILINE))
            if dot_lines >= 3:
                return True

            # 4) 清单密集：第X章/节/部分密集出现（一般就是目录页）
            toc_items = len(re.findall(r"第\s*(?:[一二三四五六七八九十]{1,3}|\d{1,2})\s*(?:节|章|部分)", t))
            if toc_items >= 8:
                return True

            return False

        def _looks_like_mda_heading(t: str) -> bool:
            """Return True only when the page contains a REAL MDA chapter heading line.

            This must be strict to avoid false positives from front-matter cross references,
            e.g. “重大风险提示…详见第三节‘管理层讨论与分析’…敬请查阅”.
            """
            if not t:
                return False

            compact_page = re.sub(r"\s+", "", t)

            # Strong reject: front-matter risk hints often mention the MDA chapter.
            if ("重大风险提示" in compact_page or "风险提示" in compact_page or "重要提示" in compact_page) and (
                "管理层讨论与分析" in compact_page or "经营情况讨论与分析" in compact_page
            ):
                return False

            # Another strong reject: explicit cross-reference language.
            if ("详见" in compact_page or "敬请查阅" in compact_page or "请查阅" in compact_page or "参阅" in compact_page) and (
                "管理层讨论与分析" in compact_page or "经营情况讨论与分析" in compact_page
            ):
                return False

            # Accept only standalone heading-like short lines.
            heading_line_pats = [
                r"^第三节管理层讨论[与和]分析$",
                r"^第三节经营情况讨论[与和]分析$",
                r"^管理层讨论[与和]分析$",
                r"^经营情况讨论[与和]分析$",
                r"^第(?:[一二三四五六七八九十0-9]{1,3})节(?:管理层讨论[与和]分析|经营情况讨论[与和]分析)$",
                r"^(?:第三部分|第三章)管理层讨论[与和]分析$",
                r"^(?:第三部分|第三章)经营情况讨论[与和]分析$",
            ]

            for ln in t.split("\n"):
                s = (ln or "").strip()
                if not s:
                    continue

                # If the line contains quote marks / book-title marks, it is very likely a reference sentence.
                if any(q in s for q in ("“", "”", "\"", "'", "《", "》")):
                    continue

                s2 = re.sub(r"\s+", "", s)
                if len(s2) > 40:
                    continue

                for pat in heading_line_pats:
                    if re.match(pat, s2):
                        return True

            # Fallback: sometimes the heading line is merged with adjacent text.
            # Accept when the page contains the heading phrase in a heading-like context,
            # but reject cross-reference sentences like “详见…第三节…敬请查阅”.
            m_any = re.search(r"第三节\s*管理层讨论[与和]分析|第三节\s*经营情况讨论[与和]分析", t)
            if m_any:
                # Look at a small window around the hit to detect cross references.
                ctx0 = max(0, m_any.start() - 30)
                ctx1 = min(len(t), m_any.end() + 30)
                ctx = re.sub(r"\s+", "", t[ctx0:ctx1])
                if any(x in ctx for x in ("详见", "敬请查阅", "请查阅", "参阅")):
                    return False
                # Also reject if the same line contains quotes/book-title marks (very likely a reference)
                line_start = t.rfind("\n", 0, m_any.start())
                line_end = t.find("\n", m_any.end())
                line = t[(line_start + 1 if line_start >= 0 else 0):(line_end if line_end >= 0 else len(t))]
                if any(q in line for q in ("“", "”", "\"", "'", "《", "》")):
                    return False
                # If it's short-ish, treat as a heading page.
                if len(re.sub(r"\s+", "", line)) <= 60:
                    return True

            return False

        # 先粗扫：找标题页
        for p in range(front_matter_max_pages + 1, 160):
            page_text = _get_page_text(p)
            if not page_text:
                continue

            # 跳过前置页（尤其是前 6 页）
            if p <= front_matter_max_pages and _is_front_matter_page(page_text):
                continue
            # 任何位置出现明显目录页，也跳过
            if _is_front_matter_page(page_text):
                continue

            if _looks_like_mda_heading(page_text):
                start_page = p
                break

        # 若没命中标题页：再找正文锚点（有些标题页是图片，无法提取文本）
        if start_page is None:
            for p in range(front_matter_max_pages + 1, 180):
                page_text = _get_page_text(p)
                if not page_text:
                    continue
                if _is_front_matter_page(page_text):
                    continue
                if (
                        re.search(
                            r"(?:^|\n)\s*(?:[一二三四五六七八九十]{1,3}|\d{1,2})[、\.．:：]\s*报告期内公司所?(?:从事|经营)的(?:主要)?业务情况",
                            page_text)
                        or ("报告期内公司从事的主要业务" in page_text)
                        or ("报告期内公司从事的业务情况" in page_text)
                        or ("报告期内公司从事的主要业务、经营模式" in page_text)
                        or ("报告期内公司所从事的主要业务" in page_text)
                        or ("报告期内公司从事的业务" in page_text)
                        or re.search(r"(?:^|\n)\s*(?:业务概述|业务情况|主要业务)\b", page_text)
                ):
                    start_page = p
                    break

        if start_page is None:
            # 非标准年报兜底
            alt = self.extract_alt_sections(pdf_path)
            _close_pdf()
            if alt:
                return alt
            self._log("未找到管理层讨论与分析/经营情况讨论与分析起始位置（已跳过重要提示/目录/释义前置页）")
            return None

        # 进一步校准：若命中的是章节标题页，向后找真正正文锚点（最多 12 页）
        anchor_patterns = [
            # NOTE: do NOT calibrate using plain '管理层讨论与分析/经营情况讨论与分析' substring,
            # because many pages contain cross-references like “详见第三节...”.

            # 业务情况（覆盖：一、/二、/三、… 以及 1、/2、/3、…）
            r"(?:^|\n)\s*(?:[一二三四五六七八九十]{1,3}|\d{1,2})[、\.．:：]\s*报告期内公司所?(?:从事|经营)的(?:主要)?业务情况",
            r"报告期内公司所?(?:从事|经营)的(?:主要)?业务情况",
            r"(?:^|\n)\s*(?:[一二三四五六七八九十]{1,3}|\d{1,2})[、\.．:：]\s*报告期内公司所从事的主要业务、经营模式",
            r"报告期内公司所从事的主要业务、经营模式",

            # 一些公司把 MDA 的首段叫“业务概述/业务情况/主要业务”
            r"(?:^|\n)\s*(?:[一二三四五六七八九十]{1,3}|\d{1,2})[、\.．:：]\s*(?:业务概述|业务情况|主要业务)\b",
        ]
        calibrated = None
        for p in range(start_page, start_page + 12):
            t = _get_page_text(p)
            if not t:
                continue
            if _is_front_matter_page(t):
                continue
            if ("分季度主要财务指标" in t) or ("非经常性损益" in t) or ("非经常性损益项目及金额" in t):
                continue
            # Prefer a real heading page; otherwise use business-anchor patterns.
            if _looks_like_mda_heading(t) or any(re.search(pat, t) for pat in anchor_patterns):
                calibrated = p
                break
        if calibrated is not None:
            start_page = calibrated

        self._log(f"MDA正文起始页: {start_page}")

        # 3) 从起始页开始读到“第三节结束/第四章开始”的边界
        # 注意：不同年报模板的“第四章”不一定写成“第四节”，常见形式：
        # - 04 公司治理、环境和社会（大号“04”页）
        # - 公司治理（无“第四节”字样）
        # - 直接出现后续章节：十二、报告期内接待调研… / 十三、市值管理… 等
        mda_text = ""
        for p in range(start_page, start_page + 160):
            page_text = _get_page_text(p)
            if not page_text:
                continue

            # 3.1) 标准写法：第四节/第4节/第X节（X>=4）
            if re.search(r"(?:^|\n)\s*第\s*(?:[四五六七八九十]|[4-9]|1\d)\s*节", page_text) or re.search(r"(?:第\s*四\s*节|第四节|第\s*4\s*节)", page_text):
                break

            # 3.2) 另一类模板：用大号章节号（04/4）+ 公司治理…
            if re.search(r"(?:^|\n)\s*0?4\s*(?:公司治理|公司治理、环境和社会)", page_text):
                break
            if "公司治理、环境和社会" in page_text:
                break

            # 3.3) 兜底：明显已经进入第三节之外的后续章节（避免把第十二/十三等章节带进 MDA）
            # 这些章节在不少年报里属于“公司治理/投资者关系/市值管理”等板块
            if re.search(r"(?:^|\n|\b)\s*(?:十\s*二|12)\s*[、\.．:：]?\s*报告期内接待调研", page_text):
                break
            if re.search(r"(?:^|\n|\b)\s*(?:十\s*三|13)\s*[、\.．:：]?\s*市值管理", page_text):
                break

            mda_text += page_text + "\n"

        if not mda_text:
            _close_pdf()
            return None

        # 强制裁剪到正文开始：优先从“管理层讨论与分析/经营情况讨论与分析”标题开始，
        # 否则用稳定正文锚点（避免夹带前一章尾页）。
        cut_pos = None
        m_mda = re.search(r"管理层讨论[与和]分析", mda_text)
        if m_mda:
            cut_pos = m_mda.start()
        else:
            m_ops = re.search(r"经营情况讨论[与和]分析", mda_text)
            if m_ops:
                cut_pos = m_ops.start()
            else:
                m1 = re.search(r"(?:^|\n)\s*一、报告期内公司从事的主要业务", mda_text)
                if m1:
                    cut_pos = m1.start()
                else:
                    m_any = re.search(r"报告期内公司从事的主要业务", mda_text)
                    if m_any:
                        cut_pos = m_any.start()
                    else:
                        # Some reports use “业务情况” instead of “主要业务”。
                        # Prefer the heading-style line (e.g. “一、报告期内公司从事的业务情况”).
                        m_biz = re.search(
                            r"(?:^|\n)\s*(?:[一二三四五六七八九十]{1,3}|\d{1,2})[、\.．:：]\s*报告期内公司从事的业务情况",
                            mda_text,
                        )
                        if m_biz:
                            cut_pos = m_biz.start()
                        else:
                            m_any2 = re.search(r"报告期内公司从事的业务情况", mda_text)
                            if m_any2:
                                cut_pos = m_any2.start()

        if cut_pos is not None:
            mda_text = mda_text[cut_pos:].strip()

        stop_titles = [
            "报告期内核心竞争力分析",
            "核心竞争力分析",
            "主营业务分析",
            "公司治理",
            "重要事项",
            "公司未来发展的展望",
            "未来发展的展望",
            "风险因素",
            "风险提示",
            "经营情况讨论与分析",
        ]

        # mda_text 已从“第三节 管理层讨论与分析”开始；这里直接切到下一重大标题（如“三、报告期内核心竞争力分析”）之前
        management_overview = self._slice_to_next_heading_with_title_keywords(mda_text, 0, stop_titles)
        if not management_overview:
            management_overview = mda_text.strip() if mda_text else None

        # 二次兜底：有些PDF标题行被拆行/缺标点，导致上面的切分没命中 stop_titles。
        # 这里直接用正则定位“(三、/3.) 报告期内核心竞争力分析/核心竞争力分析”并强制截断，确保不包含该章节。
        m_core = re.search(
            r"(?:^|\n)\s*(?:[一二三四五六七八九十]{1,3}|\d{1,2})[、\.．:：]\s*[^\n]{0,80}核心竞争力分析",
            mda_text
        )
        if m_core and m_core.start() > 0:
            management_overview = mda_text[:m_core.start()].strip()

        # 未来展望/发展规划：不同年报模板标题差异很大
        # 常见：十一、公司未来发展的展望 / 公司关于公司未来发展的讨论与分析 / 未来发展战略 / 发展规划
        future = self.extract_section_by_keywords(
            mda_text,
            keywords=[
                "2026年业务展望",
                "业务展望",
                "公司未来发展的展望",
                "未来发展的展望",
                "未来发展展望",
                "公司关于公司未来发展的讨论与分析",
                "公司关于未来发展的讨论与分析",
                "关于公司未来发展的讨论与分析",
                "未来发展战略",
                "未来发展规划",
                "发展规划",
                "未来规划",
            ],
            fallback_ordinals=None,
            # 未来展望结束边界：只在遇到这些“后续大章”标题时才结束，避免被正文里的“二、三、…”条目误截断
            end_title_keywords=[
                "可能面对的风险",
                "风险",
                "公司治理",
                "重要事项",
                "股份变动",
                "股东情况",
                "董事",
                "监事",
                "高级管理人员",
                "员工情况",
                "融资",
                "利润分配",
                "财务会计报告",
                "财务报告",
                "备查文件",
                "报告期内接待调研",
                "市值管理",
            ],
        )

        # Safety trim: do not leak later non-MDA chapters into future section.
        # 一些年报会在后续出现“十二、报告期内接待调研…”、“十三、市值管理…”等章节，必须截断。
        if future:
            leak_patterns = [
                r"(?:^|\n|\b)\s*(?:十\s*二|12)\s*[、\.．:：]?\s*报告期内接待调研",
                r"(?:^|\n|\b)\s*(?:十\s*三|13)\s*[、\.．:：]?\s*市值管理",
                r"(?:^|\n|\b)\s*(?:十\s*四|14)\s*[、\.．:：]?\s*质量回报双提升",
            ]
            cut_at = None
            for pat in leak_patterns:
                m_leak = re.search(pat, future)
                if m_leak:
                    cut_at = m_leak.start() if cut_at is None else min(cut_at, m_leak.start())
            if cut_at is not None:
                future = future[:cut_at].strip()

        # 校验/兜底
        if management_overview is not None and len(management_overview) < 500:
            management_overview = mda_text.strip() if mda_text else management_overview
        if future is not None and len(future) < 200:
            future = None

        # 非标准模板兜底：若第三节里仍未抓到“未来展望/业务展望”，尝试从全文兜底抽取（如“2026年业务展望”）
        if future is None:
            try:
                alt = self.extract_alt_sections(pdf_path)
                if alt and alt.get("future"):
                    future = alt.get("future")
            except Exception:
                pass
        industry = None
        business = management_overview

        ## 董事长致辞
        chairman_letter = self.extract_chairman_letter(pdf_path, max_pages=25)

        _close_pdf()
        return {
            "industry": industry,
            "business": business,
            "future": future,
            "chairman_letter": chairman_letter,
            "full_mda": mda_text
        }

    def _slice_to_next_heading_with_title_keywords(self, text, start_idx, title_keywords):
        return slice_to_next_heading_with_title_keywords_impl(text, start_idx, title_keywords)

    def _slice_to_next_major_heading(self, text, start_idx):
        return slice_to_next_major_heading_impl(text, start_idx, self.MAJOR_HEADING_KEYWORDS)

    def _next_ordinal_candidates(self, current_ordinal: str):
        return next_ordinal_candidates_impl(current_ordinal)

    def _slice_to_next_ordinal(self, text, start_idx, current_ordinal: str):
        return slice_to_next_ordinal_impl(text, start_idx, current_ordinal, self.MAJOR_HEADING_KEYWORDS)

    def extract_section_by_ordinal(self, text, ordinal_cn, keyword_fallback=None):
        return extract_section_by_ordinal_impl(
            text,
            ordinal_cn,
            keyword_fallback=keyword_fallback,
            major_heading_keywords=self.MAJOR_HEADING_KEYWORDS,
        )

    def extract_section_by_keywords(self, text, keywords, fallback_ordinals=None, end_title_keywords=None):
        return extract_section_by_keywords_impl(
            text,
            keywords,
            fallback_ordinals=fallback_ordinals,
            end_title_keywords=end_title_keywords,
            major_heading_keywords=self.MAJOR_HEADING_KEYWORDS,
        )

    def extract_section(self, text, title):
        return extract_section_impl(text, title)


# ===============================
# 主逻辑
# ===============================
def main():
    """
    主流程（Orchestration）

    默认模式：全市场增量抓取
    1) 读取 crawler 配置（days_back / use_last_crawl / reparse_existing）
    2) 分别拉取深交所(szse)与上交所(sse)公告分页（只保留“年度报告全文”，排除摘要）
    3) 严格按时间窗口过滤（避免 seDate 失效导致拉到历史公告）
    4) 下载 PDF（若已存在则跳过下载），并做页数阈值过滤（<50 页视为非完整年报）
    5) 解析第三节管理层讨论与分析，入库 annual_reports + annual_report_mda

    单公司模式：历史年报抓取 + 自动打分
    - 通过 `--single-company` 启用。
    - 通过 `--stock-code` 指定股票代码；不传时默认使用 600519。
    - 通过 `--years` 指定最近多少个报告年度，默认 10。
    - 单公司模式下会：
      1) 关闭 use_last_crawl，避免历史年报被增量窗口截断；
      2) 强制 reparse_existing=True，允许覆盖更新历史解析结果；
      3) 仅抓取目标公司的最近 N 个报告年度年报；
      4) 解析入库后，自动调用 report_scoring.py 对本次写入的 report_id 打分。

    常用命令：
    - 全市场增量：
      PYTHONPATH=src ./venv/bin/python -m reportclaw.mainM

    - 单公司（指定股票）：
      PYTHONPATH=src ./venv/bin/python -m reportclaw.mainM --single-company --stock-code 000559 --years 10

    - 单公司（默认 600519）：
      PYTHONPATH=src ./venv/bin/python -m reportclaw.mainM --single-company --years 10
    """
    args = parse_args()
    download_dir = str(DOWNLOADS_DIR)
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Phase 0: 初始化运行上下文
    # - 数据库客户端
    # - 解析器
    # - 行业补充客户端
    # ------------------------------------------------------------------
    db = MySQLClient(conf_dir=CONF_DIR)
    parser = AnnualReportParser()
    industry_client = build_industry_client()

    # ------------------------------------------------------------------
    # Phase 1: 读取配置并解析运行模式
    # - 全市场增量
    # - 单公司历史模式
    #
    # 后续拆分建议：
    # - 已提炼到 `reportclaw.runtime_config.load_main_runtime_config()`
    # - 入口只消费 runtime config，不再关心细节来源
    # ------------------------------------------------------------------
    _cfg, runtime = load_main_runtime_config(
        args,
        project_root=PROJECT_ROOT,
        conf_dir=CONF_DIR,
        default_state_file=LAST_CRAWL_STATE_FILE,
    )
    days_back = runtime.days_back
    reparse_existing = runtime.reparse_existing
    use_last_crawl = runtime.use_last_crawl
    last_crawl_state_file = runtime.last_crawl_state_file
    company_mode = runtime.company_mode
    stock_code_filter = runtime.stock_code_filter
    company_years = runtime.company_years
    min_report_year = runtime.min_report_year
    max_workers_download = runtime.max_workers_download
    max_workers_parse = runtime.max_workers_parse
    parse_backend = runtime.parse_backend

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "X-Requested-With": "XMLHttpRequest"
    })

    POST_TIMEOUT = (5, 20)   # (connect, read)
    GET_TIMEOUT = (5, 60)    # pdf download can be slower
    MAX_RETRY = 3

    base_url = "http://www.cninfo.com.cn/new/hisAnnouncement/query"

    # ------------------------------------------------------------------
    # Phase 2: 计算本次抓取窗口
    # - 默认 days_back 窗口
    # - 如启用 use_last_crawl，则与状态文件做 max()
    # - 单公司模式按年份展开，不走增量截断
    # ------------------------------------------------------------------
    # CNINFO 的 seDate/endDate 在“当天新增披露”场景下经常会漏（按日边界/索引延迟）。
    # 经验最稳：把查询窗口的 end 向后延 1 天（明天），确保抓到“今天新增但接口尚未完全可见”的公告。
    # 注意：增量状态仍以 real_end_date（当前时间）持久化，避免把未来时间写入 last_crawl_end_iso。
    real_end_date = datetime.now()
    query_end_date = real_end_date + timedelta(days=1)
    if company_mode:
        start_date = datetime(min_report_year, 1, 1)
    else:
        start_date = real_end_date - timedelta(days=days_back)

    # 若启用增量窗口：start_date 取 max(days_back窗口起点, 上次抓取截止时间)
    if use_last_crawl:
        last_end = load_last_crawl_ts(last_crawl_state_file)
        if last_end is not None:
            # 给 2 分钟安全边际（避免时钟误差/边界重复）
            safe_last_end = last_end - timedelta(minutes=2)
            if safe_last_end > start_date:
                start_date = safe_last_end

    start_ts = start_date.timestamp()
    end_ts = query_end_date.timestamp()

    # ------------------------------------------------------------------
    # Phase 3: 枚举交易所分页接口，收集候选公告元数据
    #
    # 这一段是典型的“crawler 层”逻辑：
    # - 翻页请求 cninfo
    # - 基于标题/时间窗口/股票代码过滤
    # - 组装后续下载所需元数据
    #
    # 后续拆分建议：
    # - extract fetch_candidate_announcements(...)
    # - 返回统一的 candidate dict 列表
    # ------------------------------------------------------------------
    candidates = fetch_candidate_announcements(
        session=session,
        base_url=base_url,
        download_dir=download_dir,
        start_date=start_date,
        query_end_date=query_end_date,
        start_ts=start_ts,
        end_ts=end_ts,
        company_mode=company_mode,
        stock_code_filter=stock_code_filter,
        company_years=company_years,
        min_report_year=min_report_year,
        use_last_crawl=use_last_crawl,
        last_crawl_state_file=last_crawl_state_file,
        post_timeout=POST_TIMEOUT,
        max_retry=MAX_RETRY,
    )

    # ------------------------------------------------------------------
    # Phase 4: 候选去重
    # - 逻辑唯一键：(stock_code, report_year)
    # - 同一年出现多个版本时，优先保留披露时间更晚的记录
    #
    # 这是一个很适合独立抽成纯函数的步骤，拆分成本低，回归也容易。
    # ------------------------------------------------------------------
    candidates = dedupe_candidates(candidates)
    print(f"[perf] candidates(after dedup)={len(candidates)}")

    # ------------------------------------------------------------------
    # Phase 5: 下载缺失 PDF
    # - 下载使用线程池
    # - 这里只负责把文件落到本地，不做解析和入库
    #
    # 后续拆分建议：
    # - download_missing_pdfs(candidates, session, ...)
    # ------------------------------------------------------------------
    download_missing_pdfs(
        candidates,
        session=session,
        get_timeout=GET_TIMEOUT,
        max_retry=MAX_RETRY,
        max_workers_download=max_workers_download,
    )

    # 主线程：页数过滤 + 数据库去重/是否需要重解析判定
    # ------------------------------------------------------------------
    # Phase 6: 构建解析任务
    # - 确认 PDF 已存在
    # - 过滤 B 股和短 PDF
    # - 基于 DB 现状判断是否需要重解析
    #
    # 这段实际上是在做“解析前调度决策”，适合将来拆到 pipeline/planner 层。
    # ------------------------------------------------------------------
    parse_jobs = build_parse_jobs(
        candidates,
        db=db,
        reparse_existing=reparse_existing,
    )

    print(f"[perf] parse_jobs={len(parse_jobs)} (backend={parse_backend}, workers={max_workers_parse})")

    # ------------------------------------------------------------------
    # Phase 7: 并行解析 PDF
    # - worker 内只做纯解析，不共享 DB 连接
    # - 若 worker 异常，主线程构造 fallback placeholder
    #
    # 这里已经天然具备拆成 parser worker 层的条件。
    # ------------------------------------------------------------------
    parse_results = run_parse_jobs(
        parse_jobs,
        parse_backend=parse_backend,
        max_workers_parse=max_workers_parse,
        parse_fn=_parse_pdf_task,
        fallback_on_worker_error=_build_worker_fallback_result,
    )

    # 主线程写库（避免多进程共享 DB 连接）
    # 按披露时间从新到旧写入，便于你查看日志
    parse_results.sort(key=lambda x: (x[0].get("ts") or 0), reverse=True)
    written_report_ids: list[int] = []

    # ------------------------------------------------------------------
    # Phase 8: 落库与日志输出
    # - annual_reports upsert
    # - annual_report_mda upsert
    # - 补充行业信息
    #
    # 未来可以把“数据组装”和“repository 调用”再分开一层，进一步瘦身 main()。
    # ------------------------------------------------------------------
    for c, res in parse_results:
        col = c.get("col")
        stock_code = c["stock_code"]
        year = c["year"]
        title = c["title"]

        elapsed = float(res.get("elapsed_sec") or 0.0)
        pdf_name = os.path.basename(c.get("file_path") or "")
        tag = f"{stock_code}-{year} | {pdf_name}"

        industry_info = industry_client.get_sw_industry(stock_code, c.get("publish_date"))

        report_id = db.upsert_report(
            stock_code,
            c["stock_name"],
            year,
            c["publish_date"],
            c["file_path"],
            industry_info=industry_info,
        )
        db.upsert_stock_master_industry(stock_code, c["stock_name"], industry_info)
        db.insert_mda(report_id, res.get("mda") or {})
        written_report_ids.append(int(report_id))

        if res.get("ok"):
            print(f"完成：{tag} ({col}) elapsed={elapsed:.2f}s")
        else:
            print(f"[{col}] 已入库 placeholder（可重试覆盖）: {tag} reason={res.get('reason')} elapsed={elapsed:.2f}s")

    # 性能摘要：打印最慢的若干个 PDF 解析耗时
    if parse_results:
        # --------------------------------------------------------------
        # Phase 9: 解析性能摘要与单公司模式后处理
        # - 打印最慢 PDF
        # - 单公司模式下触发 report_scoring
        # --------------------------------------------------------------
        perf_rows = []
        for c, res in parse_results:
            try:
                elapsed = float(res.get("elapsed_sec") or 0.0)
            except Exception:
                elapsed = 0.0
            pdf_name = os.path.basename(c.get("file_path") or "")
            perf_rows.append((elapsed, c.get("stock_code"), c.get("year"), pdf_name, c.get("col")))

        perf_rows.sort(key=lambda x: x[0], reverse=True)
        topn = perf_rows[:10]
        print("[perf] slowest parses (top 10):")
        for elapsed, sc, yr, pdfn, col in topn:
            print(f"[perf] {elapsed:.2f}s | {sc}-{yr} | {pdfn} | {col}")

        # Also highlight any extremely slow parses
        very_slow = [r for r in perf_rows if r[0] >= 120]
        if very_slow:
            print(f"[perf] WARNING: {len(very_slow)} PDFs took >=120s to parse")

        if company_mode:
            try:
                _score_reports_by_ids(written_report_ids)
            except Exception as e:
                print(f"[score] 公司历史年报打分失败: {e}")
                raise

    # ------------------------------------------------------------------
    # Phase 10: 持久化增量状态
    # - 只在 use_last_crawl 模式下推进状态
    # - 始终写 real_end_date，不写 query_end_date
    #
    # 这一段可以在拆分后收敛到 state.py / pipeline.py 的收尾逻辑。
    # ------------------------------------------------------------------
    # 写入本次抓取的截止时间（用于下次增量）；解析已并行化但截止时间仍以本次运行结束时刻为准
    if use_last_crawl:
        # 写入真实抓取截止时间（当前时间），不要写入 query_end_date（明天）以免影响下次增量窗口。
        save_last_crawl_ts(last_crawl_state_file, real_end_date)
    print("增量更新完成")


if __name__ == "__main__":
    main()
