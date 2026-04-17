from __future__ import annotations

import configparser
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class MainRuntimeConfig:
    days_back: int
    reparse_existing: bool
    use_last_crawl: bool
    last_crawl_state_file: Path
    company_mode: bool
    stock_code_filter: str | None
    company_years: int
    min_report_year: int
    max_workers_download: int
    max_workers_parse: int
    parse_backend: str


def _normalize_stock_code(stock_code: str | None) -> str | None:
    if stock_code is None:
        return None
    s = str(stock_code).strip()
    if not s:
        return None
    digits = re.sub(r"\D+", "", s)
    if not digits:
        return None
    return digits.zfill(6)


def load_main_runtime_config(
    args: Any,
    *,
    project_root: Path,
    conf_dir: Path,
    default_state_file: Path,
) -> tuple[configparser.ConfigParser, MainRuntimeConfig]:
    """Resolve `main.py` runtime configuration from CLI args + config.ini.

    This keeps the entrypoint focused on orchestration while preserving current behavior.
    """
    cfg = configparser.ConfigParser()
    cfg.read(conf_dir / "config.ini", encoding="utf-8")

    days_back = 30
    reparse_existing = True
    use_last_crawl = True
    last_crawl_state_file = default_state_file
    company_mode = bool(getattr(args, "single_company", False))
    stock_code_filter = _normalize_stock_code(getattr(args, "stock_code", None)) if company_mode else None
    if company_mode and not stock_code_filter:
        stock_code_filter = "600519"
    company_years = max(1, int(getattr(args, "years", 10) or 10))
    current_year = datetime.now().year
    min_report_year = current_year - company_years + 1

    try:
        if cfg.has_section("crawler") and cfg.get("crawler", "days_back", fallback=""):
            days_back = int(cfg.get("crawler", "days_back"))
        if cfg.has_section("crawler"):
            reparse_existing = cfg.getboolean("crawler", "reparse_existing", fallback=True)
            use_last_crawl = cfg.getboolean("crawler", "use_last_crawl", fallback=True)
            p = cfg.get("crawler", "last_crawl_state_file", fallback="").strip()
            if p:
                last_crawl_state_file = (project_root / p).resolve()
    except Exception:
        days_back = 30
        reparse_existing = True
        use_last_crawl = True
        last_crawl_state_file = default_state_file
        company_mode = bool(getattr(args, "single_company", False))
        stock_code_filter = _normalize_stock_code(getattr(args, "stock_code", None)) if company_mode else None
        if company_mode and not stock_code_filter:
            stock_code_filter = "600519"
        company_years = max(1, int(getattr(args, "years", 10) or 10))
        current_year = datetime.now().year
        min_report_year = current_year - company_years + 1

    if company_mode:
        use_last_crawl = False
        reparse_existing = True

    max_workers_download = 8
    max_workers_parse = max(1, min((os.cpu_count() or 4), 8))
    parse_backend = "process"
    try:
        if cfg.has_section("perf"):
            max_workers_download = int(cfg.get("perf", "max_workers_download", fallback=str(max_workers_download)))
            max_workers_parse = int(cfg.get("perf", "max_workers_parse", fallback=str(max_workers_parse)))
            parse_backend = cfg.get("perf", "parse_backend", fallback=parse_backend).strip().lower() or parse_backend
    except Exception:
        pass

    runtime = MainRuntimeConfig(
        days_back=days_back,
        reparse_existing=reparse_existing,
        use_last_crawl=use_last_crawl,
        last_crawl_state_file=last_crawl_state_file,
        company_mode=company_mode,
        stock_code_filter=stock_code_filter,
        company_years=company_years,
        min_report_year=min_report_year,
        max_workers_download=max_workers_download,
        max_workers_parse=max_workers_parse,
        parse_backend=parse_backend,
    )
    return cfg, runtime
