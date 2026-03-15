"""
ReportClaw - Google Sheets 增量同步（Service Account）

目标
- 将 daily_report.py 查询到的“新增入库”年报记录同步到 Google Sheets：每份年报一行。
- 只写“客观字段 + 链接”，不覆盖人工字段（score/tags/notes/status）。

设计要点
- 幂等 upsert：用 key 作为唯一键（默认 stock_code-report_year-publish_date）。
- 已存在 key：仅更新“程序维护列”（不动人工列）。
- 新 key：append 新行。
- 可选：把 PDF 上传到 Google Drive 并写入 drive_url（默认关闭）。

配置（conf/config.ini）
[sheets]
enabled = true
spreadsheet_id = <YOUR_SPREADSHEET_ID>
worksheet = AnnualReports
credentials_json = conf/google_service_account.json
# 可选
drive_upload = false
drive_folder_id = <FOLDER_ID>         # 可选：上传到指定文件夹
key_mode = stock_year_date            # stock_year / stock_year_date
worksheet_mode = single             # single / monthly / weekly
worksheet_prefix = AnnualReports    # monthly 模式下 tab 名：<prefix>_YYYY-MM
week_start = mon                   # mon / sun （仅 weekly 模式：周起始日）
month_source = publish_date         # publish_date / created_at
tab_without_year = false          # true 时：monthly 用 MM，weekly 用 Wxx（去掉 YYYY- 前缀）

[sheets_daily]
# 可选：每日快照（第二个 Google Sheet，每天一个 tab，tab 名：M.D，例如 3.6）
enabled = false
spreadsheet_id =
credentials_json = conf/google_service_account.json   # 可复用同一个 service account
worksheet_prefix =                                   # 留空即可；tab 直接用日期（M.D）
key_mode = stock_year_date
# tab_auto_sort = true
# tab_sort_desc = true

说明
- 需要在 Google Sheet 里把该 Sheet 分享给 Service Account 邮箱（Editor 权限）。
- credentials_json 必须加入 .gitignore，不要提交到 GitHub。
"""

from __future__ import annotations

import configparser
import datetime as dt
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Third-party (install):
#   pip install google-api-python-client google-auth google-auth-httplib2 google-auth-oauthlib
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_httplib2 import AuthorizedHttp

import re
import socket
import httplib2
try:
    # PySocks (pip install pysocks) provides proxy type constants used by httplib2.ProxyInfo
    import socks  # type: ignore
except Exception:  # pragma: no cover
    socks = None



def _build_http_with_proxy(timeout_sec: int = 25) -> httplib2.Http:
    """
    Build an httplib2.Http with an explicit timeout and (optional) proxy.

    Why:
    - google-api-python-client uses httplib2 under the hood.
    - In some environments, it does NOT reliably honor only HTTPS_PROXY/HTTP_PROXY env vars.
    - We parse proxy from env and pass proxy_info explicitly to avoid 'timed out'.
    """
    socket.setdefaulttimeout(timeout_sec)

    # Prefer HTTPS proxy envs; fall back to HTTP proxy envs.
    proxy = (
        os.environ.get("HTTPS_PROXY")
        or os.environ.get("https_proxy")
        or os.environ.get("HTTP_PROXY")
        or os.environ.get("http_proxy")
        or ""
    ).strip()

    if not proxy:
        return httplib2.Http(timeout=timeout_sec)

    # Accept forms like:
    #   http://127.0.0.1:1092
    #   https://127.0.0.1:1092
    #   127.0.0.1:1092
    m = re.match(r"^(?:(?P<scheme>https?|socks5h?|socks4)://)?(?P<host>[^:/]+):(?P<port>\d+)$", proxy)
    if not m:
        # Unknown format; fall back to default Http (but keep timeout)
        return httplib2.Http(timeout=timeout_sec)

    scheme = (m.group("scheme") or "http").lower()
    host = m.group("host")
    port = int(m.group("port"))

    if socks is None:
        raise RuntimeError(
            "代理已配置，但缺少 PySocks 依赖（pip install pysocks）。"
            " 或者你也可以在网络允许的环境下关闭代理。"
        )

    if scheme.startswith("socks5"):
        ptype = socks.PROXY_TYPE_SOCKS5
    elif scheme.startswith("socks4"):
        ptype = socks.PROXY_TYPE_SOCKS4
    else:
        # http/https
        ptype = socks.PROXY_TYPE_HTTP
    proxy_info = httplib2.ProxyInfo(proxy_type=ptype, proxy_host=host, proxy_port=port)
    return httplib2.Http(timeout=timeout_sec, proxy_info=proxy_info)

# -------------------------
# Public API
# -------------------------

DEFAULT_HEADER = [
    # --- program fields ---
    "key",
    "stock_name",
    "publish_date",
    "pdf_name",
    # --- manual fields (do NOT overwrite) ---
    "score",
    "notes",
]


def sync_rows_to_google_sheet(
    cfg: configparser.ConfigParser,
    rows: List[Dict[str, Any]],
    *,
    project_root: Optional[Path] = None,
    run_date: Optional[dt.date] = None,
) -> None:
    """
    将 rows 同步到 Google Sheets（幂等 upsert）。

    rows: daily_report.py 查询得到的记录 dict，至少包含：
      - stock_code, stock_name, report_year, publish_date, file_path
      - created_at（可选）
      - exchange（可选，若无会填空）
    """
    if not rows:
        return
    run_date = run_date or dt.date.today()

    if not cfg.has_section("sheets"):
        print("[sheets] 未配置 [sheets] 段，跳过同步")
        return

    enabled = _cfg_bool(cfg, "sheets", "enabled", default=False)
    if not enabled:
        print("[sheets] enabled=false，跳过同步")
        return

    spreadsheet_id = cfg.get("sheets", "spreadsheet_id", fallback="").strip()
    worksheet = cfg.get("sheets", "worksheet", fallback="AnnualReports").strip() or "AnnualReports"
    worksheet_mode = cfg.get("sheets", "worksheet_mode", fallback="single").strip().lower() or "single"
    worksheet_prefix = cfg.get("sheets", "worksheet_prefix", fallback=worksheet).strip() or worksheet
    month_source = cfg.get("sheets", "month_source", fallback="publish_date").strip().lower() or "publish_date"
    tab_without_year = _cfg_bool(cfg, "sheets", "tab_without_year", default=False)
    tab_auto_sort = _cfg_bool(cfg, "sheets", "tab_auto_sort", default=False)
    tab_sort_desc = _cfg_bool(cfg, "sheets", "tab_sort_desc", default=True)
    cred_path = cfg.get("sheets", "credentials_json", fallback="").strip()

    if not spreadsheet_id:
        raise RuntimeError("[sheets] spreadsheet_id 为空")
    if not cred_path:
        raise RuntimeError("[sheets] credentials_json 为空")

    root = project_root or _guess_project_root()
    cred_file = Path(cred_path)
    if not cred_file.is_absolute():
        cred_file = (root / cred_file).resolve()
    if not cred_file.exists():
        raise RuntimeError(f"[sheets] credentials_json 不存在: {cred_file}")

    drive_upload = _cfg_bool(cfg, "sheets", "drive_upload", default=False)
    drive_folder_id = cfg.get("sheets", "drive_folder_id", fallback="").strip() or None
    key_mode = cfg.get("sheets", "key_mode", fallback="stock_year_date").strip() or "stock_year_date"

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    if drive_upload:
        scopes.append("https://www.googleapis.com/auth/drive.file")

    creds = service_account.Credentials.from_service_account_file(str(cred_file), scopes=scopes)

    http = AuthorizedHttp(creds, http=_build_http_with_proxy(timeout_sec=25))
    sheets = build("sheets", "v4", http=http, cache_discovery=False)

    if worksheet_mode == "monthly":
        buckets: Dict[str, List[Dict[str, Any]]] = {}
        for r in rows:
            month = _row_month(r, source=month_source)
            if tab_without_year and len(month) >= 7:
                month = month[5:7]
            ws = f"{worksheet_prefix}_{month}"
            buckets.setdefault(ws, []).append(r)

        for ws, rs in buckets.items():
            _sync_rows_to_worksheet(
                sheets,
                spreadsheet_id,
                ws,
                rs,
                header=DEFAULT_HEADER,
                key_mode=key_mode,
            )
        print(f"[sheets] 月度同步完成：tabs={len(buckets)}")
        if tab_auto_sort:
            _auto_sort_tabs(
                sheets,
                spreadsheet_id,
                mode="monthly",
                prefix=worksheet_prefix,
                tab_without_year=tab_without_year,
                desc=tab_sort_desc,
            )
    elif worksheet_mode == "weekly":
        week_start = cfg.get("sheets", "week_start", fallback="mon").strip().lower() or "mon"
        buckets: Dict[str, List[Dict[str, Any]]] = {}
        for r in rows:
            wk = _row_week(r, source=month_source, week_start=week_start)
            if tab_without_year and wk.startswith("20") and "-W" in wk:
                wk = wk.split("-", 1)[1]  # keep 'Wxx'
            ws = f"{worksheet_prefix}_{wk}"
            buckets.setdefault(ws, []).append(r)

        for ws, rs in buckets.items():
            _sync_rows_to_worksheet(
                sheets,
                spreadsheet_id,
                ws,
                rs,
                header=DEFAULT_HEADER,
                key_mode=key_mode,
            )
        print(f"[sheets] 周度同步完成：tabs={len(buckets)}")
        if tab_auto_sort:
            _auto_sort_tabs(
                sheets,
                spreadsheet_id,
                mode="weekly",
                prefix=worksheet_prefix,
                tab_without_year=tab_without_year,
                desc=tab_sort_desc,
            )
    else:
        _sync_rows_to_worksheet(
            sheets,
            spreadsheet_id,
            worksheet,
            rows,
            header=DEFAULT_HEADER,
            key_mode=key_mode,
        )
        print("[sheets] 同步完成")

    # Optional: daily snapshot spreadsheet (each day one tab named M.D, e.g. 3.6)
    _maybe_sync_daily_snapshot(
        cfg,
        rows,
        sheets_http=http,
        run_date=run_date,
        project_root=root,
    )


# --- Optional daily snapshot sync ---
def _maybe_sync_daily_snapshot(
    cfg: configparser.ConfigParser,
    rows: List[Dict[str, Any]],
    *,
    sheets_http,
    run_date: dt.date,
    project_root: Path,
) -> None:
    """
    Optional second spreadsheet sync:
    - A separate Google Sheet configured in [sheets_daily]
    - One worksheet(tab) per run date, named M.D (e.g. 3.6)
    - Uses the same row schema as the primary sheet (DEFAULT_HEADER)
    """
    if not cfg.has_section("sheets_daily"):
        return

    enabled = _cfg_bool(cfg, "sheets_daily", "enabled", default=False)
    if not enabled:
        return

    spreadsheet_id = cfg.get("sheets_daily", "spreadsheet_id", fallback="").strip()
    if not spreadsheet_id:
        print("[sheets_daily] spreadsheet_id 为空，跳过每日快照同步")
        return

    # Allow reuse of the same credentials_json, defaulting to [sheets].credentials_json
    cred_path = cfg.get("sheets_daily", "credentials_json", fallback="").strip()
    if not cred_path:
        cred_path = cfg.get("sheets", "credentials_json", fallback="").strip()

    if not cred_path:
        print("[sheets_daily] credentials_json 为空，跳过每日快照同步")
        return

    cred_file = Path(cred_path)
    if not cred_file.is_absolute():
        cred_file = (project_root / cred_file).resolve()
    if not cred_file.exists():
        print(f"[sheets_daily] credentials_json 不存在: {cred_file}，跳过每日快照同步")
        return

    key_mode = cfg.get("sheets_daily", "key_mode", fallback="stock_year_date").strip() or "stock_year_date"
    tab_auto_sort = _cfg_bool(cfg, "sheets_daily", "tab_auto_sort", default=True)
    tab_sort_desc = _cfg_bool(cfg, "sheets_daily", "tab_sort_desc", default=True)

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = service_account.Credentials.from_service_account_file(str(cred_file), scopes=scopes)

    http = AuthorizedHttp(creds, http=sheets_http)
    sheets = build("sheets", "v4", http=http, cache_discovery=False)

    def _to_date(v: Any) -> Optional[dt.date]:
        if isinstance(v, dt.datetime):
            return v.date()
        if isinstance(v, dt.date):
            return v
        if isinstance(v, str):
            s = v.strip()
            if len(s) >= 10:
                # Accept: YYYY-MM-DD..., YYYY/MM/DD..., YYYY.MM.DD...
                head = s[:10].replace("/", "-").replace(".", "-")
                try:
                    return dt.date.fromisoformat(head)
                except Exception:
                    pass
            # Accept compact: YYYYMMDD
            if len(s) >= 8 and s[:8].isdigit():
                try:
                    y = int(s[0:4]); m = int(s[4:6]); d = int(s[6:8])
                    return dt.date(y, m, d)
                except Exception:
                    pass
            return None

    buckets: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        d = _to_date(r.get("publish_date")) or run_date
        tab = f"{d.month}.{d.day}"
        buckets.setdefault(tab, []).append(r)

    if len(buckets) > 1:
        summary = ", ".join([f"{k}:{len(v)}" for k, v in sorted(buckets.items())])
        print(f"[sheets_daily] bucket tabs={len(buckets)} ({summary})")
    else:
        only = next(iter(buckets.keys())) if buckets else ""
        print(f"[sheets_daily] bucket tabs=1 ({only})")

    for tab, rs in buckets.items():
        _sync_rows_to_worksheet(
            sheets,
            spreadsheet_id,
            tab,
            rs,
            header=DEFAULT_HEADER,
            key_mode=key_mode,
        )
        print(f"[sheets_daily] {tab}: 同步完成（rows={len(rs)}）")

    if tab_auto_sort:
        _auto_sort_tabs_daily(sheets, spreadsheet_id, desc=tab_sort_desc)


def _sync_rows_to_worksheet(
    sheets,
    spreadsheet_id: str,
    worksheet: str,
    rows: List[Dict[str, Any]],
    *,
    header: List[str],
    key_mode: str,
) -> None:
    """Sync a batch of rows into a single worksheet (tab) with idempotent upsert."""

    _ensure_worksheet_and_header(sheets, spreadsheet_id, worksheet, header)
    key_to_row = _read_existing_key_map(sheets, spreadsheet_id, worksheet)

    updates: List[Tuple[int, List[Any]]] = []
    appends: List[List[Any]] = []

    for r in rows:
        key = _make_key(r, mode=key_mode)
        pdf_path = str(r.get("file_path", "") or "")
        pdf_name = Path(pdf_path).name if pdf_path else ""

        # publish_date 可能是 date/datetime/str
        pd = r.get("publish_date")
        if isinstance(pd, dt.datetime):
            publish_date = pd.date().isoformat()
        elif isinstance(pd, dt.date):
            publish_date = pd.isoformat()
        else:
            publish_date = str(pd or "")

        row_values = [
            key,
            str(r.get("stock_name", "") or ""),
            publish_date,
            pdf_name,
            "",  # score (manual)
            "",  # notes (manual)
        ]

        if key in key_to_row:
            updates.append((key_to_row[key], row_values))
        else:
            appends.append(row_values)

    if updates:
        _batch_update_rows_A_to_D(sheets, spreadsheet_id, worksheet, updates)
    if appends:
        _append_rows(sheets, spreadsheet_id, worksheet, appends)

    print(f"[sheets] {worksheet}: updates={len(updates)}, appends={len(appends)}")


# -------------------------
# Helpers
# -------------------------

# -------------------------
# Helpers
# -------------------------

def _auto_sort_tabs_daily(
    sheets,
    spreadsheet_id: str,
    *,
    desc: bool = True,
) -> None:
    """
    Auto-sort daily snapshot tabs whose titles are like 'M.D' (e.g. 3.6, 2.28).
    Tabs not matching this pattern are kept after the dated tabs in their original order.
    """
    try:
        meta = sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_entries = meta.get("sheets", []) or []
        if not sheet_entries:
            return

        tabs: List[Dict[str, Any]] = []
        for s in sheet_entries:
            p = s.get("properties", {}) or {}
            tabs.append(
                {
                    "title": str(p.get("title", "")),
                    "sheetId": int(p.get("sheetId")),
                    "index": int(p.get("index", 0)),
                }
            )

        def _key(t: str) -> Optional[Tuple[int, int]]:
            m = re.fullmatch(r"(\d{1,2})\.(\d{1,2})", t.strip())
            if not m:
                return None
            mm = int(m.group(1))
            dd = int(m.group(2))
            if not (1 <= mm <= 12 and 1 <= dd <= 31):
                return None
            return (mm, dd)

        targets: List[Tuple[Tuple[int, int], Dict[str, Any]]] = []
        for t in tabs:
            k = _key(t["title"])
            if k is not None:
                targets.append((k, t))

        if not targets:
            return

        targets.sort(key=lambda x: x[0], reverse=desc)

        target_ids = {t["sheetId"] for _, t in targets}
        rest = [t for t in sorted(tabs, key=lambda x: x["index"]) if t["sheetId"] not in target_ids]

        new_order = [t for _, t in targets] + rest

        reqs = []
        for new_idx, t in enumerate(new_order):
            reqs.append(
                {
                    "updateSheetProperties": {
                        "properties": {"sheetId": t["sheetId"], "index": new_idx},
                        "fields": "index",
                    }
                }
            )

        sheets.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": reqs}).execute()
        print(f"[sheets_daily] 已自动重排 tab（desc={desc}）")
    except Exception as e:
        print(f"[sheets_daily] 自动重排 tab 失败（忽略）：{e}")

def _auto_sort_tabs(
    sheets,
    spreadsheet_id: str,
    *,
    mode: str,
    prefix: str,
    tab_without_year: bool,
    desc: bool = True,
) -> None:
    """
    Auto-sort worksheet tabs for weekly/monthly modes by their time label.

    - weekly: <prefix>_YYYY-Www or <prefix>_Www
    - monthly: <prefix>_YYYY-MM or <prefix>_MM

    Google Sheets displays tabs in their `index` order (creation order by default).
    We reassign indices so that the newest tabs appear first (desc=True).
    """
    try:
        meta = sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_entries = meta.get("sheets", []) or []
        if not sheet_entries:
            return

        tabs: List[Dict[str, Any]] = []
        for s in sheet_entries:
            p = s.get("properties", {}) or {}
            tabs.append(
                {
                    "title": str(p.get("title", "")),
                    "sheetId": int(p.get("sheetId")),
                    "index": int(p.get("index", 0)),
                }
            )

        targets: List[Tuple[Tuple[int, int], Dict[str, Any]]] = []
        for t in tabs:
            k = _tab_sort_key(t["title"], mode=mode, prefix=prefix, tab_without_year=tab_without_year)
            if k is not None:
                targets.append((k, t))

        if not targets:
            return

        targets.sort(key=lambda x: x[0], reverse=desc)

        target_ids = {t["sheetId"] for _, t in targets}
        rest = [t for t in sorted(tabs, key=lambda x: x["index"]) if t["sheetId"] not in target_ids]

        new_order = [t for _, t in targets] + rest

        reqs = []
        for new_idx, t in enumerate(new_order):
            reqs.append(
                {
                    "updateSheetProperties": {
                        "properties": {"sheetId": t["sheetId"], "index": new_idx},
                        "fields": "index",
                    }
                }
            )

        sheets.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": reqs}).execute()
        print(f"[sheets] 已自动重排 tab（mode={mode}, desc={desc}）")
    except Exception as e:
        print(f"[sheets] 自动重排 tab 失败（忽略）：{e}")


def _tab_sort_key(title: str, *, mode: str, prefix: str, tab_without_year: bool) -> Optional[Tuple[int, int]]:
    """
    Return a comparable sort key for tab title.

    weekly:
      - with year:  <prefix>_YYYY-Www   -> (YYYY, ww)
      - no year:    <prefix>_Www        -> (0, ww)  (assumes single-year usage)

    monthly:
      - with year:  <prefix>_YYYY-MM    -> (YYYY, MM)
      - no year:    <prefix>_MM         -> (0, MM)

    If not matching expected naming, return None.
    """
    if not title.startswith(prefix + "_"):
        return None

    suffix = title[len(prefix) + 1 :]

    if mode == "weekly":
        m = re.fullmatch(r"(?:(\d{4})-)?W(\d{2})", suffix)
        if not m:
            return None
        y = int(m.group(1)) if m.group(1) else 0
        w = int(m.group(2))
        return (y, w)

    if mode == "monthly":
        m = re.fullmatch(r"(?:(\d{4})-)?(\d{2})", suffix)
        if not m:
            return None
        y = int(m.group(1)) if m.group(1) else 0
        mm = int(m.group(2))
        return (y, mm)

    return None

def _cfg_bool(cfg: configparser.ConfigParser, section: str, key: str, default: bool) -> bool:
    try:
        if not cfg.has_section(section):
            return default
        v = cfg.get(section, key, fallback=str(default)).strip().lower()
        return v in ("1", "true", "yes", "y", "on")
    except Exception:
        return default


def _guess_project_root() -> Path:
    # sheet_sync.py 通常位于 src/reportclaw/ 下
    return Path(__file__).resolve().parents[2]


def _make_key(r: Dict[str, Any], mode: str = "stock_year_date") -> str:
    code = str(r.get("stock_code", "") or "")
    year = str(r.get("report_year", "") or "")
    pdate = str(r.get("publish_date", "") or "")
    if mode == "stock_year":
        return f"{code}-{year}"
    return f"{code}-{year}-{pdate}"



def _row_month(r: Dict[str, Any], source: str = "publish_date") -> str:
    """Return YYYY-MM for worksheet naming."""
    def _fmt(d: dt.date) -> str:
        return f"{d.year:04d}-{d.month:02d}"

    if source == "created_at":
        v = r.get("created_at")
        # created_at 可能是 datetime/date/str
        if isinstance(v, dt.datetime):
            return _fmt(v.date())
        if isinstance(v, dt.date):
            return _fmt(v)
        if isinstance(v, str) and len(v) >= 7:
            return v[:7]

    # default publish_date (may be date/datetime/str depending on DB driver)
    v = r.get("publish_date")
    if isinstance(v, dt.datetime):
        return _fmt(v.date())
    if isinstance(v, dt.date):
        return _fmt(v)
    if isinstance(v, str) and len(v) >= 7:
        return v[:7]

    return _fmt(dt.date.today())


# Weekly worksheet helper
def _row_week(r: Dict[str, Any], source: str = "publish_date", week_start: str = "mon") -> str:
    """
    Return week label for worksheet naming.

    - If week_start == 'mon': ISO week (Mon-Sun), label: YYYY-Www (ISO year + ISO week)
    - If week_start == 'sun': US-style week (Sun-Sat), label: YYYY-Www based on the year of the week's Thursday-equivalent
      (implemented by shifting date +1 day then using ISO week to approximate Sun-start weeks).
    """
    def _to_date(v: Any) -> Optional[dt.date]:
        if isinstance(v, dt.datetime):
            return v.date()
        if isinstance(v, dt.date):
            return v
        if isinstance(v, str) and len(v) >= 10:
            try:
                return dt.date.fromisoformat(v[:10])
            except Exception:
                return None
        if isinstance(v, str) and len(v) >= 7:
            # if only YYYY-MM, fall back to first day
            try:
                y, m = v[:7].split("-")
                return dt.date(int(y), int(m), 1)
            except Exception:
                return None
        return None

    d: Optional[dt.date] = None
    if source == "created_at":
        d = _to_date(r.get("created_at"))
    if d is None:
        d = _to_date(r.get("publish_date"))
    if d is None:
        d = dt.date.today()

    if week_start == "sun":
        # shift one day forward so that Sun-start week aligns closer to ISO week buckets
        d = d + dt.timedelta(days=1)

    iso_year, iso_week, _ = d.isocalendar()
    return f"{iso_year:04d}-W{iso_week:02d}"


def _ensure_worksheet_and_header(
    sheets, spreadsheet_id: str, worksheet: str, header: List[str]
) -> int:
    """
    Ensure worksheet exists and first row is header.
    Returns sheetId.
    """
    meta = sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets_info = meta.get("sheets", [])

    sheet_id = None
    for s in sheets_info:
        props = s.get("properties", {})
        if props.get("title") == worksheet:
            sheet_id = props.get("sheetId")
            break

    if sheet_id is None:
        # Create sheet
        req = {"requests": [{"addSheet": {"properties": {"title": worksheet}}}]}
        resp = sheets.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=req).execute()
        sheet_id = resp["replies"][0]["addSheet"]["properties"]["sheetId"]
        print(f"[sheets] 创建 worksheet: {worksheet}")

    # Check header row
    rng = f"{worksheet}!A1:F1"
    values = sheets.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute().get("values", [])

    # --- schema migration: shrink columns (drop stock_code, report_year, tags) ---
    # Old (9 cols): key, stock_code, stock_name, report_year, publish_date, pdf_name, score, tags, notes
    # New (6 cols): key, stock_name, publish_date, pdf_name, score, notes
    if values and values[0]:
        old = values[0]
        if old[:9] == [
            "key",
            "stock_code",
            "stock_name",
            "report_year",
            "publish_date",
            "pdf_name",
            "score",
            "tags",
            "notes",
        ]:
            # Delete columns in descending index order to avoid shifting:
            # H(tags) idx=7, D(report_year) idx=3, B(stock_code) idx=1 (0-based)
            req = {
                "requests": [
                    {
                        "deleteDimension": {
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "COLUMNS",
                                "startIndex": 7,
                                "endIndex": 8,
                            }
                        }
                    },
                    {
                        "deleteDimension": {
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "COLUMNS",
                                "startIndex": 3,
                                "endIndex": 4,
                            }
                        }
                    },
                    {
                        "deleteDimension": {
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "COLUMNS",
                                "startIndex": 1,
                                "endIndex": 2,
                            }
                        }
                    },
                ]
            }
            sheets.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=req).execute()
            print("[sheets] schema migration: removed columns stock_code/report_year/tags")
            # Re-read header after deleting columns
            values = sheets.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id, range=rng
            ).execute().get("values", [])

    # --- schema migration: old header without publish_date (legacy) ---
    # Old (8 cols): key, stock_code, stock_name, report_year, pdf_name, score, tags, notes
    # We first insert publish_date at E, then apply the shrink migration above on next run.
    if values and values[0]:
        old = values[0]
        if (
            len(old) >= 8
            and old[:4] == ["key", "stock_code", "stock_name", "report_year"]
            and old[4] == "pdf_name"
            and ("publish_date" not in old)
        ):
            req = {
                "requests": [
                    {
                        "insertDimension": {
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "COLUMNS",
                                "startIndex": 4,
                                "endIndex": 5,
                            },
                            "inheritFromBefore": True,
                        }
                    }
                ]
            }
            sheets.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=req).execute()
            print("[sheets] schema migration: inserted publish_date column (E)")
            values = sheets.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id, range=rng
            ).execute().get("values", [])

    if not values or values[0][: len(header)] != header:
        body = {"values": [header]}
        sheets.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{worksheet}!A1",
            valueInputOption="RAW",
            body=body,
        ).execute()
        print("[sheets] 写入/修复 header")

    return int(sheet_id)


def _read_existing_key_map(sheets, spreadsheet_id: str, worksheet: str) -> Dict[str, int]:
    """
    Read column A (key) and build key -> 1-based row index.
    """
    rng = f"{worksheet}!A2:A"
    resp = sheets.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
    values = resp.get("values", []) or []

    mp: Dict[str, int] = {}
    # Row index starts at 2 for first data row
    for i, row in enumerate(values, start=2):
        if not row:
            continue
        k = str(row[0]).strip()
        if k:
            mp[k] = i
    return mp



def _batch_update_rows_A_to_D(
    sheets, spreadsheet_id: str, worksheet: str, updates: List[Tuple[int, List[Any]]]
) -> None:
    """
    Update only columns A:D (program columns), leaving manual columns E:F untouched.
    """
    data = []
    for row_idx, row_values in updates:
        # A:D are first 4 columns
        vals = row_values[:4]
        rng = f"{worksheet}!A{row_idx}:D{row_idx}"
        data.append({"range": rng, "values": [vals]})

    body = {"valueInputOption": "RAW", "data": data}
    sheets.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def _append_rows(sheets, spreadsheet_id: str, worksheet: str, rows: List[List[Any]]) -> None:
    body = {"values": rows}
    sheets.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"{worksheet}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body=body,
    ).execute()


def _upload_pdf_to_drive(drive, file_path: str, folder_id: Optional[str]) -> str:
    """
    Upload PDF to Drive (drive.file scope). Returns a shareable URL (view link).
    Note: drive.file only grants access to files created by this service account; you may need to share the folder/file.
    """
    from googleapiclient.http import MediaFileUpload

    p = Path(file_path)
    metadata: Dict[str, Any] = {"name": p.name}
    if folder_id:
        metadata["parents"] = [folder_id]

    media = MediaFileUpload(str(p), mimetype="application/pdf", resumable=True)
    created = drive.files().create(body=metadata, media_body=media, fields="id,webViewLink").execute()
    fid = created.get("id")
    link = created.get("webViewLink")
    if link:
        return link
    if fid:
        return f"https://drive.google.com/file/d/{fid}/view"
    return ""