"""
ReportClaw - 每日年报摘录汇总（PDF 生成 + 邮件发送）

作用
- 从 MySQL（annual_reports + annual_report_mda）读取“新增入库”的年报摘录（以 annual_report_mda.created_at 为准）。
- 将每个标的的摘要按固定版式渲染为一个汇总 PDF（默认输出到 data/report/）。
- 可选通过 SMTP 发送邮件（支持多收件人）。
- rows 字段将包含可选的 `chairman_letter`（董事长致辞/致股东信），如从年报中成功提取。

增量逻辑（不漏发）
- 以 annual_report_mda.created_at 做增量边界（防重复）：
    m.created_at ∈ (last_generated_iso, now]
- 状态文件：data/state/last_sent.json
    - last_generated_iso：上次成功生成日报 PDF 的截止时间（用于防止第二天重复出现在报表里）
    - last_sent_iso：上次成功发邮件的截止时间（仅用于邮件侧的审计/可选重发）

配置（conf/config.ini）
- [mysql] 必填：host/port/user/pass/db
- [email] 可选：
    enabled=true/false
    host, port, use_ssl, timeout
    user, pass, from, to
  说明：to 支持多个收件人，逗号/分号/空格分隔。

用法
1) 默认增量（推荐）：按 created_at 从 last_sent_iso 到 now 生成 PDF 并按配置发送
    python src/reportclaw/daily_report.py

2) 只生成不发邮件：
    python src/reportclaw/daily_report.py --no-email

3) 仅发送邮件（假设 PDF 已生成）：
    python src/reportclaw/daily_report.py --only-email

4) 手工指定某个披露日（publish_date）生成（不影响 last_sent_at）：
    python src/reportclaw/daily_report.py --date YYYY-MM-DD

5) 忽略 last_sent_iso，仅取今天 00:00 到现在的入库记录：
    python src/reportclaw/daily_report.py --today-only

6) 使用代理方式修改google sheet
export HTTPS_PROXY=http://127.0.0.1:1092
export HTTP_PROXY=http://127.0.0.1:1092
export https_proxy=http://127.0.0.1:1092
export http_proxy=http://127.0.0.1:1092
PYTHONPATH=src ./venv/bin/python -m reportclaw.daily_report --no-email --date 2026-02-28

输出
- PDF：data/report/annual_report_summary_YYYY-MM-DD.pdf
- 状态：data/state/last_sent.json
"""
import argparse
import configparser
import datetime as dt
from email.message import EmailMessage
import smtplib
import json
import re
import time
import zipfile
import uuid
from xml.sax.saxutils import escape

import mysql.connector

from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont


from pathlib import Path

from reportclaw.sheet_sync import sync_rows_to_google_sheet

 # daily_report.py 位于 src/reportclaw/ 下，所以项目根目录是再向上两级
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONF_DIR = PROJECT_ROOT / "conf"
DATA_DIR = PROJECT_ROOT / "data"
DAILY_DIR = DATA_DIR / "report"
STATE_DIR = DATA_DIR / "state"

# --- State helpers for last sent timestamp (created_at) ---
def _parse_dt(s: str) -> dt.datetime:
    # stored as "YYYY-MM-DD HH:MM:SS" in local time
    return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")

def _ar_pdf_base_name(d: dt.date) -> str:
    """Base daily PDF filename like: AR-0319.pdf (MMDD)."""
    return f"AR-{d.strftime('%m%d')}.pdf"


def _pick_daily_pdf_path(run_date: dt.date) -> Path:
    """Pick a daily PDF path under DAILY_DIR.

    If today's base name already exists, append a suffix:
      AR-0319.pdf, AR-0319-2.pdf, AR-0319-3.pdf, ...
    """
    DAILY_DIR.mkdir(parents=True, exist_ok=True)
    base = _ar_pdf_base_name(run_date)
    p0 = DAILY_DIR / base
    if not p0.exists():
        return p0

    stem = p0.stem  # e.g. AR-0319
    for k in range(2, 1000):
        pk = DAILY_DIR / f"{stem}-{k}.pdf"
        if not pk.exists():
            return pk

    raise RuntimeError(f"Too many daily PDFs for {run_date}: {p0}")


def _list_existing_daily_pdfs(run_date: dt.date) -> list[Path]:
    """List existing daily PDFs for a given date, including suffixed ones."""
    DAILY_DIR.mkdir(parents=True, exist_ok=True)
    base = _ar_pdf_base_name(run_date)
    stem = Path(base).stem  # AR-0319
    found: list[Path] = []

    p0 = DAILY_DIR / f"{stem}.pdf"
    if p0.exists():
        found.append(p0)

    for k in range(2, 1000):
        pk = DAILY_DIR / f"{stem}-{k}.pdf"
        if pk.exists():
            found.append(pk)
        else:
            # stop at first gap to avoid scanning too much
            break

    return found


def _latest_daily_pdf_path(run_date: dt.date) -> Path | None:
    """Return the latest existing daily PDF path (highest suffix) for the given date."""
    found = _list_existing_daily_pdfs(run_date)
    if not found:
        return None
    return found[-1]

def _state_path() -> Path:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    return STATE_DIR / "last_sent.json"



def load_last_sent_at() -> dt.datetime | None:
    """Load last sent timestamp from shared state file.

    Shared schema (one file):
      - last_sent_iso: used by daily_report (preferred)
      - last_crawl_end_iso: used by main crawler (must be preserved)

    Backward compatible:
      - legacy key: last_sent_at (format: YYYY-MM-DD HH:MM:SS)
    """
    sp = _state_path()
    if not sp.exists():
        return None
    try:
        data = json.loads(sp.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return None

        v = data.get("last_sent_iso") or data.get("last_sent_at")
        if not v:
            return None

        # accept unix ts
        if isinstance(v, (int, float)):
            return dt.datetime.fromtimestamp(float(v))

        s = str(v).strip()
        if not s:
            return None

        # legacy: "YYYY-MM-DD HH:MM:SS"
        if re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", s):
            return _parse_dt(s)

        # date-only
        if len(s) == 10:
            return dt.datetime.strptime(s, "%Y-%m-%d")

        # iso datetime
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None


def save_last_sent_at(ts: dt.datetime):
    """Save last sent timestamp into shared state file without overwriting other keys."""
    sp = _state_path()
    sp.parent.mkdir(parents=True, exist_ok=True)

    obj = {}
    if sp.exists():
        try:
            old = json.loads(sp.read_text(encoding="utf-8"))
            if isinstance(old, dict):
                obj.update(old)
        except Exception:
            obj = {}

    # preferred key (ISO, seconds)
    obj["last_sent_iso"] = ts.replace(microsecond=0).isoformat()

    # 不再写入重复的 legacy 字段 last_sent_at（避免状态文件冗余）。
    # 读取端仍兼容 last_sent_at，方便你历史文件平滑过渡。
    if "last_sent_at" in obj:
        obj.pop("last_sent_at", None)

    sp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


# --- Helpers for last generated timestamp (防止重复出现在报表) ---
def load_last_generated_at() -> dt.datetime | None:
    """Load last generated timestamp from shared state file.

    Preferred key: last_generated_iso (ISO)
    Backward compatible: if missing, fall back to last_sent_iso / last_sent_at.
    """
    sp = _state_path()
    if not sp.exists():
        return None
    try:
        data = json.loads(sp.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return None

        v = data.get("last_generated_iso") or data.get("last_sent_iso") or data.get("last_sent_at")
        if not v:
            return None

        if isinstance(v, (int, float)):
            return dt.datetime.fromtimestamp(float(v))

        s = str(v).strip()
        if not s:
            return None

        if re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", s):
            return _parse_dt(s)
        if len(s) == 10:
            return dt.datetime.strptime(s, "%Y-%m-%d")
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None


def save_last_generated_at(ts: dt.datetime):
    """Save last generated timestamp (ISO) into shared state file without overwriting other keys."""
    sp = _state_path()
    sp.parent.mkdir(parents=True, exist_ok=True)

    obj = {}
    if sp.exists():
        try:
            old = json.loads(sp.read_text(encoding="utf-8"))
            if isinstance(old, dict):
                obj.update(old)
        except Exception:
            obj = {}

    obj["last_generated_iso"] = ts.replace(microsecond=0).isoformat()

    sp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def mysql_connect(cfg: configparser.ConfigParser):
    if "mysql" not in cfg:
        raise RuntimeError("config.ini missing [mysql] section")
    return mysql.connector.connect(
        host=cfg["mysql"].get("host", "127.0.0.1"),
        port=int(cfg["mysql"].get("port", "3306")),
        user=cfg["mysql"].get("user", ""),
        password=cfg["mysql"].get("pass", ""),
        database=cfg["mysql"].get("db", ""),
    )


def fetch_rows_by_publish_date(conn, publish_date: str):
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT
          r.stock_code, r.stock_name, r.report_year, r.publish_date, r.file_path,
          m.industry_section, m.main_business_section, m.future_section, m.chairman_letter, m.full_mda
        FROM annual_reports r
        JOIN annual_report_mda m ON m.report_id = r.id
        WHERE r.publish_date = %s
        ORDER BY r.stock_code ASC
        """,
        (publish_date,),
    )
    rows = cur.fetchall()
    return rows or []


# --- Fetch by publish_date range ---
def fetch_rows_by_publish_date_range(conn, start_date: str, end_date: str):
    """
    Fetch rows where publish_date between [start_date, end_date] inclusive. Dates are YYYY-MM-DD.
    """
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT
          r.stock_code, r.stock_name, r.report_year, r.publish_date, r.file_path,
          m.industry_section, m.main_business_section, m.future_section, m.chairman_letter, m.full_mda
        FROM annual_reports r
        JOIN annual_report_mda m ON m.report_id = r.id
        WHERE r.publish_date >= %s AND r.publish_date <= %s
        ORDER BY r.publish_date DESC, r.stock_code ASC
        """,
        (start_date, end_date),
    )
    rows = cur.fetchall()
    return rows or []


def fetch_rows_by_created_at_range(conn, start_ts: str, end_ts: str):
    """
    Fetch rows where annual_report_mda.created_at is in (start_ts, end_ts] (timestamps as strings 'YYYY-MM-DD HH:MM:SS').
    """
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT
          r.stock_code, r.stock_name, r.report_year, r.publish_date, r.file_path,
          m.industry_section, m.main_business_section, m.future_section, m.chairman_letter, m.full_mda,
          m.created_at
        FROM annual_reports r
        JOIN annual_report_mda m ON m.report_id = r.id
        WHERE m.created_at > %s AND m.created_at <= %s
        ORDER BY m.created_at DESC, r.stock_code ASC
        """,
        (start_ts, end_ts),
    )
    rows = cur.fetchall()
    return rows or []



def _ensure_chinese_font(styles):
    """
    Best-effort Chinese font registration for ReportLab.
    ReportLab handles TTF reliably; TTC may not work depending on build.
    If no TTF font is found, we fall back to the default font (may render tofu for CJK).
    """
    candidates = [
        "/Library/Fonts/Arial Unicode.ttf",
        "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
        "/System/Library/Fonts/Supplemental/Arial Unicode MS.ttf",
    ]

    for fp in candidates:
        p = Path(fp)
        if not p.exists():
            continue
        try:
            if p.suffix.lower() == ".ttf":
                pdfmetrics.registerFont(TTFont("CNFont", str(p)))
                styles["Normal"].fontName = "CNFont"
                return "CNFont"
        except Exception:
            continue

    return styles["Normal"].fontName


# --- New unified text cleaning helper for both PDF and EPUB ---
def clean_text_for_reading(t: str) -> str:
    """Common cleaner for both PDF and EPUB rendering.

    Goal: remove header/footer/page numbers and merge artificial hard-wraps introduced by PDF text extraction,
    while preserving real headings and list item prefixes.
    """
    if not t:
        return t

    # 1) Basic normalize
    raw_lines = str(t).replace("\u00a0", " ").splitlines()

    def is_header_footer(line: str) -> bool:
        s = (line or "").strip()
        if not s:
            return True
        # pure page number
        if s.isdigit():
            return True
        # x/y page number
        if "/" in s:
            parts = [p.strip() for p in s.split("/") if p.strip()]
            if parts and all(p.isdigit() for p in parts) and len(s) <= 15:
                return True
        # common headers
        if ("年度报告" in s) and ("股份有限公司" in s):
            return True
        if s.startswith("公司代码：") or s.startswith("公司简称：") or ("公司代码：" in s):
            return True
        if (s.endswith("股份有限公司") or s.endswith("有限公司")) and len(s) <= 30:
            return True
        if ("年度报告" in s) and len(s) <= 25:
            return True
        return False

    tmp: list[str] = []
    for ln in raw_lines:
        s = (ln or "").strip()
        if is_header_footer(s):
            continue
        tmp.append(s)

    # 2) Soften very long url/identifier-like tokens so renderers can wrap
    def soften_long_tokens(line: str) -> str:
        def _soften_token(tok: str) -> str:
            if len(tok) < 25:
                return tok
            if re.fullmatch(r"[A-Za-z0-9_\\./-]+", tok):
                step = 20
                return "\u200b".join(tok[i : i + step] for i in range(0, len(tok), step))
            return tok

        parts = re.split(r"(\s+)", line)
        for i in range(0, len(parts), 2):
            parts[i] = _soften_token(parts[i])
        return "".join(parts)

    tmp = [soften_long_tokens(x) for x in tmp]

    # 3) Merge ultra-short table-like fragments (one word per line)
    merged: list[str] = []
    buf: list[str] = []

    def flush_buf():
        nonlocal buf
        if buf:
            merged.append("  ".join(buf).strip())
            buf = []

    for s in tmp:
        is_short = (len(s) <= 6) and (not re.search(r"[。；;：:]$", s))
        if is_short:
            buf.append(s)
            if len(buf) >= 12:
                flush_buf()
            continue

        # Special case: split ordinals like "2" + "新能源"
        if len(buf) == 1:
            token = buf[0].strip()
            if re.fullmatch(r"\d{1,2}", token):
                if not re.match(r"^\d{1,2}\s*[、\.．:：)]", s):
                    merged.append(f"{token}、{s}")
                    buf = []
                    continue
            if re.fullmatch(r"[一二三四五六七八九十]{1,3}", token):
                if not re.match(r"^[一二三四五六七八九十]{1,3}\s*[、\.．:：)]", s):
                    merged.append(f"{token}、{s}")
                    buf = []
                    continue
            if re.fullmatch(r"[（(]\s*(?:\d{1,2}|[一二三四五六七八九十]{1,3})\s*[）)]", token):
                merged.append(f"{token}{s}")
                buf = []
                continue

        flush_buf()
        merged.append(s)

    flush_buf()

    # 4) Reflow: join hard-wrapped lines into paragraphs
    heading_re = re.compile(
        r"^(第[一二三四五六七八九十0-9]{1,3}[节章节]|"
        r"[一二三四五六七八九十]{1,3}、|"
        r"\d{1,2}、|"
        r"[（(][一二三四五六七八九十0-9]{1,3}[）)]|"
        r"\([一二三四五六七八九十0-9]{1,3}\))"
    )
    end_punct_re = re.compile(r"[。！？；：:）)」』】]$")

    paras: list[str] = []
    cur = ""

    def flush_cur():
        nonlocal cur
        if cur.strip():
            paras.append(cur.strip())
        cur = ""

    for s in merged:
        s = (s or "").strip()
        if not s:
            flush_cur()
            continue

        if heading_re.match(s):
            flush_cur()
            paras.append(s)
            paras.append("")
            continue

        if not cur:
            cur = s
            continue

        if end_punct_re.search(cur):
            flush_cur()
            cur = s
        else:
            cur += s

    flush_cur()
    while paras and paras[-1] == "":
        paras.pop()

    return "\n".join(paras).strip()


def generate_daily_summary_pdf(rows, out_path: str, title_date: str) -> str:
    """
    将 rows（DB 查询结果）渲染为汇总 PDF。

    rows: list[dict]，字段包含：
      - stock_code/stock_name/report_year/publish_date/file_path
      - main_business_section（管理层综述摘录）
      - future_section（未来展望摘录，可为空）
      - created_at（用于展示与增量范围说明）

    版式策略（面向可读性）
    - 清理页眉页脚/页码
    - 合并短行（表格抽取污染）
    - 软断行超长 token（避免右侧溢出）
    - 重新排版为自然段 + 小标题分隔
    - 段首缩进（HTML 全角空格实体）
    """
    styles = getSampleStyleSheet()
    base_font = _ensure_chinese_font(styles)

    h1 = ParagraphStyle(
        name="H1",
        parent=styles["Title"],
        fontName=base_font,
        fontSize=16,
        leading=22,
        alignment=1,  # center
        spaceAfter=10,
    )
    stock_header = ParagraphStyle(
        name="StockHeader",
        parent=styles["Heading2"],
        fontName=base_font,
        fontSize=12.5,
        leading=18,
        spaceBefore=4,
        spaceAfter=6,
        alignment=0,
    )
    stock_footer = ParagraphStyle(
        name="StockFooter",
        parent=styles["Normal"],
        fontName=base_font,
        fontSize=9.5,
        leading=13,
        spaceBefore=8,
        spaceAfter=4,
        alignment=1,  # center
    )
    section_header = ParagraphStyle(
        name="SectionHeader",
        parent=styles["Heading3"],
        fontName=base_font,
        fontSize=11.5,
        leading=16,
        spaceBefore=6,
        spaceAfter=4,
    )
    body = ParagraphStyle(
        name="Body",
        parent=styles["Normal"],
        fontName=base_font,
        fontSize=10.5,
        leading=15.5,
        firstLineIndent=0,
        spaceAfter=4,
        wordWrap="CJK",
        splitLongWords=1,
        allowWidows=1,
    )
    pre = ParagraphStyle(
        name="Pre",
        parent=styles["Normal"],
        fontName=base_font,
        fontSize=10.2,
        leading=14.0,
        wordWrap="CJK",
        splitLongWords=1,
        allowWidows=1,
    )

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

    doc = SimpleDocTemplate(
        out_path,
        pagesize=A4,
        leftMargin=18 * mm,
        rightMargin=18 * mm,
        topMargin=16 * mm,
        bottomMargin=16 * mm,
        title=f"年报摘录汇总 {title_date}",
    )

    story = []
    story.append(Paragraph(f"年报摘录汇总（{title_date}）", h1))
    story.append(Spacer(1, 6 * mm))
    def pick_section_text(primary: str | None, fallback_full: str, max_chars: int = 12000) -> str | None:
        """Prefer primary section text; otherwise fall back to full_mda.

        For parse-failed placeholders, we strip the sentinel first line and then cap length.
        """
        if primary and str(primary).strip():
            return str(primary)
        if not fallback_full:
            return None
        t = str(fallback_full)
        if t.startswith("[PARSE_FAILED]"):
            # drop sentinel line
            parts = t.split("\n", 1)
            t = parts[1] if len(parts) == 2 else ""
        t = t.strip()
        if not t:
            return None
        if len(t) > max_chars:
            t = t[:max_chars] + "\n...（内容过长已截断）"
        return t

    def safe_block(text: str):
        if not text:
            return Paragraph("（未提取到内容）", body)

        cleaned = clean_text_for_reading(text)

        # Paragraph 会折叠行首空白，因此用 HTML 实体来做“首行缩进”
        heading_re = re.compile(
            r"^(第[一二三四五六七八九十0-9]{1,3}[节章节]|"
            r"[一二三四五六七八九十]{1,3}、|"
            r"\d{1,2}、|"
            r"[（(][一二三四五六七八九十0-9]{1,3}[）)]|"
            r"\([一二三四五六七八九十0-9]{1,3}\))"
        )

        lines = cleaned.split("\n")
        html_lines = []
        indent = "&#12288;&#12288;"  # 两个全角空格
        for ln in lines:
            s = ln.strip()
            if not s:
                html_lines.append("")
                continue
            esc = escape(s)
            if heading_re.match(s):
                html_lines.append(esc)
            else:
                html_lines.append(indent + esc)

        html = "<br/>".join(html_lines)
        return Paragraph(html, body)

    for i, r in enumerate(rows):
        file_path = r.get("file_path", "") or ""
        pdf_name = ""
        try:
            if file_path:
                pdf_name = Path(str(file_path)).name
        except Exception:
            pdf_name = ""

        # Fallback full_mda and parse-failed marker
        full_mda = r.get("full_mda") or ""
        is_parse_failed = full_mda.startswith("[PARSE_FAILED]")

        # header 尽量控制在一行：不展示入库时间；文件名过长则截断
        if pdf_name and len(pdf_name) > 42:
            pdf_name = pdf_name[:39] + "..."

        header = (
            f"{r.get('stock_code','')} {r.get('stock_name','')} | {r.get('report_year','')}年年报"
            f" | 公告 {r.get('publish_date','')}"
        )
        if pdf_name:
            header += f" | 文件 {pdf_name}"
        if is_parse_failed:
            header += " | PARSE_FAILED"

        story.append(Paragraph(header, stock_header))
        story.append(Spacer(1, 3 * mm))

        def add_divider():
            # 使用文本分界线（更稳定，不易被阅读器当作“横线/页眉页脚”吞掉）
            story.append(Spacer(1, 2 * mm))
            story.append(Paragraph("────────────────────────────────", stock_footer))
            story.append(Spacer(1, 3 * mm))

        # 1) 董事长致辞 / 致股东(投资者)信（如果有）
        chairman = r.get("chairman_letter")
        has_chairman = chairman and str(chairman).strip()
        if has_chairman:
            story.append(Paragraph("董事长致辞 / 致股东(投资者)信", section_header))
            story.append(safe_block(str(chairman)))
            add_divider()

        # 2) 管理层综述
        story.append(Paragraph("管理层综述（摘录）", section_header))
        story.append(safe_block(pick_section_text(r.get("main_business_section"), full_mda)))

        # 3) 未来展望（与上一段用分界线隔开）
        add_divider()
        story.append(Paragraph("未来展望（摘录）", section_header))
        story.append(safe_block(pick_section_text(r.get("future_section"), "")))
        # 在每个标的摘录末尾再次标记一次股票信息，方便移动端阅读器定位
        end_mark = header
        story.append(Spacer(1, 3 * mm))
        # 结尾再次输出股票信息，便于移动端阅读器定位
        story.append(Paragraph(end_mark, stock_footer))
        # 结尾标记：单独一行 + 额外留白（避免被阅读器当作横线/页眉页脚误删）
        story.append(Spacer(1, 2 * mm))
        story.append(Paragraph("###########**end****############", stock_footer))
        story.append(Spacer(1, 4 * mm))

        if i < len(rows) - 1:
            # Next ticker on a new page
            story.append(PageBreak())

    doc.build(story)
    return out_path


def _pick_daily_epub_path_for_pdf(pdf_path: str) -> Path:
    """Generate an EPUB path matching the PDF filename (same stem, .epub)."""
    p = Path(pdf_path)
    return p.with_suffix(".epub")


def _escape_xhtml(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
         .replace('"', "&quot;")
         .replace("'", "&apos;")
    )


def _text_to_xhtml_paras(t: str) -> str:
    """Render text into reflowable EPUB XHTML.

    Key goal: produce *more paragraph breaks* (closer to the PDF output) instead of
    collapsing everything into a few long paragraphs.

    Approach:
    - Run `clean_text_for_reading` (removes header/footer/page numbers + light cleanup).
    - Treat EACH non-empty line as a paragraph (<p>), because `clean_text_for_reading`
      already inserts line breaks for sentence boundaries and headings.
    - Headings/list-prefix lines use `noindent`.
    """
    if not t:
        return "<p>（未提取到内容）</p>"

    t = clean_text_for_reading(t)
    t = t.replace("\r\n", "\n").replace("\r", "\n")

    heading_re = re.compile(
        r"^(第[一二三四五六七八九十0-9]{1,3}[节章节]|"
        r"[一二三四五六七八九十]{1,3}、|"
        r"\d{1,2}、|"
        r"[（(][一二三四五六七八九十0-9]{1,3}[）)]|"
        r"\([一二三四五六七八九十0-9]{1,3}\))"
    )

    out: list[str] = []
    blank_run = 0

    for ln in t.split("\n"):
        s = (ln or "").strip()
        if not s:
            # keep at most ONE blank separator in XHTML (handled by CSS margins)
            blank_run += 1
            continue

        blank_run = 0
        inner = _escape_xhtml(s)
        if heading_re.match(s):
            out.append(f"<p class=\"noindent\">{inner}</p>")
        else:
            out.append(f"<p>{inner}</p>")

    return "\n".join(out) if out else "<p>（未提取到内容）</p>"


def generate_daily_summary_epub(rows, out_path: str, title_date: str) -> str:
    """Generate a minimal EPUB2 (zip-based) alongside the PDF."""
    out_p = Path(out_path)
    out_p.parent.mkdir(parents=True, exist_ok=True)

    book_id = str(uuid.uuid4())
    title = f"年报摘录汇总（{title_date}）"

    def make_header(r: dict) -> str:
        file_path = r.get("file_path", "") or ""
        pdf_name = ""
        try:
            if file_path:
                pdf_name = Path(str(file_path)).name
        except Exception:
            pdf_name = ""
        if pdf_name and len(pdf_name) > 60:
            pdf_name = pdf_name[:57] + "..."

        header = (
            f"{r.get('stock_code','')} {r.get('stock_name','')} | {r.get('report_year','')}年年报"
            f" | 公告 {r.get('publish_date','')}"
        )
        if pdf_name:
            header += f" | 文件 {pdf_name}"
        return header

    manifest_items = []
    spine_items = []
    navpoints = []
    xhtml_files: dict[str, str] = {}

    for idx, r in enumerate(rows, start=1):
        header = make_header(r)
        full_mda = r.get("full_mda") or ""

        biz = r.get("main_business_section") or ""
        if (not str(biz).strip()) and full_mda:
            biz = full_mda
            if str(biz).startswith("[PARSE_FAILED]"):
                parts = str(biz).split("\n", 1)
                biz = parts[1] if len(parts) == 2 else ""

        fut = r.get("future_section") or ""

        chairman = r.get("chairman_letter") or ""
        chairman = str(chairman).strip()

        parts = [
            f"<h2>{_escape_xhtml(header)}</h2>",
        ]

        def add_divider():
            # EPUB 阅读器可能会弱化/忽略 <hr>，所以用“文本分隔符”做双保险
            parts.append("<hr class=\"divider\" />")
            parts.append("<p class=\"sep\">####******####</p>")
            parts.append("<p class=\"noindent\">&nbsp;</p>")

        # 1) 董事长致辞 / 致股东(投资者)信（如果有）
        if chairman:
            parts += [
                "<h3>董事长致辞 / 致股东(投资者)信</h3>",
                _text_to_xhtml_paras(chairman),
            ]
            add_divider()

        # 2) 管理层综述
        parts += [
            "<h3>管理层综述（摘录）</h3>",
            _text_to_xhtml_paras(str(biz)),
        ]

        # 3) 未来展望
        add_divider()
        parts += [
            "<h3>未来展望（摘录）</h3>",
            _text_to_xhtml_paras(str(fut)),
            "<p class='center' style='margin-top:1em;'>###########**end****############</p>",
        ]

        body_html = "\n".join(parts)

        xhtml = f"""<?xml version='1.0' encoding='utf-8'?>
<!DOCTYPE html PUBLIC '-//W3C//DTD XHTML 1.1//EN'
  'http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd'>
<html xmlns=\"http://www.w3.org/1999/xhtml\">
<head>
  <title>{_escape_xhtml(r.get('stock_code',''))}</title>
  <meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />
  <style type=\"text/css\">
    body {{ font-family: serif; line-height: 1.7; margin: 0 0.9em; }}
    h2 {{ margin-top: 0.8em; font-size: 1.15em; }}
    h3 {{ margin-top: 1.0em; font-size: 1.05em; }}
    /* More visible paragraph breaks + first-line indent in EPUB */
    p {{ margin: 1.4em 0; text-indent: 2em; }}
    p.noindent {{ text-indent: 0; }}
    p.center {{ text-indent: 0; text-align: center; }}
    p.sep {{ text-indent: 0; text-align: center; margin: 1.0em 0; letter-spacing: 0.08em; }}
    hr.divider {{ border: 0; border-top: 1px solid #999; margin: 1.2em 0; }}
  </style>
</head>
<body>
{body_html}
</body>
</html>
"""

        fn = f"OEBPS/chap{idx:03d}.xhtml"
        xhtml_files[fn] = xhtml

        item_id = f"chap{idx:03d}"
        manifest_items.append((item_id, f"chap{idx:03d}.xhtml", "application/xhtml+xml"))
        spine_items.append(item_id)

        navpoints.append(
            f"""<navPoint id=\"navPoint-{idx}\" playOrder=\"{idx}\">
  <navLabel><text>{_escape_xhtml(r.get('stock_code',''))} {_escape_xhtml(r.get('stock_name',''))}</text></navLabel>
  <content src=\"chap{idx:03d}.xhtml\"/>
</navPoint>"""
        )

    manifest_xml = "\n".join(
        [
            '<item id="ncx" href="toc.ncx" media-type="application/x-dtbncx+xml"/>',
            *[f'<item id="{i}" href="{href}" media-type="{mt}"/>' for (i, href, mt) in manifest_items],
        ]
    )
    spine_xml = "\n".join([f'<itemref idref="{i}"/>' for i in spine_items])

    opf = f"""<?xml version='1.0' encoding='utf-8'?>
<package xmlns=\"http://www.idpf.org/2007/opf\" unique-identifier=\"BookId\" version=\"2.0\">
  <metadata xmlns:dc=\"http://purl.org/dc/elements/1.1/\">
    <dc:title>{_escape_xhtml(title)}</dc:title>
    <dc:language>zh-CN</dc:language>
    <dc:identifier id=\"BookId\">urn:uuid:{book_id}</dc:identifier>
  </metadata>
  <manifest>
{manifest_xml}
  </manifest>
  <spine toc=\"ncx\">
{spine_xml}
  </spine>
</package>
"""

    ncx = f"""<?xml version='1.0' encoding='utf-8'?>
<!DOCTYPE ncx PUBLIC "-//NISO//DTD ncx 2005-1//EN" "http://www.daisy.org/z3986/2005/ncx-2005-1.dtd">
<ncx xmlns=\"http://www.daisy.org/z3986/2005/ncx/\" version=\"2005-1\">
  <head>
    <meta name=\"dtb:uid\" content=\"urn:uuid:{book_id}\"/>
    <meta name=\"dtb:depth\" content=\"1\"/>
    <meta name=\"dtb:totalPageCount\" content=\"0\"/>
    <meta name=\"dtb:maxPageNumber\" content=\"0\"/>
  </head>
  <docTitle><text>{_escape_xhtml(title)}</text></docTitle>
  <navMap>
{''.join(navpoints)}
  </navMap>
</ncx>
"""

    container_xml = """<?xml version='1.0' encoding='utf-8'?>
<container version=\"1.0\" xmlns=\"urn:oasis:names:tc:opendocument:xmlns:container\">
  <rootfiles>
    <rootfile full-path=\"OEBPS/content.opf\" media-type=\"application/oebps-package+xml\"/>
  </rootfiles>
</container>
"""

    with zipfile.ZipFile(out_p, "w") as zf:
        zf.writestr("mimetype", "application/epub+zip", compress_type=zipfile.ZIP_STORED)
        zf.writestr("META-INF/container.xml", container_xml)
        zf.writestr("OEBPS/content.opf", opf)
        zf.writestr("OEBPS/toc.ncx", ncx)
        for path, content in xhtml_files.items():
            zf.writestr(path, content)

    return str(out_p)


def send_email_with_attachment_smtp(cfg: configparser.ConfigParser, to_addr: str, subject: str, body: str, attachment_path: str, epub_path: str | None = None):
    """
    通过 SMTP 发送带 PDF 附件的邮件。

    注意
    - cfg 来自 conf/config.ini 的 [email] 段
    - to_addr 可为逗号分隔的多个收件人
    - use_ssl=true 使用 SMTP_SSL；否则使用 STARTTLS（仅当服务器支持 starttls）
    """
    if "email" not in cfg:
        raise RuntimeError("config.ini missing [email] section")

    host = cfg["email"].get("host", "")
    port = int(cfg["email"].get("port", "465"))
    user = cfg["email"].get("user", "")
    password = cfg["email"].get("pass", "")
    from_addr = cfg["email"].get("from", user)
    use_ssl = cfg["email"].get("use_ssl", "true").lower() in ("1", "true", "yes", "y")
    timeout_sec = float(cfg["email"].get("timeout", "30"))

    # Retry + hint controls
    retries = int(cfg["email"].get("retries", "2"))          # additional retries; total attempts = 1 + retries
    retry_sleep = float(cfg["email"].get("retry_sleep", "3")) # base seconds, exponential backoff
    warn_mb = float(cfg["email"].get("warn_mb", "2.0"))

    if not host or not user or not password or not from_addr:
        raise RuntimeError("email config incomplete: need host/user/pass/from")

    msg = EmailMessage()
    msg["From"] = from_addr
    msg["To"] = to_addr  # may be comma-separated
    msg["Subject"] = subject
    msg.set_content(body)

    ap = Path(attachment_path)
    data = ap.read_bytes()
    size_mb = len(data) / (1024 * 1024)

    # Optional EPUB attachment
    epub_p: Path | None = None
    epub_size_mb: float | None = None
    if epub_path:
        try:
            epub_p = Path(epub_path)
            if not epub_p.exists() or not epub_p.is_file():
                epub_p = None
            else:
                epub_size_mb = epub_p.stat().st_size / (1024 * 1024)
        except Exception:
            epub_p = None

    # Diagnostics
    print(f"[email] smtp={host}:{port} ssl={use_ssl} timeout={timeout_sec}s to={to_addr}")
    if epub_p is not None:
        print(f"[email] attach: {ap.name} size={size_mb:.2f}MB + {epub_p.name} size={epub_size_mb:.2f}MB")
    else:
        print(f"[email] attach: {ap.name} size={size_mb:.2f}MB")

    total_mb = size_mb + (epub_size_mb or 0.0)
    if total_mb >= warn_mb:
        print(
            f"[email][warn] 附件较大（总计 {total_mb:.2f}MB），可能在发送 DATA 阶段超时。"
            f"建议：email.timeout=180~300，或减少日报内容/标的数量。"
        )

    # Attach PDF (required)
    msg.add_attachment(data, maintype="application", subtype="pdf", filename=ap.name)

    # Attach EPUB (optional)
    if epub_p is not None:
        epub_bytes = epub_p.read_bytes()
        # Use correct epub+zip mime; many clients rely on this
        msg.add_attachment(
            epub_bytes,
            maintype="application",
            subtype="epub+zip",
            filename=epub_p.name,
        )

    last_err = None

    for attempt in range(1, retries + 2):
        try:
            if use_ssl:
                with smtplib.SMTP_SSL(host, port, timeout=timeout_sec) as s:
                    s.login(user, password)
                    s.send_message(msg)
            else:
                with smtplib.SMTP(host, port, timeout=timeout_sec) as s:
                    s.ehlo()
                    # Only starttls if server supports it; some providers do not.
                    if s.has_extn("starttls"):
                        s.starttls()
                        s.ehlo()
                    else:
                        raise RuntimeError(
                            f"SMTP 服务器未宣告 STARTTLS（{host}:{port}）。"
                            f"若是新浪邮箱，通常请使用 465 + SSL（use_ssl=true）。"
                        )
                    s.login(user, password)
                    s.send_message(msg)

            if attempt > 1:
                print(f"[email] send succeeded on attempt {attempt}")
            return

        except smtplib.SMTPAuthenticationError as e:
            raise RuntimeError(
                "SMTP 认证失败：请确认账号/授权码无误（很多邮箱需要“客户端授权码/应用专用密码”），并确认已开启 SMTP。"
                f"原始错误: {e}"
            ) from e

        except (TimeoutError, smtplib.SMTPServerDisconnected, OSError, RuntimeError) as e:
            last_err = e
            if attempt <= retries:
                sleep_s = retry_sleep * (2 ** (attempt - 1))
                print(f"[email][warn] send failed (attempt {attempt}/{retries+1}): {e}")
                print(f"[email] retrying in {sleep_s:.1f}s ...")
                time.sleep(sleep_s)
                continue
            break

    raise RuntimeError(
        f"SMTP 发送失败（{host}:{port}）。常见原因：附件较大导致 DATA 阶段写入超时（可将 email.timeout 调到 180~300），或网络抖动/服务器限流断开。"
        f"建议：email.timeout=180~300，或减少日报内容；可设置 email.retries/email.retry_sleep。"
        f"最后错误: {last_err}"
    ) from last_err


def parse_args():
    p = argparse.ArgumentParser(description="Generate daily annual report excerpt PDF and optionally email it.")
    p.add_argument("--date", default=None, help="publish_date in YYYY-MM-DD. Default: today.")
    p.add_argument("--no-email", action="store_true", help="Do not send email even if enabled in config.")
    p.add_argument("--only-email", action="store_true", help="Only send email (assumes PDF already generated).")
    p.add_argument("--config", default=str(CONF_DIR / "config.ini"), help="Path to config.ini (default: conf/config.ini)")
    p.add_argument("--today-only", action="store_true", help="Force sending only today's created_at (ignore last_sent history).")
    p.add_argument("--epub", action="store_true", help="Also generate an EPUB alongside the PDF")
    return p.parse_args()

def load_config(path: str) -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    p = Path(path)
    if not p.is_absolute():
        # allow running from any working directory
        p = (PROJECT_ROOT / p).resolve()
    if not p.exists():
        raise RuntimeError(f"config file not found: {p}")
    cfg.read(p, encoding="utf-8")
    return cfg

def main():
    """
    CLI 入口：
    - 手工模式（--date）：按 publish_date 生成，不更新 last_sent_at
    - 增量模式（默认）：按 created_at 生成，成功后更新 last_sent_at
    - 支持 --no-email / --only-email / --today-only
    """
    args = parse_args()
    cfg = load_config(args.config)

    now = dt.datetime.now()
    # 模式1：手工指定某个披露日（保持老逻辑）
    manual_publish_date = args.date

    if manual_publish_date:
        day = manual_publish_date
        range_label = day
        DAILY_DIR.mkdir(parents=True, exist_ok=True)
        # Set run_date for Google Sheets sync
        run_date = dt.date.fromisoformat(day)
        out_epub: str | None = None
        if args.only_email:
            latest = _latest_daily_pdf_path(run_date)
            out_pdf = str(latest) if latest else str(_pick_daily_pdf_path(run_date))
            # If an EPUB exists for this PDF, attach it too
            guessed_epub = str(_pick_daily_epub_path_for_pdf(out_pdf))
            out_epub = guessed_epub if Path(guessed_epub).exists() else None
        else:
            out_pdf = str(_pick_daily_pdf_path(run_date))

        if not args.only_email:
            conn = mysql_connect(cfg)
            try:
                rows = fetch_rows_by_publish_date(conn, day)
            finally:
                conn.close()

            if not rows:
                print(f"{day} 无披露年报记录，不生成汇总PDF")
                return

            generate_daily_summary_pdf(rows, out_pdf, range_label)
            print(f"已生成每日汇总PDF: {out_pdf}")
            epub_enabled = args.epub or cfg.get("epub", "enabled", fallback="false").lower() in ("1", "true", "yes", "y")
            if epub_enabled:
                out_epub = str(_pick_daily_epub_path_for_pdf(out_pdf))
                generate_daily_summary_epub(rows, out_epub, range_label)
                print(f"已生成每日汇总EPUB: {out_epub}")
            # 同步到 Google Sheets（仅写客观字段，不覆盖 score/tags/notes/status）
            try:
                sync_rows_to_google_sheet(cfg, rows, run_date=run_date)
            except Exception as e:
                print(f"[sheets] 同步失败（忽略，不影响主流程）：{e}")
        else:
            if not Path(out_pdf).exists():
                print(f"未找到PDF文件: {out_pdf}，无法仅发送邮件")
                return

        # 手工模式不更新 last_sent_at（避免影响增量逻辑）
    else:
        # 模式2：按入库时间增量（m.created_at）
        # Incremental boundary (生成侧)：以 last_generated_iso 为准，避免“同一天已发过邮件”导致后续增量被跳过。
        # last_sent_iso 仅用于邮件侧审计/可选重发，不应作为生成窗口边界。
        last_sent_at = load_last_sent_at()
        last_generated_at = load_last_generated_at()

        if args.today_only or (last_sent_at is None and last_generated_at is None):
            start_at = dt.datetime.combine(dt.date.today(), dt.time(0, 0, 0))
            start_src = "today_only"
        else:
            start_at = last_generated_at or last_sent_at
            start_src = "last_generated" if last_generated_at else "last_sent"

        print(f"[daily_report] incremental start_at={start_at} (src={start_src}), end_at={now}")

        end_at = now

        # Set run_date for Google Sheets sync
        run_date = dt.date.today()

        start_ts = start_at.strftime("%Y-%m-%d %H:%M:%S")
        end_ts = end_at.strftime("%Y-%m-%d %H:%M:%S")

        range_label = f"{start_ts} ~ {end_ts}"
        DAILY_DIR.mkdir(parents=True, exist_ok=True)
        if args.only_email:
            latest = _latest_daily_pdf_path(run_date)
            out_pdf = str(latest) if latest else str(_pick_daily_pdf_path(run_date))
            # If an EPUB exists for this PDF, attach it too
            guessed_epub = str(_pick_daily_epub_path_for_pdf(out_pdf))
            out_epub = guessed_epub if Path(guessed_epub).exists() else None
        else:
            out_pdf = str(_pick_daily_pdf_path(run_date))

        if not args.only_email:
            conn = mysql_connect(cfg)
            try:
                rows = fetch_rows_by_created_at_range(conn, start_ts, end_ts)
            finally:
                conn.close()

            if not rows:
                print(f"{range_label} 无新增入库年报记录，不生成汇总PDF")
                # 不推进 last_generated_iso：避免出现“后续补入库但 created_at 落在旧窗口内”而被永远跳过。
                return

            generate_daily_summary_pdf(rows, out_pdf, range_label)
            print(f"已生成每日汇总PDF: {out_pdf}")
            out_epub: str | None = None
            epub_enabled = args.epub or cfg.get("epub", "enabled", fallback="false").lower() in ("1", "true", "yes", "y")
            if epub_enabled:
                out_epub = str(_pick_daily_epub_path_for_pdf(out_pdf))
                generate_daily_summary_epub(rows, out_epub, range_label)
                print(f"已生成每日汇总EPUB: {out_epub}")
            # 同步到 Google Sheets（仅写客观字段，不覆盖 score/tags/notes/status）
            try:
                sync_rows_to_google_sheet(cfg, rows, run_date=run_date)
            except Exception as e:
                print(f"[sheets] 同步失败（忽略，不影响主流程）：{e}")
            # 成功生成日报后推进 last_generated_iso：确保“已经出现在报表里的公司”不会在第二天重复出现
            save_last_generated_at(end_at)
        else:
            if not Path(out_pdf).exists():
                print(f"未找到PDF文件: {out_pdf}，无法仅发送邮件")
                out_epub = None
                return

    enabled = cfg.get("email", "enabled", fallback="false").lower() in ("1", "true", "yes", "y")
    if args.no_email:
        enabled = False

    if enabled:
        to_raw = cfg.get("email", "to", fallback="")
        # 支持多个收件人：逗号/分号/空格分隔
        to_list = []
        for part in to_raw.replace(";", ",").replace(" ", ",").split(","):
            part = part.strip()
            if part:
                to_list.append(part)

        if not to_list:
            print("邮箱发送已启用，但未配置 email.to，跳过发送")
            return

        to_addr = ", ".join(to_list)

        subject = f"年报摘录汇总 {range_label}"
        body = f"附件为 {range_label} 披露年报的摘录汇总。"
        send_email_with_attachment_smtp(cfg, to_addr, subject, body, out_pdf, out_epub)
        print(f"已发送邮件到: {to_addr}")

        if not manual_publish_date:
            save_last_sent_at(end_at)
    else:
        print("邮件发送未启用（email.enabled=false 或使用了 --no-email）")


if __name__ == "__main__":
    main()

"""
config.ini email example:

[email]
enabled = true
host = smtp.gmail.com
port = 587
user = your_email@gmail.com
pass = your_app_password_16_chars_no_spaces
from = your_email@gmail.com
to = a@example.com,b@example.com
use_ssl = false
timeout = 30

Tip: Gmail requires an App Password (enable 2FA first).
"""
