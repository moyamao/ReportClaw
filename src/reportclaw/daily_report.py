"""
ReportClaw - 每日年报摘录汇总（PDF 生成 + 邮件发送）

作用
- 从 MySQL（annual_reports + annual_report_mda）读取“新增入库”的年报摘录（以 annual_report_mda.created_at 为准）。
- 将每个标的的摘要按固定版式渲染为一个汇总 PDF（默认输出到 data/report/）。
- 可选通过 SMTP 发送邮件（支持多收件人）。

增量逻辑（不漏发）
- 以 annual_report_mda.created_at 做增量边界：
    m.created_at ∈ (last_sent_at, now]
- 状态文件：data/state/last_sent.json，记录 last_sent_at（精确到秒）。
- 这样即使你昨天上午发过一次，昨天下午/晚上新入库的年报也会在今天再次发送，不会漏。

配置（conf/config.ini）
- [mysql] 必填：host/port/user/pass/db
- [email] 可选：
    enabled=true/false
    host, port, use_ssl, timeout
    user, pass, from, to
  说明：to 支持多个收件人，逗号/分号/空格分隔。

用法
1) 默认增量（推荐）：按 created_at 从 last_sent_at 到 now 生成 PDF 并按配置发送
    python src/reportclaw/daily_report.py

2) 只生成不发邮件：
    python src/reportclaw/daily_report.py --no-email

3) 仅发送邮件（假设 PDF 已生成）：
    python src/reportclaw/daily_report.py --only-email

4) 手工指定某个披露日（publish_date）生成（不影响 last_sent_at）：
    python src/reportclaw/daily_report.py --date YYYY-MM-DD

5) 忽略 last_sent_at，仅取今天 00:00 到现在的入库记录：
    python src/reportclaw/daily_report.py --today-only

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
from xml.sax.saxutils import escape

import mysql.connector

from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Preformatted, HRFlowable
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont


from pathlib import Path

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


def _state_path() -> Path:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    return STATE_DIR / "last_sent.json"


def load_last_sent_at() -> dt.datetime | None:
    sp = _state_path()
    if not sp.exists():
        return None
    try:
        data = json.loads(sp.read_text(encoding="utf-8"))
        v = data.get("last_sent_at")
        if not v:
            return None
        return _parse_dt(v)
    except Exception:
        return None


def save_last_sent_at(ts: dt.datetime):
    sp = _state_path()
    sp.parent.mkdir(parents=True, exist_ok=True)
    sp.write_text(
        json.dumps({"last_sent_at": ts.strftime("%Y-%m-%d %H:%M:%S")}, ensure_ascii=False),
        encoding="utf-8",
    )


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
          m.industry_section, m.main_business_section, m.future_section
        FROM annual_reports r
        JOIN annual_report_mda m ON m.report_id = r.id
        WHERE r.publish_date = %s
        ORDER BY r.stock_code
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
          m.industry_section, m.main_business_section, m.future_section
        FROM annual_reports r
        JOIN annual_report_mda m ON m.report_id = r.id
        WHERE r.publish_date >= %s AND r.publish_date <= %s
        ORDER BY r.publish_date, r.stock_code
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
          m.industry_section, m.main_business_section, m.future_section,
          m.created_at
        FROM annual_reports r
        JOIN annual_report_mda m ON m.report_id = r.id
        WHERE m.created_at > %s AND m.created_at <= %s
        ORDER BY m.created_at, r.stock_code
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

    def safe_block(text: str):
        if not text:
            return Paragraph("（未提取到内容）", body)

        def clean_text_for_pdf(t: str) -> str:
            """
            清洗年报摘录文本，改善 PDF 排版：
            - 去掉常见页眉页脚（公司名+年度报告、页码、x/y页码）
            - 合并“表格被抽取成一列一行”的短行，避免竖排一堆单词/短语
            - 对超长 token 进行软断行，避免 XPreformatted 右侧溢出
            - 重新排版为自然段，合并 PDF 抽取导致的人工换行
            """
            if not t:
                return t
            lines = []
            raw_lines = t.splitlines()

            def is_header_footer(line: str) -> bool:
                s = line.strip()
                if not s:
                    return True
                # 纯页码 / x/y 页码
                if s.isdigit():
                    return True
                if "/" in s and all(part.strip().isdigit() for part in s.split("/") if part.strip()):
                    # e.g. 14/248
                    if len(s) <= 15:
                        return True
                # 公司名 + 年度报告（页眉常见）
                if ("年度报告" in s) and ("股份有限公司" in s):
                    return True
                if s.startswith("公司代码：") or s.startswith("公司简称：") or ("公司代码：" in s):
                    return True
                # 仅公司名一行的页眉（常见：XXX股份有限公司 / XXX有限公司）
                if (s.endswith("股份有限公司") or s.endswith("有限公司")) and len(s) <= 30:
                    return True
                # 仅“XXXX年年度报告/年度报告全文/年度报告”一行的页眉（常与公司名分成两行）
                if ("年度报告" in s) and len(s) <= 25:
                    return True
                return False

            # 先做页眉页脚过滤 + 基础规整
            tmp = []
            for ln in raw_lines:
                s = ln.replace("\u00a0", " ").strip()
                if is_header_footer(s):
                    continue
                tmp.append(s)

            # 对超长token做软断行，避免XPreformatted右溢
            def soften_long_tokens(line: str) -> str:
                # Insert zero-width spaces into very long alnum/url-like tokens so ReportLab can wrap them.
                def _soften_token(tok: str) -> str:
                    if len(tok) < 25:
                        return tok
                    # only soften url/identifier-like tokens
                    if re.fullmatch(r"[A-Za-z0-9_\\./-]+", tok):
                        step = 20
                        return "\u200b".join(tok[i:i+step] for i in range(0, len(tok), step))
                    return tok

                parts = re.split(r"(\s+)", line)
                for i in range(0, len(parts), 2):
                    parts[i] = _soften_token(parts[i])
                return "".join(parts)

            tmp = [soften_long_tokens(x) for x in tmp]

            # 合并连续短行：常见于表格抽取（列名/单元格变成一行一个词）
            merged = []
            buf = []
            def flush_buf():
                nonlocal buf
                if buf:
                    merged.append("  ".join(buf).strip())
                    buf = []

            for s in tmp:
                # “短行”判定：长度很短且不以句号/分号等结束（更像表格单元格）
                is_short = (len(s) <= 6) and (not re.search(r"[。；;：:]$", s))
                if is_short:
                    buf.append(s)
                    # 防止无限累积：到一定数量就先输出
                    if len(buf) >= 12:
                        flush_buf()
                    continue
                else:
                    flush_buf()
                    merged.append(s)

            flush_buf()

            # 再做一次：去掉重复的空行效果（merged 已无空行，但保险）
            for s in merged:
                if not s:
                    continue
                lines.append(s)

            # —— 重新排版：把“PDF 抽取导致的一行一断”尽量合并成自然段 ——
            # 规则：标题/序号行单独成段；普通行若不以句末标点结尾则与下一行拼接。
            heading_re = re.compile(
                r"^(第[一二三四五六七八九十0-9]{1,3}[节章节]|"
                r"[一二三四五六七八九十]{1,3}、|"
                r"\d{1,2}、|"
                r"[（(][一二三四五六七八九十0-9]{1,3}[）)]|"
                r"\([一二三四五六七八九十0-9]{1,3}\))"
            )
            end_punct_re = re.compile(r"[。！？；：:）)」』】]$")

            paras: list[str] = []
            buf = ""

            def flush_buf():
                nonlocal buf
                if buf.strip():
                    paras.append(buf.strip())
                buf = ""

            for s in lines:
                s = s.strip()
                if not s:
                    flush_buf()
                    continue

                if heading_re.match(s):
                    flush_buf()
                    # 小标题“上方”留一行空白（段落间距更自然），但避免开头就是空行
                    if paras and paras[-1] != "":
                        paras.append("")
                    paras.append(s)
                    continue

                if not buf:
                    buf = s
                else:
                    # 若上一段已是句末，另起一段；否则拼接（不加空格，适合中文）
                    if end_punct_re.search(buf):
                        flush_buf()
                        buf = s
                    else:
                        buf += s

            flush_buf()
            # 去掉末尾多余空行
            while paras and paras[-1] == "":
                paras.pop()
            # 用单个换行作为段落/标题分隔（标题后已手动插入一行空白）
            return "\n".join(paras).strip()

        cleaned = clean_text_for_pdf(text)

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

        # header 尽量控制在一行：不展示入库时间；文件名过长则截断
        if pdf_name and len(pdf_name) > 42:
            pdf_name = pdf_name[:39] + "..."

        header = (
            f"{r.get('stock_code','')} {r.get('stock_name','')} | {r.get('report_year','')}年年报"
            f" | 公告 {r.get('publish_date','')}"
        )
        if pdf_name:
            header += f" | 文件 {pdf_name}"

        story.append(Paragraph(header, stock_header))
        story.append(HRFlowable(width="100%", thickness=0.8, spaceBefore=2, spaceAfter=6))

        story.append(Paragraph("管理层综述（摘录）", section_header))
        story.append(safe_block(r.get("main_business_section")))
        story.append(Spacer(1, 2 * mm))
        story.append(Preformatted("-" * 60, pre))
        story.append(Spacer(1, 2 * mm))

        story.append(Paragraph("未来展望（摘录）", section_header))
        story.append(safe_block(r.get("future_section")))

        if i < len(rows) - 1:
            # Big visual separation between tickers
            story.append(Spacer(1, 8 * mm))
            story.append(HRFlowable(width="100%", thickness=1.2, spaceBefore=2, spaceAfter=2))
            story.append(Spacer(1, 8 * mm))
            story.append(PageBreak())

    doc.build(story)
    return out_path


def send_email_with_attachment_smtp(cfg: configparser.ConfigParser, to_addr: str, subject: str, body: str, attachment_path: str):
    """
    通过 SMTP 发送带 PDF 附件的邮件。

    注意
    - cfg 来自 conf/config.ini 的 [email] 段
    - to_addr 可为逗号分隔的多个收件人
    - use_ssl=true 使用 SMTP_SSL；否则使用 STARTTLS
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

    if not host or not user or not password or not from_addr:
        raise RuntimeError("email config incomplete: need host/user/pass/from")

    msg = EmailMessage()
    msg["From"] = from_addr
    msg["To"] = to_addr  # may be comma-separated
    msg["Subject"] = subject
    msg.set_content(body)

    ap = Path(attachment_path)
    data = ap.read_bytes()
    msg.add_attachment(data, maintype="application", subtype="pdf", filename=ap.name)

    try:
        if use_ssl:
            with smtplib.SMTP_SSL(host, port, timeout=timeout_sec) as s:
                s.login(user, password)
                s.send_message(msg)
        else:
            with smtplib.SMTP(host, port, timeout=timeout_sec) as s:
                s.ehlo()
                s.starttls()
                s.ehlo()
                s.login(user, password)
                s.send_message(msg)
    except (TimeoutError, OSError) as e:
        raise RuntimeError(
            f"SMTP 连接失败（{host}:{port}），多半是网络/防火墙阻断或端口不可达。"
            f"建议：改用 Gmail 587+STARTTLS（config.ini: port=587,use_ssl=false），并在终端测试端口："
            f"nc -vz {host} 465 && nc -vz {host} 587。原始错误: {e}"
        ) from e
    except smtplib.SMTPAuthenticationError as e:
        raise RuntimeError(
            "SMTP 认证失败：Gmail 必须使用“应用专用密码”(App Password)，不能用网页登录密码。"
            "同时确保已开启两步验证(2FA)。原始错误: "
            + str(e)
        ) from e


def parse_args():
    p = argparse.ArgumentParser(description="Generate daily annual report excerpt PDF and optionally email it.")
    p.add_argument("--date", default=None, help="publish_date in YYYY-MM-DD. Default: today.")
    p.add_argument("--no-email", action="store_true", help="Do not send email even if enabled in config.")
    p.add_argument("--only-email", action="store_true", help="Only send email (assumes PDF already generated).")
    p.add_argument("--config", default=str(CONF_DIR / "config.ini"), help="Path to config.ini (default: conf/config.ini)")
    p.add_argument("--today-only", action="store_true", help="Force sending only today's created_at (ignore last_sent history).")
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
        out_pdf = str(DAILY_DIR / f"annual_report_summary_{day}.pdf")

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
        else:
            if not Path(out_pdf).exists():
                print(f"未找到PDF文件: {out_pdf}，无法仅发送邮件")
                return

        # 手工模式不更新 last_sent_at（避免影响增量逻辑）
    else:
        # 模式2：按入库时间增量（m.created_at）
        last_sent_at = load_last_sent_at()

        if args.today_only or last_sent_at is None:
            start_at = dt.datetime.combine(dt.date.today(), dt.time(0, 0, 0))
        else:
            start_at = last_sent_at

        end_at = now

        start_ts = start_at.strftime("%Y-%m-%d %H:%M:%S")
        end_ts = end_at.strftime("%Y-%m-%d %H:%M:%S")

        range_label = f"{start_ts} ~ {end_ts}"
        DAILY_DIR.mkdir(parents=True, exist_ok=True)
        out_pdf = str(DAILY_DIR / f"annual_report_summary_{dt.date.today().strftime('%Y-%m-%d')}.pdf")

        if not args.only_email:
            conn = mysql_connect(cfg)
            try:
                rows = fetch_rows_by_created_at_range(conn, start_ts, end_ts)
            finally:
                conn.close()

            if not rows:
                print(f"{range_label} 无新增入库年报记录，不生成汇总PDF")
                # 即使没有新增，也把 last_sent_at 推进到 now，避免下次重复扫描同一窗口
                save_last_sent_at(end_at)
                return

            generate_daily_summary_pdf(rows, out_pdf, range_label)
            print(f"已生成每日汇总PDF: {out_pdf}")
        else:
            if not Path(out_pdf).exists():
                print(f"未找到PDF文件: {out_pdf}，无法仅发送邮件")
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
        send_email_with_attachment_smtp(cfg, to_addr, subject, body, out_pdf)
        print(f"已发送邮件到: {to_addr}")

        if not manual_publish_date:
            save_last_sent_at(end_at)
    else:
        print("邮件发送未启用（email.enabled=false 或使用了 --no-email）")
        if not manual_publish_date:
            save_last_sent_at(end_at)


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
