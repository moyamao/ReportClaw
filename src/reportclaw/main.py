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
"""
import os
import re
import time
import requests
import pdfplumber
from pdfminer.high_level import extract_text as pdfminer_extract_text
import configparser
import mysql.connector
from datetime import datetime, timedelta
import traceback
import json

from pathlib import Path

# main.py 位于 src/reportclaw/ 下，所以项目根目录是再向上两级
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONF_DIR = PROJECT_ROOT / "conf"
DATA_DIR = PROJECT_ROOT / "data"
DOWNLOADS_DIR = DATA_DIR / "downloads"
DAILY_DIR = DATA_DIR / "report"
STATE_DIR = DATA_DIR / "state"


# ===============================
# 增量抓取状态（避免重复爬取）
# ===============================
LAST_CRAWL_STATE_FILE = STATE_DIR / "last_sent.json"


def load_last_crawl_ts(path: Path) -> datetime | None:
    """Load last crawl end time from shared json state file.

    Shared schema (one file):
      - last_sent_iso: used by daily_report
      - last_crawl_end_iso: used by main crawler

    Backward compatible:
      - legacy key: last_end_iso
    """
    try:
        if not path.exists():
            return None
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        if not isinstance(obj, dict):
            return None

        s = obj.get("last_crawl_end_iso") or obj.get("last_end_iso")
        if not s:
            return None

        if isinstance(s, (int, float)):
            return datetime.fromtimestamp(float(s))

        s = str(s).strip()
        if len(s) == 10:
            return datetime.strptime(s, "%Y-%m-%d")
        return datetime.fromisoformat(s)
    except Exception:
        return None


def save_last_crawl_ts(path: Path, dt: datetime) -> None:
    """Persist last crawl end time into shared json state file (do not overwrite other keys)."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        obj = {}
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    old = json.load(f)
                if isinstance(old, dict):
                    obj.update(old)
            except Exception:
                obj = {}

        obj["last_crawl_end_iso"] = dt.isoformat(timespec="seconds")

        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


# ===============================
# 数据库客户端
# ===============================

class MySQLClient:
    """
    MySQL 访问封装（最小职责）
    - 读取 conf/config.ini 的 [mysql] 段建立连接
    - 提供年报入库所需的基础操作：
        * exists(stock_code, year): 判断同公司同年份是否已入库
        * insert_report(...): 写 annual_reports，返回 report_id
        * insert_mda(report_id, mda): 写 annual_report_mda

    约定
    - annual_reports 以 (stock_code, report_year) 作为逻辑唯一键（代码层面去重）。
      如需更强一致性，建议在 DB 上加唯一索引 uq_stock_year(stock_code, report_year)。
    """

    def __init__(self):
        config = configparser.ConfigParser()
        config.read(CONF_DIR / "config.ini", encoding="utf-8")
        if not config.has_section("mysql"):
            raise RuntimeError(
                f"config.ini 未读取到 [mysql] 段。请检查路径是否存在：{CONF_DIR / 'config.ini'}"
            )

        self.conn = mysql.connector.connect(
            host=config["mysql"]["host"],
            port=config.getint("mysql", "port"),
            user=config["mysql"]["user"],
            password=config["mysql"]["pass"],
            database=config["mysql"]["db"]
        )

    def get_report_id(self, stock_code, year):
        """Return existing annual_reports.id if present, else None."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT id FROM annual_reports WHERE stock_code=%s AND report_year=%s",
            (stock_code, year)
        )
        row = cursor.fetchone()
        return row[0] if row else None

    def is_mda_complete(self, report_id: int) -> bool:
        """Treat placeholder/failed parses as NOT complete so the pipeline can retry on later runs."""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT industry_section, main_business_section, future_section, full_mda
            FROM annual_report_mda
            WHERE report_id=%s
            LIMIT 1
            """,
            (report_id,)
        )
        row = cursor.fetchone()
        if not row:
            return False

        ind, biz, fut, full = row

        # If we stored a failure sentinel, allow retry.
        if isinstance(full, str) and full.startswith("[PARSE_FAILED]"):
            return False

        # Otherwise: consider complete if we have any meaningful extracted section.
        if (biz and len(biz) >= 500) or (fut and len(fut) >= 200) or (ind and len(ind) >= 200):
            return True

        # If only full_mda exists but is tiny, treat as incomplete.
        if full and isinstance(full, str) and len(full) >= 5000:
            return True

        return False

    def upsert_report(self, stock_code, stock_name, year, publish_date, file_path):
        """Insert annual_reports if missing; otherwise update basic fields and return report_id."""
        existing_id = self.get_report_id(stock_code, year)
        cursor = self.conn.cursor()
        if existing_id is None:
            sql = """
            INSERT INTO annual_reports
            (stock_code, stock_name, report_year, publish_date, file_path)
            VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (stock_code, stock_name, year, publish_date, file_path))
            self.conn.commit()
            return cursor.lastrowid

        # Update metadata in case file path / name changed (e.g.,修订版)
        sql = """
        UPDATE annual_reports
        SET stock_name=%s, publish_date=%s, file_path=%s
        WHERE id=%s
        """
        cursor.execute(sql, (stock_name, publish_date, file_path, existing_id))
        self.conn.commit()
        return existing_id

    def insert_report(self, stock_code, stock_name, year, publish_date, file_path):
        cursor = self.conn.cursor()
        sql = """
        INSERT INTO annual_reports
        (stock_code, stock_name, report_year, publish_date, file_path)
        VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(sql, (stock_code, stock_name, year, publish_date, file_path))
        self.conn.commit()
        return cursor.lastrowid

    def insert_mda(self, report_id, mda):
        cursor = self.conn.cursor()
        # If row exists, update; else insert.
        cursor.execute("SELECT id FROM annual_report_mda WHERE report_id=%s LIMIT 1", (report_id,))
        row = cursor.fetchone()
        if row:
            sql = """
            UPDATE annual_report_mda
            SET industry_section=%s,
                main_business_section=%s,
                future_section=%s,
                full_mda=%s
            WHERE report_id=%s
            """
            cursor.execute(sql, (
                mda.get("industry"),
                mda.get("business"),
                mda.get("future"),
                mda.get("full_mda"),
                report_id
            ))
        else:
            sql = """
            INSERT INTO annual_report_mda
            (report_id, industry_section, main_business_section, future_section, full_mda)
            VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                report_id,
                mda.get("industry"),
                mda.get("business"),
                mda.get("future"),
                mda.get("full_mda"),
            ))
        self.conn.commit()


# ===============================
# PDF解析器
# ===============================
class AnnualReportParser:
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

    def _extract_between_markers(self, text: str, start_pat: str, end_pats: list[str], *, flags=re.MULTILINE):
        """Extract substring starting at start_pat until earliest match of any end_pats."""
        m_start = re.search(start_pat, text, flags)
        if not m_start:
            return None
        start = m_start.start()

        end = None
        tail = text[m_start.end():]
        for ep in end_pats:
            m_end = re.search(ep, tail, flags)
            if m_end:
                cand = m_start.end() + m_end.start()
                end = cand if end is None else min(end, cand)

        if end is None:
            end = len(text)
        return text[start:end].strip()


    def _truncate(self, s: str | None, max_len: int) -> str | None:
        if not s:
            return s
        if len(s) <= max_len:
            return s
        return s[:max_len].rstrip() + "\n\n[...TRUNCATED...]"


    def extract_alt_sections(self, pdf_path: str, *, max_pages: int = 260) -> dict | None:
        """Fallback extractor for non-standard annual reports without '第三节 管理层讨论与分析'.

        Strategy:
        - Drop boilerplates: 重要提示 / 目录 / 释义 / 词汇表
        - Prefer extracting from: 董事长致辞 + 第二章 董事会报告
        - Outlook: prefer numeric heading '2.3.1 2026年业务展望', otherwise keyword '2026年业务展望/业务展望'
        """
        try:
            raw = self.extract_text(pdf_path, page_numbers=list(range(0, max_pages)))
        except Exception:
            raw = ""
        if not raw:
            return None

        t = raw

        # --- helper: drop common boilerplate blocks ---
        def _drop_block(src: str, start_pat: str, end_pats: list[str]) -> str:
            blk = self._extract_between_markers(src, start_pat, end_pats)
            return src.replace(blk, "") if blk else src

        # 目录/释义/词汇表 经常会非常长，优先剔除
        t = _drop_block(t, r"(?:^|\n)目\s*录\b", [r"(?:^|\n)第一[章节]\b", r"(?:^|\n)第一节\b"])
        t = _drop_block(t, r"(?:^|\n)释\s*义\b", [r"(?:^|\n)(?:词\s*汇\s*表|第一[章节]|第一节)\b"])
        t = _drop_block(t, r"(?:^|\n)词\s*汇\s*表\b", [r"(?:^|\n)(?:第一[章节]|第一节)\b"])

        # 对于某些模板：重要提示会出现在最前面，直接截到更有信息密度的起点
        # 优先：管理层综述/董事长致辞/董事会报告
        for anchor in [
            r"(?:^|\n)董事长致辞\b",
            r"(?:^|\n)管理层综述\b",
            r"(?:^|\n)第二章\s*董事会报告\b",
            r"(?:^|\n)(?:董事会报告|董事会工作报告|董事会报告书)\b",
        ]:
            m = re.search(anchor, t)
            if m:
                t = t[m.start():]
                break

        # --- 1) Summary / Overview: combine multiple useful sections ---
        # A) 管理层综述（若存在，优先）
        overview = self._extract_between_markers(
            t,
            r"(?:^|\n)管理层综述\b",
            [
                r"(?:^|\n)董事长致辞\b",
                r"(?:^|\n)第二章\s*董事会报告\b",
                r"(?:^|\n)董事会报告\b",
                r"(?:^|\n)第三节\s*管理层讨论与分析\b",
                r"(?:^|\n)公司治理\b",
                r"(?:^|\n)重要事项\b",
                r"(?:^|\n)(?:[一二三四五六七八九十]{1,3}|\d{1,2})[、\.．:：]\s*报告期内核心竞争力分析\b",
            ],
        )

        # B) 董事长致辞（若存在）
        chairman = self._extract_between_markers(
            t,
            r"(?:^|\n)董事长致辞\b",
            [
                r"(?:^|\n)第二章\s*董事会报告\b",
                r"(?:^|\n)董事会报告\b",
                r"(?:^|\n)第三章\b",
                r"(?:^|\n)第三节\s*管理层讨论与分析\b",
                r"(?:^|\n)公司治理\b",
                r"(?:^|\n)重要事项\b",
            ],
        )

        # C) 董事会报告（不同模板写法差异大）
        board = self._extract_between_markers(
            t,
            r"(?:^|\n)(?:第二章\s*)?(?:董事会报告|董事会工作报告|董事会报告书)\b",
            [
                r"(?:^|\n)第三章\b",
                r"(?:^|\n)第三节\s*管理层讨论与分析\b",
                r"(?:^|\n)公司治理\b",
                r"(?:^|\n)重要事项\b",
            ],
        )

        # Combine and de-duplicate (avoid repeating identical blocks)
        summary_parts: list[str] = []
        for part in [overview, chairman, board]:
            if not part:
                continue
            p = part.strip()
            if not p:
                continue
            # skip if largely contained in previous parts
            if any((p in x) or (x in p) for x in summary_parts):
                continue
            summary_parts.append(p)

        summary_text = "\n\n".join(summary_parts).strip() if summary_parts else None

        # --- 2) Outlook: keyword-first fallback (do NOT rely on ordinal numbers) ---
        # Some annual reports (e.g., ZTE) use a different structure and do not have the standard
        # “十一、公司未来发展的展望”. In that case, we treat “业务展望/2026年业务展望/2026年业务发展展望”
        # as the primary signal and slice until the next major heading.
        outlook = None

        # 2.1 Prefer the most explicit heading form first:
        # e.g. "2.3 2026年业务展望和面对的经营风险" / "2．3 2026年业务展望…"
        # NOTE: normalize() removed normal spaces, but may keep newlines; be tolerant.
        m_head = re.search(
            r"(?:^|\n)\s*2\s*[\.．]\s*3(?:\s*[\.．]\s*\d+)?\s*[^\n]{0,80}?(?:2026\s*年?)?\s*业务展望[^\n]{0,120}?(?:经营风险|风险)?\b",
            t,
        )

        out_kw = None
        out_idx = None
        if m_head:
            out_idx = m_head.start()
            out_kw = "业务展望"
        else:
            # 2.2 Fallback: search by keyword only (works when numbering is missing)
            for kw in ["2026年业务展望", "2026年业务发展展望", "业务展望"]:
                idx = t.find(kw)
                if idx != -1:
                    out_kw = kw
                    out_idx = idx
                    break

        if out_idx is not None:
            # Start from the beginning of the line that contains the keyword.
            line_start = t.rfind("\n", 0, out_idx)
            start = 0 if line_start < 0 else (line_start + 1)

            tail = t[out_idx:]

            # End at the next major heading.
            # Be tolerant: extracted text sometimes loses newlines around headings.
            end_patterns = [
                # Next sibling subsection (e.g. 2.3.2 / 2.3.3 ...) or next major section (2.4 / 3.)
                r"(?:\n\s*|\s)2\s*[\.．]\s*3\s*[\.．]\s*[2-9]",
                r"(?:\n\s*|\s)2\s*[\.．]\s*4\b",
                r"(?:\n\s*|\s)3\s*[\.．]\s*\d",

                # Also accept heading without explicit whitespace/newline before it (rare but happens)
                r"2\s*[\.．]\s*4\b",
                r"3\s*[\.．]\s*\d\b",

                # Next Chinese/Arabic major ordinal like 十二、/12、
                r"(?:\n)\s*(?:[一二三四五六七八九十]{1,3}|\d{1,2})[、\.．:：]",

                # Next chapter/section style
                r"(?:\n)\s*第\s*[一二三四五六七八九十]{1,3}\s*[章节]",

                # Strong stop words (chapters we never want inside outlook)
                r"(?:\n)\s*(?:十\s*二|12)\s*[、\.．:：]?\s*报告期内接待调研",
                r"(?:\n)\s*(?:十\s*三|13)\s*[、\.．:：]?\s*市值管理",
                r"(?:\n)\s*(?:目\s*录|释\s*义|词\s*汇\s*表)",
                r"(?:\n)\s*公司治理",
                r"(?:\n)\s*重要事项",
                r"(?:\n)\s*(?:可能面对的风险|风险因素|风险提示)",
            ]

            end = None
            for ep in end_patterns:
                m_end = re.search(ep, tail)
                if not m_end:
                    continue
                cand = out_idx + m_end.start()
                # Avoid cutting too early (must be meaningfully after the start)
                if cand <= start + 50:
                    continue
                end = cand if end is None else min(end, cand)

            if end is None:
                end = len(t)

            outlook = t[start:end].strip()

        # As a hard safety: if outlook accidentally contains '报告期内接待调研/市值管理', cut it.
        if outlook:
            leak_pats = [
                r"(?:\n)\s*(?:十\s*二|12)\s*[、\.．:：]?\s*报告期内接待调研",
                r"(?:\n)\s*(?:十\s*三|13)\s*[、\.．:：]?\s*市值管理",
            ]
            cut_at = None
            for pat in leak_pats:
                m = re.search(pat, outlook)
                if m:
                    cut_at = m.start() if cut_at is None else min(cut_at, m.start())
            if cut_at is not None:
                outlook = outlook[:cut_at].strip()

        # Final truncation bounds
        if summary_text:
            summary_text = self._truncate(summary_text, 70000)
        if outlook:
            outlook = self._truncate(outlook, 40000)

        # If we still got nothing meaningful, give up.
        if not summary_text and not outlook:
            return None

        full_parts = []
        if summary_text:
            full_parts.append(summary_text)
        if outlook:
            full_parts.append(outlook)
        full = "\n\n".join(full_parts)
        full = self._truncate(full, 140000)

        return {
            "industry": None,
            "business": summary_text,
            "future": outlook,
            "full_mda": full,
        }

    def extract_text(self, pdf_path, page_numbers=None):
        # 使用 pdfminer 按页提取（支持只读指定页）
        text = pdfminer_extract_text(pdf_path, page_numbers=page_numbers)
        if not text:
            return ""
        return self.normalize(text)

    def build_fallback_mda(self, pdf_path: str, reason: str, page_count: int) -> dict:
        """Fallback payload for DB so the report is not ignored even if MDA parsing fails.

        IMPORTANT:
        - Do NOT dump front-matter (重要提示/目录/释义) into summaries.
        - If parsing fails, prefer telling the user to read the original PDF.
        """
        excerpt = ""
        try:
            probe_pages = list(range(0, min(40, max(int(page_count or 0), 1))))
            probe = self.extract_text(pdf_path, page_numbers=probe_pages) or ""
            t = probe

            # drop blocks helper
            def _drop_block(src: str, start_pat: str, end_pats: list[str]) -> str:
                blk = self._extract_between_markers(src, start_pat, end_pats)
                return src.replace(blk, "") if blk else src

            # remove “重要提示/目录/释义/词汇表”
            t = _drop_block(
                t,
                r"(?:^|\n)重\s*要\s*提\s*示\b",
                [
                    r"(?:^|\n)目\s*录\b",
                    r"(?:^|\n)释\s*义\b",
                    r"(?:^|\n)词\s*汇\s*表\b",
                    r"(?:^|\n)第[一二三四五六七八九十]+章\b",
                    r"(?:^|\n)第一[章节]\b",
                    r"(?:^|\n)第二[章节]\b",
                    r"(?:^|\n)第三[章节]\b",
                ],
            )
            t = _drop_block(t, r"(?:^|\n)目\s*录\b", [r"(?:^|\n)第一[章节]\b", r"(?:^|\n)第一节\b"])
            t = _drop_block(t, r"(?:^|\n)释\s*义\b", [r"(?:^|\n)(?:词\s*汇\s*表|第一[章节]|第一节)\b"])
            t = _drop_block(t, r"(?:^|\n)词\s*汇\s*表\b", [r"(?:^|\n)(?:第一[章节]|第一节)\b"])

            # try to find a real-content anchor
            anchors = [
                r"(?:^|\n)第三章\b",
                r"(?:^|\n)第三节\s*管理层讨论与分析\b",
                r"(?:^|\n)第一节\s*公司业务概要\b",
                r"(?:^|\n)(?:董事会报告|董事会工作报告|董事会报告书)\b",
                r"(?:^|\n)管理层综述\b",
            ]
            cut = None
            for ap in anchors:
                m = re.search(ap, t)
                if m:
                    cut = m.start()
                    break
            if cut is not None:
                t = t[cut:].strip()

            # if still looks like front-matter, abandon
            if re.match(r"^(重\s*要\s*提\s*示|目\s*录|释\s*义|词\s*汇\s*表)\b", t):
                t = ""

            if t and len(t) >= 300:
                excerpt = t[:6000].rstrip()

        except Exception:
            excerpt = ""

        pdf_name = os.path.basename(pdf_path)
        base = f"[PARSE_FAILED] reason={reason} pages={page_count}\n请查看原PDF：{pdf_name}"

        # If we managed to get any real-content excerpt, surface it in `business`
        # so the daily report doesn't look empty. If we have no excerpt, keep it None.
        biz = None
        if excerpt:
            biz = "（解析失败：以下为正文片段，可能不完整；建议打开原PDF核对）\n\n" + excerpt

        full = base
        if biz:
            full = base + "\n\n" + biz

        return {
            "industry": None,
            "business": biz,
            "future": None,
            "full_mda": full,
        }

    def _slice_to_next_bracket_heading(self, text: str, start_idx: int):
        """
        从（X）/ (X) 这类括号小标题开始切，到下一条同级括号小标题或下一条“一级大标题（序号+、）”。
        """
        if start_idx is None or start_idx < 0:
            return None

        next_sub = None
        m_sub = re.search(r"(?:\n\s*[（(][一二三四五六七八九十0-9]{1,3}[）)])", text[start_idx + 1:])
        if m_sub:
            next_sub = start_idx + 1 + m_sub.start()

        next_major = None
        m_major = re.search(r"(?:\n\s*(?:[一二三四五六七八九十]{1,3}|\d{1,2})、)", text[start_idx + 1:])
        if m_major:
            next_major = start_idx + 1 + m_major.start()

        candidates = [p for p in (next_sub, next_major) if p is not None]
        end_idx = min(candidates) if candidates else len(text)
        return text[start_idx:end_idx].strip()

    def normalize(self, text):
        # 统一换行/去除不可见分页符
        text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\x0c", "\n")

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

        # 1) 直接正文扫描定位：不要依赖目录页码（目录/重要提示/释义常导致误判）
        # 目标：找到“管理层讨论与分析/经营情况讨论与分析”的真实正文起点页。
        start_page = None

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
            if not t:
                return False

            bad_words = ("详见", "敬请", "查阅", "参阅", "阅读", "提示")
            kw = ("管理层讨论与分析", "经营情况讨论与分析")

            for ln in t.split("\n"):
                s = (ln or "").strip()
                if not s:
                    continue

                # 引用句排雷：出现“详见/敬请查阅”等 + 关键词，直接否
                if any(w in s for w in bad_words) and any(k in s for k in kw):
                    continue

                # 引号/书名号排雷：大概率是“详见第三节…”那类引用
                if any(q in s for q in ('“', '”', '"', "'", '《', '》')) and any(k in s for k in kw):
                    # 但允许纯标题（很短）
                    if len(re.sub(r"\s+", "", s)) > 20:
                        continue

                s2 = re.sub(r"\s+", "", s)
                if len(s2) > 60:
                    continue

                # 只要是“标题式短行”包含关键词就接受
                if any(k in s2 for k in kw):
                    return True

            return False

        # 先粗扫：找标题页
        for p in range(front_matter_max_pages + 1, 220):
            try:
                page_text = self.extract_text(pdf_path, page_numbers=[p])
            except Exception:
                page_text = ""
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
            for p in range(front_matter_max_pages + 1, 260):
                try:
                    page_text = self.extract_text(pdf_path, page_numbers=[p])
                except Exception:
                    page_text = ""
                if not page_text:
                    continue
                if _is_front_matter_page(page_text):
                    continue
                if re.search(r"(?:^|\n)\s*一、报告期内公司从事的主要业务", page_text) or (
                        "报告期内公司从事的主要业务" in page_text):
                    start_page = p
                    break

        if start_page is None:
            # 非标准年报兜底
            alt = self.extract_alt_sections(pdf_path)
            if alt:
                return alt
            print("未找到管理层讨论与分析/经营情况讨论与分析起始位置（已跳过重要提示/目录/释义前置页）")
            return None

        # 进一步校准：若命中的是章节标题页，向后找真正正文锚点（最多 12 页）
        anchor_patterns = [
            r"管理层讨论[与和]分析",
            r"经营情况讨论[与和]分析",
            r"(?:^|\n)\s*一、报告期内公司从事的主要业务",
            r"报告期内公司从事的主要业务",
        ]
        calibrated = None
        for p in range(start_page, start_page + 12):
            try:
                t = self.extract_text(pdf_path, page_numbers=[p])
            except Exception:
                t = ""
            if not t:
                continue
            if _is_front_matter_page(t):
                continue
            if ("分季度主要财务指标" in t) or ("非经常性损益" in t) or ("非经常性损益项目及金额" in t):
                continue
            if any(re.search(pat, t) for pat in anchor_patterns):
                calibrated = p
                break
        if calibrated is not None:
            start_page = calibrated

        print("MDA正文起始页:", start_page)

        # 3) 从起始页开始读到“第三节结束/第四章开始”的边界
        # 注意：不同年报模板的“第四章”不一定写成“第四节”，常见形式：
        # - 04 公司治理、环境和社会（大号“04”页）
        # - 公司治理（无“第四节”字样）
        # - 直接出现后续章节：十二、报告期内接待调研… / 十三、市值管理… 等
        mda_text = ""
        for p in range(start_page, start_page + 200):
            page_text = self.extract_text(pdf_path, page_numbers=[p])
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

        return {
            "industry": industry,
            "business": business,
            "future": future,
            "full_mda": mda_text
        }

    def _slice_to_next_heading_with_title_keywords(self, text, start_idx, title_keywords):
        """
        从 start_idx 切到“下一条标题”，且该标题行的标题部分包含 title_keywords 之一。
        同时支持：
        - 一级标题：二、xxx / 2、xxx
        - 括号小标题：（三）xxx / (3)xxx
        """
        if start_idx is None or start_idx < 0:
            return None

        candidates = []

        # A) 一级标题：二、xxx
        for m in re.finditer(
            r"(?:^|\n)\s*([一二三四五六七八九十]{1,3}|\d{1,2})[、\.．:：]\s*([^\n]{1,80})",
            text[start_idx + 1:]
        ):
            title = m.group(2)
            if any(kw in title for kw in title_keywords):
                candidates.append(start_idx + 1 + m.start())
                break

        # B) 括号小标题：（三）xxx / (3)xxx
        for m in re.finditer(
            r"(?:^|\n)\s*[（(][一二三四五六七八九十0-9]{1,3}[）)]\s*([^\n]{1,80})",
            text[start_idx + 1:]
        ):
            title = m.group(1)
            if any(kw in title for kw in title_keywords):
                candidates.append(start_idx + 1 + m.start())
                break

        if not candidates:
            return None

        end_idx = min(candidates)
        return text[start_idx:end_idx].strip()

    def _slice_to_next_major_heading(self, text, start_idx):
        """
        从 start_idx 切到“下一条重大一级标题”（标题行包含 MAJOR_HEADING_KEYWORDS）。
        目的：避免把同一段里的城市/地区小标题（三、深圳…）当作章节结束。
        若找不到重大标题，则返回 None 让调用方自行兜底。
        """
        if start_idx is None or start_idx < 0:
            return None

        # 形如：三、核心竞争力分析 / 四、主营业务分析 / 十一、公司未来发展的展望
        heading_iter = re.finditer(
            r"(?:\n\s*([一二三四五六七八九十]{1,3}|\d{1,2})、([^\n]{1,60}))",
            text[start_idx + 1:]
        )

        for m in heading_iter:
            title = m.group(2)
            if any(kw in title for kw in self.MAJOR_HEADING_KEYWORDS):
                end_idx = start_idx + 1 + m.start()
                return text[start_idx:end_idx].strip()

        return None

    def _next_ordinal_candidates(self, current_ordinal: str):
        """
        给定当前一级序号（中文或阿拉伯），返回可能的“下一个一级序号”候选列表。
        例如：'二' -> ['三', '3']；'11' 或 '十一' -> ['十二', '12']。
        """
        cn_list = ["一","二","三","四","五","六","七","八","九","十",
                   "十一","十二","十三","十四","十五","十六","十七","十八","十九","二十"]
        cn_to_ar = {"一":"1","二":"2","三":"3","四":"4","五":"5","六":"6","七":"7","八":"8","九":"9","十":"10",
                    "十一":"11","十二":"12","十三":"13","十四":"14","十五":"15","十六":"16","十七":"17","十八":"18","十九":"19","二十":"20"}
        ar_to_cn = {v:k for k,v in cn_to_ar.items()}

        cur_cn = current_ordinal
        cur_ar = None

        # 标准化：如果输入是阿拉伯数字，转成中文索引
        if re.fullmatch(r"\d{1,2}", current_ordinal):
            cur_ar = current_ordinal
            cur_cn = ar_to_cn.get(current_ordinal)

        if cur_cn in cn_list:
            idx = cn_list.index(cur_cn)
            if idx + 1 < len(cn_list):
                nxt_cn = cn_list[idx + 1]
                nxt_ar = cn_to_ar.get(nxt_cn)
                return [nxt_cn, nxt_ar] if nxt_ar else [nxt_cn]

        # 找不到就不给候选
        return []

    def _slice_to_next_ordinal(self, text, start_idx, current_ordinal: str):
        """
        从 start_idx 开始切片到“下一一级序号”（例如 二、 -> 三、；十一、 -> 十二、）。
        这样不会被正文中的“一、/1、”等项目符号截断。
        """
        if start_idx is None or start_idx < 0:
            return None

        # 先尝试按“重大标题”结束，避免被城市/地区小标题截断
        major_slice = self._slice_to_next_major_heading(text, start_idx)
        if major_slice:
            return major_slice

        candidates = self._next_ordinal_candidates(current_ordinal)

        def _cand_regex(c: str) -> str:
            """Build a tolerant regex for next ordinal.
            - For Arabic numbers: exact match.
            - For Chinese numerals like '十二': allow optional whitespace/newlines between characters (e.g. '十\n二').
            """
            if re.fullmatch(r"\d{1,2}", c):
                return re.escape(c)
            # Allow whitespace/newlines between each Chinese numeral character
            return r"\\s*".join(re.escape(ch) for ch in c)

        end_positions = []
        for cand in candidates:
            if not cand:
                continue
            cand_pat = _cand_regex(cand)
            # IMPORTANT: Some PDFs may emit headings without a preceding newline in extracted text.
            # So we must match both start-of-string and newline.
            m = re.search(rf"(?:^|\n)\s*{cand_pat}\s*、", text[start_idx + 1:])
            if m:
                end_positions.append(start_idx + 1 + m.start())

        # Fallback: if the next-ordinal marker is present but formatting is weird,
        # try a more permissive scan for the next ordinal line.
        if not end_positions and candidates:
            for cand in candidates:
                if not cand:
                    continue
                cand_pat = _cand_regex(cand)
                m2 = re.search(rf"{cand_pat}\s*、", text[start_idx + 1:])
                if m2:
                    end_positions.append(start_idx + 1 + m2.start())

        end_idx = min(end_positions) if end_positions else len(text)
        return text[start_idx:end_idx].strip()

    def extract_section_by_ordinal(self, text, ordinal_cn, keyword_fallback=None):
        """
        按“同级序号”提取段落：
        - 起始优先匹配：二、 / 2、 （一级）
        - 仅当找不到一级时，才尝试： （二）/(二) 这类括号形式（有些报告一级也用括号）
        - 结束仅识别同级：一、二、三… / 1、2、3…（不把（ 一 ）当作结束）
        """
        cn_to_arabic = {"一": "1", "二": "2", "三": "3", "四": "4", "五": "5",
                        "六": "6", "七": "7", "八": "8", "九": "9", "十": "10",
                        "十一": "11", "十二": "12"}

        start_idx = None

        # 1) 一级：中文序号 + 顿号（最可靠，避免误命中（二）子标题）
        m1 = re.search(rf"(?:^|\n)\s*{ordinal_cn}、", text)
        if m1:
            start_idx = m1.start()
        else:
            # 2) 一级：阿拉伯数字 + 顿号
            arabic = cn_to_arabic.get(ordinal_cn)
            if arabic:
                m2 = re.search(rf"(?:^|\n)\s*{arabic}、", text)
                if m2:
                    start_idx = m2.start()

        # 3) 仍找不到：才尝试括号形式（有些报告一级标题可能写成（十一））
        if start_idx is None:
            m3 = re.search(rf"(?:^|\n)\s*(?:（{ordinal_cn}）|\({ordinal_cn}\))", text)
            if m3:
                start_idx = m3.start()
            else:
                arabic = cn_to_arabic.get(ordinal_cn)
                if arabic:
                    m4 = re.search(rf"(?:^|\n)\s*(?:（{arabic}）|\({arabic}\))", text)
                    if m4:
                        start_idx = m4.start()

        # 4) 关键词兜底
        if start_idx is None and keyword_fallback:
            for kw in keyword_fallback:
                mk = re.search(rf"(?:^|\n)\s*{kw}", text)
                if mk:
                    start_idx = mk.start()
                    break

        return self._slice_to_next_ordinal(text, start_idx, ordinal_cn)

    def extract_section_by_keywords(self, text, keywords, fallback_ordinals=None, end_title_keywords=None):
        """
        通过“一级标题行（序号 + 顿号）+ 关键词”定位并提取整段，并按该序号切到下一序号。
        关键：结束边界是“下一一级序号”，避免被正文里的项目符号截断。
        """
        for kw in keywords:
            # Support regex keywords by prefix: REGEX:<pattern>
            if isinstance(kw, str) and kw.startswith("REGEX:"):
                kw_pat = kw[len("REGEX:"):]
            else:
                kw_pat = re.escape(str(kw))

            m = re.search(
                rf"(?:^|\n)\s*([一二三四五六七八九十]{{1,3}}|\d{{1,2}})[、\.．:：]\s*[^\n]*{kw_pat}[^\n]*",
                text
            )
            if m:
                start = m.start()
                ordinal = m.group(1)

                # If end_title_keywords is provided: only stop at a major heading whose title contains any of these keywords.
                # If not found, fall back to slicing by next same-level ordinal.
                if end_title_keywords:
                    sliced = self._slice_to_next_heading_with_title_keywords(text, start, end_title_keywords)
                    if sliced:
                        return sliced
                    return self._slice_to_next_ordinal(text, start, ordinal)

                return self._slice_to_next_ordinal(text, start, ordinal)

        # 兼容“2.3.1 2026年业务展望/2．3．1 …”这类点分数字标题（部分非标年报，如中兴通讯）
        for kw in keywords:
            if isinstance(kw, str) and kw.startswith("REGEX:"):
                kw_pat = kw[len("REGEX:"):]
            else:
                kw_pat = re.escape(str(kw))

            # 形如：2.3.1 2026年业务展望 / 2．3．1 2026年业务展望
            m = re.search(
                rf"(?:^|\n)\s*(\d+(?:[\.．]\d+){{1,3}})\s*[、\.．:：]?\s*[^\n]*{kw_pat}[^\n]*",
                text
            )
            if not m:
                continue

            start = m.start()

            # 若提供了 end_title_keywords：优先用“后续大章标题”来截断
            if end_title_keywords:
                sliced = self._slice_to_next_heading_with_title_keywords(text, start, end_title_keywords)
                if sliced:
                    return sliced

            # 否则/或未命中：用“下一个点分标题/一级序号标题/第X章(节)”中的最早者截断
            tail = text[start + 1:]
            end_candidates: list[int] = []

            # A) 下一个点分标题（任意 1~3 级，如 2.3.2 / 2.4 / 3.1）
            m_dot = re.search(r"(?:^|\n)\s*\d+(?:[\.．]\d+){1,3}\b", tail)
            if m_dot:
                end_candidates.append(start + 1 + m_dot.start())

            # B) 下一个一级中文/阿拉伯序号标题：二、 / 12、（注意不要把正文项目符号误判为大章；这里只作为兜底）
            m_ord = re.search(r"(?:^|\n)\s*(?:[一二三四五六七八九十]{1,3}|\d{1,2})[、\.．:：]", tail)
            if m_ord:
                end_candidates.append(start + 1 + m_ord.start())

            # C) 下一个“第X章/节”
            m_ch = re.search(r"(?:^|\n)\s*第\s*[一二三四五六七八九十]{1,3}\s*[章节]", tail)
            if m_ch:
                end_candidates.append(start + 1 + m_ch.start())

            end_idx = min(end_candidates) if end_candidates else len(text)
            return text[start:end_idx].strip()

        # 兼容括号小标题：如（三）所处行业情况 / (一) 主要业务 / （三）行业情况说明
        for kw in keywords:
            if isinstance(kw, str) and kw.startswith("REGEX:"):
                kw_pat = kw[len("REGEX:"):]
            else:
                kw_pat = re.escape(str(kw))

            m = re.search(
                rf"(?:^|\n)\s*[（(]([一二三四五六七八九十0-9]{{1,3}})[）)]\s*[^\n]*{kw_pat}[^\n]*",
                text
            )
            if m:
                start = m.start()
                sliced = self._slice_to_next_bracket_heading(text, start)
                if sliced:
                    return sliced
                return text[start:].strip()

        # 兜底：按候选序号（注意这会依赖报告结构，优先级放后面）
        if fallback_ordinals:
            for o in fallback_ordinals:
                sec = self.extract_section_by_ordinal(text, o)
                if sec:
                    return sec
        return None

    def extract_section(self, text, title):
        pattern = rf"{title}[\s\S]*?(?=\n[一二三四五六七八九十]+、|\Z)"
        match = re.search(pattern, text)
        return match.group(0).strip() if match else None


# ===============================
# 主逻辑
# ===============================
def main():
    """
    主流程（Orchestration）
    1) 读取 crawler 配置（days_back）
    2) 分别拉取深交所(szse)与上交所(sse)公告分页（只保留“年度报告全文”，排除摘要）
    3) 严格按时间窗口过滤（避免 seDate 失效导致拉到历史公告）
    4) 下载 PDF（若已存在则跳过下载），并做页数阈值过滤（<50 页视为非完整年报）
    5) 解析第三节管理层讨论与分析，入库 annual_reports + annual_report_mda
    """
    download_dir = str(DOWNLOADS_DIR)
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)

    db = MySQLClient()
    parser = AnnualReportParser()

    # 最近N天窗口（默认30天）；可在 config.ini 里配置：
    # [crawler]
    # days_back = 30
    cfg = configparser.ConfigParser()
    cfg.read(CONF_DIR / "config.ini", encoding="utf-8")
    days_back = 30
    reparse_existing = True  # 默认允许覆盖更新，方便你迭代解析逻辑
    use_last_crawl = True    # 新增：默认使用“上次抓取截止时间”做增量窗口，避免重复爬取
    last_crawl_state_file = LAST_CRAWL_STATE_FILE
    try:
        if cfg.has_section("crawler") and cfg.get("crawler", "days_back", fallback=""):
            days_back = int(cfg.get("crawler", "days_back"))
        if cfg.has_section("crawler"):
            reparse_existing = cfg.getboolean("crawler", "reparse_existing", fallback=True)
            use_last_crawl = cfg.getboolean("crawler", "use_last_crawl", fallback=True)
            # 可选：自定义状态文件路径（相对 conf/config.ini 的项目根目录）
            p = cfg.get("crawler", "last_crawl_state_file", fallback="").strip()
            if p:
                last_crawl_state_file = (PROJECT_ROOT / p).resolve()
    except Exception:
        days_back = 30
        reparse_existing = True
        use_last_crawl = True
        last_crawl_state_file = LAST_CRAWL_STATE_FILE

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "X-Requested-With": "XMLHttpRequest"
    })

    POST_TIMEOUT = (5, 20)   # (connect, read)
    GET_TIMEOUT = (5, 60)    # pdf download can be slower
    MAX_RETRY = 3

    base_url = "http://www.cninfo.com.cn/new/hisAnnouncement/query"

    # CNINFO 的 seDate/endDate 在“当天新增披露”场景下经常会漏（按日边界/索引延迟）。
    # 经验最稳：把查询窗口的 end 向后延 1 天（明天），确保抓到“今天新增但接口尚未完全可见”的公告。
    # 注意：增量状态仍以 real_end_date（当前时间）持久化，避免把未来时间写入 last_crawl_end_iso。
    real_end_date = datetime.now()
    query_end_date = real_end_date + timedelta(days=1)
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

    date_range = f"{start_date.strftime('%Y-%m-%d')}~{query_end_date.strftime('%Y-%m-%d')}"
    if use_last_crawl:
        print(f"[crawler] use_last_crawl=true, state={last_crawl_state_file}, window={date_range}")

    columns = ["szse", "sse"]  # 全市场：深交所 + 上交所

    for col in columns:
        page = 1
        while True:
            plate = "sz" if col == "szse" else "sh"  # 深市/沪市
            print(f"[{col}] 拉取第 {page} 页...")
            data = {
                "pageNum": page,
                "pageSize": 30,
                "column": col,          # szse / sse
                "plate": plate,         # sz / sh
                "tabName": "fulltext",
                "category": "category_ndbg_szsh;",
                "seDate": date_range,
                "isHLtitle": "false",
                "searchkey": "年度报告",
                "secid": ""
            }

            result = None
            for attempt in range(1, MAX_RETRY + 1):
                try:
                    r = session.post(base_url, data=data, timeout=POST_TIMEOUT)
                    r.raise_for_status()
                    result = r.json()
                    break
                except Exception as e:
                    print(f"[{col}] 第 {page} 页请求失败 attempt={attempt}/{MAX_RETRY}: {e}")
                    if attempt == MAX_RETRY:
                        print(f"[{col}] 连续失败，跳过该交易所后续分页。")
                        result = {"announcements": []}
                    else:
                        time.sleep(1.0 * attempt)

            announcements = result.get("announcements") or []
            print(f"[{col}] 第 {page} 页返回 {len(announcements)} 条公告")

            # 若服务端忽略 seDate，分页会无限向历史拉取。我们用 announcementTime 强制截断。
            # 已按 announcementTime desc 排序：当本页最老公告都早于 start_date 时，后续页只会更早，直接停止。
            oldest_ts = None
            newest_ts = None
            for a in announcements:
                t = a.get("announcementTime")
                ts = None
                if isinstance(t, int):
                    ts = t / 1000 if t > 1e12 else t
                elif isinstance(t, str):
                    # could be millis string or date string
                    if t.isdigit():
                        ti = int(t)
                        ts = ti / 1000 if ti > 1e12 else ti
                    else:
                        # try YYYY-MM-DD...
                        try:
                            ts = datetime.strptime(t[:10], "%Y-%m-%d").timestamp()
                        except Exception:
                            ts = None
                if ts is None:
                    continue
                oldest_ts = ts if oldest_ts is None else min(oldest_ts, ts)
                newest_ts = ts if newest_ts is None else max(newest_ts, ts)

            if oldest_ts is not None and oldest_ts < start_ts:
                print(f"[{col}] 已到达时间窗口下界（本页最老 {datetime.fromtimestamp(oldest_ts).strftime('%Y-%m-%d')} < {start_date.strftime('%Y-%m-%d')}），处理完本页后停止分页。")
                stop_after_page = True
            else:
                stop_after_page = False

            # 如果本页最新也早于窗口下界，说明全页都过期，直接停止（无需再处理/翻页）
            if newest_ts is not None and newest_ts < start_ts:
                print(f"[{col}] 本页最新也早于时间窗口（{datetime.fromtimestamp(newest_ts).strftime('%Y-%m-%d')} < {start_date.strftime('%Y-%m-%d')}），立即停止分页。")
                break

            if page >= 50:
                print(f"[{col}] 已达到最大页数上限 50，停止分页（防止异常无限分页）。")
                break

            if not announcements:
                break

            processed_this_page = 0
            if col == "sse" and page == 1:
                for a in announcements:
                    print("[SSE DEBUG]", a.get("secCode"), a.get("secName"), a.get("announcementTitle"),
                          a.get("announcementTime"))
            for ann in announcements:

                title = ann["announcementTitle"]

                # 过滤：必须是年度报告（排除摘要）
                if "年度报告" not in title:
                    continue
                if "摘要" in title:
                    continue
                # 排除纯公告（例如“关于披露年度报告的公告”）
                if "关于" in title and "年度报告" not in title.split("关于")[0]:
                    continue

                # 解析年份：兼容 “2025年年度报告… / 2025年度报告… / 2025 年度报告…”
                year_match = re.search(r"(20\d{2})\s*年?\s*年度报告", title)
                if year_match:
                    year = int(year_match.group(1))
                else:
                    # 兜底：标题以年份开头（如：2025年度报告全文）
                    m0 = re.match(r"^(20\d{2})", title)
                    if not m0:
                        continue
                    year = int(m0.group(1))

                stock_code = ann["secCode"]
                stock_name = ann["secName"]
                timestamp = ann["announcementTime"]

                if isinstance(timestamp, int):
                    ts = timestamp / 1000 if timestamp > 1e12 else timestamp
                    publish_dt = datetime.fromtimestamp(ts)
                    publish_date = publish_dt.strftime("%Y-%m-%d")
                else:
                    # Fallback (should be rare)
                    publish_date = str(timestamp)[:10]
                    try:
                        publish_dt = datetime.strptime(publish_date, "%Y-%m-%d")
                        ts = publish_dt.timestamp()
                    except Exception:
                        ts = None

                # 强制时间窗口过滤（避免 seDate 失效导致拉到历史数据）
                if ts is not None:
                    if ts < start_ts or ts > end_ts:
                        continue
                else:
                    # 若无法得到时间戳，则按 publish_date 兜底判断；解析失败则直接跳过（避免拉到太老数据）
                    try:
                        pd = datetime.strptime(publish_date, "%Y-%m-%d").timestamp()
                    except Exception:
                        continue
                    if pd < start_ts or pd > end_ts:
                        continue

                # 去重/重试策略：
                # - 若该公司该年已存在且解析完整，则跳过（仅当 reparse_existing=False）
                # - 若已存在但解析不完整（或曾失败），允许重试并覆盖 placeholder
                existing_id = db.get_report_id(stock_code, year)
                if existing_id is not None and db.is_mda_complete(existing_id):
                    # 默认允许覆盖更新，便于你迭代解析算法；如需旧行为，在 config.ini 里设置 reparse_existing=false
                    if not reparse_existing:
                        continue

                adj_url = ann["adjunctUrl"]
                file_name = adj_url.split("/")[-1]
                file_path = os.path.join(download_dir, file_name)

                if not os.path.exists(file_path):
                    pdf_url = "http://static.cninfo.com.cn/" + adj_url
                    print(f"[{col}] 下载PDF: {stock_code}-{year} {title}")
                    pdf_resp = None
                    for attempt in range(1, MAX_RETRY + 1):
                        try:
                            pdf_resp = session.get(pdf_url, timeout=GET_TIMEOUT)
                            pdf_resp.raise_for_status()
                            break
                        except Exception as e:
                            print(f"[{col}] PDF下载失败 attempt={attempt}/{MAX_RETRY}: {e}")
                            if attempt == MAX_RETRY:
                                pdf_resp = None
                            else:
                                time.sleep(1.0 * attempt)

                    if pdf_resp is None:
                        print(f"[{col}] 放弃下载，跳过: {title}")
                        continue

                    with open(file_path, "wb") as f:
                        f.write(pdf_resp.content)
                    time.sleep(0.8)

                # ✅ 页数判断（防止公告/更正等短PDF）
                try:
                    with pdfplumber.open(file_path) as pdf:
                        page_count = len(pdf.pages)
                except Exception as e:
                    print(f"无法读取PDF页数，跳过: {title} err={e}")
                    continue

                if page_count < 50:
                    print(f"跳过非完整年报（{page_count}页）: {title}")
                    continue

                print(f"[{col}] 解析PDF第三节: {stock_code}-{year}")
                mda = None
                parse_reason = ""
                try:
                    mda = parser.extract_mda(file_path)
                    if not mda:
                        parse_reason = "mda_not_found"
                except Exception as e:
                    parse_reason = f"exception:{type(e).__name__}"
                    print(f"[{col}] 解析失败: {title} err={e}")
                    traceback.print_exc()

                # 先 upsert 报告主表（即使解析失败也要入库，避免这份年报被忽略）
                report_id = db.upsert_report(
                    stock_code,
                    stock_name,
                    year,
                    publish_date,
                    file_path
                )

                # 如果解析失败，写入 placeholder（带 sentinel，方便后续重跑覆盖）
                if not mda:
                    fb = parser.build_fallback_mda(file_path, reason=parse_reason or "unknown", page_count=page_count)
                    db.insert_mda(report_id, fb)
                    print(f"[{col}] 已入库 placeholder（可重试覆盖）: {stock_code}-{year} reason={parse_reason}")
                else:
                    db.insert_mda(report_id, mda)
                    print(f"完成：{stock_code}-{year} ({col})")

                processed_this_page += 1

            if processed_this_page == 0 and page >= 3:
                print(f"[{col}] 连续分页未处理到任何有效年报（processed=0），停止分页以防异常无限翻页。")
                break

            if stop_after_page:
                break
            page += 1
            time.sleep(1.5)

    # 写入本次抓取的截止时间（用于下次增量）
    if use_last_crawl:
        # 写入真实抓取截止时间（当前时间），不要写入 query_end_date（明天）以免影响下次增量窗口。
        save_last_crawl_ts(last_crawl_state_file, real_end_date)
    print("增量更新完成")


if __name__ == "__main__":
    main()