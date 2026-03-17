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
# Same-day incremental crawl state helpers
# ===============================
def load_crawl_state_obj(path: Path) -> dict:
    try:
        if not path.exists():
            return {}
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def save_crawl_state_obj(path: Path, obj: dict) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def build_announcement_key(col: str, ann: dict) -> str:
    """Build a stable unique key for an announcement.

    Prefer announcementId when available; fall back to adjunctUrl/title/code/time.
    """
    ann_id = ann.get("announcementId") or ann.get("id")
    if ann_id:
        return f"{col}:id:{ann_id}"
    adj = ann.get("adjunctUrl") or ""
    if adj:
        return f"{col}:adj:{adj}"
    # last resort (less stable)
    return f"{col}:mix:{ann.get('secCode')}|{ann.get('announcementTitle')}|{ann.get('announcementTime')}"


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
    def normalize_page(self, text: str) -> str:
        """Normalize a *single page* of extracted text.

        Applies de-duplication and cleanup, but does NOT attempt to detect or remove tables/images or insert any placeholders.
        """
        if not text:
            return ""

        def _dedup_doubled_chars(s: str) -> str:
            """De-duplicate common PDF text-layer artifacts.

            This version fixes BOTH cases:
              1) whole-line pairwise doubling: 全全球球 / 2222000022225555
              2) pairwise-doubled SUBSTRINGS inside an otherwise normal line:
                 22002255年全球...  / 回回升。 / （（如…））

            We only collapse pairwise runs when the run is long enough (>= 3 pairs)
            to avoid harming legitimate double characters.
            """
            if not s:
                return s

            s = s.strip()

            # Fast-path: duplicated year patterns like 20252025 -> 2025
            s = re.sub(r"((?:19|20)\d{2})\1", r"\1", s)

            def _collapse_pair_runs(x: str) -> str:
                out_chars: list[str] = []
                i = 0
                n = len(x)
                # Only consider these chars for pair-run collapsing
                punct = {"，", ",", "。", ".", "；", ";", "：", ":", "、", "（", "）", "(", ")"}

                while i < n:
                    # Try to detect a run of repeated pairs: AA BB CC ... emitted as AABBCC...
                    if i + 1 < n and x[i] == x[i + 1]:
                        ch = x[i]
                        is_digit = ch.isdigit()
                        is_cjk = "\u4e00" <= ch <= "\u9fff"
                        is_punct = ch in punct

                        if is_digit or is_cjk or is_punct:
                            j = i
                            pairs: list[str] = []
                            while j + 1 < n and x[j] == x[j + 1]:
                                pairs.append(x[j])
                                j += 2

                            # Collapse only when the run is long enough to be clearly an artifact
                            if len(pairs) >= 3:
                                out_chars.extend(pairs)
                                i = j
                                continue

                    out_chars.append(x[i])
                    i += 1

                return "".join(out_chars)

            # First collapse pairwise-doubled runs anywhere inside the line
            s = _collapse_pair_runs(s)

            # 1) Whole-line pairwise duplication detection (covers: 全全球球..., 2222000022225555...)
            digit_ratio = sum(ch.isdigit() for ch in s) / max(len(s), 1)
            if len(s) % 2 == 0:
                total_pairs = len(s) // 2
                same_pairs = 0
                for i in range(0, len(s), 2):
                    if s[i] == s[i + 1]:
                        same_pairs += 1
                if total_pairs > 0 and (same_pairs / total_pairs) >= 0.65:
                    if len(s) >= 20 or digit_ratio >= 0.60:
                        s = "".join(s[i] for i in range(0, len(s), 2))

            # 2) Collapse consecutive duplicates.
            #    - Digits/ASCII letters: collapse if run >= 2 (often duplicated text-layer)
            #    - CJK: be more conservative; only collapse if run >= 3
            #    - Punctuation: collapse if run >= 2 (avoid 、、、 （（（）））)
            out = []
            i = 0
            n = len(s)
            while i < n:
                ch = s[i]
                j = i + 1
                while j < n and s[j] == ch:
                    j += 1
                run = j - i

                is_ascii_alnum = ("0" <= ch <= "9") or ("A" <= ch <= "Z") or ("a" <= ch <= "z")
                is_cjk = "\u4e00" <= ch <= "\u9fff"
                is_punct = ch in {"，", ",", "。", ".", "；", ";", "：", ":", "、", "（", "）", "(", ")"}

                if is_ascii_alnum:
                    out.append(ch)
                elif is_punct:
                    out.append(ch)
                elif is_cjk:
                    if run >= 3:
                        out.append(ch)
                    else:
                        out.extend([ch] * run)
                else:
                    out.extend([ch] * run)

                i = j

            collapsed = "".join(out)

            # 3) Final light collapse for adjacent duplicates in alnum / a few symbols.
            dup_adj = 0
            for k in range(len(collapsed) - 1):
                if collapsed[k] == collapsed[k + 1] and (collapsed[k].isalnum() or collapsed[k] in {"、", "（", "）", "(", ")"}):
                    dup_adj += 1
            if (len(collapsed) - 1) > 0 and (dup_adj / (len(collapsed) - 1)) >= 0.20:
                out2 = [collapsed[0]]
                for ch in collapsed[1:]:
                    if ch == out2[-1] and (ch.isalnum() or ch in {"、", "（", "）", "(", ")"}):
                        continue
                    out2.append(ch)
                collapsed = "".join(out2)

            return collapsed

        text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\x0c", "\n")
        text = re.sub(r"\n{3,}", "\n\n", text)
        lines = []
        for raw in text.split("\n"):
            line = (raw or "").replace("\u3000", " ").strip()
            line = _dedup_doubled_chars(line)
            if not line:
                continue
            lines.append(line)
        out = "\n".join(lines)
        out = out.replace("\u3000", " ")
        out = re.sub(r"[ \t]+\n", "\n", out)
        out = re.sub(r"[ \t]+", " ", out)
        out = re.sub(r"\n{3,}", "\n\n", out)
        # Punctuation cleanup: collapse repeated runs (include 顿号/括号)
        # Punctuation cleanup: collapse repeated runs (include 顿号/括号)
        # IMPORTANT: do NOT collapse dot-leader runs used by TOC lines (e.g. "...... 12"),
        # otherwise TOC detection in later steps will fail.
        out = re.sub(r"[，,]{2,}", "，", out)
        out = re.sub(r"[、]{2,}", "、", out)
        out = re.sub(r"[；;]{2,}", "；", out)
        out = re.sub(r"。{2,}", "。", out)  # only collapse Chinese full-stop
        out = re.sub(r"[：:]{2,}", "：", out)
        out = re.sub(r"[（(]{2,}", "（", out)
        out = re.sub(r"[）)]{2,}", "）", out)

        # Collapse adjacent punctuation even if mixed with spaces
        # NOTE: exclude '.' from collapsing to preserve TOC dot leaders.
        out = re.sub(r"([，,、；;。：:])(?:\s*\1)+", r"\1", out)
        out = re.sub(r"[，,、；;。：:]\s*[，,、；;。：:]\s*", lambda m: m.group(0)[0], out)
        out = re.sub(r"（\s*（+", "（", out)
        out = re.sub(r"）\s*）+", "）", out)

        # --- Extra de-dup: consecutive duplicate lines / short trailing sentences ---
        lines2: list[str] = []
        prev = ""
        for ln in out.split("\n"):
            s = (ln or "").strip()
            if not s:
                continue
            # 1) exact consecutive duplicate line
            if prev and s == prev:
                continue
            # 2) very short duplicated tail (often PDF text-layer repeats the sentence ending)
            #    e.g. "回升。" appears twice across layout blocks
            if prev and len(s) <= 8 and (prev.endswith(s) or prev.endswith(s + "。") or prev.endswith(s + "；")):
                continue
            lines2.append(s)
            prev = s
        out = "\n".join(lines2)

        # Also collapse accidental double-sentence repetition inside the same line: “回升。回升。”
        out = re.sub(r"([\u4e00-\u9fff]{1,6}[。；！？])\s*\1", r"\1", out)
        # Merge soft-wrapped newlines (keep paragraphs/lists/headings)
        out = self._unwrap_soft_linebreaks(out)
        return out

    def _unwrap_soft_linebreaks(self, text: str) -> str:
        """
        Merge PDF soft-wrapped line breaks back into a single paragraph line,
        while preserving true paragraph breaks and headings/list structure.

        Key fix:
        - Some PDFs insert a SINGLE blank line inside a wrapped paragraph/list item.
          We treat a single blank line as a *soft* break when the surrounding
          lines clearly belong to the same paragraph, and only treat 2+ blank
          lines as a real paragraph separator.

        Rules:
        - Keep 2+ consecutive blank lines as paragraph separators.
        - Do NOT merge across headings / list-item starts.
        - Merge when the previous line looks like it continues (doesn't end with strong terminal punctuation),
          OR ends with comma-like punctuation (，、：；, etc. treated as "continue" by default).
        """
        if not text:
            return ""

        t = text.replace("\r\n", "\n").replace("\r", "\n")
        # keep paragraph breaks, but collapse 3+ newlines into a single blank line
        t = re.sub(r"\n{3,}", "\n\n", t)

        lines = t.split("\n")

        # Heading / list starts: never merge a previous line into these
        heading_or_list_re = re.compile(
            r"^\s*(?:"
            r"第[一二三四五六七八九十]{1,3}[章节]|"  # 第三章/节
            r"\d+(?:[\.．]\d+){1,3}\b|"          # 2.3 / 2.3.1 / 10.2.3 (dot headings)
            r"[一二三四五六七八九十]{1,3}[、\.．:：]|"  # 三、 / 三. / 三：
            r"\d{1,2}[、\.．:：]|"  # 3、 / 3. / 3：
            r"[（(][一二三四五六七八九十0-9]{1,3}[）)]|"  # （三） / (3)
            r"[-•●]\s+"  # - / • bullet
            r")"
        )

        # Strong terminal punctuation: if a line ends with these, treat as paragraph end (do not merge)
        strong_end_re = re.compile(r"[。！？!?]\s*$")

        out: list[str] = []
        buf = ""
        buf_is_pure_heading = False
        buf_is_heading_line = False  # any heading/list starter line; never merge body into it

        def flush_buf():
            nonlocal buf, buf_is_pure_heading, buf_is_heading_line
            if buf.strip():
                out.append(buf.strip())
            buf = ""
            buf_is_pure_heading = False
            buf_is_heading_line = False

        i = 0
        n = len(lines)
        while i < n:
            raw = lines[i]
            s = (raw or "").strip()

            # --- handle blank lines ---
            if not s:
                # count consecutive blanks
                j = i
                while j < n and not (lines[j] or "").strip():
                    j += 1
                blank_cnt = j - i

                # Lookahead to next non-empty line
                nxt = (lines[j] or "").strip() if j < n else ""

                if blank_cnt >= 2:
                    # Real paragraph break
                    flush_buf()
                    out.append("")
                else:
                    # Single blank line: treat as soft break IF it looks like the same paragraph continues
                    prev = buf.rstrip()
                    can_soft_merge = False
                    if prev and nxt:
                        if (not strong_end_re.search(prev)) and (not heading_or_list_re.match(nxt)):
                            # also avoid soft-merge when the previous line is very short (often a heading-ish fragment)
                            if len(prev) > 6:
                                can_soft_merge = True
                    if can_soft_merge:
                        # keep in same paragraph; insert one space so words don't glue
                        if not prev.endswith(" "):
                            buf = prev + " "
                    else:
                        flush_buf()
                        out.append("")

                i = j
                continue

            # --- non-blank line ---
            if not buf:
                buf = s
                buf_is_heading_line = bool(heading_or_list_re.match(s))
                # Treat short title-like lines as "pure headings" so we do NOT merge the next body line into them.
                buf_is_pure_heading = buf_is_heading_line and (len(s) <= 30) and (not re.search(r"[。！？!?；;，,：:]", s))
                i += 1
                continue

            # If current line is a heading/list start, keep a hard break before it
            if heading_or_list_re.match(s):
                flush_buf()
                buf = s
                buf_is_heading_line = True
                buf_is_pure_heading = (len(s) <= 30) and (not re.search(r"[。！？!?；;，,：:]", s))
                i += 1
                continue

            prev = buf.rstrip()
            # If current buffer is a heading/list starter line, keep a hard break to the next body line.
            # This prevents cases like "1、研发模式：..." being merged with the next paragraph.
            if buf_is_heading_line:
                flush_buf()
                buf = s
                buf_is_heading_line = bool(heading_or_list_re.match(s))
                buf_is_pure_heading = buf_is_heading_line and (len(s) <= 30) and (not re.search(r"[。！？!?；;，,：:]", s))
                i += 1
                continue

            # If the current buffer is a short heading line, keep the newline after it.
            # Example: "1、公司所处行业分类" should NOT be merged with the following body line.
            if buf_is_pure_heading:
                flush_buf()
                buf = s
                buf_is_heading_line = bool(heading_or_list_re.match(s))
                buf_is_pure_heading = buf_is_heading_line and (len(s) <= 30) and (not re.search(r"[。！？!?；;，,：:]", s))
                i += 1
                continue

            # 1) If prev already ends with strong terminal punctuation, do not merge
            if strong_end_re.search(prev):
                flush_buf()
                buf = s
                buf_is_heading_line = bool(heading_or_list_re.match(s))
                buf_is_pure_heading = buf_is_heading_line and (len(s) <= 30) and (not re.search(r"[。！？!?；;，,：:]", s))
                i += 1
                continue

            # 2) If prev ends with obvious "still continuing" punctuation, merge
            if re.search(r"[，,、：:（(\-]\s*$", prev):
                joiner = ""
                # add space only when both sides are ASCII-ish words/numbers
                if re.search(r"[A-Za-z0-9]$", prev) and re.match(r"^[A-Za-z0-9]", s):
                    joiner = " "
                buf = prev + joiner + s
                buf_is_pure_heading = False
                i += 1
                continue

            # 3) If prev line is short, prefer NOT to merge
            if len(prev) <= 6:
                flush_buf()
                buf = s
                buf_is_heading_line = bool(heading_or_list_re.match(s))
                buf_is_pure_heading = buf_is_heading_line and (len(s) <= 30) and (not re.search(r"[。！？!?；;，,：:]", s))
                i += 1
                continue

            # 4) Default: merge
            joiner = ""
            if re.search(r"[A-Za-z0-9]$", prev) and re.match(r"^[A-Za-z0-9]", s):
                joiner = " "
            buf = prev + joiner + s
            buf_is_pure_heading = False
            i += 1

        flush_buf()

        rebuilt = "\n".join(out)
        rebuilt = re.sub(r"\n{3,}", "\n\n", rebuilt).strip()
        return rebuilt

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
        # --- helper: TOC-like detection / removal ---
        _toc_line_re = re.compile(r"[\.·…]{6,}\s*\d{1,4}$")
        _toc_dense_re = re.compile(r"^[^\n]{1,120}[\.·…]{6,}[^\n]{0,40}\d{1,4}$")

        def _is_toc_like_line(line: str) -> bool:
            s = (line or "").strip()
            if not s:
                return False
            if _toc_line_re.search(s):
                return True
            if _toc_dense_re.search(s):
                return True
            if ("……" in s or "..." in s) and re.search(r"\d{1,4}$", s):
                return True
            return False

        def _strip_toc_block(text: str) -> str:
            """Remove TOC/目录 blocks that often cause false hits (e.g. '第二章董事会报告....12')."""
            if not text:
                return text
            lines = text.split("\n")

            # Heuristic A: if we see '目录' early, drop subsequent TOC-like lines until a real chapter heading.
            out = []
            in_toc = False
            saw_dir = False
            for raw_line in lines:
                line = (raw_line or "").strip()

                if not saw_dir and (line == "目录" or "目 录" in line):
                    saw_dir = True
                    in_toc = True
                    continue

                if in_toc:
                    if _is_toc_like_line(line):
                        continue
                    # End TOC when we hit a real-looking heading line
                    if (
                        re.match(r"^(第[一二三四五六七八九十]{1,3}[章节])", line)
                        or re.match(r"^\d+(?:[\.．]\d+)*", line)
                        or re.match(r"^[一二三四五六七八九十]{1,3}、", line)
                    ):
                        in_toc = False
                        out.append(raw_line)
                    else:
                        continue
                else:
                    out.append(raw_line)

            text2 = "\n".join(out)

            # Heuristic B: even without explicit '目录', remove a dense TOC prefix if the first chunk is TOC-heavy.
            head = lines[:200]
            if head:
                toc_like_cnt = sum(1 for x in head if _is_toc_like_line((x or "").strip()))
                if toc_like_cnt >= 15:
                    out2 = []
                    dropping = True
                    for raw_line in lines:
                        line = (raw_line or "").strip()
                        if dropping and _is_toc_like_line(line):
                            continue
                        dropping = False
                        out2.append(raw_line)
                    text2 = "\n".join(out2)

            return text2

        def _find_heading_pos(text: str, patterns: list[str]) -> int | None:
            """Find first non-TOC occurrence of any heading pattern."""
            if not text:
                return None
            for pat in patterns:
                for m in re.finditer(pat, text):
                    ls = text.rfind("\n", 0, m.start())
                    le = text.find("\n", m.start())
                    line = text[(ls + 1 if ls >= 0 else 0):(le if le >= 0 else len(text))].strip()
                    if _is_toc_like_line(line):
                        continue
                    return m.start()
            return None

        # Drop TOC-like blocks early
        t = _strip_toc_block(t)

        # --- helper: drop common boilerplate blocks ---
        def _drop_block(src: str, start_pat: str, end_pats: list[str]) -> str:
            blk = self._extract_between_markers(src, start_pat, end_pats)
            return src.replace(blk, "") if blk else src

        # 目录/释义/词汇表 经常会非常长，优先剔除
        t = _drop_block(t, r"(?:^|\n)目\s*录", [r"(?:^|\n)第一[章节]", r"(?:^|\n)第一节"])
        t = _drop_block(t, r"(?:^|\n)释\s*义", [r"(?:^|\n)(?:词\s*汇\s*表|第一[章节]|第一节)"])
        t = _drop_block(t, r"(?:^|\n)词\s*汇\s*表", [r"(?:^|\n)(?:第一[章节]|第一节)"])

        # 对于某些模板：重要提示会出现在最前面。
        # 这里我们只做“可用内容”起点定位，并且强制跳过目录/TOC 行的误命中。
        preferred_starts = [
            r"(?:^|\n)第二章\s*董事会报告",
            r"(?:^|\n)(?:第二章\s*)?(?:董事会报告|董事会工作报告|董事会报告书)",
            r"(?:^|\n)管理层综述",
            r"(?:^|\n)董事长致辞",
        ]

        board_pos = _find_heading_pos(t, [preferred_starts[0], preferred_starts[1]])
        if board_pos is not None:
            t = t[board_pos:]
        else:
            any_pos = _find_heading_pos(t, preferred_starts)
            if any_pos is not None:
                t = t[any_pos:]

        # --- 1) Summary / Overview: combine multiple useful sections ---
        # A) 管理层综述（若存在，优先）
        overview = self._extract_between_markers(
            t,
            r"(?:^|\n)管理层综述",
            [
                r"(?:^|\n)董事长致辞",
                r"(?:^|\n)第二章\s*董事会报告",
                r"(?:^|\n)董事会报告",
                r"(?:^|\n)第三节\s*管理层讨论与分析",
                r"(?:^|\n)公司治理",
                r"(?:^|\n)重要事项",
                r"(?:^|\n)(?:[一二三四五六七八九十]{1,3}|\d{1,2})[、\.．:：]\s*报告期内核心竞争力分析",
            ],
        )

        # B) 董事长致辞（若存在）
        chairman = self._extract_between_markers(
            t,
            r"(?:^|\n)董事长致辞",
            [
                r"(?:^|\n)第二章\s*董事会报告",
                r"(?:^|\n)董事会报告",
                r"(?:^|\n)第三章",
                r"(?:^|\n)第三节\s*管理层讨论与分析",
                r"(?:^|\n)公司治理",
                r"(?:^|\n)重要事项",
            ],
        )

        # C) 董事会报告（不同模板写法差异大）
        board = self._extract_between_markers(
            t,
            r"(?:^|\n)(?:第二章\s*)?(?:董事会报告|董事会工作报告|董事会报告书)",
            [
                r"(?:^|\n)第三章",
                r"(?:^|\n)第三节\s*管理层讨论与分析",
                r"(?:^|\n)公司治理",
                r"(?:^|\n)重要事项",
            ],
        )
        # ✅ 研究向：只要有“董事会报告”，就不要“董事长致辞”
        if board and isinstance(board, str) and len(board.strip()) >= 200:
            chairman = None


        # Combine and de-duplicate (avoid repeating identical blocks)
        # Prefer 董事会报告 over 董事长致辞 (致辞通常偏口号/情绪，信息密度较低)
        summary_parts: list[str] = []
        for part in ([overview, board] if (board and isinstance(board, str) and len(board.strip()) >= 200)
        else [overview, board, chairman]):
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

        # If we have a substantial 董事会报告, use it as the primary business content.
        # ZTE-like templates often have a long 董事长致辞 before 董事会报告; we don't want the letter.
        if board and isinstance(board, str) and len(board) >= 1200:
            # Only keep overview when it is substantial AND clearly not contained by the board report.
            if (overview and isinstance(overview, str)
                and len(overview.strip()) >= 1500
                and (overview.strip() not in board)
                and (board.strip() not in overview)):
                summary_text = "\n\n".join([overview.strip(), board.strip()]).strip()
            else:
                summary_text = board.strip()

        # --- 2) Outlook: keyword-first fallback (do NOT rely on ordinal numbers) ---
        # Some annual reports (e.g., ZTE) use a different structure and do not have the standard
        # “十一、公司未来发展的展望”. In that case, we treat “业务展望/2026年业务展望/2026年业务发展展望”
        # as the primary signal and slice until the next major heading.
        outlook = None

        # 2.1 Prefer the most explicit heading form first:
        # e.g. "2.3 2026年业务展望和面对的经营风险" / "2．3 2026年业务展望…"
        # NOTE: normalize() removed normal spaces, but may keep newlines; be tolerant.
        m_head = re.search(
            r"(?:^|\n)\s*2\s*[\.．]\s*3(?:\s*[\.．]\s*\d+)?\s*[^\n]{0,80}?(?:2026\s*年?)?\s*业务展望[^\n]{0,120}?(?:经营风险|风险)?",
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

        # 2.1 Prefer explicit dot-number headings (e.g. 2.3 / 2.3.1) first, and anchor at the heading start.
        head_patterns = [
            # 2.3 2026年业务展望和面对的经营风险 / 2．3 业务展望...
            r"(?:^|\n)\s*2\s*[\.．]\s*3(?:\s*[\.．]\s*\d+)?\s*[^\n]{0,120}?(?:2026\s*年?)?\s*业务展望[^\n]{0,160}?",
            # Generic dot headings that include 业务展望
            r"(?:^|\n)\s*\d+(?:[\.．]\d+){1,3}\s*[^\n]{0,80}?业务展望[^\n]{0,160}?",
        ]
        m_head = None
        for pat in head_patterns:
            for m in re.finditer(pat, t):
                ls = t.rfind("\n", 0, m.start())
                le = t.find("\n", m.start())
                line = t[(ls + 1 if ls >= 0 else 0):(le if le >= 0 else len(t))].strip()
                if _is_toc_like_line(line):
                    continue
                m_head = m
                break
            if m_head:
                break

        out_idx = None
        if m_head:
            out_idx = m_head.start()
        else:
            # 2.2 Fallback: keyword only (still reject TOC-like line hits)
            for kw in ["2026年业务展望", "2026年业务发展展望", "业务展望"]:
                for m in re.finditer(re.escape(kw), t):
                    ls = t.rfind("\n", 0, m.start())
                    le = t.find("\n", m.start())
                    line = t[(ls + 1 if ls >= 0 else 0):(le if le >= 0 else len(t))].strip()
                    if _is_toc_like_line(line):
                        continue
                    out_idx = m.start()
                    break
                if out_idx is not None:
                    break

        if out_idx is not None:
            start = out_idx
            tail = t[start:]

            # End at next major heading / next dot heading / next big ordinal chapter.
            end_patterns = [
                r"(?:^|\n)\s*2\s*[\.．]\s*3\s*[\.．]\s*[2-9]",
                r"(?:^|\n)\s*2\s*[\.．]\s*4\b",
                r"(?:^|\n)\s*3\s*[\.．]\s*\d",

                r"(?:^|\n)\s*(?:[一二三四五六七八九十]{1,3}|\d{1,2})[、\.．:：]",
                r"(?:^|\n)\s*第\s*[一二三四五六七八九十]{1,3}\s*[章节]",

                r"(?:^|\n)\s*(?:十\s*二|12)\s*[、\.．:：]?\s*报告期内接待调研",
                r"(?:^|\n)\s*(?:十\s*三|13)\s*[、\.．:：]?\s*市值管理",
                r"(?:^|\n)\s*(?:目\s*录|释\s*义|词\s*汇\s*表)",
                r"(?:^|\n)\s*公司治理",
                r"(?:^|\n)\s*重要事项",
                r"(?:^|\n)\s*(?:可能面对的风险|风险因素|风险提示)",
            ]

            end = None
            for ep in end_patterns:
                m_end = re.search(ep, tail)
                if not m_end:
                    continue
                cand = start + m_end.start()
                if cand <= start + 80:
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

    def extract_future_from_fulltext(self, pdf_path: str, *, max_pages: int = 260) -> str | None:
        """Extract 'future outlook' chapter from the FULL report text (not limited to 第三节).

        Preferred order is chapter 11 first; if not found, try other likely chapters.
        """
        return self.extract_future_from_fulltext_by_ordinals(pdf_path, ordinals=None, max_pages=max_pages)

    def extract_future_from_fulltext_by_ordinals(
        self,
        pdf_path: str,
        *,
        ordinals: list[int] | None = None,
        max_pages: int = 260,
    ) -> str | None:
        """Try to extract 'future outlook' chapter from the FULL report text by preferred ordinal chapters.

        Default preference is chapter 11 first. If not found, try other likely chapters (6,7,8,9,10,12,13).

        This is designed to be robust against:
        - TOC/目录 false hits
        - variants like "六、公司关于公司未来发展的讨论与分析" / "11、公司未来发展的展望" / "十二、…" etc.
        """
        if ordinals is None:
            ordinals = [11, 6, 7, 8, 9, 10, 12, 13]

        try:
            raw = self.extract_text(pdf_path, page_numbers=list(range(0, max_pages)))
        except Exception:
            raw = ""
        if not raw:
            return None

        t = raw

        # --- strip TOC-like blocks/lines to avoid matching headings from 目录 ---
        def _is_toc_like_line(line: str) -> bool:
            s = (line or "").strip()
            if not s:
                return False

            # 1) 经典：点引导 + 页码
            if re.search(r"[\.·…]{6,}\s*\d{1,4}$", s):
                return True
            if ("……" in s or "..." in s) and re.search(r"\d{1,4}$", s):
                return True

            # 2) 新增：章节清单式目录（无点引导）
            # 例如：第二章 公司简介和主要财务指标 / 第三节 管理层讨论与分析 / 第十一章 公司未来发展的展望
            if re.match(r"^\s*第\s*(?:[一二三四五六七八九十]{1,3}|\d{1,2})\s*(?:章|节|部分)\b", s):
                # 目录行通常很短、没有句号类终止
                if len(s) <= 60 and (not re.search(r"[。！？；;]", s)):
                    return True

            # 3) 新增：类似“第二章… 第三章…”这种密集章标题
            if re.match(
                    r"^\s*(?:第一|第二|第三|第四|第五|第六|第七|第八|第九|第十|第十一|第十二|第十三)\s*(?:章|节|部分)\b",
                    s):
                if len(s) <= 60 and (not re.search(r"[。！？；;]", s)):
                    return True

            return False

        lines = t.split("\n")
        out_lines = []
        in_toc = False
        for raw_line in lines:
            line = (raw_line or "").strip()
            if not in_toc and (line == "目录" or "目 录" in line):
                in_toc = True
                continue
            if in_toc:
                # skip TOC-like listing lines
                if _is_toc_like_line(line):
                    continue
                # end TOC at first real chapter/section heading
                if (
                    re.match(r"^(第[一二三四五六七八九十]{1,3}[章节])", line)
                    or re.match(r"^\d+(?:[\.．]\d+)*", line)
                    or re.match(r"^[一二三四五六七八九十]{1,3}、", line)
                ):
                    in_toc = False
                    out_lines.append(raw_line)
                else:
                    continue
            else:
                out_lines.append(raw_line)
        t = "\n".join(out_lines)

        # also drop common boilerplates that frequently contain outlook headings in TOC
        def _drop_block(src: str, start_pat: str, end_pats: list[str]) -> str:
            blk = self._extract_between_markers(src, start_pat, end_pats)
            return src.replace(blk, "") if blk else src

        t = _drop_block(t, r"(?:^|\n)目\s*录", [r"(?:^|\n)第一[章节]\b", r"(?:^|\n)第一节\b", r"(?:^|\n)一、\b", r"(?:^|\n)1、\b"])
        t = _drop_block(t, r"(?:^|\n)释\s*义", [r"(?:^|\n)(?:词\s*汇\s*表|名\s*词\s*解\s*释|第一[章节]|第一节|一、|1、)\b"])
        t = _drop_block(t, r"(?:^|\n)词\s*汇\s*表", [r"(?:^|\n)(?:名\s*词\s*解\s*释|第一[章节]|第一节|一、|1、)\b"])
        t = _drop_block(t, r"(?:^|\n)名\s*词\s*解\s*释", [r"(?:^|\n)(?:第一[章节]|第一节|一、|1、)\b"])
        t = _drop_block(t, r"(?:^|\n)重\s*要\s*提\s*示", [r"(?:^|\n)(?:目\s*录|第一[章节]|第一节|一、|1、)\b"])

        # keywords used to confirm the target chapter is indeed an outlook/discussion section
        kw_re = r"(?:未来|展望|规划|发展|讨论与分析|业务展望|经营展望)"

        def _find_heading_pos_for_ordinal(text: str, ordinal: int) -> int | None:
            """Find the first non-TOC heading line for a given ordinal that looks like outlook.

            Accept multiple 'chapter/part/section' notations, e.g.:
              - 十一、… / 11、… / 11.…
              - 第十一部分… / 第11部分…
              - 第十一章… / 第11章…
              - 第十一节… / 第11节…
              - PART XI / PART 11 / SECTION 11 (some bilingual reports)

            We still require the title to contain outlook-like keywords to avoid random hits.

            IMPORTANT anti-false-positive guards:
            - Reject hits that are actually cross-references from “重要提示/释义/目录”等前置段落。
              These often appear as: 上一行包含“详见/请参阅/敬请…”，下一行单独出现“十一、公司未来发展的展望”。
            - Reject headings that start with quotes/brackets used in reference sentences.
            """

            cn = self._arabic_to_cn_ordinal(ordinal)
            ar = str(ordinal)

            # Candidate heading patterns ordered from strongest to weakest.
            # Anchor to line starts (or after newline) to reduce false positives.
            patterns = [
                # 第十一部分 / 第11部分
                rf"(?:^|\n)\s*第\s*(?:{cn}|{ar})\s*部\s*分\s*[、\.．:：]?\s*([^\n]{{1,160}})",
                # 第十一章 / 第11章
                rf"(?:^|\n)\s*第\s*(?:{cn}|{ar})\s*章\s*[、\.．:：]?\s*([^\n]{{1,160}})",
                # 第十一节 / 第11节
                rf"(?:^|\n)\s*第\s*(?:{cn}|{ar})\s*节\s*[、\.．:：]?\s*([^\n]{{1,160}})",
                # 十一、 / 11、 / 11. / 11：
                rf"(?:^|\n)\s*(?:{cn}|{ar})\s*[、\.．:：]\s*([^\n]{{1,160}})",
                # 0X / 0X. (some reports use 06/11 as big headings)
                rf"(?:^|\n)\s*0?{ar}\s*(?:[、\.．:：]|\s+)\s*([^\n]{{1,160}})",
                # English-ish (PART/SECTION) variants
                rf"(?:^|\n)\s*(?:PART|SECTION)\s*(?:{ar}|{cn})\s*[\.:：]?\s*([^\n]{{1,160}})",
            ]

            # Words that indicate the line is not a real chapter heading but a reference sentence.
            ref_hint_re = re.compile(r"(详见|请参阅|敬请|参见|详阅|详见本报告|详见本章|见本报告)")

            for pat in patterns:
                for m in re.finditer(pat, text, flags=re.IGNORECASE):
                    # Extract the whole line for TOC-like rejection.
                    ls = text.rfind("\n", 0, m.start())
                    le = text.find("\n", m.start())
                    line_start = ls + 1 if ls >= 0 else 0
                    line_end = le if le >= 0 else len(text)
                    line = text[line_start:line_end].strip()
                    if not line:
                        continue

                    # Reject obvious TOC-like lines
                    if _is_toc_like_line(line):
                        continue

                    # Reject headings that begin with quote/bracket characters commonly used in references
                    # e.g. “十一、公司未来发展的展望” / 『十一、...』
                    if line and line[0] in {"\"", "'", "“", "”", "‘", "’", "《", "》", "〈", "〉", "【", "】", "「", "」", "『", "』", "（", "(", "["}:
                        continue

                    # If the matched line ends with a small page number, it's likely TOC
                    if re.search(r"\s\d{1,4}\s*$", line) and (not re.search(r"(?:19|20)\d{2}\s*$", line)):
                        continue

                    # Reject obvious boilerplate / preface / TOC-ish hint lines on the same line
                    compact = re.sub(r"\s+", "", line)
                    if re.search(r"(章节|章节的相关内容|重要提示|目录|释义|声明|报告全文)", compact):
                        continue
                    if ref_hint_re.search(compact):
                        continue

                    # Extra guard: if previous line contains reference hints, current line is likely just a referenced title.
                    prev_line = ""
                    if ls >= 0:
                        pls = text.rfind("\n", 0, ls)
                        prev_line = text[(pls + 1 if pls >= 0 else 0):ls].strip()
                    if prev_line:
                        prev_compact = re.sub(r"\s+", "", prev_line)
                        if ref_hint_re.search(prev_compact):
                            # If previous line is clearly a reference sentence and current line looks like a clean heading, skip.
                            # (This avoids pulling headings out of “重要提示” cross-references.)
                            if len(line) <= 60 and (not re.search(r"[。！？!?；;]", line)):
                                continue

                    title = (m.group(1) or "").strip()
                    if not title:
                        continue

                    # Must look like an outlook/discussion chapter.
                    if not re.search(kw_re, title):
                        continue

                    # Reject “章节清单式目录页”：仅在文档前部启用（避免误伤正文）
                    if m.start() < 80000:
                        win_start = max(0, m.start() - 2000)
                        win_end = min(len(text), m.start() + 4000)
                        window = text[win_start:win_end]
                        toc_item_cnt = len(
                            re.findall(r"第\s*(?:[一二三四五六七八九十]{1,3}|\d{1,2})\s*(?:节|章|部分)", window)
                        )
                        toc_item_cnt += len(
                            re.findall(
                                r"(?:^|\n)\s*(?:第一|第二|第三|第四|第五|第六|第七|第八|第九|第十|第十一|第十二|第十三|\d{1,2})\s*节",
                                window,
                            )
                        )
                        # 目录页通常非常密集；正文里出现 3-5 次很正常
                        if toc_item_cnt >= 6:
                            continue

                        # Extra TOC guard: count TOC-like lines in a wider window
                        win2_start = max(0, m.start() - 6000)
                        win2_end = min(len(text), m.start() + 6000)
                        w2 = text[win2_start:win2_end]
                        toc_like_lines = 0
                        for _ln in w2.split("\n")[:300]:
                            if _is_toc_like_line((_ln or "").strip()):
                                toc_like_lines += 1
                        if toc_like_lines >= 8:
                            continue

                    return m.start()

            return None

        def _slice_by_next_ordinal(text: str, start: int, ordinal: int) -> str:
            """Slice from start to next ordinal chapter heading."""
            # Prefer using existing helper if present
            try:
                return self._slice_to_next_ordinal(text, start, str(ordinal)).strip()
            except Exception:
                # fallback: stop at next 'chapter/part/section' or ordinal heading
                tail = text[start:]
                # Skip a little to avoid matching the current heading again
                probe = tail[10:]
                m_next = re.search(
                    r"(?:^|\n)\s*(?:"
                    r"第\s*(?:[一二三四五六七八九十]{1,3}|\d{1,2})\s*(?:部分|章|节)"
                    r"|(?:[一二三四五六七八九十]{1,3}|\d{1,2})\s*[、\.．:：]"
                    r"|(?:PART|SECTION)\s*(?:\d{1,2}|[一二三四五六七八九十]{1,3})\b"
                    r")",
                    probe,
                    flags=re.IGNORECASE,
                )
                if m_next:
                    end = start + 10 + m_next.start()
                    return text[start:end].strip()
                return text[start:].strip()

        # try each ordinal in priority order
        for ord_no in ordinals:
            pos = _find_heading_pos_for_ordinal(t, ord_no)
            if pos is None:
                continue
            fut = _slice_by_next_ordinal(t, pos, ord_no)
            # If we accidentally matched a TOC/目录 entry, skip this hit.
            if fut:
                head = fut[:1500]
                toc_like_cnt = 0
                for _ln in head.split("\n")[:120]:
                    if _is_toc_like_line((_ln or "").strip()):
                        toc_like_cnt += 1
                if ("目录" in head or "目 录" in head) or toc_like_cnt >= 6:
                    fut = None

            # hard safety trims for later chapters
            if fut:
                leak_patterns = [
                    r"(?:^|\n|\b)\s*(?:十\s*二|12)\s*[、\.．:：]?\s*报告期内接待调研",
                    r"(?:^|\n|\b)\s*(?:十\s*三|13)\s*[、\.．:：]?\s*市值管理",
                    r"(?:^|\n|\b)\s*(?:十\s*四|14)\s*[、\.．:：]?\s*质量回报双提升",
                    r"(?:^|\n|\b)\s*(?:财务报告|财务会计报告|备查文件)",
                ]
                cut_at = None
                for pat in leak_patterns:
                    m_leak = re.search(pat, fut)
                    if m_leak:
                        cut_at = m_leak.start() if cut_at is None else min(cut_at, m_leak.start())
                if cut_at is not None:
                    fut = fut[:cut_at].strip()

            if fut and len(fut) >= 200:
                return self._truncate(fut, 40000)

        return None

    def _arabic_to_cn_ordinal(self, n: int) -> str:
        """Convert 1..20 to common Chinese ordinals used in annual reports."""
        cn_list = [
            "一", "二", "三", "四", "五", "六", "七", "八", "九", "十",
            "十一", "十二", "十三", "十四", "十五", "十六", "十七", "十八", "十九", "二十",
        ]
        if 1 <= n <= 20:
            return cn_list[n - 1]
        return str(n)


    def extract_text(self, pdf_path, page_numbers=None):
        """Extract text from PDF.

        NOTE: pdfminer_extract_text reparses the whole file each call and can be extremely slow
        when called page-by-page in a loop. For heavy/complex PDFs, prefer using
        `extract_text_with_pdfplumber()` which keeps the PDF parsed.
        """
        try:
            text = pdfminer_extract_text(pdf_path, page_numbers=page_numbers)
        except Exception:
            text = ""
        if not text:
            return ""
        return self.normalize(text)

    def extract_text_with_pdfplumber(self, pdf, page_numbers=None):
        """Fast path: extract text using an already-opened pdfplumber PDF object.

        This avoids reparsing the full PDF for every page.
        We normalize ONCE on the combined text so that page numbers / headers / footers
        are removed consistently and we don't accidentally glue a page-number line
        to the next page's first line.
        """
        if page_numbers is None:
            page_numbers = list(range(len(pdf.pages)))
        chunks: list[str] = []
        for p in page_numbers:
            if p < 0 or p >= len(pdf.pages):
                continue
            try:
                t = pdf.pages[p].extract_text() or ""
            except Exception:
                t = ""
            if t:
                # Keep raw page text; normalize after joining all pages.
                chunks.append(t)
        if not chunks:
            return ""
        combined = "\n".join(chunks)
        return self.normalize(combined)

    def build_fallback_mda(self, pdf_path: str, reason: str, page_count: int) -> dict:
        """Fallback payload for DB so the report is not ignored even if MDA parsing fails.

        We store a sentinel prefix so later runs can retry and overwrite this placeholder.
        """
        # Keep it reasonably bounded to avoid huge DB rows.
        head_pages = list(range(0, min(25, max(int(page_count or 0), 1))))
        head_text = ""
        try:
            head_text = self.extract_text(pdf_path, page_numbers=head_pages)
        except Exception:
            head_text = ""

        full = f"[PARSE_FAILED] reason={reason}\n" + (head_text or "")
        return {
            "industry": None,
            "business": None,
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

        # 保留段落空行：只把 3 个以上连续换行压到 2 个（不要把所有空行压没）
        text = re.sub(r"\n{3,}", "\n\n", text)
        # ---- Fix: some PDFs glue a heading line and its body into the SAME physical line ----
        # e.g. "二、报告期内公司所处行业情况本公司..." -> break after the heading title.
        # We only do this when the immediate following content looks like paragraph body (common starters).
        body_starters = r"(?:本公司|公司|本集团|集团|报告期|本报告|主要|我们|目前|当前|截至|为|在|根据|同时|此外|其中|因此|所以|另一方面|另方面|并且|而且|受益于|得益于|在\d{4}年)"

        # 1) 一级标题后如果紧跟正文，补一个换行（放宽标题长度：60->140）
        text = re.sub(
            rf"((?:第\s*[一二三四五六七八九十]{{1,3}}\s*[章节]|(?:[一二三四五六七八九十]{{1,3}}|\d{{1,2}})[、\.．:：])\s*[^\n]{{2,140}}?)(?=\s*{body_starters})",
            r"\1\n",
            text,
        )

        # 1.1) 兜底：标题语义明显结束（情况/分析/概况/展望/讨论与分析/主要业务…）时，直接断行
        text = re.sub(
            r"((?:第\s*[一二三四五六七八九十]{1,3}\s*[章节]|(?:[一二三四五六七八九十]{1,3}|\d{1,2})[、\.．:：])\s*[^\n]{2,140}?(?:情况|分析|概况|展望|讨论与分析|主要业务|发展状况|行业情况|行业状况|所处行业情况))(?!\n)(?=\S)",
            r"\1\n",
            text,
        )

        # 2) 括号小标题（（一）/(1)）后如果紧跟正文，补一个换行
        text = re.sub(
            rf"(((?:[（(]\s*[一二三四五六七八九十0-9]{{1,3}}\s*[）)]))\s*[^\n]{{2,60}}?)(?=\s*{body_starters})",
            r"\1\n",
            text,
        )
        # ---- Fix: some PDFs glue a heading directly after previous text without punctuation ----
        # e.g. "...增长12一、报告期内公司所处行业情况" -> insert a newline before the heading token.
        text = re.sub(
            r"(?<=\d)(?=(?:第\s*[一二三四五六七八九十]{1,3}\s*[章节]|(?:[一二三四五六七八九十]{1,3}|\d{1,2})[、\.．:：]))",
            "\n",
            text,
        )
        # ---- Fix: some PDFs lose a newline between a sentence end and the next heading ----
        # e.g. "……。二、报告期内公司所处行业情况" becomes one line; re-insert a newline.
        text = re.sub(
            r"([。！？!?；;])\s*(?=(?:第\s*[一二三四五六七八九十]{1,3}\s*[章节]|(?:[一二三四五六七八九十]{1,3}|\d{1,2})[、\.．:：]))",
            r"\1\n",
            text,
        )
        # ---- Fix: some PDFs glue bracket sub-headings to the previous text ----
        # e.g. "...讨论与分析（一）行业格局..." or "...讨论与分析(1)行业格局...".
        # Insert a newline before the bracket heading token.
        text = re.sub(
            r"(?<=[\u4e00-\u9fff0-9。！？!?；;，,：:、])(?=(?:[（(]\s*[一二三四五六七八九十0-9]{1,3}\s*[）)]))",
            "\n",
            text,
        )

        def _dedup_doubled_chars(s: str) -> str:
            """De-duplicate common PDF text-layer artifacts.

            This version fixes BOTH cases:
              1) whole-line pairwise doubling: 全全球球 / 2222000022225555
              2) pairwise-doubled SUBSTRINGS inside an otherwise normal line:
                 22002255年全球...  / 回回升。 / （（如…））

            We only collapse pairwise runs when the run is long enough (>= 3 pairs)
            to avoid harming legitimate double characters.
            """
            if not s:
                return s

            s = s.strip()

            # Fast-path: duplicated year patterns like 20252025 -> 2025
            s = re.sub(r"((?:19|20)\d{2})\1", r"\1", s)

            def _collapse_pair_runs(x: str) -> str:
                out_chars: list[str] = []
                i = 0
                n = len(x)
                # Only consider these chars for pair-run collapsing
                punct = {"，", ",", "。", ".", "；", ";", "：", ":", "、", "（", "）", "(", ")"}

                while i < n:
                    # Try to detect a run of repeated pairs: AA BB CC ... emitted as AABBCC...
                    if i + 1 < n and x[i] == x[i + 1]:
                        ch = x[i]
                        is_digit = ch.isdigit()
                        is_cjk = "\u4e00" <= ch <= "\u9fff"
                        is_punct = ch in punct

                        if is_digit or is_cjk or is_punct:
                            j = i
                            pairs: list[str] = []
                            while j + 1 < n and x[j] == x[j + 1]:
                                pairs.append(x[j])
                                j += 2

                            # Collapse only when the run is long enough to be clearly an artifact
                            if len(pairs) >= 3:
                                out_chars.extend(pairs)
                                i = j
                                continue

                    out_chars.append(x[i])
                    i += 1

                return "".join(out_chars)

            # First collapse pairwise-doubled runs anywhere inside the line
            s = _collapse_pair_runs(s)

            # 1) Whole-line pairwise duplication detection (covers: 全全球球..., 2222000022225555...)
            digit_ratio = sum(ch.isdigit() for ch in s) / max(len(s), 1)
            if len(s) % 2 == 0:
                total_pairs = len(s) // 2
                same_pairs = 0
                for i in range(0, len(s), 2):
                    if s[i] == s[i + 1]:
                        same_pairs += 1
                if total_pairs > 0 and (same_pairs / total_pairs) >= 0.65:
                    if len(s) >= 20 or digit_ratio >= 0.60:
                        s = "".join(s[i] for i in range(0, len(s), 2))

            # 2) Collapse consecutive duplicates, but do NOT collapse digit runs (e.g. 2000).
            out: list[str] = []
            i = 0
            n = len(s)
            while i < n:
                ch = s[i]
                j = i + 1
                while j < n and s[j] == ch:
                    j += 1
                run = j - i

                is_digit = ch.isdigit()
                is_ascii_letter = ("A" <= ch <= "Z") or ("a" <= ch <= "z")
                is_cjk = "\u4e00" <= ch <= "\u9fff"
                is_punct = ch in {"，", ",", "。", ".", "；", ";", "：", ":", "、", "（", "）", "(", ")"}

                # IMPORTANT: do NOT collapse normal digit runs (e.g. 2000, 1000V, 0.48).
                # Digit de-duplication is handled by the earlier pair-run/whole-line heuristics.
                if is_digit:
                    out.extend([ch] * run)
                elif is_punct:
                    # repeated punctuation is almost always an extraction artifact
                    out.append(ch)
                elif is_ascii_letter:
                    # keep short runs; only collapse very long repeats which are almost certainly noise
                    if run >= 4:
                        out.append(ch)
                    else:
                        out.extend([ch] * run)
                elif is_cjk:
                    # be conservative for CJK; only collapse when it is clearly an artifact
                    if run >= 3:
                        out.append(ch)
                    else:
                        out.extend([ch] * run)
                else:
                    out.extend([ch] * run)

                i = j

            collapsed = "".join(out)
            return collapsed

        lines: list[str] = []
        for raw in text.split("\n"):
            line = (raw or "").replace("\u3000", " ").strip()
            line = _dedup_doubled_chars(line)
            if not line:
                continue

            # 0) 纯标点/分隔符噪声
            if re.fullmatch(r"[，,。\.；;：:、]{2,}", line):
                continue
            # 1) 纯页码行（如：11）
            if re.fullmatch(r"\d{1,4}", line):
                continue
            # 1.1) 形如 14/248 或 25 / 277 的页码（兼容不可见字符/各种空白）
            _line_compact = re.sub(r"[\s\u00A0\u2000-\u200B]", "", line)
            # Normalize common separators
            _line_compact = _line_compact.replace("／", "/")
            if re.fullmatch(r"\d{1,4}/\d{1,4}", _line_compact):
                continue
            # 1.1.5) 纯页码行兜底：可能夹杂不可见字符（肉眼看是 14，但 fullmatch 不命中）
            _digits_only = re.sub(r"\D", "", line)
            if _digits_only and len(_digits_only) <= 4:
                # 保护年份：1900-2099 不当作页码
                if not re.fullmatch(r"(?:19|20)\d{2}", _digits_only):
                    _non_space = re.sub(r"\s", "", line)
                    _non_space2 = re.sub(r"[\[\]（）()<>《》\u00A0]", "", _non_space)
                    # 若去掉空白/常见括号后只剩数字，则认为是页码
                    if _non_space2 == _digits_only and len(_non_space2) <= 4:
                        continue
            # 1.6) 行尾页码（例如标题末尾带 "... 14"），更强兜底：允许夹杂不可见字符
            #     目标：移除页眉/页脚页码，不影响年份(2025/2026等)
            m_tail_page = re.match(r"^(.*?)(?:\s+)(\d{1,4})\s*$", line)
            if m_tail_page:
                left = m_tail_page.group(1).strip()
                tail_num_raw = m_tail_page.group(2)
                tail_num = re.sub(r"\D", "", tail_num_raw)
                # 只剥离页码：1~4 位且不是年份（1900-2099），避免误伤 2025/2026
                if tail_num and not re.fullmatch(r"(?:19|20)\d{2}", tail_num):
                    if left:
                        # Guard: only treat as page-number tail when the left part looks like a header/footer,
                        # i.e., it has no sentence punctuation and is relatively short.
                        if (len(left) <= 80) and (not re.search(r"[。！？；;，,：:]", left)):
                            line = left
            # 1.2) 常见页眉（公司名 + 年度报告）
            # 仅当它看起来像“纯页眉”时才删除：通常无句读标点，且包含“全文/年度报告全文”或行尾就是“年度报告”。
            if ("年度报告" in line) and ("股份有限公司" in line) and len(line) <= 80:
                if ("全文" in line or line.endswith("年度报告") or line.endswith("年度报告全文")) and (not re.search(r"[。！？；;，,：:]", line)):
                    continue
            # 1.3) “公司代码/公司简称”页眉
            if line.startswith("公司代码：") or line.startswith("公司简称：") or ("公司代码：" in line):
                continue
            # 1.4) 仅公司名一行的页眉
            if (line.endswith("股份有限公司") or line.endswith("有限公司")) and len(line) <= 30:
                continue
            # 1.5) 仅“年度报告”一行的页眉
            if ("年度报告" in line) and len(line) <= 20:
                continue
            # 2) 表格边框/分隔符
            if re.fullmatch(r"[-+|]{3,}", line):
                continue
            # 3) 常见年报页眉（包含“年度报告全文”）
            #      只在“短行”时认为是页眉，避免把正文里提到的“年度报告全文”整行删掉。
            if "年度报告全文" in line and len(line) <= 60:
                continue

            lines.append(line)

        text = "\n".join(lines)
        text = text.replace("\u3000", " ")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)

        # Punctuation cleanup: collapse repeated runs (include 顿号/括号)
        text = re.sub(r"[，,]{2,}", "，", text)
        text = re.sub(r"[、]{2,}", "、", text)
        text = re.sub(r"[；;]{2,}", "；", text)
        text = re.sub(r"。{2,}", "。", text)  # only collapse Chinese full-stop
        text = re.sub(r"[：:]{2,}", "：", text)
        text = re.sub(r"[（(]{2,}", "（", text)
        text = re.sub(r"[）)]{2,}", "）", text)

        # Collapse adjacent punctuation even if mixed with spaces
        # NOTE: exclude '.' from collapsing to preserve TOC dot leaders.
        text = re.sub(r"([，,、；;。：:])(?:\s*\1)+", r"\1", text)
        text = re.sub(r"[，,、；;。：:]\s*[，,、；;。：:]\s*", lambda m: m.group(0)[0], text)
        text = re.sub(r"（\s*（+", "（", text)
        text = re.sub(r"）\s*）+", "）", text)

        # --- Extra de-dup: consecutive duplicate lines / short trailing sentences ---
        _lines = []
        _prev = ""
        for _ln in text.split("\n"):
            _s = (_ln or "").strip()
            if not _s:
                continue
            if _prev and _s == _prev:
                continue
            if _prev and len(_s) <= 8 and (_prev.endswith(_s) or _prev.endswith(_s + "。") or _prev.endswith(_s + "；")):
                continue
            _lines.append(_s)
            _prev = _s
        text = "\n".join(_lines)

        # Collapse duplicated short sentence inside the same line: “回升。回升。”
        text = re.sub(r"([\u4e00-\u9fff]{1,6}[。；！？])\s*\1", r"\1", text)

        # Merge soft-wrapped newlines (keep paragraphs/lists/headings)
        text = self._unwrap_soft_linebreaks(text)
        return text

    def extract_mda(self, pdf_path):

        # IMPORTANT: do not call pdfminer_extract_text page-by-page in a loop (very slow on some PDFs).
        # Open once with pdfplumber and reuse to avoid repeated full-file parsing.
        try:
            _pdf = pdfplumber.open(pdf_path)
        except Exception:
            _pdf = None

        def _page_text(p: int) -> str:
            if _pdf is None:
                return self.extract_text(pdf_path, page_numbers=[p])
            return self.extract_text_with_pdfplumber(_pdf, page_numbers=[p])

        # 1) 目录定位：先在前20页里寻找“目录”所在页，再只在目录附近提取第三节页码
        start_page = None
        toc_page = None
        for p in range(0, 20):
            t = _page_text(p)
            if t and ("目录" in t or "目 录" in t):
                toc_page = p
                break

        toc_pages = list(range(toc_page, min(toc_page + 4, 20))) if toc_page is not None else list(range(0, 6))
        # IMPORTANT: reuse the already-opened pdfplumber object to avoid reparsing the whole PDF
        toc_chunks = []
        for p in toc_pages:
            try:
                toc_chunks.append(_page_text(p))
            except Exception:
                pass
        toc_text = "\n".join([c for c in toc_chunks if c])

        # 目录行通常形如：第三节 管理层讨论与分析........14
        m = re.search(r"第三节\s*管理层讨论与分析[\.·…\s]{2,200}(\d{1,4})", toc_text)
        if m:
            try:
                page_num = int(m.group(1))
                # 目录页码不可能是 1/2/3 这种（通常 > 5）；小数字很可能误匹配到“(一)/(二)”
                if page_num >= 5:
                    start_page = page_num - 1
                else:
                    start_page = None
            except Exception:
                start_page = None
        # 1.3) 目录页码与 PDF 物理页号常常不一致：用“候选页”附近扫描校准真正的第三节标题页。
        # 典型现象：目录标注第三节起始页=11，但 PDF 第11页可能仍是第二节末尾，第三节标题在下一页。
        if start_page is not None:
            real_start = None
            # 在候选页附近（向前2页、向后6页）寻找“第三节 管理层讨论与分析”或“03 管理层讨论与分析”标题页
            for p in range(max(start_page - 2, 0), start_page + 7):
                try:
                    t = _page_text(p)
                except Exception:
                    t = ""
                if not t:
                    continue
                # 跳过目录页，目录页经常包含“第三节...11”导致误命中
                if "目录" in t or "目 录" in t:
                    continue
                if re.search(r"第三节\s*管理层讨论与分析", t) or re.search(r"(?:^|\n)\s*0?3\s*管理层讨论与分析", t):
                    real_start = p
                    break
            if real_start is not None:
                start_page = real_start
        # 1.5) 起始页校验：避免把“目录页/前言页”误当作第三节正文起点
        if start_page is not None:
            # 年报第三节一般不会在很靠前的页（<5 绝大概率是误判/目录页）
            if start_page < 5:
                start_page = None
            else:
                # 若落在目录页附近或该页包含“目录”，说明仍是目录区域，强制放弃目录定位
                try:
                    check_text = _page_text(start_page)
                except Exception:
                    check_text = ""
                if check_text and ("目录" in check_text or "目 录" in check_text):
                    start_page = None
                # 若 start_page 仍然落在 toc_pages 范围内，也视为目录页误判
                if start_page is not None and start_page in toc_pages:
                    start_page = None

        # 2) 目录失败：正文扫描定位（前200页逐页找“第三节 管理层讨论与分析”）
        if start_page is None:
            scan_start = (toc_page + 1) if toc_page is not None else 0
            scan_end = 200
            if _pdf is not None:
                scan_end = min(scan_end, len(_pdf.pages))
            for p in range(scan_start, scan_end):
                page_text = _page_text(p)
                if not page_text:
                    continue
                # 跳过目录页（目录页经常包含“第三节…14”导致误命中）
                if "目录" in page_text or "目 录" in page_text:
                    continue
                if re.search(r"第三节\s*管理层讨论与分析", page_text):
                    start_page = p
                    break

        if start_page is None:
            # 非标准年报兜底：没有“第三节 管理层讨论与分析”时，尝试抽取“董事长致辞/董事会报告/业务展望”。
            alt = self.extract_alt_sections(pdf_path)
            if _pdf is not None:
                try:
                    _pdf.close()
                except Exception:
                    pass
            if alt:
                return alt

            print("未找到第三节起始位置（目录无页码且正文未命中）")
            return None

        # 2.5) 进一步校准：有些报告的“03 管理层讨论与分析”标题页是图片（无法提取文本），
        # 但紧随其后的正文页会出现“一、报告期内公司从事的主要业务”等稳定锚点。
        # 若 start_page 落在第二节尾页（如“分季度主要财务指标/非经常性损益”），这里把起点前移到真正正文页。
        anchor_patterns = [
            r"第三节\s*管理层讨论与分析",
            r"(?:^|\n)\s*0?3\s*管理层讨论与分析",
            r"(?:^|\n)\s*一、报告期内公司从事的主要业务",
            r"报告期内公司从事的主要业务",
        ]
        calibrated = None
        anchor_end = start_page + 12
        if _pdf is not None:
            anchor_end = min(anchor_end, len(_pdf.pages))
        for p in range(start_page, anchor_end):
            try:
                t = _page_text(p)
            except Exception:
                t = ""
            if not t:
                continue
            if ("分季度主要财务指标" in t) or ("非经常性损益" in t) or ("非经常性损益项目及金额" in t):
                # 明显是第二节尾部，继续往后找
                continue
            if any(re.search(pat, t) for pat in anchor_patterns):
                calibrated = p
                break
        if calibrated is not None and calibrated != start_page:
            start_page = calibrated
        print("第三节正文起始页:", start_page)

        # 3) 从起始页开始读到“第三节结束/第四章开始”的边界
        # 注意：不同年报模板的“第四章”不一定写成“第四节”，常见形式：
        # - 04 公司治理、环境和社会（大号“04”页）
        # - 公司治理（无“第四节”字样）
        # - 直接出现后续章节：十二、报告期内接待调研… / 十三、市值管理… 等
        mda_text = ""
        max_p = start_page + 200
        if _pdf is not None:
            max_p = min(max_p, len(_pdf.pages))
        for p in range(start_page, max_p):
            page_text = _page_text(p)
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
            if _pdf is not None:
                try:
                    _pdf.close()
                except Exception:
                    pass
            return None

        # 强制裁剪到第三节正文开始：优先“第三节/03 管理层讨论与分析”，否则用稳定正文锚点（避免夹带第二节尾页）。
        cut_pos = None
        m3 = re.search(r"第三节\s*管理层讨论与分析", mda_text)
        if m3:
            cut_pos = m3.start()
        else:
            m03 = re.search(r"(?:^|\n)\s*0?3\s*管理层讨论与分析", mda_text)
            if m03:
                cut_pos = m03.start()
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

        # 未来展望提取优先级（按你的新规则）：
        # 1) 默认优先从全文的“第十一部分/十一章”提取
        # 2) 十一部分取不到，再按优先级尝试 6/7/8/9/10/12/13 等章节
        # 3) 全文仍取不到，再从第三节（管理层讨论与分析）里按关键词兜底
        # 4) 最后兜底：alt_sections
        future = None

        # (1) 全文优先：11 -> 6/7/8/9/10/12/13
        try:
            future = self.extract_future_from_fulltext_by_ordinals(
                pdf_path,
                ordinals=[11, 6, 7, 8, 9, 10, 12, 13],
            )
        except Exception:
            future = None

        # (2) 全文没取到，再从第三节里兜底（仅在 MDA 内部找关键词）
        if future is None:
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
                    "风险因素",
                    "风险提示",
                    "经营风险",
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

        # 最后兜底：全文也抓不到时，才用 alt_sections（更像“综述+展望”打包）
        if future is None:
            try:
                alt = self.extract_alt_sections(pdf_path)
                if alt and alt.get("future"):
                    future = alt.get("future")
            except Exception:
                pass

        if future is not None and len(future) < 200:
            future = None
        industry = None
        business = management_overview

        if _pdf is not None:
            try:
                _pdf.close()
            except Exception:
                pass

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

        规则增强：
        - “风险/可能面对的风险”可以作为结束词，但如果命中的标题行很短（基本是一行标题），
          则把该标题行也包含进返回内容里作为结尾（不包含其后正文），避免把结尾信息截掉。
        """
        if start_idx is None or start_idx < 0:
            return None

        def _line_bounds(pos: int) -> tuple[int, int]:
            """Return [line_start, line_end) bounds around pos in text."""
            ls = text.rfind("\n", 0, pos)
            le = text.find("\n", pos)
            line_start = ls + 1 if ls >= 0 else 0
            line_end = le if le >= 0 else len(text)
            return line_start, line_end

        candidates = []

        # 风险类作为结束标题：只在“标题行”层面触发（不会因为正文里出现“风险”而误截断）。
        # 同时兼容不同表述：例如“公司面临的风险和应对措施 / 风险因素 / 风险提示 / 经营风险”等。
        def _risk_end_enabled() -> bool:
            if not title_keywords:
                return False
            # 只要调用方在 end_title_keywords 中显式放了“风险类”关键词，就认为允许用风险章作为结束边界
            enabled_keys = [
                "可能面对的风险", "公司面临的风险", "风险因素", "风险提示", "经营风险",
                "风险及应对", "风险和应对", "应对措施", "风险应对"
            ]
            return any(any((k in kw) or (kw in k) for kw in title_keywords) for k in enabled_keys)

        def _is_risk_heading_line(title_line: str) -> bool:
            tl = (title_line or "").strip()
            if not tl:
                return False
            # 必须是标题行（由外层正则定位到 end_idx），这里只做标题文本匹配
            if "风险" not in tl:
                return False
            # 避免把“强化风险管控/风险管理”等正文用语当成章节标题：要求同时包含“因素/提示/应对/措施/面临”等之一
            if re.search(r"(因素|提示|应对|措施|面临)", tl):
                return True
            return False

        # A) 一级标题：二、xxx
        for m in re.finditer(
            r"(?:^|\n)\s*([一二三四五六七八九十]{1,3}|\d{1,2})[、\.．:：]\s*([^\n]{1,80})",
            text[start_idx + 1:]
        ):
            title = m.group(2)
            hit = any(kw in title for kw in title_keywords)
            # 风险类标题的同义/变体：当调用方允许风险作为结束边界时，标题包含“风险”且包含“因素/提示/应对/措施/面临”也算命中
            if (not hit) and title_keywords:
                if _risk_end_enabled() and ("风险" in title) and re.search(r"(因素|提示|应对|措施|面临)", title):
                    hit = True
            if hit:
                candidates.append(start_idx + 1 + m.start())
                break

        # B) 括号小标题：（三）xxx / (3)xxx
        for m in re.finditer(
            r"(?:^|\n)\s*[（(][一二三四五六七八九十0-9]{1,3}[）)]\s*([^\n]{1,80})",
            text[start_idx + 1:]
        ):
            title = m.group(1)
            hit = any(kw in title for kw in title_keywords)
            # 风险类标题的同义/变体：当调用方允许风险作为结束边界时，标题包含“风险”且包含“因素/提示/应对/措施/面临”也算命中
            if (not hit) and title_keywords:
                if _risk_end_enabled() and ("风险" in title) and re.search(r"(因素|提示|应对|措施|面临)", title):
                    hit = True
            if hit:
                candidates.append(start_idx + 1 + m.start())
                break

        if not candidates:
            return None

        end_idx = min(candidates)

        line_start, line_end = _line_bounds(end_idx)
        end_line = text[line_start:line_end].strip()
        # 如果当前命中的结束标题行是风险类标题，则直接在标题起点截断（不包含该标题行及其后正文）。
        if _risk_end_enabled() and _is_risk_heading_line(end_line):
            return text[start_idx:end_idx].strip()

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
            return r"\s*".join(re.escape(ch) for ch in c)

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
        def _is_toc_like_line_local(line: str) -> bool:
            s = (line or "").strip()
            if not s:
                return False
            # Typical TOC leader dots + trailing page number
            if re.search(r"[\.·…]{6,}\s*\d{1,4}$", s):
                return True
            # Also treat '……12' / '... 12' patterns as TOC
            if ("……" in s or "..." in s) and re.search(r"\d{1,4}$", s):
                return True
            return False

        def _looks_like_real_heading_line_local(line: str) -> bool:
            """Reject false hits from 重要提示/目录 that merely *mention* a heading."""
            s = (line or "").strip()
            if not s:
                return False
            # Heading lines are usually short; sentences in 重要提示 are often long.
            if len(s) > 80:
                return False
            # Common 'please refer to...' sentences
            if re.search(r"(敬请|请\s*查阅|详见|参见|有关|章节的相关内容|本报告|本年度报告|投资者)", s):
                return False
            # Full-sentence punctuation usually means it's not a pure heading line
            if re.search(r"[。；]", s):
                return False
            # Too many commas usually indicates a sentence, not a heading
            if len(re.findall(r"[，,]", s)) >= 2:
                return False
            return True
        for kw in keywords:
            # Support regex keywords by prefix: REGEX:<pattern>
            if isinstance(kw, str) and kw.startswith("REGEX:"):
                kw_pat = kw[len("REGEX:"):]
            else:
                kw_pat = re.escape(str(kw))

            for m in re.finditer(
                rf"(?:^|\n)\s*([一二三四五六七八九十]{{1,3}}|\d{{1,2}})[、\.．:：]\s*[^\n]*{kw_pat}[^\n]*",
                text,
            ):
                # Extract the physical line containing this match
                ls = text.rfind("\n", 0, m.start())
                le = text.find("\n", m.start())
                line = text[(ls + 1 if ls >= 0 else 0):(le if le >= 0 else len(text))].strip()

                # Skip TOC-like lines such as '第二章董事会报告......12'
                if _is_toc_like_line_local(line):
                    continue
                # Skip sentences from 重要提示/前言 that merely reference a chapter title
                if not _looks_like_real_heading_line_local(line):
                    continue

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

            # Guard against TOC false hits (e.g. '2.3 业务展望......45')
            ls = text.rfind("\n", 0, m.start())
            le = text.find("\n", m.start())
            line = text[(ls + 1 if ls >= 0 else 0):(le if le >= 0 else len(text))].strip()
            if _is_toc_like_line_local(line):
                continue
            if not _looks_like_real_heading_line_local(line):
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
            m_dot = re.search(r"(?:^|\n)\s*\d+(?:[\.．]\d+){1,3}(?!\d)", tail)
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


from reportlab.platypus import Paragraph, Spacer, HRFlowable, PageBreak
from reportlab.platypus import Preformatted
import re
import xml.sax.saxutils

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

    now_dt = datetime.now()
    # cninfo 的 announcementTime 在不少情况下会被“按天归一化”为当天 00:00（并可能表现为次日 00:00，受时区/入库口径影响）。
    # 为了避免把“当天晚些时候披露、但时间戳落在次日 00:00”的公告过滤掉：
    # - seDate 查询上界使用“明天 00:00”
    # - 真实时间戳过滤上界也放宽到“明天 00:00”（同一天二次增量尤其需要）
    end_date = now_dt
    query_end_date = datetime.combine((now_dt + timedelta(days=1)).date(), datetime.min.time())

    start_date = end_date - timedelta(days=days_back)

    # 若启用增量窗口：start_date 取 max(days_back窗口起点, 上次抓取截止时间)
    last_end = None
    same_day_incremental = False
    if use_last_crawl:
        last_end = load_last_crawl_ts(last_crawl_state_file)
        if last_end is not None:
            # 给 2 分钟安全边际（避免时钟误差/边界重复）
            safe_last_end = last_end - timedelta(minutes=2)
            if safe_last_end > start_date:
                start_date = safe_last_end

            # If last crawl end is today, enable same-day incremental mode (use seen-keys to stop early)
            if last_end.date() == end_date.date():
                same_day_incremental = True

    # NOTE: cninfo 的 announcementTime 经常被“按天归一化”（当天全部是 00:00:00）。
    # 同一天二次/多次增量时，不能用 last_crawl_end_iso(到秒) 作为下界，否则会把“今天”整天过滤掉。
    # 解决：只要进入 same_day_incremental，就把 start_date 强制归零到“今天 00:00:00”，
    # 并依赖 last_crawl_seen（已见公告 key）来做到“只抓新增 + 命中已见即早停分页”。
    if same_day_incremental:
        start_date = datetime.combine(end_date.date(), datetime.min.time())

    # Load seen announcement keys for same-day incremental
    state_obj = load_crawl_state_obj(last_crawl_state_file) if use_last_crawl else {}
    today_key = end_date.strftime("%Y-%m-%d")
    seen_map = state_obj.get("last_crawl_seen") if isinstance(state_obj.get("last_crawl_seen"), dict) else {}
    seen_szse = set(seen_map.get("szse") or [])
    seen_sse = set(seen_map.get("sse") or [])

    if not same_day_incremental:
        # Different day: reset the seen keys (we don't need them across days)
        seen_szse.clear()
        seen_sse.clear()

    start_ts = start_date.timestamp()
    # 真实过滤上界：放宽到 query_end_date，避免“当日披露但时间戳落在次日 00:00”的公告被过滤掉
    end_ts = query_end_date.timestamp() + 60

    # cninfo seDate 只认日期；上界用 query_end_date（次日 00:00）确保覆盖“当天披露但被标记到次日 00:00”的公告。
    date_range = f"{start_date.strftime('%Y-%m-%d')}~{query_end_date.strftime('%Y-%m-%d')}"
    if use_last_crawl:
        extra = ", same_day_incremental=true" if same_day_incremental else ""
        print(f"[crawler] use_last_crawl=true, state={last_crawl_state_file}, window={date_range}{extra}")

    # Track latest successfully processed announcement time in this run.
    # We persist this as last_crawl_end_iso so multiple runs in the same day can pick up newly posted reports.
    max_processed_dt: datetime | None = None

    columns = ["szse", "sse"]  # 全市场：深交所 + 上交所

    for col in columns:
        page = 1
        # Same-day incremental: stop as soon as we hit the first previously-seen announcement (newest-first ordering)
        seen_set = seen_szse if col == "szse" else seen_sse
        newly_seen_this_run: list[str] = []
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
            ts_count = 0
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
                ts_count += 1
                oldest_ts = ts if oldest_ts is None else min(oldest_ts, ts)
                newest_ts = ts if newest_ts is None else max(newest_ts, ts)

            # 用完整时间打印，避免同一天但不同时间造成误解
            start_dt_str = start_date.strftime("%Y-%m-%d %H:%M:%S")

            if ts_count > 0 and oldest_ts is not None and oldest_ts < start_ts:
                print(
                    f"[{col}] 已到达时间窗口下界（本页最老 {datetime.fromtimestamp(oldest_ts).strftime('%Y-%m-%d %H:%M:%S')} < {start_dt_str}），处理完本页后停止分页。"
                )
                stop_after_page = True
            else:
                stop_after_page = False

            # 如果本页最新也早于窗口下界，说明全页都过期
            # 只有在我们成功解析到了 announcementTime（ts_count>0）时才启用这个快速停止，避免解析失败导致误判。
            if ts_count > 0 and newest_ts is not None and newest_ts < start_ts:
                print(
                    f"[{col}] 本页最新也早于时间窗口（{datetime.fromtimestamp(newest_ts).strftime('%Y-%m-%d %H:%M:%S')} < {start_dt_str}），立即停止分页。"
                )
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

                ann_key = build_announcement_key(col, ann)
                if same_day_incremental and ann_key in seen_set:
                    # 同一天二次/多次增量：只跳过已处理公告，不要直接 break。
                    # 实测同一页内的排序并不总是严格“新->旧”，直接 break 可能错过同页后面的新公告。
                    continue

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

                # Update run checkpoint candidate (only if we have a real timestamp)
                if isinstance(publish_dt, datetime):
                    if max_processed_dt is None or publish_dt > max_processed_dt:
                        max_processed_dt = publish_dt

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

                # mark this announcement as seen for same-day incremental
                if use_last_crawl:
                    if ann_key not in seen_set:
                        seen_set.add(ann_key)
                        newly_seen_this_run.append(ann_key)

                processed_this_page += 1

            # same-day incremental：若这一页完全没有新增可处理年报，则不再翻页（避免扫全日）
            if same_day_incremental and processed_this_page == 0:
                print(f"[{col}] same-day incremental: 本页无新增可处理年报，停止分页。")
                break

            if processed_this_page == 0 and page >= 3:
                print(f"[{col}] 连续分页未处理到任何有效年报（processed=0），停止分页以防异常无限翻页。")
                break
            page += 1
            time.sleep(1.5)

    # 写入本次抓取的截止时间（用于下次增量）
    # Persist the latest processed announcement time instead of the script run time.
    # This allows multiple runs in the same day to pick up newly posted reports.
    if use_last_crawl:
        # update crawl end iso (time-based checkpoint)
        if max_processed_dt is not None:
            save_last_crawl_ts(last_crawl_state_file, max_processed_dt)

        # update same-day seen keys checkpoint
        state_obj = load_crawl_state_obj(last_crawl_state_file)
        state_obj["last_crawl_day"] = end_date.strftime("%Y-%m-%d")

        # cap seen list to keep the state file small
        def _cap(lst: list[str], cap_n: int = 3000) -> list[str]:
            if len(lst) <= cap_n:
                return lst
            return lst[-cap_n:]

        state_obj["last_crawl_seen"] = {
            "szse": _cap(list(seen_szse)),
            "sse": _cap(list(seen_sse)),
        }
        save_crawl_state_obj(last_crawl_state_file, state_obj)
    print("增量更新完成")


if __name__ == "__main__":
    main()
def safe_block(text, body_style):
    """
    Build a reportlab flowable for section text, preserving line breaks for multi-line content.
    """
    from xml.sax.saxutils import escape
    # Make table placeholder always appear as its own paragraph
    ph = "[表格/图表内容已省略，详见原文PDF]"
    if text:
        if ph in text:
            # Ensure placeholder has blank lines around it
            text = re.sub(rf"\s*{re.escape(ph)}\s*", f"\n\n{ph}\n\n", text)
    if not text:
        return Paragraph("（未提取到内容）", body_style)
    if "\n" in text or ph in text:
        # Escape but keep newlines; Preformatted preserves them
        safe = escape(text)
        return Preformatted(safe, body_style)
    else:
        safe = escape(text)
        return Paragraph(safe, body_style)