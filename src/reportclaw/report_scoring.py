

"""report_scoring.py

ReportClaw - Annual Report Extract Scoring

Purpose
-------
Compute a keyword-based score for each (stock_code, report_year) annual report
based on the extracted sections already stored in MySQL (annual_report_mda).

It:
1) Loads extracted text from DB (chairman_letter / main_business_section / future_section / industry_section / full_mda).
2) Counts occurrences of configured positive/negative keywords.
3) Aggregates into a total score per report.
4) Persists score + per-keyword hit counts back to DB.

Database writes
---------------
- Adds columns to `annual_reports` (if missing):
    - score INT NULL
    - score_updated_at DATETIME NULL
- Creates a detail table (if missing) to store keyword hits:
    - annual_report_score_hits

Config
------
- MySQL connection: conf/config.ini [mysql]
- Keyword file (optional): conf/scoring_keywords.json

Keyword file format (JSON)
--------------------------
{
  "positive": {"0到1": 3, "爆发": 2},
  "negative": {"过剩": -2, "竞争白热化": -3, "竞争": -1}
}

Notes
-----
- To reduce overlap double-counting (e.g. “竞争白热化” vs “竞争”), the script counts
  longer keywords first and masks matched spans before counting shorter ones.
- If keyword file is missing, built-in defaults are used.

Run examples
------------
PYTHONPATH=src ./venv/bin/python -m reportclaw.report_scoring --since-days 45
PYTHONPATH=src ./venv/bin/python -m reportclaw.report_scoring --report-id 123
PYTHONPATH=src ./venv/bin/python -m reportclaw.report_scoring --dry-run
"""

from __future__ import annotations

import argparse
import configparser
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


# -------------------------
# Defaults (when no config file)
# -------------------------
DEFAULT_KEYWORDS = {
    "positive": {
        "0到1": 3,
    },
    "negative": {
        "过剩": -2,
        "竞争白热化": -3,
    },
}


 # -------------------------
# Configurable extra scoring rule: CAGR
# -------------------------
DEFAULT_CAGR_RULE: Dict[str, Any] = {
    "enabled": True,
    "phrase": "复合增长率",
    # mode:
    #   - "linear": score = floor(pct * k)
    #   - "thresholds": use thresholds list (see comment below)
    "mode": "linear",
    # linear params
    "k": 0.1,          # 20%->2, 50%->5, 100%->10, 200%->20 when k=0.1
    "cap": 30,         # max points per hit
    "min_pct": 20.0,   # ignore pct below this
    # scanning window
    "window_chars": 220,
    # Stop scanning at real sentence-ending punctuation only; do NOT stop at line-wrap newlines.
    "stop_chars": "。！？!?；;",
    # If the extracted sentence contains any of these words, flip the score to negative (e.g., costs/expenses up)
    "negate_if_contains": ["成本", "费用"],
    # thresholds example:
    #   "thresholds": [{"ge": 20, "score": 2}, {"ge": 50, "score": 5}, ...]
}

# runtime-loaded rules from json (merged on top of defaults)
_CAGR_RULES: List[Dict[str, Any]] = [dict(DEFAULT_CAGR_RULE)]

# -------------------------
# DB helpers
# -------------------------
@dataclass
class MySQLCfg:
    host: str
    port: int
    user: str
    password: str
    db: str


def _project_root() -> Path:
    # src/reportclaw/report_scoring.py -> repo root is parents[2]
    return Path(__file__).resolve().parents[2]


def load_mysql_cfg(config_path: Path) -> MySQLCfg:
    cfg = configparser.ConfigParser()
    if not config_path.exists():
        raise RuntimeError(f"config file not found: {config_path}")
    cfg.read(config_path, encoding="utf-8")
    if "mysql" not in cfg:
        raise RuntimeError(f"missing [mysql] section in {config_path}")

    sec = cfg["mysql"]
    host = sec.get("host", "127.0.0.1")
    port = int(sec.get("port", "3306"))
    user = sec.get("user", "")
    password = sec.get("pass", "") or sec.get("password", "")
    db = sec.get("db", "stock")

    if not user:
        raise RuntimeError("mysql.user is empty in config")

    return MySQLCfg(host=host, port=port, user=user, password=password, db=db)


class MySQL:
    """Tiny DB wrapper with dict rows, using whichever connector is available."""

    def __init__(self, cfg: MySQLCfg):
        self.cfg = cfg
        self._conn = None
        self._backend = None

    def connect(self):
        if self._conn is not None:
            return
        # Prefer PyMySQL (common in lightweight scripts), fallback to mysql-connector.
        try:
            import pymysql  # type: ignore

            self._backend = "pymysql"
            self._conn = pymysql.connect(
                host=self.cfg.host,
                port=self.cfg.port,
                user=self.cfg.user,
                password=self.cfg.password,
                database=self.cfg.db,
                charset="utf8mb4",
                autocommit=True,
                cursorclass=pymysql.cursors.DictCursor,
            )
            return
        except Exception:
            pass

        try:
            import mysql.connector  # type: ignore

            self._backend = "mysql.connector"
            self._conn = mysql.connector.connect(
                host=self.cfg.host,
                port=self.cfg.port,
                user=self.cfg.user,
                password=self.cfg.password,
                database=self.cfg.db,
                autocommit=True,
            )
            return
        except Exception as e:
            raise RuntimeError(
                "No supported MySQL driver found. Please install one of: pymysql, mysql-connector-python"
            ) from e

    def close(self):
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def _cursor(self):
        if self._conn is None:
            self.connect()
        if self._backend == "pymysql":
            return self._conn.cursor()
        # mysql.connector
        return self._conn.cursor(dictionary=True)

    def query(self, sql: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
        cur = self._cursor()
        try:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return list(rows or [])
        finally:
            try:
                cur.close()
            except Exception:
                pass

    def exec(self, sql: str, params: Tuple[Any, ...] = ()) -> int:
        cur = self._cursor()
        try:
            cur.execute(sql, params)
            return int(getattr(cur, "rowcount", 0) or 0)
        finally:
            try:
                cur.close()
            except Exception:
                pass


def _column_exists(db: MySQL, table: str, column: str) -> bool:
    rows = db.query(
        """
        SELECT 1
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
          AND COLUMN_NAME = %s
        LIMIT 1
        """,
        (table, column),
    )
    return bool(rows)


def ensure_schema(db: MySQL):
    # 1) Add score columns to annual_reports
    if not _column_exists(db, "annual_reports", "score"):
        db.exec("ALTER TABLE annual_reports ADD COLUMN score INT NULL")
    if not _column_exists(db, "annual_reports", "score_updated_at"):
        db.exec("ALTER TABLE annual_reports ADD COLUMN score_updated_at DATETIME NULL")

    # 2) Create detail table
    db.exec(
        """
        CREATE TABLE IF NOT EXISTS annual_report_score_hits (
          id BIGINT NOT NULL AUTO_INCREMENT,
          report_id BIGINT NOT NULL,
          keyword VARCHAR(128) NOT NULL,
          weight INT NOT NULL,
          hit_count INT NOT NULL,
          polarity ENUM('pos','neg') NOT NULL,
          updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          UNIQUE KEY uk_report_kw (report_id, keyword),
          KEY idx_report_id (report_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    # Add context column if missing
    if not _column_exists(db, "annual_report_score_hits", "context"):
        db.exec("ALTER TABLE annual_report_score_hits ADD COLUMN context TEXT NULL")

    # Add context_sentence column if missing (newer reports use this)
    if not _column_exists(db, "annual_report_score_hits", "context_sentence"):
        db.exec("ALTER TABLE annual_report_score_hits ADD COLUMN context_sentence TEXT NULL")

    # Backward compatibility: some older builds used example_text
    if not _column_exists(db, "annual_report_score_hits", "example_text"):
        db.exec("ALTER TABLE annual_report_score_hits ADD COLUMN example_text LONGTEXT NULL")


# -------------------------
# Scoring logic
# -------------------------
@dataclass
class Hit:
    keyword: str
    weight: int
    count: int
    polarity: str  # 'pos'|'neg'
    # Human-readable example text to show in reports (sentence/snippet).
    # We write the same value into both `context` and `context_sentence` columns for compatibility.
    context: Optional[str] = None
    context_sentence: Optional[str] = None


def load_keywords(keyword_path: Path) -> Dict[str, Dict[str, int]]:
    global _CAGR_RULES
    if keyword_path.exists():
        data = json.loads(keyword_path.read_text(encoding="utf-8"))
        pos = data.get("positive", {})
        neg = data.get("negative", {})
        if not isinstance(pos, dict) or not isinstance(neg, dict):
            raise RuntimeError("keywords json must include dict fields: positive / negative")

        # Force int values
        pos2 = {str(k): int(v) for k, v in pos.items()}
        neg2 = {str(k): int(v) for k, v in neg.items()}

        # Load optional CAGR rules (fully override previous run)
        rules: List[Dict[str, Any]] = []

        cagr_rules = data.get("cagr_rules", None)
        cagr_rule = data.get("cagr_rule", None)

        # New format: list of rules
        if isinstance(cagr_rules, list):
            for item in cagr_rules:
                if isinstance(item, dict):
                    merged = dict(DEFAULT_CAGR_RULE)
                    merged.update(item)
                    rules.append(merged)

        # Backward-compatible format: single dict
        if not rules and isinstance(cagr_rule, dict):
            merged = dict(DEFAULT_CAGR_RULE)
            merged.update(cagr_rule)
            rules.append(merged)

        if not rules:
            rules = [dict(DEFAULT_CAGR_RULE)]

        _CAGR_RULES = rules

        try:
            brief = []
            for rr in _CAGR_RULES:
                brief.append(
                    f"phrase={rr.get('phrase')} enabled={rr.get('enabled')} mode={rr.get('mode')} k={rr.get('k')} cap={rr.get('cap')} min_pct={rr.get('min_pct')}"
                )
            print(f"[score] CAGR rules: {len(_CAGR_RULES)} | " + " | ".join(brief))
        except Exception:
            pass

        # Log what is actually loaded (so we can prove which file is used)
        try:
            print(
                f"[score] loaded keywords from: {keyword_path} (pos={len(pos2)}, neg={len(neg2)})"
            )
        except Exception:
            pass

        return {"positive": pos2, "negative": neg2}

    # Fallback to defaults
    try:
        print(f"[score] keywords file not found, using DEFAULT_KEYWORDS: {keyword_path}")
    except Exception:
        pass
    _CAGR_RULES = [dict(DEFAULT_CAGR_RULE)]
    return DEFAULT_KEYWORDS


def _mask_spans(text: str, needle: str) -> Tuple[str, int]:
    """Count needle occurrences and mask them to prevent overlap double-count."""
    if not needle:
        return text, 0
    cnt = 0
    start = 0
    out = []
    n = len(needle)
    while True:
        idx = text.find(needle, start)
        if idx < 0:
            out.append(text[start:])
            break
        cnt += 1
        out.append(text[start:idx])
        out.append("\u2588" * n)  # mask block
        start = idx + n
    return "".join(out), cnt



# --- Helper: extract sentence/snippet for keyword context ---
def _extract_sentence_around(text: str, idx: int, stop_chars: str, max_len: int = 1800) -> str:
    """Return the clause containing position idx.

    We treat Chinese punctuation as boundaries: 。！？； and also English '.' ';' '?' '!'.
    This is intentionally broader than a single line to keep keyword context complete.

    The returned string is whitespace-normalized and length-capped.
    """
    if not text:
        return ""

    # Sentence boundaries should be real punctuation only.
    # IMPORTANT: do NOT treat '.' as a boundary because it often appears in decimals like 39.24%.
    boundary_chars = set((stop_chars or "")) | {"。", "！", "？", "；", ";", "?", "!"}
    boundary_chars.discard("\n")
    boundary_chars.discard("\r")

    n = len(text)
    idx = max(0, min(idx, n - 1))

    # --- left boundary: nearest sentence-ending punctuation (do NOT use blank lines) ---
    left = 0
    for ch in boundary_chars:
        p = text.rfind(ch, 0, idx)
        if p >= 0:
            left = max(left, p + 1)

    # --- right boundary: nearest sentence-ending punctuation (do NOT use blank lines) ---
    right = n
    for ch in boundary_chars:
        p = text.find(ch, idx)
        if p >= 0:
            right = min(right, p)

    sent = text[left:right].strip()

    # Normalize whitespace but keep content continuous (do not break on hard line wraps).
    sent = re.sub(r"\s+", " ", sent).strip()

    # Cap to keep DB/PDF sane; keep tail ellipsis.
    if max_len and len(sent) > max_len:
        # If the sentence is extremely long (often due to PDF layout / table-like noise),
        # keep a centered window around the match position when possible.
        # This helps keep CAGR hits concise and prevents dumping whole pages.
        half = max_len // 2
        # Map idx to the sliced sentence coordinate space (best-effort)
        local_idx = max(0, min(len(sent) - 1, idx - left))
        start = max(0, local_idx - half)
        end = min(len(sent), start + max_len)
        start = max(0, end - max_len)
        clipped = sent[start:end].strip()
        if start > 0:
            clipped = "…" + clipped
        if end < len(sent):
            clipped = clipped.rstrip() + "…"
        sent = clipped

    return sent


def _collect_keyword_sentences(text: str, kw: str, stop_chars: str, max_sentences: int = 5) -> Optional[str]:
    """Collect up to max_sentences distinct sentences containing kw.

    Used for report display. If nothing found, returns None.
    """
    if not text or not kw:
        return None
    out: List[str] = []
    # Use literal match (same semantics as _mask_spans)
    start = 0
    while len(out) < max_sentences:
        pos = text.find(kw, start)
        if pos < 0:
            break
        s = _extract_sentence_around(text, pos, stop_chars)
        if s and s not in out:
            out.append(s)
        start = pos + len(kw)

    if not out:
        return None
    merged = " | ".join(out)
    if len(merged) > 4000:
        merged = merged[:4000].rstrip() + "…"
    return merged


def _looks_table_noise(s: str) -> bool:
    """Heuristic: detect table-like / directory-like noisy snippets.
    If true, we should not dump the whole clause; instead use a short window around the match.
    """
    if not s:
        return False
    t = re.sub(r"\s+", " ", s)

    digits = sum(ch.isdigit() for ch in t)
    pct = t.count("%")
    pipes = t.count("|")
    commas = t.count(",") + t.count("，")

    if re.search(r"(单位[:：]|同比增减|金额|占比|毛利率|营业收入|营业成本|现金流|项目|合计|本期数|上期数)", t):
        return True

    if len(t) >= 260 and (digits / max(1, len(t)) > 0.22):
        return True

    if pct >= 3 and len(t) >= 180:
        return True

    if pipes >= 2 and len(t) >= 140:
        return True

    if commas >= 12 and len(t) >= 220:
        return True

    return False


def _extract_window_around(text: str, idx: int, window: int = 180) -> str:
    """Fallback snippet: fixed window around idx, whitespace-normalized."""
    if not text:
        return ""
    n = len(text)
    idx = max(0, min(idx, n - 1))
    half = max(20, window // 2)
    a = max(0, idx - half)
    b = min(n, idx + half)
    s = text[a:b]
    s = re.sub(r"\s+", " ", s).strip()
    if a > 0:
        s = "…" + s
    if b < n:
        s = s.rstrip() + "…"
    return s


def score_cagr_rules(text: str) -> Tuple[int, List[Hit]]:
    """Extra scoring rule for CAGR mentions.

    Config key: cagr_rule/cagr_rules in scoring_keywords.json

    Behavior:
    - Find occurrences of phrase (default "复合增长率").
    - Look forward within a limited window and within the same sentence/line
      (stop at any of stop_chars) for the FIRST percentage number.
    - Score by:
        mode=linear: score = floor(pct * k), then apply cap/min_pct
        mode=thresholds: pick the largest threshold ge <= pct and use its score
    - Each hit is recorded as a synthetic positive Hit so it can be displayed.
    """
    if not text:
        return 0, []

    rules = _CAGR_RULES if isinstance(_CAGR_RULES, list) and _CAGR_RULES else [dict(DEFAULT_CAGR_RULE)]

    total = 0
    hits: List[Hit] = []

    pct_re = re.compile(r"(\d+(?:\.\d+)?)\s*%")

    for r in rules:
        if not isinstance(r, dict):
            continue
        if not r.get("enabled", False):
            continue

        phrase = str(r.get("phrase") or DEFAULT_CAGR_RULE.get("phrase") or "复合增长率")
        mode = str(r.get("mode") or "linear").lower()
        window_chars = int(r.get("window_chars") or 220)
        stop_chars = str(r.get("stop_chars") or "。！？!?；;")
        # Never stop at commas for CAGR scanning/context (we only stop at sentence-ending punctuation)
        stop_chars = stop_chars.replace(",", "").replace("，", "")
        stop_re = re.compile("[" + re.escape(stop_chars) + "]")

        k = float(r.get("k") or 0.1)
        cap = int(r.get("cap") or 30)
        min_pct = float(r.get("min_pct") or 0)

        # If CAGR phrase is in a "negative" sentence (e.g., cost/expense increase), we can flip the score.
        # Config: negate_if_contains: ["成本","费用",...]
        negate_if_contains = r.get("negate_if_contains", ["成本", "费用"])
        if not isinstance(negate_if_contains, list):
            negate_if_contains = ["成本", "费用"]
        negate_if_contains = [str(x) for x in negate_if_contains if str(x).strip()]

        # thresholds mode support
        thr_list: List[Tuple[float, int]] = []
        thresholds = r.get("thresholds", [])
        if isinstance(thresholds, list):
            for item in thresholds:
                if isinstance(item, dict) and "ge" in item and "score" in item:
                    try:
                        thr_list.append((float(item["ge"]), int(item["score"])))
                    except Exception:
                        pass
        thr_list.sort(key=lambda x: x[0])

        agg: Dict[str, Hit] = {}
        for m in re.finditer(re.escape(phrase), text):
            start = m.end()
            tail = text[start : start + window_chars]
            stop_m = stop_re.search(tail)
            if stop_m:
                tail = tail[: stop_m.start()]

            pm = pct_re.search(tail)
            if not pm:
                continue

            try:
                val = float(pm.group(1))
            except Exception:
                continue

            pts = 0
            label = ""

            if mode == "thresholds":
                best_sc = 0
                best_ge: Optional[float] = None
                for ge, sc in thr_list:
                    if val >= ge:
                        best_sc = sc
                        best_ge = ge
                if best_ge is None or best_sc <= 0:
                    continue
                ge_txt = str(int(best_ge)) if float(best_ge).is_integer() else str(best_ge)
                pts = best_sc
                label = f">={ge_txt}%"
            else:
                # linear
                if val < min_pct:
                    continue
                pts = int(val * k)
                if pts <= 0:
                    continue
                if pts > cap:
                    pts = cap
                label = f"={val:g}%"

            # Context: prefer the clause containing the *percentage* (more specific than phrase).
            # IMPORTANT: clause boundaries are only 。！？；; (NOT commas), otherwise context gets cut too early.
            abs_pct_idx = m.end() + pm.start()
            ctx = _extract_sentence_around(text, abs_pct_idx, "。！？!?；;", max_len=1800)
            if _looks_table_noise(ctx):
                ctx = _extract_window_around(text, abs_pct_idx, window=220)

            # If the same sentence indicates cost/expense increase, treat it as negative.
            pol = "pos"
            if ctx and negate_if_contains:
                compact_ctx = re.sub(r"\s+", "", ctx)
                for bad in negate_if_contains:
                    if bad and bad in compact_ctx:
                        pts = -abs(int(pts))
                        pol = "neg"
                        break

            kw_label = f"{phrase}{label}"
            total += pts
            if kw_label in agg:
                agg_hit = agg[kw_label]
                agg_hit.count += 1
                # If this occurrence flips polarity (e.g., cost/expense context), keep the stronger negative weight.
                if pol == "neg" and agg_hit.polarity != "neg":
                    agg_hit.polarity = "neg"
                    agg_hit.weight = pts
                # Do NOT concatenate multiple contexts for CAGR hits; it quickly becomes unreadable
                # (and often duplicates the same long sentence).
                if not (agg_hit.context_sentence or agg_hit.context):
                    if ctx:
                        agg_hit.context = ctx
                        agg_hit.context_sentence = ctx
            else:
                agg[kw_label] = Hit(
                    keyword=kw_label,
                    weight=pts,
                    count=1,
                    polarity=pol,
                    context=ctx or None,
                    context_sentence=ctx or None,
                )
        hits.extend(agg.values())

    return total, hits


def score_text(text: str, keywords: Dict[str, Dict[str, int]]) -> Tuple[int, List[Hit]]:
    """Return total score and detailed hits."""
    if not text:
        return 0, []

    original_text = text
    stop_chars_for_ctx = "。！？!?；;"

    # Extra CAGR rule scoring (synthetic hits)
    cagr_score, cagr_hits = score_cagr_rules(text)

    # Build a single list of (kw, weight, polarity) and sort by kw length desc
    items: List[Tuple[str, int, str]] = []
    for kw, w in keywords.get("positive", {}).items():
        items.append((kw, int(w), "pos"))
    for kw, w in keywords.get("negative", {}).items():
        items.append((kw, int(w), "neg"))

    # Longest-first to reduce overlap issues
    items.sort(key=lambda x: len(x[0]), reverse=True)

    working = text
    hits: List[Hit] = list(cagr_hits)
    total = int(cagr_score)
    for kw, w, pol in items:
        working, cnt = _mask_spans(working, kw)
        if cnt > 0:
            total += w * cnt
            ctx = _collect_keyword_sentences(original_text, kw, stop_chars_for_ctx, max_sentences=3)
            hits.append(
                Hit(
                    keyword=kw,
                    weight=w,
                    count=cnt,
                    polarity=pol,
                    context=ctx,
                    context_sentence=ctx,
                )
            )

    return total, hits


def _merge_report_text(row: Dict[str, Any]) -> str:
    parts = []
    for k in (
        "chairman_letter",
        "industry_section",
        "main_business_section",
        "future_section",
        "full_mda",
    ):
        v = row.get(k)
        if isinstance(v, str) and v.strip():
            parts.append(v)
    # Keep sections separated
    return "\n\n".join(parts)


def fetch_reports_to_score(
    db: MySQL,
    since_days: int,
    report_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    if report_id is not None:
        sql = """
        SELECT r.id AS report_id, r.stock_code, r.stock_name, r.report_year, r.publish_date,
               m.chairman_letter, m.industry_section, m.main_business_section, m.future_section, m.full_mda
        FROM annual_reports r
        JOIN annual_report_mda m ON m.report_id = r.id
        WHERE r.id = %s
        """
        return db.query(sql, (report_id,))

    # Score recent reports by publish_date
    since_date = (datetime.now() - timedelta(days=since_days)).date()
    sql = """
    SELECT r.id AS report_id, r.stock_code, r.stock_name, r.report_year, r.publish_date,
           m.chairman_letter, m.industry_section, m.main_business_section, m.future_section, m.full_mda
    FROM annual_reports r
    JOIN annual_report_mda m ON m.report_id = r.id
    WHERE r.publish_date >= %s
    ORDER BY r.publish_date DESC, r.id DESC
    """
    return db.query(sql, (since_date,))


def upsert_score(
    db: MySQL,
    report_id: int,
    total_score: int,
    hits: List[Hit],
    dry_run: bool = False,
):
    if dry_run:
        return

    # Update annual_reports
    db.exec(
        "UPDATE annual_reports SET score=%s, score_updated_at=NOW() WHERE id=%s",
        (total_score, report_id),
    )

    # Overwrite mode: remove old keyword hits so removed/changed keywords won't linger
    db.exec("DELETE FROM annual_report_score_hits WHERE report_id=%s", (report_id,))

    # Upsert hit rows
    for h in hits:
        # Prefer sentence-level context; fall back to snippet; keep both columns aligned.
        ctx = (h.context_sentence or h.context or None)

        db.exec(
            """
            INSERT INTO annual_report_score_hits (
              report_id, keyword, weight, hit_count, polarity,
              context, context_sentence, example_text
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              weight=VALUES(weight),
              hit_count=VALUES(hit_count),
              polarity=VALUES(polarity),
              context=VALUES(context),
              context_sentence=VALUES(context_sentence),
              example_text=VALUES(example_text),
              updated_at=NOW()
            """,
            (report_id, h.keyword, h.weight, h.count, h.polarity, ctx, ctx, ctx),
        )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--config",
        default=str(_project_root() / "conf" / "config.ini"),
        help="Path to config.ini containing [mysql]",
    )
    ap.add_argument(
        "--keywords",
        default="",
        help=(
            "Path to scoring_keywords.json (optional). "
            "If omitted, will try repo_root/scoring_keywords.json then conf/scoring_keywords.json"
        ),
    )
    ap.add_argument(
        "--since-days",
        type=int,
        default=60,
        help="Score reports whose publish_date is within N days (default 60)",
    )
    ap.add_argument(
        "--report-id",
        type=int,
        default=None,
        help="Score only a single report id",
    )
    ap.add_argument("--dry-run", action="store_true", help="Do not write DB")

    args = ap.parse_args()

    cfg = load_mysql_cfg(Path(args.config))

    # Resolve keyword file path.
    # Priority:
    #   1) --keywords (if provided and exists)
    #   2) repo_root/scoring_keywords.json (common when edited in IDE root)
    #   3) conf/scoring_keywords.json (legacy/default location)
    repo_root = _project_root()
    kw_candidates = []

    if str(args.keywords).strip():
        kw_candidates.append(Path(args.keywords).expanduser())

    kw_candidates.append(repo_root / "scoring_keywords.json")
    kw_candidates.append(repo_root / "conf" / "scoring_keywords.json")

    kw_path = None
    for p in kw_candidates:
        if p.exists():
            kw_path = p
            break

    # If none exist, keep the last candidate so load_keywords prints the helpful message.
    if kw_path is None:
        kw_path = kw_candidates[-1]

    print(f"[score] keywords path: {kw_path} (exists={kw_path.exists()})")
    keywords = load_keywords(kw_path)

    # Print the actual keyword lists (truncated) for verification
    try:
        pos_keys = list(keywords.get("positive", {}).keys())
        neg_keys = list(keywords.get("negative", {}).keys())
        print(f"[score] positive keywords: {pos_keys}")
        print(f"[score] negative keywords: {neg_keys}")
    except Exception:
        pass

    db = MySQL(cfg)
    db.connect()
    try:
        ensure_schema(db)

        rows = fetch_reports_to_score(db, since_days=args.since_days, report_id=args.report_id)
        if not rows:
            print("[score] no reports to score")
            return

        print(f"[score] reports to score: {len(rows)}")

        for r in rows:
            rid = int(r["report_id"])
            merged = _merge_report_text(r)
            total, hits = score_text(merged, keywords)

            # Simple console summary
            stock_code = r.get("stock_code")
            year = r.get("report_year")
            print(f"[score] {stock_code}-{year} report_id={rid} score={total} hits={len(hits)}")
            # Print CAGR matches with context
            cagr_prefixes = [str(rr.get("phrase") or "") for rr in _CAGR_RULES if isinstance(rr, dict)]
            for hh in hits:
                if hh.polarity == "pos" and isinstance(hh.keyword, str) and any(p and hh.keyword.startswith(p) for p in cagr_prefixes):
                    if hh.context:
                        print(f"  [cagr_hit] {hh.keyword} x{hh.count} :: {hh.context}")

            upsert_score(db, rid, total, hits, dry_run=bool(args.dry_run))

        if args.dry_run:
            print("[score] dry-run done (no db write)")
        else:
            print("[score] done")

    finally:
        db.close()


if __name__ == "__main__":
    main()