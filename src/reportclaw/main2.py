import os
import re
import json
from dataclasses import dataclass, field
from typing import Any
from pathlib import Path

import pdfplumber
import configparser

# ... other imports and code ...

# Project paths
# Allow running from repo root or from src/.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
STATE_DIR = DATA_DIR / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

# ===============================
# 文档结构树（大纲）构建：标题 -> 树
# ===============================

@dataclass
class OutlineNode:
    """A structural node (heading) detected from PDF text/layout."""
    level: int
    title: str
    page_idx: int
    y_top: float | None = None
    numbering: str | None = None
    font_size: float | None = None
    font_name: str | None = None
    children: list["OutlineNode"] = field(default_factory=list)


def normalize_line_text(s: str) -> str:
    """Normalize PDF-extracted line text.

    Removes NUL characters and common spacing artifacts.
    """
    if not s:
        return ""
    s = s.replace("\x00", "").replace("\u0000", "")
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    # Join CJK characters split by spaces
    s = re.sub(r"(?<=[\u4e00-\u9fff])\s+(?=[\u4e00-\u9fff])", "", s)
    # Trim spaces around common punctuation
    s = re.sub(r"\s*([:：,，。；;、])\s*", r"\1", s)
    return s.strip()


def infer_heading_level(title: str) -> tuple[int, str | None]:
    """Infer heading level by numbering pattern.

    Returns: (level, numbering)
      level=1: 章/节/一、/1、
      level=2: （一）/(1)/1.1/2.3
      level=3: 1.1.1/2.3.1/（1）/1）/①
      level=4+: fallback
    """
    t = (title or "").strip()

    # Chapter/section style
    m = re.match(r"^第\s*([一二三四五六七八九十]{1,3}|\d{1,2})\s*[章节]", t)
    if m:
        return 1, m.group(0)

    # 一级：中文序号 一、二、三、...
    m = re.match(r"^([一二三四五六七八九十]{1,3})\s*[、:：]", t)
    if m:
        return 1, m.group(1)

    # 二级：阿拉伯序号 1、2、3、...（年报里通常是节内小标题）
    m = re.match(r"^(\d{1,2})\s*[、:：]", t)
    if m:
        return 2, m.group(1)

    # 二级：括号序号（（一）/(1)）
    m = re.match(r"^[（(]\s*([一二三四五六七八九十]{1,3}|\d{1,2})\s*[）)]", t)
    if m:
        return 2, m.group(1)

    # 二级：点分 1.1 / 2.3 / 1．1
    m = re.match(r"^(\d{1,2})([\.．])(\d{1,2})\b", t)
    if m:
        return 2, m.group(0)

    # 三级：点分 1.1.1 / 2.3.1
    m = re.match(r"^(\d{1,2})([\.．])(\d{1,2})\2(\d{1,2})\b", t)
    if m:
        return 3, m.group(0)

    # 三级：1）/2）
    m = re.match(r"^(\d{1,2})\s*[）)]", t)
    if m:
        return 3, m.group(1)

    # 三级：①②③
    m = re.match(r"^[①②③④⑤⑥⑦⑧⑨⑩]", t)
    if m:
        return 3, m.group(0)

    return 4, None


def build_outline_tree(flat_nodes: list[OutlineNode]) -> list[OutlineNode]:
    """Build a heading tree from a flat, ordered node list."""
    root: list[OutlineNode] = []
    stack: list[OutlineNode] = []

    for n in flat_nodes:
        lvl = max(1, min(int(n.level or 4), 6))
        n.level = lvl

        while stack and stack[-1].level >= n.level:
            stack.pop()

        if not stack:
            root.append(n)
            stack.append(n)
        else:
            stack[-1].children.append(n)
            stack.append(n)

    return root


def print_outline_tree(nodes: list[OutlineNode], *, max_nodes: int = 200) -> None:
    """Pretty print tree for debugging."""
    cnt = 0

    def _walk(ns: list[OutlineNode], depth: int):
        nonlocal cnt
        for x in ns:
            if cnt >= max_nodes:
                return
            indent = "  " * depth
            loc = f"p{x.page_idx+1}" if x.page_idx is not None else "p?"
            title = (x.title or "").strip().replace("\n", " ")
            if len(title) > 90:
                title = title[:90] + "…"
            print(f"{indent}- L{x.level} [{loc}] {title}")
            cnt += 1
            if x.children:
                _walk(x.children, depth + 1)

    _walk(nodes, 0)


# ... other code ...

class AnnualReportParser:
    # ... other methods ...

    def extract_text(self, pdf_path: str) -> str:
        """Basic full-text extraction (debug helper)."""
        with pdfplumber.open(pdf_path) as pdf:
            parts: list[str] = []
            for p in pdf.pages:
                t = p.extract_text() or ""
                if t:
                    parts.append(t)
            return "\n".join(parts)

    # ===============================
    # 结构树/大纲：从PDF正文识别标题并构建树
    # ===============================
    def build_outline_flat(self, pdf_path: str, *, max_pages: int = 80) -> list[OutlineNode]:
        """Build a flat list of heading candidates ordered by (page, y)."""
        flat: list[OutlineNode] = []
        # Front-matter sections: keep as L1 only, suppress collecting L2/L3 under them
        suppress_sublevels = False
        suppress_l1_keywords = (
            "重要提示", "重要提示、目录和释义", "重要提示、目录和释义", "目录", "目 录", "释义", "词汇表", "术语表", "名词解释"
        )
        toc_titles: list[tuple[str, int]] = []  # (title, page_idx) captured from TOC pages for later validation
        try:
            with pdfplumber.open(pdf_path) as pdf:
                n_pages = min(len(pdf.pages), max_pages)
                for pno in range(n_pages):
                    page = pdf.pages[pno]

                    try:
                        chars = page.chars or []
                    except Exception:
                        chars = []

                    lines: list[dict[str, Any]] = []
                    if chars:
                        buckets: dict[int, list[dict[str, Any]]] = {}
                        for ch in chars:
                            t = ch.get("top")
                            if t is None:
                                continue
                            key = int(round(float(t)))
                            buckets.setdefault(key, []).append(ch)

                        page_w = float(getattr(page, "width", 0.0) or 0.0)
                        for key in sorted(buckets.keys()):
                            row = buckets[key]
                            row.sort(key=lambda c: c.get("x0", 0.0))

                            txt = normalize_line_text("".join(c.get("text", "") for c in row))
                            if not txt:
                                continue

                            # Basic geometry
                            xs0 = [float(c.get("x0", 0.0)) for c in row if c.get("x0") is not None]
                            xs1 = [float(c.get("x1", 0.0)) for c in row if c.get("x1") is not None]
                            x0 = min(xs0) if xs0 else None
                            x1 = max(xs1) if xs1 else None

                            # Font stats
                            sizes = [float(c.get("size", 0.0)) for c in row if c.get("size") is not None]
                            fs = sum(sizes) / len(sizes) if sizes else None
                            fn = None
                            for c in row:
                                if c.get("fontname"):
                                    fn = str(c.get("fontname"))
                                    break

                            # Bold heuristic
                            is_bold = False
                            if fn:
                                fn_l = fn.lower()
                                # Keep conservative; avoid overly-broad matches that cause body text to be treated as headings.
                                is_bold = ("bold" in fn_l) or ("black" in fn_l) or ("heiti" in fn_l) or ("simhei" in fn_l)

                            # Centered heuristic (useful for real headings)
                            is_centered = False
                            if page_w and x0 is not None and x1 is not None:
                                mid = (x0 + x1) / 2.0
                                is_centered = abs(mid - page_w / 2.0) <= max(18.0, page_w * 0.06)

                            lines.append({
                                "text": txt,
                                "y": float(key),
                                "x0": x0,
                                "x1": x1,
                                "centered": is_centered,
                                "bold": is_bold,
                                "font_size": fs,
                                "font_name": fn,
                            })

                    if not lines:
                        t = page.extract_text() or ""
                        for i, ln in enumerate(t.splitlines()):
                            s = normalize_line_text(ln or "")
                            if not s:
                                continue
                            lines.append({"text": s, "y": float(i), "font_size": None, "font_name": None})

                    # Detect table-of-contents page: contains '目录/目 录' and many section-like entries
                    is_toc_page = False
                    has_toc_word = any((normalize_line_text(x.get("text") or "") in {"目录", "目 录"} or "目录" in (x.get("text") or "")) for x in lines)
                    if has_toc_word:
                        sec_like = 0
                        for x in lines:
                            st = normalize_line_text(x.get("text") or "")
                            if not st:
                                continue
                            if re.match(r"^第\s*([一二三四五六七八九十]{1,3}|\d{1,2})\s*[章节]", st):
                                sec_like += 1
                            elif re.match(r"^[一二三四五六七八九十]{1,3}\s*[、:：]", st):
                                sec_like += 1
                        # If there are many section-like entries, treat as TOC
                        if sec_like >= 6:
                            is_toc_page = True

                    if is_toc_page:
                        # Record TOC entries for later validation, but do not pollute the structural outline.
                        for x in lines:
                            st = normalize_line_text(x.get("text") or "")
                            if not st:
                                continue
                            if st in {"目录", "目 录"}:
                                continue
                            if re.match(r"^第\s*([一二三四五六七八九十]{1,3}|\d{1,2})\s*[章节]", st):
                                toc_titles.append((st, pno))
                        # Keep only a single L1 node '目录' for this page and skip the rest of the page.
                        flat.append(OutlineNode(level=1, title="目录", page_idx=pno, y_top=0.0))
                        continue

                    fs_list = [x["font_size"] for x in lines if x.get("font_size")]
                    fs_th = None
                    if fs_list:
                        fs_sorted = sorted(fs_list)
                        # Use higher percentile to reduce false positives
                        fs_th = fs_sorted[int(len(fs_sorted) * 0.90)]

                    # Page geometry for header/footer suppression
                    page_h = float(getattr(page, "height", 0.0) or 0.0)

                    def _numeric_ratio(text: str) -> float:
                        if not text:
                            return 0.0
                        digits = sum(ch.isdigit() for ch in text)
                        return digits / max(1, len(text))

                    # Common boilerplate headings we do NOT want in outline
                    _skip_exact = {
                        "目录", "目 录", "释义", "词汇表", "术语表", "重要提示", "重要提示、目录和释义",
                        "公司简介", "公司概况", "公司基本情况", "公司基本情况简介",
                        "备查文件目录", "本报告分别以中英文两种文字编制",
                    }

                    for ln in lines:
                        s = normalize_line_text((ln.get("text") or ""))
                        if not s:
                            continue
                        # Normalize for pattern checks (remove spaces/tabs/newlines)
                        s_norm = re.sub(r"\s+", "", s)
                        # Extra suppression for cover/important-notice pages
                        # Many PDFs have dense boilerplate on the first pages that should never be treated as outline.
                        if pno <= 1:
                            # Drop company code/stock code/abbr by regex
                            if re.search(r"公司代码\s*[:：]|股票代码\s*[:：]|证券代码\s*[:：]|公司简称\s*[:：]", s):
                                continue
                            if any(k in s for k in [
                                "公司代码", "公司简称", "股票代码", "证券代码",
                                "公司负责人", "主管会计", "会计机构负责人", "会计主管人员",
                                "标准无保留", "审计报告", "会计师事务所",
                                "利润分配", "现金红利", "不派发", "不送红股", "转增股本",
                                "是否存在", "非经营性占用资金", "对外提供担保", "治理特殊安排",
                                "真实性", "准确性", "完整性", "重大遗漏",
                                "前瞻性", "不构成", "投资者", "风险提示",
                                "www.", "http://", "https://", "@",
                            ]):
                                continue
                            # Drop long sentence-like lines on first pages
                            if len(s) >= 18 and any(p in s for p in ["，", "。", "；", ";"]):
                                continue

                        # Suppress likely header/footer lines
                        y = ln.get("y")
                        if page_h and y is not None:
                            if float(y) <= 35.0:
                                continue
                            if float(y) >= page_h - 35.0:
                                continue

                        # Hard length limits
                        if len(s) > 90:
                            continue

                        # Skip pure numeric / punctuation lines (percentages, amounts, ratios)
                        if re.fullmatch(r"[\d\.,%/（）()]+", s) and len(s) >= 3:
                            continue
                        if re.fullmatch(r"\d+(\.\d+)?%", s):
                            continue

                        # Skip pure page numbers / roman numerals like '10' '19/219'
                        if re.fullmatch(r"\d{1,4}(\s*/\s*\d{1,4})?", s):
                            continue

                        # Skip exact boilerplate
                        if s in _skip_exact:
                            continue
                        # Typical SSE compliance Q&A headings (not business content)
                        if "是否存在" in s:
                            if any(k in s for k in ["占用资金", "担保", "治理", "无法保证", "重大事项", "关联方"]):
                                continue
                            # Many of these end with “情况/事项”，but not always; keep a fallback
                            if re.search(r"是否存在.*(情况|事项)", s):
                                continue
                        if any(k in s for k in ["非经营性占用资金", "对外提供担保", "公司治理特殊安排", "违反规定决策程序"]):
                            continue

                        # Skip obvious URLs / emails / stock-code header lines
                        if re.search(r"\bwww\.", s, flags=re.IGNORECASE) or re.search(r"https?://", s, flags=re.IGNORECASE):
                            continue
                        if re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", s):
                            continue
                        if any(k in s for k in ["公司代码", "公司简称", "股票代码", "证券代码", "公司外文名称", "公司英文名称"]):
                            continue
                        # Also drop the common combined cover line: "公司代码：公司简称：XXX"
                        if re.search(r"公司代码\s*[:：].*公司简称\s*[:：]", s):
                            continue

                        # Skip typical report title lines
                        if "年度报告" in s and len(s) <= 30:
                            continue

                        # Skip lines that are mostly numbers/punctuation (amounts, tables)
                        if _numeric_ratio(s) >= 0.28 and len(s) >= 10:
                            continue
                        if re.search(r"\d{1,3}(,\d{3})+(\.\d+)?", s):
                            continue
                        if re.fullmatch(r"\d+(?:\.\d+)?(?:,\d+)*(?:\.\d+)?", s) and len(s) >= 8:
                            continue
                        if re.search(r"\d+\.\d+\.\d+\.\d+", s):
                            continue

                        # Skip obvious table-unit lines
                        if re.fullmatch(r"(单位\s*[:：].{0,6})", s):
                            continue

                        # If it looks like a sentence (contains comma/period/semicolon) and is not very short, it's probably正文而不是标题
                        if len(s) >= 24 and any(p in s for p in ["，", "。", "；", ";"]):
                            # allow centered short headings even with punctuation
                            if not (ln.get("centered") and len(s) <= 30):
                                continue

                        lvl, num = infer_heading_level(s)

                        # Promote front-matter headings to level-1
                        if any(k in s for k in suppress_l1_keywords):
                            lvl = 1

                        # When we see a new L1 heading, decide whether to suppress sublevels until next L1
                        if lvl == 1:
                            suppress_sublevels = any(k in s for k in suppress_l1_keywords)
                        else:
                            # If we are inside front-matter sections, ignore all L2/L3 candidates
                            if suppress_sublevels:
                                continue

                        # Aggressive suppression for non-numbered level-4 candidates (often body/table/glossary noise)
                        if lvl >= 4 and not num:
                            # Only keep very short centered/bold “true headings” containing key words
                            key_heading = re.search(
                                r"(管理层讨论与分析|董事会报告|董事长致辞|公司简介|重要事项|财务报告|公司治理|未来|展望|业务展望|发展战略|经营计划)",
                                s)
                            if not key_heading:
                                continue
                            if len(s) > 26:
                                continue
                            if not (ln.get("centered") or ln.get("bold")):
                                continue

                        # Reduce noisy "single-number" headings like 1、/2、 with no content.
                        if re.fullmatch(r"\s*([一二三四五六七八九十]{1,3}|\d{1,2})\s*[、\.．:：]\s*", s):
                            continue
                        if re.fullmatch(r"\s*\d{1,2}\s*\.?\s*", s):
                            continue
                        # Normalized variants (handles invisible spaces)
                        if s_norm in {"1、", "2、", "3、", "4、", "5、", "6、", "7、", "8、", "9、", "10、", "1.", "2.", "3.", "4.", "5.", "6.", "7.", "8.", "9.", "10."}:
                            continue
                        looks_numbered = (lvl <= 3 and num is not None)
                        if looks_numbered:
                            # Numbered headings should be short; long sentences are usually正文/提示条款
                            if lvl == 1 and len(s) > 45:
                                looks_numbered = False
                            elif lvl == 2 and len(s) > 60:
                                looks_numbered = False
                            elif lvl >= 3 and len(s) > 70:
                                looks_numbered = False

                            # Drop common “重要提示/声明” bullet items on the first few pages
                            if looks_numbered and pno <= 5:
                                if any(k in s for k in [
                                    "本公司", "董事会", "监事", "高级管理", "保证", "声明",
                                    "会计师", "无保留", "审计", "真实性", "准确性", "完整性", "重大遗漏",
                                    "利润分配", "现金分红", "红利", "公积金", "转增", "不派发",
                                    "担保", "占用资金", "治理", "特殊安排", "前瞻性", "承诺", "风险提示", "投资风险",
                                    "是否存在","上市时未盈利","尚未实现盈利","现金分红","回购","扣除非经常"
                                ]):
                                    looks_numbered = False

                        looks_big_font = False
                        if fs_th and ln.get("font_size"):
                            big_enough = float(ln["font_size"]) >= float(fs_th)
                            if big_enough and len(s) <= 45 and (ln.get("bold") or ln.get("centered")):
                                looks_big_font = True

                        # Centered short lines are often headings
                        looks_centered = bool(ln.get("centered")) and len(s) <= 35

                        if not (looks_numbered or looks_big_font or looks_centered):
                            continue

                        flat.append(
                            OutlineNode(
                                level=lvl,
                                title=s,
                                page_idx=pno,
                                y_top=ln.get("y"),
                                numbering=num,
                                font_size=ln.get("font_size"),
                                font_name=ln.get("font_name"),
                            )
                        )
        except Exception:
            return flat

        flat.sort(key=lambda n: (n.page_idx, n.y_top if n.y_top is not None else 1e9))

        dedup: list[OutlineNode] = []
        for n in flat:
            if not dedup:
                dedup.append(n)
                continue
            prev = dedup[-1]
            if prev.page_idx == n.page_idx:
                if prev.title == n.title:
                    continue
                if prev.title and n.title and (prev.title in n.title or n.title in prev.title):
                    if len(n.title) > len(prev.title):
                        dedup[-1] = n
                    continue
                # Only drop near-same-line duplicates if they look like repeated header/footer patterns
                if prev.y_top is not None and n.y_top is not None and abs(prev.y_top - n.y_top) <= 1.5:
                    if prev.title == n.title:
                        continue
            dedup.append(n)

        self._last_toc_titles = toc_titles
        return dedup

    def build_outline(self, pdf_path: str, *, max_pages: int = 80) -> list[OutlineNode]:
        """Build a tree outline from detected headings."""
        flat = self.build_outline_flat(pdf_path, max_pages=max_pages)
        return build_outline_tree(flat)

    def debug_outline(self, pdf_path: str, *, max_pages: int = 80, max_nodes: int = 200) -> None:
        """Print the outline tree for debugging."""
        tree = self.build_outline(pdf_path, max_pages=max_pages)
        print_outline_tree(tree, max_nodes=max_nodes)
        # Print TOC summary (captured from TOC pages)
        try:
            toc = getattr(self, "_last_toc_titles", [])
            if toc:
                uniq = []
                seen = set()
                for t, p in toc:
                    if t not in seen:
                        seen.add(t)
                        uniq.append((t, p))
                print("\n[outline_debug] TOC candidates (for validation):")
                for t, p in uniq[:40]:
                    print(f"  - [p{p+1}] {t}")
        except Exception:
            pass

# ... other code ...

def main():
    # ... other code ...

    import argparse
    parser_args = argparse.ArgumentParser()
    parser_args.add_argument("--config", required=True)
    # Optional: directly debug a single PDF without running the full crawler loop
    parser_args.add_argument("--pdf", default="", help="path to a PDF to print outline tree")
    parser_args.add_argument("--max-pages", type=int, default=30, help="max pages to scan for outline")
    parser_args.add_argument("--max-nodes", type=int, default=200, help="max outline nodes to print")
    args = parser_args.parse_args()

    cfg = configparser.ConfigParser()
    cfg.read(args.config, encoding="utf-8")

    # Read config values
    try:
        days_back = cfg.getint("crawler", "days_back", fallback=7)
        use_last_crawl = cfg.getboolean("crawler", "use_last_crawl", fallback=True)
        outline_debug = cfg.getboolean("crawler", "outline_debug", fallback=False)
        outline_debug_max_pages = cfg.getint("crawler", "outline_debug_max_pages", fallback=30)
    except Exception as e:
        print(f"[config] failed to read crawler settings: {e}")
        # Safe defaults
        days_back = 7
        use_last_crawl = True
        outline_debug = False
        outline_debug_max_pages = 30

    parser = AnnualReportParser()

    if outline_debug:
        print(f"[outline_debug] enabled, max_pages={outline_debug_max_pages}")

    # Direct outline debug for a single PDF (safe, does not touch DB/crawler)
    if args.pdf:
        pdf_path = str(Path(args.pdf).expanduser())
        if not Path(pdf_path).exists():
            raise SystemExit(f"[outline_debug] pdf not found: {pdf_path}")
        print(f"[outline_debug] printing outline for: {pdf_path}")
        parser.debug_outline(pdf_path, max_pages=args.max_pages, max_nodes=args.max_nodes)
        return

    print("[outline_debug] no --pdf provided; this tool only prints outline for a single PDF.")
    print("Example:")
    print("  PYTHONPATH=src ./venv/bin/python -m reportclaw.main2 --config conf/config.ini --pdf data/downloads/<file>.PDF --max-pages 40 --max-nodes 200")
    return


# Ensure script runs when executed directly
if __name__ == "__main__":
    main()
