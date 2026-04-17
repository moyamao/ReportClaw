from __future__ import annotations

import os
import re
from typing import Callable


def extract_between_markers(text: str, start_pat: str, end_pats: list[str], *, flags=re.MULTILINE):
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


def truncate_text(s: str | None, max_len: int) -> str | None:
    if not s:
        return s
    if len(s) <= max_len:
        return s
    return s[:max_len].rstrip() + "\n\n[...TRUNCATED...]"


def extract_alt_sections(
    pdf_path: str,
    *,
    max_pages: int,
    extract_text_fn: Callable[..., str],
    extract_between_markers_fn: Callable[..., str | None],
    truncate_text_fn: Callable[[str | None, int], str | None],
    extract_chairman_letter_fn: Callable[..., str | None],
) -> dict | None:
    """Fallback extractor for non-standard annual reports without standard MDA chapter."""
    try:
        raw = extract_text_fn(pdf_path, page_numbers=list(range(0, max_pages)))
    except Exception:
        raw = ""
    if not raw:
        return None

    t = raw

    def _drop_block(src: str, start_pat: str, end_pats: list[str]) -> str:
        blk = extract_between_markers_fn(src, start_pat, end_pats)
        return src.replace(blk, "") if blk else src

    t = _drop_block(t, r"(?:^|\n)目\s*录\b", [r"(?:^|\n)第一[章节]\b", r"(?:^|\n)第一节\b"])
    t = _drop_block(t, r"(?:^|\n)释\s*义\b", [r"(?:^|\n)(?:词\s*汇\s*表|第一[章节]|第一节)\b"])
    t = _drop_block(t, r"(?:^|\n)词\s*汇\s*表\b", [r"(?:^|\n)(?:第一[章节]|第一节)\b"])

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

    overview = extract_between_markers_fn(
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

    board = extract_between_markers_fn(
        t,
        r"(?:^|\n)(?:第二章\s*)?(?:董事会报告|董事会工作报告|董事会报告书)\b",
        [
            r"(?:^|\n)第三章\b",
            r"(?:^|\n)第三节\s*管理层讨论与分析\b",
            r"(?:^|\n)公司治理\b",
            r"(?:^|\n)重要事项\b",
        ],
    )

    summary_parts: list[str] = []
    for part in [overview, board]:
        if not part:
            continue
        p = part.strip()
        if not p:
            continue
        if any((p in x) or (x in p) for x in summary_parts):
            continue
        summary_parts.append(p)

    summary_text = "\n\n".join(summary_parts).strip() if summary_parts else None

    outlook = None
    m_head = re.search(
        r"(?:^|\n)\s*2\s*[\.．]\s*3(?:\s*[\.．]\s*\d+)?\s*[^\n]{0,80}?(?:2026\s*年?)?\s*业务展望[^\n]{0,120}?(?:经营风险|风险)?\b",
        t,
    )
    out_idx = None
    if m_head:
        out_idx = m_head.start()
    else:
        for kw in ["2026年业务展望", "2026年业务发展展望", "业务展望"]:
            idx = t.find(kw)
            if idx != -1:
                out_idx = idx
                break

    if out_idx is not None:
        line_start = t.rfind("\n", 0, out_idx)
        start = 0 if line_start < 0 else (line_start + 1)
        tail = t[out_idx:]
        end_patterns = [
            r"(?:\n\s*|\s)2\s*[\.．]\s*3\s*[\.．]\s*[2-9]",
            r"(?:\n\s*|\s)2\s*[\.．]\s*4\b",
            r"(?:\n\s*|\s)3\s*[\.．]\s*\d",
            r"2\s*[\.．]\s*4\b",
            r"3\s*[\.．]\s*\d\b",
            r"(?:\n)\s*(?:[一二三四五六七八九十]{1,3}|\d{1,2})[、\.．:：]",
            r"(?:\n)\s*第\s*[一二三四五六七八九十]{1,3}\s*[章节]",
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
            if cand <= start + 50:
                continue
            end = cand if end is None else min(end, cand)
        if end is None:
            end = len(t)
        outlook = t[start:end].strip()

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

    if summary_text:
        summary_text = truncate_text_fn(summary_text, 70000)
    if outlook:
        outlook = truncate_text_fn(outlook, 40000)

    if not summary_text and not outlook:
        return None

    full_parts = []
    if summary_text:
        full_parts.append(summary_text)
    if outlook:
        full_parts.append(outlook)
    full = "\n\n".join(full_parts)
    full = truncate_text_fn(full, 140000)

    chairman_letter = extract_chairman_letter_fn(pdf_path, max_pages=25)
    return {
        "industry": None,
        "chairman_letter": chairman_letter,
        "business": summary_text,
        "future": outlook,
        "full_mda": full,
    }


def build_fallback_mda(
    pdf_path: str,
    *,
    reason: str,
    page_count: int,
    extract_text_fn: Callable[..., str],
    extract_between_markers_fn: Callable[..., str | None],
    extract_chairman_letter_fn: Callable[..., str | None],
) -> dict:
    """Build fallback payload for DB when full MDA parsing fails."""
    if reason == "image_heavy_skip":
        pdf_name = os.path.basename(pdf_path)
        base = f"[PARSE_FAILED] reason={reason} pages={page_count}\n请查看原PDF：{pdf_name}"
        hint = "[PARSE_SKIPPED_IMAGE_HEAVY] 该PDF图文混排/图片占比高，文本抽取成本很高且成功率低；建议直接打开原PDF查看。"
        return {
            "industry": None,
            "business": None,
            "future": None,
            "chairman_letter": hint,
            "full_mda": base,
        }

    excerpt = ""
    try:
        probe_pages = list(range(0, min(40, max(int(page_count or 0), 1))))
        probe = extract_text_fn(pdf_path, page_numbers=probe_pages) or ""
        t = probe

        def _drop_block(src: str, start_pat: str, end_pats: list[str]) -> str:
            blk = extract_between_markers_fn(src, start_pat, end_pats)
            return src.replace(blk, "") if blk else src

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

        if re.match(r"^(重\s*要\s*提\s*示|目\s*录|释\s*义|词\s*汇\s*表)\b", t):
            t = ""

        if t and len(t) >= 300:
            excerpt = t[:6000].rstrip()
    except Exception:
        excerpt = ""

    pdf_name = os.path.basename(pdf_path)
    base = f"[PARSE_FAILED] reason={reason} pages={page_count}\n请查看原PDF：{pdf_name}"

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
        "chairman_letter": extract_chairman_letter_fn(pdf_path, max_pages=25),
        "full_mda": full,
    }
