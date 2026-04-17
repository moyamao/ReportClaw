from __future__ import annotations

import re


def slice_to_next_bracket_heading(text: str, start_idx: int):
    """从（X）/ (X) 这类括号小标题开始切，到下一条同级括号小标题或下一条一级大标题。"""
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


def slice_to_next_heading_with_title_keywords(text, start_idx, title_keywords):
    """
    从 start_idx 切到“下一条标题”，且该标题行的标题部分包含 title_keywords 之一。
    同时支持：
    - 一级标题：二、xxx / 2、xxx
    - 括号小标题：（三）xxx / (3)xxx
    """
    if start_idx is None or start_idx < 0:
        return None

    candidates = []

    for m in re.finditer(
        r"(?:^|\n)\s*([一二三四五六七八九十]{1,3}|\d{1,2})[、\.．:：]\s*([^\n]{1,80})",
        text[start_idx + 1:]
    ):
        title = m.group(2)
        if any(kw in title for kw in title_keywords):
            candidates.append(start_idx + 1 + m.start())
            break

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


def slice_to_next_major_heading(text, start_idx, major_heading_keywords):
    """
    从 start_idx 切到“下一条重大一级标题”（标题行包含 major_heading_keywords）。
    """
    if start_idx is None or start_idx < 0:
        return None

    heading_iter = re.finditer(
        r"(?:\n\s*([一二三四五六七八九十]{1,3}|\d{1,2})、([^\n]{1,60}))",
        text[start_idx + 1:]
    )

    for m in heading_iter:
        title = m.group(2)
        if any(kw in title for kw in major_heading_keywords):
            end_idx = start_idx + 1 + m.start()
            return text[start_idx:end_idx].strip()

    return None


def next_ordinal_candidates(current_ordinal: str):
    """给定当前一级序号，返回可能的下一个一级序号候选列表。"""
    cn_list = ["一", "二", "三", "四", "五", "六", "七", "八", "九", "十",
               "十一", "十二", "十三", "十四", "十五", "十六", "十七", "十八", "十九", "二十"]
    cn_to_ar = {"一": "1", "二": "2", "三": "3", "四": "4", "五": "5", "六": "6", "七": "7", "八": "8", "九": "9", "十": "10",
                "十一": "11", "十二": "12", "十三": "13", "十四": "14", "十五": "15", "十六": "16", "十七": "17", "十八": "18", "十九": "19", "二十": "20"}
    ar_to_cn = {v: k for k, v in cn_to_ar.items()}

    cur_cn = current_ordinal
    if re.fullmatch(r"\d{1,2}", current_ordinal):
        cur_cn = ar_to_cn.get(current_ordinal)

    if cur_cn in cn_list:
        idx = cn_list.index(cur_cn)
        if idx + 1 < len(cn_list):
            nxt_cn = cn_list[idx + 1]
            nxt_ar = cn_to_ar.get(nxt_cn)
            return [nxt_cn, nxt_ar] if nxt_ar else [nxt_cn]

    return []


def slice_to_next_ordinal(text, start_idx, current_ordinal: str, major_heading_keywords):
    """从 start_idx 开始切片到下一一级序号。"""
    if start_idx is None or start_idx < 0:
        return None

    major_slice = slice_to_next_major_heading(text, start_idx, major_heading_keywords)
    if major_slice:
        return major_slice

    candidates = next_ordinal_candidates(current_ordinal)

    def _cand_regex(c: str) -> str:
        if re.fullmatch(r"\d{1,2}", c):
            return re.escape(c)
        return r"\\s*".join(re.escape(ch) for ch in c)

    end_positions = []
    for cand in candidates:
        if not cand:
            continue
        cand_pat = _cand_regex(cand)
        m = re.search(rf"(?:^|\n)\s*{cand_pat}\s*、", text[start_idx + 1:])
        if m:
            end_positions.append(start_idx + 1 + m.start())

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


def extract_section_by_ordinal(text, ordinal_cn, keyword_fallback=None, *, major_heading_keywords):
    """按同级序号提取段落。"""
    cn_to_arabic = {"一": "1", "二": "2", "三": "3", "四": "4", "五": "5",
                    "六": "6", "七": "7", "八": "8", "九": "9", "十": "10",
                    "十一": "11", "十二": "12"}

    start_idx = None

    m1 = re.search(rf"(?:^|\n)\s*{ordinal_cn}、", text)
    if m1:
        start_idx = m1.start()
    else:
        arabic = cn_to_arabic.get(ordinal_cn)
        if arabic:
            m2 = re.search(rf"(?:^|\n)\s*{arabic}、", text)
            if m2:
                start_idx = m2.start()

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

    if start_idx is None and keyword_fallback:
        for kw in keyword_fallback:
            mk = re.search(rf"(?:^|\n)\s*{kw}", text)
            if mk:
                start_idx = mk.start()
                break

    return slice_to_next_ordinal(text, start_idx, ordinal_cn, major_heading_keywords)


def extract_section_by_keywords(text, keywords, fallback_ordinals=None, end_title_keywords=None, *, major_heading_keywords):
    """
    通过“一级标题行（序号 + 顿号）+ 关键词”定位并提取整段，并按该序号切到下一序号。
    """
    for kw in keywords:
        kw_pat = kw[len("REGEX:"):] if isinstance(kw, str) and kw.startswith("REGEX:") else re.escape(str(kw))

        m = re.search(
            rf"(?:^|\n)\s*([一二三四五六七八九十]{{1,3}}|\d{{1,2}})[、\.．:：]\s*[^\n]*{kw_pat}[^\n]*",
            text
        )
        if m:
            start = m.start()
            ordinal = m.group(1)

            if end_title_keywords:
                sliced = slice_to_next_heading_with_title_keywords(text, start, end_title_keywords)
                if sliced:
                    return sliced
                return slice_to_next_ordinal(text, start, ordinal, major_heading_keywords)

            return slice_to_next_ordinal(text, start, ordinal, major_heading_keywords)

    for kw in keywords:
        kw_pat = kw[len("REGEX:"):] if isinstance(kw, str) and kw.startswith("REGEX:") else re.escape(str(kw))
        m = re.search(
            rf"(?:^|\n)\s*(\d+(?:[\.．]\d+){{1,3}})\s*[、\.．:：]?\s*[^\n]*{kw_pat}[^\n]*",
            text
        )
        if not m:
            continue

        start = m.start()

        if end_title_keywords:
            sliced = slice_to_next_heading_with_title_keywords(text, start, end_title_keywords)
            if sliced:
                return sliced

        tail = text[start + 1:]
        end_candidates: list[int] = []

        m_dot = re.search(r"(?:^|\n)\s*\d+(?:[\.．]\d+){1,3}\b", tail)
        if m_dot:
            end_candidates.append(start + 1 + m_dot.start())

        m_ord = re.search(r"(?:^|\n)\s*(?:[一二三四五六七八九十]{1,3}|\d{1,2})[、\.．:：]", tail)
        if m_ord:
            end_candidates.append(start + 1 + m_ord.start())

        m_ch = re.search(r"(?:^|\n)\s*第\s*[一二三四五六七八九十]{1,3}\s*[章节]", tail)
        if m_ch:
            end_candidates.append(start + 1 + m_ch.start())

        end_idx = min(end_candidates) if end_candidates else len(text)
        return text[start:end_idx].strip()

    for kw in keywords:
        kw_pat = kw[len("REGEX:"):] if isinstance(kw, str) and kw.startswith("REGEX:") else re.escape(str(kw))
        m = re.search(
            rf"(?:^|\n)\s*[（(]([一二三四五六七八九十0-9]{{1,3}})[）)]\s*[^\n]*{kw_pat}[^\n]*",
            text
        )
        if m:
            start = m.start()
            sliced = slice_to_next_bracket_heading(text, start)
            if sliced:
                return sliced
            return text[start:].strip()

    if fallback_ordinals:
        for o in fallback_ordinals:
            sec = extract_section_by_ordinal(text, o, major_heading_keywords=major_heading_keywords)
            if sec:
                return sec
    return None


def extract_section(text, title):
    pattern = rf"{title}[\s\S]*?(?=\n[一二三四五六七八九十]+、|\Z)"
    match = re.search(pattern, text)
    return match.group(0).strip() if match else None
