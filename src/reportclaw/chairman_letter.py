from __future__ import annotations

import re
from typing import Callable

from pdfminer.high_level import extract_text as pdfminer_extract_text


def normalize_for_letter(text: str, *, preprocess_text: Callable[[str], str]) -> str:
    """A gentler normalizer for chairman letter extraction."""
    if not text:
        return ""

    text = preprocess_text(text)
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\x0c", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)

    lines: list[str] = []
    for raw in text.split("\n"):
        if raw is None:
            continue
        s = raw.strip()
        if s == "":
            lines.append("")
            continue

        if re.fullmatch(r"\d{1,4}", s):
            continue
        if re.fullmatch(r"\d{1,4}\s*/\s*\d{1,4}", s):
            continue
        if ("年度报告" in s) and ("股份有限公司" in s or "有限公司" in s):
            continue
        if "年度报告全文" in s:
            continue
        if s.startswith("公司代码：") or s.startswith("公司简称：") or ("公司代码：" in s):
            continue
        if (s.endswith("股份有限公司") or s.endswith("有限公司")) and len(s) <= 30:
            continue

        lines.append(re.sub(r"\s+", " ", s))

    out = "\n".join(lines)
    out = re.sub(r"\n{3,}", "\n\n", out)
    return out.strip()


def extract_chairman_letter(
    pdf_path: str,
    *,
    max_pages: int,
    normalize_for_letter_fn: Callable[[str], str],
    is_image_heavy_pdf_fn: Callable[..., tuple[bool, dict]],
    log_fn: Callable[[str], None],
) -> str | None:
    """Extract chairman/board-chair letter from the front matter (first N pages)."""
    is_heavy, st = is_image_heavy_pdf_fn(
        pdf_path,
        sample_pages=min(6, max_pages),
        min_images=16,
        max_total_text_chars=500,
        min_avg_text_chars_per_page=80,
        max_file_mb=30,
    )
    if is_heavy:
        log_fn(
            f"[perf] image-heavy front matter, skip chairman letter. "
            f"file_mb={st.get('file_mb')} images={st.get('images')} text_chars={st.get('text_chars')} avg_text_chars={st.get('avg_text_chars')}"
        )
        return "[CHAIRMAN_LETTER_HINT] 该PDF前部页面图片占比高，董事长致辞正文抽取失败概率大；建议打开原PDF查看。"

    try:
        raw_front = pdfminer_extract_text(pdf_path, page_numbers=list(range(0, max_pages))) or ""
        front = normalize_for_letter_fn(raw_front)
    except Exception:
        front = ""
    if not front:
        return None

    t = front

    def _refine_letter_tail(s: str) -> str:
        if not s:
            return s

        def _split_inline_signature_line(ln: str) -> list[str]:
            if not ln:
                return [ln]

            src = ln.strip()
            if not src:
                return [""]

            m_date = re.search(
                r"(?:\d{4}年\d{1,2}月(?:\d{1,2}日)?|[二〇○零一二三四五六七八九十]{2,4}年[一二三四五六七八九十]{1,3}月(?:[一二三四五六七八九十]{1,3}日)?)",
                src,
            )
            date_part = m_date.group(0) if m_date else None

            m_role = re.search(r"(?:董事长|主席|董事会主席|董事会全体成员|董事会)[^\d]{0,20}", src)
            role_part = None
            if m_role:
                role_part = m_role.group(0).strip()

            if not (date_part or role_part):
                return [ln]

            parts: list[str] = []
            working = src
            if date_part and date_part in working:
                working = working.replace(date_part, "")

            if role_part and role_part in working:
                before, _, after = working.partition(role_part)
                name_part = before.strip(" ：:，,;；")
                if name_part:
                    parts.append(name_part)
                parts.append(role_part.strip(" ：:，,;；"))
                rest = after.strip(" ：:，,;；")
                if rest:
                    parts.append(rest)
            else:
                rem = working.strip(" ：:，,;；")
                if rem:
                    parts.append(rem)

            if date_part:
                parts.append(date_part)

            if any(len(re.sub(r"\s+", "", p)) > 40 for p in parts):
                return [ln]

            return parts

        raw_lines: list[str] = []
        for ln in s.split("\n"):
            ln = (ln or "").replace("\r", "")
            if ln.strip() == "":
                raw_lines.append("")
                continue
            raw_lines.extend(_split_inline_signature_line(ln))

        date_pat = re.compile(
            r"(?:\d{4}年\d{1,2}月(?:\d{1,2}日)?|[二〇○零一二三四五六七八九十]{2,4}年[一二三四五六七八九十]{1,3}月(?:[一二三四五六七八九十]{1,3}日)?)"
        )
        company_pat = re.compile(r"[\u4e00-\u9fffA-Za-z0-9]{2,60}(?:股份有限公司|有限公司|集团|公司)(?:董事会)?")
        role_pat = re.compile(r"^(?:董事长|主席|董事会主席|董事会)(?:[：:]?\s*[\u4e00-\u9fffA-Za-z·•]{1,16})?$")
        role_inline_pat = re.compile(r"(?:董事长|主席|董事会主席)[：:]?\s*[\u4e00-\u9fffA-Za-z·•]{1,16}")

        def _is_section_heading_line(x: str) -> bool:
            c = re.sub(r"\s+", "", x or "")
            if not c:
                return False
            if re.match(
                r"^(?:第[一二三四五六七八九十0-9]{1,3}(?:章|节|部分)|(?:第一|第二|第三|第四|第五|第六|第七|第八|第九|第十|第十一|第十二|第十三)(?:章|节|部分))",
                c,
            ):
                return True
            if re.match(r"^(?:[一二三四五六七八九十]{1,3}|\d{1,2})[、\.．:：]", c):
                return True
            if any(k in c for k in ("重要提示", "目录", "释义", "名词解释", "公司概况", "公司简介", "董事会报告", "董事局报告", "管理层讨论与分析", "财务报告", "财务报表")):
                if len(c) <= 40:
                    return True
            return False

        def _sig_types(ln: str) -> set[str]:
            tags: set[str] = set()
            t = (ln or "").strip()
            if not t:
                return tags
            compact = re.sub(r"\s+", "", t)

            if len(compact) <= 20 and date_pat.fullmatch(compact):
                tags.add("date")
            if ("年度报告" not in t) and ("报告全文" not in t) and (len(compact) <= 40):
                if company_pat.search(t) and any(
                        k in t for k in ("董事会", "董事局", "董事会全体成员", "董事会主席", "董事长", "主席")):
                    tags.add("company")
            if role_pat.match(re.sub(r"\s+", "", t)):
                tags.add("role")
            if len(compact) <= 25 and role_inline_pat.search(t):
                tags.add("signer")
            if len(compact) <= 8 and re.fullmatch(r"[\u4e00-\u9fff·•]{2,8}", compact):
                tags.add("name")

            return tags

        n = len(raw_lines)
        if n == 0:
            return s

        scan_start = max(0, n - 40)
        sig_idxs: list[int] = []
        for i in range(n - 1, scan_start - 1, -1):
            if _sig_types(raw_lines[i]):
                sig_idxs.append(i)

        cut_end_idx = None
        if sig_idxs:
            cluster_end = max(sig_idxs)
            cluster_start = cluster_end

            while cluster_start - 1 >= scan_start:
                prev = raw_lines[cluster_start - 1]
                if _sig_types(prev):
                    cluster_start -= 1
                    continue
                if (prev or "").strip() == "":
                    cluster_start -= 1
                    continue
                break

            cluster_types: set[str] = set()
            for k in range(cluster_start, cluster_end + 1):
                cluster_types |= _sig_types(raw_lines[k])

            extend_to = cluster_end
            for k in range(cluster_end + 1, min(n, cluster_end + 3)):
                if _is_section_heading_line(raw_lines[k]):
                    break
                if _sig_types(raw_lines[k]) or (raw_lines[k] or "").strip() == "":
                    extend_to = k
                    cluster_types |= _sig_types(raw_lines[k])
                    continue
                break

            short_sig_run = 0
            for k in range(cluster_start, extend_to + 1):
                if (raw_lines[k] or "").strip() == "":
                    continue
                tags = _sig_types(raw_lines[k])
                if tags and len(re.sub(r"\s+", "", raw_lines[k])) <= 20:
                    short_sig_run += 1
                    if short_sig_run >= 2:
                        extend_to = k
                        break
                else:
                    short_sig_run = 0

            if len(cluster_types) >= 2:
                cut_end_idx = extend_to

        if cut_end_idx is not None:
            kept = raw_lines[: cut_end_idx + 1]

            for i in range(max(0, len(kept) - 30), len(kept)):
                if _is_section_heading_line(kept[i]) and i > 0:
                    kept = kept[:i]
                    break

            sig_i = None
            for i in range(max(0, len(kept) - 25), len(kept)):
                if _sig_types(kept[i]):
                    sig_i = i
                    break
            if sig_i is not None and sig_i > 0:
                head = kept[:sig_i]
                tail = kept[sig_i:]
                while head and (head[-1] or "").strip() == "":
                    head.pop()
                while tail and (tail[0] or "").strip() == "":
                    tail.pop(0)
                return "\n".join(head) + "\n\n" + "\n".join(tail).rstrip()

            return "\n".join(kept).rstrip()

        trimmed = "\n".join(raw_lines).rstrip()
        guard_pats = [
            r"(?:^|\n)\s*第\s*[一二三四五六七八九十0-9]{1,3}\s*节\s*重要提示",
            r"(?:^|\n)\s*第一节\s*重要提示",
            r"(?:^|\n)\s*重要提示(?:、|\s)",
            r"(?:^|\n)\s*目\s*录\b",
            r"(?:^|\n)\s*释\s*义\b",
            r"(?:^|\n)\s*(?:公司概况|公司简介|公司基本情况|董事会报告|董事局报告|管理层讨论与分析)\b",
        ]
        cut_pos = None
        for pat in guard_pats:
            mm = re.search(pat, trimmed)
            if mm and mm.start() > 200:
                cut_pos = mm.start() if cut_pos is None else min(cut_pos, mm.start())
        if cut_pos is not None:
            trimmed = trimmed[:cut_pos].rstrip()
        return trimmed

    def _drop_block(src: str, start_pat: str, end_pats: list[str], *, max_chars: int = 18000) -> str:
        m_start = re.search(start_pat, src, re.MULTILINE)
        if not m_start:
            return src
        start = m_start.start()
        end = None
        tail = src[m_start.end():]
        for ep in end_pats:
            m_end = re.search(ep, tail, re.MULTILINE)
            if m_end:
                cand = m_start.end() + m_end.start()
                end = cand if end is None else min(end, cand)
        if end is None:
            return src
        blk = src[start:end].strip()
        if len(blk) > max_chars:
            return src
        return src.replace(blk, "")

    def _strip_toc_like_lines(src: str) -> str:
        out: list[str] = []
        for ln in src.split("\n"):
            s = (ln or "").strip()
            if not s:
                out.append(ln)
                continue
            if re.search(r"[\.·…]{4,}\s*\d{1,4}\s*$", s):
                continue
            if len(s) <= 30 and re.search(r"\s\d{1,4}\s*$", s) and not re.search(r"(?:19|20)\d{2}\s*$", s):
                continue
            out.append(ln)
        return "\n".join(out)

    t = _drop_block(
        t,
        r"(?:^|\n)重\s*要\s*提\s*示\b",
        [
            r"(?:^|\n)目\s*录\b",
            r"(?:^|\n)释\s*义\b",
            r"(?:^|\n)词\s*汇\s*表\b",
            r"(?:^|\n)公司简介\b",
            r"(?:^|\n)公司概况\b",
            r"(?:^|\n)主要财务数据\b",
            r"(?:^|\n)董事长致辞\b",
            r"(?:^|\n)第\s*[一二三四五六七八九十0-9]{1,3}\s*(?:章|节|部分|篇)\b",
            r"(?:^|\n)第一[章节]\b",
        ],
    )
    t = _drop_block(
        t,
        r"(?:^|\n)目\s*录\b",
        [
            r"(?:^|\n)重\s*要\s*提\s*示\b",
            r"(?:^|\n)公司简介\b",
            r"(?:^|\n)公司概况\b",
            r"(?:^|\n)主要财务数据\b",
            r"(?:^|\n)董事长致辞\b",
            r"(?:^|\n)经营业绩回顾",
            r"(?:^|\n)管理层讨论与分析\b",
            r"(?:^|\n)第\s*[一二三四五六七八九十0-9]{1,3}\s*(?:章|节|部分|篇)\b",
            r"(?:^|\n)第一[章节]\b",
            r"(?:^|\n)第一节\b",
        ],
    )
    t = _drop_block(
        t,
        r"(?:^|\n)释\s*义\b",
        [
            r"(?:^|\n)(?:词\s*汇\s*表|名词解释)\b",
            r"(?:^|\n)公司简介\b",
            r"(?:^|\n)董事长致辞\b",
            r"(?:^|\n)第\s*[一二三四五六七八九十0-9]{1,3}\s*(?:章|节|部分|篇)\b",
            r"(?:^|\n)第一[章节]\b",
            r"(?:^|\n)第一节\b",
        ],
    )
    t = _drop_block(
        t,
        r"(?:^|\n)(?:词\s*汇\s*表|名词解释)\b",
        [
            r"(?:^|\n)公司简介\b",
            r"(?:^|\n)董事长致辞\b",
            r"(?:^|\n)第\s*[一二三四五六七八九十0-9]{1,3}\s*(?:章|节|部分|篇)\b",
            r"(?:^|\n)第一[章节]\b",
            r"(?:^|\n)第一节\b",
        ],
    )
    t = _strip_toc_like_lines(t)

    sec_prefix = r"(?:第\s*[一二三四五六七八九十0-9]{1,3}\s*(?:章|节|部分|篇)\s*)?"
    start_pats = [
        rf"(?:^|\n)\s*{sec_prefix}(?:董事长\s*致辞|董事长\s*致信|董事长\s*致股东信|董事长\s*致投资者信)\b",
        rf"(?:^|\n)\s*{sec_prefix}(?:主席\s*致辞|主席\s*致信)\b",
        rf"(?:^|\n)\s*{sec_prefix}(?:董事会\s*主席\s*致辞|董事会\s*主席\s*致信|董事会主席\s*致辞|董事会主席\s*致信)\b",
        rf"(?:^|\n)\s*{sec_prefix}(?:董事会\s*致辞|董事会\s*致信|致\s*董事会\s*的\s*信)\b",
        rf"(?:^|\n)\s*{sec_prefix}(?:致\s*股东\s*信|致\s*股东\s*的\s*信|致\s*股东\s*函|致\s*投资者\s*信|致\s*投资者\s*函|致\s*股东\s*及\s*投资者\s*(?:信|函)|致\s*全体\s*股东\s*(?:信|函))\b",
        rf"(?:^|\n)\s*{sec_prefix}(?:致\s*股东|致\s*投资者)\b",
    ]
    end_pats = [
        r"(?:^|\n)\s*(?:目\s*录|释\s*义|词\s*汇\s*表|名词解释)\b",
        r"(?:^|\n)\s*重\s*要\s*提\s*示\b",
        r"(?:^|\n)\s*第\s*[一二三四五六七八九十0-9]{1,3}\s*(?:章|节|部分)\b",
        r"(?:^|\n)\s*(?:第一|第二|第三|第四|第五|第六|第七|第八|第九|第十|第十一|第十二|第十三)\s*(?:章|节|部分)\b",
        r"(?:^|\n)\s*(?:公司概况|公司简介|公司基本情况|董事会报告|管理层讨论与分析|经营情况讨论与分析)\b",
    ]

    toc_suggests_letter = False
    if re.search(r"董事长致辞|董事会致辞|主席致辞|致股东信|致投资者信", front):
        if re.search(r"目\s*录", front) or re.search(r"[\.·…]{4,}.*\d{1,4}\s*$", front, flags=re.MULTILINE):
            toc_suggests_letter = True

    best = None
    best_start = None
    for sp in start_pats:
        for m in re.finditer(sp, t):
            start = m.start()
            ls = t.rfind("\n", 0, start)
            le = t.find("\n", start)
            line = t[(ls + 1 if ls >= 0 else 0):(le if le >= 0 else len(t))].strip()
            compact = re.sub(r"\s+", "", line)
            if re.search(r"[\.·…]{4,}\s*\d{1,4}\s*$", line):
                toc_suggests_letter = True
                continue
            if re.search(r"\s\d{1,4}\s*$", line) and (not re.search(r"(?:19|20)\d{2}\s*$", line)):
                toc_suggests_letter = True
                continue
            if ("详见" in compact or "敬请" in compact or "查阅" in compact) and len(compact) > 20:
                continue

            tail = t[m.end():]
            end = None
            for ep in end_pats:
                me = re.search(ep, tail)
                if not me:
                    continue
                cand = m.end() + me.start()
                if cand <= start + 120:
                    continue
                end = cand if end is None else min(end, cand)

            if end is None:
                end = len(t)

            chunk = t[start:end].strip()
            chunk = _refine_letter_tail(chunk)
            if len(re.sub(r"\s+", "", chunk)) < 400:
                continue
            if len(chunk) < 400:
                continue
            if chunk.count("\n") < 2 and len(chunk) < 900:
                continue

            if best is None or (best_start is not None and start < best_start):
                best = chunk
                best_start = start

    if not best and toc_suggests_letter:
        t2 = _strip_toc_like_lines(front)
        for sp in start_pats:
            for m in re.finditer(sp, t2):
                start = m.start()
                ls = t2.rfind("\n", 0, start)
                le = t2.find("\n", start)
                line = t2[(ls + 1 if ls >= 0 else 0):(le if le >= 0 else len(t2))].strip()
                if re.search(r"[\.·…]{4,}\s*\d{1,4}\s*$", line):
                    continue

                tail = t2[m.end():]
                end = None
                for ep in end_pats:
                    me = re.search(ep, tail)
                    if not me:
                        continue
                    cand = m.end() + me.start()
                    if cand <= start + 120:
                        continue
                    end = cand if end is None else min(end, cand)
                if end is None:
                    end = len(t2)

                chunk = t2[start:end].strip()
                chunk = _refine_letter_tail(chunk)
                if len(re.sub(r"\s+", "", chunk)) < 400:
                    continue
                if chunk.count("\n") < 2 and len(chunk) < 900:
                    continue
                best = chunk
                best_start = start
                break
            if best:
                break

    if not best:
        if toc_suggests_letter:
            return "[CHAIRMAN_LETTER_HINT] 目录显示存在董事长/董事会/主席致辞或致股东(投资者)信，但正文抽取失败；建议打开原PDF查看原文与落款。"
        return None

    if len(best) > 50000:
        head = best[:46000].rstrip()
        tail = best[-12000:].lstrip()
        best = head + "\n\n[...TRUNCATED...]\n\n" + tail

    return best
