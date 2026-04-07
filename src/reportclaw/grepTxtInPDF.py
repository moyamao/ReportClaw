#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from pathlib import Path
from typing import Iterable, List, Tuple

from pypdf import PdfReader

# Optional fallback extractor (better for some PDFs)
try:
    from pdfminer.high_level import extract_text as pdfminer_extract_text  # type: ignore
except Exception:
    pdfminer_extract_text = None

import re
import unicodedata

# --------------------
# User config (edit me)
# --------------------
DEFAULT_ROOT = "/Users/mhy/python/ReportClaw/data/downloads"  # Directory to scan
DEFAULT_TEXT = "总量收缩、结构分化、区域再平衡"                 # Exact text to search
DEFAULT_JOIN_NEWLINES = True           # True recommended for long paragraphs
DEFAULT_TRY_PDFMINER_ALWAYS = True     # Try pdfminer extraction even when pypdf returns text
DEFAULT_STRIP_ALL_WHITESPACE = True   # For matching: also try removing all whitespace/zero-width chars
DEFAULT_OUT = "/Users/mhy/python/ReportClaw/logs/matches.txt"
DEFAULT_ERRORS = "/Users/mhy/python/ReportClaw/logs/errors.txt"
DEFAULT_NO_TEXT = "/Users/mhy/python/ReportClaw/logs/no_text.txt"
DEFAULT_DEBUG_NOT_FOUND = "/Users/mhy/python/ReportClaw/logs/not_found_debug.txt"
DEFAULT_NOT_FOUND_LIST = "/Users/mhy/python/ReportClaw/logs/not_found.txt"


def iter_pdfs(root: Path) -> Iterable[Path]:
    yield from root.rglob("*.pdf")


def extract_text(reader: PdfReader) -> Tuple[str, List[str]]:
    """Extract text via pypdf. Returns (all_text, per_page_texts)."""
    parts: List[str] = []
    per_page: List[str] = []
    for page in reader.pages:
        try:
            t = page.extract_text() or ""
        except Exception:
            t = ""
        per_page.append(t)
        parts.append(t)
    return "\n".join(parts), per_page


def normalize(text: str, *, join_newlines: bool) -> str:
    if join_newlines:
        # 把换行/多空白压缩成单空格，解决“段落被硬换行切断”的问题
        return " ".join(text.split())
    return text


def normalize_for_match(text: str) -> str:
    """Aggressive normalization for PDF text matching."""
    # Unicode compatibility normalization (full-width, etc.)
    t = unicodedata.normalize("NFKC", text)
    # Remove common zero-width chars that appear in extracted PDF text
    t = t.replace("\u200b", "").replace("\u200c", "").replace("\u200d", "").replace("\ufeff", "")
    return t


def strip_all_whitespace(text: str) -> str:
    # Remove all whitespace characters (spaces, tabs, newlines, etc.)
    return re.sub(r"\s+", "", text)


def all_chars_present(hay: str, needle: str) -> bool:
    # Useful diagnostic: characters exist but may be separated by spaces/newlines
    for ch in needle:
        if ch and ch not in hay:
            return False
    return True


def search_one(pdf_path: Path, needle: str, *, join_newlines: bool) -> Tuple[bool, str]:
    """Return (matched, info). info is empty on success; otherwise includes reason."""
    try:
        needle_norm = normalize_for_match(needle)
        needle_compact = strip_all_whitespace(needle_norm)

        # 1) pypdf extraction
        reader = PdfReader(str(pdf_path))
        all_text, per_page = extract_text(reader)
        norm_all = normalize_for_match(normalize(all_text, join_newlines=join_newlines))

        if needle_norm and needle_norm in norm_all:
            return True, ""

        # 2) Fallback to pdfminer (optionally always)
        mined_text = ""
        if pdfminer_extract_text is not None:
            try_pdfminer = DEFAULT_TRY_PDFMINER_ALWAYS or (len(norm_all.strip()) < 50)
            if try_pdfminer:
                try:
                    mined_text = pdfminer_extract_text(str(pdf_path)) or ""
                    norm_mined = normalize_for_match(normalize(mined_text, join_newlines=join_newlines))
                    if needle_norm and needle_norm in norm_mined:
                        return True, ""
                    # Prefer the richer text for later diagnostics/matching
                    if len(norm_mined) > len(norm_all):
                        norm_all = norm_mined
                except Exception:
                    pass

        # 3) Per-page strict match (pypdf)
        if needle_norm:
            for page_text in per_page:
                page_norm = normalize_for_match(normalize(page_text, join_newlines=join_newlines))
                if needle_norm in page_norm:
                    return True, ""

        # 4) Aggressive whitespace-stripped match (handles PDFs that insert spaces between every CJK char)
        if DEFAULT_STRIP_ALL_WHITESPACE and needle_compact:
            hay_compact = strip_all_whitespace(norm_all)
            if needle_compact in hay_compact:
                return True, ""

        # 5) No text layer (likely scanned)
        if len(norm_all.strip()) < 50:
            return False, "NO_TEXT: likely scanned/image PDF (needs OCR)"

        # 6) Diagnostic: chars exist but contiguous match fails
        if needle_norm and all_chars_present(norm_all, needle_norm):
            return False, "SUSPECT_SPLIT: chars present but not contiguous (spacing/ordering issue)"

        return False, "NOT_FOUND"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Search all PDFs under a directory for an exact (fixed-string) match."
    )
    ap.add_argument(
        "root",
        nargs="?",
        default=DEFAULT_ROOT,
        help="Root directory to scan (default: DEFAULT_ROOT).",
    )
    ap.add_argument(
        "text",
        nargs="?",
        default=DEFAULT_TEXT,
        help="Text to search (exact fixed string; default: DEFAULT_TEXT).",
    )
    ap.add_argument(
        "--join-newlines",
        action=argparse.BooleanOptionalAction,
        default=DEFAULT_JOIN_NEWLINES,
        help="Normalize whitespace/newlines before matching (default: DEFAULT_JOIN_NEWLINES).",
    )
    ap.add_argument(
        "--out",
        default=DEFAULT_OUT,
        help="Output file to write matched PDF paths (default: DEFAULT_OUT).",
    )
    ap.add_argument(
        "--errors",
        default=DEFAULT_ERRORS,
        help="Output file to write PDFs that could not be parsed (default: DEFAULT_ERRORS).",
    )
    ap.add_argument(
        "--no-text",
        default=DEFAULT_NO_TEXT,
        help="Output file to write PDFs with no extractable text (likely scanned).",
    )
    ap.add_argument(
        "--not-found",
        default=DEFAULT_NOT_FOUND_LIST,
        help="Output file to write PDFs where text exists but pattern not found.",
    )
    ap.add_argument(
        "--debug-not-found",
        default=DEFAULT_DEBUG_NOT_FOUND,
        help="Debug file with small extracted text samples for NOT_FOUND/SUSPECT_SPLIT PDFs.",
    )

    args = ap.parse_args()
    root = Path(args.root).expanduser().resolve()
    needle = args.text

    if not root.exists() or not root.is_dir():
        raise SystemExit(f"Not a directory: {root}")

    matched: List[str] = []
    errors: List[str] = []
    no_text: List[str] = []
    not_found: List[str] = []
    debug_nf: List[str] = []
    total = 0

    for pdf in iter_pdfs(root):
        total += 1
        ok, err = search_one(pdf, needle, join_newlines=args.join_newlines)
        if ok:
            matched.append(str(pdf))
        else:
            if err.startswith("NO_TEXT"):
                no_text.append(str(pdf))
            elif err.startswith("SUSPECT_SPLIT") or err == "NOT_FOUND":
                not_found.append(str(pdf))
                debug_nf.append(f"{pdf}\t{err}")
            elif err:
                errors.append(f"{pdf}\t{err}")

        if total % 50 == 0:
            print(f"Scanned {total} PDFs... matched={len(matched)} errors={len(errors)}", flush=True)

    Path(args.out).write_text("\n".join(matched) + ("\n" if matched else ""), encoding="utf-8")
    Path(args.errors).write_text("\n".join(errors) + ("\n" if errors else ""), encoding="utf-8")
    Path(args.no_text).write_text("\n".join(no_text) + ("\n" if no_text else ""), encoding="utf-8")
    Path(args.not_found).write_text("\n".join(not_found) + ("\n" if not_found else ""), encoding="utf-8")
    Path(args.debug_not_found).write_text("\n".join(debug_nf) + ("\n" if debug_nf else ""), encoding="utf-8")

    print(f"Done. Scanned={total}, matched={len(matched)}, no_text={len(no_text)}, not_found={len(not_found)}, errors={len(errors)}")
    print(f"Matches saved to: {args.out}")
    print(f"No-text PDFs saved to: {args.no_text}")
    print(f"Not-found PDFs saved to: {args.not_found}")
    print(f"Not-found debug saved to: {args.debug_not_found}")
    print(f"Errors saved to:  {args.errors}")


if __name__ == "__main__":
    main()