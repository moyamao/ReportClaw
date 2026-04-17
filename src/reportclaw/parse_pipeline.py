from __future__ import annotations

import concurrent.futures
import os
from typing import Any, Callable

import pdfplumber


def build_parse_jobs(
    candidates: list[dict[str, Any]],
    *,
    db: Any,
    reparse_existing: bool,
) -> list[dict[str, Any]]:
    """Build parse jobs after local-file checks, PDF page checks, and DB checks."""
    parse_jobs: list[dict[str, Any]] = []
    for c in candidates:
        file_path = c["file_path"]
        if not os.path.exists(file_path):
            continue

        if str(c.get("stock_code") or "").startswith(("200", "201", "900")):
            continue

        try:
            with pdfplumber.open(file_path) as pdf:
                page_count = len(pdf.pages)
        except Exception as e:
            print(f"无法读取PDF页数，跳过: {c['title']} err={e}")
            continue

        if page_count < 50:
            continue

        c["page_count"] = page_count

        existing_id = db.get_report_id(c["stock_code"], c["year"])
        if existing_id is not None and db.is_mda_complete(existing_id):
            if not reparse_existing:
                continue

        parse_jobs.append(c)

    return parse_jobs


def run_parse_jobs(
    parse_jobs: list[dict[str, Any]],
    *,
    parse_backend: str,
    max_workers_parse: int,
    parse_fn: Callable[[tuple[str, int | None, str | None, int | None]], dict[str, Any]],
    fallback_on_worker_error: Callable[[dict[str, Any], Exception], dict[str, Any]],
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """Run parse jobs in parallel and return paired (candidate, result) rows."""
    parse_results: list[tuple[dict[str, Any], dict[str, Any]]] = []
    if not parse_jobs:
        return parse_results

    executor_cls = (
        concurrent.futures.ProcessPoolExecutor
        if parse_backend == "process"
        else concurrent.futures.ThreadPoolExecutor
    )

    with executor_cls(max_workers=max_workers_parse) as ex:
        fut_map = {
            ex.submit(parse_fn, (c["file_path"], c.get("page_count"), c.get("stock_code"), c.get("year"))): c
            for c in parse_jobs
        }
        for fut in concurrent.futures.as_completed(fut_map):
            c = fut_map[fut]
            try:
                res = fut.result()
            except Exception as e:
                res = fallback_on_worker_error(c, e)
            parse_results.append((c, res))

    return parse_results
