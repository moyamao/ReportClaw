from __future__ import annotations

import concurrent.futures
import os
import re
import time
from datetime import datetime
from typing import Any

import requests


def guess_cninfo_exchange(stock_code: str) -> tuple[str, str]:
    code = str(stock_code).strip()
    if code.startswith(("600", "601", "603", "605", "688", "689", "900")):
        return "sse", "sh"
    return "szse", "sz"


def _download_pdf_task(args: tuple[dict, str, requests.Session, tuple[int, int], int]) -> tuple[bool, str]:
    """Download a PDF if missing.

    Returns: (ok, message)
    """
    ann, file_path, session, get_timeout, max_retry = args
    if os.path.exists(file_path):
        return True, "exists"

    try:
        adj_url = ann.get("adjunctUrl")
        if not adj_url:
            return False, "no adjunctUrl"
        pdf_url = "http://static.cninfo.com.cn/" + adj_url

        pdf_resp = None
        for attempt in range(1, max_retry + 1):
            try:
                pdf_resp = session.get(pdf_url, timeout=get_timeout)
                pdf_resp.raise_for_status()
                break
            except Exception as e:
                if attempt == max_retry:
                    pdf_resp = None
                    return False, f"download failed: {e}"
                time.sleep(0.8 * attempt)

        if pdf_resp is None:
            return False, "download failed"

        with open(file_path, "wb") as f:
            f.write(pdf_resp.content)

        return True, "downloaded"
    except Exception as e:
        return False, f"download exception: {e}"


def fetch_candidate_announcements(
    *,
    session: requests.Session,
    base_url: str,
    download_dir: str,
    start_date: datetime,
    query_end_date: datetime,
    start_ts: float,
    end_ts: float,
    company_mode: bool,
    stock_code_filter: str | None,
    company_years: int,
    min_report_year: int,
    use_last_crawl: bool,
    last_crawl_state_file: Any,
    post_timeout: tuple[int, int],
    max_retry: int,
    max_pages: int = 50,
) -> list[dict[str, Any]]:
    """Fetch cninfo announcement metadata and return candidate records."""
    date_range = f"{start_date.strftime('%Y-%m-%d')}~{query_end_date.strftime('%Y-%m-%d')}"
    company_plate = None
    if company_mode:
        company_column, company_plate = guess_cninfo_exchange(stock_code_filter or "")
        columns = [company_column]
        print(
            f"[crawler] company_mode=true stock={stock_code_filter}, years={company_years}, "
            f"report_year>={min_report_year}, window={date_range}, exchange={company_column}"
        )
    else:
        if use_last_crawl:
            print(f"[crawler] use_last_crawl=true, state={last_crawl_state_file}, window={date_range}")
        columns = ["szse", "sse"]

    candidates: list[dict[str, Any]] = []

    for col in columns:
        page = 1
        while True:
            plate = company_plate if company_mode else ("sz" if col == "szse" else "sh")
            print(f"[{col}] 拉取第 {page} 页...")
            data = {
                "pageNum": page,
                "pageSize": 30,
                "column": col,
                "plate": plate,
                "tabName": "fulltext",
                "category": "category_ndbg_szsh;",
                "seDate": date_range,
                "isHLtitle": "false",
                "searchkey": stock_code_filter if company_mode else "年度报告",
                "secid": ""
            }

            result = None
            for attempt in range(1, max_retry + 1):
                try:
                    r = session.post(base_url, data=data, timeout=post_timeout)
                    r.raise_for_status()
                    result = r.json()
                    break
                except Exception as e:
                    print(f"[{col}] 第 {page} 页请求失败 attempt={attempt}/{max_retry}: {e}")
                    if attempt == max_retry:
                        print(f"[{col}] 连续失败，跳过该交易所后续分页。")
                        result = {"announcements": []}
                    else:
                        time.sleep(1.0 * attempt)

            announcements = result.get("announcements") or []
            print(f"[{col}] 第 {page} 页返回 {len(announcements)} 条公告")

            oldest_ts = None
            newest_ts = None
            for a in announcements:
                t = a.get("announcementTime")
                ts = None
                if isinstance(t, int):
                    ts = t / 1000 if t > 1e12 else t
                elif isinstance(t, str):
                    if t.isdigit():
                        ti = int(t)
                        ts = ti / 1000 if ti > 1e12 else ti
                    else:
                        try:
                            ts = datetime.strptime(t[:10], "%Y-%m-%d").timestamp()
                        except Exception:
                            ts = None
                if ts is None:
                    continue
                oldest_ts = ts if oldest_ts is None else min(oldest_ts, ts)
                newest_ts = ts if newest_ts is None else max(newest_ts, ts)

            if oldest_ts is not None and oldest_ts < start_ts:
                print(
                    f"[{col}] 已到达时间窗口下界（本页最老 {datetime.fromtimestamp(oldest_ts).strftime('%Y-%m-%d')} < {start_date.strftime('%Y-%m-%d')}），处理完本页后停止分页。"
                )
                stop_after_page = True
            else:
                stop_after_page = False

            if newest_ts is not None and newest_ts < start_ts:
                print(
                    f"[{col}] 本页最新也早于时间窗口（{datetime.fromtimestamp(newest_ts).strftime('%Y-%m-%d')} < {start_date.strftime('%Y-%m-%d')}），立即停止分页。"
                )
                break

            if page >= max_pages:
                print(f"[{col}] 已达到最大页数上限 {max_pages}，停止分页（防止异常无限分页）。")
                break

            if not announcements:
                break

            if col == "sse" and page == 1:
                for a in announcements:
                    print("[SSE DEBUG]", a.get("secCode"), a.get("secName"), a.get("announcementTitle"), a.get("announcementTime"))

            for ann in announcements:
                title = ann.get("announcementTitle") or ""
                if "年度报告" not in title:
                    continue
                if "摘要" in title:
                    continue
                if "关于" in title and "年度报告" not in title.split("关于")[0]:
                    continue

                year_match = re.search(r"(20\d{2})\s*年?\s*年度报告", title)
                if year_match:
                    year = int(year_match.group(1))
                else:
                    m0 = re.match(r"^(20\d{2})", title)
                    if not m0:
                        continue
                    year = int(m0.group(1))

                if company_mode and year < min_report_year:
                    continue

                stock_code = ann.get("secCode")
                stock_name = ann.get("secName")
                timestamp = ann.get("announcementTime")

                if stock_code is not None:
                    stock_code = str(stock_code).strip().zfill(6)

                if company_mode and stock_code != stock_code_filter:
                    continue

                if not stock_code or not stock_name or not timestamp:
                    continue
                if str(stock_code).startswith(("200", "201", "900")):
                    continue

                if isinstance(timestamp, int):
                    ts = timestamp / 1000 if timestamp > 1e12 else timestamp
                    publish_dt = datetime.fromtimestamp(ts)
                    publish_date = publish_dt.strftime("%Y-%m-%d")
                else:
                    publish_date = str(timestamp)[:10]
                    try:
                        publish_dt = datetime.strptime(publish_date, "%Y-%m-%d")
                        ts = publish_dt.timestamp()
                    except Exception:
                        ts = None

                if ts is not None:
                    if ts < start_ts or ts > end_ts:
                        continue
                else:
                    try:
                        pd = datetime.strptime(publish_date, "%Y-%m-%d").timestamp()
                    except Exception:
                        continue
                    if pd < start_ts or pd > end_ts:
                        continue

                adj_url = ann.get("adjunctUrl") or ""
                if not adj_url:
                    continue
                file_name = adj_url.split("/")[-1]
                file_path = os.path.join(download_dir, file_name)

                candidates.append({
                    "col": col,
                    "title": title,
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "year": year,
                    "publish_date": publish_date,
                    "ts": ts,
                    "ann": ann,
                    "file_path": file_path,
                })

            if stop_after_page:
                break

            page += 1
            time.sleep(0.6)

    return candidates


def dedupe_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep the latest announcement for each (stock_code, report_year)."""
    dedup: dict[tuple[str, int], dict[str, Any]] = {}
    for c in candidates:
        key = (c["stock_code"], int(c["year"]))
        if key not in dedup:
            dedup[key] = c
            continue
        old = dedup[key]
        if (c.get("ts") or 0) >= (old.get("ts") or 0):
            dedup[key] = c

    items = list(dedup.values())
    items.sort(key=lambda x: (x.get("ts") or 0), reverse=True)
    return items


def download_missing_pdfs(
    candidates: list[dict[str, Any]],
    *,
    session: requests.Session,
    get_timeout: tuple[int, int],
    max_retry: int,
    max_workers_download: int,
) -> None:
    """Download candidate PDFs that are not yet present on disk."""
    to_download: list[tuple[dict, str, requests.Session, tuple[int, int], int]] = []
    for c in candidates:
        if not os.path.exists(c["file_path"]):
            to_download.append((c["ann"], c["file_path"], session, get_timeout, max_retry))

    if not to_download:
        return

    print(f"[perf] downloading missing PDFs: {len(to_download)} (threads={max_workers_download})")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers_download) as ex:
        futs = [ex.submit(_download_pdf_task, a) for a in to_download]
        for fut in concurrent.futures.as_completed(futs):
            ok, msg = fut.result()
            if not ok:
                print(f"[download] failed: {msg}")
    time.sleep(0.5)
