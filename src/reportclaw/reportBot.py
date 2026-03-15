from __future__ import annotations

import configparser
import hashlib
import json
import os
import re
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import fitz  # PyMuPDF
import pymysql
import requests


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DOWNLOAD_DIR = DATA_DIR / "downloads"
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

CNINFO_QUERY_URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
CNINFO_STATIC_PREFIX = "https://static.cninfo.com.cn/"


EXCLUDE_TITLE_KEYWORDS = [
    "摘要",
    "英文版",
    "外文版",
    "取消",
    "更正",
    "修订",
    "补充",
    "提示性公告",
]

TARGET_TITLE_ALIASES = {
    "management_discussion": [
        "管理层讨论与分析",
        "董事会报告",
        "经营情况讨论与分析",
        "公司经营情况讨论与分析",
        "经营讨论与分析",
    ],
    "future_outlook": [
        "未来发展展望",
        "公司未来发展的展望",
        "未来经营计划",
        "发展战略",
        "行业格局和趋势",
        "公司发展战略",
        "经营计划",
        "未来发展规划",
        "未来展望",
    ],
}


@dataclass
class SectionNode:
    level: int
    title: str
    normalized_title: str
    page: int
    end_page: int | None = None
    parent_idx: int | None = None
    path: str | None = None
    title_no: str | None = None
    start_line_no: int | None = None
    end_line_no: int | None = None
    title_score: float | None = None
    match_method: str | None = None
    content_preview: str | None = None


class ConfigLoader:
    @staticmethod
    def load_mysql_config(config_path: Path) -> dict[str, Any]:
        parser = configparser.ConfigParser()
        parser.read(config_path, encoding="utf-8")
        if "mysql" not in parser:
            raise ValueError("config.ini 缺少 [mysql] 配置段")

        section = parser["mysql"]
        return {
            "host": section.get("host", "127.0.0.1"),
            "port": section.getint("port", 3306),
            "user": section.get("user"),
            "password": section.get("pass"),
            "database": section.get("db"),
            "charset": "utf8mb4",
            "autocommit": False,
            "cursorclass": pymysql.cursors.DictCursor,
        }


class AnnualReportRepository:
    def __init__(self, mysql_config: dict[str, Any]) -> None:
        self.conn = pymysql.connect(**mysql_config)

    def close(self) -> None:
        self.conn.close()

    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        self.conn.rollback()

    def log(self, announcement_id: str, stage: str, level_name: str, message: str, extra: dict | None = None) -> None:
        sql = """
        INSERT INTO annual_report_parse_logs
        (announcement_id, stage, level_name, message, extra_json)
        VALUES (%s, %s, %s, %s, %s)
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (
                announcement_id,
                stage,
                level_name,
                message,
                json.dumps(extra, ensure_ascii=False) if extra else None,
            ))

    def save_announcement(self, item: dict[str, Any]) -> None:
        sql = """
        INSERT INTO annual_report_announcements
        (
            announcement_id, sec_code, sec_name, org_id, market, report_year,
            announcement_title, announcement_time, adjunct_url, pdf_url, source_json
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            sec_name = VALUES(sec_name),
            org_id = VALUES(org_id),
            market = VALUES(market),
            report_year = VALUES(report_year),
            announcement_title = VALUES(announcement_title),
            announcement_time = VALUES(announcement_time),
            adjunct_url = VALUES(adjunct_url),
            pdf_url = VALUES(pdf_url),
            source_json = VALUES(source_json),
            updated_at = CURRENT_TIMESTAMP
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (
                item["announcement_id"],
                item["sec_code"],
                item.get("sec_name"),
                item.get("org_id"),
                item.get("market"),
                item.get("report_year"),
                item["announcement_title"],
                item.get("announcement_time"),
                item.get("adjunct_url"),
                item.get("pdf_url"),
                json.dumps(item.get("source_json", {}), ensure_ascii=False),
            ))

    def save_file_record(self, record: dict[str, Any]) -> None:
        sql = """
        INSERT INTO annual_report_files
        (
            announcement_id, local_path, file_name, file_size, file_md5, page_count,
            is_text_pdf, download_status, parse_status, parse_error, downloaded_at, parsed_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            local_path = VALUES(local_path),
            file_name = VALUES(file_name),
            file_size = VALUES(file_size),
            file_md5 = VALUES(file_md5),
            page_count = VALUES(page_count),
            is_text_pdf = VALUES(is_text_pdf),
            download_status = VALUES(download_status),
            parse_status = VALUES(parse_status),
            parse_error = VALUES(parse_error),
            downloaded_at = VALUES(downloaded_at),
            parsed_at = VALUES(parsed_at),
            updated_at = CURRENT_TIMESTAMP
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (
                record["announcement_id"],
                record["local_path"],
                record.get("file_name"),
                record.get("file_size"),
                record.get("file_md5"),
                record.get("page_count"),
                record.get("is_text_pdf", 1),
                record.get("download_status", "pending"),
                record.get("parse_status", "pending"),
                record.get("parse_error"),
                record.get("downloaded_at"),
                record.get("parsed_at"),
            ))

    def delete_sections(self, announcement_id: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM annual_report_sections WHERE announcement_id=%s", (announcement_id,))

    def save_sections(self, announcement_id: str, sections: list[SectionNode]) -> list[int]:
        inserted_ids: list[int] = []
        parent_db_ids: dict[int, int] = {}

        sql = """
        INSERT INTO annual_report_sections
        (
            announcement_id, parent_id, level_num, section_title, normalized_title, title_no,
            path, start_page, end_page, start_line_no, end_line_no, title_score,
            match_method, content_preview
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with self.conn.cursor() as cur:
            for idx, sec in enumerate(sections):
                parent_id = parent_db_ids.get(sec.parent_idx) if sec.parent_idx is not None else None
                cur.execute(sql, (
                    announcement_id,
                    parent_id,
                    sec.level,
                    sec.title,
                    sec.normalized_title,
                    sec.title_no,
                    sec.path,
                    sec.page,
                    sec.end_page,
                    sec.start_line_no,
                    sec.end_line_no,
                    sec.title_score,
                    sec.match_method,
                    sec.content_preview,
                ))
                db_id = cur.lastrowid
                inserted_ids.append(db_id)
                parent_db_ids[idx] = db_id
        return inserted_ids

    def save_target_section(self, announcement_id: str, target: dict[str, Any]) -> None:
        sql = """
        INSERT INTO annual_report_target_sections
        (
            announcement_id, target_key, matched_section_id, matched_title, matched_path,
            start_page, end_page, extract_confidence, content
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            matched_section_id = VALUES(matched_section_id),
            matched_title = VALUES(matched_title),
            matched_path = VALUES(matched_path),
            start_page = VALUES(start_page),
            end_page = VALUES(end_page),
            extract_confidence = VALUES(extract_confidence),
            content = VALUES(content),
            updated_at = CURRENT_TIMESTAMP
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (
                announcement_id,
                target["target_key"],
                target.get("matched_section_id"),
                target.get("matched_title"),
                target.get("matched_path"),
                target.get("start_page"),
                target.get("end_page"),
                target.get("extract_confidence"),
                target.get("content"),
            ))


class CninfoAnnouncementFetcher:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.cninfo.com.cn/",
            "X-Requested-With": "XMLHttpRequest",
        })

    @staticmethod
    def _build_date_range(days: int) -> str:
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        return f"{start_date:%Y-%m-%d}~{end_date:%Y-%m-%d}"

    @staticmethod
    def _is_valid_annual_report(row: dict[str, Any]) -> bool:
        title = (row.get("announcementTitle") or "").strip()
        if "年度报告" not in title:
            return False
        for kw in EXCLUDE_TITLE_KEYWORDS:
            if kw in title:
                return False
        adjunct_url = row.get("adjunctUrl") or ""
        if not adjunct_url.lower().endswith(".pdf"):
            return False
        return True

    @staticmethod
    def _infer_market(col_name: str | None) -> str | None:
        if not col_name:
            return None
        if "szse" in col_name.lower():
            return "SZ"
        if "sse" in col_name.lower():
            return "SH"
        return None

    @staticmethod
    def _infer_report_year(title: str) -> int | None:
        m = re.search(r"(20\d{2})年年度报告", title)
        if m:
            return int(m.group(1))
        return None

    def fetch_recent_annual_reports(self, days: int = 3, page_size: int = 50) -> list[dict[str, Any]]:
        page_num = 1
        results: list[dict[str, Any]] = []
        se_date = self._build_date_range(days)

        while True:
            data = {
                "pageNum": page_num,
                "pageSize": page_size,
                "column": "szse",   # 先用 all stock + 两市场轮训更稳，当前先抓全站常用列
                "tabName": "fulltext",
                "plate": "",
                "stock": "",
                "searchkey": "年度报告",
                "secid": "",
                "category": "category_ndbg_szsh;",
                "trade": "",
                "seDate": se_date,
                "sortName": "nothing",
                "sortType": "desc",
                "isHLtitle": "true",
            }

            resp = self.session.post(CNINFO_QUERY_URL, data=data, timeout=30)
            resp.raise_for_status()
            payload = resp.json()

            rows = payload.get("announcements") or []
            if not rows:
                break

            for row in rows:
                if not self._is_valid_annual_report(row):
                    continue

                adjunct_url = row.get("adjunctUrl") or ""
                pdf_url = CNINFO_STATIC_PREFIX + adjunct_url.lstrip("/")
                title = (row.get("announcementTitle") or "").strip()

                item = {
                    "announcement_id": str(row.get("announcementId") or row.get("announcement_id")),
                    "sec_code": str(row.get("secCode") or "").zfill(6),
                    "sec_name": row.get("secName"),
                    "org_id": row.get("orgId"),
                    "market": self._infer_market(row.get("columnCode")),
                    "report_year": self._infer_report_year(title),
                    "announcement_title": title,
                    "announcement_time": datetime.fromtimestamp((row.get("announcementTime") or 0) / 1000)
                    if row.get("announcementTime") else None,
                    "adjunct_url": adjunct_url,
                    "pdf_url": pdf_url,
                    "source_json": row,
                }
                results.append(item)

            total_record_num = payload.get("totalRecordNum", 0)
            if page_num * page_size >= total_record_num:
                break
            page_num += 1
            time.sleep(0.3)

        dedup: dict[str, dict[str, Any]] = {}
        for item in results:
            key = f"{item['sec_code']}_{item['report_year']}"
            old = dedup.get(key)
            if old is None or (item["announcement_time"] and old["announcement_time"] and item["announcement_time"] > old["announcement_time"]):
                dedup[key] = item

        return list(dedup.values())


class PdfDownloader:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36"
            )
        })

    @staticmethod
    def _safe_filename(name: str) -> str:
        return re.sub(r'[\\\\/:*?"<>|]+', "_", name)

    @staticmethod
    def _md5_of_file(file_path: Path) -> str:
        h = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()

    def download(self, item: dict[str, Any]) -> Path:
        report_year = item.get("report_year") or "unknown"
        sec_code = item["sec_code"]
        sec_name = item.get("sec_name") or ""
        title = self._safe_filename(item["announcement_title"])
        sub_dir = DOWNLOAD_DIR / str(report_year)
        sub_dir.mkdir(parents=True, exist_ok=True)

        file_path = sub_dir / f"{sec_code}_{sec_name}_{title}.pdf"
        if file_path.exists() and file_path.stat().st_size > 20 * 1024:
            return file_path

        resp = self.session.get(item["pdf_url"], timeout=60)
        resp.raise_for_status()

        with open(file_path, "wb") as f:
            f.write(resp.content)

        return file_path


class PdfStructureParser:
    TITLE_PATTERNS = [
        re.compile(r"^第[一二三四五六七八九十百零〇\d]+节[ 　]+"),
        re.compile(r"^[一二三四五六七八九十]+[、.]"),
        re.compile(r"^[(（][一二三四五六七八九十]+[)）]"),
        re.compile(r"^\d+[、.]"),
        re.compile(r"^\d+\.\d+([.．]\d+)*"),
    ]

    @staticmethod
    def normalize_title(title: str) -> str:
        title = re.sub(r"\s+", "", title or "")
        title = title.replace(" ", "").replace("\u3000", "")
        return title.strip("：:;； ")

    @staticmethod
    def extract_title_no(title: str) -> str | None:
        m = re.match(r"^(第[一二三四五六七八九十百零〇\d]+节|[一二三四五六七八九十]+[、.]|[(（][一二三四五六七八九十]+[)）]|\d+[、.]|\d+(?:\.\d+)+)", title)
        return m.group(1) if m else None

    @staticmethod
    def detect_level_by_title(title: str) -> int:
        s = title.strip()
        if re.match(r"^第[一二三四五六七八九十百零〇\d]+节", s):
            return 1
        if re.match(r"^[一二三四五六七八九十]+[、.]", s):
            return 2
        if re.match(r"^[(（][一二三四五六七八九十]+[)）]", s):
            return 3
        if re.match(r"^\d+[、.]", s):
            return 3
        if re.match(r"^\d+\.\d+(\.\d+)*", s):
            return min(4, s.count(".") + 2)
        return 2

    def extract_toc(self, pdf_path: Path) -> list[SectionNode]:
        doc = fitz.open(pdf_path)
        toc = doc.get_toc(simple=False)
        nodes: list[SectionNode] = []

        for entry in toc:
            # [lvl, title, page, ...]
            if len(entry) < 3:
                continue
            level, title, page = entry[0], str(entry[1]).strip(), int(entry[2])
            if not title:
                continue

            nodes.append(SectionNode(
                level=level,
                title=title,
                normalized_title=self.normalize_title(title),
                page=max(page, 1),
                title_no=self.extract_title_no(title),
                match_method="toc",
                title_score=1.0,
            ))

        doc.close()
        return nodes

    def detect_titles_by_text(self, pdf_path: Path) -> list[SectionNode]:
        doc = fitz.open(pdf_path)
        candidates: list[SectionNode] = []

        for page_index in range(len(doc)):
            page = doc[page_index]
            page_dict = page.get_text("dict")
            line_no = 0

            for block in page_dict.get("blocks", []):
                if block.get("type") != 0:
                    continue

                for line in block.get("lines", []):
                    spans = line.get("spans", [])
                    if not spans:
                        continue

                    text = "".join(span.get("text", "") for span in spans).strip()
                    line_no += 1

                    if not text or len(text) > 60:
                        continue

                    matched = any(p.search(text) for p in self.TITLE_PATTERNS)
                    if not matched:
                        continue

                    font_sizes = [float(span.get("size", 0)) for span in spans if span.get("size")]
                    avg_size = sum(font_sizes) / len(font_sizes) if font_sizes else 0
                    score = 0.5
                    if avg_size >= 13:
                        score += 0.2
                    if len(text) <= 30:
                        score += 0.1
                    if len(spans) == 1:
                        score += 0.1

                    candidates.append(SectionNode(
                        level=self.detect_level_by_title(text),
                        title=text,
                        normalized_title=self.normalize_title(text),
                        page=page_index + 1,
                        start_line_no=line_no,
                        title_no=self.extract_title_no(text),
                        title_score=round(score, 4),
                        match_method="text_heading",
                    ))

        doc.close()
        return candidates

    def build_tree(self, nodes: list[SectionNode], total_pages: int) -> list[SectionNode]:
        if not nodes:
            return []

        nodes = sorted(nodes, key=lambda x: (x.page, x.start_line_no or 0, x.level))
        stack: list[int] = []

        for idx, node in enumerate(nodes):
            while stack and nodes[stack[-1]].level >= node.level:
                stack.pop()

            node.parent_idx = stack[-1] if stack else None
            if node.parent_idx is None:
                node.path = node.title
            else:
                parent_path = nodes[node.parent_idx].path or nodes[node.parent_idx].title
                node.path = f"{parent_path} / {node.title}"

            stack.append(idx)

        for idx, node in enumerate(nodes):
            end_page = total_pages
            end_line_no = None

            for j in range(idx + 1, len(nodes)):
                next_node = nodes[j]
                if next_node.level <= node.level:
                    end_page = max(node.page, next_node.page - 1)
                    end_line_no = (next_node.start_line_no or 1) - 1 if next_node.page == node.page else None
                    break

            node.end_page = end_page
            node.end_line_no = end_line_no

        return nodes

    def parse(self, pdf_path: Path) -> tuple[list[SectionNode], int, bool]:
        doc = fitz.open(pdf_path)
        total_pages = len(doc)

        is_text_pdf = False
        for i in range(min(5, total_pages)):
            text = doc[i].get_text("text").strip()
            if len(text) > 100:
                is_text_pdf = True
                break
        doc.close()

        toc_nodes = self.extract_toc(pdf_path)
        if toc_nodes:
            return self.build_tree(toc_nodes, total_pages), total_pages, is_text_pdf

        detected_nodes = self.detect_titles_by_text(pdf_path)
        return self.build_tree(detected_nodes, total_pages), total_pages, is_text_pdf


class TargetSectionExtractor:
    @staticmethod
    def _best_match(target_key: str, sections: list[SectionNode]) -> tuple[int | None, SectionNode | None, float]:
        aliases = TARGET_TITLE_ALIASES.get(target_key, [])
        best_idx = None
        best_node = None
        best_score = 0.0

        for idx, sec in enumerate(sections):
            title = sec.normalized_title

            score = 0.0
            for alias in aliases:
                alias_norm = re.sub(r"\s+", "", alias)
                if title == alias_norm:
                    score = max(score, 1.0)
                elif alias_norm in title or title in alias_norm:
                    score = max(score, 0.85)

            if target_key == "future_outlook" and sec.parent_idx is not None:
                parent_title = sections[sec.parent_idx].normalized_title
                if "管理层讨论与分析" in parent_title or "董事会报告" in parent_title:
                    score += 0.05

            if score > best_score:
                best_score = score
                best_idx = idx
                best_node = sec

        return best_idx, best_node, round(min(best_score, 1.0), 4)

    @staticmethod
    def _extract_text_by_pages(pdf_path: Path, start_page: int, end_page: int) -> str:
        doc = fitz.open(pdf_path)
        texts = []
        for p in range(max(start_page, 1) - 1, min(end_page, len(doc))):
            texts.append(doc[p].get_text("text"))
        doc.close()
        return "\n".join(texts).strip()

    def extract_targets(
        self,
        pdf_path: Path,
        sections: list[SectionNode],
        section_db_ids: list[int] | None = None,
    ) -> list[dict[str, Any]]:
        outputs: list[dict[str, Any]] = []

        for target_key in TARGET_TITLE_ALIASES:
            idx, node, confidence = self._best_match(target_key, sections)
            if node is None or confidence < 0.6:
                outputs.append({
                    "target_key": target_key,
                    "matched_section_id": None,
                    "matched_title": None,
                    "matched_path": None,
                    "start_page": None,
                    "end_page": None,
                    "extract_confidence": confidence,
                    "content": None,
                })
                continue

            content = self._extract_text_by_pages(pdf_path, node.page, node.end_page or node.page)
            outputs.append({
                "target_key": target_key,
                "matched_section_id": section_db_ids[idx] if section_db_ids and idx < len(section_db_ids) else None,
                "matched_title": node.title,
                "matched_path": node.path,
                "start_page": node.page,
                "end_page": node.end_page,
                "extract_confidence": confidence,
                "content": content,
            })

        return outputs


class ReportBot:
    def __init__(self, config_path: Path) -> None:
        mysql_config = ConfigLoader.load_mysql_config(config_path)
        self.repo = AnnualReportRepository(mysql_config)
        self.fetcher = CninfoAnnouncementFetcher()
        self.downloader = PdfDownloader()
        self.parser = PdfStructureParser()
        self.extractor = TargetSectionExtractor()

    @staticmethod
    def _file_md5(file_path: Path) -> str:
        h = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()

    def process_one(self, item: dict[str, Any]) -> None:
        announcement_id = item["announcement_id"]
        try:
            self.repo.save_announcement(item)
            self.repo.log(announcement_id, "fetch", "INFO", "公告元数据已写入")

            pdf_path = self.downloader.download(item)
            self.repo.log(announcement_id, "download", "INFO", f"PDF已下载: {pdf_path}")

            sections, page_count, is_text_pdf = self.parser.parse(pdf_path)

            self.repo.save_file_record({
                "announcement_id": announcement_id,
                "local_path": str(pdf_path),
                "file_name": pdf_path.name,
                "file_size": pdf_path.stat().st_size,
                "file_md5": self._file_md5(pdf_path),
                "page_count": page_count,
                "is_text_pdf": 1 if is_text_pdf else 0,
                "download_status": "done",
                "parse_status": "done" if sections else "partial",
                "parse_error": None,
                "downloaded_at": datetime.now(),
                "parsed_at": datetime.now(),
            })

            self.repo.delete_sections(announcement_id)
            section_db_ids = self.repo.save_sections(announcement_id, sections)

            targets = self.extractor.extract_targets(pdf_path, sections, section_db_ids)
            for target in targets:
                self.repo.save_target_section(announcement_id, target)

            self.repo.log(
                announcement_id,
                "parse",
                "INFO",
                "解析完成",
                extra={
                    "section_count": len(sections),
                    "page_count": page_count,
                    "is_text_pdf": is_text_pdf,
                }
            )
            self.repo.commit()

            print(f"[OK] {item['sec_code']} {item['announcement_title']} sections={len(sections)}")

        except Exception as e:
            self.repo.rollback()
            try:
                self.repo.save_file_record({
                    "announcement_id": announcement_id,
                    "local_path": "",
                    "file_name": None,
                    "file_size": None,
                    "file_md5": None,
                    "page_count": None,
                    "is_text_pdf": 1,
                    "download_status": "failed",
                    "parse_status": "failed",
                    "parse_error": str(e),
                    "downloaded_at": None,
                    "parsed_at": None,
                })
                self.repo.log(announcement_id, "error", "ERROR", str(e))
                self.repo.commit()
            except Exception:
                self.repo.rollback()

            print(f"[ERROR] {announcement_id}: {e}")

    def run(self, days: int = 3) -> None:
        try:
            items = self.fetcher.fetch_recent_annual_reports(days=days)
            print(f"最近 {days} 天抓到候选年报: {len(items)}")

            for item in items:
                self.process_one(item)
        finally:
            self.repo.close()


if __name__ == "__main__":
    bot = ReportBot(BASE_DIR / "config.ini")
    bot.run(days=3)