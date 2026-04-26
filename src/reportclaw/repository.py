from __future__ import annotations

import configparser
from datetime import date, datetime
from pathlib import Path

import mysql.connector


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

    def __init__(self, *, conf_dir: Path):
        config = configparser.ConfigParser()
        config.read(conf_dir / "config.ini", encoding="utf-8")
        if not config.has_section("mysql"):
            raise RuntimeError(
                f"config.ini 未读取到 [mysql] 段。请检查路径是否存在：{conf_dir / 'config.ini'}"
            )

        self.conn = mysql.connector.connect(
            host=config["mysql"]["host"],
            port=config.getint("mysql", "port"),
            user=config["mysql"]["user"],
            password=config["mysql"]["pass"],
            database=config["mysql"]["db"]
        )
        self.annual_reports_columns = self._load_table_columns("annual_reports")
        self.stock_master_columns = self._load_table_columns("stock_master_cn")

    def close(self) -> None:
        try:
            if self.conn:
                self.conn.close()
        except Exception:
            pass

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
            SELECT industry_section, main_business_section, future_section, chairman_letter, full_mda
            FROM annual_report_mda
            WHERE report_id=%s
            LIMIT 1
            """,
            (report_id,)
        )
        row = cursor.fetchone()
        if not row:
            return False

        ind, biz, fut, chairman, full = row

        if isinstance(full, str) and full.startswith("[PARSE_FAILED]"):
            return False

        if (biz and len(biz) >= 500) or (fut and len(fut) >= 200) or (ind and len(ind) >= 200) or (
                chairman and len(chairman) >= 200):
            return True

        if full and isinstance(full, str) and len(full) >= 5000:
            return True

        return False

    def upsert_report(self, stock_code, stock_name, year, publish_date, file_path, industry_info=None):
        base_data = {
            "stock_code": stock_code,
            "stock_name": stock_name,
            "report_year": year,
            "publish_date": publish_date,
            "file_path": file_path,
        }
        if industry_info:
            base_data.update(industry_info)

        write_data = self._filter_existing_columns(base_data, "annual_reports")

        existing_id = self.get_report_id(stock_code, year)
        cursor = self.conn.cursor()

        if existing_id is None:
            cols = list(write_data.keys())
            placeholders = ", ".join(["%s"] * len(cols))
            sql = f"INSERT INTO annual_reports ({', '.join(cols)}) VALUES ({placeholders})"
            cursor.execute(sql, tuple(write_data[c] for c in cols))
            self.conn.commit()
            return cursor.lastrowid

        update_data = {k: v for k, v in write_data.items() if k not in {"stock_code", "report_year"}}
        if update_data:
            set_sql = ", ".join([f"{k}=%s" for k in update_data.keys()])
            sql = f"UPDATE annual_reports SET {set_sql} WHERE id=%s"
            cursor.execute(sql, tuple(update_data[k] for k in update_data.keys()) + (existing_id,))
            self.conn.commit()

        return existing_id

    def get_cached_industry_info(self, stock_code: str) -> dict:
        """Prefer stock_master_cn, then fall back to latest annual_reports cached industry."""
        code = str(stock_code or "").strip()
        base = {
            "sw_l1_code": None,
            "sw_l1_name": None,
            "sw_l2_code": None,
            "sw_l2_name": None,
            "sw_l3_code": None,
            "sw_l3_name": None,
            "industry_source": None,
            "industry_lookup_date": None,
        }
        if not code:
            return dict(base)

        cursor = self.conn.cursor(dictionary=True)
        try:
            stock_master_select = [
                "sw_l1_code",
                "sw_l1 AS sw_l1_name",
            ]
            if "sw_l2_code" in self.stock_master_columns:
                stock_master_select.append("sw_l2_code")
            if "sw_l2" in self.stock_master_columns:
                stock_master_select.append("sw_l2 AS sw_l2_name")
            if "sw_l3_code" in self.stock_master_columns:
                stock_master_select.append("sw_l3_code")
            if "sw_l3" in self.stock_master_columns:
                stock_master_select.append("sw_l3 AS sw_l3_name")
            if "industry_source" in self.stock_master_columns:
                stock_master_select.append("industry_source")
            if "industry_lookup_date" in self.stock_master_columns:
                stock_master_select.append("industry_lookup_date")

            cursor.execute(
                f"""
                SELECT {', '.join(stock_master_select)}
                FROM stock_master_cn
                WHERE stock_code=%s
                LIMIT 1
                """,
                (code,),
            )
            row = cursor.fetchone()
            if row and any(row.get(k) for k in ("sw_l1_code", "sw_l1_name", "sw_l2_code", "sw_l2_name", "sw_l3_code", "sw_l3_name")):
                out = dict(base)
                for key in out.keys():
                    if key in row and row.get(key) not in (None, ""):
                        out[key] = row.get(key)
                if not out.get("industry_source"):
                    out["industry_source"] = "stock_master_cn"
                return out

            cursor.execute(
                """
                SELECT
                  sw_l1_code, sw_l1_name,
                  sw_l2_code, sw_l2_name,
                  sw_l3_code, sw_l3_name,
                  industry_source, industry_lookup_date
                FROM annual_reports
                WHERE stock_code=%s
                  AND (
                    COALESCE(sw_l1_code, '') <> '' OR
                    COALESCE(sw_l1_name, '') <> '' OR
                    COALESCE(sw_l2_code, '') <> '' OR
                    COALESCE(sw_l2_name, '') <> '' OR
                    COALESCE(sw_l3_code, '') <> '' OR
                    COALESCE(sw_l3_name, '') <> ''
                  )
                ORDER BY
                  CASE WHEN publish_date IS NULL THEN 1 ELSE 0 END,
                  publish_date DESC,
                  id DESC
                LIMIT 1
                """,
                (code,),
            )
            row = cursor.fetchone()
            if not row:
                return dict(base)
            out = dict(base)
            for key in out.keys():
                if key in row and row.get(key) not in (None, ""):
                    out[key] = row.get(key)
            return out
        finally:
            cursor.close()

    def upsert_stock_master_industry(self, stock_code: str, stock_name: str, industry_info: dict | None) -> None:
        """Persist best-effort industry cache into stock_master_cn."""
        code = str(stock_code or "").strip()
        if not code or not industry_info:
            return

        sw_l1_name = industry_info.get("sw_l1_name")
        sw_l1_code = industry_info.get("sw_l1_code")
        sw_l2_name = industry_info.get("sw_l2_name")
        sw_l2_code = industry_info.get("sw_l2_code")
        sw_l3_name = industry_info.get("sw_l3_name")
        sw_l3_code = industry_info.get("sw_l3_code")
        if not any((sw_l1_name, sw_l1_code, sw_l2_name, sw_l2_code, sw_l3_name, sw_l3_code)):
            return

        exchange = None
        if code.startswith("6"):
            exchange = "SSE"
        elif code.startswith(("0", "3")):
            exchange = "SZSE"
        elif code.startswith(("8", "4", "9")):
            exchange = "BSE"

        data = {
            "stock_code": code,
            "stock_name": stock_name,
            "industry": sw_l1_name,
            "sw_l1": sw_l1_name,
            "sw_l1_code": sw_l1_code,
            "sw_l2": sw_l2_name,
            "sw_l2_code": sw_l2_code,
            "sw_l3": sw_l3_name,
            "sw_l3_code": sw_l3_code,
            "industry_source": industry_info.get("industry_source"),
            "industry_lookup_date": industry_info.get("industry_lookup_date"),
            "exchange": exchange,
        }
        write_data = {k: v for k, v in data.items() if k in self.stock_master_columns}
        cols = list(write_data.keys())
        placeholders = ", ".join(["%s"] * len(cols))
        updates = ", ".join([f"{c}=VALUES({c})" for c in cols if c != "stock_code"])
        sql = (
            f"INSERT INTO stock_master_cn ({', '.join(cols)}) VALUES ({placeholders}) "
            f"ON DUPLICATE KEY UPDATE {updates}"
        )
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql, tuple(write_data[c] for c in cols))
            self.conn.commit()
        finally:
            cursor.close()

    def ensure_stock_master_industry_schema(self) -> None:
        """Add missing industry columns to stock_master_cn so full hierarchy can be stored."""
        existing = set(self.stock_master_columns)
        alters: list[str] = []

        def add_col(name: str, ddl: str) -> None:
            if name not in existing:
                alters.append(f"ADD COLUMN {ddl}")

        add_col("sw_l2", "sw_l2 VARCHAR(64) NULL AFTER sw_l1_code")
        add_col("sw_l2_code", "sw_l2_code VARCHAR(32) NULL AFTER sw_l2")
        add_col("sw_l3", "sw_l3 VARCHAR(64) NULL AFTER sw_l2_code")
        add_col("sw_l3_code", "sw_l3_code VARCHAR(32) NULL AFTER sw_l3")
        add_col("industry_source", "industry_source VARCHAR(32) NULL AFTER citic_l1_code")
        add_col("industry_lookup_date", "industry_lookup_date DATE NULL AFTER industry_source")

        if not alters:
            return

        cursor = self.conn.cursor()
        try:
            cursor.execute("ALTER TABLE stock_master_cn " + ", ".join(alters))
            self.conn.commit()
        finally:
            cursor.close()

        self.stock_master_columns = self._load_table_columns("stock_master_cn")

    def list_stock_universe_for_industry_sync(self) -> list[dict]:
        """Return distinct stock universe merged from stock_master_cn and annual_reports."""
        cursor = self.conn.cursor(dictionary=True)
        try:
            merged: dict[str, dict] = {}

            for sql in (
                """
                SELECT stock_code, stock_name
                FROM stock_master_cn
                WHERE stock_code IS NOT NULL AND stock_code <> ''
                ORDER BY stock_code ASC
                """,
                """
                SELECT stock_code, stock_name
                FROM annual_reports
                WHERE stock_code IS NOT NULL AND stock_code <> ''
                ORDER BY stock_code ASC
                """,
            ):
                cursor.execute(sql)
                rows = cursor.fetchall() or []
                for row in rows:
                    code = str(row.get("stock_code") or "").strip()
                    name = str(row.get("stock_name") or "").strip()
                    if not code:
                        continue
                    old = merged.get(code)
                    if old is None:
                        merged[code] = {"stock_code": code, "stock_name": name}
                    elif (not old.get("stock_name")) and name:
                        old["stock_name"] = name

            out = []
            for code in sorted(merged.keys()):
                row = merged[code]
                code = str(row.get("stock_code") or "").strip()
                name = str(row.get("stock_name") or "").strip()
                if code:
                    out.append({"stock_code": code, "stock_name": name})
            return out
        finally:
            cursor.close()

    def update_annual_reports_industry_by_stock(self, stock_code: str, industry_info: dict | None) -> int:
        """Bulk update industry fields for all annual_reports rows of one stock."""
        code = str(stock_code or "").strip()
        if not code or not industry_info:
            return 0

        data = self._normalize_industry_info_for_write(industry_info)
        if not any(data.get(k) for k in ("sw_l1_code", "sw_l1_name", "sw_l2_code", "sw_l2_name", "sw_l3_code", "sw_l3_name")):
            return 0

        write_data = self._filter_existing_columns(data, "annual_reports")
        if not write_data:
            return 0

        set_sql = ", ".join([f"{k}=%s" for k in write_data.keys()])
        sql = f"UPDATE annual_reports SET {set_sql} WHERE stock_code=%s"
        cursor = self.conn.cursor()
        try:
            params = tuple(write_data[k] for k in write_data.keys()) + (code,)
            cursor.execute(sql, params)
            self.conn.commit()
            return int(cursor.rowcount or 0)
        finally:
            cursor.close()

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
        cursor.execute("SELECT id FROM annual_report_mda WHERE report_id=%s LIMIT 1", (report_id,))
        row = cursor.fetchone()
        if row:
            sql = """
            UPDATE annual_report_mda
            SET industry_section=%s,
                main_business_section=%s,
                future_section=%s,
                chairman_letter=%s,
                full_mda=%s
            WHERE report_id=%s
            """
            cursor.execute(sql, (
                mda.get("industry"),
                mda.get("business"),
                mda.get("future"),
                mda.get("chairman_letter"),
                mda.get("full_mda"),
                report_id
            ))
        else:
            sql = """
            INSERT INTO annual_report_mda
            (report_id, industry_section, main_business_section, future_section, chairman_letter, full_mda)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                report_id,
                mda.get("industry"),
                mda.get("business"),
                mda.get("future"),
                mda.get("chairman_letter"),
                mda.get("full_mda"),
            ))
        self.conn.commit()

    def _load_table_columns(self, table_name: str) -> set[str]:
        cursor = self.conn.cursor()
        cursor.execute(f"SHOW COLUMNS FROM {table_name}")
        return {row[0] for row in cursor.fetchall()}

    def _filter_existing_columns(self, data: dict, table_name: str) -> dict:
        if table_name == "annual_reports":
            allowed = self.annual_reports_columns
        else:
            allowed = self._load_table_columns(table_name)
        return {k: v for k, v in data.items() if k in allowed}

    @staticmethod
    def _normalize_industry_info_for_write(industry_info: dict) -> dict:
        out = {}
        for key in (
            "sw_l1_code",
            "sw_l1_name",
            "sw_l2_code",
            "sw_l2_name",
            "sw_l3_code",
            "sw_l3_name",
            "industry_source",
            "industry_lookup_date",
        ):
            if key not in industry_info:
                continue
            val = industry_info.get(key)
            if isinstance(val, (datetime, date)):
                out[key] = val.strftime("%Y-%m-%d")
            else:
                out[key] = val
        return out
