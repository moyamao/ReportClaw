from __future__ import annotations

import configparser
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
