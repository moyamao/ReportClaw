"""
ReportClaw - A股股票主数据（stock_master_cn）同步脚本（AKShare 申万一级）

你要解决的问题
- 年报入库时：根据 stock_code 快速查到公司所属“大行业”（投研口径更偏申万一级）。
- 这张表只服务 A 股（CN），港股/美股你计划用另一张表，不在此脚本范围内。

数据来源（研究口径）
- 申万一级行业：AKShare
  - 一级行业列表：sw_index_first_info
  - 一级行业成分股：index_component_sw(symbol=<申万一级指数代码>)
  说明：这是抓取公开网站的数据，稳定性不如付费源，因此脚本做了重试/容错/降级。

- 目标股票集合：默认从本地 MySQL 的 annual_reports 表读取 DISTINCT stock_code/stock_name（只补你抓到过年报的股票）；需要全市场时用 --full-market。

配置（conf/config.ini）
[mysql]
host=127.0.0.1
port=3306
user=...
pass=...
db=stock

运行
    ./venv/bin/python src/reportclaw/sync_stock_master.py

参数
- --config: 指定 config.ini 路径（默认 conf/config.ini）
- --no-sw: 不拉取申万行业（只同步股票列表 + 交易所）
- --limit: 仅同步前 N 条（调试用）
- --dry-run: 不写库，只打印统计
- --sleep: 拉申万成分股时每个指数之间的 sleep 秒数（默认 0.25）
- --retries: 单个指数失败重试次数（默认 3）
- --full-market: 用 AKShare/Eastmoney 拉全市场股票（默认只同步你抓到过年报的股票）
- --sw-map-csv: 本地申万一级行业映射 CSV（stock_code,sw_l1,sw_l1_code，可选）
- --sw-cache: 申万一级行业映射缓存 JSON 路径（默认 data/cache/sw_l1_map.json）

输出（upsert 到 stock_master_cn）
- stock_code(6位), stock_name, exchange(SSE/SZSE/BSE), industry(=sw_l1), sw_l1, sw_l1_code
- citic_l1/citic_l1_code 字段保留但本脚本不填（置 NULL），方便你未来接入别的中信口径源。

依赖
    pip install akshare pymysql pandas lxml requests

注意
- 申万映射是“按申万一级指数成分股”反推行业：一个股票理论上只应落在一个一级行业指数里；
  若出现重复（极少数边界情况/数据源问题），脚本默认“后覆盖前”，并记录冲突计数。
"""

from __future__ import annotations

import argparse
import configparser
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import os
import socket
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

import pymysql

try:
    import akshare as ak
except Exception as e:
    raise RuntimeError("缺少 akshare 依赖。请先安装：pip install akshare") from e

# Avoid infinite hangs in requests/urllib3 (AKShare sometimes does not set timeout)
socket.setdefaulttimeout(25)


# -----------------------------
# Config / DB
# -----------------------------

def _guess_project_root() -> Path:
    # sync_stock_master.py 位于 src/reportclaw/
    return Path(__file__).resolve().parents[2]


def load_config(config_path: Optional[str]) -> configparser.ConfigParser:
    root = _guess_project_root()
    if config_path:
        p = Path(config_path)
        if not p.is_absolute():
            p = (root / p).resolve()
    else:
        p = (root / "conf" / "config.ini").resolve()

    if not p.exists():
        raise RuntimeError(f"config file not found: {p}")

    cfg = configparser.ConfigParser()
    cfg.read(p, encoding="utf-8")
    return cfg


def mysql_connect(cfg: configparser.ConfigParser):
    if not cfg.has_section("mysql"):
        raise RuntimeError("config.ini 缺少 [mysql] 段")

    return pymysql.connect(
        host=cfg.get("mysql", "host"),
        port=cfg.getint("mysql", "port", fallback=3306),
        user=cfg.get("mysql", "user"),
        password=cfg.get("mysql", "pass"),
        database=cfg.get("mysql", "db"),
        charset="utf8mb4",
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


def ensure_schema(conn) -> None:
    """确保 stock_master_cn 所需字段存在（幂等）。"""
    cols_sql = "SHOW COLUMNS FROM stock_master_cn"
    with conn.cursor() as cur:
        cur.execute(cols_sql)
        cols = {r["Field"] for r in cur.fetchall()}

    alters: List[str] = []

    def add_col(name: str, ddl: str) -> None:
        if name not in cols:
            alters.append(f"ADD COLUMN {ddl}")

    # 交易所（可选，但很实用）
    add_col("exchange", "exchange ENUM('SSE','SZSE','BSE') NULL AFTER industry")

    # 申万一级
    add_col("sw_l1", "sw_l1 VARCHAR(64) NULL AFTER industry")
    add_col("sw_l1_code", "sw_l1_code VARCHAR(32) NULL AFTER sw_l1")

    # 中信一级（保留字段但本脚本不填）
    add_col("citic_l1", "citic_l1 VARCHAR(64) NULL AFTER sw_l1_code")
    add_col("citic_l1_code", "citic_l1_code VARCHAR(32) NULL AFTER citic_l1")

    if not alters:
        return

    sql = "ALTER TABLE stock_master_cn " + ", ".join(alters)
    with conn.cursor() as cur:
        cur.execute(sql)
    print(f"[db] schema updated: {len(alters)} columns added")


# -----------------------------
# AKShare fetch helpers
# -----------------------------

@dataclass
class StockBasic:
    stock_code: str  # 6位
    name: str
    exchange: str    # SSE/SZSE/BSE


def _guess_exchange_from_code(code: str) -> str:
    code = (code or "").strip()
    if code.startswith("6"):
        return "SSE"
    if code.startswith(("0", "3")):
        return "SZSE"
    # 北交所常见 8/4/9 开头（含 92xxxx）
    return "BSE"


def _call_if_exists(fn_name: str):
    """
    Call ak.<fn_name>() if it exists, otherwise return None.
    This helps handle AKShare version differences.
    """
    fn = getattr(ak, fn_name, None)
    if fn is None:
        return None
    return fn()


def _concat_hs_list():
    """
    Build沪深A股列表（不触发北交所/BSE 请求）.

    AKShare 新版本的 stock_info_a_code_name() 可能内部会去拉 www.bse.cn，
    在某些网络/代理环境下会出现 SSL EOF，导致整个同步失败。
    这里优先使用分别的沪/深列表接口（若存在），否则再退回 stock_info_a_code_name。
    """
    import pandas as pd

    # Avoid using global proxy env vars for AKShare requests
    with _without_proxy_env():
        sh = _call_if_exists("stock_info_sh_name_code")
        sz = _call_if_exists("stock_info_sz_name_code")
    if sh is not None and sz is not None:
        return pd.concat([sh, sz], ignore_index=True)

    # Fallback: try A-share list (may still try BJ in some versions)
    with _without_proxy_env():
        return _call_if_exists("stock_info_a_code_name")


def fetch_stock_basic_em() -> List[StockBasic]:
    """
    获取沪深A股列表（东方财富现货接口），不依赖北交所/BSE，通常更稳定且数量完整（~5000+）。
    """
    with _without_proxy_env():
        df = ak.stock_zh_a_spot_em()
    cols = list(df.columns)
    # Common columns: 代码, 名称
    code_col = "代码" if "代码" in cols else ("code" if "code" in cols else None)
    name_col = "名称" if "名称" in cols else ("name" if "name" in cols else None)
    if not code_col or not name_col:
        # Fallback to first two columns
        if len(cols) >= 2:
            code_col, name_col = cols[0], cols[1]
        else:
            raise RuntimeError(f"stock_zh_a_spot_em 返回列异常: {cols}")

    res: List[StockBasic] = []
    for _, r in df.iterrows():
        code = str(r.get(code_col) or "").strip()
        name = str(r.get(name_col) or "").strip()
        if not code:
            continue
        # Eastmoney returns 6-digit A-share codes
        res.append(StockBasic(stock_code=code, name=name, exchange=_guess_exchange_from_code(code)))
    # 去重
    mp: Dict[str, StockBasic] = {x.stock_code: x for x in res}
    return list(mp.values())


def fetch_stock_basic_akshare() -> List[StockBasic]:
    """获取全市场 A股列表（沪深 + 北交所）。"""
    res: List[StockBasic] = []

    # 优先使用东方财富接口获取沪深A股列表（数量更完整，且不触发 BSE）
    try:
        em_list = fetch_stock_basic_em()
        res.extend(em_list)
        # Sanity check: Eastmoney should usually return 5000+ A-share codes (沪深).
        # If too small, fall back to SH+SZ list and merge.
        if len(em_list) < 4000:
            print(f"[warn] 东方财富沪深A股列表数量偏小（{len(em_list)}），回退合并沪深列表以补全…")
            df2 = _concat_hs_list()
            if df2 is not None:
                cols2 = list(df2.columns)
                if "code" in cols2 and "name" in cols2:
                    code_col2, name_col2 = "code", "name"
                elif "代码" in cols2 and "名称" in cols2:
                    code_col2, name_col2 = "代码", "名称"
                elif len(cols2) >= 2:
                    code_col2, name_col2 = cols2[0], cols2[1]
                else:
                    code_col2, name_col2 = None, None

                if code_col2 and name_col2:
                    for _, rr in df2.iterrows():
                        code2 = str(rr.get(code_col2) or "").strip()
                        name2 = str(rr.get(name_col2) or "").strip()
                        if not code2:
                            continue
                        res.append(StockBasic(stock_code=code2, name=name2, exchange=_guess_exchange_from_code(code2)))
    except Exception as e:
        print(f"[warn] 东方财富沪深A股列表获取失败，回退到 AKShare 内置列表：{e}")
        # 沪深 A 股（避免触发北交所/BSE SSL 问题）
        df = _concat_hs_list()
        if df is None:
            raise RuntimeError("无法获取沪深A股列表：AKShare 缺少 stock_info_sh_name_code/stock_info_sz_name_code 或 stock_info_a_code_name")
        # 历史版本列名可能是 item/value 或 code/name，做兼容
        cols = list(df.columns)
        if "code" in cols and "name" in cols:
            code_col, name_col = "code", "name"
        elif "代码" in cols and "名称" in cols:
            code_col, name_col = "代码", "名称"
        elif len(cols) >= 2:
            code_col, name_col = cols[0], cols[1]
        else:
            raise RuntimeError(f"stock_info_a_code_name 返回列异常: {cols}")

        for _, r in df.iterrows():
            code = str(r.get(code_col) or "").strip()
            name = str(r.get(name_col) or "").strip()
            if not code:
                continue
            res.append(StockBasic(stock_code=code, name=name, exchange=_guess_exchange_from_code(code)))

    # 北交所
    try:
        with _without_proxy_env():
            bj = ak.stock_info_bj_name_code()
        bj_cols = list(bj.columns)
        if "code" in bj_cols and "name" in bj_cols:
            ccol, ncol = "code", "name"
        elif "代码" in bj_cols and "名称" in bj_cols:
            ccol, ncol = "代码", "名称"
        elif len(bj_cols) >= 2:
            ccol, ncol = bj_cols[0], bj_cols[1]
        else:
            ccol, ncol = None, None

        if ccol and ncol:
            for _, r in bj.iterrows():
                code = str(r.get(ccol) or "").strip()
                name = str(r.get(ncol) or "").strip()
                if not code:
                    continue
                # BJ 统一标记 BSE
                res.append(StockBasic(stock_code=code, name=name, exchange="BSE"))
    except Exception as e:
        # 允许没有北交所接口/网络失败（常见：BSE 站点 SSL EOF）
        print(f"[warn] 获取北交所列表失败，已跳过：{e}")

    # 去重（以 code 为准，后出现覆盖前出现）
    mp: Dict[str, StockBasic] = {}
    for x in res:
        mp[x.stock_code] = x
    return list(mp.values())


def _pick_first_matching_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in cols:
            return c
    return None


def _retry_call(fn, *, retries: int, sleep_sec: float, desc: str, fatal: bool = False):
    """
    Retry wrapper.
    - fatal=False: after retries, return None and let caller decide to skip
    - fatal=True:  after retries, raise RuntimeError
    """
    last = None
    for i in range(retries):
        try:
            return fn()
        except Exception as e:
            last = e
            time.sleep(sleep_sec * (i + 1))
    if fatal:
        raise RuntimeError(f"{desc} 失败（retries={retries}），最后错误: {last}") from last
    print(f"[warn] {desc} 连续失败（retries={retries}），已跳过。最后错误: {last}")
    return None


# --- Add: helper to call with hard timeout (for index_component_sw) ---
def _call_with_timeout(fn, *, timeout_sec: int, desc: str):
    """Run a callable with a hard wall-clock timeout. Return None on timeout."""
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(fn)
        try:
            return fut.result(timeout=timeout_sec)
        except FuturesTimeoutError:
            print(f"[warn] {desc} 超时（{timeout_sec}s），已跳过")
            return None


@contextmanager
def _without_proxy_env():
    keys = ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "NO_PROXY", "no_proxy"]
    old = {k: os.environ.get(k) for k in keys}
    try:
        for k in keys:
            if k in os.environ:
                del os.environ[k]
        yield
    finally:
        for k, v in old.items():
            if v is None:
                if k in os.environ:
                    del os.environ[k]
            else:
                os.environ[k] = v


def _safe_index_component_sw(symbol: str, *, timeout_sec: int = 20):
    """
    Call ak.index_component_sw(symbol=...) but be robust to hangs / upstream changes.

    - Hard timeout to avoid indefinite hang (AKShare requests may not set timeout).
    - Any error/timeout returns None so the caller can skip this index.
    """
    def _do():
        with _without_proxy_env():
            return ak.index_component_sw(symbol=symbol)

    try:
        return _call_with_timeout(_do, timeout_sec=timeout_sec, desc=f"index_component_sw({symbol})")
    except KeyError:
        return None
    except Exception as e:
        return None


def fetch_sw_l1_map_akshare(*, sleep_between: float = 0.25, retries: int = 3, target_codes: Optional[set] = None, comp_timeout_sec: int = 20) -> Dict[str, Tuple[str, str]]:
    """
    申万一级 -> 股票映射
    return: stock_code -> (sw_l1_name, sw_l1_code)

    原理：
    - 先拿申万一级行业指数列表（sw_index_first_info）
    - 对每个一级指数 code，拉成分股（index_component_sw）
    - 将成分股的“证券代码”映射到该一级行业
    - Early-stop if all target_codes mapped (if provided)
    """
    df = ak.sw_index_first_info()
    cols = list(df.columns)

    code_col = _pick_first_matching_col(cols, ["行业代码", "指数代码", "代码", "symbol"])
    name_col = _pick_first_matching_col(cols, ["行业名称", "指数名称", "名称", "name"])

    if not code_col or not name_col:
        # 最保守：按前两列兜底
        if len(cols) >= 2:
            code_col, name_col = cols[0], cols[1]
        else:
            raise RuntimeError(f"sw_index_first_info 返回列异常: {cols}")

    mp: Dict[str, Tuple[str, str]] = {}
    conflict = 0

    for _, r in df.iterrows():
        idx_code = str(r.get(code_col) or "").strip()
        idx_name = str(r.get(name_col) or "").strip()
        if not idx_code or not idx_name:
            continue

        def _get_component():
            return _safe_index_component_sw(idx_code, timeout_sec=comp_timeout_sec)

        comp = _retry_call(_get_component, retries=retries, sleep_sec=0.5, desc=f"index_component_sw({idx_code})")
        if comp is None:
            print(f"[warn] index_component_sw({idx_code}) 返回异常（列缺失/反爬/空页），已跳过该一级行业：{idx_name}")
            time.sleep(max(0.0, sleep_between))
            continue

        ccols = list(comp.columns)
        sec_code_col = _pick_first_matching_col(ccols, ["证券代码", "成份券代码", "股票代码", "代码", "code"])
        if not sec_code_col:
            if len(ccols) >= 2:
                sec_code_col = ccols[0]
            else:
                continue

        for _, rr in comp.iterrows():
            stock_code = str(rr.get(sec_code_col) or "").strip()
            if not stock_code:
                continue
            if stock_code in mp and mp[stock_code][0] != idx_name:
                conflict += 1
            mp[stock_code] = (idx_name, idx_code)

        if target_codes is not None and target_codes.issubset(mp.keys()):
            print("[ak] SW mapping early-stop: all target stocks mapped")
            break
        time.sleep(max(0.0, sleep_between))

    if conflict:
        print(f"[warn] 申万一级映射发现冲突（同一股票被多个一级指数包含）：{conflict}（已采用后覆盖前）")

    return mp
# -----------------------------
# CLI
# -----------------------------

# --- Add: DB helper to load target stock universe from annual_reports ---
def load_target_stocks_from_annual_reports(conn) -> List[StockBasic]:
    """
    Build target universe from existing annual_reports table, avoiding any external
    'full market stock list' dependency.

    We only need industry for stocks we have reports for.
    """
    sql = (
        "SELECT DISTINCT stock_code, stock_name "
        "FROM annual_reports "
        "WHERE stock_code IS NOT NULL AND stock_code<>''"
    )
    out: List[StockBasic] = []
    with conn.cursor() as cur:
        cur.execute(sql)
        for r in cur.fetchall():
            code = str(r.get("stock_code") or "").strip()
            name = str(r.get("stock_name") or "").strip()
            if not code:
                continue
            out.append(StockBasic(stock_code=code, name=name, exchange=_guess_exchange_from_code(code)))
    return out


# -----------------------------
# Upsert
# -----------------------------

def upsert_stock_master_cn(
    conn,
    rows: List[Tuple[str, str, Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], str]],
) -> None:
    """
    rows:
      (stock_code, stock_name, industry, sw_l1, sw_l1_code, citic_l1, citic_l1_code, exchange)
    """
    sql = (
        "INSERT INTO stock_master_cn (stock_code, stock_name, industry, sw_l1, sw_l1_code, citic_l1, citic_l1_code, exchange) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE "
        "stock_name=VALUES(stock_name), "
        "industry=VALUES(industry), "
        "sw_l1=VALUES(sw_l1), sw_l1_code=VALUES(sw_l1_code), "
        "citic_l1=VALUES(citic_l1), citic_l1_code=VALUES(citic_l1_code), "
        "exchange=VALUES(exchange)"
    )
    with conn.cursor() as cur:
        cur.executemany(sql, rows)


# -----------------------------
# CLI
# -----------------------------


# --- Add: helper functions for SW map CSV/JSON cache ---
def _read_sw_map_csv(path: str) -> Dict[str, Tuple[str, str]]:
    """Read local CSV mapping: stock_code, sw_l1, sw_l1_code."""
    import csv

    mp: Dict[str, Tuple[str, str]] = {}
    if not path:
        return mp
    p = Path(path)
    if not p.exists():
        raise RuntimeError(f"sw-map-csv not found: {p}")

    with p.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            code = str(r.get("stock_code") or r.get("code") or "").strip()
            l1 = str(r.get("sw_l1") or r.get("industry") or "").strip()
            l1c = str(r.get("sw_l1_code") or r.get("industry_code") or "").strip()
            if code and l1:
                mp[code] = (l1, l1c)
    return mp


def _read_sw_cache_json(path: str) -> Dict[str, Tuple[str, str]]:
    """Read SW map cache json: {stock_code: [sw_l1, sw_l1_code]} or {stock_code: {sw_l1, sw_l1_code}}."""
    import json

    mp: Dict[str, Tuple[str, str]] = {}
    if not path:
        return mp
    p = Path(path)
    if not p.exists():
        return mp

    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return mp

    if isinstance(obj, dict):
        for k, v in obj.items():
            code = str(k).strip()
            if not code:
                continue
            if isinstance(v, (list, tuple)) and len(v) >= 1:
                l1 = str(v[0] or "").strip()
                l1c = str(v[1] or "").strip() if len(v) >= 2 else ""
                if l1:
                    mp[code] = (l1, l1c)
            elif isinstance(v, dict):
                l1 = str(v.get("sw_l1") or v.get("industry") or "").strip()
                l1c = str(v.get("sw_l1_code") or v.get("industry_code") or "").strip()
                if l1:
                    mp[code] = (l1, l1c)
    return mp


def _write_sw_cache_json(path: str, mp: Dict[str, Tuple[str, str]]) -> None:
    """Write SW map cache json."""
    import json

    if not path:
        return
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    obj = {k: [v[0], v[1]] for k, v in mp.items()}
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=None, help="config.ini path (default: conf/config.ini)")
    ap.add_argument("--no-sw", action="store_true", help="skip SW industry mapping")
    ap.add_argument("--limit", type=int, default=0, help="debug limit N stocks")
    ap.add_argument("--dry-run", action="store_true", help="do not write MySQL")
    ap.add_argument("--sleep", type=float, default=0.25, help="sleep seconds between SW index calls")
    ap.add_argument("--retries", type=int, default=3, help="retries per SW index_component_sw call")
    ap.add_argument("--full-market", action="store_true", help="sync full A-share list via AKShare/Eastmoney (may be blocked)")
    ap.add_argument("--sw-map-csv", default="", help="local CSV mapping for SW L1: stock_code,sw_l1,sw_l1_code (optional)")
    ap.add_argument("--sw-cache", default="data/cache/sw_l1_map.json", help="path to SW map cache json (default: data/cache/sw_l1_map.json)")
    args = ap.parse_args()

    cfg = load_config(args.config)

    conn = mysql_connect(cfg)
    ensure_schema(conn)

    print("[ak] building target stock universe ...")
    if args.full_market:
        basics = fetch_stock_basic_akshare()
        src = "full-market"
    else:
        basics = load_target_stocks_from_annual_reports(conn)
        src = "annual_reports"
    if args.limit and args.limit > 0:
        basics = basics[: args.limit]
    print(f"[ak] stock list size: {len(basics)} (source={src})")

    sw_map: Dict[str, Tuple[str, str]] = {}

    # 1) local CSV mapping (highest priority)
    if args.sw_map_csv:
        try:
            sw_map.update(_read_sw_map_csv(args.sw_map_csv))
            print(f"[ak] SW map loaded from CSV: {len(sw_map)}")
        except Exception as e:
            print(f"[warn] 读取 sw-map-csv 失败：{e}")

    # 2) cache json (fallback)
    if args.sw_cache:
        cached = _read_sw_cache_json(args.sw_cache)
        if cached:
            # do not overwrite CSV-provided keys
            for k, v in cached.items():
                if k not in sw_map:
                    sw_map[k] = v
            print(f"[ak] SW map loaded from cache: {len(cached)} (merged_total={len(sw_map)})")

    if not args.no_sw:
        print("[ak] building SW L1 map (sw_index_first_info + index_component_sw) ...")
        target_codes = {b.stock_code for b in basics} if basics else None

        online = fetch_sw_l1_map_akshare(
            sleep_between=args.sleep,
            retries=args.retries,
            target_codes=target_codes,
            comp_timeout_sec=20,
        )

        if online:
            # merge online, overwriting cache for freshness
            sw_map.update(online)
            print(f"[ak] SW map size (online): {len(online)} (merged_total={len(sw_map)})")
            _write_sw_cache_json(args.sw_cache, sw_map)
        else:
            print(f"[warn] 在线申万成分抓取结果为空（可能被反爬/网络阻断）。将仅使用本地 CSV/缓存（size={len(sw_map)}）")

    out_rows: List[Tuple[str, str, Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], str]] = []
    miss_sw = 0

    for b in basics:
        sw_l1 = None
        sw_l1_code = None
        if sw_map and b.stock_code in sw_map:
            sw_l1, sw_l1_code = sw_map[b.stock_code]
        else:
            miss_sw += 1

        # 为兼容主流程：industry 默认写申万一级
        industry = sw_l1

        out_rows.append(
            (
                b.stock_code,
                b.name,
                industry,
                sw_l1,
                sw_l1_code,
                None,  # citic_l1 (not supported here)
                None,  # citic_l1_code
                b.exchange,
            )
        )

    print(f"[stat] stocks={len(basics)}, miss_sw={miss_sw if not args.no_sw else 'SKIP'}")

    if args.dry_run:
        print("[dry-run] skip db write")
        return

    print("[db] upserting stock_master_cn ...")
    upsert_stock_master_cn(conn, out_rows)
    print("[db] done")


if __name__ == "__main__":
    main()
