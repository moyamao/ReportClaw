import os
import re
import time
import requests
import pdfplumber
from pdfminer.high_level import extract_text as pdfminer_extract_text
import configparser
import mysql.connector
from datetime import datetime, timedelta
import traceback

from pathlib import Path

# main.py 位于 src/reportclaw/ 下，所以项目根目录是再向上两级
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONF_DIR = PROJECT_ROOT / "conf"
DATA_DIR = PROJECT_ROOT / "data"
DOWNLOADS_DIR = DATA_DIR / "downloads"
DAILY_DIR = DATA_DIR / "report"
STATE_DIR = DATA_DIR / "state"


# ===============================
# 数据库客户端
# ===============================
class MySQLClient:

    def __init__(self):
        config = configparser.ConfigParser()
        config.read(CONF_DIR / "config.ini", encoding="utf-8")
        if not config.has_section("mysql"):
            raise RuntimeError(
                f"config.ini 未读取到 [mysql] 段。请检查路径是否存在：{CONF_DIR / 'config.ini'}"
            )

        self.conn = mysql.connector.connect(
            host=config["mysql"]["host"],
            port=config.getint("mysql", "port"),
            user=config["mysql"]["user"],
            password=config["mysql"]["pass"],
            database=config["mysql"]["db"]
        )

    def exists(self, stock_code, year):
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT id FROM annual_reports WHERE stock_code=%s AND report_year=%s",
            (stock_code, year)
        )
        return cursor.fetchone() is not None

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
        sql = """
        INSERT INTO annual_report_mda
        (report_id, industry_section, main_business_section, future_section, full_mda)
        VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(sql, (
            report_id,
            mda["industry"],
            mda["business"],
            mda["future"],
            mda["full_mda"]
        ))
        self.conn.commit()


# ===============================
# PDF解析器
# ===============================
class AnnualReportParser:
    MAJOR_HEADING_KEYWORDS = [
        "核心竞争力", "核心竞争力分析",
        "主营业务分析", "主营业务",
        "非主营业务分析", "非主营业务",
        "资产及负债状况分析", "资产及负债", "资产负债",
        "投资状况分析", "投资状况",
        "公司治理", "重要事项",
        "公司未来发展的展望", "未来发展的展望",
        "行业情况", "行业状况", "所属行业", "所处行业", "行业概况",
        "从事的主要业务", "主要业务", "主营业务",
        "公司未来发展的展望"
    ]

    def extract_text(self, pdf_path, page_numbers=None):
        # 使用 pdfminer 按页提取（支持只读指定页）
        text = pdfminer_extract_text(pdf_path, page_numbers=page_numbers)
        if not text:
            return ""
        return self.normalize(text)

    def _slice_to_next_bracket_heading(self, text: str, start_idx: int):
        """
        从（X）/ (X) 这类括号小标题开始切，到下一条同级括号小标题或下一条“一级大标题（序号+、）”。
        """
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

    def normalize(self, text):
        # 统一换行/去除不可见分页符
        text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\x0c", "\n")

        # 先做一次基本清理
        text = re.sub(r"\n+", "\n", text)

        # 按行过滤页眉/页脚/页码等噪声
        lines = []
        for raw in text.split("\n"):
            line = raw.strip()
            if not line:
                continue

            # 1) 纯页码行（如：11）
            if re.fullmatch(r"\d{1,4}", line):
                continue

            # 1.1) 形如 14/248 的页码
            if re.fullmatch(r"\d{1,4}\s*/\s*\d{1,4}", line):
                continue

            # 1.2) 常见页眉（公司名 + 年度报告）
            if ("年度报告" in line) and ("股份有限公司" in line):
                continue

            # 1.3) “公司代码/公司简称”页眉
            if line.startswith("公司代码：") or line.startswith("公司简称：") or ("公司代码：" in line):
                continue

            # 1.4) 仅公司名一行的页眉（常见：XXX股份有限公司 / XXX有限公司）
            if (line.endswith("股份有限公司") or line.endswith("有限公司")) and len(line) <= 30:
                continue

            # 1.5) 仅“XXXX年年度报告/年度报告全文/年度报告”一行的页眉（常与公司名分成两行）
            if ("年度报告" in line) and len(line) <= 20:
                continue

            # 2) 表格边框/分隔符（如：---+、|、+--- 等）
            if re.fullmatch(r"[-+|]{3,}", line):
                continue

            # 3) 常见年报页眉（包含“年度报告全文”）
            if "年度报告全文" in line:
                continue

            lines.append(line)

        # 最后再做你原先的处理：去掉所有空格，并压缩多余空行
        text = "\n".join(lines)
        text = text.replace(" ", "")
        text = re.sub(r"\n+", "\n", text)
        return text

    def extract_mda(self, pdf_path):

        # 1) 目录定位：先在前20页里寻找“目录”所在页，再只在目录附近提取第三节页码
        start_page = None
        toc_page = None
        for p in range(0, 20):
            t = self.extract_text(pdf_path, page_numbers=[p])
            if t and ("目录" in t or "目 录" in t):
                toc_page = p
                break

        toc_pages = list(range(toc_page, min(toc_page + 4, 20))) if toc_page is not None else list(range(0, 6))
        toc_text = self.extract_text(pdf_path, page_numbers=toc_pages)

        # 目录行通常形如：第三节 管理层讨论与分析........14
        m = re.search(r"第三节\s*管理层讨论与分析[\.·…\s]{2,200}(\d{1,4})", toc_text)
        if m:
            try:
                page_num = int(m.group(1))
                # 目录页码不可能是 1/2/3 这种（通常 > 5）；小数字很可能误匹配到“(一)/(二)”
                if page_num >= 5:
                    start_page = page_num - 1
                else:
                    start_page = None
            except Exception:
                start_page = None
        # 1.5) 起始页校验：避免把“目录页/前言页”误当作第三节正文起点
        if start_page is not None:
            # 年报第三节一般不会在很靠前的页（<5 绝大概率是误判/目录页）
            if start_page < 5:
                start_page = None
            else:
                # 若落在目录页附近或该页包含“目录”，说明仍是目录区域，强制放弃目录定位
                try:
                    check_text = self.extract_text(pdf_path, page_numbers=[start_page])
                except Exception:
                    check_text = ""
                if check_text and ("目录" in check_text or "目 录" in check_text):
                    start_page = None
                # 若 start_page 仍然落在 toc_pages 范围内，也视为目录页误判
                if start_page is not None and start_page in toc_pages:
                    start_page = None

        # 2) 目录失败：正文扫描定位（前200页逐页找“第三节 管理层讨论与分析”）
        if start_page is None:
            scan_start = (toc_page + 1) if toc_page is not None else 0
            for p in range(scan_start, 200):
                page_text = self.extract_text(pdf_path, page_numbers=[p])
                if not page_text:
                    continue
                # 跳过目录页（目录页经常包含“第三节…14”导致误命中）
                if "目录" in page_text or "目 录" in page_text:
                    continue
                if re.search(r"第三节\s*管理层讨论与分析", page_text):
                    start_page = p
                    break

        if start_page is None:
            print("未找到第三节起始位置（目录无页码且正文未命中）")
            return None

        print("第三节正文起始页:", start_page)

        # 3) 从起始页开始读到第四节
        mda_text = ""
        for p in range(start_page, start_page + 200):
            page_text = self.extract_text(pdf_path, page_numbers=[p])
            if not page_text:
                continue
            if re.search(r"第\s*四\s*节|第四节", page_text):
                break
            mda_text += page_text + "\n"

        if not mda_text:
            return None

        # 强制裁剪到“第三节 管理层讨论与分析”开始（防止夹带重要提示/目录等导致误匹配“一、…保证…”）
        m3 = re.search(r"第三节\s*管理层讨论与分析", mda_text)
        if m3:
            mda_text = mda_text[m3.start():].strip()

        stop_titles = [
            "报告期内核心竞争力分析",
            "核心竞争力分析",
            "主营业务分析",
            "公司治理",
            "重要事项",
            "公司未来发展的展望",
            "未来发展的展望",
            "风险因素",
            "风险提示",
            "经营情况讨论与分析",
        ]

        # mda_text 已从“第三节 管理层讨论与分析”开始；这里直接切到下一重大标题（如“三、报告期内核心竞争力分析”）之前
        management_overview = self._slice_to_next_heading_with_title_keywords(mda_text, 0, stop_titles)
        if not management_overview:
            management_overview = mda_text.strip() if mda_text else None

        # 二次兜底：有些PDF标题行被拆行/缺标点，导致上面的切分没命中 stop_titles。
        # 这里直接用正则定位“(三、/3.) 报告期内核心竞争力分析/核心竞争力分析”并强制截断，确保不包含该章节。
        m_core = re.search(
            r"(?:^|\n)\s*(?:[一二三四五六七八九十]{1,3}|\d{1,2})[、\.．:：]\s*[^\n]{0,80}核心竞争力分析",
            mda_text
        )
        if m_core and m_core.start() > 0:
            management_overview = mda_text[:m_core.start()].strip()

        future = self.extract_section_by_keywords(
            mda_text,
            keywords=[
                "公司未来发展的展望",
                "未来发展的展望",
                "未来发展展望",
                "发展规划",
                "未来规划"
            ],
            fallback_ordinals=None,
            end_title_keywords=None
        )

        # 校验/兜底
        if management_overview is not None and len(management_overview) < 500:
            management_overview = mda_text.strip() if mda_text else management_overview
        if future is not None and len(future) < 200:
            future = None

        industry = None
        business = management_overview

        return {
            "industry": industry,
            "business": business,
            "future": future,
            "full_mda": mda_text
        }

    def _slice_to_next_heading_with_title_keywords(self, text, start_idx, title_keywords):
        """
        从 start_idx 切到“下一条标题”，且该标题行的标题部分包含 title_keywords 之一。
        同时支持：
        - 一级标题：二、xxx / 2、xxx
        - 括号小标题：（三）xxx / (3)xxx
        """
        if start_idx is None or start_idx < 0:
            return None

        candidates = []

        # A) 一级标题：二、xxx
        for m in re.finditer(
            r"(?:^|\n)\s*([一二三四五六七八九十]{1,3}|\d{1,2})[、\.．:：]\s*([^\n]{1,80})",
            text[start_idx + 1:]
        ):
            title = m.group(2)
            if any(kw in title for kw in title_keywords):
                candidates.append(start_idx + 1 + m.start())
                break

        # B) 括号小标题：（三）xxx / (3)xxx
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

    def _slice_to_next_major_heading(self, text, start_idx):
        """
        从 start_idx 切到“下一条重大一级标题”（标题行包含 MAJOR_HEADING_KEYWORDS）。
        目的：避免把同一段里的城市/地区小标题（三、深圳…）当作章节结束。
        若找不到重大标题，则返回 None 让调用方自行兜底。
        """
        if start_idx is None or start_idx < 0:
            return None

        # 形如：三、核心竞争力分析 / 四、主营业务分析 / 十一、公司未来发展的展望
        heading_iter = re.finditer(
            r"(?:\n\s*([一二三四五六七八九十]{1,3}|\d{1,2})、([^\n]{1,60}))",
            text[start_idx + 1:]
        )

        for m in heading_iter:
            title = m.group(2)
            if any(kw in title for kw in self.MAJOR_HEADING_KEYWORDS):
                end_idx = start_idx + 1 + m.start()
                return text[start_idx:end_idx].strip()

        return None

    def _next_ordinal_candidates(self, current_ordinal: str):
        """
        给定当前一级序号（中文或阿拉伯），返回可能的“下一个一级序号”候选列表。
        例如：'二' -> ['三', '3']；'11' 或 '十一' -> ['十二', '12']。
        """
        cn_list = ["一","二","三","四","五","六","七","八","九","十",
                   "十一","十二","十三","十四","十五","十六","十七","十八","十九","二十"]
        cn_to_ar = {"一":"1","二":"2","三":"3","四":"4","五":"5","六":"6","七":"7","八":"8","九":"9","十":"10",
                    "十一":"11","十二":"12","十三":"13","十四":"14","十五":"15","十六":"16","十七":"17","十八":"18","十九":"19","二十":"20"}
        ar_to_cn = {v:k for k,v in cn_to_ar.items()}

        cur_cn = current_ordinal
        cur_ar = None

        # 标准化：如果输入是阿拉伯数字，转成中文索引
        if re.fullmatch(r"\d{1,2}", current_ordinal):
            cur_ar = current_ordinal
            cur_cn = ar_to_cn.get(current_ordinal)

        if cur_cn in cn_list:
            idx = cn_list.index(cur_cn)
            if idx + 1 < len(cn_list):
                nxt_cn = cn_list[idx + 1]
                nxt_ar = cn_to_ar.get(nxt_cn)
                return [nxt_cn, nxt_ar] if nxt_ar else [nxt_cn]

        # 找不到就不给候选
        return []

    def _slice_to_next_ordinal(self, text, start_idx, current_ordinal: str):
        """
        从 start_idx 开始切片到“下一一级序号”（例如 二、 -> 三、；十一、 -> 十二、）。
        这样不会被正文中的“一、/1、”等项目符号截断。
        """
        if start_idx is None or start_idx < 0:
            return None

        # 先尝试按“重大标题”结束，避免被城市/地区小标题截断
        major_slice = self._slice_to_next_major_heading(text, start_idx)
        if major_slice:
            return major_slice

        candidates = self._next_ordinal_candidates(current_ordinal)

        end_positions = []
        for cand in candidates:
            if not cand:
                continue
            m = re.search(rf"(?:\n\s*{re.escape(cand)}、)", text[start_idx + 1:])
            if m:
                end_positions.append(start_idx + 1 + m.start())

        end_idx = min(end_positions) if end_positions else len(text)
        return text[start_idx:end_idx].strip()

    def extract_section_by_ordinal(self, text, ordinal_cn, keyword_fallback=None):
        """
        按“同级序号”提取段落：
        - 起始优先匹配：二、 / 2、 （一级）
        - 仅当找不到一级时，才尝试： （二）/(二) 这类括号形式（有些报告一级也用括号）
        - 结束仅识别同级：一、二、三… / 1、2、3…（不把（ 一 ）当作结束）
        """
        cn_to_arabic = {"一": "1", "二": "2", "三": "3", "四": "4", "五": "5",
                        "六": "6", "七": "7", "八": "8", "九": "9", "十": "10",
                        "十一": "11", "十二": "12"}

        start_idx = None

        # 1) 一级：中文序号 + 顿号（最可靠，避免误命中（二）子标题）
        m1 = re.search(rf"(?:^|\n)\s*{ordinal_cn}、", text)
        if m1:
            start_idx = m1.start()
        else:
            # 2) 一级：阿拉伯数字 + 顿号
            arabic = cn_to_arabic.get(ordinal_cn)
            if arabic:
                m2 = re.search(rf"(?:^|\n)\s*{arabic}、", text)
                if m2:
                    start_idx = m2.start()

        # 3) 仍找不到：才尝试括号形式（有些报告一级标题可能写成（十一））
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

        # 4) 关键词兜底
        if start_idx is None and keyword_fallback:
            for kw in keyword_fallback:
                mk = re.search(rf"(?:^|\n)\s*{kw}", text)
                if mk:
                    start_idx = mk.start()
                    break

        return self._slice_to_next_ordinal(text, start_idx, ordinal_cn)

    def extract_section_by_keywords(self, text, keywords, fallback_ordinals=None, end_title_keywords=None):
        """
        通过“一级标题行（序号 + 顿号）+ 关键词”定位并提取整段，并按该序号切到下一序号。
        关键：结束边界是“下一一级序号”，避免被正文里的项目符号截断。
        """
        for kw in keywords:
            m = re.search(
                rf"(?:^|\n)\s*([一二三四五六七八九十]{{1,3}}|\d{{1,2}})[、\.．:：]\s*[^\n]*{re.escape(kw)}[^\n]*",
                text
            )
            if m:
                start = m.start()
                # 若提供 end_title_keywords：只在遇到“标题包含关键字”的一级标题行时才结束；
                # 若找不到这样的标题，则直接取到第三节末尾，避免被内部“二、/三、”条目误截断。
                if end_title_keywords:
                    sliced = self._slice_to_next_heading_with_title_keywords(text, start, end_title_keywords)
                    if sliced:
                        return sliced
                    return text[start:].strip()

                # 未提供 end_title_keywords 时，才使用序号规则（如 十一 -> 十二）
                ordinal = m.group(1)
                return self._slice_to_next_ordinal(text, start, ordinal)

        # 兼容括号小标题：如（三）所处行业情况 / (一) 主要业务 / （三）行业情况说明
        for kw in keywords:
            m = re.search(
                rf"(?:^|\n)\s*[（(]([一二三四五六七八九十0-9]{{1,3}})[）)]\s*[^\n]*{re.escape(kw)}[^\n]*",
                text
            )
            if m:
                start = m.start()
                sliced = self._slice_to_next_bracket_heading(text, start)
                if sliced:
                    return sliced
                return text[start:].strip()

        # 兜底：按候选序号（注意这会依赖报告结构，优先级放后面）
        if fallback_ordinals:
            for o in fallback_ordinals:
                sec = self.extract_section_by_ordinal(text, o)
                if sec:
                    return sec
        return None

    def extract_section(self, text, title):
        pattern = rf"{title}[\s\S]*?(?=\n[一二三四五六七八九十]+、|\Z)"
        match = re.search(pattern, text)
        return match.group(0).strip() if match else None


# ===============================
# 主逻辑
# ===============================
def main():
    download_dir = str(DOWNLOADS_DIR)
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)

    db = MySQLClient()
    parser = AnnualReportParser()

    # 最近N天窗口（默认30天）；可在 config.ini 里配置：
    # [crawler]
    # days_back = 30
    cfg = configparser.ConfigParser()
    cfg.read(CONF_DIR / "config.ini", encoding="utf-8")
    days_back = 30
    try:
        if cfg.has_section("crawler") and cfg.get("crawler", "days_back", fallback=""):
            days_back = int(cfg.get("crawler", "days_back"))
    except Exception:
        days_back = 30

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "X-Requested-With": "XMLHttpRequest"
    })

    POST_TIMEOUT = (5, 20)   # (connect, read)
    GET_TIMEOUT = (5, 60)    # pdf download can be slower
    MAX_RETRY = 3

    base_url = "http://www.cninfo.com.cn/new/hisAnnouncement/query"

    end_date = datetime.today()
    start_date = end_date - timedelta(days=days_back)
    start_ts = start_date.timestamp()
    end_ts = end_date.timestamp()

    date_range = f"{start_date.strftime('%Y-%m-%d')}~{end_date.strftime('%Y-%m-%d')}"

    columns = ["szse", "sse"]  # 全市场：深交所 + 上交所

    for col in columns:
        page = 1
        while True:
            plate = "sz" if col == "szse" else "sh"  # 深市/沪市
            print(f"[{col}] 拉取第 {page} 页...")
            data = {
                "pageNum": page,
                "pageSize": 30,
                "column": col,          # szse / sse
                "plate": plate,         # sz / sh
                "tabName": "fulltext",
                "category": "category_ndbg_szsh;",
                "seDate": date_range,
                "isHLtitle": "false",
                "searchkey": "年度报告",
                "secid": ""
            }

            result = None
            for attempt in range(1, MAX_RETRY + 1):
                try:
                    r = session.post(base_url, data=data, timeout=POST_TIMEOUT)
                    r.raise_for_status()
                    result = r.json()
                    break
                except Exception as e:
                    print(f"[{col}] 第 {page} 页请求失败 attempt={attempt}/{MAX_RETRY}: {e}")
                    if attempt == MAX_RETRY:
                        print(f"[{col}] 连续失败，跳过该交易所后续分页。")
                        result = {"announcements": []}
                    else:
                        time.sleep(1.0 * attempt)

            announcements = result.get("announcements") or []
            print(f"[{col}] 第 {page} 页返回 {len(announcements)} 条公告")

            # 若服务端忽略 seDate，分页会无限向历史拉取。我们用 announcementTime 强制截断。
            # 已按 announcementTime desc 排序：当本页最老公告都早于 start_date 时，后续页只会更早，直接停止。
            oldest_ts = None
            newest_ts = None
            for a in announcements:
                t = a.get("announcementTime")
                ts = None
                if isinstance(t, int):
                    ts = t / 1000 if t > 1e12 else t
                elif isinstance(t, str):
                    # could be millis string or date string
                    if t.isdigit():
                        ti = int(t)
                        ts = ti / 1000 if ti > 1e12 else ti
                    else:
                        # try YYYY-MM-DD...
                        try:
                            ts = datetime.strptime(t[:10], "%Y-%m-%d").timestamp()
                        except Exception:
                            ts = None
                if ts is None:
                    continue
                oldest_ts = ts if oldest_ts is None else min(oldest_ts, ts)
                newest_ts = ts if newest_ts is None else max(newest_ts, ts)

            if oldest_ts is not None and oldest_ts < start_ts:
                print(f"[{col}] 已到达时间窗口下界（本页最老 {datetime.fromtimestamp(oldest_ts).strftime('%Y-%m-%d')} < {start_date.strftime('%Y-%m-%d')}），处理完本页后停止分页。")
                stop_after_page = True
            else:
                stop_after_page = False

            # 如果本页最新也早于窗口下界，说明全页都过期，直接停止（无需再处理/翻页）
            if newest_ts is not None and newest_ts < start_ts:
                print(f"[{col}] 本页最新也早于时间窗口（{datetime.fromtimestamp(newest_ts).strftime('%Y-%m-%d')} < {start_date.strftime('%Y-%m-%d')}），立即停止分页。")
                break

            if page >= 50:
                print(f"[{col}] 已达到最大页数上限 50，停止分页（防止异常无限分页）。")
                break

            if not announcements:
                break

            processed_this_page = 0
            if col == "sse" and page == 1:
                for a in announcements:
                    print("[SSE DEBUG]", a.get("secCode"), a.get("secName"), a.get("announcementTitle"),
                          a.get("announcementTime"))
            for ann in announcements:

                title = ann["announcementTitle"]

                # 过滤：必须是年度报告（排除摘要）
                if "年度报告" not in title:
                    continue
                if "摘要" in title:
                    continue
                # 排除纯公告（例如“关于披露年度报告的公告”）
                if "关于" in title and "年度报告" not in title.split("关于")[0]:
                    continue

                # 解析年份：兼容 “2025年年度报告… / 2025年度报告… / 2025 年度报告…”
                year_match = re.search(r"(20\d{2})\s*年?\s*年度报告", title)
                if year_match:
                    year = int(year_match.group(1))
                else:
                    # 兜底：标题以年份开头（如：2025年度报告全文）
                    m0 = re.match(r"^(20\d{2})", title)
                    if not m0:
                        continue
                    year = int(m0.group(1))

                stock_code = ann["secCode"]
                stock_name = ann["secName"]
                timestamp = ann["announcementTime"]

                if isinstance(timestamp, int):
                    ts = timestamp / 1000 if timestamp > 1e12 else timestamp
                    publish_dt = datetime.fromtimestamp(ts)
                    publish_date = publish_dt.strftime("%Y-%m-%d")
                else:
                    # Fallback (should be rare)
                    publish_date = str(timestamp)[:10]
                    try:
                        publish_dt = datetime.strptime(publish_date, "%Y-%m-%d")
                        ts = publish_dt.timestamp()
                    except Exception:
                        ts = None

                # 强制时间窗口过滤（避免 seDate 失效导致拉到历史数据）
                if ts is not None:
                    if ts < start_ts or ts > end_ts:
                        continue
                else:
                    # 若无法得到时间戳，则按 publish_date 兜底判断；解析失败则直接跳过（避免拉到太老数据）
                    try:
                        pd = datetime.strptime(publish_date, "%Y-%m-%d").timestamp()
                    except Exception:
                        continue
                    if pd < start_ts or pd > end_ts:
                        continue

                # 去重：同一公司同一年只入库一次（跨交易所也适用）
                if db.exists(stock_code, year):
                    continue

                adj_url = ann["adjunctUrl"]
                file_name = adj_url.split("/")[-1]
                file_path = os.path.join(download_dir, file_name)

                if not os.path.exists(file_path):
                    pdf_url = "http://static.cninfo.com.cn/" + adj_url
                    print(f"[{col}] 下载PDF: {stock_code}-{year} {title}")
                    pdf_resp = None
                    for attempt in range(1, MAX_RETRY + 1):
                        try:
                            pdf_resp = session.get(pdf_url, timeout=GET_TIMEOUT)
                            pdf_resp.raise_for_status()
                            break
                        except Exception as e:
                            print(f"[{col}] PDF下载失败 attempt={attempt}/{MAX_RETRY}: {e}")
                            if attempt == MAX_RETRY:
                                pdf_resp = None
                            else:
                                time.sleep(1.0 * attempt)

                    if pdf_resp is None:
                        print(f"[{col}] 放弃下载，跳过: {title}")
                        continue

                    with open(file_path, "wb") as f:
                        f.write(pdf_resp.content)
                    time.sleep(0.8)

                # ✅ 页数判断（防止公告/更正等短PDF）
                try:
                    with pdfplumber.open(file_path) as pdf:
                        page_count = len(pdf.pages)
                except Exception as e:
                    print(f"无法读取PDF页数，跳过: {title} err={e}")
                    continue

                if page_count < 50:
                    print(f"跳过非完整年报（{page_count}页）: {title}")
                    continue

                print(f"[{col}] 解析PDF第三节: {stock_code}-{year}")
                try:
                    mda = parser.extract_mda(file_path)
                except Exception as e:
                    print(f"[{col}] 解析失败: {title} err={e}")
                    traceback.print_exc()
                    continue

                if not mda:
                    print("未找到第三节:", title)
                    continue

                report_id = db.insert_report(
                    stock_code,
                    stock_name,
                    year,
                    publish_date,
                    file_path
                )

                db.insert_mda(report_id, mda)

                processed_this_page += 1
                print(f"完成：{stock_code}-{year} ({col})")

            if processed_this_page == 0 and page >= 3:
                print(f"[{col}] 连续分页未处理到任何有效年报（processed=0），停止分页以防异常无限翻页。")
                break

            if stop_after_page:
                break
            page += 1
            time.sleep(1.5)

    print("增量更新完成")


if __name__ == "__main__":
    main()