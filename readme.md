# ReportClaw

抓取 A 股年报，提取管理层讨论与分析等关键内容，写入 MySQL，按规则打分，并生成每日汇总 PDF/EPUB、邮件和 Google Sheets 同步。

当前主线以这套流程为准：

- `reportclaw.main`
- `reportclaw.report_scoring`
- `reportclaw.daily_report`
- `reportclaw.sync_industry_info`

`reportBot.py` 目前仍是实验性解析方案，不属于当前生产主流程。

行业独立同步说明见：

- [docs/industry_sync.md](/Users/mhy/python/ReportClaw/docs/industry_sync.md)

## 环境准备

- Python 3.11+
- 本地 MySQL
- 可访问巨潮资讯的网络环境
- 如需 Google Sheets：Service Account 凭据文件和目标表格权限
- 如需邮件：SMTP 账号与授权码
- 如需代理：本地 HTTP/HTTPS 代理

## 安装依赖

主线最小依赖：

```bash
cd /Users/mhy/python/ReportClaw
python3 -m venv venv
./venv/bin/pip install -r requirements-min.txt
```

完整依赖：

- 在主线基础上，额外包含仓库里的辅助脚本依赖
- 例如 `sync_stock_master.py`、`grepTxtInPDF.py`

```bash
cd /Users/mhy/python/ReportClaw
python3 -m venv venv
./venv/bin/pip install -r requirements.txt
```

## 初始化配置

复制配置模板：

```bash
cp conf/config.example.ini conf/config.ini
```

然后修改 `conf/config.ini`，至少需要这些配置：

- `[mysql]`：数据库连接
- `[crawler]`：抓取窗口和增量状态
- `[email]`：邮件发送配置，可先关闭
- `[sheets]`：Google Sheets 同步配置，可先关闭

## 初始化数据库

当前主线使用的表结构以 `conf/db.sql` 和 `conf/ddl.sql` 为准。

如果是首次初始化，至少要创建：

- `annual_reports`
- `annual_report_mda`
- `annual_report_score_hits`
- `stock_master_cn`

可直接把 SQL 导入本地 MySQL，例如：

```bash
mysql -uroot -p stock < conf/db.sql
mysql -uroot -p stock < conf/ddl.sql
```

如果库名不是 `stock`，请按你的实际库名调整，并同步更新 `conf/config.ini`。

## 常用命令

抓取最近窗口内的新年报并入库：

```bash
cd /Users/mhy/python/ReportClaw
PYTHONPATH=src ./venv/bin/python -m reportclaw.main
```

对已入库内容重新打分：

```bash
cd /Users/mhy/python/ReportClaw
PYTHONPATH=src ./venv/bin/python -m reportclaw.report_scoring
```

生成日报 PDF/EPUB，并按配置决定是否发邮件、同步 Google Sheets：

```bash
cd /Users/mhy/python/ReportClaw
PYTHONPATH=src ./venv/bin/python -m reportclaw.daily_report
```

如果需要代理，再运行 `daily_report`：

```bash
export HTTPS_PROXY=http://127.0.0.1:1092
export HTTP_PROXY=http://127.0.0.1:1092
export https_proxy=http://127.0.0.1:1092
export http_proxy=http://127.0.0.1:1092

cd /Users/mhy/python/ReportClaw
PYTHONPATH=src ./venv/bin/python -m reportclaw.daily_report
```

只生成不发邮件：

```bash
cd /Users/mhy/python/ReportClaw
PYTHONPATH=src ./venv/bin/python -m reportclaw.daily_report --no-email
```

按指定披露日生成：

```bash
cd /Users/mhy/python/ReportClaw
PYTHONPATH=src ./venv/bin/python -m reportclaw.daily_report --date 2026-02-28 --no-email
```

只生成单个公司多年的汇总：

```bash
cd /Users/mhy/python/ReportClaw
PYTHONPATH=src ./venv/bin/python -m reportclaw.daily_report --stock-code 300750 --no-email
```

只刷新行业信息，不做财报抓取和解析：

```bash
cd /Users/mhy/python/ReportClaw
PYTHONPATH=src ./venv/bin/python -m reportclaw.sync_industry_info
```

## 增量状态文件

默认增量状态保存在：

```bash
data/state/last_sent.json
```

这个文件里通常会记录：

- `last_crawl_end_iso`
- `last_generated_iso`
- `last_sent_iso`

如果你要重跑某段历史数据，通常需要先确认这个状态文件是否要手动调整。

## 每日自动执行

仓库内已提供：

```bash
bin/daily_update.sh
```

这个脚本会按顺序执行：

1. `reportclaw.main`
2. `reportclaw.report_scoring`
3. `reportclaw.daily_report`

并负责：

- 日志输出
- 锁文件避免重复运行
- 代理环境变量
- macOS 下的唤醒时间设置

如果你用 macOS `launchd`，可以创建：

```bash
~/Library/LaunchAgents/com.mhy.reportclaw.dailyupdate.plist
```

仓库里已经提供了模板文件：

```bash
conf/com.mhy.reportclaw.dailyupdate.plist
```

可以直接复制过去：

```bash
mkdir -p ~/Library/LaunchAgents
cp conf/com.mhy.reportclaw.dailyupdate.plist ~/Library/LaunchAgents/com.mhy.reportclaw.dailyupdate.plist
```

然后执行：

```bash
launchctl unload ~/Library/LaunchAgents/com.mhy.reportclaw.dailyupdate.plist 2>/dev/null
launchctl load ~/Library/LaunchAgents/com.mhy.reportclaw.dailyupdate.plist
launchctl start com.mhy.reportclaw.dailyupdate
```

为了让机器在“睡眠状态”下也能在下次任务前自动唤醒，还需要给 `pmset` 配置免密码 sudo。
否则脚本里的这一步会失败，并出现类似日志：

```text
sudo: a password is required
设置系统唤醒时间失败（可能尚未配置 sudo 免密码执行 pmset）
```

推荐做法是执行 `visudo -f /etc/sudoers.d/reportclaw-pmset`，加入一行：

```text
你的用户名 ALL=(root) NOPASSWD: /usr/bin/pmset
```

例如当前机器用户名如果是 `mhy`，则写成：

```text
mhy ALL=(root) NOPASSWD: /usr/bin/pmset
```

配置完成后，可以手动验证：

```bash
sudo -n /usr/bin/pmset schedule wakeorpoweron "04/16/26 07:55:00"
pmset -g sched
```

说明：

- 这套方案适用于“机器已登录，只是进入睡眠”的场景。
- 如果机器处于关机、重启后未登录用户会话，`LaunchAgent` 不会替你进入图形会话执行脚本。
- `bin/daily_update.sh` 现在会在任务开始前先预设下一次唤醒时间，避免中途失败导致第二次任务无法自动唤醒。

## 常用 SQL

删除某个公司某年的财报：

```sql
DELETE m
FROM annual_report_mda m
JOIN annual_reports r ON r.id = m.report_id
WHERE r.stock_code = '000001' AND r.report_year = 2025;

DELETE FROM annual_reports
WHERE stock_code = '000001' AND report_year = 2025;
```

查看某个股票的截取内容：

```sql
SELECT
  r.id AS report_id,
  r.stock_code,
  r.stock_name,
  r.report_year,
  r.publish_date,
  r.file_path,
  LENGTH(m.industry_section) AS l_industry,
  LENGTH(m.main_business_section) AS l_business,
  LENGTH(m.future_section) AS l_future,
  LENGTH(m.full_mda) AS l_full,
  LEFT(m.industry_section, 200) AS industry_head,
  LEFT(m.main_business_section, 200) AS business_head,
  LEFT(m.future_section, 200) AS future_head,
  LEFT(m.full_mda, 200) AS full_head
FROM annual_reports r
JOIN annual_report_mda m ON m.report_id = r.id
WHERE r.stock_code = '000408' AND r.report_year = 2025;
```
