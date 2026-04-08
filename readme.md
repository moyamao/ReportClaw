# 功能描述
- 抓取A股年报，截取最关键的管理层综述和未来展望部分，存入数据库。
- 生成pdf，发送到邮箱。
  - 

## 配置文件

- 初始化，设置数据库，邮件网关等
```
cp conf/config.example.ini conf/config.ini

修改： conf/config.ini
```
- 前置资源
  - 本地mysql数据库
  - google sheet的权限文件，和同步的表格
  - 发送邮件的smtp地址和用户


## 常用命令
- 抓取数据，使用代理生成pdf，发邮件，生成google sheet
```
cd /Users/mhy/python/ReportClaw
PYTHONPATH=src ./venv/bin/python -m reportclaw.main

export HTTPS_PROXY=http://127.0.0.1:1092
export HTTP_PROXY=http://127.0.0.1:1092
export https_proxy=http://127.0.0.1:1092
export http_proxy=http://127.0.0.1:1092
cd /Users/mhy/python/ReportClaw
PYTHONPATH=src ./venv/bin/python -m reportclaw.daily_report
```
-  默认抓取最近三十天A股数据，重复抓取需要修改状态文件 
```
./data/state/last_sent.json
```

## 设置每日自动执行
- 创建文件 ~/Library/LaunchAgents/com.mhy.reportclaw.dailyupdate.plist
内容如：com.mhy.reportclaw.dailyupdate.plist
- 然后执行
```
launchctl unload ~/Library/LaunchAgents/com.mhy.reportclaw.dailyupdate.plist 2>/dev/null
launchctl load ~/Library/LaunchAgents/com.mhy.reportclaw.dailyupdate.plist
launchctl start com.mhy.reportclaw.dailyupdate
```

## 常用sql

- 删除某个公司某年的财报
```
- DELETE m
FROM annual_report_mda m
JOIN annual_reports r ON r.id=m.report_id
WHERE r.stock_code='xxxxxxx' AND r.report_year=2025;

DELETE FROM annual_reports
WHERE stock_code='xxxxxx' AND report_year=2025;
```

- 查看某个股票的截取内容
```
SELECT
  r.id AS report_id,
  r.stock_code, r.stock_name, r.report_year, r.publish_date, r.file_path,
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
WHERE r.stock_code='000408' AND r.report_year=2025;
```

