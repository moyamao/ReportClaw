# 配置文件
```
cp conf/config.example.ini conf/config.ini
```
- edit conf/config.ini


# 常用命令
-  使用代理生成pdf，发邮件，生成google sheet
export HTTPS_PROXY=http://127.0.0.1:1092
export HTTP_PROXY=http://127.0.0.1:1092
export https_proxy=http://127.0.0.1:1092
export http_proxy=http://127.0.0.1:1092
cd /Users/mhy/python/ReportClaw
PYTHONPATH=src ./venv/bin/python -m reportclaw.daily_report

   
# 常用sql

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