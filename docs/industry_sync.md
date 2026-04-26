# 行业同步说明

## 作用

项目现在支持独立的“行业信息刷新”流程，作用是基于数据库中已有股票列表，单独更新行业信息，而不执行任何财报抓取、PDF 下载、文本解析、摘要生成或评分任务。

## 股票池来源

行业同步的股票池来自两张表的并集：

- `stock_master_cn`
- `annual_reports`

## 数据源

支持以下行业数据源：

- `jqdata`
- `tushare`
- `auto`

其中 `auto` 模式按下面顺序依次尝试：

1. `jqdata`
2. `tushare`

说明：

- 这个独立同步脚本不会优先命中本地行业缓存后直接返回
- 它的目标是“刷新远端行业信息”，而不是简单复用旧值

## 更新范围

同步结果会同时回写到两张表：

- `stock_master_cn`
- `annual_reports`

其中：

- `stock_master_cn` 用于保存股票主数据和当前行业缓存
- `annual_reports` 会按 `stock_code` 批量更新该股票的所有历史年报记录的行业字段

## stock_master_cn 当前支持字段

行业同步完成后，`stock_master_cn` 支持保存完整申万层级：

- `industry`
- `sw_l1`
- `sw_l1_code`
- `sw_l2`
- `sw_l2_code`
- `sw_l3`
- `sw_l3_code`
- `industry_source`
- `industry_lookup_date`

说明：

- `industry` 默认等于 `sw_l1`
- `industry_source` 用于标记行业来源，例如 `joinquant`、`tushare`
- `industry_lookup_date` 记录本次行业查询实际使用的日期

## 脚本入口

脚本位置：

[sync_industry_info.py](/Users/mhy/python/ReportClaw/src/reportclaw/sync_industry_info.py)

推荐运行方式：

```bash
cd /Users/mhy/python/ReportClaw
PYTHONPATH=src ./venv/bin/python -m reportclaw.sync_industry_info
```

## 常用用法

默认自动模式：

```bash
cd /Users/mhy/python/ReportClaw
PYTHONPATH=src ./venv/bin/python -m reportclaw.sync_industry_info
```

只使用聚宽：

```bash
cd /Users/mhy/python/ReportClaw
PYTHONPATH=src ./venv/bin/python -m reportclaw.sync_industry_info --provider jqdata
```

只使用 Tushare：

```bash
cd /Users/mhy/python/ReportClaw
PYTHONPATH=src ./venv/bin/python -m reportclaw.sync_industry_info --provider tushare
```

只更新指定股票：

```bash
cd /Users/mhy/python/ReportClaw
PYTHONPATH=src ./venv/bin/python -m reportclaw.sync_industry_info --stock-code 300750 002594 688981
```

调试时只处理前 N 条：

```bash
cd /Users/mhy/python/ReportClaw
PYTHONPATH=src ./venv/bin/python -m reportclaw.sync_industry_info --limit 50
```

只看结果不写库：

```bash
cd /Users/mhy/python/ReportClaw
PYTHONPATH=src ./venv/bin/python -m reportclaw.sync_industry_info --dry-run
```

## 聚宽权限日期回退

如果聚宽账号的行业权限存在日期范围限制，脚本会自动处理：

- 默认使用“今天”作为查询日期
- 如果超出聚宽权限范围，会自动回退到聚宽允许的截止日期

日志示例：

```text
[jqdata] fallback date used: 000001 requested=2026-04-20 actual=2026-01-17
```

这表示：

- 请求日期原本是 `2026-04-20`
- 但由于账号权限限制，实际使用了 `2026-01-17`

## Tushare 缓存机制

为避免 `tushare.stock_basic` 接口频率限制过严，项目已经增加本地磁盘缓存。

缓存文件位置：

[tushare_stock_basic_industry.json](/Users/mhy/python/ReportClaw/data/cache/tushare_stock_basic_industry.json)

行为如下：

1. 首次成功拉取 `stock_basic` 后，写入本地缓存
2. 后续运行优先读取本地缓存
3. 这样即使 Tushare 限频，也能继续复用已成功拉取过的行业映射

## 运行结果说明

脚本运行结束后会输出统计信息，例如：

```text
[industry_sync] done touched=2032 miss=2 stock_master_updates=2032 annual_report_updates=2160
```

字段含义：

- `touched=2032`：成功拿到行业并完成更新的股票数
- `miss=2`：未获取到行业信息的股票数
- `stock_master_updates=2032`：更新了 `stock_master_cn` 的记录数
- `annual_report_updates=2160`：更新了 `annual_reports` 的记录数

如果存在未命中的股票，结尾还会输出汇总，例如：

```text
[industry_sync] missed stocks: 688816(易思维), 123456(某公司)
```

## 当前效果示例

同步完成后，`stock_master_cn` 中的行业字段会类似这样：

```text
sw_l1 = 银行I
sw_l2 = 股份制银行II
sw_l3 = 股份制银行III
industry_source = joinquant
industry_lookup_date = 2026-01-17
```

这表示本地主表已经支持保存完整三级申万行业，并记录来源与实际查询日期。
