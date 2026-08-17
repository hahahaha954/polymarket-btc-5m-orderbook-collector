# Polymarket BTC 5m Order-Book Collector

这是 Polymarket BTC 5 分钟 Up/Down 市场的 CLOB 盘口采集器核心代码。项目订阅下一场和下下场市场，在开盘前约 5 分钟开始采集，并把盘口变化写入 JSONL、Parquet 和每日 ZIP 归档。



## 公开数据与文档

- 网站：[hahahaha.online](https://hahahaha.online/)
- 完整历史数据：通过 Telegram 频道 [@polymarketbookdata](https://t.me/polymarketbookdata) 免费获取，无需注册或付费
- 数据覆盖、字段定义和回测限制：[BACKTEST_DATA_GUIDE.md](BACKTEST_DATA_GUIDE.md)

网站只托管少量真实示例文件；本仓库不包含完整历史数据归档。

## 当前范围

当前公开的采集器实现固定面向 BTC 5 分钟市场：

- 通过 Gamma API 发现相邻的 BTC 5m 市场；
- 通过 CLOB WebSocket 订阅 UP / DOWN token；
- 记录 best bid、best ask、mid、spread 和最多 20 档深度；
- 查询已结束市场的结算结果；
- 生成小时 Parquet、每日 ZIP，并可通过 Telegram Bot API 发布；
- 支持待发送队列、失败重试、日包分包和测试模式。

网站中的 15 分钟历史数据是独立归档，目前不代表本仓库的采集器已经支持 15 分钟实时采集。

## 数据格式

回测所需字段、关联方式、成交模拟规则和数据限制见 [BACKTEST_DATA_GUIDE.md](BACKTEST_DATA_GUIDE.md)。

`ticks` 保存 best bid/ask/mid/spread。字段与样例 `btc_5m` 保持一致：

```text
ts, ts_sec, datetime, event_slug, expires,
poly_up_bid, poly_up_ask, poly_down_bid, poly_down_ask,
interval_min, window_start_ts, window_end_ts, offset_s,
up_mid, down_mid, up_spread, down_spread
```

`depth` 默认保存前 20 档盘口深度，使用长表格式；每次有效 best quote 变化时，最多输出 `UP/DOWN * BID/ASK * 20 = 80` 行：

```text
ts, ts_sec, datetime, event_slug, expires,
interval_min, window_start_ts, window_end_ts, offset_s,
outcome, side, level, price, size
```

`outcome` 为 `UP` 或 `DOWN`，`side` 为 `BID` 或 `ASK`。`BID` 按价格从高到低排序，`ASK` 按价格从低到高排序。可用 `DEPTH_ENABLED` 和 `DEPTH_LEVELS` 控制是否采集深度及档数。

`market_outcomes` 使用 `UP -> 1`、`DOWN -> 0` 编码。

## 本地运行

需要 Python 3.11+。建议使用虚拟环境：

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp .env.example .env.collector
```

采集器默认只读取 `.env.collector`，避免误读其他本地配置。首次运行建议保持 `TELEGRAM_DRY_RUN=true`，确认文件生成和归档逻辑后，再配置 Telegram Bot。

```bash
python -m collector.main
```

`DAILY_SEND_TIME` 和每日 ZIP 日期都按 UTC 解释。

## 测试

```bash
pytest -q
```

## Telegram

默认使用官方 Telegram Bot API。发送失败会保留待发送任务，并按 `TELEGRAM_RETRY_INTERVAL_SEC` 持续重试。每日定时发送的是前一日 UTC 数据包；如果 ticks 对应的 outcomes 还没全部结算，会延后打包并持续检查，补齐后再发送。日包超过 `TELEGRAM_MAX_FILE_SIZE_MB` 时会自动拆成两份发送，默认阈值为 45 MiB；两份都发送成功后才会清理小时文件。

```env
TELEGRAM_BOT_API_BASE=https://api.telegram.org
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHANNEL_ID=
```

`TEST_MODE=true` 时，第一场采集完成后会立刻提取该场 ticks，转成 Parquet，打 zip 并发送到 Telegram，用来提前测试完整链路。

## 目录结构

```text
data/live/    高频 JSONL buffer
data/hourly/  每小时 Parquet
data/daily/   每天 zip 总包
```


## 重要限制

- 这是订单簿快照采集器，不是逐笔成交采集器；
- `size` 是当时展示的 token 数量，不代表真实成交量或成交保证；
- 数据不包含排队位置、真实主动买卖方向和网络延迟；
- `winner_side` / `target` 只能用于事后结算，不能作为交易时点特征；
- 回测必须显式设置延迟、成交比例、手续费、滑点和深度不足处理方式。
