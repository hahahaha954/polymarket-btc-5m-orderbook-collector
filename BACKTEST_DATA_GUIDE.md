# 回测数据说明

## 1. 文档目的

本文档说明当前项目实际采集、保存和结算的数据，以及如何把这些数据整理成 Polymarket BTC 5 分钟涨跌市场的回测输入。

当前项目是一个 CLOB 盘口采集器，不是逐笔成交采集器。它订阅相邻的 BTC 5 分钟市场，维护 UP 和 DOWN 两个 token 的本地订单簿，并在 best bid/ask 发生变化时保存盘口快照。

因此，当前数据适合用于：

- 盘口价格和价差研究
- 不同深度下的理论成交模拟
- 基于盘口变化的信号回测
- 结算方向与盘口状态的关系分析

当前数据不能直接证明：

- 某一笔订单确实成交
- 某个价位的全部展示数量都可以被策略吃到
- 订单在盘口中的排队位置
- 主动买入或主动卖出的真实成交量

## 2. 数据生命周期与文件结构

采集流程如下：

```text
CLOB WebSocket
    -> 本地订单簿
    -> live/*.jsonl
    -> 每小时转为 hourly/*.parquet
    -> 每日打包为 daily/*.zip
```

目录和文件含义：

| 位置 | 文件 | 用途 |
| --- | --- | --- |
| `data/live/` | `ticks_YYYYMMDD_HH.jsonl` | best quote 快照的实时缓冲 |
| `data/live/` | `depth_YYYYMMDD_HH.jsonl` | 20 档深度长表的实时缓冲 |
| `data/live/` | `market_outcomes.jsonl` | 已发现市场的最终结算结果缓冲 |
| `data/live/` | `seen_markets.jsonl` | 市场与 token 的关联信息 |
| `data/hourly/` | `ticks_YYYYMMDD_HH.parquet` | 每小时 best quote 数据 |
| `data/hourly/` | `depth_YYYYMMDD_HH.parquet` | 每小时深度长表数据 |
| `data/hourly/` | `btc_5m_market_outcomes.parquet` | 市场结算结果表 |
| `data/daily/` | `btc_5m_YYYYMMDD.zip` | 当日完整归档包 |
| `data/daily/` | `btc_5m_YYYYMMDD_part1of2.zip`、`part2of2.zip` | 日包超过 Telegram 限制时的发送分包 |

日包通常包含当天的 ticks、depth 和 market outcomes Parquet。分包只是传输层拆分，两个 part 合起来才是完整日包；原始完整 zip 也会保留在本地。

当前深度采集配置为：

```env
DEPTH_ENABLED=true
DEPTH_LEVELS=20
```

如果关闭 `DEPTH_ENABLED`，后续只会有 ticks，不会生成 depth 文件。`DEPTH_LEVELS` 决定每个方向保存的最大档数。

## 3. 市场定义与关联键

### 3.1 市场时间窗口

每个市场是 5 分钟窗口：

- `window_start_ts`：窗口开始时间，Unix epoch 秒
- `window_end_ts`：窗口结束时间，Unix epoch 秒
- `window_end_ts - window_start_ts` 应为 300 秒
- `expires`：以 UTC ISO 8601 字符串保存的结束时间
- `offset_s`：`ts - window_start_ts`

采集器按当前时间向下取整到 5 分钟，并尝试发现当前场和下一场市场。市场元数据从 Gamma API 获取，盘口从 CLOB WebSocket 获取。

市场的主要关联键是 `event_slug`，例如：

```text
btc-updown-5m-1782930000
```

回测时应始终使用文件中的 `event_slug` 关联数据，不要仅根据日期或行号推断市场。

### 3.2 token 与 outcome 的对应关系

一个市场通常有两个 token：

- `UP`：上涨或 YES 方向
- `DOWN`：下跌或 NO 方向

token 的真实 asset ID 保存在 `seen_markets.jsonl` 的市场上下文中。ticks 和 depth 表为了便于分析，使用 `UP`、`DOWN` 标签，但没有直接保存 `asset_id`。

`seen_markets.jsonl` 的字段如下：

| 字段 | 含义 |
| --- | --- |
| `event_slug` | 市场唯一标识 |
| `market_id` | Gamma 市场 ID |
| `condition_id` | 市场条件 ID |
| `up_asset_id` | UP token 的 CLOB asset ID |
| `down_asset_id` | DOWN token 的 CLOB asset ID |
| `asset_ids` | `[up_asset_id, down_asset_id]` |
| `window_start_ts` | 市场窗口开始时间 |
| `window_end_ts` | 市场窗口结束时间 |
| `expires` | 市场结束时间，UTC ISO 8601 |

如果回测需要核对原始 token，使用以下文件关联：

```text
event_slug
    -> seen_markets.jsonl
    -> up_asset_id / down_asset_id
```

## 4. ticks 表

### 4.1 字段定义

`ticks` 对应代码中的 `TICK_SCHEMA`，每行是一个市场在某个时间点的 best quote 快照。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `ts` | float64 | 快照时间，Unix epoch 秒，保留小数 |
| `ts_sec` | int64 | `ts` 向下取整后的秒级时间 |
| `datetime` | timestamp(ms) | 快照时间，毫秒精度；JSONL 阶段是 epoch 毫秒数 |
| `event_slug` | string | 市场唯一标识 |
| `expires` | string | 市场结束时间，UTC ISO 8601 |
| `poly_up_bid` | float64 | UP token 当前最高买价 |
| `poly_up_ask` | float64 | UP token 当前最低卖价 |
| `poly_down_bid` | float64 | DOWN token 当前最高买价 |
| `poly_down_ask` | float64 | DOWN token 当前最低卖价 |
| `interval_min` | int32 | 市场周期，当前固定为 5 |
| `window_start_ts` | int64 | 市场窗口开始时间，Unix epoch 秒 |
| `window_end_ts` | int64 | 市场窗口结束时间，Unix epoch 秒 |
| `offset_s` | float64 | 快照相对市场开始时间的偏移秒数 |
| `up_mid` | float64 | `(poly_up_bid + poly_up_ask) / 2` |
| `down_mid` | float64 | `(poly_down_bid + poly_down_ask) / 2` |
| `up_spread` | float64 | `poly_up_ask - poly_up_bid` |
| `down_spread` | float64 | `poly_down_ask - poly_down_bid` |

价格是 Polymarket token 价格，不是 BTC 美元价格。对于二元市场，通常可以把它理解为 0 到 1 之间的合约价格，但回测仍应以实际文件中的值为准。

### 4.2 ticks 的采样规则

采集器只有在以下条件都满足时才写入一行：

1. `offset_s` 在 `[-300, 300]` 范围内。
2. UP 和 DOWN 的 bid/ask 四个 best quote 都不为空。
3. 当前四个 best quote 与该市场上一行相比至少有一个发生变化。

因此，`ticks` 具有以下特征：

- 不是固定 1 秒、100 毫秒或其他固定间隔采样。
- 没有 best quote 变化时，不会产生新行。
- 深层价位发生变化但 best bid/ask 没变时，不会产生 ticks 行。
- 连接中断、市场未发现或某一边没有完整报价时，可能出现空档。
- 采集时间由本机 `time.time()` 记录，当前没有保存交易所事件时间、消息序号或网络延迟。

### 4.3 回测使用建议

ticks 可以用来计算：

- 中间价变化
- bid-ask spread
- UP 和 DOWN 的价格差
- best quote 出现后的理论可成交价
- 市场结束前不同时间段的信号

不能把相邻两行之间的价格变化解释为一笔成交，也不能把没有记录的时间段自动当成价格不变。是否前向填充，必须由回测规则明确规定；建议先区分“没有盘口变化”和“没有数据”。

## 5. depth 表

### 5.1 字段定义

`depth` 是长表，每个盘口档位单独一行，对应代码中的 `DEPTH_SCHEMA`。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `ts` | float64 | 深度快照时间，Unix epoch 秒 |
| `ts_sec` | int64 | `ts` 向下取整后的秒级时间 |
| `datetime` | timestamp(ms) | 深度快照时间，毫秒精度 |
| `event_slug` | string | 市场唯一标识 |
| `expires` | string | 市场结束时间，UTC ISO 8601 |
| `interval_min` | int32 | 市场周期，当前固定为 5 |
| `window_start_ts` | int64 | 市场窗口开始时间 |
| `window_end_ts` | int64 | 市场窗口结束时间 |
| `offset_s` | float64 | 快照相对市场开始时间的偏移秒数 |
| `outcome` | string | token 方向，`UP` 或 `DOWN` |
| `side` | string | 订单方向，`BID` 或 `ASK` |
| `level` | int32 | 盘口档位，从 1 开始 |
| `price` | float64 | 该档价格 |
| `size` | float64 | 该价格档位当时展示的可用股份数量 |

### 5.2 排序与含义

对于同一个 `(event_slug, ts, outcome)`：

- `BID` 的 `level=1` 是最高买价，后续档位价格从高到低。
- `ASK` 的 `level=1` 是最低卖价，后续档位价格从低到高。
- 默认最多保存 20 档。
- 每个完整快照最多有 `2 个 outcome × 2 个 side × 20 档 = 80 行`。
- 如果订单簿实际不足 20 档，实际行数会少于 80 行。

`size` 是本机订单簿在该时刻看到的展示数量，单位是 token 股份。它不是成交数量，也不是对本策略的保证成交数量。真实成交还会受到排队位置、其他交易者抢先成交、网络延迟、订单最小数量和市场状态影响。

### 5.3 depth 的采样规则

depth 与 ticks 使用同一个触发点：只有 best quote 发生变化并且四个 best quote 完整时，采集器才把当时订单簿中的前 20 档写入 depth。

因此：

- depth 不是每个 WebSocket 消息一份完整快照。
- 深层价位变化但 best quote 不变时，当前版本不会保存这一变化。
- 同一个快照的公共字段相同，可用 `(event_slug, ts)` 聚合成一个订单簿快照。
- 回测中必须按 `event_slug` 和 `ts` 同时分组，不能只按 `ts` 分组。

## 6. market outcomes 表

### 6.1 字段定义

`market_outcomes` 对应代码中的 `OUTCOME_SCHEMA`：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `event_slug` | string | 市场唯一标识 |
| `window_start_ts` | int64 | 市场窗口开始时间 |
| `window_end_ts` | int64 | 市场窗口结束时间 |
| `winner_side` | string | 最终获胜方向，`UP` 或 `DOWN` |
| `target` | int8 | `UP -> 1`，`DOWN -> 0` |

每个 `event_slug` 最终只保留一行。若结果还未结算、API 暂时没有明确价格或数据缺失，则该市场可能暂时没有 outcome 行。

### 6.2 结算时序

采集器在 `window_end_ts + 300` 秒之后开始查询市场结果，并要求 Gamma API 返回某个 outcome 的价格至少达到 `0.99`，才记录为明确结算结果。

这意味着：

- `winner_side` 是事后标签，不能在交易决策时使用。
- 结果表的写入时间晚于市场结束时间，但表中没有单独保存“结果被发现的时间”。
- 只要用 outcome 作为标签，不把它作为当时的特征，就不会产生这类未来数据泄漏。

## 7. 推荐的回测数据整理流程

### 第一步：读取并统一文件

优先读取 `data/hourly/` 中的 Parquet；日包需要先解压。不要把 `ticks`、`depth` 和 `market_outcomes` 混在同一个无模式约束的数据集中读取，因为三者字段集合不同。

读取后建议统一为 UTC，并保留原始时间字段：

- 主排序时间使用 `ts` 或 `datetime`。
- 市场边界使用 `window_start_ts` 和 `window_end_ts`。
- 不要仅使用本地文件名中的小时判断市场时间。

### 第二步：关联市场上下文

以 `event_slug` 为键关联：

```text
ticks.event_slug
depth.event_slug
market_outcomes.event_slug
seen_markets.event_slug
```

若需要 token 级别分析，再从 `seen_markets.jsonl` 补充：

- `market_id`
- `condition_id`
- `up_asset_id`
- `down_asset_id`

### 第三步：确定回测时间边界

对于只研究市场内交易的策略，建议使用：

```text
window_start_ts <= ts < window_end_ts
```

采集器实际允许 `offset_s` 到 `+300`，所以文件中可能存在市场结束后的数据。是否使用结束后的数据，必须单独定义，不能默认把它当作市场内可交易数据。

市场开始前的 `-300` 到 `0` 区间是开盘前盘口信息。如果策略允许开盘前观察或交易，可以保留；如果只研究窗口内交易，则从 `window_start_ts` 开始。

### 第四步：按市场和快照排序

建议的分组和排序键：

```text
event_slug, ts
```

depth 的快照键建议使用：

```text
event_slug, ts, outcome, side, level
```

同一快照中：

- 买入 UP 或 DOWN 使用对应 outcome 的 `ASK`。
- 卖出 UP 或 DOWN 使用对应 outcome 的 `BID`。
- 从 `level=1` 向外逐档消耗 `size`。

### 第五步：关联最终标签

在市场结束后，将 `market_outcomes` 按 `event_slug` 关联到该市场的所有历史快照。回测特征只能使用快照时间点及之前的数据，`winner_side` 和 `target` 只用于最终收益计算或监督学习标签。

## 8. 深度成交模拟建议

### 8.1 基础的理论成交

假设策略在时间 `t` 想买入某个 outcome 的 `Q` 股：

1. 从该时刻的 `ASK level=1` 开始。
2. 使用该档 `size` 与剩余数量的较小值作为理论成交量。
3. 数量不足时继续消耗 `level=2`、`level=3`，直到完成 `Q` 或深度耗尽。
4. 计算加权平均成交价：

```text
average_price = sum(fill_size * price) / sum(fill_size)
```

卖出时使用相同规则，但从 `BID level=1` 开始向更低价格档位消耗。

### 8.2 必须显式设置的假设

当前数据不足以决定真实成交，因此回测至少应明确以下参数：

- 决策延迟：看到快照后是否立即成交，还是等待下一条快照。
- 排队折扣：展示 `size` 的多少比例可以被本策略成交。
- 最大可成交量：是否允许一次吃完多档盘口。
- 深度不足：未达到目标数量时，是部分成交还是整笔订单取消。
- 手续费：按照实际交易时段和账户规则配置，不要默认手续费为零。
- 价格滑点：是否在盘口价格之外额外增加保守滑点。
- 订单有效期：订单是立即成交，否则取消，还是可以跨多个快照继续等待。

建议至少做三组情景：

```text
乐观：展示 size 的 100% 可成交，使用当前快照价格
基准：展示 size 的 25% 到 50% 可成交，使用下一条快照或固定延迟价格
保守：只有 level=1 可成交，并增加额外滑点
```

如果策略结果只在乐观情景下盈利，不能认为它已经具备可执行性。

### 8.3 不要使用的错误做法

- 用 `mid` 作为实际买入价和卖出价。
- 买入时使用 `BID`，卖出时使用 `ASK`。
- 把 `size` 当成已经成交的数量。
- 把每一个 depth 行当成一次独立的市场事件。
- 用下一条快照的盘口回填当前时刻的缺失价格。
- 让一笔订单在没有明确规则的情况下无限跨快照成交。

## 9. 数据质量检查清单

### 9.1 文件与模式

- ticks 文件列名与 `TICK_SCHEMA` 一致。
- depth 文件列名与 `DEPTH_SCHEMA` 一致。
- outcomes 文件列名与 `OUTCOME_SCHEMA` 一致。
- 文件日期和 `ts` 对应的 UTC 小时基本一致。
- 同一小时没有同时存在未处理的 JSONL 和已经生成的 Parquet，除非正处在封存过程。

### 9.2 时间与市场边界

- `ts_sec == floor(ts)`。
- `datetime` 与 `ts * 1000` 基本一致。
- `window_end_ts - window_start_ts == 300`。
- `offset_s` 与 `ts - window_start_ts` 基本一致。
- `event_slug`、窗口时间和 `expires` 之间相互匹配。
- 不把 `offset_s >= 0` 之前的开盘前数据和市场内数据混为一类。
- 对 `offset_s >= 300` 的市场结束后数据单独标记或剔除。

### 9.3 盘口结构

- bid 不应高于 ask；若出现，应记录为异常而不是静默修正。
- `price` 和 `size` 应为有限数值，`size` 通常应大于 0。
- `outcome` 只允许 `UP` 或 `DOWN`。
- `side` 只允许 `BID` 或 `ASK`。
- `level` 应从 1 开始，单个快照内不应重复。
- BID 档位价格应从高到低，ASK 档位价格应从低到高。
- 对同一 `(event_slug, ts, outcome, side)`，档位数量可能少于 20，不应强行补齐。

### 9.4 覆盖率与断线

- 检查每个市场是否至少有首个完整快照。
- 检查每个市场最后一条快照距离 `window_end_ts` 的时间。
- 检查相邻快照间隔，极大间隔应标记为数据空档。
- 检查是否存在只采集到 UP 或 DOWN 的市场。
- 预计每天约有 288 个 5 分钟市场，但当前采集器只向前发现两个市场，网络、API 或进程中断都可能造成缺失，因此 288 只能作为覆盖率参考，不能作为硬性补齐条件。
- 只有 outcome 已明确结算的市场才能用于最终收益统计；未结算市场应进入待补数据队列。

## 10. 当前项目尚未采集的数据

如果目标是做更接近真实交易的回测，建议后续增加以下字段或数据源：

### 10.1 逐笔成交

至少需要：

- `event_slug`
- `asset_id`
- 交易所事件时间
- 本机接收时间
- 成交价格
- 成交数量
- 成交方向或主动买卖方向
- 交易 ID、事件 ID 或消息序号

这些数据可以帮助判断盘口数量是否真实消耗，以及某个信号是否真的伴随成交。

### 10.2 原始 WebSocket 事件

建议保存原始 `book` 和 `price_change` 消息，至少增加：

- 原始消息接收时间
- 交易所消息时间，如果消息提供
- 消息类型
- asset ID
- 原始事件序号或唯一 ID
- 重连和订阅状态

当前代码只保存更新后的订单簿快照，无法完全重放一次断线期间发生的变化。

### 10.3 订单簿关联字段

建议在 depth 中直接保存：

- `asset_id`
- `market_id`
- `condition_id`
- `book_version` 或事件序号

这样可以减少依赖 `seen_markets.jsonl`，也能避免未来市场标签发生变化时无法还原 token 关系。

### 10.4 市场交易规则

回测前还应保存对应时段的：

- 最小价格变动单位
- 最小下单数量
- 订单数量精度
- 手续费规则
- 市场是否 active、closed 或暂停
- 结算来源和结算状态变化

手续费和交易规则可能随时间变化，不应使用当前规则无条件回填历史数据。

### 10.5 BTC 标的价格

如果策略根据 BTC 现货、指数或价格变化做决策，还需要单独获取并保存：

- 标的价格
- 数据源名称
- 数据源事件时间
- 本机接收时间
- 价格周期或逐笔成交信息

当前项目只采集 Polymarket token 盘口，不包含 BTC 标的行情。

## 11. 回测结果应如何解释

回测结果至少应同时报告：

- 市场数量和有效快照数量
- 有 outcome 的市场数量
- 缺失或断线市场数量
- 总下单数量、理论成交数量和部分成交数量
- 使用的盘口深度档数
- 决策延迟和成交延迟
- 采用的 size 可成交比例
- 手续费和滑点假设
- UP、DOWN 两个方向分别的结果
- 按市场、按日期和按市场结束前时间段的结果

不要只报告总收益。当前数据的核心不确定性是“展示盘口数量能有多少真正成交”，所以应同时报告不同成交情景下的结果范围。

## 12. 最小可用回测输入

如果只做第一版盘口信号回测，最低需要：

1. `ticks_*.parquet`
2. `depth_*.parquet`
3. `btc_5m_market_outcomes.parquet`
4. `seen_markets.jsonl`

推荐的最小关联键：

```text
event_slug + ts
```

推荐的成交模拟字段：

```text
event_slug, ts, outcome, side, level, price, size
```

推荐的最终标签字段：

```text
event_slug, winner_side, target
```

如果只使用 ticks，不使用 depth，只能做 best quote 和中间价层面的回测，不能合理估算大于顶档数量的理论成交、盘口冲击和分档滑点。
