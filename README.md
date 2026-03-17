# LOB 限价订单簿重建系统

基于沪深两市 Level2 逐笔委托 / 逐笔成交数据，完整还原限价订单簿并输出 50 ms 重采样快照，含 OFI 因子与异常监控。

> 参考文献：《沪深逐笔数据订单簿重构指南》（内部 PDF）、海通证券选股因子系列研究 #75

---

## 目录

1. [核心概念与字段](#核心概念与字段)
2. [深交所（SZSE）数据规范](#深交所szse)
3. [上交所（SSE）数据规范](#上交所sse)
4. [边界情况与细节处理](#边界情况与细节处理)
5. [输出格式](#输出格式)
6. [快速开始](#快速开始)
7. [架构说明](#架构说明)

---

## 核心概念与字段

| 字段 | 别名 | 含义 |
|---|---|---|
| ApplSeqNum / BizIndex | seqColumn | 消息序号，同 Channel 内从 1 连续递增，是确定先后顺序的唯一依据 |
| ChannelNo / Channel | channel | 通道编号，同一通道内 ApplSeqNum 才连续有序 |
| OrderNo | buyOrderColumn / sellOrderColumn | 原始委托单号（上交所成交用此字段关联委托） |
| Side | — | 1 = 买，2 = 卖 |
| Price / Qty | — | 价格（解析后整数 × 10000），数量 |

---

## 深交所（SZSE）

### 实际 CSV 列名

#### 逐笔委托（orders.csv）

| CSV 列名 | 内部字段 | 类型 | 说明 |
|---|---|---|---|
| ApplSeqNum | seq_num | int | 同 ChannelNo 内与成交统一编号（步进 1） |
| MDTime | time_raw | int | HHMMSSMMM → 自午夜纳秒（如 `091503750`） |
| OrderPrice | price | **float→int** | 元为单位浮点，解析时 `round(x * 10000)` |
| OrderQty | qty | int | 委托量 |
| OrderBSFlag | side_raw | str | `1`=买 `2`=卖 |
| OrderType | ord_type_raw | str | `1`=市价 `2`=限价 `3`=本方最优 |
| ChannelNo | channel_no | int | 通道编号 |
| SecurityID | security_id | str | 格式 `000400.SZ`，解析时去除后缀 |

#### 逐笔成交（trans.csv）

| CSV 列名 | 内部字段 | 类型 | 说明 |
|---|---|---|---|
| ApplSeqNum | seq_num | int | 同 ChannelNo 内与委托统一编号 |
| MDTime | time_raw | int | HHMMSSMMM |
| TradeBuyNo | bid_order_seq | int | 买方委托的 ApplSeqNum（0 表示不涉及买方） |
| TradeSellNo | ask_order_seq | int | 卖方委托的 ApplSeqNum（0 表示不涉及卖方） |
| TradePrice | price | **float→int** | 元为单位浮点，解析时 `round(x * 10000)`；撤单时为 0 |
| TradeQty | qty | int | 成交/撤单量 |
| TradeMoney | turnover | float | 成交金额（元） |
| **TradeType** | exec_type | str | **`1`=撤销 `2`=成交** |
| TradeBSFlag | trade_bs_flag | str | `1`=主买→`'B'` `2`=主卖→`'S'` |
| ChannelNo | channel_no | int | 通道编号 |
| SecurityID | security_id | str | 格式 `000400.SZ`，解析时去除后缀 |

### 订单簿重构方法

#### 1. 限价委托（OrderType=`2`）

```
book.add_order(seq_num, side, price, qty)
order_index[seq_num] = (side, price)
```

#### 2. 市价委托（OrderType=`1`，price ≤ 0）

IOC 扫对手方档位，余量直接丢弃，不进入盘口：

```python
fills = simulate_market_order(order, book, ts)
ref_price = fills[0].price if fills else 0  # 因子记录用第一笔成交价
```

#### 3. 本方最优委托（OrderType=`3`）

以**同侧**当前最优价挂限价单：
- 买单 → Bid1；卖单 → Ask1
- **同侧为空 → 废单**，静默丢弃

#### 4. 撤单（TradeType=`1`，通过成交流）

`TradeBuyNo` 或 `TradeSellNo` 中非零的一方即为被撤委托的 ApplSeqNum。
若委托不在 `order_index`，检查 `pending_orders`（价格笼子缓存）；仍找不到则记录异常。

#### 5. 成交（TradeType=`2`）

从 `TradeBuyNo` / `TradeSellNo` 分别找到买卖方委托，各自扣减剩余量。

---

## 上交所（SSE）

### 核心数据字段

#### 逐笔委托

| 原始字段 | 内部字段 | 说明 |
|---|---|---|
| ApplSeqNum | seq_num | 委托流独立编号（与成交流不共享） |
| **OrderNo** | order_no | **成交流 BuyNo/SellNo 对应此字段，非 seq_num** |
| OrderQty | qty | 连续竞价阶段为**剩余量**（非原始量） |
| BizIndex | biz_index | **同 Channel 内 order+trade 统一序号，是唯一正确排序键** |
| CancelFlag | cancel_flag | `D`=撤单 |

#### 逐笔成交

| 原始字段 | 内部字段 | 说明 |
|---|---|---|
| BuyNo | bid_order_seq | 买方委托的 **OrderNo**（须经 order_no_index 转换为 seq_num）|
| SellNo | ask_order_seq | 卖方委托的 OrderNo |
| TradeBSFlag | trade_bs_flag | `B`=主买 `S`=主卖 `N`=集合竞价 |
| BizIndex | biz_index | 统一序号 |

### 订单簿重构方法

#### 1. 限价委托

```python
book.add_order(seq_num, side, price, qty, order_no=order.order_no)
# 同时注册：order_no_index[order_no] = seq_num
```

#### 2. 撤单（CancelFlag=`D`，通过委托流）

```python
cancel_seq = book.order_no_index.get(order.order_no)
book.cancel_order(cancel_seq, cancel_qty)
```

#### 3. 成交 → 反查委托

```python
bid_seq = book.order_no_index.get(trade.bid_order_seq)  # OrderNo → seq_num
book.reduce_order(bid_seq, qty)
```

#### 4. 幽灵订单（Ghost Order）

全成主动单不出现在委托流，从成交记录反推：

```python
if order_no not in order_no_index:
    book.ghost_orders[order_no] = trade.price
    acc.add_order(OrderEvent(..., ord_type="market"))
```

---

## 边界情况与细节处理

### 1. 价格笼子（Price Cage）§3

深交所创业板：委托价偏离基准价超出笼子比例时，进入 `book.pending_orders` 缓存池，不立即入盘口。每笔成交后调用 `book.release_pending(trade_price, cage_pct)` 检查并迁入。

```python
SingleSecurityPipeline(..., enable_price_cage=True)
# 或
SZSEEngine(enable_price_cage=True, cage_pct=0.10)
```

### 2. BizIndex 排序（SSE）

- 同 Channel 内 order/trade 共享 BizIndex（1, 2, 3 …）
- 归并键：`(biz_index, priority, timestamp_ns)` — priority: 委托=0，成交=1
- 缺失 BizIndex 时降级用 `timestamp_ns`

### 3. 通道缓冲区（Channel Buffer）§4

实盘 ApplSeqNum 可能乱序到达，Channel Buffer 通过每通道优先队列保序：

```python
SingleSecurityPipeline(..., enable_channel_buffer=True)
```

- 序号连续时立即释放
- 检测到 gap 时等待，超时（默认 100 条）后强制推进

### 4. 日终补全（fill_to_end）

日终调用 `fill_to_end()` 而非 `flush()`，将 50ms 网格补全至 `end_ms`（默认 15:00:00），确保跨标的时间轴完整对齐：

```python
# pipeline.py 中自动调用
end_snaps = self.resampler.fill_to_end(self.book, self.current_phase)
```

### 5. 多标的相互触发（ChannelCoordinator）

同通道内任意标的时间戳跨越边界时，强制所有标的同时输出快照：

```python
coordinator = ChannelCoordinator(channel_no=2310, resample_ms=50)
coordinator.add_security("000001", book1, resampler1, engine1)
coordinator.add_security("000002", book2, resampler2, engine2)

for event in merged_channel_events:
    snaps = coordinator.ingest(event)

all_snaps = coordinator.flush_all()
```

### 6. 异常监控 §5

成交或撤单找不到对应委托时：
- `book.anomaly_count += 1`
- `acc.record_anomaly()` → 传播到 `LOBSnapshot.is_anomaly / anomaly_count`
- Parquet 的 `is_anomaly`, `anomaly_count` 列便于后续过滤

### 7. 正确性校验

将重构快照与交易所官方 L2 3s 快照对比：

```python
from lob.validator.validator import LOBValidator

reconstructed = pd.read_parquet("out/000400.parquet")
reference     = pd.read_csv("data/l2_000400.csv")

validator = LOBValidator(top_levels=10, time_tol_ms=50)
report = validator.compare_day(reconstructed, reference)
print(report.summary())
```

---

## 输出格式

50 ms 快照 Parquet，每行对应一个时间格（左开右闭区间末端，对齐到 50ms 网格）。

| 列组 | 列 | 说明 |
|---|---|---|
| 基础 | `security_id`, `timestamp_ms`, `phase` | 证券代码、Unix ms、交易阶段编码 |
| 十档盘口 | `ask/bid_px_{1..10}`, `ask/bid_vol_{1..10}`, `ask/bid_cnt_{1..10}` | 价格(float)、委托量、笔数 |
| 静态因子 | `mid_price`, `spread`, `sheet_diff` | 中间价、价差、盘口相对强弱 |
| 区间 OHLCV | `open/high/low/close`, `volume`, `turnover`, `num_trades`, `buy/sell_volume` | 区间成交统计；静默区间全为 0 |
| 动态因子 | `order/match/cancel_vol_ask/bid_{1..10}` | 各档挂/成/撤量；静默区间清零 |
| 衍生指标 | `match_diff`, `order_diff`, `cancel_diff` | 海通 §3.2 四大指标（前三个）|
| OFI | `ofi`, `ofi_norm` | 订单流不平衡（原始 / 归一化）|
| 全天累计 | `last_price`, `cum_volume`, `cum_turnover` | carry-forward：最新成交价、全天累计量/额 |
| 异常监控 | `is_anomaly`, `anomaly_count` | 本区间是否异常、异常事件数 |

---

## 快速开始

### 安装

```bash
pip install pandas numpy sortedcontainers pyarrow tqdm
# 开发环境
pip install -e ".[dev]"
```

### 单只股票

```bash
python scripts/run_single.py \
    --exchange SZSE \
    --security 000400 \
    --orders data_test/orders.csv \
    --trades data_test/trans.csv \
    --output out/000400.parquet
```

创业板（启用价格笼子）：

```bash
python scripts/run_single.py --exchange SZSE --security 300001 \
    --orders data/orders_300001.csv \
    --trades data/trades_300001.csv \
    --output out/300001.parquet \
    --price-cage
```

### 批量处理

```bash
python scripts/run_batch.py \
    --exchange SSE \
    --data-dir data/ \
    --output-dir out/
```

### 使用示例数据测试

```bash
# 用 data_test/ 中的深交所样本数据端到端验证
python scripts/test_data_test.py
```

预期输出：约 432,000 行快照（9:00:00–15:00:00），时间戳全部对齐到 50ms 网格，盘口有效率 ≥ 95%。

### 单元测试

```bash
python -m pytest tests/ -v                          # 全部（48 个）
python -m pytest tests/test_order_book.py -v        # 单文件
python -m pytest tests/ -k "test_szse" -v           # 按名称过滤
python -m pytest tests/ --cov=lob --cov=config      # 覆盖率
```

---

## 架构说明

```
CSV（orders / trans）
 └─ io/reader.py              分块读取 → 原始 DataFrame
      └─ parsers/             行 → Order / Trade 对象
           └─ [channel_buffer.py]   可选：每通道 min-heap 保序
                └─ pipeline.py      双流归并（SZSE: seq_num；SSE: BizIndex）
                     └─ engine/     更新 OrderBook，发射 OrderEvent/TradeEvent/CancelEvent
                          └─ resampler/   50ms 网格快照 + carry-forward + fill_to_end
                               └─ factors/    static / dynamic / derived / OFI
                                    └─ io/writer.py    Parquet 输出

可选横切模块：
  pipeline/channel_coordinator.py  — 多标的相互触发快照对齐
  validator/validator.py            — 与官方 L2 3s 快照对比校验
```

### 关键不变量

- **价格整数化**：所有价格 ×10000 存储，永不使用浮点比较；CSV 中的浮点价格由 parser 负责转换
- **order_index**：`seq_num → (side, price)` O(1) 撤单定位
- **order_no_index**（SSE 专用）：`OrderNo → seq_num` 供成交反查
- **pending_orders**（SZSE 可选）：`seq_num → (side, price, qty)` 价格笼子缓存
- **bids**：`SortedDict` 键为**负价**，`keys()[0]` = 买一
- **asks**：`SortedDict` 键为**正价**，`keys()[0]` = 卖一
- **fill_to_end()** 而非 `flush()`：确保 50ms 网格完整覆盖至 15:00:00，跨标的可直接 JOIN

### 添加新交易所

1. `config/exchange_config.py` — 字段映射表（CSV 列名 → 内部字段名）
2. `lob/parsers/` — Parser 子类（行 → Order/Trade，处理价格浮点转整数、证券代码格式等）
3. `lob/engine/` — Engine 子类（process_order / process_trade / process_cancel）
4. `lob/io/reader.py` — 读取函数
5. `lob/pipeline/pipeline.py` — `_merge_events_*` 函数 + `__init__` 分支
