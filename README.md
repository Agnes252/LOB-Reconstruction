# LOB 限价订单簿重建系统

基于沪深两市 Level2 逐笔委托 / 逐笔成交数据，完整还原限价订单簿并输出 50 ms 重采样快照，含 OFI 因子与异常监控。

> 本项目参考文献：《沪深逐笔数据订单簿重构指南》（内部 PDF）、海通证券选股因子系列研究 #75

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
| MsgType | — | 消息类型，0 = 委托，1 = 成交 |
| Side | — | 1 = 买，2 = 卖 |
| Price / Qty | — | 价格（整数 × 10000），数量 |

---

## 深交所（SZSE）

### 核心数据字段

#### 逐笔委托

| 原始字段 | 内部字段 | 说明 |
|---|---|---|
| ApplSeqNum | seq_num | 同 ChannelNo 内与成交统一编号（步进 1） |
| TransactTime | time_raw | HHMMSSMMM → 自午夜纳秒 |
| Price | price | ×10000；市价单 = 0 或 −1 |
| OrderQty | qty | 委托量 |
| Side | side | 1=买 2=卖 |
| OrdType | ord_type | 1=市价 2=限价 U=本方最优 |
| ChannelNo | channel_no | 通道编号 |

#### 逐笔成交

| 原始字段 | 内部字段 | 说明 |
|---|---|---|
| BidApplSeqNum | bid_order_seq | 买方委托 seq_num（直接查 order_index）|
| OfferApplSeqNum | ask_order_seq | 卖方委托 seq_num |
| ExecType | exec_type | F=成交 4=撤单 |

### 订单簿重构方法

#### 1. 限价委托（OrdType='2'）

```
book.add_order(seq_num, side, price, qty)
order_index[seq_num] = (side, price)
```

#### 2. 市价委托（OrdType='1'，price ≤ 0）

IOC 扫对手方档位，余量直接丢弃，不进入盘口：

```python
fills = simulate_market_order(order, book, ts)
ref_price = fills[0].price if fills else 0  # 因子记录用第一笔成交价
```

#### 3. 本方最优委托（OrdType='U'）

以**同侧**当前最优价挂限价单：
- 买单 → Bid1；卖单 → Ask1
- **同侧为空 → 废单**，静默丢弃

#### 4. 撤单（ExecType='4'，通过成交流）

`bid_order_seq` 或 `ask_order_seq` 其中一个非零，即为被撤委托的 seq_num。
若委托不在 `order_index`，检查 `pending_orders`（价格笼子缓存）；仍找不到则记录异常。

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
| CancelFlag | cancel_flag | D=撤单 |

#### 逐笔成交

| 原始字段 | 内部字段 | 说明 |
|---|---|---|
| BuyNo | bid_order_seq | 买方委托的 **OrderNo**（须经 order_no_index 转换为 seq_num）|
| SellNo | ask_order_seq | 卖方委托的 OrderNo |
| TradeBSFlag | trade_bs_flag | B=主买 S=主卖 N=集合竞价 |
| BizIndex | biz_index | 统一序号 |

### 订单簿重构方法

#### 1. 限价委托（OrdType='2'）

```python
book.add_order(seq_num, side, price, qty, order_no=order.order_no)
# 同时注册：order_no_index[order_no] = seq_num
```

#### 2. 市价委托（OrdType='1'）

同深交所，IOC 扫对手方，余量丢弃。

#### 3. 撤单（CancelFlag='D'，通过委托流）

```python
cancel_seq = book.order_no_index.get(order.order_no)
# 降级：若无 order_no 则尝试直接用 seq_num
book.cancel_order(cancel_seq, cancel_qty)
```
找不到则记录异常（PDF §5）。

#### 4. 成交 → 反查委托

```python
bid_seq = book.order_no_index.get(trade.bid_order_seq)  # OrderNo → seq_num
book.reduce_order(bid_seq, qty)
```

#### 5. 幽灵订单（Ghost Order）

全成主动单不出现在委托流。从成交记录反推：

```python
if order_no not in order_no_index:
    book.ghost_orders[order_no] = trade.price  # 标记，避免重复合成
    acc.add_order(OrderEvent(..., ord_type="market"))
```

---

## 边界情况与细节处理

### 1. 价格笼子（Price Cage）PDF §3

深交所创业板：委托价偏离基准价超出笼子比例时，不立即入盘口，进入 `book.pending_orders` 缓存池。
每笔成交后调用 `book.release_pending(trade_price, cage_pct)` 检查并迁入。

启用方式：
```python
SingleSecurityPipeline(..., enable_price_cage=True)
# 或
SZSEEngine(enable_price_cage=True, cage_pct=0.10)
```

### 2. BizIndex 排序（SSE）

- 同 Channel 内 order/trade 共享 BizIndex（1, 2, 3 …）
- 2025 年前可能非严格单调（双线程推送），但仍是正确排序依据
- 归并键：`(biz_index, priority, timestamp_ns)` — priority: 委托=0，成交=1
- 缺失 BizIndex 时降级用 `timestamp_ns`（保底）

### 3. 通道缓冲区（Channel Buffer）PDF §4

实盘行情可能因网络抖动导致 ApplSeqNum 乱序到达。Channel Buffer 通过每通道优先队列（min-heap）保序：

```python
SingleSecurityPipeline(..., enable_channel_buffer=True)
```

- 序号连续时立即释放
- 检测到 gap 时等待缺失消息
- 超时（默认 100 条）后强制推进并记录异常

### 4. 市价单 price = 0 / −1

- 深交所市价单 OrdType=`'1'`：`is_market()` 判断 `price <= 0`
- 因子记录用第一笔实际成交价；无成交时 ref_price = 0

### 5. 异常监控 PDF §5

成交或撤单找不到对应委托时（非幽灵订单）：
- `book.anomaly_count += 1`
- `acc.record_anomaly()` → 传播到 `LOBSnapshot.is_anomaly / anomaly_count`
- 写入 Parquet 的 `is_anomaly`, `anomaly_count` 列便于后续过滤

### 6. 上交所剩余量语义

连续竞价阶段，委托流 `OrderQty` = 被撮合后**剩余量**（非原始量）。
已全成的委托不出现在委托流（需从成交流反推幽灵订单）。

### 7. 跨日清理 PDF §5

每次创建新 `SingleSecurityPipeline` 实例即重置全部状态（OrderBook、价格跟踪、累计统计）。
日间无需手动清理。

---

## 输出格式

50 ms 快照 Parquet，每行对应一个时间格。

| 列组 | 列 | 说明 |
|---|---|---|
| 基础 | `security_id`, `timestamp_ms`, `phase` | 证券代码、Unix ms、交易阶段编码 |
| 十档盘口 | `ask/bid_px_{1..10}`, `ask/bid_vol_{1..10}`, `ask/bid_cnt_{1..10}` | 价格(float)、委托量、笔数 |
| 静态因子 | `mid_price`, `spread`, `sheet_diff` | 中间价、价差、盘口相对强弱 |
| 区间 OHLCV | `open/high/low/close`, `volume`, `turnover`, `num_trades`, `buy/sell_volume` | 区间成交统计 |
| 动态因子 | `order/match/cancel_vol_ask/bid_{1..10}` | 各档挂/成/撤量 |
| 衍生指标 | `match_diff`, `order_diff`, `cancel_diff` | 海通 §3.2 四大指标（前三个） |
| **OFI** | `ofi`, `ofi_norm` | **订单流不平衡（原始/归一化）** |
| 全天累计 | `last_price`, `cum_volume`, `cum_turnover` | 最新成交价、累计成交量/金额 |
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
    --security 000001 \
    --orders data/orders_000001.csv \
    --trades data/trades_000001.csv \
    --output out/000001.parquet
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

### 测试

```bash
python -m pytest tests/ -v                          # 全部
python -m pytest tests/test_order_book.py -v        # 单文件
python -m pytest tests/ -k "test_szse" -v           # 按名称过滤
python -m pytest tests/ --cov=lob --cov=config      # 覆盖率
```

---

## 架构说明

```
CSV
 └─ io/reader.py          分块读取 → 原始 DataFrame
      └─ parsers/          行 → Order / Trade 对象（含 channel_no / biz_index）
           └─ [channel_buffer.py]  可选：每通道 PQ，按序号保序
                └─ pipeline.py   双流归并（SZSE: seq_num 排序；SSE: BizIndex 排序）
                     └─ engine/  更新 OrderBook，发射 OrderEvent/TradeEvent/CancelEvent
                          └─ resampler/  50ms 网格快照 + 因子计算
                               └─ factors/  static / dynamic / derived / OFI
                                    └─ io/writer.py  Parquet 输出
```

### 关键不变量

- **价格整数化**：所有价格 ×10000 存储，永不使用浮点比较
- **order_index**：`seq_num → (side, price)` O(1) 撤单定位
- **order_no_index**（SSE 专用）：`OrderNo → seq_num` 供成交反查
- **pending_orders**（SZSE 可选）：`seq_num → (side, price, qty)` 价格笼子缓存
- **bids**：`SortedDict` 键为 **负价**，`keys()[0]` = 买一
- **asks**：`SortedDict` 键为 **正价**，`keys()[0]` = 卖一

### 添加新交易所

1. `config/exchange_config.py` — 字段映射表
2. `lob/parsers/` — Parser 子类（行 → Order/Trade）
3. `lob/engine/` — Engine 子类（process_order/trade/cancel）
4. `lob/io/reader.py` — 读取函数
5. `lob/pipeline/pipeline.py` — `_merge_events_*` 函数 + `__init__` 分支
