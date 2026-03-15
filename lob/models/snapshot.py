"""
快照与区间累积器数据类

PDF 参考：第 5 节「异常监控」、第 6 节「项目结果：重构好的 LOB 样式」

LOBSnapshot         : 一个 50ms 时间格的完整输出记录（不可变视角）
IntervalAccumulator : 当前区间内累积的原始事件（可变，用于计算动态因子）

新增字段（相比原版）
--------------------
LOBSnapshot:
    ofi           - Order Flow Imbalance（订单流不平衡）
    ofi_norm      - 归一化 OFI
    is_anomaly    - 本区间是否检测到异常（撤单/成交找不到对应委托）
    anomaly_count - 本区间异常事件计数
    last_price    - 区间末最新成交价（跨区间 carry-forward）
    cum_volume    - 截至本快照的累计成交量（全天）
    cum_turnover  - 截至本快照的累计成交额（全天）

IntervalAccumulator:
    anomaly_count - 本区间异常事件计数
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from config.settings import TOP_LEVELS

# ──────────────────────────────────────────────────────────────────────────────
# 辅助结构
# ──────────────────────────────────────────────────────────────────────────────

@dataclass(slots=True)
class LevelSnapshot:
    """单个价格档位的快照（价格已转为浮点）。"""
    price:  float   # ÷10000 后的浮点价格
    volume: int     # 剩余委托量
    count:  int     # 委托笔数

    @classmethod
    def empty(cls) -> "LevelSnapshot":
        return cls(price=0.0, volume=0, count=0)


# ──────────────────────────────────────────────────────────────────────────────
# 50ms 快照
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class LOBSnapshot:
    """
    50ms 时间格的完整输出行。
    timestamp_ms 为区间**起始**时刻（Unix 毫秒，对齐到 50ms 网格）。
    """
    # ── 标识 ──────────────────────────────────────────────────────────────────
    security_id:  str
    timestamp_ms: int          # 区间起始，Unix ms
    phase:        int          # TradingPhase 整数编码

    # ── 十档盘口（index 0 = 最优档）──────────────────────────────────────────
    asks: List[LevelSnapshot] = field(default_factory=lambda: [LevelSnapshot.empty() for _ in range(TOP_LEVELS)])
    bids: List[LevelSnapshot] = field(default_factory=lambda: [LevelSnapshot.empty() for _ in range(TOP_LEVELS)])

    # ── 静态盘口因子 ──────────────────────────────────────────────────────────
    mid_price:  Optional[float] = None
    spread:     Optional[float] = None
    sheet_diff: Optional[float] = None    # (VB1-VA1)/(VB1+VA1)

    # ── OHLCV（区间内成交） ───────────────────────────────────────────────────
    open_px:    Optional[float] = None
    high_px:    Optional[float] = None
    low_px:     Optional[float] = None
    close_px:   Optional[float] = None    # 区间最后一笔成交价
    volume:     int = 0                   # 成交数量
    turnover:   float = 0.0               # 成交金额
    num_trades: int = 0
    buy_volume: int = 0                   # 主买成交量（主动买入）
    sell_volume:int = 0                   # 主卖成交量（主动卖出）

    # ── 动态订单流因子（每档，列表长度 = TOP_LEVELS）─────────────────────────
    order_vol_ask:  List[int] = field(default_factory=lambda: [0] * TOP_LEVELS)
    order_vol_bid:  List[int] = field(default_factory=lambda: [0] * TOP_LEVELS)
    match_vol_ask:  List[int] = field(default_factory=lambda: [0] * TOP_LEVELS)
    match_vol_bid:  List[int] = field(default_factory=lambda: [0] * TOP_LEVELS)
    cancel_vol_ask: List[int] = field(default_factory=lambda: [0] * TOP_LEVELS)
    cancel_vol_bid: List[int] = field(default_factory=lambda: [0] * TOP_LEVELS)

    # ── 海通 §3.2 四大衍生指标 ───────────────────────────────────────────────
    match_diff:  Optional[float] = None   # 成交相对强弱
    order_diff:  Optional[float] = None   # 挂单相对强弱
    cancel_diff: Optional[float] = None   # 撤单相对强弱

    # ── OFI（订单流不平衡）PDF §6 ─────────────────────────────────────────
    ofi:          Optional[float] = None  # 原始 OFI（区间内最优档净量变化）
    ofi_norm:     Optional[float] = None  # 归一化 OFI（除以最优档总深度）

    # ── 异常监控 PDF §5 ───────────────────────────────────────────────────
    is_anomaly:    bool = False   # 本区间是否有撤单/成交找不到对应委托
    anomaly_count: int  = 0       # 本区间异常事件计数

    # ── 累计成交统计（全天，carry-forward）PDF §6 ─────────────────────────
    last_price:   Optional[float] = None  # 截至本快照的最新成交价
    cum_volume:   int   = 0               # 全天累计成交量（股）
    cum_turnover: float = 0.0             # 全天累计成交额（元）


# ──────────────────────────────────────────────────────────────────────────────
# 区间累积器（可变，每 50ms 重置）
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class CancelEvent:
    """撤单事件记录（供动态因子计算）。"""
    seq_num:   int
    side_str:  str     # 'bid' or 'ask'
    price:     int     # × 10000
    qty:       int
    timestamp_ns: int


@dataclass
class TradeEvent:
    """成交事件记录（供动态因子计算）。"""
    price:        int    # × 10000
    qty:          int
    bs_flag:      str    # 'B'=主买 'S'=主卖 'N'=集合竞价
    bid_seq:      Optional[int]
    ask_seq:      Optional[int]
    timestamp_ns: int
    turnover:     float = 0.0


@dataclass
class OrderEvent:
    """新委托事件记录（供动态因子计算）。"""
    seq_num:      int
    side_str:     str    # 'bid' or 'ask'
    price:        int    # × 10000
    qty:          int
    ord_type:     str    # 'limit', 'market', 'own_best'
    timestamp_ns: int


@dataclass
class IntervalAccumulator:
    """
    当前 50ms 区间内的原始事件累积。
    由 Resampler 管理，每 50ms 清零并用于生成 LOBSnapshot。
    """
    ts_start_ms: int
    ts_end_ms:   int

    trades:       List[TradeEvent]  = field(default_factory=list)
    new_orders:   List[OrderEvent]  = field(default_factory=list)
    cancel_events:List[CancelEvent] = field(default_factory=list)

    # OHLCV 快速累积（避免每次遍历 trades 重算）
    _open_px:   Optional[float] = None
    _high_px:   Optional[float] = None
    _low_px:    Optional[float] = None
    _close_px:  Optional[float] = None
    _volume:    int = 0
    _turnover:  float = 0.0
    _num_trades:int = 0
    _buy_vol:   int = 0
    _sell_vol:  int = 0

    # 异常监控 PDF §5：撤单/成交找不到对应委托时累计
    anomaly_count: int = 0

    def add_trade(self, t: TradeEvent) -> None:
        px = t.price / 10_000.0
        self.trades.append(t)
        if self._open_px is None:
            self._open_px = px
        self._high_px = max(self._high_px or px, px)
        self._low_px  = min(self._low_px  or px, px)
        self._close_px = px
        self._volume     += t.qty
        turnover = t.turnover if t.turnover > 0 else px * t.qty
        self._turnover   += turnover
        self._num_trades += 1
        if t.bs_flag == "B":
            self._buy_vol  += t.qty
        elif t.bs_flag == "S":
            self._sell_vol += t.qty

    def add_order(self, o: OrderEvent) -> None:
        self.new_orders.append(o)

    def add_cancel(self, c: CancelEvent) -> None:
        self.cancel_events.append(c)

    def record_anomaly(self) -> None:
        """记录一次异常（撤单/成交找不到对应委托）。"""
        self.anomaly_count += 1

    def reset(self, ts_start_ms: int, ts_end_ms: int) -> None:
        self.ts_start_ms = ts_start_ms
        self.ts_end_ms   = ts_end_ms
        self.trades.clear()
        self.new_orders.clear()
        self.cancel_events.clear()
        self._open_px   = None
        self._high_px   = None
        self._low_px    = None
        self._close_px  = None
        self._volume    = 0
        self._turnover  = 0.0
        self._num_trades= 0
        self._buy_vol   = 0
        self._sell_vol  = 0
        self.anomaly_count = 0
