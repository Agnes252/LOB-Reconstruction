"""
核心订单与成交数据类
所有价格均以整数存储（原始值 × 10000），避免浮点误差。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional


class Exchange(Enum):
    SZSE = "SZSE"   # 深圳证券交易所
    SSE  = "SSE"    # 上海证券交易所


class Side(Enum):
    BID = 1   # 买方
    ASK = 2   # 卖方


class OrdType(Enum):
    LIMIT    = "limit"     # 限价委托
    MARKET   = "market"    # 市价委托（IOC，深交所 OrdType='1'）
    OWN_BEST = "own_best"  # 本方最优（深交所 OrdType='U'）：买单挂在当前买一价，卖单挂在当前卖一价；
                           # 若同侧无挂单则为废单（不进入盘口）
    AUCTION  = "auction"   # 集合竞价期间隐含类型（集合竞价只接受限价单）


class OrderStatus(Enum):
    ACTIVE    = auto()
    PARTIAL   = auto()   # 部分成交
    FILLED    = auto()   # 全部成交
    CANCELLED = auto()


@dataclass(slots=True)
class Order:
    """
    逐笔委托（来自深/沪任一交易所）。

    Attributes
    ----------
    seq_num        ApplSeqNum，单交易所内单调递增。
    security_id    证券代码（如 '000001'）。
    exchange       交易所。
    timestamp_ns   自午夜起的纳秒数（由 parser 将 HHMMSSMMM 转换）。
    price          委托价格 × 10000；市价单 price ≤ 0。
    qty            原始委托数量（手或股，取决于数据源）。
    remaining      剩余未成交数量。
    filled_qty     已成交数量。
    side           买/卖方向。
    ord_type       委托类型。
    status         当前状态。
    cancel_flag    上交所专用：'D' 表示该记录是撤单指令。
    szse_ord_type_raw  深交所原始委托类型字符串（'1','2','U'）。
    """
    seq_num:           int
    security_id:       str
    exchange:          Exchange
    timestamp_ns:      int
    price:             int
    qty:               int
    remaining:         int
    side:              Side
    ord_type:          OrdType
    status:            OrderStatus = OrderStatus.ACTIVE
    filled_qty:        int = 0
    cancel_flag:       Optional[str] = None
    szse_ord_type_raw: Optional[str] = None
    order_no:          Optional[int] = None   # 上交所 OrderNo（撤单和成交流反查原始委托的键）
    channel_no:        Optional[int] = None   # 通道号（深交所 ChannelNo，上交所 Channel）
    biz_index:         Optional[int] = None   # 上交所统一序号 BizIndex（跨order/trade的排序键）

    @property
    def price_float(self) -> float:
        return self.price / 10_000.0

    def is_market(self) -> bool:
        return self.price <= 0

    def is_own_best(self) -> bool:
        return self.ord_type == OrdType.OWN_BEST

    def timestamp_ms(self) -> int:
        return self.timestamp_ns // 1_000_000


@dataclass(slots=True)
class Trade:
    """
    逐笔成交（或撤单记录）。

    深交所：成交和撤单均通过成交流下发。
        - is_cancel=False 表示真实成交（ExecType='F'）
        - is_cancel=True  表示撤单（ExecType='4'）
    上交所：成交通过成交流，撤单通过委托流的 CancelFlag='D'。
        - Trade 对象始终为真实成交，is_cancel=False。

    Attributes
    ----------
    bid_order_seq  买方委托 seq_num（深交所 BidApplSeqNum，上交所 BuyNo）。
    ask_order_seq  卖方委托 seq_num。
    trade_bs_flag  上交所成交方向标志：'B'=主买/'S'=主卖/'N'=集合竞价。
    """
    seq_num:       int
    security_id:   str
    exchange:      Exchange
    timestamp_ns:  int
    price:         int         # 成交价 × 10000
    qty:           int         # 成交数量
    is_cancel:     bool = False

    bid_order_seq: Optional[int] = None   # 深交所=BidApplSeqNum，上交所=BuyNo(即OrderNo)
    ask_order_seq: Optional[int] = None   # 深交所=OfferApplSeqNum，上交所=SellNo(即OrderNo)
    trade_bs_flag: Optional[str] = None   # 上交所成交方向标志：'B'=主买/'S'=主卖/'N'=集合竞价
    turnover:      float = 0.0            # 成交金额（上交所 TradeMoney 字段）
    channel_no:    Optional[int] = None   # 通道号（上交所 TradeChannel）
    biz_index:     Optional[int] = None   # 上交所统一序号 BizIndex

    @property
    def price_float(self) -> float:
        return self.price / 10_000.0

    def timestamp_ms(self) -> int:
        return self.timestamp_ns // 1_000_000
