"""
订单簿核心数据结构

PDF 参考：第 3 节「价格笼子订单」、第 4 节「Order Cache / Price Levels」

设计要点：
- bids 使用 SortedDict，键为 -price（负号），使最大买价排在最前面
- asks 使用 SortedDict，键为 +price，使最小卖价排在最前面
- order_index：seq_num → (side, price_int) 实现 O(1) 撤单定位
- pending_orders：价格笼子外的委托暂存池（PDF §3），基准价格移动后迁入盘口
- 所有价格均为整数（× 10000），不做浮点运算
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from sortedcontainers import SortedDict

from lob.models.order import Side
from lob.models.price_level import PriceLevel


@dataclass
class OrderBook:
    """
    实时限价订单簿。

    bids : SortedDict[int, PriceLevel]
        键为 **负的** 价格整数，SortedDict 按升序排列，
        因此 keys()[0] 对应负值最小 = 正值最大 = 最优买价（买一）。

    asks : SortedDict[int, PriceLevel]
        键为正的价格整数，keys()[0] = 最小 = 最优卖价（卖一）。

    order_index : Dict[int, Tuple[Side, int]]
        seq_num → (side, raw_price_int)
        供撤单、部分成交等操作 O(1) 查找委托位置。
    """
    security_id: str
    exchange:    str = ""

    # 核心盘口结构
    bids: SortedDict = field(default_factory=SortedDict)
    asks: SortedDict = field(default_factory=SortedDict)

    # O(1) 撤单索引
    order_index: Dict[int, Tuple[Side, int]] = field(default_factory=dict)

    # 上交所 OrderNo → seq_num 映射（用于通过 BuyNo/SellNo 反查 order_index）
    # 深交所无需此索引，因为 BidApplSeqNum/OfferApplSeqNum 即为 seq_num
    order_no_index: Dict[int, int] = field(default_factory=dict)

    # 集合竞价撮合缓存（由 auction_engine 写入，快照读取）
    auction_match_price: int = 0
    auction_match_qty:   int = 0

    # 上交所幽灵订单（全成主动单无委托记录）
    ghost_orders: Dict[int, int] = field(default_factory=dict)  # seq_num → price

    # ── 价格笼子（Price Cage）待入池 PDF §3 ──────────────────────────────
    # 深交所创业板：委托价格超出基准价笼子范围时，委托进入"等待进入"缓冲池，
    # 不立即入盘口。当基准价移动使委托进入笼子范围后，再迁入盘口。
    # 结构：seq_num → (side, price_int, qty)
    pending_orders: Dict[int, Tuple[Side, int, int]] = field(default_factory=dict)

    # ── 异常计数 PDF §5 ────────────────────────────────────────────────────
    # 收到撤单/成交但在 order_index 找不到对应委托时递增
    anomaly_count: int = 0

    # ── 添加委托 ──────────────────────────────────────────────────────────────

    def add_order(
        self,
        seq_num: int,
        side: Side,
        price: int,
        qty: int,
        order_no: Optional[int] = None,
    ) -> None:
        """
        将委托加入对应档位，并注册到 order_index。
        若该价格档位不存在则自动创建。
        order_no 为上交所 OrderNo，若传入则同时注册到 order_no_index。
        """
        if side == Side.BID:
            key = -price
            book = self.bids
        else:
            key = price
            book = self.asks

        if key not in book:
            book[key] = PriceLevel(price=price)
        book[key].add_order(seq_num, qty)
        self.order_index[seq_num] = (side, price)
        if order_no is not None:
            self.order_no_index[order_no] = seq_num

    # ── 撤单 ──────────────────────────────────────────────────────────────────

    def cancel_order(self, seq_num: int, cancel_qty: Optional[int] = None) -> bool:
        """
        撤销指定 seq_num 的委托（全部或部分）。
        cancel_qty=None 表示全部撤销。
        返回 True 表示成功找到并撤销，False 表示未找到。
        """
        entry = self.order_index.get(seq_num)
        if entry is None:
            return False
        side, price = entry

        if side == Side.BID:
            key = -price
            book = self.bids
        else:
            key = price
            book = self.asks

        level: Optional[PriceLevel] = book.get(key)
        if level is None:
            return False

        if cancel_qty is None:
            removed = level.remove_order_by_seq(seq_num)
        else:
            found = level.cancel_order(seq_num, cancel_qty)
            if not found:
                return False
            removed = cancel_qty

        # 若档位已空则从盘口删除
        if level.is_empty:
            del book[key]

        # 若全部撤单则从索引中移除
        if level.is_empty or cancel_qty is None:
            self.order_index.pop(seq_num, None)

        return removed > 0

    # ── 消耗（成交）──────────────────────────────────────────────────────────

    def consume_from_level(self, side: Side, price: int, qty: int) -> int:
        """
        在指定档位上按 FIFO 消耗成交量。
        返回实际消耗量。用于连续竞价撮合后更新盘口状态。
        """
        if side == Side.BID:
            key = -price
            book = self.bids
        else:
            key = price
            book = self.asks

        level: Optional[PriceLevel] = book.get(key)
        if level is None:
            return 0

        consumed = level.consume_qty(qty)
        if level.is_empty:
            del book[key]
        return consumed

    def reduce_order(self, seq_num: int, trade_qty: int) -> bool:
        """
        根据成交记录减少单个委托的剩余量。
        返回 True 若委托找到且已更新。
        """
        entry = self.order_index.get(seq_num)
        if entry is None:
            return False
        side, price = entry

        if side == Side.BID:
            key = -price
            book = self.bids
        else:
            key = price
            book = self.asks

        level: Optional[PriceLevel] = book.get(key)
        if level is None:
            return False

        found = level.cancel_order(seq_num, trade_qty)  # 复用 cancel_order 减量逻辑
        if level.is_empty:
            del book[key]
        # 若委托已全部成交则从索引中移除
        # （cancel_order 在 qty→0 时已从 orders deque 移除，需同步清理索引）
        if seq_num not in {sn for lvl in ([book.get(key)] if key in book else [])
                           for sn, _ in (lvl.orders if lvl else [])}:
            self.order_index.pop(seq_num, None)
        return found

    # ── 盘口查询 ──────────────────────────────────────────────────────────────

    def best_bid(self) -> Optional[int]:
        """最优买价（整数 × 10000），无委托返回 None。"""
        if self.bids:
            return -self.bids.keys()[0]
        return None

    def best_ask(self) -> Optional[int]:
        """最优卖价（整数 × 10000），无委托返回 None。"""
        if self.asks:
            return self.asks.keys()[0]
        return None

    def mid_price(self) -> Optional[float]:
        """买卖中间价（浮点数），买卖任一不存在时返回 None。"""
        bb = self.best_bid()
        ba = self.best_ask()
        if bb is not None and ba is not None:
            return (bb + ba) / 2.0 / 10_000.0
        return None

    def spread(self) -> Optional[float]:
        """买卖价差（浮点数）。"""
        bb = self.best_bid()
        ba = self.best_ask()
        if bb is not None and ba is not None:
            return (ba - bb) / 10_000.0
        return None

    def top_k_bids(self, k: int = 10) -> List[PriceLevel]:
        """返回前 k 个最优买档（买一在 index 0）。"""
        return [self.bids[key] for key in self.bids.keys()[:k]]

    def top_k_asks(self, k: int = 10) -> List[PriceLevel]:
        """返回前 k 个最优卖档（卖一在 index 0）。"""
        return [self.asks[key] for key in self.asks.keys()[:k]]

    def total_bid_levels(self) -> int:
        return len(self.bids)

    def total_ask_levels(self) -> int:
        return len(self.asks)

    def clear(self) -> None:
        """清空订单簿（用于交易结束后的重置）。"""
        self.bids.clear()
        self.asks.clear()
        self.order_index.clear()
        self.order_no_index.clear()
        self.pending_orders.clear()
        self.ghost_orders.clear()
        self.auction_match_price = 0
        self.auction_match_qty = 0
        self.anomaly_count = 0

    # ── 价格笼子待入池操作 PDF §3 ─────────────────────────────────────────

    def add_pending(self, seq_num: int, side: Side, price: int, qty: int) -> None:
        """
        将超出价格笼子范围的委托加入待入池。
        当基准价移动后调用 release_pending() 将符合条件的委托迁入盘口。
        """
        self.pending_orders[seq_num] = (side, price, qty)

    def release_pending(
        self,
        reference_price: int,
        cage_pct: float = 0.1,
    ) -> int:
        """
        检查待入池中是否有委托因基准价移动而满足笼子条件，若满足则迁入盘口。

        Parameters
        ----------
        reference_price : 新的基准价（整数 × 10000）
        cage_pct        : 笼子范围百分比，默认 10%（创业板偏离限制）

        Returns
        -------
        本次迁入盘口的委托数量。
        """
        if not self.pending_orders or reference_price <= 0:
            return 0

        cage_range = int(reference_price * cage_pct)
        lower = reference_price - cage_range
        upper = reference_price + cage_range

        to_release = [
            seq_num
            for seq_num, (side, price, qty) in self.pending_orders.items()
            if lower <= price <= upper
        ]

        for seq_num in to_release:
            side, price, qty = self.pending_orders.pop(seq_num)
            self.add_order(seq_num, side, price, qty)

        return len(to_release)
