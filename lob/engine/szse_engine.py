"""
深交所 LOB 引擎

职责：
1. 处理逐笔委托（限价/市价/本方最优）
2. 处理逐笔成交（ExecType='F' → 按订单索引减量）
3. 处理撤单（ExecType='4' → 从盘口移除对应委托）
4. 返回需要记录到 IntervalAccumulator 的事件

深交所协议要点：
- 同一 ChannelNo 下，order 和 trade 的 ApplSeqNum 统一编号（步进1），无乱序。
- 撤单信息包含在成交流（ExecType='4'），bid_seq 或 ask_seq 指向被撤委托的 ApplSeqNum。
- 市价单 OrdType='1'，price=0 或 -1，IOC 扫对手方档位；余量直接丢弃。
- 本方最优 OrdType='U'：买单挂在当前买一价，卖单挂在当前卖一价；
  若同侧无挂单则成为废单（不入盘口，不计入因子）。

价格笼子（Price Cage）PDF §3：
- 深交所创业板：部分报价若触发价格笼子规则（偏离基准价一定比例），
  初始不体现在订单簿中，进入 pending_orders 缓存池。
- 当基准价（通常为最新成交价）移动后，调用 book.release_pending() 迁入盘口。

异常监控 PDF §5：
- 若收到成交/撤单但 order_index 中找不到对应委托，记录异常并累计。
"""
from __future__ import annotations

import logging
from typing import List, Optional

from lob.models.order import Order, Side, Trade
from lob.models.order_book import OrderBook
from lob.models.snapshot import (
    CancelEvent, IntervalAccumulator, OrderEvent, TradeEvent
)
from lob.phase.phase_classifier import TradingPhase

logger = logging.getLogger(__name__)

# 深交所创业板价格笼子范围（偏离基准价比例）
_DEFAULT_CAGE_PCT = 0.10  # 10%（创业板典型值）


class SZSEEngine:
    """
    深交所逐笔数据 LOB 重建引擎。

    Parameters
    ----------
    enable_price_cage : 是否启用价格笼子待入池（仅创业板等需要，默认关闭）
    cage_pct          : 价格笼子范围（偏离基准价百分比），默认 10%
    """

    def __init__(
        self,
        enable_price_cage: bool = False,
        cage_pct: float = _DEFAULT_CAGE_PCT,
    ) -> None:
        self.enable_price_cage = enable_price_cage
        self.cage_pct = cage_pct
        self._last_trade_price: int = 0  # 最新成交价（价格笼子基准）

    def process_order(
        self,
        order: Order,
        book: OrderBook,
        acc: IntervalAccumulator,
        phase: TradingPhase,
    ) -> List[TradeEvent]:
        """
        处理一条逐笔委托记录。

        Returns
        -------
        合成成交事件列表（市价单即时撮合产生的 fills）
        """
        fills: List[TradeEvent] = []

        # ── 本方最优委托（OrdType='U'）────────────────────────────────────────
        if order.is_own_best():
            return self._process_own_best(order, book, acc)

        # ── 市价委托（OrdType='1'，price≤0）─────────────────────────────────
        if order.is_market():
            from lob.engine.continuous_engine import simulate_market_order
            fills = simulate_market_order(order, book, order.timestamp_ns)
            ref_price = fills[0].price if fills else 0
            acc.add_order(OrderEvent(
                seq_num      = order.seq_num,
                side_str     = "bid" if order.side == Side.BID else "ask",
                price        = ref_price,
                qty          = order.qty,
                ord_type     = "market",
                timestamp_ns = order.timestamp_ns,
            ))
            for f in fills:
                acc.add_trade(f)
            return fills

        # ── 限价委托 ──────────────────────────────────────────────────────────
        # 若启用价格笼子且委托价格超出范围，先进入待入池
        if self.enable_price_cage and self._is_outside_cage(order.price):
            book.add_pending(order.seq_num, order.side, order.price, order.qty)
            logger.debug(
                "SZSE price cage: seq=%d price=%d outside cage (ref=%d +/-%.0f%%)",
                order.seq_num, order.price,
                self._last_trade_price, self.cage_pct * 100,
            )
            return fills

        book.add_order(order.seq_num, order.side, order.price, order.qty)
        acc.add_order(OrderEvent(
            seq_num      = order.seq_num,
            side_str     = "bid" if order.side == Side.BID else "ask",
            price        = order.price,
            qty          = order.qty,
            ord_type     = "limit",
            timestamp_ns = order.timestamp_ns,
        ))
        return fills

    def _process_own_best(
        self,
        order: Order,
        book: OrderBook,
        acc: IntervalAccumulator,
    ) -> List[TradeEvent]:
        """
        处理本方最优单（深交所 OrdType='U'）。

        买单挂在当前买一价，卖单挂在当前卖一价；
        若同侧无挂单则成为废单（不入盘口，不计入因子）。
        """
        if order.side == Side.BID:
            ref_price = book.best_bid()
        else:
            ref_price = book.best_ask()

        if ref_price is None:
            logger.debug(
                "SZSE OWN_BEST void: seq=%d side=%s no same-side quote",
                order.seq_num, order.side.name,
            )
            return []

        book.add_order(order.seq_num, order.side, ref_price, order.qty)
        acc.add_order(OrderEvent(
            seq_num      = order.seq_num,
            side_str     = "bid" if order.side == Side.BID else "ask",
            price        = ref_price,
            qty          = order.qty,
            ord_type     = "own_best",
            timestamp_ns = order.timestamp_ns,
        ))
        return []

    def process_trade(
        self,
        trade: Trade,
        book: OrderBook,
        acc: IntervalAccumulator,
    ) -> None:
        """
        处理 ExecType='F' 的成交记录：减少对应委托的剩余量。
        深交所 BidApplSeqNum/OfferApplSeqNum 即为 order 的 ApplSeqNum（seq_num），
        可直接用于 order_index 查找。

        成交时同步更新最新成交价，用于价格笼子基准计算，
        并尝试释放待入池中已进入笼子范围的委托（PDF §3）。
        """
        # 减少买方委托量，未找到则记录异常（PDF §5）
        if trade.bid_order_seq:
            if trade.bid_order_seq in book.order_index:
                book.reduce_order(trade.bid_order_seq, trade.qty)
            else:
                logger.debug(
                    "SZSE trade anomaly: bid_seq=%d not in order_index",
                    trade.bid_order_seq,
                )
                book.anomaly_count += 1
                acc.record_anomaly()

        # 减少卖方委托量，未找到则记录异常
        if trade.ask_order_seq:
            if trade.ask_order_seq in book.order_index:
                book.reduce_order(trade.ask_order_seq, trade.qty)
            else:
                logger.debug(
                    "SZSE trade anomaly: ask_seq=%d not in order_index",
                    trade.ask_order_seq,
                )
                book.anomaly_count += 1
                acc.record_anomaly()

        acc.add_trade(TradeEvent(
            price        = trade.price,
            qty          = trade.qty,
            bs_flag      = "B",   # 深交所成交流无主买/主卖标志，统一记为 B
            bid_seq      = trade.bid_order_seq,
            ask_seq      = trade.ask_order_seq,
            timestamp_ns = trade.timestamp_ns,
        ))

        # 更新最新成交价并尝试释放价格笼子待入池（PDF §3）
        if trade.price > 0:
            self._last_trade_price = trade.price
            if self.enable_price_cage and book.pending_orders:
                released = book.release_pending(trade.price, self.cage_pct)
                if released > 0:
                    logger.debug(
                        "SZSE price cage: released %d pending orders (ref=%d)",
                        released, trade.price,
                    )

    def process_cancel(
        self,
        trade: Trade,
        book: OrderBook,
        acc: IntervalAccumulator,
    ) -> None:
        """
        处理 ExecType='4' 的撤单记录。
        深交所撤单通过成交流下发：bid_seq 或 ask_seq 中有一个非零指向被撤委托。
        被撤委托的 bid_seq/ask_seq 即为原始委托的 ApplSeqNum（seq_num），
        可直接用于 order_index 查找。

        若委托不在盘口中，检查是否在 pending_orders（价格笼子待入池）中，
        若是则直接移除；否则记录异常（PDF §5）。
        """
        cancel_seq: Optional[int] = None

        if trade.bid_order_seq and trade.bid_order_seq > 0:
            cancel_seq = trade.bid_order_seq
        elif trade.ask_order_seq and trade.ask_order_seq > 0:
            cancel_seq = trade.ask_order_seq

        if cancel_seq is None:
            logger.warning(
                "SZSE cancel: both bid_seq and ask_seq are 0, trade_seq=%d",
                trade.seq_num,
            )
            return

        # 先查盘口
        entry = book.order_index.get(cancel_seq)
        if entry is not None:
            side, price = entry
            acc.add_cancel(CancelEvent(
                seq_num      = cancel_seq,
                side_str     = "bid" if side == Side.BID else "ask",
                price        = price,
                qty          = trade.qty,
                timestamp_ns = trade.timestamp_ns,
            ))
            book.cancel_order(cancel_seq, trade.qty)
            return

        # 再查价格笼子待入池
        if cancel_seq in book.pending_orders:
            book.pending_orders.pop(cancel_seq)
            logger.debug(
                "SZSE cancel: removed pending seq=%d from price cage buffer",
                cancel_seq,
            )
            return

        # 均未找到 → 异常（PDF §5）
        logger.debug(
            "SZSE cancel anomaly: seq_num=%d not in order_index or pending_orders",
            cancel_seq,
        )
        book.anomaly_count += 1
        acc.record_anomaly()

    # ── 内部方法 ──────────────────────────────────────────────────────────────

    def _is_outside_cage(self, price: int) -> bool:
        """判断委托价格是否超出价格笼子范围（基于最新成交价）。"""
        if self._last_trade_price <= 0 or price <= 0:
            return False
        cage = int(self._last_trade_price * self.cage_pct)
        lower = self._last_trade_price - cage
        upper = self._last_trade_price + cage
        return price < lower or price > upper
