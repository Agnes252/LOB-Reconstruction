"""
上交所 LOB 引擎

与深交所的关键差异：
1. 撤单通过委托流下发（cancel_flag='D'），而非成交流
2. 全成主动单（aggressive fully-filled orders）可能没有委托记录，
   需要从成交记录反推"幽灵订单"
3. 成交流含 TradeBSFlag（主买/主卖/集合竞价），直接使用
4. 无 OWN_BEST 单（上交所不支持本方最优类型）
5. 关键：成交记录的 BuyNo/SellNo 对应原始委托的 OrderNo（非 ApplSeqNum/seq_num）！
   必须通过 book.order_no_index (OrderNo→seq_num) 转换后再查 order_index。
6. 上交所连续竞价阶段推送的委托数量为剩余量（非原始数量），
   已被完全成交的委托不会出现在委托流中（需通过成交流反推幽灵订单）。

异常监控 PDF §5：
- 若收到成交/撤单但 order_no_index/order_index 中找不到对应委托（且非幽灵订单），
  记录异常并累计 book.anomaly_count 及 acc.anomaly_count。
"""
from __future__ import annotations

import logging
from typing import List, Optional

from lob.engine.continuous_engine import simulate_market_order
from lob.models.order import Exchange, Order, OrdType, Side, Trade
from lob.models.order_book import OrderBook
from lob.models.snapshot import (
    CancelEvent, IntervalAccumulator, OrderEvent, TradeEvent
)
from lob.phase.phase_classifier import TradingPhase

logger = logging.getLogger(__name__)


class SSEEngine:
    """上交所逐笔数据 LOB 重建引擎。"""

    def process_order(
        self,
        order: Order,
        book: OrderBook,
        acc: IntervalAccumulator,
        phase: TradingPhase,
    ) -> List[TradeEvent]:
        """
        处理上交所逐笔委托。

        若 cancel_flag == 'D'，则为撤单指令，转发给 _process_cancel_by_order。
        否则为正常委托，加入盘口。
        """
        fills: List[TradeEvent] = []

        # ── 撤单指令 ─────────────────────────────────────────────────────────
        if order.cancel_flag == "D":
            self._process_cancel_by_order(order, book, acc)
            return fills

        # ── 市价委托 ──────────────────────────────────────────────────────────
        if order.is_market():
            fills = simulate_market_order(order, book, order.timestamp_ns)
            filled_qty = sum(f.qty for f in fills)
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

        # ── 限价委托（正常入盘口）────────────────────────────────────────────
        # 注意：上交所连续竞价阶段的 qty 为剩余量（非原始数量）
        book.add_order(
            order.seq_num, order.side, order.price, order.qty,
            order_no=order.order_no,  # 注册 OrderNo→seq_num 映射供成交反查
        )
        acc.add_order(OrderEvent(
            seq_num      = order.seq_num,
            side_str     = "bid" if order.side == Side.BID else "ask",
            price        = order.price,
            qty          = order.qty,
            ord_type     = "limit",
            timestamp_ns = order.timestamp_ns,
        ))
        return fills

    def process_trade(
        self,
        trade: Trade,
        book: OrderBook,
        acc: IntervalAccumulator,
    ) -> None:
        """
        处理上交所成交记录（TradeBSFlag='B'/'S'/'N'）。

        BuyNo/SellNo 对应原始委托的 OrderNo，需通过 order_no_index 转换为 seq_num。
        若 OrderNo 不在 order_no_index 中，说明是全成主动单（幽灵订单），
        合成虚拟 OrderEvent 用于因子统计，但不入盘口。
        """
        # 处理买方委托
        if trade.bid_order_seq:
            bid_seq_num = book.order_no_index.get(trade.bid_order_seq)
            if bid_seq_num is None:
                # 幽灵订单：全成主动买单，无委托记录，从成交流反推（PDF §2 成交-委托对应性）
                if trade.bid_order_seq not in book.ghost_orders:
                    book.ghost_orders[trade.bid_order_seq] = trade.price
                    acc.add_order(OrderEvent(
                        seq_num      = trade.bid_order_seq,
                        side_str     = "bid",
                        price        = trade.price,
                        qty          = trade.qty,
                        ord_type     = "market",
                        timestamp_ns = trade.timestamp_ns,
                    ))
            else:
                book.reduce_order(bid_seq_num, trade.qty)

        # 处理卖方委托
        if trade.ask_order_seq:
            ask_seq_num = book.order_no_index.get(trade.ask_order_seq)
            if ask_seq_num is None:
                # 幽灵订单：全成主动卖单，无委托记录
                if trade.ask_order_seq not in book.ghost_orders:
                    book.ghost_orders[trade.ask_order_seq] = trade.price
                    acc.add_order(OrderEvent(
                        seq_num      = trade.ask_order_seq,
                        side_str     = "ask",
                        price        = trade.price,
                        qty          = trade.qty,
                        ord_type     = "market",
                        timestamp_ns = trade.timestamp_ns,
                    ))
            else:
                book.reduce_order(ask_seq_num, trade.qty)

        bs_flag = trade.trade_bs_flag or "N"
        acc.add_trade(TradeEvent(
            price        = trade.price,
            qty          = trade.qty,
            bs_flag      = bs_flag,
            bid_seq      = trade.bid_order_seq,
            ask_seq      = trade.ask_order_seq,
            timestamp_ns = trade.timestamp_ns,
            turnover     = trade.turnover,
        ))

    # ── 内部方法 ──────────────────────────────────────────────────────────────

    def _process_cancel_by_order(
        self,
        order: Order,
        book: OrderBook,
        acc: IntervalAccumulator,
    ) -> None:
        """
        处理上交所通过委托流下发的撤单指令（cancel_flag='D'）。

        撤单记录的 OrderNo 与被撤原始委托的 OrderNo 相同，
        通过 order_no_index 将 OrderNo 映射为 seq_num，再操作盘口。
        若 OrderNo 不存在（原始委托已全成或从未入盘口），则静默跳过。
        """
        # 优先通过 OrderNo 找到原始委托的 seq_num
        cancel_seq: Optional[int] = None
        if order.order_no is not None:
            cancel_seq = book.order_no_index.get(order.order_no)

        # 降级：若 order_no_index 无法匹配，尝试直接用 seq_num（兼容不含 OrderNo 的数据）
        if cancel_seq is None:
            if order.seq_num in book.order_index:
                cancel_seq = order.seq_num

        if cancel_seq is None:
            logger.debug(
                "SSE cancel: order_no=%s seq=%d not in order_no_index or order_index",
                order.order_no, order.seq_num,
            )
            # 异常监控 PDF §5：撤单找不到原始委托
            book.anomaly_count += 1
            acc.record_anomaly()
            return

        entry = book.order_index.get(cancel_seq)
        if entry is None:
            logger.debug("SSE cancel: seq_num=%d not in order_index", cancel_seq)
            return

        side, price = entry
        cancel_qty = order.qty

        acc.add_cancel(CancelEvent(
            seq_num      = cancel_seq,
            side_str     = "bid" if side == Side.BID else "ask",
            price        = price,
            qty          = cancel_qty,
            timestamp_ns = order.timestamp_ns,
        ))
        book.cancel_order(cancel_seq, cancel_qty)
