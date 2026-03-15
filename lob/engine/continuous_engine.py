"""
连续竞价期间的撮合处理（通用逻辑）

连续竞价期间只负责维护订单簿状态；
实际的成交信息来自交易所下发的逐笔成交记录（不自行撮合）。
本模块仅处理市价单在到达时的即时撮合模拟。
"""
from __future__ import annotations

import logging
from typing import List, Tuple

from lob.models.order import Order, OrdType, Side
from lob.models.order_book import OrderBook
from lob.models.snapshot import TradeEvent

logger = logging.getLogger(__name__)


def simulate_market_order(
    order: Order,
    book: OrderBook,
    timestamp_ns: int,
    max_levels: int = 50,
) -> List[TradeEvent]:
    """
    模拟市价单（IOC）扫单，按 FIFO 从对手方档位依次消耗。

    深交所市价单 OrdType='1'，price=0 或 -1。
    余量丢弃（IOC 规则）。

    Parameters
    ----------
    order       : 市价委托
    book        : 当前订单簿（将被就地修改）
    timestamp_ns: 事件时间戳
    max_levels  : 最多扫对手方几个档位

    Returns
    -------
    合成成交事件列表
    """
    fills: List[TradeEvent] = []
    remaining = order.remaining

    if order.side == Side.BID:
        # 买入市价单：消耗卖方档位
        levels = book.top_k_asks(max_levels)
        for level in levels:
            if remaining <= 0:
                break
            fill_qty = min(remaining, level.total_qty)
            consumed = book.consume_from_level(Side.ASK, level.price, fill_qty)
            if consumed > 0:
                fills.append(TradeEvent(
                    price        = level.price,
                    qty          = consumed,
                    bs_flag      = "B",
                    bid_seq      = order.seq_num,
                    ask_seq      = None,
                    timestamp_ns = timestamp_ns,
                ))
                remaining -= consumed
    else:
        # 卖出市价单：消耗买方档位
        levels = book.top_k_bids(max_levels)
        for level in levels:
            if remaining <= 0:
                break
            fill_qty = min(remaining, level.total_qty)
            consumed = book.consume_from_level(Side.BID, level.price, fill_qty)
            if consumed > 0:
                fills.append(TradeEvent(
                    price        = level.price,
                    qty          = consumed,
                    bs_flag      = "S",
                    bid_seq      = None,
                    ask_seq      = order.seq_num,
                    timestamp_ns = timestamp_ns,
                ))
                remaining -= consumed

    if remaining > 0:
        logger.debug(
            "Market order %d (seq=%d) partially unfilled: %d shares discarded",
            order.seq_num, order.seq_num, remaining,
        )

    return fills


def simulate_best5_order(
    order: Order,
    book: OrderBook,
    timestamp_ns: int,
) -> List[TradeEvent]:
    """
    保留向后兼容：深交所 OrdType='U' 此前命名为 BEST5，现已更名为 OWN_BEST。
    OWN_BEST 实际逻辑在 szse_engine._process_own_best 中处理（挂在同侧最优价），
    此函数仅作兼容占位，不应在新代码中直接调用。
    """
    return simulate_market_order(order, book, timestamp_ns, max_levels=5)
