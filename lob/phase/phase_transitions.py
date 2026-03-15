"""
交易阶段切换处理器

核心职责：处理集合竞价结束 → 连续竞价开始的过渡。
此时需执行最终撮合，将撮合成交的委托从盘口移除，
剩余未成交委托留在盘口构成连续竞价的初始订单簿。
"""
from __future__ import annotations

import logging
from typing import List, Optional, Tuple

from lob.models.order_book import OrderBook
from lob.models.snapshot import TradeEvent
from lob.phase.phase_classifier import TradingPhase

logger = logging.getLogger(__name__)


class PhaseTransitionHandler:
    """处理交易阶段切换事件，主要是集合竞价 → 连续竞价的清算。"""

    def handle_auction_close(
        self,
        book: OrderBook,
        new_phase: TradingPhase,
        timestamp_ns: int,
    ) -> Tuple[int, int, List[TradeEvent]]:
        """
        执行集合竞价最终撮合并清算盘口。

        Parameters
        ----------
        book        : 当前订单簿（将被就地修改）
        new_phase   : 即将切换到的阶段（CONTINUOUS_AM 或 CONTINUOUS_PM 或 CLOSED）
        timestamp_ns: 切换事件时间戳

        Returns
        -------
        (match_price_int, match_qty, synthetic_trades)
            match_price_int : 开盘/收盘成交价（整数 × 10000），无成交时为 0
            match_qty       : 集合竞价成交量
            synthetic_trades: 由集合竞价撮合生成的合成 TradeEvent 列表
        """
        from lob.engine.auction_engine import compute_auction_match

        match_price, match_qty = compute_auction_match(book)
        synthetic_trades: List[TradeEvent] = []

        if match_qty > 0:
            # 清算：移除参与撮合的委托（价格交叉及等于撮合价的部分）
            remaining_bid_qty = match_qty
            remaining_ask_qty = match_qty

            # 消耗买方（从最优买价向下）
            for level in list(book.top_k_bids(k=len(book.bids))):
                if remaining_bid_qty <= 0:
                    break
                if level.price < match_price:
                    break   # 买价低于撮合价，不参与
                take = min(level.total_qty, remaining_bid_qty)
                consumed = book.consume_from_level(
                    side=__import__("lob.models.order", fromlist=["Side"]).Side.BID,
                    price=level.price,
                    qty=take,
                )
                remaining_bid_qty -= consumed

            # 消耗卖方（从最优卖价向上）
            for level in list(book.top_k_asks(k=len(book.asks))):
                if remaining_ask_qty <= 0:
                    break
                if level.price > match_price:
                    break   # 卖价高于撮合价，不参与
                take = min(level.total_qty, remaining_ask_qty)
                consumed = book.consume_from_level(
                    side=__import__("lob.models.order", fromlist=["Side"]).Side.ASK,
                    price=level.price,
                    qty=take,
                )
                remaining_ask_qty -= consumed

            # 生成合成成交事件
            synthetic_trades.append(TradeEvent(
                price        = match_price,
                qty          = match_qty,
                bs_flag      = "N",    # 集合竞价，无主买/主卖之分
                bid_seq      = None,
                ask_seq      = None,
                timestamp_ns = timestamp_ns,
            ))

        # 更新盘口缓存
        book.auction_match_price = match_price
        book.auction_match_qty   = match_qty

        logger.debug(
            "Auction closed for %s: price=%.4f qty=%d → %s",
            book.security_id,
            match_price / 10_000.0,
            match_qty,
            new_phase.name,
        )
        return match_price, match_qty, synthetic_trades
