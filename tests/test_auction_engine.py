"""
集合竞价撮合算法单元测试

对照 ob_workflow.md §4 手工推导结果验证：
1. 无委托 → 成交价=0
2. 单侧有委托 → 成交价=该侧，数量=0
3. 无交叉 → 成交价=0
4. 有交叉各种场景
"""
import sys
sys.path.insert(0, r"d:\LOB")

import pytest
from lob.engine.auction_engine import compute_auction_match
from lob.models.order import Side
from lob.models.order_book import OrderBook


def make_book(bids, asks):
    """
    快速构建订单簿。
    bids / asks: [(price_int, qty), ...]
    """
    book = OrderBook(security_id="TEST")
    seq = 1
    for price, qty in bids:
        book.add_order(seq, Side.BID, price, qty)
        seq += 1
    for price, qty in asks:
        book.add_order(seq, Side.ASK, price, qty)
        seq += 1
    return book


class TestComputeAuctionMatch:

    def test_empty_book(self):
        book = OrderBook(security_id="TEST")
        price, qty = compute_auction_match(book)
        assert price == 0
        assert qty == 0

    def test_only_bids(self):
        book = make_book(bids=[(196500, 1000)], asks=[])
        price, qty = compute_auction_match(book)
        assert price == 196500
        assert qty == 0

    def test_only_asks(self):
        book = make_book(bids=[], asks=[(196500, 1000)])
        price, qty = compute_auction_match(book)
        assert price == 196500
        assert qty == 0

    def test_no_cross(self):
        """买一 < 卖一：无交叉"""
        book = make_book(
            bids=[(196400, 1000)],
            asks=[(196500, 1000)],
        )
        price, qty = compute_auction_match(book)
        assert price == 0
        assert qty == 0

    def test_simple_cross_bid_larger(self):
        """买方量 > 卖方量：成交价 = 卖方价格（卖方耗尽）"""
        book = make_book(
            bids=[(196500, 2000)],   # 买1：19.65 x 2000
            asks=[(196500, 500)],    # 卖1：19.65 x 500
        )
        price, qty = compute_auction_match(book)
        assert qty == 500
        assert price == 196500

    def test_simple_cross_ask_larger(self):
        """卖方量 > 买方量：成交价 = 买方价格（买方耗尽）"""
        book = make_book(
            bids=[(196500, 300)],
            asks=[(196500, 800)],
        )
        price, qty = compute_auction_match(book)
        assert qty == 300
        assert price == 196500

    def test_equal_quantities(self):
        """双方量相等：成交价 = 均价（整数除法）"""
        book = make_book(
            bids=[(196600, 1000)],   # 买1：19.66
            asks=[(196400, 1000)],   # 卖1：19.64
        )
        price, qty = compute_auction_match(book)
        assert qty == 1000
        # 均价 (196600 + 196400) / 2 = 196500
        assert price == 196500

    def test_multi_level_cross(self):
        """多档交叉撮合"""
        book = make_book(
            bids=[(196600, 500), (196500, 800)],   # 买1=19.66 x500, 买2=19.65 x800
            asks=[(196400, 600), (196500, 400)],   # 卖1=19.64 x600, 卖2=19.65 x400
        )
        price, qty = compute_auction_match(book)
        assert qty > 0

    def test_exact_cross_at_one_price(self):
        """买卖价格完全相同，双方量不等"""
        book = make_book(
            bids=[(196500, 1000)],
            asks=[(196500, 600)],
        )
        price, qty = compute_auction_match(book)
        assert qty == 600
        assert price == 196500
