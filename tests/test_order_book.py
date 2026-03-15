"""
订单簿核心数据结构单元测试

验证：
- SortedDict 负键买盘排序（买一在最前）
- FIFO 消耗顺序
- O(1) 撤单查找
- 档位自动创建/删除
"""
import sys
sys.path.insert(0, r"d:\LOB")

import pytest
from lob.models.order import Side
from lob.models.order_book import OrderBook


class TestOrderBook:

    def setup_method(self):
        self.book = OrderBook(security_id="000001")

    def test_add_bid_and_best_bid(self):
        self.book.add_order(1, Side.BID, 196500, 1000)
        self.book.add_order(2, Side.BID, 196600, 500)
        # 买一应为价格更高的 19.66
        assert self.book.best_bid() == 196600

    def test_add_ask_and_best_ask(self):
        self.book.add_order(1, Side.ASK, 196700, 1000)
        self.book.add_order(2, Side.ASK, 196600, 500)
        # 卖一应为价格更低的 19.66
        assert self.book.best_ask() == 196600

    def test_bid_descending_order(self):
        prices = [196500, 196700, 196600]
        for i, p in enumerate(prices, start=1):
            self.book.add_order(i, Side.BID, p, 100)
        bids = self.book.top_k_bids(3)
        assert [lvl.price for lvl in bids] == [196700, 196600, 196500]

    def test_ask_ascending_order(self):
        prices = [196700, 196500, 196600]
        for i, p in enumerate(prices, start=1):
            self.book.add_order(i, Side.ASK, p, 100)
        asks = self.book.top_k_asks(3)
        assert [lvl.price for lvl in asks] == [196500, 196600, 196700]

    def test_cancel_removes_from_book(self):
        self.book.add_order(1, Side.BID, 196500, 1000)
        assert self.book.best_bid() == 196500
        self.book.cancel_order(1)
        assert self.book.best_bid() is None
        assert 1 not in self.book.order_index

    def test_cancel_partial(self):
        self.book.add_order(1, Side.BID, 196500, 1000)
        self.book.cancel_order(1, 300)
        level = self.book.top_k_bids(1)[0]
        assert level.total_qty == 700

    def test_cancel_nonexistent_returns_false(self):
        result = self.book.cancel_order(999)
        assert result is False

    def test_fifo_consume(self):
        """FIFO：先到的委托先被消耗"""
        self.book.add_order(1, Side.ASK, 196500, 300)   # 先来
        self.book.add_order(2, Side.ASK, 196500, 500)   # 后来
        consumed = self.book.consume_from_level(Side.ASK, 196500, 400)
        assert consumed == 400
        level = self.book.top_k_asks(1)[0]
        # 委托1（300）全成，委托2（500）剩余 400
        assert level.total_qty == 400
        assert level.order_count == 1  # 委托1已移除

    def test_mid_price(self):
        self.book.add_order(1, Side.BID, 196500, 100)
        self.book.add_order(2, Side.ASK, 196700, 100)
        assert self.book.mid_price() == pytest.approx((196500 + 196700) / 2.0 / 10000.0)

    def test_spread(self):
        self.book.add_order(1, Side.BID, 196500, 100)
        self.book.add_order(2, Side.ASK, 196700, 100)
        assert self.book.spread() == pytest.approx((196700 - 196500) / 10000.0)

    def test_order_index_updated_on_cancel(self):
        self.book.add_order(1, Side.BID, 196500, 100)
        assert 1 in self.book.order_index
        self.book.cancel_order(1)
        assert 1 not in self.book.order_index

    def test_level_removed_when_empty(self):
        self.book.add_order(1, Side.BID, 196500, 100)
        self.book.cancel_order(1)
        # 该价格档位应已被删除
        assert len(self.book.bids) == 0
