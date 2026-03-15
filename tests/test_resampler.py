"""
50ms 重采样器单元测试

验证：
- 50ms 边界检测
- 跨多个边界一次到达
- carry-forward（静默区间）
- 日终 flush
"""
import sys
sys.path.insert(0, r"d:\LOB")

import pytest
from lob.models.order import Exchange, Order, OrdType, OrderStatus, Side
from lob.models.order_book import OrderBook
from lob.phase.phase_classifier import TradingPhase
from lob.resampler.resampler import LOBResampler


def make_order(seq, ts_ms, price=196500, qty=100, side=Side.BID):
    return Order(
        seq_num      = seq,
        security_id  = "000001",
        exchange     = Exchange.SZSE,
        timestamp_ns = ts_ms * 1_000_000,
        price        = price,
        qty          = qty,
        remaining    = qty,
        side         = side,
        ord_type     = OrdType.LIMIT,
        status       = OrderStatus.ACTIVE,
    )


class TestLOBResampler:

    def setup_method(self):
        self.book = OrderBook(security_id="000001")
        self.resampler = LOBResampler(
            security_id   = "000001",
            date_unix_ms  = 0,
            start_phase_ms= 9 * 3600 * 1000,  # 9:00:00
            end_phase_ms  = 15 * 3600 * 1000,
        )
        self.phase = TradingPhase.CONTINUOUS_AM

    def test_no_boundary_crossed(self):
        """事件未越过边界，不应生成快照"""
        t_ms = 9 * 3600 * 1000 + 10   # 9:00:00.010
        order = make_order(1, t_ms)
        self.book.add_order(1, Side.BID, 196500, 100)
        self.resampler.acc.add_order(__import__("lob.models.snapshot", fromlist=["OrderEvent"]).OrderEvent(
            seq_num=1, side_str="bid", price=196500, qty=100, ord_type="limit",
            timestamp_ns=t_ms * 1_000_000
        ))
        snaps = self.resampler.ingest(t_ms * 1_000_000, order, self.book, self.phase)
        assert len(snaps) == 0

    def test_one_boundary_crossed(self):
        """越过一个 50ms 边界，应生成 1 个快照"""
        t1_ms = 9 * 3600 * 1000 + 10   # 9:00:00.010（第一个区间内）
        t2_ms = 9 * 3600 * 1000 + 60   # 9:00:00.060（越过第一个边界 9:00:00.050）
        order1 = make_order(1, t1_ms)
        order2 = make_order(2, t2_ms)

        self.resampler.ingest(t1_ms * 1_000_000, order1, self.book, self.phase)
        snaps = self.resampler.ingest(t2_ms * 1_000_000, order2, self.book, self.phase)
        assert len(snaps) == 1

    def test_multiple_boundaries_crossed(self):
        """一次性跨越多个 50ms 边界（静默区间后来了一个事件）"""
        t1_ms = 9 * 3600 * 1000 + 10    # 9:00:00.010
        t2_ms = 9 * 3600 * 1000 + 310   # 9:00:00.310（跨越了 6 个边界）
        order1 = make_order(1, t1_ms)
        order2 = make_order(2, t2_ms)

        self.resampler.ingest(t1_ms * 1_000_000, order1, self.book, self.phase)
        snaps = self.resampler.ingest(t2_ms * 1_000_000, order2, self.book, self.phase)
        assert len(snaps) == 6   # 共跨越 50,100,150,200,250,300 这 6 个边界

    def test_flush_emits_last_interval(self):
        """日终 flush 应输出最后一个区间的快照"""
        t_ms = 9 * 3600 * 1000 + 10
        order = make_order(1, t_ms)
        self.resampler.ingest(t_ms * 1_000_000, order, self.book, self.phase)
        snap = self.resampler.flush(self.book, self.phase)
        assert snap is not None
        assert snap.security_id == "000001"

    def test_snapshot_timestamp_aligned_to_50ms(self):
        """快照时间戳应对齐到 50ms 网格"""
        t_ms = 9 * 3600 * 1000 + 73   # 不对齐
        order = make_order(1, t_ms)
        t_ms2 = 9 * 3600 * 1000 + 105
        order2 = make_order(2, t_ms2)

        self.resampler.ingest(t_ms * 1_000_000, order, self.book, self.phase)
        snaps = self.resampler.ingest(t_ms2 * 1_000_000, order2, self.book, self.phase)
        for s in snaps:
            assert s.timestamp_ms % 50 == 0, f"时间戳未对齐: {s.timestamp_ms}"
