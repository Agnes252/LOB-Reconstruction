"""
LOB 因子计算单元测试

验证：
- SheetDiff 公式
- MatchDiff/OrderDiff/CancelDiff 公式
- 极端情况（全零分母）
"""
import sys
sys.path.insert(0, r"d:\LOB")

import pytest
from lob.factors.derived_indicators import compute_derived_indicators
from lob.factors.static_factors import compute_static_factors
from lob.models.snapshot import LevelSnapshot, LOBSnapshot


def make_snap(bid1_vol=1000, ask1_vol=500, bid_px=196500, ask_px=196700):
    snap = LOBSnapshot(
        security_id  = "000001",
        timestamp_ms = 34200000,
        phase        = 3,
    )
    snap.bids[0] = LevelSnapshot(price=bid_px / 10000, volume=bid1_vol, count=5)
    snap.asks[0] = LevelSnapshot(price=ask_px / 10000, volume=ask1_vol, count=3)
    return snap


class TestStaticFactors:

    def test_mid_price(self):
        snap = make_snap(bid_px=196500, ask_px=196700)
        compute_static_factors(snap)
        assert snap.mid_price == pytest.approx((196500 + 196700) / 2.0 / 10000.0)

    def test_spread(self):
        snap = make_snap(bid_px=196500, ask_px=196700)
        compute_static_factors(snap)
        assert snap.spread == pytest.approx((196700 - 196500) / 10000.0)

    def test_sheet_diff_buy_heavy(self):
        """买方量大 → sheet_diff > 0"""
        snap = make_snap(bid1_vol=1000, ask1_vol=200)
        compute_static_factors(snap)
        expected = (1000 - 200) / (1000 + 200)
        assert snap.sheet_diff == pytest.approx(expected)

    def test_sheet_diff_sell_heavy(self):
        """卖方量大 → sheet_diff < 0"""
        snap = make_snap(bid1_vol=200, ask1_vol=1000)
        compute_static_factors(snap)
        expected = (200 - 1000) / (200 + 1000)
        assert snap.sheet_diff == pytest.approx(expected)

    def test_sheet_diff_balanced(self):
        snap = make_snap(bid1_vol=500, ask1_vol=500)
        compute_static_factors(snap)
        assert snap.sheet_diff == pytest.approx(0.0)

    def test_zero_volumes(self):
        snap = make_snap(bid1_vol=0, ask1_vol=0)
        compute_static_factors(snap)
        assert snap.sheet_diff is None


class TestDerivedIndicators:

    def test_match_diff_buy_active(self):
        """买方成交量大 → match_diff > 0"""
        snap = make_snap()
        snap.match_vol_bid[0] = 500
        snap.match_vol_ask[0] = 100
        # 需要设置分母（tot_vol）
        snap.order_vol_bid[0] = 100
        snap.order_vol_ask[0] = 100
        compute_derived_indicators(snap)
        assert snap.match_diff is not None
        assert snap.match_diff > 0

    def test_zero_tot_vol(self):
        """分母为零时所有衍生指标应为 None"""
        snap = make_snap()
        # 所有流量均为 0（默认）
        compute_derived_indicators(snap)
        assert snap.match_diff is None
        assert snap.order_diff is None
        assert snap.cancel_diff is None

    def test_order_diff_symmetry(self):
        """买卖挂单量相同时 order_diff 应接近 0"""
        snap = make_snap()
        snap.order_vol_bid[0] = 300
        snap.order_vol_ask[0] = 300
        snap.match_vol_bid[0] = 100
        snap.match_vol_ask[0] = 100
        compute_derived_indicators(snap)
        assert snap.order_diff == pytest.approx(0.0, abs=1e-9)
