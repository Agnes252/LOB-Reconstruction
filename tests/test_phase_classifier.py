"""
交易阶段分类器单元测试
"""
import sys
sys.path.insert(0, r"d:\LOB")

import pytest
from lob.phase.phase_classifier import PhaseClassifier, TradingPhase


class TestPhaseClassifier:

    def _ns(self, h, m=0, s=0, ms=0):
        """构造午夜起纳秒时间戳"""
        return ((h * 3600 + m * 60 + s) * 1000 + ms) * 1_000_000

    def test_pre_open(self):
        assert PhaseClassifier.classify(self._ns(9, 0)) == TradingPhase.PRE_OPEN

    def test_opening_auction_start(self):
        assert PhaseClassifier.classify(self._ns(9, 15)) == TradingPhase.OPENING_AUCTION

    def test_opening_auction_end_boundary(self):
        # 9:25:00 进入 post_auction_break
        assert PhaseClassifier.classify(self._ns(9, 25)) == TradingPhase.POST_AUCTION_BREAK

    def test_continuous_am_start(self):
        assert PhaseClassifier.classify(self._ns(9, 30)) == TradingPhase.CONTINUOUS_AM

    def test_continuous_am_mid(self):
        assert PhaseClassifier.classify(self._ns(10, 30)) == TradingPhase.CONTINUOUS_AM

    def test_lunch_break(self):
        assert PhaseClassifier.classify(self._ns(11, 30)) == TradingPhase.LUNCH_BREAK

    def test_continuous_pm_start(self):
        assert PhaseClassifier.classify(self._ns(13, 0)) == TradingPhase.CONTINUOUS_PM

    def test_closing_auction(self):
        assert PhaseClassifier.classify(self._ns(14, 57)) == TradingPhase.CLOSING_AUCTION

    def test_closed(self):
        assert PhaseClassifier.classify(self._ns(15, 0)) == TradingPhase.CLOSED

    def test_is_auction(self):
        assert PhaseClassifier.is_auction(TradingPhase.OPENING_AUCTION)
        assert PhaseClassifier.is_auction(TradingPhase.CLOSING_AUCTION)
        assert not PhaseClassifier.is_auction(TradingPhase.CONTINUOUS_AM)

    def test_is_continuous(self):
        assert PhaseClassifier.is_continuous(TradingPhase.CONTINUOUS_AM)
        assert PhaseClassifier.is_continuous(TradingPhase.CONTINUOUS_PM)
        assert not PhaseClassifier.is_continuous(TradingPhase.OPENING_AUCTION)
