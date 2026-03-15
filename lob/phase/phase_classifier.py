"""
交易阶段分类器

根据时间戳（纳秒）推算当前交易阶段，与交易所无关（沪深规则相同）。
快照行情含显式阶段代码，逐笔行情仅有时间戳，通过此模块推算。

交易阶段枚举值与 ob_workflow.md 内部编码完全对应：
    0  启动（开市前）
    1  开盘集合竞价
    2  开盘集合竞价后休市
    3  连续竞价（上午）
    4  中午休市
    5  连续竞价（下午）
    6  收盘集合竞价
    7  盘后交易
    8  已闭市
    9  波动性中断（不可通过时间戳推算，由引擎显式设置）
    10 停牌（不可通过时间戳推算，由引擎显式设置）
    11 熔断时段（不可通过时间戳推算，由引擎显式设置）
"""
from __future__ import annotations

from enum import IntEnum

from config.settings import PHASE_BOUNDARIES


class TradingPhase(IntEnum):
    PRE_OPEN           = 0
    OPENING_AUCTION    = 1
    POST_AUCTION_BREAK = 2
    CONTINUOUS_AM      = 3
    LUNCH_BREAK        = 4
    CONTINUOUS_PM      = 5
    CLOSING_AUCTION    = 6
    AFTER_HOURS        = 7
    CLOSED             = 8
    VOLATILITY_BREAK   = 9
    HALTED             = 10
    CIRCUIT_BREAK      = 11


# 以秒数为界的有序规则列表（从 settings.py 派生）
_PB = PHASE_BOUNDARIES
_RULES = [
    # (起始秒数含, 结束秒数不含, TradingPhase)
    (0,                                _PB["opening_auction_start"],  TradingPhase.PRE_OPEN),
    (_PB["opening_auction_start"],     _PB["opening_auction_end"],    TradingPhase.OPENING_AUCTION),
    (_PB["opening_auction_end"],       _PB["continuous_am_start"],    TradingPhase.POST_AUCTION_BREAK),
    (_PB["continuous_am_start"],       _PB["continuous_am_end"],      TradingPhase.CONTINUOUS_AM),
    (_PB["continuous_am_end"],         _PB["continuous_pm_start"],    TradingPhase.LUNCH_BREAK),
    (_PB["continuous_pm_start"],       _PB["closing_auction_start"],  TradingPhase.CONTINUOUS_PM),
    (_PB["closing_auction_start"],     _PB["market_close"],           TradingPhase.CLOSING_AUCTION),
    (_PB["market_close"],              _PB["after_hours_start"],      TradingPhase.CLOSED),
    (_PB["after_hours_start"],         _PB["after_hours_end"],        TradingPhase.AFTER_HOURS),
    (_PB["after_hours_end"],           86400,                         TradingPhase.CLOSED),
]


class PhaseClassifier:
    """
    将事件时间戳（纳秒）转换为 TradingPhase。

    线程安全：无状态，可在多股票场景中共享单一实例。
    """

    @staticmethod
    def classify(timestamp_ns: int) -> TradingPhase:
        """
        根据时间戳推算交易阶段。
        timestamp_ns 为自午夜起的纳秒数。
        """
        ts_sec = timestamp_ns // 1_000_000_000
        for start, end, phase in _RULES:
            if start <= ts_sec < end:
                return phase
        return TradingPhase.CLOSED

    @staticmethod
    def is_auction(phase: TradingPhase) -> bool:
        return phase in (TradingPhase.OPENING_AUCTION,
                         TradingPhase.CLOSING_AUCTION)

    @staticmethod
    def is_continuous(phase: TradingPhase) -> bool:
        return phase in (TradingPhase.CONTINUOUS_AM,
                         TradingPhase.CONTINUOUS_PM)

    @staticmethod
    def is_break(phase: TradingPhase) -> bool:
        return phase in (TradingPhase.PRE_OPEN,
                         TradingPhase.POST_AUCTION_BREAK,
                         TradingPhase.LUNCH_BREAK,
                         TradingPhase.CLOSED,
                         TradingPhase.AFTER_HOURS)
