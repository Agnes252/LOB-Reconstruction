"""
动态订单流因子（从区间累积器计算，反映本 50ms 内的挂/撤/成交活动）

对每个档位（level i，i=0 为最优档）统计：
- order_vol_ask/bid[i]  : 本区间在该价格档位的新增委托量
- match_vol_ask/bid[i]  : 本区间在该价格档位的成交量
- cancel_vol_ask/bid[i] : 本区间在该价格档位的撤单量

价格档位归属使用**快照末时刻**的盘口档位作为参考（与研究报告实践一致）。
"""
from __future__ import annotations

from typing import Dict, List

from config.settings import TOP_LEVELS
from lob.models.snapshot import IntervalAccumulator, LevelSnapshot, LOBSnapshot


def _price_to_level_idx(
    price: int,           # × 10000
    level_snapshots: List[LevelSnapshot],
) -> int:
    """
    将价格映射到档位索引（0 = 最优档）。
    若价格不在前 TOP_LEVELS 档内则返回 -1（不计入统计）。
    """
    for i, lvl in enumerate(level_snapshots):
        lvl_price_int = round(lvl.price * 10_000)
        if lvl_price_int == price:
            return i
    return -1


def compute_dynamic_factors(
    snap: LOBSnapshot,
    acc: IntervalAccumulator,
    ask_levels: List[LevelSnapshot],
    bid_levels: List[LevelSnapshot],
) -> None:
    """
    就地修改 snap，填充订单流因子。

    Parameters
    ----------
    snap       : 待填充的快照
    acc        : 本区间事件累积器
    ask_levels : 快照时刻的卖档盘口
    bid_levels : 快照时刻的买档盘口
    """
    order_vol_ask  = [0] * TOP_LEVELS
    order_vol_bid  = [0] * TOP_LEVELS
    match_vol_ask  = [0] * TOP_LEVELS
    match_vol_bid  = [0] * TOP_LEVELS
    cancel_vol_ask = [0] * TOP_LEVELS
    cancel_vol_bid = [0] * TOP_LEVELS

    # ── 新增委托量 ────────────────────────────────────────────────────────────
    for oe in acc.new_orders:
        if oe.side_str == "ask":
            idx = _price_to_level_idx(oe.price, ask_levels)
            if 0 <= idx < TOP_LEVELS:
                order_vol_ask[idx] += oe.qty
        else:
            idx = _price_to_level_idx(oe.price, bid_levels)
            if 0 <= idx < TOP_LEVELS:
                order_vol_bid[idx] += oe.qty

    # ── 成交量 ────────────────────────────────────────────────────────────────
    for te in acc.trades:
        # 成交同时影响买卖双方档位
        ask_idx = _price_to_level_idx(te.price, ask_levels)
        bid_idx = _price_to_level_idx(te.price, bid_levels)
        if 0 <= ask_idx < TOP_LEVELS:
            match_vol_ask[ask_idx] += te.qty
        if 0 <= bid_idx < TOP_LEVELS:
            match_vol_bid[bid_idx] += te.qty

    # ── 撤单量 ────────────────────────────────────────────────────────────────
    for ce in acc.cancel_events:
        if ce.side_str == "ask":
            idx = _price_to_level_idx(ce.price, ask_levels)
            if 0 <= idx < TOP_LEVELS:
                cancel_vol_ask[idx] += ce.qty
        else:
            idx = _price_to_level_idx(ce.price, bid_levels)
            if 0 <= idx < TOP_LEVELS:
                cancel_vol_bid[idx] += ce.qty

    snap.order_vol_ask  = order_vol_ask
    snap.order_vol_bid  = order_vol_bid
    snap.match_vol_ask  = match_vol_ask
    snap.match_vol_bid  = match_vol_bid
    snap.cancel_vol_ask = cancel_vol_ask
    snap.cancel_vol_bid = cancel_vol_bid
