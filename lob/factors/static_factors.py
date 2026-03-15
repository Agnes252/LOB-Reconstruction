"""
静态盘口因子（从当前盘口快照直接计算，无需历史数据）

- mid_price  : (ask1 + bid1) / 2
- spread     : ask1 - bid1
- sheet_diff : (VB1 - VA1) / (VB1 + VA1)  盘口相对强弱（海通 §3.2 公式1）
"""
from __future__ import annotations

from lob.models.snapshot import LOBSnapshot


def compute_static_factors(snap: LOBSnapshot) -> None:
    """就地修改 snap，填充所有静态盘口因子。"""
    if not snap.asks or not snap.bids:
        return

    a1 = snap.asks[0]
    b1 = snap.bids[0]

    if a1.price > 0 and b1.price > 0:
        snap.mid_price = (a1.price + b1.price) / 2.0
        snap.spread    = a1.price - b1.price

        denom = float(a1.volume + b1.volume)
        if denom > 0:
            snap.sheet_diff = (b1.volume - a1.volume) / denom
