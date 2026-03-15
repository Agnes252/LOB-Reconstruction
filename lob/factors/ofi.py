"""
订单流不平衡因子（Order Flow Imbalance, OFI）

PDF 参考：第 6 节「衍生指标 - 订单流不平衡(OFI)：反映买卖力量对比」

OFI 定义（Cont, Kukanov & Stoikov, 2014）
-----------------------------------------
在时间区间 [t-Δt, t] 内，以快照末时刻最优买/卖价作为参照档位：

    OFI = Σ ΔBid1_qty - Σ ΔAsk1_qty

其中：
    ΔBid1_qty > 0 ：新委托挂在买一档（增大买盘深度）
    ΔBid1_qty < 0 ：撤单或成交消耗买一档（减小买盘深度）
    ΔAsk1_qty 反向（增大卖盘深度时 ΔAsk < 0）

直观含义：
    OFI > 0 → 区间内买方力量净增强（更多买单进来或卖单被消耗）
    OFI < 0 → 卖方力量净增强
    OFI ≈ 0 → 买卖基本均衡

实现简化
--------
以快照末时刻的 best_bid / best_ask 价格为参照，扫描区间内的
OrderEvent / CancelEvent / TradeEvent，统计各自对最优档位深度的影响。

对于跨越多个档位的市价单成交，只统计实际成交价格落在最优档的那部分。
"""
from __future__ import annotations

from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from lob.models.snapshot import IntervalAccumulator, LOBSnapshot


def compute_ofi(snap: "LOBSnapshot", acc: "IntervalAccumulator") -> Optional[float]:
    """
    计算一个 50ms 区间的 OFI 值。

    Parameters
    ----------
    snap : 已填充盘口快照（asks/bids）的 LOBSnapshot
    acc  : 对应区间的 IntervalAccumulator

    Returns
    -------
    OFI 浮点值；若最优档价格不可用则返回 None。
    """
    # ── 取最优档价格（整数 × 10000）────────────────────────────────────────
    best_bid_px: Optional[int] = None
    best_ask_px: Optional[int] = None

    if snap.bids and snap.bids[0].price > 0:
        best_bid_px = int(round(snap.bids[0].price * 10_000))
    if snap.asks and snap.asks[0].price > 0:
        best_ask_px = int(round(snap.asks[0].price * 10_000))

    if best_bid_px is None and best_ask_px is None:
        return None

    ofi: float = 0.0

    # ── 新委托对最优档深度的贡献 ─────────────────────────────────────────
    for order in acc.new_orders:
        if order.side_str == "bid" and best_bid_px and order.price == best_bid_px:
            ofi += order.qty   # 买一档深度增加 → OFI 正向
        elif order.side_str == "ask" and best_ask_px and order.price == best_ask_px:
            ofi -= order.qty   # 卖一档深度增加 → OFI 负向

    # ── 撤单对最优档深度的影响 ───────────────────────────────────────────
    for cancel in acc.cancel_events:
        if cancel.side_str == "bid" and best_bid_px and cancel.price == best_bid_px:
            ofi -= cancel.qty  # 买一档深度减小 → OFI 负向
        elif cancel.side_str == "ask" and best_ask_px and cancel.price == best_ask_px:
            ofi += cancel.qty  # 卖一档深度减小 → OFI 正向

    # ── 成交对最优档深度的影响 ───────────────────────────────────────────
    # 主买成交（BSFlag='B'）：消耗卖一档 → OFI 正向（卖盘变薄）
    # 主卖成交（BSFlag='S'）：消耗买一档 → OFI 负向（买盘变薄）
    # 集合竞价（BSFlag='N'）：双边消耗，OFI 贡献为 0
    for trade in acc.trades:
        if trade.bs_flag == "B" and best_ask_px and trade.price == best_ask_px:
            ofi += trade.qty   # 卖一被主买消耗，买方力量强
        elif trade.bs_flag == "S" and best_bid_px and trade.price == best_bid_px:
            ofi -= trade.qty   # 买一被主卖消耗，卖方力量强

    return ofi


def compute_ofi_normalized(
    snap: "LOBSnapshot",
    acc: "IntervalAccumulator",
) -> Optional[float]:
    """
    归一化 OFI：除以最优档总深度，限制在 [-1, 1] 之间。

    适用于跨股票或跨时间段的因子比较。
    归一化分母 = best_bid_vol + best_ask_vol
    """
    raw = compute_ofi(snap, acc)
    if raw is None:
        return None

    bid_vol = snap.bids[0].volume if snap.bids else 0
    ask_vol = snap.asks[0].volume if snap.asks else 0
    denom = bid_vol + ask_vol

    if denom == 0:
        return None

    return max(-1.0, min(1.0, raw / denom))
