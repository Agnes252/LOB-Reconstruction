"""
LOB 四大衍生指标（海通研究报告 §3.2 完整公式实现）

SheetDiff  = (VB₁ - VA₁) / (VB₁ + VA₁)
    订单簿相对强弱（静态因子，已在 static_factors 中计算）

MatchDiff  = (MatchVol_B₁ - MatchVol_A₁) / TotVol
    成交相对强弱，TotVol = 所有委托+撤单+成交量之和

OrderDiff  = (OrderVol_B₁ - OrderVol_A₁) / TotVol_B
    挂单相对强弱，TotVol_B = 买方总活动量

CancelDiff = (CancelVol_B₁ - CancelVol_A₁) / TotVol_B
    撤单相对强弱
"""
from __future__ import annotations

from lob.models.snapshot import LOBSnapshot


def compute_derived_indicators(snap: LOBSnapshot) -> None:
    """就地修改 snap，填充四大衍生指标。SheetDiff 已由 static_factors 填充。"""

    # ── 计算分母 TotVol ───────────────────────────────────────────────────────
    # TotVol = Σ(OrderVol_A) + Σ(OrderVol_B) + Σ(CancelVol_A)
    #        + Σ(CancelVol_B) + Σ(MatchVol_A) + Σ(MatchVol_B)
    tot_vol = (
        sum(snap.order_vol_ask)   + sum(snap.order_vol_bid)
      + sum(snap.cancel_vol_ask)  + sum(snap.cancel_vol_bid)
      + sum(snap.match_vol_ask)   + sum(snap.match_vol_bid)
    )

    # ── TotVol_B = 买方活动量 ────────────────────────────────────────────────
    tot_vol_b = (
        sum(snap.order_vol_bid)
      + sum(snap.cancel_vol_bid)
      + sum(snap.match_vol_bid)
    )

    # ── MatchDiff ─────────────────────────────────────────────────────────────
    if tot_vol > 0:
        mv_b1 = snap.match_vol_bid[0] if snap.match_vol_bid else 0
        mv_a1 = snap.match_vol_ask[0] if snap.match_vol_ask else 0
        snap.match_diff = (mv_b1 - mv_a1) / tot_vol

    # ── OrderDiff ─────────────────────────────────────────────────────────────
    if tot_vol_b > 0:
        ov_b1 = snap.order_vol_bid[0] if snap.order_vol_bid else 0
        ov_a1 = snap.order_vol_ask[0] if snap.order_vol_ask else 0
        snap.order_diff = (ov_b1 - ov_a1) / tot_vol_b

    # ── CancelDiff ───────────────────────────────────────────────────────────
    if tot_vol_b > 0:
        cv_b1 = snap.cancel_vol_bid[0] if snap.cancel_vol_bid else 0
        cv_a1 = snap.cancel_vol_ask[0] if snap.cancel_vol_ask else 0
        snap.cancel_diff = (cv_b1 - cv_a1) / tot_vol_b
