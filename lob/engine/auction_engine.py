"""
集合竞价撮合算法（纯函数，不修改订单簿）

完整实现 ob_workflow.md §4 描述的算法：
1. 查找最低卖价/最高买价
2. 无交叉 → 撮合价=0
3. 有交叉 → 循环撮合
4. 生成临时快照（阶段未切换）或正常快照（阶段已切换）
"""
from __future__ import annotations

from typing import List, Tuple

from lob.models.order_book import OrderBook
from lob.models.snapshot import LevelSnapshot


def compute_auction_match(book: OrderBook) -> Tuple[int, int]:
    """
    计算当前订单簿的集合竞价撮合价格和数量。

    此函数是纯函数：仅读取 book，不修改任何状态。
    实际盘口清算在 PhaseTransitionHandler 中执行。

    Returns
    -------
    (match_price_int, match_qty)
        match_price_int : 撮合成交价（× 10000），无成交时为 0
        match_qty       : 撮合成交数量，无成交时为 0
    """
    best_bid = book.best_bid()
    best_ask = book.best_ask()

    # Step 2.1: 双侧均无委托
    if best_bid is None and best_ask is None:
        return 0, 0

    # Step 2.2: 只有一侧有委托
    if best_bid is None:
        return best_ask, 0   # type: ignore[return-value]
    if best_ask is None:
        return best_bid, 0

    # Step 2.3.1: 无交叉（最高买价 < 最低卖价）
    if best_bid < best_ask:
        return 0, 0

    # Step 3: 有交叉，执行撮合循环（在副本上操作）
    bid_levels = [
        (lvl.price, lvl.total_qty)
        for lvl in book.top_k_bids(k=book.total_bid_levels())
    ]
    ask_levels = [
        (lvl.price, lvl.total_qty)
        for lvl in book.top_k_asks(k=book.total_ask_levels())
    ]

    bi = ai = 0
    total_matched = 0
    final_price = 0

    while bi < len(bid_levels) and ai < len(ask_levels):
        bp, bq = bid_levels[bi]
        ap, aq = ask_levels[ai]

        # Step 4.1: 无交叉则退出
        if bp < ap:
            break

        # Step 4.2: 匹配量 = 双方中的较小值
        match_vol = min(bq, aq)
        total_matched += match_vol

        # Step 4.4: 双方减去成交量
        bq -= match_vol
        aq -= match_vol
        bid_levels[bi] = (bp, bq)
        ask_levels[ai] = (ap, aq)

        # Step 4.5 / 4.6: 确定成交价
        if bq == 0 and aq == 0:
            # 双方同时耗尽：成交价取均价（整数除法取整，精确到 tick）
            final_price = (bp + ap) // 2
            bi += 1
            ai += 1
        elif bq == 0:
            # 买方耗尽：成交价 = 买方价格（卖方仍有余量）
            final_price = bp
            bi += 1
        else:
            # 卖方耗尽：成交价 = 卖方价格（买方仍有余量）
            final_price = ap
            ai += 1

    return final_price, total_matched


def build_auction_snapshot(
    book: OrderBook,
    phase_changed: bool,
    top_k: int = 10,
) -> Tuple[List[LevelSnapshot], List[LevelSnapshot]]:
    """
    生成集合竞价期间的盘口快照。

    phase_changed=False（阶段未切换）：
        按 ob_workflow.md §5.1 揭示 2 档虚拟信息。
    phase_changed=True（阶段已切换）：
        揭示真实十档盘口（盘口清算后的剩余委托）。

    Returns
    -------
    (bid_levels, ask_levels) 各为 LevelSnapshot 列表
    """
    if phase_changed:
        # 正常快照：直接读取盘口
        bids = [
            LevelSnapshot(price=lvl.price / 10_000.0,
                          volume=lvl.total_qty,
                          count=lvl.order_count)
            for lvl in book.top_k_bids(top_k)
        ]
        asks = [
            LevelSnapshot(price=lvl.price / 10_000.0,
                          volume=lvl.total_qty,
                          count=lvl.order_count)
            for lvl in book.top_k_asks(top_k)
        ]
        # 补齐至 top_k 档
        while len(bids) < top_k:
            bids.append(LevelSnapshot.empty())
        while len(asks) < top_k:
            asks.append(LevelSnapshot.empty())
        return bids, asks

    # 临时撮合快照（§5.1）
    match_price = book.auction_match_price
    match_qty   = book.auction_match_qty

    empty_levels = [LevelSnapshot.empty() for _ in range(top_k)]

    if match_qty == 0:
        # §5.1.2: 无匹配成交，不揭示任何档位
        return list(empty_levels), list(empty_levels)

    # §5.1.2~5.1.4: 揭示买一=卖一=撮合价+成交量；买二=卖二=0价格+剩余量
    match_px_float = match_price / 10_000.0

    # 撮合后最优档剩余量（某方应为 0）
    best_bid_level = book.top_k_bids(1)
    best_ask_level = book.top_k_asks(1)
    bid_residual = best_bid_level[0].total_qty if best_bid_level else 0
    ask_residual = best_ask_level[0].total_qty if best_ask_level else 0

    bids = [
        LevelSnapshot(price=match_px_float, volume=match_qty, count=0),
        LevelSnapshot(price=0.0, volume=bid_residual, count=0),
    ] + [LevelSnapshot.empty() for _ in range(top_k - 2)]

    asks = [
        LevelSnapshot(price=match_px_float, volume=match_qty, count=0),
        LevelSnapshot(price=0.0, volume=ask_residual, count=0),
    ] + [LevelSnapshot.empty() for _ in range(top_k - 2)]

    return bids, asks
