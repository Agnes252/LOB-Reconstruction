"""
50ms 重采样器

PDF 参考：第 5 节「时间戳对齐」、第 6 节「项目结果：重构好的 LOB 样式」

核心逻辑：
- 维护一个 50ms 时间网格
- 每个事件到来时检查是否越过边界
- 越过边界则生成快照（含因子计算），重置累积器
- 静默区间（无事件）执行 carry-forward：拷贝上一时刻盘口，订单流清零

新增（相比原版）：
- OFI 计算（lob/factors/ofi.py）
- 累计成交量/金额 cum_volume / cum_turnover（全天）
- 最新成交价 last_price（carry-forward）
- 异常标记 is_anomaly / anomaly_count（从 acc 传播到快照）

时间戳对齐：
- 使用 TransactTime（逐笔数据中的交易时间）而非机器接收时间（PDF §5）
"""
from __future__ import annotations

import logging
from typing import List, Optional, Union

from config.settings import RESAMPLE_MS, TOP_LEVELS
from lob.engine.auction_engine import build_auction_snapshot, compute_auction_match
from lob.factors.derived_indicators import compute_derived_indicators
from lob.factors.dynamic_factors import compute_dynamic_factors
from lob.factors.ofi import compute_ofi, compute_ofi_normalized
from lob.factors.static_factors import compute_static_factors
from lob.models.order import Order, Trade
from lob.models.order_book import OrderBook
from lob.models.snapshot import (
    IntervalAccumulator, LevelSnapshot, LOBSnapshot
)
from lob.phase.phase_classifier import PhaseClassifier, TradingPhase

logger = logging.getLogger(__name__)

_NS_PER_MS = 1_000_000


class LOBResampler:
    """
    将事件流重采样为 50ms 均匀快照序列。

    Parameters
    ----------
    security_id     : 证券代码
    date_unix_ms    : 交易日起始时刻（Unix 毫秒，即交易日 00:00:00 对应的毫秒数）
                      用于将"自午夜纳秒"转换为 Unix 毫秒输出。
                      若为 0 则输出相对毫秒（自午夜起）。
    start_phase_ms  : 第一个快照的网格起点（自午夜的毫秒数，默认 09:15:00）
    end_phase_ms    : 最后一个快照的网格终点（默认 15:00:00）
    """

    def __init__(
        self,
        security_id: str,
        date_unix_ms: int = 0,
        start_phase_ms: int = 9 * 3600 * 1000,
        end_phase_ms:   int = 15 * 3600 * 1000,
        resample_ms:    int = RESAMPLE_MS,
    ) -> None:
        self.security_id   = security_id
        self.date_unix_ms  = date_unix_ms
        self.resample_ms   = resample_ms

        # next_boundary_ms = 当前区间的**结束**时刻（即下一区间的起始时刻）
        # 语义：当 event_ts_ms >= next_boundary_ms 时，关闭当前区间，生成快照
        aligned_start = (start_phase_ms // resample_ms) * resample_ms
        self.next_boundary_ms: int = aligned_start + resample_ms
        self.end_ms = end_phase_ms

        self.acc: Optional[IntervalAccumulator] = None
        self._last_book_snapshot: Optional[List] = None  # 上一时刻十档快照（carry-forward 用）
        self._last_phase: int = TradingPhase.PRE_OPEN.value

        # ── 全天累计统计（PDF §6）────────────────────────────────────────────
        self._cum_volume:   int   = 0
        self._cum_turnover: float = 0.0
        self._last_price:   Optional[float] = None   # 最新成交价（carry-forward）

        self._init_accumulator()

    # ── 公共接口 ──────────────────────────────────────────────────────────────

    def ingest(
        self,
        event_ts_ns: int,
        event: Union[Order, Trade],
        book: OrderBook,
        phase: TradingPhase,
    ) -> List[LOBSnapshot]:
        """
        处理一个事件，返回在此之前应当输出的所有快照（可能 0 个或多个）。

        当事件时间戳越过一个或多个 50ms 边界时：
        - 先生成越过的全部区间快照（含 carry-forward）
        - 再将事件加入新区间的累积器
        """
        event_ts_ms = event_ts_ns // _NS_PER_MS
        emitted: List[LOBSnapshot] = []

        while event_ts_ms >= self.next_boundary_ms:
            snap = self._finalize_interval(book, phase)
            emitted.append(snap)
            self.next_boundary_ms += self.resample_ms
            self._init_accumulator()

        # 将事件加入当前区间（实际写入由引擎通过 acc.add_* 完成）
        self._update_last_price_from_acc()
        return emitted

    def flush(self, book: OrderBook, phase: TradingPhase) -> Optional[LOBSnapshot]:
        """
        强制输出最后一个未满区间的快照（日终调用）。
        """
        if self.acc is None:
            return None
        snap = self._finalize_interval(book, phase)
        self.acc = None
        return snap

    # ── 内部方法 ──────────────────────────────────────────────────────────────

    def _init_accumulator(self) -> None:
        ts_start = self.next_boundary_ms - self.resample_ms
        self.acc = IntervalAccumulator(
            ts_start_ms = ts_start,
            ts_end_ms   = self.next_boundary_ms,
        )

    def _update_last_price_from_acc(self) -> None:
        """从累积器的最新成交价更新 last_price 和累计统计。"""
        if self.acc and self.acc._close_px is not None:
            self._last_price = self.acc._close_px

    def _finalize_interval(
        self,
        book: OrderBook,
        phase: TradingPhase,
    ) -> LOBSnapshot:
        """
        生成当前区间的 LOBSnapshot：
        1. 取盘口快照（十档）
        2. 计算静态因子
        3. 计算动态因子
        4. 计算衍生指标
        5. 计算 OFI（PDF §6）
        6. 更新全天累计统计
        7. 传播异常监控信息
        """
        acc = self.acc
        ts_ms = acc.ts_start_ms

        is_auction = PhaseClassifier.is_auction(phase)

        if is_auction:
            mp, mq = compute_auction_match(book)
            book.auction_match_price = mp
            book.auction_match_qty   = mq
            bids, asks = build_auction_snapshot(book, phase_changed=False, top_k=TOP_LEVELS)
        else:
            bids = self._snapshot_bid(book)
            asks = self._snapshot_ask(book)

        # 保存本次快照供 carry-forward 使用
        self._last_book_snapshot = (bids, asks)
        self._last_phase = phase.value

        # 更新全天累计统计（PDF §6）
        self._cum_volume   += acc._volume
        self._cum_turnover += acc._turnover
        if acc._close_px is not None:
            self._last_price = acc._close_px

        # 输出时间戳转换（Unix ms）
        out_ts_ms = self.date_unix_ms + ts_ms if self.date_unix_ms else ts_ms

        snap = LOBSnapshot(
            security_id  = self.security_id,
            timestamp_ms = out_ts_ms,
            phase        = phase.value,
            asks         = asks,
            bids         = bids,
            # 区间 OHLCV
            open_px      = acc._open_px,
            high_px      = acc._high_px,
            low_px       = acc._low_px,
            close_px     = acc._close_px,
            volume       = acc._volume,
            turnover     = acc._turnover,
            num_trades   = acc._num_trades,
            buy_volume   = acc._buy_vol,
            sell_volume  = acc._sell_vol,
            # 全天累计（PDF §6）
            last_price   = self._last_price,
            cum_volume   = self._cum_volume,
            cum_turnover = self._cum_turnover,
            # 异常监控（PDF §5）
            is_anomaly    = acc.anomaly_count > 0,
            anomaly_count = acc.anomaly_count,
        )

        # 计算盘口因子
        compute_static_factors(snap)
        compute_dynamic_factors(snap, acc, asks, bids)
        compute_derived_indicators(snap)

        # 计算 OFI（PDF §6）
        snap.ofi      = compute_ofi(snap, acc)
        snap.ofi_norm = compute_ofi_normalized(snap, acc)

        return snap

    @staticmethod
    def _snapshot_bid(book: OrderBook) -> List[LevelSnapshot]:
        levels = book.top_k_bids(TOP_LEVELS)
        result = [
            LevelSnapshot(
                price  = lvl.price / 10_000.0,
                volume = lvl.total_qty,
                count  = lvl.order_count,
            )
            for lvl in levels
        ]
        while len(result) < TOP_LEVELS:
            result.append(LevelSnapshot.empty())
        return result

    @staticmethod
    def _snapshot_ask(book: OrderBook) -> List[LevelSnapshot]:
        levels = book.top_k_asks(TOP_LEVELS)
        result = [
            LevelSnapshot(
                price  = lvl.price / 10_000.0,
                volume = lvl.total_qty,
                count  = lvl.order_count,
            )
            for lvl in levels
        ]
        while len(result) < TOP_LEVELS:
            result.append(LevelSnapshot.empty())
        return result
