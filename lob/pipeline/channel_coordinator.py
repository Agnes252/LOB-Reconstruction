"""
通道协调器（Channel Coordinator）— 相互触发快照机制

修改建议来源：改进建议 §3「完善快照触发机制 (Trigger Type)」

问题背景
--------
在批量处理多只股票时，不同股票（即使在同一交易通道上）的行情事件密度差异可能
导致各股 Parquet 输出的 timestamp_ms 时间轴不完全对齐。两种极端情形：

  - 流动性好的股票：每 50ms 内有大量事件，快照完整；
  - 流动性差的股票（ST、停牌等）：可能数分钟内无事件，`fill_to_end()` 虽已补全，
    但若批量处理时任务串行，各股快照仍可独立生成。

建议的「全标的相互触发」（Mutual Trigger）模式
----------------------------------------------
当通道内任意一只股票的时间戳跨越采样周期时，触发该通道内所有股票同时输出当前
订单簿截面。

离线批处理的替代方案
--------------------
本项目以 SingleSecurityPipeline 每股独立运行，各自调用 fill_to_end() 保证了
时间轴完整性（9:15:00 ~ 15:00:00 全覆盖）。

ChannelCoordinator 实现的是真正的「同时触发」语义：
  - 维护通道内所有股票的 pipeline 引用；
  - 将通道内所有事件按统一时间轴归并处理；
  - 每当任意股票的时间轴跨越边界时，强制所有股票输出当前快照。

适用场景：同一通道内多股票共享一个完整逐笔文件（按 SecurityID 分流）时的处理。

使用示例
--------
coordinator = ChannelCoordinator(
    channel_no   = 2310,
    resample_ms  = 50,
    date_unix_ms = 0,
)
coordinator.add_security("000001", book_1, resampler_1, engine_1)
coordinator.add_security("000002", book_2, resampler_2, engine_2)

for event in merged_channel_events:
    snaps = coordinator.ingest(event)   # snaps 包含所有触发的快照
    ...

# 日终
all_snaps = coordinator.flush_all()
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, Iterator, List, Optional, Tuple, Union

from lob.engine.szse_engine import SZSEEngine
from lob.engine.sse_engine import SSEEngine
from lob.models.order import Exchange, Order, Trade
from lob.models.order_book import OrderBook
from lob.models.snapshot import IntervalAccumulator, LOBSnapshot
from lob.phase.phase_classifier import PhaseClassifier, TradingPhase
from lob.resampler.resampler import LOBResampler

logger = logging.getLogger(__name__)

_NS_PER_MS = 1_000_000


@dataclass
class _SecurityState:
    """单只股票在通道协调器内的状态容器。"""
    security_id: str
    book:        OrderBook
    resampler:   LOBResampler
    engine:      Union[SZSEEngine, SSEEngine]
    phase:       TradingPhase = TradingPhase.PRE_OPEN
    snapshots:   List[LOBSnapshot] = field(default_factory=list)


class ChannelCoordinator:
    """
    通道级多股票快照协调器（相互触发模式）。

    Parameters
    ----------
    channel_no   : 通道编号（仅用于日志标识）
    resample_ms  : 采样间隔（毫秒），默认 50
    date_unix_ms : 交易日 Unix 毫秒起始（0 = 相对时间）
    """

    def __init__(
        self,
        channel_no:   int = 0,
        resample_ms:  int = 50,
        date_unix_ms: int = 0,
    ) -> None:
        self.channel_no   = channel_no
        self.resample_ms  = resample_ms
        self.date_unix_ms = date_unix_ms

        self._securities: Dict[str, _SecurityState] = {}
        # 通道级时间轴：所有股票共享的下一个 50ms 边界（ms）
        self._channel_boundary_ms: Optional[int] = None

    def add_security(
        self,
        security_id: str,
        book:        OrderBook,
        resampler:   LOBResampler,
        engine:      Union[SZSEEngine, SSEEngine],
    ) -> None:
        """注册一只股票到通道协调器。"""
        self._securities[security_id] = _SecurityState(
            security_id = security_id,
            book        = book,
            resampler   = resampler,
            engine      = engine,
        )

    def ingest(
        self,
        event: Union[Order, Trade],
    ) -> List[LOBSnapshot]:
        """
        处理一条事件，返回本次触发的所有快照（跨所有股票）。

        触发逻辑
        --------
        1. 将事件路由到对应 security_id 的 pipeline；
        2. 若事件时间戳跨越通道级边界，对 **所有** 股票强制触发 carry-forward 快照；
        3. 更新通道级边界。
        """
        sec_id = event.security_id
        state  = self._securities.get(sec_id)
        if state is None:
            logger.warning(
                "ChannelCoordinator ch=%d: unknown security=%s, skipping",
                self.channel_no, sec_id,
            )
            return []

        event_ts_ms = event.timestamp_ns // _NS_PER_MS
        emitted: List[LOBSnapshot] = []

        # 初始化通道级边界
        if self._channel_boundary_ms is None:
            self._channel_boundary_ms = state.resampler.next_boundary_ms

        # ── 相互触发：跨边界时对所有股票输出快照 ──────────────────────────────
        while event_ts_ms >= self._channel_boundary_ms:
            for s in self._securities.values():
                # 确保该股票的 resampler 也对齐到通道边界
                while s.resampler.next_boundary_ms <= self._channel_boundary_ms:
                    snap = s.resampler._finalize_interval(s.book, s.phase)
                    s.snapshots.append(snap)
                    emitted.append(snap)
                    s.resampler.next_boundary_ms += s.resampler.resample_ms
                    s.resampler._init_accumulator()

            self._channel_boundary_ms += self.resample_ms

        # ── 路由事件到对应引擎 ────────────────────────────────────────────────
        classifier = PhaseClassifier()
        new_phase  = classifier.classify(event.timestamp_ns)
        if new_phase != state.phase:
            state.phase = new_phase

        if isinstance(event, Order):
            state.engine.process_order(
                event, state.book, state.resampler.acc, state.phase
            )
        else:
            if hasattr(event, "is_cancel") and event.is_cancel:
                state.engine.process_cancel(event, state.book, state.resampler.acc)
            else:
                state.engine.process_trade(event, state.book, state.resampler.acc)

        return emitted

    def flush_all(self) -> List[LOBSnapshot]:
        """日终：对所有股票调用 fill_to_end()，补全至收盘。"""
        all_snaps: List[LOBSnapshot] = []
        for s in self._securities.values():
            end_snaps = s.resampler.fill_to_end(s.book, s.phase)
            s.snapshots.extend(end_snaps)
            all_snaps.extend(end_snaps)
        return all_snaps

    def get_snapshots(self, security_id: str) -> List[LOBSnapshot]:
        """获取指定股票已生成的全部快照。"""
        state = self._securities.get(security_id)
        return state.snapshots if state else []

    @property
    def security_count(self) -> int:
        return len(self._securities)
