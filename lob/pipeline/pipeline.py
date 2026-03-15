"""
单股票单日 LOB 重建流水线

PDF 参考：第 4 节「Channel Buffer」、第 5 节「乱序处理/跨日清理」

流程：
    1. 读取逐笔委托和成交 CSV（分块）
    2. 解析为 Order/Trade 对象
    3. [可选] 通道缓冲区过滤：每通道独立 PQ，按序号排序后释放（PDF §4）
    4. 按序号归并排序（两流合并）
       - 深交所：同一 ChannelNo 下 order 和 trade 的 ApplSeqNum 统一编号，
         直接按 (timestamp_ns, seq_num, priority) 排序即可
       - 上交所：BizIndex 是同一 Channel 内 order 与 trade 的统一排序键，
         必须以 BizIndex 作为主排序键，才能正确还原事件先后顺序；
         priority 用于 BizIndex 相同时委托先于成交（0 < 1）
    5. 驱动主循环：阶段判断 → 引擎处理 → 重采样
    6. 输出 Parquet 快照文件

跨日清理（PDF §5）：
    每次创建新的 SingleSecurityPipeline 实例即相当于重置所有状态，
    OrderBook、引擎内部的成交价跟踪等均从零开始。
"""
from __future__ import annotations

import heapq
import logging
from pathlib import Path
from typing import Iterator, List, Optional, Tuple, Union

from lob.engine.channel_buffer import MultiChannelBuffer
from lob.engine.szse_engine import SZSEEngine
from lob.engine.sse_engine import SSEEngine
from lob.io.reader import read_szse_orders, read_szse_trades, read_sse_orders, read_sse_trades
from lob.io.writer import write_parquet
from lob.models.order import Exchange, Order, Trade
from lob.models.order_book import OrderBook
from lob.models.snapshot import IntervalAccumulator, LOBSnapshot
from lob.parsers.szse_parser import SZSEParser
from lob.parsers.sse_parser import SSEParser
from lob.phase.phase_classifier import PhaseClassifier, TradingPhase
from lob.phase.phase_transitions import PhaseTransitionHandler
from lob.resampler.resampler import LOBResampler

logger = logging.getLogger(__name__)

# 事件流排序优先级：同排序键时委托先于成交（0 < 1），保证先挂单再成交
_ORDER_PRIORITY  = 0
_TRADE_PRIORITY  = 1

# 无 BizIndex 时的默认值（使用超大值排到最后，仅作保底）
_NO_BIZ_INDEX = 2**62


def _merge_events_szse(
    orders: Iterator[Order],
    trades: Iterator[Trade],
) -> Iterator[Union[Order, Trade]]:
    """
    深交所双路归并：按 (timestamp_ns, seq_num, priority) 升序合并。
    同一 ChannelNo 下 order 和 trade 的 ApplSeqNum 统一连续编号，不会乱序。
    """
    heap: List[Tuple] = []

    def push_order(o: Order):
        heapq.heappush(heap, (o.timestamp_ns, o.seq_num, _ORDER_PRIORITY, o))

    def push_trade(t: Trade):
        heapq.heappush(heap, (t.timestamp_ns, t.seq_num, _TRADE_PRIORITY, t))

    order_iter = iter(orders)
    trade_iter = iter(trades)

    for _ in range(10_000):
        try:
            push_order(next(order_iter))
        except StopIteration:
            order_iter = iter([])
            break

    for _ in range(10_000):
        try:
            push_trade(next(trade_iter))
        except StopIteration:
            trade_iter = iter([])
            break

    order_exhausted = False
    trade_exhausted = False

    while heap:
        _, _, _, event = heapq.heappop(heap)
        yield event

        if not order_exhausted:
            try:
                push_order(next(order_iter))
            except StopIteration:
                order_exhausted = True

        if not trade_exhausted:
            try:
                push_trade(next(trade_iter))
            except StopIteration:
                trade_exhausted = True


def _merge_events_sse(
    orders: Iterator[Order],
    trades: Iterator[Trade],
) -> Iterator[Union[Order, Trade]]:
    """
    上交所双路归并：以 BizIndex 为主排序键，同 BizIndex 时委托先于成交。

    BizIndex 是同一 Channel 内 order 和 trade 的统一编号（2025年前可能非严格单调，
    但仍是唯一正确的事件先后顺序标准）。若 BizIndex 缺失则降级到 timestamp_ns。
    """
    heap: List[Tuple] = []

    def push_order(o: Order):
        biz = o.biz_index if o.biz_index is not None else _NO_BIZ_INDEX
        heapq.heappush(heap, (biz, _ORDER_PRIORITY, o.timestamp_ns, o))

    def push_trade(t: Trade):
        biz = t.biz_index if t.biz_index is not None else _NO_BIZ_INDEX
        heapq.heappush(heap, (biz, _TRADE_PRIORITY, t.timestamp_ns, t))

    order_iter = iter(orders)
    trade_iter = iter(trades)

    for _ in range(10_000):
        try:
            push_order(next(order_iter))
        except StopIteration:
            order_iter = iter([])
            break

    for _ in range(10_000):
        try:
            push_trade(next(trade_iter))
        except StopIteration:
            trade_iter = iter([])
            break

    order_exhausted = False
    trade_exhausted = False

    while heap:
        _, _, _, event = heapq.heappop(heap)
        yield event

        if not order_exhausted:
            try:
                push_order(next(order_iter))
            except StopIteration:
                order_exhausted = True

        if not trade_exhausted:
            try:
                push_trade(next(trade_iter))
            except StopIteration:
                trade_exhausted = True


class SingleSecurityPipeline:
    """
    单证券单日 LOB 重建流水线。

    Parameters
    ----------
    exchange             : Exchange.SZSE 或 Exchange.SSE
    security_id          : 证券代码
    orders_path          : 逐笔委托文件路径
    trades_path          : 逐笔成交文件路径
    output_path          : 输出 Parquet 文件路径
    date_unix_ms         : 交易日 00:00:00 的 Unix 毫秒（用于输出时间戳，0 = 相对时间）
    enable_channel_buffer: 是否启用通道缓冲区处理乱序（PDF §4，默认关闭）
    enable_price_cage    : 是否启用深交所价格笼子待入池（仅创业板，PDF §3，默认关闭）
    """

    def __init__(
        self,
        exchange:              Exchange,
        security_id:           str,
        orders_path:           str,
        trades_path:           str,
        output_path:           str,
        date_unix_ms:          int  = 0,
        enable_channel_buffer: bool = False,
        enable_price_cage:     bool = False,
    ) -> None:
        self.exchange      = exchange
        self.security_id   = security_id
        self.orders_path   = orders_path
        self.trades_path   = trades_path
        self.output_path   = output_path
        self.date_unix_ms  = date_unix_ms
        self.enable_channel_buffer = enable_channel_buffer

        # 初始化组件
        if exchange == Exchange.SZSE:
            self.parser = SZSEParser()
            self.engine = SZSEEngine(enable_price_cage=enable_price_cage)
            self._read_orders = read_szse_orders
            self._read_trades = read_szse_trades
            self._merge_events = _merge_events_szse
        else:
            self.parser = SSEParser()
            self.engine = SSEEngine()
            self._read_orders = read_sse_orders
            self._read_trades = read_sse_trades
            self._merge_events = _merge_events_sse

        self.book        = OrderBook(security_id=security_id,
                                     exchange=exchange.value)
        self.resampler   = LOBResampler(
            security_id  = security_id,
            date_unix_ms = date_unix_ms,
        )
        self.classifier  = PhaseClassifier()
        self.transition  = PhaseTransitionHandler()
        self.current_phase: TradingPhase = TradingPhase.PRE_OPEN
        self.snapshots: List[LOBSnapshot] = []

    # ── 运行 ──────────────────────────────────────────────────────────────────

    def run(self) -> int:
        """
        执行完整流水线。

        Returns
        -------
        生成的快照数量
        """
        logger.info("开始重建 %s %s", self.exchange.value, self.security_id)

        orders_iter = self._iter_orders()
        trades_iter = self._iter_trades()
        events      = self._merge_events(orders_iter, trades_iter)

        if self.enable_channel_buffer:
            # 通道缓冲区模式：每个事件先经过通道缓冲区，按序号有序释放（PDF §4）
            ch_buf = MultiChannelBuffer(gap_timeout=100)
            for raw_event in events:
                ch_no = getattr(raw_event, "channel_no", None) or 0
                biz   = getattr(raw_event, "biz_index", None) \
                        or getattr(raw_event, "seq_num", 0)
                for ready in ch_buf.push(ch_no, biz, raw_event):
                    self._process_event(ready)
            # 日终刷出所有残留
            for leftover in ch_buf.flush_all():
                self._process_event(leftover)
            if ch_buf.total_anomalies:
                logger.warning(
                    "%s %s 通道缓冲区异常汇总: %s",
                    self.exchange.value, self.security_id, ch_buf.summary(),
                )
        else:
            for event in events:
                self._process_event(event)

        # 日终补全：生成从最后一个事件到收盘的全部 carry-forward 快照
        # 确保每只股票 50ms 网格完整覆盖至 15:00，便于跨标的时间轴对齐
        end_snaps = self.resampler.fill_to_end(self.book, self.current_phase)
        self.snapshots.extend(end_snaps)

        # 写出
        write_parquet(self.snapshots, self.output_path)
        logger.info(
            "完成 %s %s：共 %d 个快照，盘口异常 %d 次",
            self.exchange.value, self.security_id,
            len(self.snapshots), self.book.anomaly_count,
        )
        return len(self.snapshots)

    # ── 内部方法 ──────────────────────────────────────────────────────────────

    def _process_event(self, event: Union[Order, Trade]) -> None:
        """处理单个事件：阶段更新 → 引擎处理 → 重采样。"""
        # 1. 阶段判断
        new_phase = self.classifier.classify(event.timestamp_ns)
        if new_phase != self.current_phase:
            self._handle_phase_change(new_phase, event.timestamp_ns)

        # 2. 引擎处理（更新 book + acc）
        if isinstance(event, Order):
            fills = self.engine.process_order(
                event, self.book, self.resampler.acc, self.current_phase
            )
        else:
            if hasattr(event, 'is_cancel') and event.is_cancel:
                # 深交所撤单通过成交流下发
                self.engine.process_cancel(event, self.book, self.resampler.acc)
            else:
                self.engine.process_trade(event, self.book, self.resampler.acc)

        # 3. 重采样（检查是否越过 50ms 边界）
        snaps = self.resampler.ingest(
            event.timestamp_ns, event, self.book, self.current_phase
        )
        self.snapshots.extend(snaps)

    def _handle_phase_change(
        self,
        new_phase: TradingPhase,
        timestamp_ns: int,
    ) -> None:
        """处理阶段切换，集合竞价→连续竞价时触发最终撮合。"""
        old_phase = self.current_phase

        if (PhaseClassifier.is_auction(old_phase)
                and PhaseClassifier.is_continuous(new_phase)):
            mp, mq, synth_trades = self.transition.handle_auction_close(
                self.book, new_phase, timestamp_ns
            )
            for t in synth_trades:
                self.resampler.acc.add_trade(t)
            logger.info(
                "%s %s 集合竞价结束，开盘价=%.4f 成交量=%d",
                self.exchange.value, self.security_id,
                mp / 10_000.0, mq,
            )

        self.current_phase = new_phase
        self.book.auction_match_price = 0
        self.book.auction_match_qty   = 0

    def _iter_orders(self) -> Iterator[Order]:
        for chunk in self._read_orders(self.orders_path):
            yield from self.parser.parse_orders(chunk)

    def _iter_trades(self) -> Iterator[Trade]:
        for chunk in self._read_trades(self.trades_path):
            yield from self.parser.parse_trades(chunk)
