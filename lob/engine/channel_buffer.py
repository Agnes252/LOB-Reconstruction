"""
通道缓冲区（Channel Buffer）

PDF 参考：第 4 节「Channel Buffer（通道缓冲区）」、第 5 节「乱序处理」

实盘行情推送中，同一通道内的 ApplSeqNum（深交所）或 BizIndex（上交所）
可能因网络抖动、多线程推送等原因乱序到达。Channel Buffer 通过每通道维护
一个按序号排序的优先队列（min-heap），在序号连续时批量释放事件，
检测到 gap 时等待或在超时后强制推进并记录异常。

使用场景
--------
- 深交所：同一 ChannelNo 内 order 和 trade 共享 ApplSeqNum 编号
- 上交所：同一 Channel 内 order 和 trade 共享 BizIndex 编号

注意
----
对于已排序的离线 CSV 文件，Channel Buffer 开销极小（序号严格连续，
每条消息立即释放）。主要价值在于实时行情流的容错处理。
"""
from __future__ import annotations

import heapq
import logging
from typing import Dict, Iterator, List, Optional, Tuple, Union

from lob.models.order import Order, Trade

logger = logging.getLogger(__name__)

Event = Union[Order, Trade]


class ChannelBuffer:
    """
    单通道序号缓冲区。

    维护一个按序号排序的 min-heap，仅当序号连续时才释放事件。
    若出现 gap（序号跳变），将已缓冲的事件保留，等待缺失序号到达；
    若连续 gap_timeout 条消息都未能补全 gap，则强制推进并标记异常。

    Parameters
    ----------
    channel_no   : 通道编号（仅用于日志标识）
    expected_next: 下一条期望的序号（默认 1）
    gap_timeout  : 连续发现 gap 多少次后强制推进（默认 100）
    """

    def __init__(
        self,
        channel_no: int,
        expected_next: int = 1,
        gap_timeout: int = 100,
    ) -> None:
        self.channel_no    = channel_no
        self.expected_next = expected_next
        self.gap_timeout   = gap_timeout

        self._heap: List[Tuple[int, int, Event]] = []  # (seq_num, tie_breaker, event)
        self._tie: int = 0          # 同序号时按插入顺序保持稳定性
        self._gap_counter: int = 0  # 连续 gap 计数
        self.anomaly_count: int = 0 # 累计异常次数（gap 强制推进 + 重复序号）

    def push(self, seq_num: int, event: Event) -> None:
        """将事件压入缓冲区。"""
        heapq.heappush(self._heap, (seq_num, self._tie, event))
        self._tie += 1

    def pop_ready(self) -> List[Event]:
        """
        弹出所有序号连续的就绪事件。

        Returns
        -------
        当前可释放的事件列表（按序号升序）。若无就绪事件返回空列表。
        """
        ready: List[Event] = []

        while self._heap:
            seq_num = self._heap[0][0]

            if seq_num == self.expected_next:
                # 正常：序号与期望一致，弹出并推进
                _, _, event = heapq.heappop(self._heap)
                ready.append(event)
                self.expected_next += 1
                self._gap_counter = 0

            elif seq_num < self.expected_next:
                # 重复/过期序号，丢弃
                heapq.heappop(self._heap)
                logger.debug(
                    "Channel %d: 丢弃重复序号 %d (expected %d)",
                    self.channel_no, seq_num, self.expected_next,
                )
                self.anomaly_count += 1

            else:
                # Gap：seq_num > expected_next，等待缺失消息
                self._gap_counter += 1
                if self._gap_counter >= self.gap_timeout:
                    logger.warning(
                        "Channel %d: gap 超时，强制跳过 %d→%d，已丢失 %d 条消息",
                        self.channel_no,
                        self.expected_next,
                        seq_num,
                        seq_num - self.expected_next,
                    )
                    self.anomaly_count += 1
                    self.expected_next = seq_num  # 跳到现有最小序号
                    self._gap_counter = 0
                else:
                    break  # 继续等待

        return ready

    def flush_all(self) -> List[Event]:
        """
        日终强制刷出所有剩余事件（按序号升序，忽略 gap）。
        通常在交易日结束时调用，确保不遗漏任何事件。
        """
        result = sorted(self._heap, key=lambda x: x[0])
        self._heap.clear()
        return [event for _, _, event in result]

    def is_empty(self) -> bool:
        return len(self._heap) == 0

    def __len__(self) -> int:
        return len(self._heap)


class MultiChannelBuffer:
    """
    多通道缓冲区管理器。

    为每个 ChannelNo / BizIndex Channel 维护独立的 ChannelBuffer，
    统一管理 push / pop_ready / flush_all 操作。

    Parameters
    ----------
    gap_timeout : 各通道的 gap 超时阈值，传递给 ChannelBuffer
    """

    def __init__(self, gap_timeout: int = 100) -> None:
        self.gap_timeout = gap_timeout
        self._channels: Dict[int, ChannelBuffer] = {}

    def get_or_create(self, channel_no: int) -> ChannelBuffer:
        """获取指定通道的缓冲区（不存在则创建）。"""
        if channel_no not in self._channels:
            self._channels[channel_no] = ChannelBuffer(
                channel_no=channel_no,
                gap_timeout=self.gap_timeout,
            )
        return self._channels[channel_no]

    def push(self, channel_no: int, seq_num: int, event: Event) -> List[Event]:
        """
        推入一条事件，立即返回该通道当前可释放的就绪事件。

        Parameters
        ----------
        channel_no : 通道编号
        seq_num    : 该事件的序号（ApplSeqNum 或 BizIndex）
        event      : Order 或 Trade 对象

        Returns
        -------
        已就绪可处理的事件列表（按序号升序）。
        """
        ch = self.get_or_create(channel_no)
        ch.push(seq_num, event)
        return ch.pop_ready()

    def flush_all(self) -> List[Event]:
        """
        刷出所有通道的剩余事件。
        各通道内部按序号升序，通道间按 timestamp_ns 合并排序。
        """
        all_events: List[Event] = []
        for ch in self._channels.values():
            all_events.extend(ch.flush_all())
        # 跨通道按时间戳合并
        all_events.sort(key=lambda e: e.timestamp_ns)
        return all_events

    @property
    def total_anomalies(self) -> int:
        """所有通道累计异常次数。"""
        return sum(ch.anomaly_count for ch in self._channels.values())

    @property
    def channel_count(self) -> int:
        return len(self._channels)

    def summary(self) -> Dict[int, int]:
        """返回各通道异常计数摘要 {channel_no: anomaly_count}。"""
        return {ch.channel_no: ch.anomaly_count
                for ch in self._channels.values()
                if ch.anomaly_count > 0}
