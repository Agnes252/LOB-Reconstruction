"""
解析器抽象基类

所有解析器将 CSV DataFrame 行转换为 Order/Trade 对象，
并将时间戳统一转换为 纳秒（自午夜起算）。
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterator, Tuple

import pandas as pd

from lob.models.order import Order, Trade


def hhmmssmmm_to_ns(time_raw: int) -> int:
    """
    将交易所 HHMMSSMMM 格式整数（9 位）转换为自午夜起的纳秒数。

    示例：93015500 → 09:30:15.500 → 34215500 * 1_000_000 ns

    Parameters
    ----------
    time_raw : int
        如 93015500（= 9:30:15.500）或 93015500（无前导零时 8 位）
    """
    ms  = time_raw % 1_000
    ss  = (time_raw // 1_000) % 100
    mm  = (time_raw // 100_000) % 100
    hh  = time_raw // 10_000_000
    total_ms = ((hh * 3600 + mm * 60 + ss) * 1_000) + ms
    return total_ms * 1_000_000


class BaseParser(ABC):
    """
    抽象解析器接口。
    子类分别实现深/沪两市的逐笔委托和逐笔成交解析。
    """

    @abstractmethod
    def parse_orders(self, df: pd.DataFrame) -> Iterator[Order]:
        """将委托 DataFrame 的每一行解析为 Order 对象。"""
        ...

    @abstractmethod
    def parse_trades(self, df: pd.DataFrame) -> Iterator[Trade]:
        """将成交 DataFrame 的每一行解析为 Trade 对象。"""
        ...
