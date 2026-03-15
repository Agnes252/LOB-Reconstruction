"""
价格档位数据结构

每个价格档位维护一个 FIFO 委托队列（按时间优先），支持：
- O(log n) 追加新委托
- O(n) 按 seq_num 撤单（实际每档委托数极少，n < 50）
- O(1) 批量消耗（连续竞价 FIFO 撮合）
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Tuple


@dataclass
class PriceLevel:
    """
    单个价格档位。

    orders : Deque[Tuple[int, int]]
        (seq_num, remaining_qty) 队列，头部为最早到达的委托（时间优先）。
    total_qty   : 档位内所有委托的剩余总量（快速访问，避免遍历）。
    order_count : 档位内有效委托数量。
    price       : 原始价格整数（× 10000）。
    """
    price:       int
    orders:      Deque[Tuple[int, int]] = field(default_factory=deque)
    total_qty:   int = 0
    order_count: int = 0

    # ── 写操作 ────────────────────────────────────────────────────────────────

    def add_order(self, seq_num: int, qty: int) -> None:
        """追加新委托到队列末尾（时间最晚，优先级最低）。"""
        self.orders.append((seq_num, qty))
        self.total_qty += qty
        self.order_count += 1

    def cancel_order(self, seq_num: int, cancel_qty: int) -> bool:
        """
        按 seq_num 撤单，减少 cancel_qty 数量。
        若剩余为0则完全移除；否则部分撤单。
        返回 True 表示找到目标委托，False 表示未找到。
        """
        for i, (sn, qty) in enumerate(self.orders):
            if sn == seq_num:
                reduce = min(qty, cancel_qty)
                new_qty = qty - reduce
                self.total_qty -= reduce
                if new_qty <= 0:
                    del self.orders[i]
                    self.order_count -= 1
                else:
                    self.orders[i] = (sn, new_qty)
                return True
        return False

    def consume_qty(self, qty: int) -> int:
        """
        从队列头部按 FIFO 顺序消耗指定数量（用于连续竞价撮合）。
        返回实际消耗量（可能少于请求量，若档位总量不足）。
        """
        consumed = 0
        while self.orders and qty > 0:
            sn, order_qty = self.orders[0]
            take = min(order_qty, qty)
            consumed += take
            qty -= take
            self.total_qty -= take
            if take == order_qty:
                self.orders.popleft()
                self.order_count -= 1
            else:
                self.orders[0] = (sn, order_qty - take)
        return consumed

    def remove_order_by_seq(self, seq_num: int) -> int:
        """
        完全移除指定 seq_num 的委托，返回被移除的数量（0 表示未找到）。
        """
        for i, (sn, qty) in enumerate(self.orders):
            if sn == seq_num:
                del self.orders[i]
                self.total_qty -= qty
                self.order_count -= 1
                return qty
        return 0

    # ── 只读属性 ──────────────────────────────────────────────────────────────

    @property
    def is_empty(self) -> bool:
        return self.order_count == 0

    @property
    def price_float(self) -> float:
        return self.price / 10_000.0

    def __repr__(self) -> str:
        return (f"PriceLevel(price={self.price_float:.4f}, "
                f"qty={self.total_qty}, cnt={self.order_count})")
