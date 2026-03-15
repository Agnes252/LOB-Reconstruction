"""
上海证券交易所 LDDS 协议解析器

逐笔委托字段：
    ApplSeqNum, SecurityID, TransactTime(HHMMSSMMM),
    Price(×10000), OrderQty, Side('1'=买/'2'=卖),
    OrdType('1'=市价/'2'=限价), OrderNo, Channel, BizIndex,
    CancelFlag（可选，'D'=撤单指令，空=正常委托）

逐笔成交字段：
    TradeIndex, SecurityID, TransactTime,
    BuyNo, SellNo,             # 对应原始order的OrderNo（非ApplSeqNum/OrderIndex）
    TradePrice(×10000), TradeQty, TradeMoney,
    TradeBSFlag('B'=主买/'S'=主卖/'N'=集合竞价),
    TradeChannel, BizIndex

关键说明：
- BizIndex 为统一序号：同一 Channel 内 order 和 trade 使用统一编号，是还原事件先后顺序的唯一标准。
- BuyNo/SellNo 对应原始委托的 OrderNo 字段（非 OrderIndex 也非 ApplSeqNum），用于成交→委托的反查。
- 上交所连续竞价阶段，OrderQty 为被撮合后的剩余数量（非原始委托数量）；若订单被一次性全部撮合，
  则该委托不出现在逐笔委托流中（幽灵订单），需从成交记录反推。
"""
from __future__ import annotations

import logging
from typing import Iterator

import pandas as pd

from config.exchange_config import SSE_SIDE_MAP, SSE_ORD_TYPE_MAP
from lob.models.order import Exchange, Order, OrdType, OrderStatus, Side, Trade
from lob.parsers.base_parser import BaseParser, hhmmssmmm_to_ns

logger = logging.getLogger(__name__)


class SSEParser(BaseParser):
    """解析上交所逐笔委托和逐笔成交 CSV 数据。"""

    def parse_orders(self, df: pd.DataFrame) -> Iterator[Order]:
        """
        解析委托流。
        若 CancelFlag == 'D' 则该记录为撤单指令，
        对应的 Order 对象通过 cancel_flag='D' 标记，由引擎做特殊处理。

        重要：OrderNo 是上交所内部订单编号，用于 trade 的 BuyNo/SellNo 反查；
             BizIndex 是同一 Channel 内 order 与 trade 的统一排序序号。
        """
        for row in df.itertuples(index=False):
            try:
                side_raw = str(getattr(row, "side_raw", "") or "").strip()
                side = Side.BID if side_raw == "1" else Side.ASK

                ord_type_raw = str(getattr(row, "ord_type_raw", "") or "").strip()
                ord_type_str = SSE_ORD_TYPE_MAP.get(ord_type_raw, "limit")
                ord_type = OrdType(ord_type_str)

                cancel_flag = str(getattr(row, "cancel_flag", "") or "").strip()
                order_no    = int(getattr(row, "order_no", 0) or 0)
                channel_no  = int(getattr(row, "channel_no", 0) or 0)
                biz_index   = int(getattr(row, "biz_index", 0) or 0)

                ts_ns = hhmmssmmm_to_ns(int(row.time_raw))
                price = int(row.price)
                qty   = int(row.qty)

                yield Order(
                    seq_num      = int(row.seq_num),
                    security_id  = str(row.security_id).strip().zfill(6),
                    exchange     = Exchange.SSE,
                    timestamp_ns = ts_ns,
                    price        = price,
                    qty          = qty,
                    remaining    = qty,
                    side         = side,
                    ord_type     = ord_type,
                    status       = OrderStatus.ACTIVE,
                    cancel_flag  = cancel_flag if cancel_flag else None,
                    order_no     = order_no if order_no > 0 else None,
                    channel_no   = channel_no if channel_no > 0 else None,
                    biz_index    = biz_index if biz_index > 0 else None,
                )
            except Exception as exc:
                logger.warning("SSE order parse error: %s | row=%s", exc, row)

    def parse_trades(self, df: pd.DataFrame) -> Iterator[Trade]:
        """
        解析成交流。上交所成交流不含撤单，is_cancel 始终为 False。

        注意：bid_order_seq/ask_order_seq 存储的是 BuyNo/SellNo，
             这两个值对应原始委托的 OrderNo 字段，引擎将通过
             book.order_no_index 将其映射回 seq_num 再操作盘口。
        """
        for row in df.itertuples(index=False):
            try:
                ts_ns      = hhmmssmmm_to_ns(int(row.time_raw))
                bid_seq    = int(getattr(row, "bid_seq", 0) or 0)
                ask_seq    = int(getattr(row, "ask_seq", 0) or 0)
                turnover   = float(getattr(row, "turnover", 0.0) or 0.0)
                bs_flag    = str(getattr(row, "trade_bs_flag", "N") or "N").strip()
                channel_no = int(getattr(row, "channel_no", 0) or 0)
                biz_index  = int(getattr(row, "biz_index", 0) or 0)

                yield Trade(
                    seq_num       = int(row.seq_num),
                    security_id   = str(row.security_id).strip().zfill(6),
                    exchange      = Exchange.SSE,
                    timestamp_ns  = ts_ns,
                    price         = int(row.price),
                    qty           = int(row.qty),
                    is_cancel     = False,
                    bid_order_seq = bid_seq if bid_seq > 0 else None,
                    ask_order_seq = ask_seq if ask_seq > 0 else None,
                    trade_bs_flag = bs_flag,
                    turnover      = turnover,
                    channel_no    = channel_no if channel_no > 0 else None,
                    biz_index     = biz_index if biz_index > 0 else None,
                )
            except Exception as exc:
                logger.warning("SSE trade parse error: %s | row=%s", exc, row)
