"""
深圳证券交易所 STEP 协议解析器

逐笔委托字段：
    ApplSeqNum, SecurityID, TransactTime(HHMMSSMMM),
    Price(×10000), OrderQty, Side('1'=买/'2'=卖),
    OrdType('1'=市价/'2'=限价/'U'=本方最优), ChannelNo

逐笔成交字段：
    ApplSeqNum, SecurityID, TransactTime,
    BidApplSeqNum, OfferApplSeqNum,
    LastPx(×10000), LastQty,
    ExecType('F'=成交/'4'=撤单), ChannelNo

注：ChannelNo 为可选字段，缺失时默认为 0。
在同一 ChannelNo 下，order 和 trade 的 ApplSeqNum 统一连续编号（步进1）。
"""
from __future__ import annotations

import logging
from typing import Iterator

import pandas as pd

from config.exchange_config import SZSE_SIDE_MAP, SZSE_ORD_TYPE_MAP
from lob.models.order import Exchange, Order, OrdType, OrderStatus, Side, Trade
from lob.parsers.base_parser import BaseParser, hhmmssmmm_to_ns

logger = logging.getLogger(__name__)


class SZSEParser(BaseParser):
    """解析深交所逐笔委托和逐笔成交 CSV 数据。"""

    def parse_orders(self, df: pd.DataFrame) -> Iterator[Order]:
        """
        逐行将委托 DataFrame 转换为 Order 对象。
        支持列名大小写不敏感（通过 rename 预处理）。
        """
        for row in df.itertuples(index=False):
            try:
                side_raw = str(getattr(row, "side_raw", "") or "").strip()
                side = Side.BID if side_raw == "1" else Side.ASK

                ord_type_raw = str(getattr(row, "ord_type_raw", "") or "").strip()
                ord_type_str = SZSE_ORD_TYPE_MAP.get(ord_type_raw, "limit")
                ord_type = OrdType(ord_type_str)

                ts_ns = hhmmssmmm_to_ns(int(row.time_raw))
                price = int(row.price)
                qty   = int(row.qty)
                channel_no = int(getattr(row, "channel_no", 0) or 0)

                yield Order(
                    seq_num           = int(row.seq_num),
                    security_id       = str(row.security_id).strip().zfill(6),
                    exchange          = Exchange.SZSE,
                    timestamp_ns      = ts_ns,
                    price             = price,
                    qty               = qty,
                    remaining         = qty,
                    side              = side,
                    ord_type          = ord_type,
                    status            = OrderStatus.ACTIVE,
                    szse_ord_type_raw = ord_type_raw,
                    channel_no        = channel_no if channel_no > 0 else None,
                )
            except Exception as exc:
                logger.warning("SZSE order parse error: %s | row=%s", exc, row)

    def parse_trades(self, df: pd.DataFrame) -> Iterator[Trade]:
        """
        逐行将成交 DataFrame 转换为 Trade 对象。
        ExecType='4' 的记录标记为撤单（is_cancel=True）。
        """
        for row in df.itertuples(index=False):
            try:
                exec_type = str(getattr(row, "exec_type", "F") or "F").strip()
                is_cancel = (exec_type == "4")

                ts_ns   = hhmmssmmm_to_ns(int(row.time_raw))
                bid_seq = int(getattr(row, "bid_seq", 0) or 0)
                ask_seq = int(getattr(row, "ask_seq", 0) or 0)
                channel_no = int(getattr(row, "channel_no", 0) or 0)

                yield Trade(
                    seq_num       = int(row.seq_num),
                    security_id   = str(row.security_id).strip().zfill(6),
                    exchange      = Exchange.SZSE,
                    timestamp_ns  = ts_ns,
                    price         = int(row.price),
                    qty           = int(row.qty),
                    is_cancel     = is_cancel,
                    bid_order_seq = bid_seq if bid_seq > 0 else None,
                    ask_order_seq = ask_seq if ask_seq > 0 else None,
                    channel_no    = channel_no if channel_no > 0 else None,
                )
            except Exception as exc:
                logger.warning("SZSE trade parse error: %s | row=%s", exc, row)
