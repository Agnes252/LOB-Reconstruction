"""
Parquet 输出写入器

PDF 参考：第 6 节「项目结果：重构好的 LOB 样式」

将 LOBSnapshot 列表转换为扁平化 DataFrame 并写入 Parquet 文件。
列命名规范：{字段}_{档位序号}（如 ask_px_1, bid_vol_3）。

新增列（相比原版）：
- last_price, cum_volume, cum_turnover  全天累计统计
- ofi, ofi_norm                         订单流不平衡因子
- is_anomaly, anomaly_count             异常监控标记
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional

import pandas as pd

from config.settings import PARQUET_COMPRESSION, PARQUET_ENGINE, TOP_LEVELS
from lob.models.snapshot import LOBSnapshot

logger = logging.getLogger(__name__)


def snapshots_to_dataframe(snapshots: List[LOBSnapshot]) -> pd.DataFrame:
    """
    将 LOBSnapshot 列表展开为扁平化 DataFrame。

    十档字段展开规则：
        asks[0].price → ask_px_1 （档位序号从 1 开始，与交易软件习惯一致）
        bids[0].volume → bid_vol_1
        asks[2].count  → ask_cnt_3
    """
    rows = []
    for snap in snapshots:
        row: dict = {
            "security_id":  snap.security_id,
            "timestamp_ms": snap.timestamp_ms,
            "phase":        snap.phase,
            # 静态因子
            "mid_price":    snap.mid_price,
            "spread":       snap.spread,
            "sheet_diff":   snap.sheet_diff,
            # 区间 OHLCV
            "open":         snap.open_px,
            "high":         snap.high_px,
            "low":          snap.low_px,
            "close":        snap.close_px,
            "volume":       snap.volume,
            "turnover":     snap.turnover,
            "num_trades":   snap.num_trades,
            "buy_volume":   snap.buy_volume,
            "sell_volume":  snap.sell_volume,
            # 衍生指标
            "match_diff":   snap.match_diff,
            "order_diff":   snap.order_diff,
            "cancel_diff":  snap.cancel_diff,
            # OFI（PDF §6）
            "ofi":          snap.ofi,
            "ofi_norm":     snap.ofi_norm,
            # 全天累计统计（PDF §6）
            "last_price":   snap.last_price,
            "cum_volume":   snap.cum_volume,
            "cum_turnover": snap.cum_turnover,
            # 异常监控（PDF §5）
            "is_anomaly":    snap.is_anomaly,
            "anomaly_count": snap.anomaly_count,
        }

        # 十档盘口展开
        for i in range(TOP_LEVELS):
            n = i + 1  # 档位序号从 1 开始
            a = snap.asks[i] if i < len(snap.asks) else None
            b = snap.bids[i] if i < len(snap.bids) else None
            row[f"ask_px_{n}"]  = a.price  if a else 0.0
            row[f"ask_vol_{n}"] = a.volume if a else 0
            row[f"ask_cnt_{n}"] = a.count  if a else 0
            row[f"bid_px_{n}"]  = b.price  if b else 0.0
            row[f"bid_vol_{n}"] = b.volume if b else 0
            row[f"bid_cnt_{n}"] = b.count  if b else 0

        # 动态因子展开（各档）
        for i in range(TOP_LEVELS):
            n = i + 1
            row[f"order_vol_ask_{n}"]  = snap.order_vol_ask[i]  if i < len(snap.order_vol_ask)  else 0
            row[f"order_vol_bid_{n}"]  = snap.order_vol_bid[i]  if i < len(snap.order_vol_bid)  else 0
            row[f"match_vol_ask_{n}"]  = snap.match_vol_ask[i]  if i < len(snap.match_vol_ask)  else 0
            row[f"match_vol_bid_{n}"]  = snap.match_vol_bid[i]  if i < len(snap.match_vol_bid)  else 0
            row[f"cancel_vol_ask_{n}"] = snap.cancel_vol_ask[i] if i < len(snap.cancel_vol_ask) else 0
            row[f"cancel_vol_bid_{n}"] = snap.cancel_vol_bid[i] if i < len(snap.cancel_vol_bid) else 0

        rows.append(row)

    return pd.DataFrame(rows)


def write_parquet(
    snapshots: List[LOBSnapshot],
    output_path: str,
    partition_cols: Optional[List[str]] = None,
) -> None:
    """
    将快照列表写入 Parquet 文件。

    Parameters
    ----------
    snapshots    : LOBSnapshot 列表
    output_path  : 输出文件路径（.parquet 后缀）
    partition_cols: 分区列（可选，如 ['security_id']）
    """
    if not snapshots:
        logger.warning("无快照数据，跳过写入: %s", output_path)
        return

    df = snapshots_to_dataframe(snapshots)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(
        str(out),
        engine      = PARQUET_ENGINE,
        compression = PARQUET_COMPRESSION,
        index       = False,
    )
    logger.info("已写入 %d 行快照 → %s", len(df), output_path)
