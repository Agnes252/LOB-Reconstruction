"""
data_test 数据端到端测试脚本

用实际深交所数据（data_test/orders.csv + data_test/trans.csv）运行完整流水线，
验证：
  1. 解析不报错
  2. 订单簿能正常重建（有委托进盘口）
  3. 50ms 快照正常生成
  4. 十档价格单调（买档降序，卖档升序）
  5. 时间戳对齐到 50ms 网格
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import logging
import tempfile
from pathlib import Path

import pandas as pd

from lob.models.order import Exchange
from lob.pipeline.pipeline import SingleSecurityPipeline

logging.basicConfig(
    level=logging.WARNING,
    format="%(levelname)s %(name)s: %(message)s",
)

DATA_DIR    = Path(__file__).parent.parent / "data_test"
ORDERS_PATH = DATA_DIR / "orders.csv"
TRADES_PATH = DATA_DIR / "trans.csv"
SECURITY_ID = "000400"   # 000400.SZ 去后缀


def run_pipeline(output_path: str) -> pd.DataFrame:
    pipe = SingleSecurityPipeline(
        exchange      = Exchange.SZSE,
        security_id   = SECURITY_ID,
        orders_path   = str(ORDERS_PATH),
        trades_path   = str(TRADES_PATH),
        output_path   = output_path,
        date_unix_ms  = 0,   # 使用相对时间（测试用）
    )
    n = pipe.run()
    print(f"生成快照数: {n}")
    return pd.read_parquet(output_path)


def check_monotonicity(df: pd.DataFrame) -> None:
    """验证十档价格单调性（随机抽查20行）."""
    sample = df.sample(min(20, len(df)), random_state=42)
    errors = 0
    for _, row in sample.iterrows():
        # 买档降序：bid_px_1 >= bid_px_2 >= ...
        bid_prices = [row.get(f"bid_px_{i}", 0) for i in range(1, 11) if row.get(f"bid_px_{i}", 0) > 0]
        if bid_prices != sorted(bid_prices, reverse=True):
            print(f"  [WARN] 买档价格非降序 @ ts={row['timestamp_ms']}: {bid_prices}")
            errors += 1

        # 卖档升序：ask_px_1 <= ask_px_2 <= ...
        ask_prices = [row.get(f"ask_px_{i}", 0) for i in range(1, 11) if row.get(f"ask_px_{i}", 0) > 0]
        if ask_prices != sorted(ask_prices):
            print(f"  [WARN] 卖档价格非升序 @ ts={row['timestamp_ms']}: {ask_prices}")
            errors += 1
    print(f"单调性检查（抽查{len(sample)}行）: {errors} 处异常")


def main():
    print("=" * 60)
    print(f"输入委托: {ORDERS_PATH}")
    print(f"输入成交: {TRADES_PATH}")
    print("=" * 60)

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        output_path = f.name

    try:
        df = run_pipeline(output_path)
    finally:
        pass  # 保留文件方便检查

    print(f"\n输出 Parquet: {output_path}")
    print(f"总行数: {len(df)}")

    if df.empty:
        print("[FAIL] 未生成任何快照！")
        return

    print(f"时间范围: {df['timestamp_ms'].min()} ~ {df['timestamp_ms'].max()} ms")
    print(f"证券代码: {df['security_id'].unique()}")

    # 1. 时间戳对齐 50ms 网格
    misaligned = (df["timestamp_ms"] % 50 != 0).sum()
    if misaligned:
        print(f"[FAIL] 时间戳未对齐到50ms网格: {misaligned} 行")
    else:
        print("[PASS] 时间戳全部对齐到 50ms 网格")

    # 2. 快照内容抽样
    print("\n前3行快照摘要:")
    cols = ["timestamp_ms", "bid_px_1", "bid_vol_1", "ask_px_1", "ask_vol_1", "volume", "num_trades"]
    avail = [c for c in cols if c in df.columns]
    print(df[avail].head(3).to_string(index=False))

    # 3. 单调性检查
    print()
    check_monotonicity(df)

    # 4. 盘口非空率（至少买一或卖一有报价）
    has_quote = 0
    for _, row in df.iterrows():
        if row.get("bid_px_1", 0) > 0 or row.get("ask_px_1", 0) > 0:
            has_quote += 1
    print(f"盘口有效快照: {has_quote}/{len(df)} ({has_quote/len(df)*100:.1f}%)")

    # 5. 成交统计
    total_vol = df["volume"].sum() if "volume" in df.columns else 0
    total_trades = df["num_trades"].sum() if "num_trades" in df.columns else 0
    print(f"全日成交量: {total_vol}, 全日成交笔数: {total_trades}")

    print("\n[完成]")


if __name__ == "__main__":
    main()
