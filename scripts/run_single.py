"""
单股票单日重建 CLI 脚本

使用示例：
    python scripts/run_single.py \\
        --exchange SZSE \\
        --security 000001 \\
        --orders data/000001/20230101/orders.csv \\
        --trades data/000001/20230101/trades.csv \\
        --output output/000001/20230101/lob_50ms.parquet \\
        --date 20230101
"""
import sys
sys.path.insert(0, r"d:\LOB")

import argparse
import logging
from datetime import datetime, timezone


def parse_date_to_unix_ms(date_str: str) -> int:
    """将 YYYYMMDD 转换为 UTC 午夜 Unix 毫秒（中国时区 UTC+8）。"""
    if not date_str:
        return 0
    try:
        dt = datetime.strptime(date_str, "%Y%m%d")
        # 中国标准时间 UTC+8，取 00:00:00 CST = 前一日 16:00:00 UTC
        # 但对于本系统，时间戳使用"自午夜起纳秒"的相对格式，不需要 UTC 转换
        # 若需要 Unix 绝对时间戳，可在此处添加 UTC+8 偏移
        return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000) - 8 * 3600 * 1000
    except ValueError:
        return 0


def main():
    parser = argparse.ArgumentParser(
        description="LOB 订单簿重建：单股票单日",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--exchange",  required=True, choices=["SZSE", "SSE"],
                        help="交易所：SZSE（深交所）或 SSE（上交所）")
    parser.add_argument("--security",  required=True, help="证券代码（如 000001）")
    parser.add_argument("--orders",    required=True, help="逐笔委托 CSV 文件路径")
    parser.add_argument("--trades",    required=True, help="逐笔成交 CSV 文件路径")
    parser.add_argument("--output",    required=True, help="输出 Parquet 文件路径")
    parser.add_argument("--date",      default="",    help="交易日 YYYYMMDD（可选，用于生成 Unix 时间戳）")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(
        level   = getattr(logging, args.log_level),
        format  = "%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt = "%H:%M:%S",
    )

    from lob.models.order import Exchange
    from lob.pipeline.pipeline import SingleSecurityPipeline

    exchange     = Exchange.SZSE if args.exchange == "SZSE" else Exchange.SSE
    date_unix_ms = parse_date_to_unix_ms(args.date)

    pipeline = SingleSecurityPipeline(
        exchange      = exchange,
        security_id   = args.security,
        orders_path   = args.orders,
        trades_path   = args.trades,
        output_path   = args.output,
        date_unix_ms  = date_unix_ms,
    )
    n = pipeline.run()
    print(f"完成：生成 {n} 个 50ms 快照 → {args.output}")


if __name__ == "__main__":
    main()
