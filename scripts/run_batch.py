"""
批量 LOB 重建 CLI 脚本

使用示例：
    python scripts/run_batch.py \\
        --exchange SZSE \\
        --data-dir data/SZSE \\
        --output-dir output/SZSE \\
        --workers 8

期望数据目录结构：
    {data-dir}/{security_id}/{date}/orders.csv
    {data-dir}/{security_id}/{date}/trades.csv
"""
import sys
sys.path.insert(0, r"d:\LOB")

import argparse
import logging


def main():
    parser = argparse.ArgumentParser(
        description="LOB 订单簿批量重建",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--exchange",    required=True, choices=["SZSE", "SSE"])
    parser.add_argument("--data-dir",    required=True, help="逐笔数据根目录")
    parser.add_argument("--output-dir",  required=True, help="Parquet 输出根目录")
    parser.add_argument("--workers",     type=int, default=None,
                        help="并行进程数（默认=CPU 核心数）")
    parser.add_argument("--log-level",   default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(
        level   = getattr(logging, args.log_level),
        format  = "%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt = "%H:%M:%S",
    )

    from lob.models.order import Exchange
    from lob.pipeline.batch_runner import BatchRunner

    exchange = Exchange.SZSE if args.exchange == "SZSE" else Exchange.SSE

    runner = BatchRunner.from_directory(
        data_dir    = args.data_dir,
        output_dir  = args.output_dir,
        exchange    = exchange,
        max_workers = args.workers,
    )
    runner.run()


if __name__ == "__main__":
    main()
