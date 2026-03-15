"""
批量多股票多日 LOB 重建运行器

支持按目录组织的数据文件，通过 ProcessPoolExecutor 并行处理多只股票。

文件组织约定：
    {data_dir}/{exchange}/{security_id}/{date}/orders.csv
    {data_dir}/{exchange}/{security_id}/{date}/trades.csv

输出约定：
    {output_dir}/{exchange}/{security_id}/{date}/lob_50ms.parquet
"""
from __future__ import annotations

import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from lob.models.order import Exchange
from lob.pipeline.pipeline import SingleSecurityPipeline

logger = logging.getLogger(__name__)


@dataclass
class BatchTask:
    """单个批量处理任务描述。"""
    exchange:     Exchange
    security_id:  str
    orders_path:  str
    trades_path:  str
    output_path:  str
    date_unix_ms: int = 0


def _run_task(task: BatchTask) -> tuple:
    """在子进程中执行单个任务（供 ProcessPoolExecutor 调用）。"""
    try:
        pipeline = SingleSecurityPipeline(
            exchange      = task.exchange,
            security_id   = task.security_id,
            orders_path   = task.orders_path,
            trades_path   = task.trades_path,
            output_path   = task.output_path,
            date_unix_ms  = task.date_unix_ms,
        )
        n = pipeline.run()
        return (task.security_id, n, None)
    except Exception as exc:
        return (task.security_id, 0, str(exc))


class BatchRunner:
    """
    批量 LOB 重建运行器。

    Parameters
    ----------
    tasks      : BatchTask 列表
    max_workers: 并行进程数（None = CPU 核心数）
    """

    def __init__(
        self,
        tasks:       List[BatchTask],
        max_workers: Optional[int] = None,
    ) -> None:
        self.tasks       = tasks
        self.max_workers = max_workers

    def run(self) -> None:
        """并行执行所有任务，记录成功/失败统计。"""
        total   = len(self.tasks)
        success = 0
        failed  = 0

        logger.info("批量启动 %d 个重建任务，并行度=%s", total, self.max_workers)

        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(_run_task, t): t for t in self.tasks}
            for fut in as_completed(futures):
                sec_id, n_snaps, err = fut.result()
                if err:
                    logger.error("任务失败 [%s]: %s", sec_id, err)
                    failed += 1
                else:
                    logger.info("任务完成 [%s]: %d 个快照", sec_id, n_snaps)
                    success += 1

        logger.info("批量完成：成功 %d / 失败 %d / 总计 %d", success, failed, total)

    @classmethod
    def from_directory(
        cls,
        data_dir:    str,
        output_dir:  str,
        exchange:    Exchange,
        max_workers: Optional[int] = None,
    ) -> "BatchRunner":
        """
        扫描目录自动构建任务列表。

        期望目录结构：
            {data_dir}/{security_id}/{date}/orders.csv
            {data_dir}/{security_id}/{date}/trades.csv
        """
        tasks: List[BatchTask] = []
        base = Path(data_dir)

        for sec_dir in sorted(base.iterdir()):
            if not sec_dir.is_dir():
                continue
            security_id = sec_dir.name

            for date_dir in sorted(sec_dir.iterdir()):
                if not date_dir.is_dir():
                    continue
                date_str = date_dir.name

                orders_path = date_dir / "orders.csv"
                trades_path = date_dir / "trades.csv"

                if not orders_path.exists() or not trades_path.exists():
                    logger.warning("缺少数据文件，跳过: %s/%s", security_id, date_str)
                    continue

                out_path = (
                    Path(output_dir) / security_id / date_str / "lob_50ms.parquet"
                )
                tasks.append(BatchTask(
                    exchange     = exchange,
                    security_id  = security_id,
                    orders_path  = str(orders_path),
                    trades_path  = str(trades_path),
                    output_path  = str(out_path),
                ))

        logger.info("发现 %d 个任务", len(tasks))
        return cls(tasks, max_workers=max_workers)
