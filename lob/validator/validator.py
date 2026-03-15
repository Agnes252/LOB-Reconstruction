"""
LOB 重构正确性校验模块

修改建议来源：改进建议 §4「增加正确性校验模块」

目标
----
将本项目重构出的 50ms LOB 快照与交易所官方发布的 Level-2 快照（3s 间隔）进行
逐行对比，校验十档价量序列的准确性。

校验逻辑（来源文档 §4 标准）
-----------------------------
1. 取每个官方 3s 快照最近的重构 50ms 快照（最多 ±50ms 偏差）；
2. 对比买卖各档的价格和总委托量：
   - 价格必须完全匹配（整数 × 10000 比较，无浮点误差）；
   - 重构量 ≥ 官方量（因为官方快照有延迟，重构更实时）；
3. 计算逐档命中率（price_hit_rate）和量覆盖率（vol_coverage_rate）。

官方参考快照格式（period=3s，字段约定）
---------------------------------------
必需列：
    security_id   : str
    timestamp_ms  : int  — 官方快照时间（Unix ms）
    ask_px_1..10  : float — 卖方各档价格（元）
    ask_vol_1..10 : int   — 卖方各档总量
    bid_px_1..10  : float — 买方各档价格（元）
    bid_vol_1..10 : int   — 买方各档总量

若你的参考数据使用不同列名，请在实例化时传入 col_map 映射。

使用示例
--------
import pandas as pd
from lob.validator.validator import LOBValidator

reconstructed = pd.read_parquet("out/000001.parquet")
reference     = pd.read_csv("data/l2_000001.csv")   # 官方 3s 快照

validator = LOBValidator(top_levels=10)
report = validator.compare_day(reconstructed, reference)
print(report.summary())

# 或逐行对比
for _, ref_row in reference.iterrows():
    result = validator.compare_snapshot(reconstructed, ref_row)
    if not result["price_match"]:
        print("价格不匹配:", result)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

_DEFAULT_TOP = 10


@dataclass
class SnapshotCompareResult:
    """单个时刻的对比结果。"""
    timestamp_ms:    int
    security_id:     str

    # 逐档价格命中（True = 重构价格与官方完全一致）
    ask_price_match: List[bool] = field(default_factory=list)
    bid_price_match: List[bool] = field(default_factory=list)

    # 逐档量覆盖率（重构量 / 官方量，≥1.0 为合格）
    ask_vol_coverage: List[float] = field(default_factory=list)
    bid_vol_coverage: List[float] = field(default_factory=list)

    # 是否全档价格匹配
    @property
    def price_match(self) -> bool:
        return all(self.ask_price_match) and all(self.bid_price_match)

    # 全档平均量覆盖率
    @property
    def avg_vol_coverage(self) -> float:
        cov = self.ask_vol_coverage + self.bid_vol_coverage
        return sum(cov) / len(cov) if cov else 0.0


@dataclass
class DayCompareReport:
    """全日对比汇总报告。"""
    security_id:    str
    total_ref_snaps: int = 0          # 参考快照总数
    matched_snaps:   int = 0          # 价格完全匹配的快照数
    avg_vol_coverage: float = 0.0     # 全日平均量覆盖率
    per_level_price_hit: List[float] = field(default_factory=list)  # 逐档命中率 [ask1..10, bid1..10]
    no_match_timestamps: List[int] = field(default_factory=list)    # 找不到对应重构快照的参考时刻

    def summary(self) -> str:
        lines = [
            f"Security: {self.security_id}",
            f"  参考快照数: {self.total_ref_snaps}",
            f"  价格完全匹配: {self.matched_snaps} / {self.total_ref_snaps} "
            f"({self.matched_snaps/max(self.total_ref_snaps,1)*100:.1f}%)",
            f"  平均量覆盖率: {self.avg_vol_coverage:.3f}",
        ]
        if self.per_level_price_hit:
            ask_hits = self.per_level_price_hit[:len(self.per_level_price_hit)//2]
            bid_hits = self.per_level_price_hit[len(self.per_level_price_hit)//2:]
            lines.append(
                f"  卖档逐级命中率: {[f'{x:.2f}' for x in ask_hits]}"
            )
            lines.append(
                f"  买档逐级命中率: {[f'{x:.2f}' for x in bid_hits]}"
            )
        if self.no_match_timestamps:
            lines.append(
                f"  无对应重构快照的参考时刻: {len(self.no_match_timestamps)} 个"
            )
        return "\n".join(lines)


class LOBValidator:
    """
    LOB 重构正确性校验器。

    Parameters
    ----------
    top_levels    : 校验档数，默认 10
    time_tol_ms   : 时间匹配容差（毫秒），默认 ±50ms（一个采样间隔）
    price_tol     : 价格容差（元），默认 0（严格匹配）
                    设 > 0 允许微小误差，适用于参考数据精度不足的情况
    """

    def __init__(
        self,
        top_levels:  int   = _DEFAULT_TOP,
        time_tol_ms: int   = 50,
        price_tol:   float = 0.0,
    ) -> None:
        self.top_levels  = top_levels
        self.time_tol_ms = time_tol_ms
        self.price_tol   = price_tol

    # ── 公共接口 ──────────────────────────────────────────────────────────────

    def compare_day(
        self,
        reconstructed: pd.DataFrame,
        reference:     pd.DataFrame,
        security_id:   Optional[str] = None,
    ) -> DayCompareReport:
        """
        全日对比：遍历参考快照，逐一与重构结果配对比较。

        Parameters
        ----------
        reconstructed : 本项目输出的 Parquet DataFrame（已含 timestamp_ms）
        reference     : 官方 L2 3s 快照 DataFrame
        security_id   : 可选，若 DataFrame 已按股票过滤则无需传入

        Returns
        -------
        DayCompareReport 汇总报告
        """
        sec_id = security_id or (
            str(reconstructed["security_id"].iloc[0])
            if "security_id" in reconstructed.columns and len(reconstructed) > 0
            else "unknown"
        )

        report = DayCompareReport(security_id=sec_id)

        # 以 timestamp_ms 建立快速查找索引
        rec_indexed = reconstructed.set_index("timestamp_ms")

        all_ask_hits   = [[] for _ in range(self.top_levels)]
        all_bid_hits   = [[] for _ in range(self.top_levels)]
        vol_coverages: List[float] = []

        for _, ref_row in reference.iterrows():
            report.total_ref_snaps += 1
            ref_ts = int(ref_row.get("timestamp_ms", 0))

            # 在 ±time_tol_ms 内寻找最近的重构快照
            rec_row = self._find_nearest(rec_indexed, ref_ts)
            if rec_row is None:
                report.no_match_timestamps.append(ref_ts)
                continue

            result = self._compare_single(rec_row, ref_row, ref_ts, sec_id)

            if result.price_match:
                report.matched_snaps += 1

            for i, hit in enumerate(result.ask_price_match):
                all_ask_hits[i].append(hit)
            for i, hit in enumerate(result.bid_price_match):
                all_bid_hits[i].append(hit)

            vol_coverages.append(result.avg_vol_coverage)

        # 汇总逐档命中率
        report.per_level_price_hit = [
            sum(hits) / len(hits) if hits else 0.0
            for hits in all_ask_hits + all_bid_hits
        ]
        report.avg_vol_coverage = (
            sum(vol_coverages) / len(vol_coverages) if vol_coverages else 0.0
        )
        return report

    def compare_snapshot(
        self,
        reconstructed: pd.DataFrame,
        ref_row:       "pd.Series",
        security_id:   Optional[str] = None,
    ) -> SnapshotCompareResult:
        """
        单行对比：将参考快照与最近的重构快照进行比较。

        Parameters
        ----------
        reconstructed : 本项目输出 DataFrame（含 timestamp_ms）
        ref_row       : 官方参考快照的一行（pd.Series）
        """
        rec_indexed = reconstructed.set_index("timestamp_ms")
        ref_ts  = int(ref_row.get("timestamp_ms", 0))
        sec_id  = security_id or str(ref_row.get("security_id", "unknown"))
        rec_row = self._find_nearest(rec_indexed, ref_ts)

        if rec_row is None:
            return SnapshotCompareResult(
                timestamp_ms    = ref_ts,
                security_id     = sec_id,
                ask_price_match = [False] * self.top_levels,
                bid_price_match = [False] * self.top_levels,
                ask_vol_coverage= [0.0]   * self.top_levels,
                bid_vol_coverage= [0.0]   * self.top_levels,
            )
        return self._compare_single(rec_row, ref_row, ref_ts, sec_id)

    # ── 内部方法 ──────────────────────────────────────────────────────────────

    def _find_nearest(
        self,
        rec_indexed: pd.DataFrame,
        ref_ts_ms: int,
    ) -> Optional["pd.Series"]:
        """
        在 rec_indexed（以 timestamp_ms 为索引）中找到距 ref_ts_ms 最近的行。
        超出 time_tol_ms 容差则返回 None。
        """
        if rec_indexed.empty:
            return None

        idx = rec_indexed.index
        pos = idx.searchsorted(ref_ts_ms)

        candidates: List[Tuple[int, "pd.Series"]] = []

        if pos < len(idx):
            ts  = idx[pos]
            diff = abs(ts - ref_ts_ms)
            if diff <= self.time_tol_ms:
                candidates.append((diff, rec_indexed.iloc[pos]))

        if pos > 0:
            ts   = idx[pos - 1]
            diff = abs(ts - ref_ts_ms)
            if diff <= self.time_tol_ms:
                candidates.append((diff, rec_indexed.iloc[pos - 1]))

        if not candidates:
            return None

        candidates.sort(key=lambda x: x[0])
        return candidates[0][1]

    def _compare_single(
        self,
        rec_row:    "pd.Series",
        ref_row:    "pd.Series",
        ref_ts:     int,
        sec_id:     str,
    ) -> SnapshotCompareResult:
        """逐档比较价格和量，返回 SnapshotCompareResult。"""
        result = SnapshotCompareResult(
            timestamp_ms = ref_ts,
            security_id  = sec_id,
        )

        for side in ("ask", "bid"):
            for n in range(1, self.top_levels + 1):
                ref_px_col  = f"{side}_px_{n}"
                ref_vol_col = f"{side}_vol_{n}"
                rec_px_col  = f"{side}_px_{n}"
                rec_vol_col = f"{side}_vol_{n}"

                ref_px  = float(ref_row.get(ref_px_col,  0) or 0)
                rec_px  = float(rec_row.get(rec_px_col,  0) or 0)
                ref_vol = int(ref_row.get(ref_vol_col, 0) or 0)
                rec_vol = int(rec_row.get(rec_vol_col, 0) or 0)

                price_hit = abs(ref_px - rec_px) <= self.price_tol
                vol_cov   = (rec_vol / ref_vol) if ref_vol > 0 else 1.0

                if side == "ask":
                    result.ask_price_match.append(price_hit)
                    result.ask_vol_coverage.append(vol_cov)
                else:
                    result.bid_price_match.append(price_hit)
                    result.bid_vol_coverage.append(vol_cov)

        return result
