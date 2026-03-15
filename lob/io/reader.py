"""
CSV/TXT 分块读取器

支持大文件（单股票单日逐笔数据可超百万行）的内存安全读取。
通过 chunksize 分块并按需应用列名映射和类型转换。
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

import pandas as pd

from config.exchange_config import (
    ColumnMap,
    SZSE_ORDER_COLUMNS, SZSE_TRADE_COLUMNS,
    SSE_ORDER_COLUMNS,  SSE_TRADE_COLUMNS,
)
from config.settings import CSV_CHUNK_SIZE

logger = logging.getLogger(__name__)


def _build_dtype_map(col_map: ColumnMap) -> Dict[str, type]:
    """从字段映射中提取 pandas dtype 参数（str 类型不指定，让 pandas 自动处理）。"""
    dtype = {}
    for orig_col, (_, py_type) in col_map.items():
        if py_type == int:
            dtype[orig_col] = "Int64"    # 可空整数，避免 NaN 引发转换错误
        elif py_type == float:
            dtype[orig_col] = "float64"
        # str 类型不指定 dtype，保持默认 object
    return dtype


def _normalize_columns(df: pd.DataFrame, col_map: ColumnMap) -> pd.DataFrame:
    """
    1. 仅保留 col_map 中定义的列（忽略多余列）
    2. 重命名为内部字段名
    3. 按需填充缺失的可选列
    """
    # 支持大小写不敏感：先统一列名
    df.columns = [c.strip() for c in df.columns]

    # 找到实际存在的列（大小写不敏感）
    col_lower_map = {c.lower(): c for c in df.columns}
    rename_map = {}
    missing_optional = []

    for orig, (internal, _) in col_map.items():
        orig_lower = orig.lower()
        if orig_lower in col_lower_map:
            rename_map[col_lower_map[orig_lower]] = internal
        else:
            missing_optional.append(internal)

    df = df.rename(columns=rename_map)

    # 补充缺失的可选字段（填充空字符串或 0）
    for col in missing_optional:
        df[col] = ""

    return df[[v for _, (v, _) in col_map.items() if v in df.columns]]


def iter_csv_chunks(
    path: str,
    col_map: ColumnMap,
    chunksize: int = CSV_CHUNK_SIZE,
    sep: str = ",",
    encoding: str = "utf-8",
) -> Iterator[pd.DataFrame]:
    """
    分块迭代读取 CSV/TXT 文件，每块经过列名映射处理。

    Parameters
    ----------
    path     : 文件路径
    col_map  : 字段映射字典（来自 exchange_config.py）
    chunksize: 每块行数
    sep      : 列分隔符（默认逗号，TXT 文件可能为 '\t'）
    encoding : 文件编码
    """
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"数据文件不存在: {path}")

    dtype_map = _build_dtype_map(col_map)

    # 自动检测 TXT 文件的分隔符
    if file_path.suffix.lower() in (".txt", ".tsv"):
        sep = "\t"

    try:
        reader = pd.read_csv(
            path,
            sep         = sep,
            dtype       = dtype_map,
            chunksize   = chunksize,
            na_filter   = False,     # 不将空字符串转为 NaN
            encoding    = encoding,
            low_memory  = False,
        )
        for chunk in reader:
            yield _normalize_columns(chunk, col_map)
    except UnicodeDecodeError:
        # 尝试 GBK 编码（Wind 等国内数据源常用）
        logger.warning("UTF-8 解码失败，尝试 GBK 编码: %s", path)
        reader = pd.read_csv(
            path,
            sep         = sep,
            dtype       = dtype_map,
            chunksize   = chunksize,
            na_filter   = False,
            encoding    = "gbk",
            low_memory  = False,
        )
        for chunk in reader:
            yield _normalize_columns(chunk, col_map)


def read_szse_orders(path: str, **kwargs) -> Iterator[pd.DataFrame]:
    """读取深交所逐笔委托 CSV。"""
    return iter_csv_chunks(path, SZSE_ORDER_COLUMNS, **kwargs)

def read_szse_trades(path: str, **kwargs) -> Iterator[pd.DataFrame]:
    """读取深交所逐笔成交 CSV。"""
    return iter_csv_chunks(path, SZSE_TRADE_COLUMNS, **kwargs)

def read_sse_orders(path: str, **kwargs) -> Iterator[pd.DataFrame]:
    """读取上交所逐笔委托 CSV。"""
    return iter_csv_chunks(path, SSE_ORDER_COLUMNS, **kwargs)

def read_sse_trades(path: str, **kwargs) -> Iterator[pd.DataFrame]:
    """读取上交所逐笔成交 CSV。"""
    return iter_csv_chunks(path, SSE_TRADE_COLUMNS, **kwargs)
