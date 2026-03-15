"""
全局常量配置
"""

# ─── 价格精度 ────────────────────────────────────────────────────────────────
PRICE_SCALE: int = 10_000       # 交易所原始整数价格放大倍数（×10000 = 4位小数）

# ─── 重采样参数 ───────────────────────────────────────────────────────────────
RESAMPLE_MS: int = 50           # 重采样间隔（毫秒）
TOP_LEVELS:  int = 10           # 输出档位数

# ─── 交易时段边界（秒数，从午夜零点起算）────────────────────────────────────
PHASE_BOUNDARIES = {
    "pre_open_start":        9 * 3600,                    #  9:00:00
    "opening_auction_start": 9 * 3600 + 15 * 60,         #  9:15:00
    "opening_auction_end":   9 * 3600 + 25 * 60,         #  9:25:00
    "continuous_am_start":   9 * 3600 + 30 * 60,         #  9:30:00
    "continuous_am_end":     11 * 3600 + 30 * 60,        # 11:30:00
    "continuous_pm_start":   13 * 3600,                  # 13:00:00
    "closing_auction_start": 14 * 3600 + 57 * 60,        # 14:57:00
    "market_close":          15 * 3600,                  # 15:00:00
    "after_hours_start":     15 * 3600 + 5 * 60,         # 15:05:00（创业板）
    "after_hours_end":       15 * 3600 + 30 * 60,        # 15:30:00
}

# ─── I/O 参数 ─────────────────────────────────────────────────────────────────
CSV_CHUNK_SIZE:       int = 200_000
PARQUET_ENGINE:       str = "pyarrow"
PARQUET_COMPRESSION:  str = "snappy"
