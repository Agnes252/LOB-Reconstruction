"""
沪深两市逐笔数据字段映射与协议常量

每个映射字典格式：
    { "原始CSV列名": ("内部字段名", python类型) }
"""

from typing import Dict, Tuple, Any

ColumnMap = Dict[str, Tuple[str, Any]]

# ──────────────────────────────────────────────────────────────────────────────
# 深交所 STEP 协议
# ──────────────────────────────────────────────────────────────────────────────

SZSE_ORDER_COLUMNS: ColumnMap = {
    "ApplSeqNum":   ("seq_num",        int),
    "SecurityID":   ("security_id",    str),
    "TransactTime": ("time_raw",       int),   # HHMMSSMMM 9位整数
    "Price":        ("price",          int),   # 已×10000；市价单=0或-1
    "OrderQty":     ("qty",            int),
    "Side":         ("side_raw",       str),   # '1'=买 '2'=卖
    "OrdType":      ("ord_type_raw",   str),   # '1'=市价 '2'=限价 'U'=本方最优
    "ChannelNo":    ("channel_no",     int),   # 逐笔数据通道号
}

SZSE_TRADE_COLUMNS: ColumnMap = {
    "ApplSeqNum":        ("seq_num",    int),
    "SecurityID":        ("security_id", str),
    "TransactTime":      ("time_raw",   int),
    "BidApplSeqNum":     ("bid_seq",    int),
    "OfferApplSeqNum":   ("ask_seq",    int),
    "LastPx":            ("price",      int),  # 已×10000
    "LastQty":           ("qty",        int),
    "ExecType":          ("exec_type",  str),  # 'F'=成交 '4'=撤单
    "ChannelNo":         ("channel_no", int),  # 逐笔数据通道号（与order同域内唯一编号）
}

# 深交所委托方向映射
SZSE_SIDE_MAP = {"1": "bid", "2": "ask"}

# 深交所委托类型映射
SZSE_ORD_TYPE_MAP = {
    "1": "market",
    "2": "limit",
    "U": "own_best",   # 本方最优：买单用当前买一价，卖单用当前卖一价；若同侧无挂单则废单
}

# 深交所交易阶段代码
SZSE_PHASE_CODES = {
    "S": 0,   # 启动/开市前
    "O": 1,   # 开盘集合竞价
    "B": 2,   # 休市（集合竞价后/中午）
    "T": 3,   # 连续竞价
    "C": 6,   # 收盘集合竞价
    "A": 7,   # 盘后交易
    "E": 8,   # 已闭市
    "V": 9,   # 波动性中断
    "H": 10,  # 临时停牌
}

# ──────────────────────────────────────────────────────────────────────────────
# 上交所 LDDS 协议
# ──────────────────────────────────────────────────────────────────────────────

SSE_ORDER_COLUMNS: ColumnMap = {
    "ApplSeqNum":   ("seq_num",        int),
    "SecurityID":   ("security_id",    str),
    "TransactTime": ("time_raw",       int),   # HHMMSSMMM 9位整数
    "Price":        ("price",          int),   # 已×10000
    "OrderQty":     ("qty",            int),
    "Side":         ("side_raw",       str),   # '1'=买 '2'=卖
    "OrdType":      ("ord_type_raw",   str),   # '1'=市价 '2'=限价
    "OrderNo":      ("order_no",       int),   # 上交所特有订单编号（用于trade反查）
    "Channel":      ("channel_no",     int),   # 通道号
    "BizIndex":     ("biz_index",      int),   # 统一序号：order/trade在同一channel的唯一排序键
    # CancelFlag 为可选字段；若不存在则用空字符串
}

SSE_TRADE_COLUMNS: ColumnMap = {
    "TradeIndex":    ("seq_num",        int),
    "SecurityID":    ("security_id",    str),
    "TransactTime":  ("time_raw",       int),
    "BuyNo":         ("bid_seq",        int),  # 对应原始order的OrderNo（买方）
    "SellNo":        ("ask_seq",        int),  # 对应原始order的OrderNo（卖方）
    "TradePrice":    ("price",          int),  # 已×10000
    "TradeQty":      ("qty",            int),
    "TradeMoney":    ("turnover",       float),
    "TradeBSFlag":   ("trade_bs_flag",  str),  # 'B'=主买 'S'=主卖 'N'=集合竞价
    "TradeChannel":  ("channel_no",     int),  # 通道号（与order的Channel含义一致）
    "BizIndex":      ("biz_index",      int),  # 统一序号（与order的BizIndex统一编号）
}

# 上交所委托方向映射（与深交所相同）
SSE_SIDE_MAP = {"1": "bid", "2": "ask"}

# 上交所委托类型映射
SSE_ORD_TYPE_MAP = {
    "1": "market",
    "2": "limit",
}

# 上交所交易阶段代码
SSE_PHASE_CODES = {
    "S": 0,   # 启动/开市前
    "C": 1,   # 开盘集合竞价
    "B": 2,   # 休市
    "T": 3,   # 连续交易
    "U": 6,   # 收盘集合竞价
    "E": 8,   # 闭市
    "V": 9,   # 波动性中断
    "P": 10,  # 产品停牌
    "M": 11,  # 熔断
    "N": 11,  # 熔断
}
