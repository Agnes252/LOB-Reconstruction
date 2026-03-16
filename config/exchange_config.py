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
    "MDTime":       ("time_raw",       int),   # HHMMSSMMM 9位整数（如 091500000）
    "OrderPrice":   ("price",          float), # 浮点价格（如18.52），解析后×10000转整数
    "OrderQty":     ("qty",            int),
    "OrderBSFlag":  ("side_raw",       str),   # '1'=买 '2'=卖
    "OrderType":    ("ord_type_raw",   str),   # '1'=市价 '2'=限价 '3'=本方最优
    "ChannelNo":    ("channel_no",     int),   # 逐笔数据通道号
}

SZSE_TRADE_COLUMNS: ColumnMap = {
    "ApplSeqNum":   ("seq_num",        int),
    "SecurityID":   ("security_id",    str),
    "MDTime":       ("time_raw",       int),   # HHMMSSMMM 9位整数
    "TradeBuyNo":   ("bid_seq",        int),   # 买方委托 ApplSeqNum（撤单时对应被撤委托号，卖方撤单则为0）
    "TradeSellNo":  ("ask_seq",        int),   # 卖方委托 ApplSeqNum（撤单时对应被撤委托号，买方撤单则为0）
    "TradePrice":   ("price",          float), # 浮点成交价（撤单时为0），解析后×10000转整数
    "TradeQty":     ("qty",            int),
    "TradeMoney":   ("turnover",       float), # 成交金额（元）
    "TradeType":    ("exec_type",      str),   # '1'=撤销 '2'=成交（对应文档 ExecType：撤销=1 成交=2）
    "TradeBSFlag":  ("trade_bs_flag",  str),   # '1'=主买 '2'=主卖
    "ChannelNo":    ("channel_no",     int),   # 逐笔数据通道号（与order同域内唯一编号）
}

# 深交所委托方向映射
SZSE_SIDE_MAP = {"1": "bid", "2": "ask"}

# 深交所委托类型映射
SZSE_ORD_TYPE_MAP = {
    "1": "market",
    "2": "limit",
    "3": "own_best",   # 本方最优（数据中用整数 3）
    "U": "own_best",   # 本方最优（旧格式字母 U，保留兼容）
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
