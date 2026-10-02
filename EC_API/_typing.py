from typing import TypeVar, Any, Callable, TypeAlias, Final

# ------ MsgType ------
ServerMsgType = TypeVar("ServerMsgType")
ClientMsgType = TypeVar("ClientMsgType")

# ------ Routers ------
# Router Keys (Universal)
RouterKey = tuple[str, str, str, Any]

RK_MSG_FAMILY = 0
RK_MSG_TYPE = 1
RK_ID_FIELD_NAME = 2
RK_ID = 3

# Key Hit (Universal)
KeyHit = tuple[str, int | None, bool, bool]

KH_MSG_FAMILY = 0
KH_MSG_TYPE = 1
KH_IS_REPEATED = 2
KH_IS_FIELD = 3

type Extractor_func[ServerMsgType] = Callable[[ServerMsgType, str], Any]

# ------ Builders ------
type Builder_func[ClientMsgType] = Callable[[Any, ClientMsgType], Any]

_FieldSpec = dict[str, tuple[str, type | tuple[type, ...] | Any, Callable[..., Any] | None]]

# ------ Parsers ------
type Parser_func[ServerMsgType] = Callable[[ServerMsgType], Any]

# ----- OrderStatuses ---
OrderStatusType: TypeAlias = dict[str, Any]  # tuple
PositionStatusType: TypeAlias = dict[str, Any]  # tuple
AccountSummaryType: TypeAlias = dict[str, Any]  # tuple

# ----- Channel -----
OrderIntent: TypeAlias = tuple[int, dict[str, Any]] # (Request_type, Order_Info)


# ====== Vendor-Specific Types ======
# ------------- CQG -----------------
# ===================================

# ------ Connect (CQG) ------
LogonResultType = dict[str, Any]
LogoffResultType = dict[str, Any]
RestoreResultType = dict[str, Any]

# Pong tuple layout (Universal)
PongType = tuple[str, str, int, int]

PONG_MSG_NAME: Final = 0
PONG_TOKEN: Final = 1
PONG_PINGTIME: Final = 2
PONG_PONGTIME: Final = 3

# Connect:metadata
ContractMetaDataType = dict[str, Any]

# ------ Monitor (CQG) ------
# Quote tick tuple layout (CQG) # Top Level
QuotesValueTypeCQG: TypeAlias = tuple

Q_CONTRACT_ID = 0  # [0]  contract_id              int
Q_TYPE = 1  # [1]  type                     int
Q_UTC_TIME = 2  # [2]  quote_utc_time           int   (microseconds since epoch)
Q_SCALED_PRICE = 3  # [3]  scaled_price             int
Q_SCALED_SOURCE_PRICE = 4  # [4]  scaled_source_price      int
Q_PRICE_YIELD = 5  # [5]  price_yield              float
Q_VOL_SIGNIFICAND = 6  # [6]  vol_significand          int
Q_VOL_EXPONENT = 7  # [7]  vol_exponent             int
Q_INDICATORS = 8  # [8]  indicators               tuple[int, ...]
Q_SALES_CONDITION = 9  # [9]  sales_condition          int
Q_TRADE_ATTRS = 10  # [10] trade_attributes         tuple (TradeAttrsCQG)
Q_SCALED_CURRENCY_RATE_PRICE = 11  # [11] scaled_currency_rate     int
Q_SCALED_PREMIUM = 12  # [12] scaled_premium           int
Q_MARKET_STATE = 13  # [13] market_state             tuple (MarketStateCQG)
Q_CORRECT_PRICE_SCALE = 14  # [14] correct_price_scale      float
Q_IS_SNAPSHOT = 15  # [15] is_snapshot              bool

# MarketValues tick tuple layout (CQG)
MarketValueTypeCQG: TypeAlias = tuple

MV_CONTRACT_ID = 0  # [0]  contract_id                      int
MV_SCALED_OPEN = 1  # [1]  scaled_open_price                int
MV_SCALED_HIGH = 2  # [2]  scaled_high_price                int
MV_SCALED_LOW = 3  # [3]  scaled_low_price                 int
MV_SCALED_CLOSE = 4  # [4]  scaled_close_price               int
MV_VOL_SIGNIFICAND = 5  # [5]  vol_significand                  int
MV_VOL_EXPONENT = 6  # [6]  vol_exponent                     int
MV_LAST_PRICE_NO_SETTLEMENT = 7  # [7]  scaled_last_price_no_settlement  int
MV_SCALE_EX_CLOSE_PRICE = 8  # [8]  scaled_exchange_close_price      int
MV_SCALED_YESTERDAY_SETTLEMENT = 9  # [9]  scaled_yesterday_settlement      int
MV_SCALED_INDICATIVE_OPEN = 10  # [10] scaled_indicative_open           int
MV_OI_SIGNIFICAND = 11  # [11] open_interest_significand        int
MV_OI_EXPONENT = 12  # [12] open_interest_exponent           int
MV_INDICATIVE_OI_VOL_SIGNIFICAND = 13  # [13] indicative_open_vol_significand  int
MV_INDICATIVE_OI_VOL_EXPONENT = 14  # [14] indicative_open_vol_exponent     int
MV_TICK_VOL = 15  # [15] tick_volume                      int
MV_SCALED_SETTLEMENT = 16  # [16] scaled_settlement                int
MV_SCALED_MARKER_PRICE = 17  # [17] scaled_marker_price              int
MV_SCALED_LAST_TRADE_PRICE = 18  # [18] scaled_last_trade_price          int
MV_SCALED_TRADE_VOL_SIGNIFICAND = 19  # [19] last_trade_vol_significand       int
MV_SCALED_TRADE_VOL_EXPONENT = 20  # [20] last_trade_vol_exponent          int
MV_LAST_TRADE_UTC_TIMESTAMP = 21  # [21] last_trade_utc_timestamp         float  (seconds.nanos)
MV_CLEARED_FIELDS = 22  # [22] cleared_fields                   tuple[int, ...]
MV_TRADE_DATE = 23  # [23] trade_date                       int
MV_SESSION_INDEX = 24  # [24] session_index                    int
MV_MARKET_YIELDS = 25  # [25] market_yields                    tuple  (MarketYieldsCQG)
MV_SCALED_CURRENCY_RATE_PRICE = 26  # [26] scaled_currency_rate_price       int
MV_MARKET_STATE = 27  # [27] market_state                     tuple  (MarketStateCQG)
MV_CORRECT_PRICE_SCALE = 28  # [28] correct_price_scale              float
MV_IS_SNAPSHOT = 29  # [29] is_snapshot                      bool

# DetailedDOMAtPriceCQG
DOMValueTypeCQG = tuple[int]  # scaled_price

# MarketYieldsCQG layout (nested at [25] of MarketValueTypeCQG): # Inner sub-tuples
MarketYieldsCQG: TypeAlias = tuple

MY_YIELD_OPEN_PRICE = 0  # [0]  yield_of_open_price              float
MY_YIELD_HIGH_PRICE = 1  # [1]  yield_of_high_price              float
MY_YIELD_LOW_PRICE = 2  # [2]  yield_of_low_price               float
MY_YIELD_CLOSE_PRICE = 3  # [3]  yield_of_close_price             float
MY_YIELD_YESTERDAY_SETTLEMENT = 4  # [4]  yield_of_yesterday_settlement    float
MY_YIELD_INDICATIVE_OPEN = 5  # [5]  yield_of_indicative_open         float
MY_YIELD_SETTLEMENT = 6  # [6]  yield_of_settlement              float

# MarketStateCQG layout (nested in quotes [13] and market_values [27]):
MarketStateCQG: TypeAlias = tuple

MS_EXCHANGE_STATE = (
    0  # [0]  exchange_state                   int    (PRE_OPEN/OPEN/CLOSED/HALTED/SUSPENDED)
)
MS_ALLOW_PLACE_ORDER = 1  # [1]  allow_place_order                bool
MS_ALLOW_CANCEL_ORDER = 2  # [2]  allow_cancel_order               bool
MS_ALLOW_MODIFY_ORDER = 3  # [3]  allow_modify_order               bool
MS_MATCH_ENABLED = 4  # [4]  matching_enabled                 bool
MS_IS_SNAPSHOT = 5  # [5]  is_snapshot                      bool

# TradeAttrsCQG layout (...)
TradeAttrsCQG: TypeAlias = tuple

TA_BUYER = 0  # [0]  buyer                    int
TA_SELLER = 1  # [1]  seller                   int
TA_TRADE_TYPE = 2  # [2]  trade_type               str
TA_MATCH_ID = 3  # [3]  match_id                 str
TA_AGREEMENT_TIME_UTC = 4  # [4]  agreement_time_utc       float (seconds.nanos)

# General container for market data
ParsedRTMDCQG = tuple[
    list[QuotesValueTypeCQG],
    list[MarketValueTypeCQG],
    list[DOMValueTypeCQG],
    list[QuotesValueTypeCQG],  # corrections field
]

# ------ LiveOrder ------
ChainOrderId: TypeAlias = str  # OS_CHAIN_ORDER_ID field value
ClOrderId: TypeAlias = str  # OS_CL_ORDER_ID field value
OrderId: TypeAlias = str  # OS_ORDER_ID field value
ContractId: TypeAlias = int  # PS_CONTRACT_ID field value

OrderStatusTypeCQG: TypeAlias = dict[str, Any]  # tuple
PositionStatusTypeCQG: TypeAlias = dict[str, Any]  # tuple
AccountSummaryTypeCQG: TypeAlias = dict[str, Any]  # tuple


OS_CHAIN_ORDER_ID = 0  # [0]  chain_order_id      str
OS_CL_ORDER_ID = 1  # [1]  cl_order_id         str   (from order sub-field, promoted)
OS_ORDER_ID = 2  # [2]  order_id            str   (exchange-assigned, changes on modify)
OS_ACCOUNT_ID = 3  # [3]  account_id          int
OS_CONTRACT_ID = 4  # [4]  contract_id         int   (from order sub-field)
OS_STATUS_CODE = 5  # [5]  status_code         int
OS_SIDE = 6  # [6]  side                int
OS_ORDER_TYPE = 7  # [7]  order_type          int
OS_DURATION = 8  # [8]  duration            int
OS_QTY_SIGNIFICAND = 9  # [9]  qty_significand     int
OS_QTY_EXPONENT = 10  # [10] qty_exponent        int
OS_FILL_CNT = 11  # [11] fill_cnt            int
OS_SCALED_AVG_FILL = 12  # [12] scaled_avg_fill     int
OS_AVG_FILL_CORRECT = 13  # [13] avg_fill_correct    float
OS_STATUS_UTC_TS = 14  # [14] status_utc          int   (epoch ms)
OS_SUBMISSION_UTC_TS = 15  # [15] submission_utc      int   (epoch ms)
OS_SUB_IDS = 16  # [16] sub_ids             tuple[int, ...]


# OpenPositionTypeCQG — nested sub-tuple
OP_ID = 0  # int      surrogate key for updates
OP_QTY_SIGNIFICAND = 1  # int
OP_QTY_EXPONENT = 2  # int      qty=0 means this position is DELETED
OP_PRICE_CORRECT = 3  # float    average position price
OP_TRADE_DATE = 4  # int      epoch days
OP_STATEMENT_DATE = 5  # int      epoch days
OP_TRADE_UTC_TS = 6  # int      epoch ms
OP_IS_AGGREGATED = 7  # bool
OP_IS_SHORT = 8  # bool
OP_IS_YESTERDAY = 9  # bool
OP_SPECULATION_TYPE = 10  # int

# PositionStatusTypeCQG — top-level tuple

PS_SUB_IDS = 0  # tuple[int, ...]
PS_IS_SNAPSHOT = 1  # bool
PS_ACCOUNT_ID = 2  # int
PS_CONTRACT_ID = 3  # int
PS_OPEN_POSITIONS = 4  # tuple[OpenPositionTypeCQG, ...]

# AccountSummaryTypeCQG
