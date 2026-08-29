#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  9 20:45:59 2026

@author: dexter

"""
from typing import Any
from datetime import datetime, timezone
from EC_API.ext.WebAPI.webapi_2_pb2 import ServerMsg
from EC_API.ext.common.shared_1_pb2 import OrderStatus, TransactionStatus
from EC_API.ext.WebAPI.user_session_2_pb2 import LogonResult as LgRes
from EC_API.ext.WebAPI.user_session_2_pb2 import RestoreOrJoinSessionResult as RstJoinSessRes
from EC_API.ext.WebAPI.user_session_2_pb2 import LoggedOff as LgOff
from EC_API.ext.WebAPI.webapi_2_pb2 import InformationReport as InfoRp
from EC_API.ext.WebAPI.trade_routing_2_pb2 import TradeSubscription as TrsSub
from EC_API.ext.WebAPI.trade_routing_2_pb2 import TradeSubscriptionStatus as TrdSubStatus
from EC_API.ext.WebAPI.order_2_pb2 import Order as Ord
from EC_API.ext.WebAPI.order_2_pb2 import GoFlatStatus as GFltStatus
from EC_API.ext.WebAPI.market_data_2_pb2 import MarketDataSubscriptionStatus as MktDSubStatus
from EC_API.ext.WebAPI.market_data_2_pb2 import MarketDataSubscription as MktDSub
from EC_API.ext.WebAPI.market_data_2_pb2 import Quote, TradingState
from EC_API.ext.WebAPI.historical_2_pb2 import TimeAndSalesReport as TSrRep
from EC_API.ext.WebAPI.historical_2_pb2 import BarReportStatusCode as BarRpStatusCode
from EC_API.ext.WebAPI.historical_2_pb2 import VolumeProfileReport as VolPrfRep
### ---- Connection ----
# (1) logon_result (v)
# (2) restore_or_join_session_result (v)
# (3) concurrent_connection_join_results (v)
# (4) logged_off (v)
# (5) pong (v)
###LgRes.ResultCode.RESULT_CODE_SUCCESS
def build_logon_result_server_msg(
        server_msg: ServerMsg,
        res_code: LgRes.ResultCode = LgRes.ResultCode.RESULT_CODE_SUCCESS
    ) -> ServerMsg:

    logon_result = server_msg.logon_result
    
    logon_result.result_code = res_code
    logon_result.base_time = datetime.now().isoformat()
    logon_result.protocol_version_minor = 101
    logon_result.protocol_version_major = 202
    logon_result.server_time = int(datetime.now().timestamp())
    return server_msg
    
def build_restore_or_join_session_result_server_msg(
        server_msg: ServerMsg,
        res_code: RstJoinSessRes.ResultCode = RstJoinSessRes.ResultCode.RESULT_CODE_SUCCESS
    ) -> ServerMsg:

    restore_or_join_session_result = server_msg.restore_or_join_session_result
    
    restore_or_join_session_result.result_code = res_code
    restore_or_join_session_result.base_time = datetime.now().isoformat()
    restore_or_join_session_result.server_time = int(datetime.now().timestamp())
    return server_msg

def build_concurrent_connection_join_results_server_msg(
        server_msg: ServerMsg,
        res: bool = True
    ) -> ServerMsg:  

    concurrent_connection_join_results = server_msg.concurrent_connection_join_results.add()
    concurrent_connection_join_results.is_same_app_type = res
    return server_msg

def build_logged_off_server_msg(
        server_msg: ServerMsg,
        res: LgOff.LogoffReason = LgOff.LogoffReason.LOGOFF_REASON_BY_REQUEST
    ) -> ServerMsg:
    logged_off = server_msg.logged_off
    logged_off.logoff_reason = res
    return server_msg

def build_ping_server_msg(
        server_msg: ServerMsg,
        token: str,
        ping_time: int = int(datetime.now().timestamp()*1000),
        ) -> ServerMsg:
    ping = server_msg.ping
    ping.token = token
    ping.ping_utc_time = ping_time
    return server_msg

def build_pong_server_msg(
        server_msg: ServerMsg,
        token: str,
        ping_time: int = int(datetime.now().timestamp()*1000),
        delay: int = 5
    ) -> ServerMsg:
    pong = server_msg.pong
    pong.token = token
    pong.ping_utc_time = ping_time
    pong.pong_utc_time = ping_time + delay
    return server_msg

### ---- information_report ----
# accounts_report
# (1) symbol_resolution_report (v)
# last_statement_balances_report
# currency_rates_report
# (2) session_information_report (v)
# (3) historical_orders_request (v)
# (4) option_maturity_list_report (v)
# (5) instrument_group_report (v)
# (6) at_the_money_strike_report (v)
# symbol_list_report
# symbol_report
# account_risk_parameters_report
# order_status_report
def build_symbol_resolution_report_server_msg(
        server_msg: ServerMsg,
        res: InfoRp.StatusCode = InfoRp.StatusCode.STATUS_CODE_SUCCESS,
        report_id: int = 1,
        cotract_id: int = 3,
        contract_symbol: str = "CLE",
    ) -> ServerMsg:
    
    information_report = server_msg.information_reports.add()
    information_report.id = report_id
    information_report.status_code = res
    
    symbol_resolution_report = server_msg.information_reports[0].symbol_resolution_report

    symbol_resolution_report.contract_metadata.contract_id = cotract_id
    symbol_resolution_report.contract_metadata.contract_symbol = contract_symbol
    symbol_resolution_report.contract_metadata.correct_price_scale = 100
    symbol_resolution_report.contract_metadata.display_price_scale = 200
    symbol_resolution_report.contract_metadata.description = "Desc"
    symbol_resolution_report.contract_metadata.title = "Test CLE title"
    symbol_resolution_report.contract_metadata.tick_size = 10
    symbol_resolution_report.contract_metadata.currency = "USD"
    symbol_resolution_report.contract_metadata.tick_value = 23
    symbol_resolution_report.contract_metadata.cfi_code = "cfi_code"
    symbol_resolution_report.contract_metadata.instrument_group_name = "Crude Oil"
    symbol_resolution_report.contract_metadata.session_info_id = 214
    symbol_resolution_report.contract_metadata.short_instrument_group_name = ""
    symbol_resolution_report.contract_metadata.instrument_group_description = ""
    symbol_resolution_report.contract_metadata.country_code = "AUS"
    return server_msg

def build_session_info_report_server_msg(
        server_msg: ServerMsg,
        res: InfoRp.StatusCode = InfoRp.StatusCode.STATUS_CODE_SUCCESS,
        report_id: int = 1,
    ) -> ServerMsg:

    information_report = server_msg.information_reports.add()
    information_report.id = report_id
    information_report.status_code = res

    session_information_report = server_msg.information_reports[0].session_information_report
    session_information_report.session_info_id = 330
    
    session_segments = session_information_report.session_segments.add()
    session_segments.session_segment_id = 1111
    
    return server_msg
    
def build_historical_orders_report_server_msg(
        server_msg: ServerMsg,
        res: InfoRp.StatusCode = InfoRp.StatusCode.STATUS_CODE_SUCCESS,
        report_id: int = 1,
        order_id: str = "1",
        chain_order_id: str = "A"
    ) -> ServerMsg:
    TS = datetime.now(timezone.utc)
    
    information_report = server_msg.information_reports.add()
    information_report.id = report_id
    information_report.status_code = res

    historical_orders_report_order_status = information_report.historical_orders_report.order_statuses.add()
    historical_orders_report_order_status.status = OrderStatus.Status.FILLED
    historical_orders_report_order_status.order_id = order_id
    historical_orders_report_order_status.chain_order_id = chain_order_id
    historical_orders_report_order_status.status_utc_timestamp = TS
    historical_orders_report_order_status.fill_cnt = 1
    return server_msg

def build_option_maturity_list_report_server_msg(
        server_msg: ServerMsg,
        res: InfoRp.StatusCode = InfoRp.StatusCode.STATUS_CODE_SUCCESS,
        report_id: int = 1,
        option_id: str = "id_1",
        option_name: str ="CLXXXX"
    ) -> ServerMsg:
    
    information_report = server_msg.information_reports.add()
    information_report.id = report_id

    information_report.status_code = res

    option_maturities = information_report.option_maturity_list_report.option_maturities.add()
    option_maturities.id = option_id
    option_maturities.name = option_name
    option_maturities.description = "description"
    return server_msg

def build_instrument_group_report_server_msg(
        server_msg: ServerMsg,
        res: InfoRp.StatusCode = InfoRp.StatusCode.STATUS_CODE_SUCCESS,
        report_id: int = 1,
        instrument_id: str = "id_1",
        instrument_name: str = "Instrument_1"
    ) -> ServerMsg:     
    information_report = server_msg.information_reports.add()
    information_report.id = report_id
    information_report.status_code = res

    instruments = information_report.instrument_group_report.instruments.add()
    instruments.id = instrument_id
    instruments.name = instrument_name
    instruments.description = "description"
    return server_msg

def build_at_the_money_strike_report_server_msg(
        server_msg: ServerMsg,
        res: InfoRp.StatusCode = InfoRp.StatusCode.STATUS_CODE_SUCCESS,
        report_id: int = 1,
    ) -> ServerMsg: 
    information_report = server_msg.information_reports.add()
    information_report.id = report_id
    information_report.status_code = res

    at_the_money_strike_report = information_report.at_the_money_strike_report
    at_the_money_strike_report.strike =100
    return server_msg
    
### ---- Orders----
# (1) order_request_rejects (v)
# (2) order_request_acks (v)
# (3) trade_subscription_statuses (v)
# (4) trade_snapshot_completions (v)
# (5) order_statuses (v)
# (6) position_statuses (v)
# (7) account_summary_statuses (v)
# (8) go_flat_statuses (v)
def build_order_request_rejects_server_msg(
        server_msg: ServerMsg,
        request_id: int = 1
    ) -> ServerMsg:    
    order_request_rejects = server_msg.order_request_rejects.add()
    order_request_rejects.request_id = request_id
    order_request_rejects.reject_code = 1001
    
    return server_msg

def build_order_request_acks_server_msg(
        server_msg: ServerMsg,
        request_id: int = 1
    ) -> ServerMsg:
    
    order_request_acks = server_msg.order_request_acks.add()
    order_request_acks.request_id = request_id
    order_request_acks.when.FromDatetime(datetime.now())
    return server_msg

def build_trade_subscription_statuses_server_msg(
        server_msg: ServerMsg,
        res: TrdSubStatus.StatusCode = TrdSubStatus.StatusCode.STATUS_CODE_SUCCESS,
        sub_id: int = 1
    ) -> ServerMsg:
    trade_subscription_statuses = server_msg.trade_subscription_statuses.add()
    trade_subscription_statuses.id = sub_id
    
    trade_subscription_statuses.status_code = res
    return server_msg

def build_trade_snapshot_completions_server_msg(
        server_msg: ServerMsg,
        sub_id: int = 1
    ) -> ServerMsg:
    trade_snapshot_completions = server_msg.trade_snapshot_completions.add()
    
    trade_snapshot_completions.subscription_id = sub_id
    
    sub_scope = trade_snapshot_completions.subscription_scopes
    sub_scope.append(TrsSub.SubscriptionScope.SUBSCRIPTION_SCOPE_ORDERS)
    sub_scope.append(TrsSub.SubscriptionScope.SUBSCRIPTION_SCOPE_POSITIONS)
    sub_scope.append(TrsSub.SubscriptionScope.SUBSCRIPTION_SCOPE_COLLATERAL)
    sub_scope.append(TrsSub.SubscriptionScope.SUBSCRIPTION_SCOPE_ACCOUNT_SUMMARY)
    return server_msg

def build_order_server_msg(
        cl_order_id: str = "cl_order_id_1",
        account_id: int = 123466,
        contract_id: int = 0,
        side: int = Ord.Side.SIDE_BUY,
        order_type: int = Ord.OrderType.ORDER_TYPE_LMT,
        duration: int = Ord.Duration.DURATION_GTC,
        qty: int = 2,
        scaled_limit_price: int = 150,
        is_manual: bool = False,
    ) -> Ord: #utility function to build Order, not called directly to make server_msg
    order = Ord()
    order.account_id = account_id
    order.contract_id = contract_id
    order.cl_order_id = cl_order_id
    order.side = side
    order.order_type = order_type
    order.duration = duration
    order.qty.significand = qty
    order.qty.exponent = 0
    order.scaled_limit_price = scaled_limit_price
    order.is_manual = is_manual
    order.when_utc_timestamp.FromDatetime(datetime.now())
    return order

def build_order_statuses_server_msg(
        server_msg: ServerMsg,
        res: OrderStatus.Status = OrderStatus.Status.IN_TRANSIT,
        contract_id: int = 0,
        sub_ids: list[int] = [1],
        order_id: str = "order_id_1",
        chain_order_id: str = "chain_order_id_1",
        order = None,
        account_id: int = 123466
    ) -> ServerMsg:
    
    order_statuses = server_msg.order_statuses.add()
    
    # ----
    for sub_id in sub_ids:
        order_statuses.subscription_ids.append(sub_id)
    order_statuses.status = res
    order_statuses.order_id = order_id
    order_statuses.chain_order_id = chain_order_id
    order_statuses.status_utc_timestamp.FromDatetime(datetime.now())
    order_statuses.submission_utc_timestamp.FromDatetime(datetime.now())
    order_statuses.fill_cnt = 0
    order_statuses.scaled_avg_fill_price = -200
    order_statuses.avg_fill_price_correct = 10
    #----
    transaction_statuses = order_statuses.transaction_statuses.add()
    transaction_statuses.status = TransactionStatus.Status.IN_TRANSIT
    transaction_statuses.trans_id = 2
    transaction_statuses.trans_utc_timestamp.FromDatetime(datetime.now())
    transaction_statuses.cl_order_id = "cl_order_id_1"
    
    trades = transaction_statuses.trades.add()
    trades.trade_id = "trade_id_1"
    trades.contract_id = contract_id
    trades.statement_date = int(datetime.now().timestamp())
    trades.trade_utc_timestamp.FromDatetime(datetime.now())
    
    trades.scaled_price = 1000 #price = round(price_correct / correct_price_scale)
    trades.price_correct = 100100
    trades.side = Ord.Side.SIDE_BUY
    # ----
    order_statuses.entered_by_user = "user_A"
    order_statuses.first_statement_date = int(datetime.now().timestamp())
   
    if order is not None: 
        order_statuses.order.CopyFrom(order)
    # ----
    contract_metadata = order_statuses.contract_metadata.add()
    contract_metadata.contract_id = contract_id
    contract_metadata.contract_symbol = "Symbol_1"
    contract_metadata.correct_price_scale = 0.01
    contract_metadata.display_price_scale = 202
    contract_metadata.description = "description"
    contract_metadata.title = "title"
    contract_metadata.tick_size = 10
    contract_metadata.currency = "USD"
    contract_metadata.tick_value = 0
    contract_metadata.cfi_code = "ESVUFB"
    contract_metadata.instrument_group_name = "instru_group_1"
    contract_metadata.session_info_id  = 232323
    contract_metadata.short_instrument_group_name = "short_instrument_group_name"
    contract_metadata.instrument_group_description = "instrument_group_description"
    contract_metadata.country_code = "US"
    # ----
    order_statuses.account_id = account_id
    return server_msg


def build_position_statuses_server_msg(
        server_msg: ServerMsg,
        subscription_ids: list[int] = [0,1,2],
        contract_id: int = 0,
        account_id: int = 123466,
        qty: int = 1,
        open_pos_not_empty: bool = True 
    ) -> ServerMsg:    
    
    position_statuses = server_msg.position_statuses.add()
    for num in subscription_ids:
        position_statuses.subscription_ids.append(num)
    
    position_statuses.account_id = account_id
    position_statuses.contract_id = contract_id
    position_statuses.is_short_open_position = False
    
    # ----
    if open_pos_not_empty:
        open_positions = position_statuses.open_positions.add()
        open_positions.id = 2
        open_positions.price_correct = 101
        open_positions.qty.significand = qty
        open_positions.qty.exponent = 0
        open_positions.trade_date = int(datetime.now().timestamp())
        open_positions.statement_date = int(datetime.now().timestamp())
        open_positions.is_aggregated = True
        open_positions.is_short = False
    
    # ----
    purchase_and_sales_groups = position_statuses.purchase_and_sales_groups.add()
    purchase_and_sales_groups.id = 4
    purchase_and_sales_groups.realized_profit_loss = 10
    
    matched_trades = purchase_and_sales_groups.matched_trades.add()
    matched_trades.price = 292
    matched_trades.trade_date = int(datetime.now().timestamp())
    matched_trades.statement_date = int(datetime.now().timestamp())
    matched_trades.is_aggregated = True
    # ----
    today_fill_commissions = position_statuses.today_fill_commissions.add()
    today_fill_commissions.commission_currency = "USD"
    today_fill_commissions.commission = 102
    return server_msg


def build_account_summary_statuses_server_msg(
        server_msg: ServerMsg, 
        account_id: int = 1210221,
        purchasing_power: float = 1_000_000
        ) -> ServerMsg:
    account_summary_statuses = server_msg.account_summary_statuses.add()
    account_summary_statuses.subscription_ids.append(1)
    account_summary_statuses.subscription_ids.append(2)
    account_summary_statuses.subscription_ids.append(3)
    
    account_summary_statuses.account_id = account_id
    account_summary_statuses.currency = "USD"
    account_summary_statuses.purchasing_power = purchasing_power
    
    return server_msg


def build_go_flat_statuses_server_msg(
        server_msg: ServerMsg,
        res: GFltStatus.StatusCode = GFltStatus.StatusCode.STATUS_CODE_COMPLETED,
        request_id: int = 1,
        account_id: int = 1210221
    ) -> ServerMsg:
    
    go_flat_statuses = server_msg.go_flat_statuses.add()
    go_flat_statuses.request_id = request_id
    go_flat_statuses.account_id = account_id
    go_flat_statuses.status_code = res
    return server_msg

### ---- Market Data ----
# (1) market_data_subscription_statuses (v)
# (2) real_time_market_data (v)
def build_market_data_subscription_statuses_server_msg(
        server_msg: ServerMsg,
        contract_id: int = 1,
        level: MktDSub.Level =  MktDSub.Level.LEVEL_TRADES,
        res: MktDSubStatus.StatusCode = MktDSubStatus.StatusCode.STATUS_CODE_SUCCESS
    ) -> ServerMsg:
    market_data_subscription_statuses = server_msg.market_data_subscription_statuses.add()
    market_data_subscription_statuses.contract_id = contract_id
    market_data_subscription_statuses.status_code = res
    market_data_subscription_statuses.level = level
        
    return server_msg
#--------
from google.protobuf.descriptor import FieldDescriptor
def build_real_time_market_data_flexible_quotes(
        server_msg: ServerMsg,
        contract_id: int,
        in_array: list[dict[Any]] # A list of parameters to be packed in Server Message
    ) -> ServerMsg:
    real_time_market_data = server_msg.real_time_market_data.add()
    real_time_market_data.contract_id = contract_id
    
    def set_attr(obj, name: str, val: Any) -> None:
        if val is None:
            return 
        fd = obj.DESCRIPTOR.fields_by_name[name]
        if fd.label == FieldDescriptor.LABEL_REPEATED:
            getattr(obj, name).extend(val if isinstance(val, list) else [val])
        else:
            setattr(obj, name, val)

    for para in in_array:
        quotes = real_time_market_data.quotes.add()
        quotes.quote_utc_time = int(datetime.now().timestamp())
        
        set_attr(quotes, para.get('q_type')) #Quote.Type.TYPE_TRADE
        set_attr(quotes, 'q_scaled_price', para)
        set_attr(quotes, 'q_scaled_source_price', para)
        set_attr(quotes, 'q_price_yield', para)
        set_attr(quotes, 'q_volume_significand', para)
        set_attr(quotes, 'q_volume_exponent', para)
        
        set_attr(quotes, 'q_indicators', quotes.indicators)
        set_attr(quotes, 'q_sales_condition', quotes.sales_condition)
        set_attr(quotes, 'q_scaled_currency_rate_price', quotes.scaled_currency_rate_price)
        set_attr(quotes, 'q_scaled_premium', quotes.scaled_premium)
    return server_msg
    
    
# -------

def build_simple_real_time_market_data_only_1quote_server_msg(
        server_msg: ServerMsg,
        contract_id: int = 1
    ) -> ServerMsg:
    real_time_market_data = server_msg.real_time_market_data.add()
    
    real_time_market_data.contract_id = contract_id
    quotes = real_time_market_data.quotes.add()
    quotes.quote_utc_time = int(datetime.now().timestamp())
    quotes.type = Quote.Type.TYPE_TRADE
    quotes.scaled_price = 19201
    quotes.scaled_source_price = 10029
    quotes.price_yield = 100
    quotes.volume.significand = 0
    quotes.volume.exponent = 2
    quotes.indicators.append(Quote.Indicator.INDICATOR_OPEN)
    quotes.sales_condition = Quote.SalesCondition.SALES_CONDITION_HIT

    quotes.scaled_currency_rate_price = 32
    quotes.scaled_premium = 11
    return server_msg
    
def build_real_time_market_data_server_msg(
        server_msg: ServerMsg,
        contract_id: int = 1
    ) -> ServerMsg:
    real_time_market_data = server_msg.real_time_market_data.add()
    
    real_time_market_data.contract_id = contract_id
    
    ts = real_time_market_data.market_state.trading_state 
    ts.exchange_state = TradingState.ExchangeState.EXCHANGE_STATE_PRE_OPEN    # MS_EXCHANGE_STATE
    ts.allow_place_order = False  # MS_ALLOW_PLACE
    ts.allow_cancel_order = True  # MS_ALLOW_CANCEL
    ts.allow_modify_order = False  # MS_ALLOW_MODIFY
    ts.matching_enabled = True    # MS_MATCHING_ENABLED
    
    real_time_market_data.market_state.is_snapshot = True
    real_time_market_data.correct_price_scale = 10.123
    real_time_market_data.is_snapshot = False
    # ----
    quotes = real_time_market_data.quotes.add()
    quotes.quote_utc_time = int(datetime.now().timestamp())
    quotes.type = Quote.Type.TYPE_TRADE
    quotes.scaled_price = 19201
    quotes.scaled_source_price = 10029
    quotes.price_yield = 100
    quotes.volume.significand = 0
    quotes.volume.exponent = 2
    quotes.indicators.append(Quote.Indicator.INDICATOR_OPEN)
    quotes.sales_condition = Quote.SalesCondition.SALES_CONDITION_HIT
    
    quotes.trade_attributes.buyer = 12
    quotes.trade_attributes.seller = 11
    quotes.trade_attributes.trade_type ="trade_type_1"
    quotes.trade_attributes.match_id = "match_id_1"
    quotes.trade_attributes.agreement_time_utc.seconds = int(1001)
    quotes.trade_attributes.agreement_time_utc.nanos = 5

    quotes.scaled_currency_rate_price = 32
    quotes.scaled_premium = 11
    # ----
    market_values = real_time_market_data.market_values.add()
    
    market_values.scaled_open_price = 19600
    market_values.scaled_high_price = 20331
    market_values.scaled_low_price = 18890
    market_values.scaled_close_price = 19202
    market_values.scaled_last_price_no_settlement = 266
    market_values.scaled_exchange_close_price = 277
    market_values.scaled_yesterday_settlement = 288
    market_values.total_volume.significand = 12
    market_values.total_volume.exponent = 0
    market_values.scaled_indicative_open = 8
    market_values.indicative_open_volume.significand = 1
    market_values.indicative_open_volume.exponent = 0
    market_values.day_index = -1
    market_values.open_interest.significand = 121
    market_values.open_interest.exponent = 2
    market_values.tick_volume = 100
    market_values.scaled_settlement = 12411
    market_values.scaled_marker_price = 129
    market_values.scaled_last_trade_price = 18221
    market_values.last_trade_volume.significand = 21
    market_values.last_trade_volume.exponent = 5
    market_values.last_trade_utc_timestamp.seconds = 102
    market_values.last_trade_utc_timestamp.nanos = 11
    market_values.cleared_fields.append(1)
    market_values.trade_date = 20160616
    market_values.session_index = 4
    
    market_values.market_yields.yield_of_open_price = 1
    market_values.market_yields.yield_of_high_price = 2 
    market_values.market_yields.yield_of_low_price = 3
    market_values.market_yields.yield_of_close_price = 4
    market_values.market_yields.yield_of_yesterday_settlement = 5
    market_values.market_yields.yield_of_indicative_open = 6
    market_values.market_yields.yield_of_settlement = 7
    
    market_values.scaled_currency_rate_price = 10
    # ----
    quotes1 = real_time_market_data.quotes.add()
    quotes1.quote_utc_time = int(datetime.now().timestamp())
    quotes1.type = Quote.Type.TYPE_TRADE
    quotes1.scaled_price = 1400
    quotes1.scaled_source_price = 129
    quotes1.price_yield = 100
    quotes1.volume.significand = 0
    quotes1.volume.exponent = 1
    quotes1.indicators.append(Quote.Indicator.INDICATOR_OPEN)
    quotes1.sales_condition = Quote.SalesCondition.SALES_CONDITION_HIT
        
    quotes1.trade_attributes.buyer = 12
    quotes1.trade_attributes.seller = 11
    quotes1.trade_attributes.trade_type ="trade_type_2"
    quotes1.trade_attributes.match_id = "match_id_2"
    quotes1.trade_attributes.agreement_time_utc.seconds = int(1001)
    quotes1.trade_attributes.agreement_time_utc.nanos = 5

    quotes1.scaled_currency_rate_price = 32
    quotes1.scaled_premium = 11
    
    # ----
    market_values1 = real_time_market_data.market_values.add()
    
    market_values1.scaled_open_price = 786
    market_values1.scaled_high_price = 890
    market_values1.scaled_low_price = 611
    market_values1.scaled_close_price = 755
    market_values1.scaled_last_price_no_settlement = 266
    market_values1.scaled_exchange_close_price = 277
    market_values1.scaled_yesterday_settlement = 288
    market_values1.total_volume.significand = 4
    market_values1.total_volume.exponent = 0
    market_values1.scaled_indicative_open = 2
    market_values1.indicative_open_volume.significand = 5
    market_values1.indicative_open_volume.exponent = 0
    market_values1.day_index = 1
    market_values1.open_interest.significand = 1234
    market_values1.open_interest.exponent = 6
    market_values1.tick_volume = 118
    market_values1.scaled_settlement = 246
    market_values1.scaled_marker_price = 803
    market_values1.scaled_last_trade_price = 266
    market_values1.last_trade_volume.significand = 21
    market_values1.last_trade_volume.exponent = 5
    market_values1.last_trade_utc_timestamp.seconds = 102
    market_values1.last_trade_utc_timestamp.nanos = 11
    market_values1.cleared_fields.append(7)
    market_values1.trade_date = 20170717
    market_values1.session_index = 4
    
    market_values1.market_yields.yield_of_open_price = 8
    market_values1.market_yields.yield_of_high_price = 9 
    market_values1.market_yields.yield_of_low_price = 10
    market_values1.market_yields.yield_of_close_price = 11
    market_values1.market_yields.yield_of_yesterday_settlement = 12
    market_values1.market_yields.yield_of_indicative_open = 13
    market_values1.market_yields.yield_of_settlement = 14
    
    market_values1.scaled_currency_rate_price = 26
    
    return server_msg


## Historical Data
# (1) time_and_sales_reports (v)
# (2) time_bar_reports (v)
# (3) volume_profile_reports (v)
# (4) non_timed_bar_reports (v)
def build_time_and_sales_reports_server_msg(
        server_msg: ServerMsg,
        res: TSrRep.ResultCode = TSrRep.ResultCode.RESULT_CODE_SUCCESS
    ) -> ServerMsg:
    time_and_sales_reports = server_msg.time_and_sales_reports.add()
    
    time_and_sales_reports.request_id = 1
    time_and_sales_reports.result_code = res
    
    quotes = time_and_sales_reports.quotes.add()
    quotes.quote_utc_time = int(datetime.now().timestamp())
    quotes.type = Quote.Type.TYPE_TRADE
    quotes.scaled_price = 121
    quotes.scaled_source_price = 464
    quotes.volume.significand = 200
    quotes.indicators.append(Quote.Indicator.INDICATOR_OPEN)
    quotes.sales_condition = Quote.SalesCondition.SALES_CONDITION_HIT

    return server_msg

def build_time_bar_reports_server_msg(
        server_msg: ServerMsg,
        res: BarRpStatusCode = BarRpStatusCode.BAR_REPORT_STATUS_CODE_SUCCESS
    ) -> ServerMsg:
    time_bar_reports = server_msg.time_bar_reports.add()
    
    time_bar_reports.request_id = 1
    time_bar_reports.status_code = res
    
    time_bars = time_bar_reports.time_bars.add()
    time_bars.bar_utc_time = int(datetime.now().timestamp())
    time_bars.scaled_open_price = 987
    time_bars.scaled_high_price = 1241
    time_bars.scaled_low_price = 902
    time_bars.scaled_close_price = 1002
    
    time_bars.volume.significand = 19
    return server_msg

def build_volume_profile_reports_server_msg(
        server_msg: ServerMsg,
        res: VolPrfRep.ResultCode = VolPrfRep.ResultCode.RESULT_CODE_SUCCESS
    ) -> ServerMsg:
    volume_profile_reports = server_msg.volume_profile_reports.add()
    
    volume_profile_reports.request_id = 1
    volume_profile_reports.result_code = res
    
    volume_profile_items = volume_profile_reports.volume_profile_items.add()
    volume_profile_items.scaled_price = 67541
    volume_profile_items.volume.significand = 12
    volume_profile_items.ask_volume.significand = 2
    volume_profile_items.bid_volume.significand = 5
    volume_profile_items.tick_volume = 1
    volume_profile_items.ask_tick_volume.significand = 7
    volume_profile_items.bid_tick_volume.significand = 6
    return server_msg

def build_non_timed_bar_reports_server_msg(
        server_msg: ServerMsg,
        res: BarRpStatusCode = BarRpStatusCode.BAR_REPORT_STATUS_CODE_SUCCESS
    ) -> ServerMsg:
    non_timed_bar_reports = server_msg.non_timed_bar_reports.add()
    
    non_timed_bar_reports.request_id = 1
    non_timed_bar_reports.status_code = res
    # add more later
    return server_msg


def build_all_server_msgs() -> dict[str, ServerMsg]:
    # A simple function that build all available server msgs. 
    # If you need specific input parameters for these message, please
    # construct them one by one.
    logon_result_server_msg = build_logon_result_server_msg(ServerMsg())
    restore_or_join_session_result_server_msg = build_restore_or_join_session_result_server_msg(ServerMsg())
    concurrent_connection_join_results_server_msg = build_concurrent_connection_join_results_server_msg(ServerMsg())
    logged_off_server_msg = build_logged_off_server_msg(ServerMsg())
    pong_server_msg = build_pong_server_msg(ServerMsg(), "0")

    symbol_resolution_report_server_msg = build_symbol_resolution_report_server_msg(ServerMsg())
    session_info_report_server_msg = build_session_info_report_server_msg(ServerMsg())
    historical_orders_report_server_msg = build_historical_orders_report_server_msg(ServerMsg())
    option_maturity_list_report_server_msg = build_option_maturity_list_report_server_msg(ServerMsg())
    instrument_group_report_server_msg = build_instrument_group_report_server_msg(ServerMsg())
    at_the_money_strike_report_server_msg = build_at_the_money_strike_report_server_msg(ServerMsg())

    order_request_rejects_server_msg = build_order_request_rejects_server_msg(ServerMsg())
    order_request_acks_server_msg = build_order_request_acks_server_msg(ServerMsg())
    trade_subscription_statuses_server_msg = build_trade_subscription_statuses_server_msg(ServerMsg())
    trade_snapshot_completions_server_msg = build_trade_snapshot_completions_server_msg(ServerMsg())
    order_statuses_server_msg = build_order_statuses_server_msg(ServerMsg())
    position_statuses_server_msg = build_position_statuses_server_msg(ServerMsg())
    account_summary_statuses_server_msg = build_account_summary_statuses_server_msg(ServerMsg())
    go_flat_statuses_server_msg = build_go_flat_statuses_server_msg(ServerMsg())

    market_data_subscription_statuses_server_msg = build_market_data_subscription_statuses_server_msg(ServerMsg())
    real_time_market_data_server_msg = build_real_time_market_data_server_msg(ServerMsg())

    time_and_sales_reports_server_msg = build_time_and_sales_reports_server_msg(ServerMsg())
    time_bar_reports_server_msg = build_time_bar_reports_server_msg(ServerMsg())
    volume_profile_reports_server_msg = build_volume_profile_reports_server_msg(ServerMsg())
    non_timed_bar_reports_server_msg = build_non_timed_bar_reports_server_msg(ServerMsg())

    return {
        # (1) connection/session
        "logon_result": logon_result_server_msg,
        "restore_or_join_session_result": restore_or_join_session_result_server_msg,
        "concurrent_connection_join_results": concurrent_connection_join_results_server_msg,
        "logged_off": logged_off_server_msg,
        "pong": pong_server_msg,
        # (2) info report container
        "information_reports:symbol_resolution_report": symbol_resolution_report_server_msg,
        "information_reports:session_information_report": session_info_report_server_msg,
        "information_reports:historical_orders_report": historical_orders_report_server_msg,
        "information_reports:option_maturity_list_report": option_maturity_list_report_server_msg,
        "information_reports:instrument_group_report": instrument_group_report_server_msg,
        "information_reports:at_the_money_strike_report": at_the_money_strike_report_server_msg,
        # (3) order/account RPC & streams
        "order_request_rejects": order_request_rejects_server_msg,
        "order_request_acks": order_request_acks_server_msg,
        "trade_subscription_statuses": trade_subscription_statuses_server_msg,
        "trade_snapshot_completions": trade_snapshot_completions_server_msg,
        "order_statuses": order_statuses_server_msg,
        "position_statuses": position_statuses_server_msg,
        "account_summary_statuses": account_summary_statuses_server_msg,
        "go_flat_statuses": go_flat_statuses_server_msg,
        # (4) realtime
        "market_data_subscription_statuses": market_data_subscription_statuses_server_msg,
        "real_time_market_data": real_time_market_data_server_msg,
        # (5) historical
        "time_and_sales_reports": time_and_sales_reports_server_msg,
        "time_bar_reports": time_bar_reports_server_msg,
        "volume_profile_reports": volume_profile_reports_server_msg,
        "non_timed_bar_reports": non_timed_bar_reports_server_msg,
        }


