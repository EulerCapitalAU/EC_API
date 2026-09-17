#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  2 00:10:29 2026

@author: dexter
"""
import pytest
from EC_API.ext.WebAPI.webapi_2_pb2 import ServerMsg
from EC_API.ext.WebAPI.order_2_pb2 import GoFlatStatus as GFltStatus
from EC_API.ordering.enums import (Side)
from EC_API.ordering.cqg.builders import (
    build_new_order_request_msg
    )
from tests.unit.fixtures.server_msg_builders_CQG import (
    build_order_request_acks_server_msg,
    build_order_request_rejects_server_msg,
    build_trade_subscription_statuses_server_msg,
    build_trade_snapshot_completions_server_msg,
    build_order_statuses_server_msg,
    build_position_statuses_server_msg,
    build_account_summary_statuses_server_msg,
    build_go_flat_statuses_server_msg
    )
from EC_API.ordering.cqg.parsers import (
    parse_order_request_rejects,
    parse_order_request_acks,
    parse_trade_subscription_statuses,
    parse_trade_snapshot_completions,
    parse_order_statuses,
    parse_position_statuses,
    parse_account_summary_statuses,
    parse_go_flat_statuses,
    ordering_parsers
    )
from EC_API.protocol.cqg.parser_util import parse_server_msg
from EC_API.exceptions import (MsgParserError)

def test_parse_order_request_rejects_server_msg_valid() -> None:
    msg = build_order_request_rejects_server_msg(ServerMsg(), request_id=2)
    res = parse_order_request_rejects(msg)
    assert isinstance(res, list)
    assert res[0]['request_id'] == 2
    assert res[0]['reject_code'] == 1001
     
    
def test_parse_order_request_acks_server_msg_valid() -> None:
    msg = build_order_request_acks_server_msg(ServerMsg(), request_id=3)
    res = parse_order_request_acks(msg)
    assert isinstance(res, list)
    assert res[0]['request_id'] == 3


def test_parse_trade_subscription_statuses_server_msg_valid() -> None:
    msg = build_trade_subscription_statuses_server_msg(ServerMsg())
    res = parse_server_msg(msg, ordering_parsers)
    assert isinstance(res, list)


def test_parse_trade_snapshot_completions_server_msg_valid() -> None:
    msg = build_trade_snapshot_completions_server_msg(ServerMsg(),sub_id = 3)
    res = parse_server_msg(msg, ordering_parsers)
    assert isinstance(res, list)
    assert res[0]['sub_id'] == 3
    assert res[0]['sub_scopes'] == [1,2,3,4]


def test_parse_order_statuses_server_msg_valid() -> None:
    client_msg = build_new_order_request_msg(
        account_id=123466,
        request_id=10,
        contract_id=10,
        cl_order_id="cl_order_1",
        side=Side.SELL,
        qty=111
        )

    msg = build_order_statuses_server_msg(
        ServerMsg(),
        order = client_msg.order_requests[0].new_order.order)
    res = parse_order_statuses(msg)   
    assert isinstance(res, list)
    assert len(res) == 1
    assert res[0]['sub_ids'] == [1]
    assert res[0]['order_id'] == 'order_id_1'
    assert res[0]['chain_order_id'] == 'chain_order_id_1'
    assert res[0]['fill_cnt'] == 0
    assert res[0]['scaled_avg_fill_price'] == -200
    assert res[0]['avg_fill_price_correct'] == 10.0
    assert res[0]['account_id'] == 123466
    assert res[0]['entered_by_user'] == "user_A"
    
    assert res[0]['order']['account_id'] == 123466
    assert res[0]['order']['contract_id'] == 10
    assert res[0]['order']['cl_order_id'] == 'cl_order_1'
    assert res[0]['order']['side'] == 2
    assert res[0]['order']['qty'] == 111
    #assert res[0]['order']['qty']['exponent'] == 0
    
def test_parse_position_statuses_server_msg_valid() -> None:
    msg = build_position_statuses_server_msg(ServerMsg())
    res = parse_position_statuses(msg)
    assert isinstance(res, list)
    assert len(res) == 1
    assert res[0]['sub_ids'] == [0,1,2]
    assert res[0]['account_id'] == 123466
    assert res[0]['contract_id'] == 0
    assert res[0]['is_short_open_position'] == False
    
    assert res[0]['open_positions'][0]['id'] == 2
    assert res[0]['open_positions'][0]['price_correct'] == 101
    assert res[0]['open_positions'][0]['is_aggregated'] == True
    assert res[0]['open_positions'][0]['is_short'] == False
    assert 'trade_date' in res[0]['open_positions'][0].keys()
    assert 'statement_date' in res[0]['open_positions'][0].keys()
    
# =============================================================================
# def build_position_statuses_server_msg(
#         server_msg: ServerMsg,
#         subscription_ids: list[int] = [0,1,2],
#         contract_id: int = 0
#     ) -> ServerMsg:    
#     
#     position_statuses = server_msg.position_statuses.add()
#     for num in subscription_ids:
#         position_statuses.subscription_ids.append(num)
#     
#     position_statuses.account_id = 123466
#     position_statuses.contract_id = contract_id
#     position_statuses.is_short_open_position = False
#     
#     # ----
#     open_positions = position_statuses.open_positions.add()
#     open_positions.id = 2
#     open_positions.price_correct = 101
#     open_positions.trade_date = int(datetime.now().timestamp())
#     open_positions.statement_date = int(datetime.now().timestamp())
#     open_positions.is_aggregated = True
#     open_positions.is_short = False
#     
#     # ----
#     purchase_and_sales_groups = position_statuses.purchase_and_sales_groups.add()
#     purchase_and_sales_groups.id = 4
#     purchase_and_sales_groups.realized_profit_loss = 10
#     
#     matched_trades = purchase_and_sales_groups.matched_trades.add()
#     matched_trades.price = 292
#     matched_trades.trade_date = int(datetime.now().timestamp())
#     matched_trades.statement_date = int(datetime.now().timestamp())
#     matched_trades.is_aggregated = True
#     # ----
#     today_fill_commissions = position_statuses.today_fill_commissions.add()
#     today_fill_commissions.commission_currency = "USD"
#     today_fill_commissions.commission = 102
#     return server_msg
# =============================================================================
# =============================================================================
# def build_order_statuses_server_msg(
#         server_msg: ServerMsg,
#         res: OrderStatus.Status = OrderStatus.Status.IN_TRANSIT,
#         contract_id: int = 0,
#         sub_id: int = 1,
#         order_id: str = "order_id_1",
#         chain_order_id: str = "chain_order_id_1",
#         order = None
#     ) -> ServerMsg:
#     
#     order_statuses = server_msg.order_statuses.add()
#     
#     # ----
#     order_statuses.subscription_ids.append(sub_id)
#     order_statuses.status = res
#     order_statuses.order_id = order_id
#     order_statuses.chain_order_id = chain_order_id
#     order_statuses.status_utc_timestamp = datetime.now()
#     order_statuses.submission_utc_timestamp = datetime.now()
#     order_statuses.fill_cnt = 0
#     order_statuses.scaled_avg_fill_price = -200
#     order_statuses.avg_fill_price_correct = 10
#     #----
#     transaction_statuses = order_statuses.transaction_statuses.add()
#     transaction_statuses.status = TransactionStatus.Status.IN_TRANSIT
#     transaction_statuses.trans_id = 2
#     transaction_statuses.trans_utc_timestamp = datetime.now()
#     transaction_statuses.cl_order_id = "cl_order_id_1"
#     
#     trades = transaction_statuses.trades.add()
#     trades.trade_id = "trade_id_1"
#     trades.contract_id = 0
#     trades.statement_date = int(datetime.now().timestamp())
#     trades.trade_utc_timestamp = datetime.now()
#     
#     trades.scaled_price = 1000 #price = round(price_correct / correct_price_scale)
#     trades.price_correct = 100100
#     trades.side = Ord.Side.SIDE_BUY
#     # ----
#     order_statuses.entered_by_user = "user_A"
#     order_statuses.first_statement_date = int(datetime.now().timestamp())
#    
#     if order is not None: 
#         order_statuses.order.CopyFrom(order)
#     # ----
#     contract_metadata = order_statuses.contract_metadata.add()
#     contract_metadata.contract_id = 0
#     contract_metadata.contract_symbol = "Symbol_1"
#     contract_metadata.correct_price_scale = 0.01
#     contract_metadata.display_price_scale = 202
#     contract_metadata.description = "description"
#     contract_metadata.title = "title"
#     contract_metadata.tick_size = 10
#     contract_metadata.currency = "USD"
#     contract_metadata.tick_value = 0
#     contract_metadata.cfi_code = "ESVUFB"
#     contract_metadata.instrument_group_name = "instru_group_1"
#     contract_metadata.session_info_id  = 232323
#     contract_metadata.short_instrument_group_name = "short_instrument_group_name"
#     contract_metadata.instrument_group_description = "instrument_group_description"
#     contract_metadata.country_code = "US"
#     # ----
#     order_statuses.account_id = 123466    
#     return server_msg
# =============================================================================
def test_parse_account_summary_statuses_server_msg_valid() -> None:
    msg = build_account_summary_statuses_server_msg(ServerMsg())
    res = parse_account_summary_statuses(msg)
    assert isinstance(res, list)
    assert res[0]['subscription_ids'] == [1,2,3]
    assert res[0]['account_id'] == 1210221
    assert res[0]['currency'] == "USD"
    assert res[0]['purchasing_power'] == 1_000_000

def test_parse_go_flat_statuses_server_msg_valid() -> None:
    msg = build_go_flat_statuses_server_msg(ServerMsg())
    res = parse_go_flat_statuses(msg)
    assert isinstance(res, list)
    assert res[0]['request_id'] == 1
    assert res[0]['account_id'] == 1210221
    assert res[0]['status_code'] == GFltStatus.StatusCode.STATUS_CODE_COMPLETED
    
# --- Detail parsing
def test_parse_order_statuses_server_msg_in_detail_valid() -> None:
    msg = build_order_statuses_server_msg(
        ServerMsg(),
        )
    res = parse_order_statuses(msg, in_detail=True)
    print(res)
    assert res[0].get('transaction_statuses') is not None
    assert res[0].get('contract_metadata') is not None
    assert res[0]['transaction_statuses'][0]['trans_id'] == '2'
    assert res[0]['transaction_statuses'][0]['cl_order_id'] == "cl_order_id_1"
    assert res[0]['transaction_statuses'][0]['trades'][0]['trade_id'] == "trade_id_1"
    assert res[0]['transaction_statuses'][0]['trades'][0]['contract_id'] == 0
    assert res[0]['transaction_statuses'][0]['trades'][0]['scaled_price'] == '1000'
    assert res[0]['transaction_statuses'][0]['trades'][0]['price_correct'] == 100100
    assert res[0]['transaction_statuses'][0]['trades'][0]['side'] == 1

    assert res[0]['contract_metadata'][0]['contract_id'] == 0
    assert res[0]['contract_metadata'][0]['contract_symbol'] == "Symbol_1"
    assert res[0]['contract_metadata'][0]['correct_price_scale'] == 0.01
    assert res[0]['contract_metadata'][0]['display_price_scale'] == 202
    assert res[0]['contract_metadata'][0]['description'] == "description"
    assert res[0]['contract_metadata'][0]['title'] == "title"
    assert res[0]['contract_metadata'][0]['tick_size'] == 10
    assert res[0]['contract_metadata'][0]['currency'] == "USD"
    assert res[0]['contract_metadata'][0]['tick_value'] == 0
    assert res[0]['contract_metadata'][0]['cfi_code'] == "ESVUFB"
    assert res[0]['contract_metadata'][0]['instrument_group_name'] == "instru_group_1"
    assert res[0]['contract_metadata'][0]['session_info_id'] == 232323
    assert res[0]['contract_metadata'][0]['short_instrument_group_name'] == "short_instrument_group_name"
    assert res[0]['contract_metadata'][0]['instrument_group_description'] == "instrument_group_description"
    assert res[0]['contract_metadata'][0]['country_code'] == "US"

    
def test_parse_position_statuses_server_msg_in_detail_valid() -> None: 
    msg = build_position_statuses_server_msg(ServerMsg())
    res = parse_position_statuses(msg, in_detail = True)
    assert res[0].get('purchase_and_sales_groups') is not None
    assert res[0]['purchase_and_sales_groups'][0].get('matched_trades') is not None
    assert res[0].get('today_fill_commissions') is not None
    

# --- Sad Path
def test_parse_order_request_rejects_null() -> None:
      with pytest.raises(MsgParserError):
          parse_order_request_rejects(None)

def test_parse_order_request_acks_null() -> None:
    with pytest.raises(MsgParserError):
        parse_order_request_acks(None)

def test_parse_trade_subscription_statuses_null() -> None:
    with pytest.raises(MsgParserError):
        parse_trade_subscription_statuses(None)

def test_parse_trade_snapshot_completions_null() -> None:
    with pytest.raises(MsgParserError):
        parse_trade_snapshot_completions(None)

def test_parse_order_statuses_null() -> None:
    with pytest.raises(MsgParserError):
        parse_order_statuses(None)

def test_parse_position_statuses_null() -> None:
    with pytest.raises(MsgParserError):
        parse_position_statuses(None)

def test_parse_account_summary_statuses_null() -> None:
    with pytest.raises(MsgParserError):
        parse_account_summary_statuses(None)

def test_parse_go_flat_statuses_null() -> None:
    with pytest.raises(MsgParserError):
        parse_go_flat_statuses(None)