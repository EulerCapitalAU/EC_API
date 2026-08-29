#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May  8 01:15:07 2026

@author: dexter
"""
import asyncio
import pytest
from EC_API.ext.WebAPI.webapi_2_pb2 import ServerMsg
from EC_API.ext.WebAPI.trade_routing_2_pb2 import TradeSubscription as CQG_TS
from EC_API.ext.common.shared_1_pb2 import OrderStatus
from EC_API.connect.cqg.base import ConnectCQG
from EC_API.ordering.enums import (
    SubScope, Side, OrderType, Duration
    )
from EC_API.ordering.cqg.trade_session import TradeSessionCQG
from EC_API.ordering.cqg.enum_mapping import (
    OrderStatus_MAP_CQG2INT, SubScope_MAP_INT2CQG
    )
from EC_API.ordering.cqg.builders import build_new_order_request_msg
from tests.unit.fixtures.proxy_clients import FakeTransport, FakeCQGClient
from tests.unit.fixtures.server_msg_builders_CQG import (
    build_order_statuses_server_msg,
    build_position_statuses_server_msg,
    build_account_summary_statuses_server_msg,
    build_order_server_msg
    )
def assert_TS_init(ts: TradeSessionCQG):
    assert isinstance(ts._active_trade_subs, dict)
    assert isinstance(ts.cl_to_chain, dict)
    assert isinstance(ts._pending_chain_q, list)
    assert isinstance(ts._active_order_q, dict)
    assert isinstance(ts._active_pos_q, dict)
    assert isinstance(ts._active_acc_summary_q, dict)
    assert isinstance(ts.latest_order_state_by_chain, dict)
    assert isinstance(ts.latest_pos_status_by_contract_id, dict)
    assert isinstance(ts.latest_account_summaries, dict)

    assert not ts._active_trade_subs
    assert not ts.cl_to_chain
    assert not ts._pending_chain_q
    assert not ts._active_order_q
    assert not ts._active_pos_q
    assert not ts._active_acc_summary_q
    assert not ts.latest_order_state_by_chain
    assert not ts.latest_pos_status_by_contract_id
    assert not ts.latest_account_summaries

# Happy Path - Single ID Life cycle
@pytest.mark.asyncio
async def test_order_status_updates_one_chain_order_id_lifecycle_valid() -> None:
    # --- Setup ---
    fake_transport = FakeTransport()
    conn = ConnectCQG(
        "host_name", 
        "user_name", 
        "password",
        account_id = 10000,
        immediate_connect= False, 
        client = FakeCQGClient(),
        transport = fake_transport
        )
    conn._timeout = 0.0001

    proxy_client_msg = build_new_order_request_msg(
        account_id = 10000,
        request_id = 11,
        contract_id = 0,
        cl_order_id = "cl_order_id", 
        side = Side.BUY, # Delibrate choice here to return error msg if no side is provided
        qty = 1,
        order_type =  OrderType.MKT, 
        duration = Duration.DAY, 
        is_manual = False,
        scale_factor = 1.0,

        )

    async with TradeSessionCQG(conn) as TS:
        # Check if things are empty in the begining
        assert_TS_init(TS)
        
        # Trade Session Setup
        TS._symbol_registry.add_symbol('Asset_A', 0)
        TS._symbol_registry.add_metadata('Asset_A', {'A':'a'})
        
        TS._active_trade_subs[1] = [SubScope.ORDERS]
        
        # stream subscription
        TS.cl_to_chain['cl_order_id'] = "chain_order_id_1"
        q = TS._exec_stream_router.subscribe("chain_order_id_1")
        TS._active_order_q["chain_order_id_1"] = q

        # Stage 1: initial message
        # Message building
        response_1 = build_order_statuses_server_msg(
            ServerMsg(),
            res = OrderStatus.Status.IN_TRANSIT,
            contract_id = 0,
            sub_ids = [CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_ORDERS],
            order_id = "order_id_1",
            chain_order_id = "chain_order_id_1",
            order = proxy_client_msg.order_requests[0].new_order.order,
            account_id = conn._account_id
            )
        await fake_transport.in_q.put(response_1)
        await asyncio.sleep(0.001)
        # Stage 1 assert
        assert q.empty()
        assert TS._active_order_q.get("chain_order_id_1") is not None
        assert isinstance(TS.latest_order_state_by_chain["chain_order_id_1"], dict)
        assert TS.latest_order_state_by_chain["chain_order_id_1"]["order_id"] == "order_id_1"
        assert TS.latest_order_state_by_chain["chain_order_id_1"]["account_id"] == conn._account_id
        assert TS.latest_order_state_by_chain["chain_order_id_1"]["status"] == OrderStatus_MAP_CQG2INT[OrderStatus.Status.IN_TRANSIT]
        assert TS.active_order_ids_by_chain["chain_order_id_1"][0] == "order_id_1"

        # Stage 2: overwriting snapshot
        # Message building
        response_2 = build_order_statuses_server_msg(
            ServerMsg(),
            res = OrderStatus.Status.IN_MODIFY,
            contract_id = 0,
            sub_ids = [CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_ORDERS],
            order_id = "order_id_2",
            chain_order_id = "chain_order_id_1",
            order = None,
            account_id = conn._account_id
            )
        # Message building
        response_3 = build_order_statuses_server_msg(
            ServerMsg(),
            res = OrderStatus.Status.WORKING,
            contract_id = 0,
            sub_ids = [CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_ORDERS],
            order_id = "order_id_3",
            chain_order_id = "chain_order_id_1",
            order = None,
            account_id = conn._account_id
            )
        await fake_transport.in_q.put(response_2)
        await fake_transport.in_q.put(response_3)
        await asyncio.sleep(0.001)
        
        assert TS.latest_order_state_by_chain["chain_order_id_1"]["order_id"] == "order_id_3"
        assert TS.latest_order_state_by_chain["chain_order_id_1"]["status"] == OrderStatus_MAP_CQG2INT[OrderStatus.Status.WORKING]
        assert TS.active_order_ids_by_chain["chain_order_id_1"][0] == "order_id_3"

        # Stage 3 Terminal message and cleanup()
        response_4 = build_order_statuses_server_msg(
            ServerMsg(),
            res = OrderStatus.Status.EXPIRED, #<---Terminal State
            contract_id = 0,
            sub_ids = [CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_ORDERS],
            order_id = "order_id_4",
            chain_order_id = "chain_order_id_1",
            order = build_order_server_msg(cl_order_id="new_cl_order_id"),
            account_id = conn._account_id
            )
        await fake_transport.in_q.put(response_4)
        await asyncio.sleep(0.001)
        
        assert TS.latest_order_state_by_chain["chain_order_id_1"]["order_id"] == "order_id_4"
        assert TS.latest_order_state_by_chain["chain_order_id_1"]["status"] == OrderStatus_MAP_CQG2INT[OrderStatus.Status.EXPIRED]
        assert TS.active_order_ids_by_chain.get("chain_order_id_1") is None

        # ID no longer in active queue and router
        assert TS._active_order_q.get("chain_order_id_1") is None
        assert TS._exec_stream_router._subs.get("chain_order_id_1") is None
        
        # check that the latest cl_order_id is updated
        assert TS.cl_to_chain["new_cl_order_id"] == "chain_order_id_1"
        
@pytest.mark.asyncio
async def test_position_status_updates_one_contract_id_lifecycle_valid() -> None:
    # --- Setup ---
    fake_transport = FakeTransport()
    conn = ConnectCQG(
        "host_name", 
        "user_name", 
        "password",
        account_id = 10000,
        immediate_connect= False, 
        client = FakeCQGClient(),
        transport = fake_transport
        )
    conn._timeout = 0.0001

    async with TradeSessionCQG(conn) as TS:
        # Check if things are empty in the begining
        assert_TS_init(TS)
        
        # Trade Session Setup
        TS._symbol_registry.add_symbol('Asset_A', 0)
        TS._symbol_registry.add_metadata('Asset_A', {'A':'a'})
        
        TS._active_trade_subs[1] = [SubScope.POSITIONS]
        
        # stream subscription
        q = TS._pos_status_stream_router.subscribe(0)
        TS._active_pos_q[0] = q
        
        # --- Stage 1
        response_1 = build_position_statuses_server_msg(
            ServerMsg(),
            subscription_ids = [CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_ORDERS],
            contract_id = 0,
            account_id = conn._account_id,
            qty = 1
            )
        await fake_transport.in_q.put(response_1)
        await asyncio.sleep(0.001)
        
        assert TS.latest_pos_status_by_contract_id.get(0)
        assert isinstance(TS.latest_pos_status_by_contract_id[0], dict)
        assert TS.latest_pos_status_by_contract_id[0]['sub_ids'] == [
            CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_ORDERS
            ]
        assert TS.latest_pos_status_by_contract_id[0]['account_id'] == conn._account_id
        assert TS.latest_pos_status_by_contract_id[0]['contract_id'] == 0
        assert TS.latest_pos_status_by_contract_id[0].get('open_positions')
        assert TS.latest_pos_status_by_contract_id[0]['open_positions'][0]['qty'] == 1
        
        # --- Stage 2
        response_2 = build_position_statuses_server_msg(
            ServerMsg(),
            subscription_ids = [CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_ORDERS],
            contract_id = 0,
            account_id = conn._account_id,
            qty = 2
            )
        response_3 = build_position_statuses_server_msg(
            ServerMsg(),
            subscription_ids = [CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_ORDERS],
            contract_id = 0,
            account_id = conn._account_id,
            qty = 3
            )

        await fake_transport.in_q.put(response_2)
        await fake_transport.in_q.put(response_3)
        await asyncio.sleep(0.001)

        assert TS.latest_pos_status_by_contract_id[0]['contract_id'] == 0
        assert TS.latest_pos_status_by_contract_id[0].get('open_positions')
        assert TS.latest_pos_status_by_contract_id[0]['open_positions'][0]['qty'] == 3
        
        # --- Stage 3
        response_4 = build_position_statuses_server_msg(
            ServerMsg(),
            subscription_ids = [CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_ORDERS],
            contract_id = 0,
            account_id = conn._account_id,
            qty = 0 # <--- trigger cleanup
            )
        await fake_transport.in_q.put(response_4)
        await asyncio.sleep(0.001)
        
        assert TS.latest_pos_status_by_contract_id[0]['contract_id'] == 0
        assert TS.latest_pos_status_by_contract_id[0].get('open_positions')
        assert TS.latest_pos_status_by_contract_id[0]['open_positions'][0]['qty'] == 0
        
        assert TS._active_pos_q.get(0) is None
        assert TS._pos_status_stream_router._subs.get(0) is None


@pytest.mark.asyncio
async def test_account_summary_updates_single_ID_lifecycle_valid() -> None:
    # --- Setup ---
    fake_transport = FakeTransport()
    conn = ConnectCQG(
        "host_name", 
        "user_name", 
        "password",
        account_id = 10000,
        immediate_connect= False, 
        client = FakeCQGClient(),
        transport = fake_transport
        )
    conn._timeout = 0.0001

    async with TradeSessionCQG(conn) as TS:
        # Check if things are empty in the begining
        assert_TS_init(TS)
        
        # Trade Session Setup
        TS._symbol_registry.add_symbol('Asset_A', 0)
        TS._symbol_registry.add_metadata('Asset_A', {'A':'a'})
        
        TS._active_trade_subs[1] = [SubScope.ACCOUNT_SUMMARY]
        
        # stream subscription
        q = TS._acc_summary_stream_router.subscribe(conn._account_id)
        TS._active_acc_summary_q[conn._account_id] = q

        response_1 = build_account_summary_statuses_server_msg(
            ServerMsg(),
            account_id = conn._account_id,
            purchasing_power = 1_000_000
            )
        await fake_transport.in_q.put(response_1)
        await asyncio.sleep(0.001)

        assert TS.latest_account_summaries.get(conn._account_id)
        assert isinstance(TS.latest_account_summaries[conn._account_id], dict)
        assert TS.latest_account_summaries[conn._account_id]['account_id'] == conn._account_id
        assert TS.latest_account_summaries[conn._account_id]['purchasing_power'] == 1_000_000
        
        response_2 = build_account_summary_statuses_server_msg(
            ServerMsg(),
            account_id = conn._account_id,
            purchasing_power = 1_000
            )
        
        await fake_transport.in_q.put(response_2)
        await asyncio.sleep(0.001)

        assert TS.latest_account_summaries[conn._account_id]['account_id'] == conn._account_id
        assert TS.latest_account_summaries[conn._account_id]['purchasing_power'] == 1_000

# --- Multi key tests
@pytest.mark.asyncio
async def test_order_status_updates_multi_chain_order_id_lifecycle_valid() -> None:
    fake_transport = FakeTransport()
    conn = ConnectCQG(
        "host_name",
        "user_name",
        "password",
        account_id=10000,
        immediate_connect=False,
        client=FakeCQGClient(),
        transport=fake_transport
        )
    conn._timeout = 0.0001

    async with TradeSessionCQG(conn) as TS:
        assert_TS_init(TS)

        TS._symbol_registry.add_symbol('Asset_A', 0)
        TS._symbol_registry.add_metadata('Asset_A', {'A': 'a'})

        TS._active_trade_subs[1] = [SubScope.ORDERS]

        TS.cl_to_chain['cl_order_id_1'] = "chain_order_id_1"
        TS.cl_to_chain['cl_order_id_2'] = "chain_order_id_2"
        q1 = TS._exec_stream_router.subscribe("chain_order_id_1")
        q2 = TS._exec_stream_router.subscribe("chain_order_id_2")
        TS._active_order_q["chain_order_id_1"] = q1
        TS._active_order_q["chain_order_id_2"] = q2

        # --- Stage 1: both active, no cross-contamination
        response_A1 = build_order_statuses_server_msg(
            ServerMsg(),
            res=OrderStatus.Status.IN_TRANSIT,
            contract_id = 0,
            sub_ids = [CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_ORDERS],
            order_id="order_id_A1",
            chain_order_id="chain_order_id_1",
            order=None,
            account_id=conn._account_id
            )
        response_B1 = build_order_statuses_server_msg(
            ServerMsg(),
            res=OrderStatus.Status.WORKING,
            contract_id = 0,
            sub_ids = [CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_ORDERS],
            order_id="order_id_B1",
            chain_order_id="chain_order_id_2",
            order=None,
            account_id=conn._account_id
            )
        await fake_transport.in_q.put(response_A1)
        await fake_transport.in_q.put(response_B1)
        await asyncio.sleep(0.001)

        assert TS.latest_order_state_by_chain["chain_order_id_1"]["order_id"] == "order_id_A1"
        assert TS.latest_order_state_by_chain["chain_order_id_1"]["status"] == OrderStatus_MAP_CQG2INT[OrderStatus.Status.IN_TRANSIT]
        assert TS.latest_order_state_by_chain["chain_order_id_2"]["order_id"] == "order_id_B1"
        assert TS.latest_order_state_by_chain["chain_order_id_2"]["status"] == OrderStatus_MAP_CQG2INT[OrderStatus.Status.WORKING]
        assert TS.active_order_ids_by_chain["chain_order_id_1"][0] == "order_id_A1"
        assert TS.active_order_ids_by_chain["chain_order_id_2"][0] == "order_id_B1"

        # --- Stage 2: terminal on chain_1 only, chain_2 survives
        response_A2 = build_order_statuses_server_msg(
            ServerMsg(),
            res=OrderStatus.Status.FILLED,
            contract_id=0,
            sub_ids = [CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_ORDERS],
            order_id="order_id_A2",
            chain_order_id="chain_order_id_1",
            order=None,
            account_id=conn._account_id
            )
        await fake_transport.in_q.put(response_A2)
        await asyncio.sleep(0.001)

        assert TS.latest_order_state_by_chain["chain_order_id_1"]["status"] == OrderStatus_MAP_CQG2INT[OrderStatus.Status.FILLED]
        assert TS._active_order_q.get("chain_order_id_1") is None
        assert TS._exec_stream_router._subs.get("chain_order_id_1") is None

        assert TS._active_order_q.get("chain_order_id_2") is not None
        assert TS._exec_stream_router._subs.get("chain_order_id_2") is not None
        assert TS.latest_order_state_by_chain["chain_order_id_2"]["order_id"] == "order_id_B1"
        
        assert TS.active_order_ids_by_chain.get("chain_order_id_1") is None
        assert TS.active_order_ids_by_chain["chain_order_id_2"][0] == "order_id_B1"

        response_B2 = build_order_statuses_server_msg(
            ServerMsg(),
            res=OrderStatus.Status.CANCELLED,
            contract_id = 0,
            sub_ids = [CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_ORDERS],
            order_id="order_id_B2",
            chain_order_id="chain_order_id_2",
            order=None,
            account_id=conn._account_id
            )
        await fake_transport.in_q.put(response_B2)
        await asyncio.sleep(0.001)

        assert TS._active_order_q.get("chain_order_id_2") is  None
        assert TS._exec_stream_router._subs.get("chain_order_id_2") is None

@pytest.mark.asyncio
async def test_position_status_updates_multi_contract_id_lifecycle_valid() -> None:
    fake_transport = FakeTransport()
    conn = ConnectCQG(
        "host_name",
        "user_name",
        "password",
        account_id=10000,
        immediate_connect=False,
        client=FakeCQGClient(),
        transport=fake_transport
        )
    conn._timeout = 0.0001

    async with TradeSessionCQG(conn) as TS:
        assert_TS_init(TS)

        TS._symbol_registry.add_symbol('Asset_A', 0)
        TS._symbol_registry.add_metadata('Asset_A', {'A': 'a'})
        TS._symbol_registry.add_symbol('Asset_B', 1)
        TS._symbol_registry.add_metadata('Asset_B', {'B': 'b'})

        TS._active_trade_subs[1] = [SubScope.POSITIONS]

        q0 = TS._pos_status_stream_router.subscribe(0)
        q1 = TS._pos_status_stream_router.subscribe(1)
        TS._active_pos_q[0] = q0
        TS._active_pos_q[1] = q1

        # --- Stage 1: both active
        response_A1 = build_position_statuses_server_msg(
            ServerMsg(),
            subscription_ids = [CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_POSITIONS],
            contract_id = 0,
            account_id=conn._account_id,
            qty=2
            )
        response_B1 = build_position_statuses_server_msg(
            ServerMsg(),
            subscription_ids = [CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_POSITIONS],
            contract_id=1,
            account_id=conn._account_id,
            qty=5
            )
        await fake_transport.in_q.put(response_A1)
        await fake_transport.in_q.put(response_B1)
        await asyncio.sleep(0.001)

        assert TS.latest_pos_status_by_contract_id[0]['open_positions'][0]['qty'] == 2
        assert TS.latest_pos_status_by_contract_id[1]['open_positions'][0]['qty'] == 5

        # --- Stage 2: contract_id 0 goes flat, contract_id 1 survives
        response_A2 = build_position_statuses_server_msg(
            ServerMsg(),
            subscription_ids = [CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_POSITIONS],
            contract_id=0,
            account_id=conn._account_id,
            qty=0
            )
        await fake_transport.in_q.put(response_A2)
        await asyncio.sleep(0.001)

        assert TS.latest_pos_status_by_contract_id[0]['open_positions'][0]['qty'] == 0
        assert TS._active_pos_q.get(0) is None
        assert TS._pos_status_stream_router._subs.get(0) is None

        assert TS._active_pos_q.get(1) is not None
        assert TS._pos_status_stream_router._subs.get(1) is not None
        assert TS.latest_pos_status_by_contract_id[1]['open_positions'][0]['qty'] == 5


@pytest.mark.asyncio
async def test_account_summary_updates_multi_account_id_lifecycle() -> None:
    fake_transport = FakeTransport()
    conn = ConnectCQG(
        "host_name",
        "user_name",
        "password",
        account_id=10000,
        immediate_connect=False,
        client=FakeCQGClient(),
        transport=fake_transport
        )
    conn._timeout = 0.0001

    async with TradeSessionCQG(conn) as TS:
        assert_TS_init(TS)

        TS._symbol_registry.add_symbol('Asset_A', 0)
        TS._symbol_registry.add_metadata('Asset_A', {'A': 'a'})

        TS._active_trade_subs[1] = [SubScope.ACCOUNT_SUMMARY]

        account_id_2 = 20000
        q1 = TS._acc_summary_stream_router.subscribe(conn._account_id)
        q2 = TS._acc_summary_stream_router.subscribe(account_id_2)
        TS._active_acc_summary_q[conn._account_id] = q1
        TS._active_acc_summary_q[account_id_2] = q2

        # --- Stage 1: both updated
        response_1 = build_account_summary_statuses_server_msg(
            ServerMsg(),
            account_id=conn._account_id,
            purchasing_power=1_000_000
            )
        response_2 = build_account_summary_statuses_server_msg(
            ServerMsg(),
            account_id=account_id_2,
            purchasing_power=500_000
            )
        await fake_transport.in_q.put(response_1)
        await fake_transport.in_q.put(response_2)
        await asyncio.sleep(0.001)

        assert TS.latest_account_summaries[conn._account_id]['purchasing_power'] == 1_000_000
        assert TS.latest_account_summaries[account_id_2]['purchasing_power'] == 500_000

        # --- Stage 2: each overwrites independently
        response_3 = build_account_summary_statuses_server_msg(
            ServerMsg(),
            account_id=conn._account_id,
            purchasing_power=999
            )
        await fake_transport.in_q.put(response_3)
        await asyncio.sleep(0.001)

        assert TS.latest_account_summaries[conn._account_id]['purchasing_power'] == 999
        assert TS.latest_account_summaries[account_id_2]['purchasing_power'] == 500_000


# --- Multi keys, multi streams
@pytest.mark.asyncio
async def test_mix_streams_multi_id_lifecycle_valid() -> None:
    fake_transport = FakeTransport()
    conn = ConnectCQG(
        "host_name",
        "user_name",
        "password",
        account_id=10000,
        immediate_connect=False,
        client=FakeCQGClient(),
        transport=fake_transport
        )
    conn._timeout = 0.0001

    async with TradeSessionCQG(conn) as TS:
        assert_TS_init(TS)

        TS._symbol_registry.add_symbol('Asset_A', 0)
        TS._symbol_registry.add_metadata('Asset_A', {'A': 'a'})
        TS._symbol_registry.add_symbol('Asset_B', 1)
        TS._symbol_registry.add_metadata('Asset_B', {'B': 'b'})

        TS._active_trade_subs[1] = [
            SubScope.ORDERS,
            SubScope.POSITIONS,
            SubScope.ACCOUNT_SUMMARY
            ]
        
        ord_q0 = TS._exec_stream_router.subscribe("chain_order_id_0")
        ord_q1 = TS._exec_stream_router.subscribe("chain_order_id_1")
        TS._active_order_q["chain_order_id_0"] = ord_q0
        TS._active_order_q["chain_order_id_1"] = ord_q1

        pos_q0 = TS._pos_status_stream_router.subscribe(0)
        pos_q1 = TS._pos_status_stream_router.subscribe(1)
        TS._active_pos_q[0] = pos_q0
        TS._active_pos_q[1] = pos_q1
        
        account_id_2 = 20000
        acc_q1 = TS._acc_summary_stream_router.subscribe(conn._account_id)
        acc_q2 = TS._acc_summary_stream_router.subscribe(account_id_2)
        TS._active_acc_summary_q[conn._account_id] = acc_q1
        TS._active_acc_summary_q[account_id_2] = acc_q2
        
        # --- Stage 1:
        response_ord_0_1 = build_order_statuses_server_msg(
            ServerMsg(),
            res=OrderStatus.Status.IN_TRANSIT,
            contract_id=0,
            sub_ids = [
                CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_ORDERS,
                CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_POSITIONS,
                CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_ACCOUNT_SUMMARY,                
                ],
            order_id="order_id_A1",
            chain_order_id="chain_order_id_0",
            order=None,
            account_id=conn._account_id
            )
        response_ord_1_1 = build_order_statuses_server_msg(
            ServerMsg(),
            res=OrderStatus.Status.WORKING,
            contract_id=0,
            sub_ids = [
                CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_ORDERS,
                CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_POSITIONS,
                CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_ACCOUNT_SUMMARY,                
                ],
            order_id="order_id_B1",
            chain_order_id="chain_order_id_1",
            order=None,
            account_id=conn._account_id
            )    
        
        response_pos_0_1 = build_position_statuses_server_msg(
            ServerMsg(),
            subscription_ids = [
                CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_ORDERS,
                CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_POSITIONS,
                CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_ACCOUNT_SUMMARY,                
                ],
            contract_id=0,
            account_id=conn._account_id,
            qty=2
            )
        response_pos_1_1 = build_position_statuses_server_msg(
            ServerMsg(),
            subscription_ids = [
                CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_ORDERS,
                CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_POSITIONS,
                CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_ACCOUNT_SUMMARY,                
                ],
            contract_id=1,
            account_id=conn._account_id,
            qty=5
            )
            
        response_acc_1_1 = build_account_summary_statuses_server_msg(
            ServerMsg(),
            account_id=conn._account_id,
            purchasing_power=1_000_000
            )
        response_acc_2_1 = build_account_summary_statuses_server_msg(
            ServerMsg(),
            account_id=account_id_2,
            purchasing_power=500_000
            )

        # assertion for _trade_work_evt clear()
        await fake_transport.in_q.put(response_ord_0_1)
        await asyncio.sleep(0.0001)
        assert not conn._trade_work_evt.is_set()
        
        await fake_transport.in_q.put(response_pos_0_1)
        await asyncio.sleep(0.0001)
        assert not conn._trade_work_evt.is_set()

        await fake_transport.in_q.put(response_pos_1_1)
        await asyncio.sleep(0.0001)
        assert not conn._trade_work_evt.is_set()

        await fake_transport.in_q.put(response_acc_1_1)
        await asyncio.sleep(0.0001)
        assert not conn._trade_work_evt.is_set()

        await fake_transport.in_q.put(response_ord_1_1)
        await asyncio.sleep(0.0001)
        assert not conn._trade_work_evt.is_set()

        await fake_transport.in_q.put(response_acc_2_1)
        await asyncio.sleep(0.001)
        assert not conn._trade_work_evt.is_set()

        # Final result assertion
        assert ord_q0.empty()
        assert ord_q1.empty()
        assert TS.latest_order_state_by_chain["chain_order_id_0"]["order_id"] == "order_id_A1"
        assert TS.latest_order_state_by_chain["chain_order_id_0"]["status"] == OrderStatus_MAP_CQG2INT[OrderStatus.Status.IN_TRANSIT]
        assert TS.latest_order_state_by_chain["chain_order_id_1"]["order_id"] == "order_id_B1"
        assert TS.latest_order_state_by_chain["chain_order_id_1"]["status"] == OrderStatus_MAP_CQG2INT[OrderStatus.Status.WORKING]

        assert TS.active_order_ids_by_chain["chain_order_id_0"][0] == "order_id_A1"
        assert TS.active_order_ids_by_chain["chain_order_id_1"][0] == "order_id_B1"

        assert pos_q0.empty()
        assert pos_q1.empty()
        assert TS.latest_pos_status_by_contract_id[0]['open_positions'][0]['qty'] == 2
        assert TS.latest_pos_status_by_contract_id[1]['open_positions'][0]['qty'] == 5

        assert acc_q1.empty()
        assert acc_q2.empty()
        assert TS.latest_account_summaries[conn._account_id]['purchasing_power'] == 1_000_000
        assert TS.latest_account_summaries[account_id_2]['purchasing_power'] == 500_000

        # Check at least one terminal cleanup
        response_ord_1_2 = build_order_statuses_server_msg(
            ServerMsg(),
            res=OrderStatus.Status.REJECTED,
            contract_id=0,
            sub_ids= [
                CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_ORDERS,
                CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_POSITIONS,
                CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_ACCOUNT_SUMMARY,                
                ],
            order_id="order_id_B2",
            chain_order_id="chain_order_id_1",
            order=None,
            account_id=conn._account_id
            )    
        await fake_transport.in_q.put(response_ord_1_2)
        await asyncio.sleep(0.0001)
        
        assert TS.active_order_ids_by_chain.get("chain_order_id_1") is None

        assert ord_q1.empty()


# Empty open position cleanup for position status
@pytest.mark.asyncio
async def test_position_status_cleanup_empty_open_position_valid() -> None:
    # --- Setup ---
    fake_transport = FakeTransport()
    conn = ConnectCQG(
        "host_name", 
        "user_name", 
        "password",
        account_id = 10000,
        immediate_connect= False, 
        client = FakeCQGClient(),
        transport = fake_transport
        )
    conn._timeout = 0.0001

    async with TradeSessionCQG(conn) as TS:
        # Check if things are empty in the begining
        assert_TS_init(TS)
        
        # Trade Session Setup
        TS._symbol_registry.add_symbol('Asset_A', 0)
        TS._symbol_registry.add_metadata('Asset_A', {'A':'a'})
        
        TS._active_trade_subs[1] = [SubScope.POSITIONS]
        
        # stream subscription
        q = TS._pos_status_stream_router.subscribe(0)
        TS._active_pos_q[0] = q
        
        # --- Stage 1
        response_1 = build_position_statuses_server_msg(
            ServerMsg(),
            subscription_ids = [CQG_TS.SubscriptionScope.SUBSCRIPTION_SCOPE_POSITIONS],
            contract_id = 0,
            account_id = conn._account_id,
            qty = 1, # <--- despite qty != 0 but 
            open_pos_not_empty= False #<--- empty list of open positions should trigget cleanup
            )
        await fake_transport.in_q.put(response_1)
        await asyncio.sleep(0.001)
        
        assert TS.latest_pos_status_by_contract_id[0]['contract_id'] == 0
        assert not TS.latest_pos_status_by_contract_id[0].get('open_positions')
        
        assert TS._active_pos_q.get(0) is None
        assert TS._pos_status_stream_router._subs.get(0) is None
