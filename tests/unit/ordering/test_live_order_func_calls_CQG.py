#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  9 05:48:49 2026

@author: dexter
"""
import asyncio
import pytest
from datetime import datetime, timezone, timedelta
from EC_API.connect.cqg.base import ConnectCQG
from EC_API.ordering.enums import (
    Side, 
    Duration, 
    OrderType,
    ExecInstruction
    )
from EC_API.ordering.cqg.live_order import LiveOrderCQG
from EC_API.ordering.cqg.trade_session import TradeSessionCQG
from EC_API.exceptions import (
    LiveOrderTimeOutError,
    LiveOrderRequestError
    )
from tests.unit.fixtures.proxy_clients import FakeCQGClient, FakeTransport
from tests.unit.fixtures.dummy_server_CQG import FakeDataServerCQG

# --- Happy Path 1: success ---
@pytest.mark.asyncio
async def test_new_order_request_valid() -> None:
    # --- Setup Connection
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
    # --- Setup fake Server
    success_decisions = {
        "new_order_request": True
        }
    loop = asyncio.get_running_loop()
    fake_server = FakeDataServerCQG(
        conn, loop, success_decisions = success_decisions
        )
    # --- Setup input parameter
    order_info = {
        "account_id": conn._account_id, # <--- only here for internal live_order test.
        "request_id": 111, # <--- only here for internal live_order test.
        "contract_id": 0, # <--- only here for internal live_order test.
        "cl_order_id": "1231314",
        "order_type": OrderType.LMT, 
        "duration": Duration.GTC, 
        "side": Side.BUY,
        "qty": 2,
        "is_manual": False,
        "limit_price": 150,
        "exec_instructions": ExecInstruction.NONE
        }
    
    async def run_order():
        async with TradeSessionCQG(conn) as TS:
            return await LiveOrderCQG(TS, timeout = 0.01)._new_order_request(order_info)
            
    result, _ = await asyncio.gather(run_order(), fake_server.run())
    assert result 
    assert isinstance(result, list)
    assert result[0]['order_id'] == 'order_id_0'
    assert result[0]['chain_order_id'] == 'chain_order_id_0'
    
    assert result[0]['order']['account_id'] == conn._account_id
    assert result[0]['order']['contract_id'] == 0
    assert result[0]['order']['cl_order_id'] == '1231314'
    assert result[0]['order']['qty'] == 2
    #assert result[0]['order']['qty']['exponent'] == 0
    assert result[0]['order']['scaled_limit_price'] == 150
    
@pytest.mark.asyncio
async def test_modify_order_request_valid() -> None:
    # --- Setup Connection
    fake_transport = FakeTransport()
    conn = ConnectCQG(
        "host_name", 
        "user_name", 
        "password", 
        account_id = 100000,
        immediate_connect= False, 
        client = FakeCQGClient(),
        transport = fake_transport 
        )
    # --- Setup fake Server
    success_decisions = {
        "modify_order_request": True
        }
    loop = asyncio.get_running_loop()
    fake_server = FakeDataServerCQG(
        conn, loop, success_decisions = success_decisions
        )
    # --- Setup input parameter
    order_info = {
        "account_id": conn._account_id, # <--- only here for internal live_order test.
        "request_id": 111, # <--- only here for internal live_order test.
        'order_id': "1122", # <--- only here for internal live_order test.
        "cl_order_id": "1231314",
        "orig_cl_order_id" : "1313",
        "qty": 12
        }
    
    async def run_order():
        async with TradeSessionCQG(conn) as TS:
            return await LiveOrderCQG(TS, timeout = 0.01)._modify_order_request(order_info)
    # --- run test ---
    result, _ = await asyncio.gather(run_order(), fake_server.run())
    assert result[0]['order_id'] == "1122"
    assert result[0]['chain_order_id'] == "1122chain"
    assert result[0]['account_id'] == conn._account_id
    
    
@pytest.mark.asyncio
async def test_cancel_order_request_valid() -> None:
    # --- Setup Connection
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
    # --- Setup fake Server
    success_decisions = {
        "cancel_order_request": True
        }
    loop = asyncio.get_running_loop()
    fake_server = FakeDataServerCQG(
        conn, loop, success_decisions = success_decisions
        )
    # --- Setup input parameter
    order_info = {
        'account_id': conn._account_id,
        'request_id': 111,
        'order_id': "1122",
        'orig_cl_order_id': "1231314",
        'cl_order_id': "1313"
        }
    
    async def run_order():
        async with TradeSessionCQG(conn) as TS:
            return await LiveOrderCQG(TS, timeout = 0.01)._cancel_order_request(order_info)
    # --- run test ---
    result, _ = await asyncio.gather(run_order(), fake_server.run())
    assert result[0]['order_id'] == "1122"
    assert result[0]['chain_order_id'] == "1122chain"
    assert result[0]['account_id'] == conn._account_id

@pytest.mark.asyncio
async def test_activate_order_request_valid() -> None:
    # --- Setup Connection
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
    # --- Setup fake Server
    success_decisions = {
        "activate_order_request": True
        }
    loop = asyncio.get_running_loop()
    fake_server = FakeDataServerCQG(
        conn, loop, success_decisions = success_decisions
        )    
    # --- Setup input parameter
    order_info = {
        'account_id': conn._account_id,
        'request_id': 111,
        'order_id': "1122",
        'orig_cl_order_id':"1231314",
        'cl_order_id':  "1313",
        "when_utc_timestamp": datetime.now(tz=timezone.utc) + timedelta(minutes=10)
        }
    
    async def run_order():
        async with TradeSessionCQG(conn) as TS:
            return await LiveOrderCQG(TS, timeout = 0.01)._activate_order_request(order_info)
        
    result, _ = await asyncio.gather(run_order(), fake_server.run())
    assert isinstance(result, list)
    assert result[0]['order_id'] == "1122"
    assert result[0]['chain_order_id'] == "1122chain"
    assert result[0]['account_id'] == conn._account_id

@pytest.mark.asyncio
async def test_cancelall_order_request_valid() -> None:
    # --- Setup Connection
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
    # --- Setup fake Server
    success_decisions = {
        "cancelall_order_request": True
        }
    loop = asyncio.get_running_loop()
    fake_server = FakeDataServerCQG(
        conn, loop, success_decisions = success_decisions
        )    
    # --- Setup input parameter
    order_info = {
        'account_id': conn._account_id,
        'request_id': 111,
        'cl_order_id': "1313",
        'when_utc_timestamp': datetime.now(tz=timezone.utc)
        }

    async def run_order():
        async with TradeSessionCQG(conn) as TS:
            return await LiveOrderCQG(TS, timeout = 0.01)._cancelall_order_request(order_info)
        
    result, _ = await asyncio.gather(run_order(), fake_server.run())
    assert isinstance(result, list)
    assert result[0]['request_id'] == 111 

@pytest.mark.asyncio
async def test_liquidateall_order_request_valid() -> None:
    # --- Setup Connection
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
    # --- Setup fake Server
    success_decisions = {
        "liquidateall_order_request": True
        }
    loop = asyncio.get_running_loop()
    fake_server = FakeDataServerCQG(
        conn, loop, success_decisions = success_decisions
        )    
    # --- Setup input parameter
    order_info = {
        'account_id': conn._account_id,
        'request_id': 111,
        'contract_id': 0
        }

    async def run_order():
        async with TradeSessionCQG(conn) as TS:
            return await LiveOrderCQG(TS, timeout = 0.01)._liquidateall_order_request(order_info)

    result, _ = await asyncio.gather(run_order(), fake_server.run())
    assert isinstance(result, list)
    assert result[0]['request_id'] == 111 

@pytest.mark.asyncio
async def test_goflat_order_request_valid() -> None:
    # --- Setup Connection
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
    # --- Setup fake Server
    success_decisions = {
        "goflat_order_request": True
        }
    loop = asyncio.get_running_loop()
    fake_server = FakeDataServerCQG(
        conn, loop, success_decisions = success_decisions
        )    
    # --- Setup input parameter
    order_info = {
        'account_id': conn._account_id,
        'request_id': 111,
        'when_utc_timestamp': datetime.now(tz=timezone.utc)
        }
    async def run_order():
        async with TradeSessionCQG(conn) as TS:
            return await LiveOrderCQG(TS, timeout = 0.01)._goflat_order_request(order_info)
        
    result, _ = await asyncio.gather(run_order(), fake_server.run())
    assert isinstance(result, list)
    assert result[0]['request_id'] == 111
    assert result[0]['account_id'] == conn._account_id
    

# --- Happy Path 2: success ---
@pytest.mark.asyncio
async def test_new_order_request_valid_reject() -> None:
    # --- Setup Connection
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
    # --- Setup fake Server
    success_decisions = {
        "new_order_request": True
        }
    extra_instructions = {
        "new_order_request_reject": True
        }
    loop = asyncio.get_running_loop()
    fake_server = FakeDataServerCQG(
        conn, loop, success_decisions = success_decisions,
        extra_instructions = extra_instructions 
        )
    # --- Setup input parameter
    order_info = {
        "account_id": conn._account_id, # <--- only here for internal live_order test.
        "request_id": 111, # <--- only here for internal live_order test.
        "contract_id": 0, # <--- only here for internal live_order test.
        "cl_order_id": "1231314",
        "order_type": OrderType.LMT, 
        "duration": Duration.GTC, 
        "side": Side.BUY,
        "qty": 2,
        "is_manual": False,
        "limit_price": 150,
        "exec_instructions": ExecInstruction.NONE
        }
    
    async def run_order():
        async with TradeSessionCQG(conn) as TS:
            return await LiveOrderCQG(TS, timeout = 0.01)._new_order_request(order_info)
            
    result, _ = await asyncio.gather(run_order(), fake_server.run())
    assert result 
    assert isinstance(result, list)
    assert result[0]['request_id'] == 111
    
@pytest.mark.asyncio
async def test_modify_order_request_valid_reject() -> None:
    # --- Setup Connection
    fake_transport = FakeTransport()
    conn = ConnectCQG(
        "host_name", 
        "user_name", 
        "password", 
        account_id = 100000,
        immediate_connect= False, 
        client = FakeCQGClient(),
        transport = fake_transport 
        )
    # --- Setup fake Server
    success_decisions = {
        "modify_order_request": True
        }
    extra_instructions = {
        "modify_order_request_reject": True
        }
    loop = asyncio.get_running_loop()
    fake_server = FakeDataServerCQG(
        conn, loop, success_decisions = success_decisions,
        extra_instructions = extra_instructions 
        )
    # --- Setup input parameter
    order_info = {
        "account_id": conn._account_id, # <--- only here for internal live_order test.
        "request_id": 111, # <--- only here for internal live_order test.
        'order_id': "1122", # <--- only here for internal live_order test.
        "cl_order_id": "1231314",
        "orig_cl_order_id" : "1313",
        "qty": 12
        }
    
    async def run_order():
        async with TradeSessionCQG(conn) as TS:
            return await LiveOrderCQG(TS, timeout = 0.01)._modify_order_request(order_info)
    # --- run test ---
    result, _ = await asyncio.gather(run_order(), fake_server.run())
    
    assert result 
    assert isinstance(result, list)
    assert result[0]['request_id'] == 111
    
@pytest.mark.asyncio
async def test_cancel_order_request_valid_reject() -> None:
    # --- Setup Connection
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
    # --- Setup fake Server
    success_decisions = {
        "cancel_order_request": True
        }
    extra_instructions = {
        "cancel_order_request_reject": True
        }
    loop = asyncio.get_running_loop()
    fake_server = FakeDataServerCQG(
        conn, loop, success_decisions = success_decisions,
        extra_instructions = extra_instructions 
        )
    # --- Setup input parameter
    order_info = {
        'account_id': conn._account_id,
        'request_id': 111,
        'order_id': "1122",
        'orig_cl_order_id': "1231314",
        'cl_order_id': "1313"
        }
    
    async def run_order():
        async with TradeSessionCQG(conn) as TS:
            return await LiveOrderCQG(TS, timeout = 0.01)._cancel_order_request(order_info)
    # --- run test ---
    result, _ = await asyncio.gather(run_order(), fake_server.run())
    assert result 
    assert isinstance(result, list)
    assert result[0]['request_id'] == 111
    
@pytest.mark.asyncio
async def test_activate_order_request_valid_reject() -> None:
    # --- Setup Connection
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
    # --- Setup fake Server
    success_decisions = {
        "activate_order_request": True
        }
    extra_instructions = {
        "activate_order_request_reject": True
        }

    loop = asyncio.get_running_loop()
    fake_server = FakeDataServerCQG(
        conn, loop, success_decisions = success_decisions,
        extra_instructions = extra_instructions 
        )    
    # --- Setup input parameter
    order_info = {
        'account_id': conn._account_id,
        'request_id': 111,
        'order_id': "1122",
        'orig_cl_order_id':"1231314",
        'cl_order_id':  "1313",
        "when_utc_timestamp": datetime.now(tz=timezone.utc) + timedelta(minutes=10)
        }
    
    async def run_order():
        async with TradeSessionCQG(conn) as TS:
            return await LiveOrderCQG(TS, timeout = 0.01)._activate_order_request(order_info)
        
    result, _ = await asyncio.gather(run_order(), fake_server.run())
    assert isinstance(result, list)
    assert result[0]['request_id'] == 111
    
@pytest.mark.asyncio
async def test_cancelall_order_request_valid_reject() -> None:
    # --- Setup Connection
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
    # --- Setup fake Server
    success_decisions = {
        "cancelall_order_request": True
        }
    extra_instructions = {
        "cancelall_order_request_reject": True
        }
    loop = asyncio.get_running_loop()
    fake_server = FakeDataServerCQG(
        conn, loop, success_decisions = success_decisions,
        extra_instructions = extra_instructions 
        )    
    # --- Setup input parameter
    order_info = {
        'account_id': conn._account_id,
        'request_id': 111,
        'cl_order_id': "1313",
        'when_utc_timestamp': datetime.now(tz=timezone.utc)
        }

    async def run_order():
        async with TradeSessionCQG(conn) as TS:
            return await LiveOrderCQG(TS, timeout = 0.01)._cancelall_order_request(order_info)
        
    result, _ = await asyncio.gather(run_order(), fake_server.run())
    assert result 
    assert isinstance(result, list)
    assert result[0]['request_id'] == 111
    
@pytest.mark.asyncio
async def test_liquidateall_order_request_valid_reject() -> None:
    # --- Setup Connection
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
    # --- Setup fake Server
    success_decisions = {
        "liquidateall_order_request": True
        }
    extra_instructions = {
        "liquidateall_order_request_reject": True
        }
    loop = asyncio.get_running_loop()
    fake_server = FakeDataServerCQG(
        conn, loop, success_decisions = success_decisions,
        extra_instructions = extra_instructions 
        )    
    # --- Setup input parameter
    order_info = {
        'account_id': conn._account_id,
        'request_id': 111,
        'contract_id': 0
        }

    async def run_order():
        async with TradeSessionCQG(conn) as TS:
            return await LiveOrderCQG(TS, timeout = 0.01)._liquidateall_order_request(order_info)

    result, _ = await asyncio.gather(run_order(), fake_server.run())
    assert result 
    assert isinstance(result, list)
    assert result[0]['request_id'] == 111
    
@pytest.mark.asyncio
async def test_goflat_order_request_valid_reject() -> None:
    # --- Setup Connection
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
    # --- Setup fake Server
    success_decisions = {
        "goflat_order_request": True
        }
    extra_instructions = {
        "goflat_order_request_reject": True
        }
    loop = asyncio.get_running_loop()
    fake_server = FakeDataServerCQG(
        conn, loop, success_decisions = success_decisions,
        extra_instructions = extra_instructions 
        )    
    # --- Setup input parameter
    order_info = {
        'account_id': conn._account_id,
        'request_id': 111,
        'when_utc_timestamp': datetime.now(tz=timezone.utc),
        }
    async def run_order():
        async with TradeSessionCQG(conn) as TS:
            return await LiveOrderCQG(TS, timeout = 0.01)._goflat_order_request(order_info)
        
    result, _ = await asyncio.gather(run_order(), fake_server.run())
    assert result 
    assert isinstance(result, list)
    assert result[0]['request_id'] == 111
    
# --- Sad Path ---
@pytest.mark.asyncio
async def test_new_order_request_invalid() -> None:
    order_info = dict()
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
    async with TradeSessionCQG(conn) as TS:
        with pytest.raises(LiveOrderRequestError):
            await LiveOrderCQG(TS, timeout = 0.01)._new_order_request(order_info)

@pytest.mark.asyncio
async def test_modify_order_request_invalid() -> None:
    order_info = dict()
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
    async with TradeSessionCQG(conn) as TS:
        with pytest.raises(LiveOrderRequestError):
            await LiveOrderCQG(TS, timeout = 0.01)._modify_order_request(order_info)

@pytest.mark.asyncio
async def test_cancel_order_request_invalid() -> None:
    order_info = dict()
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
    async with TradeSessionCQG(conn) as TS:
        with pytest.raises(LiveOrderRequestError):
            await LiveOrderCQG(TS, timeout = 0.01)._cancel_order_request(order_info)

@pytest.mark.asyncio
async def test_activate_order_request_invalid() -> None:
    order_info = dict()
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

    async with TradeSessionCQG(conn) as TS:
        with pytest.raises(LiveOrderRequestError):
            await LiveOrderCQG(TS, timeout = 0.10)._activate_order_request(order_info)


@pytest.mark.asyncio
async def test_cancelall_order_request_invalid() -> None:
    order_info = dict()
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

    async with TradeSessionCQG(conn) as TS:
        with pytest.raises(LiveOrderRequestError):
            await LiveOrderCQG(TS, timeout = 0.01)._cancelall_order_request(order_info)
        
        

@pytest.mark.asyncio
async def test_liquidateall_order_request_invalid() -> None:
    order_info = dict()
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
    async with TradeSessionCQG(conn) as TS:
        with pytest.raises(LiveOrderRequestError):
            await LiveOrderCQG(TS, timeout = 0.01)._liquidateall_order_request(order_info)


@pytest.mark.asyncio
async def test_goflat_order_request_invalid() -> None:
    order_info = dict()
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
    async with TradeSessionCQG(conn) as TS:
        with pytest.raises(LiveOrderRequestError):
            await LiveOrderCQG(TS, timeout = 0.01)._goflat_order_request(order_info)

# --- Timeout Error
@pytest.mark.asyncio
async def test_new_order_request_invalid_timeout() -> None:
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
    
    order_info = {
        "account_id": conn._account_id, # <--- only here for internal live_order test.
        "request_id": 111, # <--- only here for internal live_order test.
        "contract_id": 0, # <--- only here for internal live_order test.
        "cl_order_id": "1231314",
        "order_type": OrderType.LMT, 
        "duration": Duration.GTC, 
        "side": Side.BUY,
        "qty": 2,
        "is_manual": False,
        "limit_price": 150,
        "exec_instructions": ExecInstruction.NONE
        }
    async with TradeSessionCQG(conn) as TS:
        with pytest.raises(LiveOrderTimeOutError):
            await LiveOrderCQG(TS, timeout = 0.01)._new_order_request(order_info)

@pytest.mark.asyncio
async def test_modify_order_request_invalid_timeout() -> None:
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
    order_info = {
        "account_id": conn._account_id, # <--- only here for internal live_order test.
        "request_id": 111, # <--- only here for internal live_order test.
        'order_id': "1122", # <--- only here for internal live_order test.
        "cl_order_id": "1231314",
        "orig_cl_order_id" : "1313",
        "qty": 12
        }
    async with TradeSessionCQG(conn) as TS:
        with pytest.raises(LiveOrderTimeOutError):
            await LiveOrderCQG(TS, timeout = 0.01)._modify_order_request(order_info)

@pytest.mark.asyncio
async def test_cancel_order_request_invalid_timeout() -> None:
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
    order_info = {
        'account_id': conn._account_id,
        'request_id': 111,
        'order_id': "1122",
        'orig_cl_order_id': "1231314",
        'cl_order_id': "1313"
        }
    async with TradeSessionCQG(conn) as TS:
        with pytest.raises(LiveOrderTimeOutError):
            await LiveOrderCQG(TS, timeout = 0.01)._cancel_order_request(order_info)

@pytest.mark.asyncio
async def test_activate_order_request_invalid_timeout() -> None:
    order_info = {}
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
    order_info = {
        'account_id': conn._account_id,
        'request_id': 111,
        'order_id': "1122",
        'orig_cl_order_id':"1231314",
        'cl_order_id':  "1313",
        "when_utc_timestamp": datetime.now(tz=timezone.utc) + timedelta(minutes=10)
        }
    async with TradeSessionCQG(conn) as TS:
        with pytest.raises(LiveOrderTimeOutError):
            await LiveOrderCQG(TS, timeout = 0.01)._activate_order_request(order_info)


@pytest.mark.asyncio
async def test_cancelall_order_request_invalid_timeout() -> None:
    order_info = dict()
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
    order_info = {
        'account_id': conn._account_id,
        'request_id': 111,
        'cl_order_id': "1313",
        'when_utc_timestamp': datetime.now(tz=timezone.utc)
        }
    async with TradeSessionCQG(conn) as TS:
        with pytest.raises(LiveOrderTimeOutError):
            await LiveOrderCQG(TS, timeout = 0.01)._cancelall_order_request(order_info)
        

@pytest.mark.asyncio
async def test_liquidateall_order_request_invalid_timeout() -> None:
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
    order_info = {
        'account_id': conn._account_id,
        'request_id': 111,
        'contract_id': 0
        }
    async with TradeSessionCQG(conn) as TS:
        with pytest.raises(LiveOrderTimeOutError):
            await LiveOrderCQG(TS, timeout = 0.01)._liquidateall_order_request(order_info)


@pytest.mark.asyncio
async def test_goflat_order_request_invalid_timeout() -> None:
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
    order_info = {
        'account_id': conn._account_id,
        'request_id': 111,
        'when_utc_timestamp': datetime.now(tz=timezone.utc),
        }
    async with TradeSessionCQG(conn) as TS:
        with pytest.raises(LiveOrderTimeOutError):
            await LiveOrderCQG(TS, timeout = 0.01)._goflat_order_request(order_info)