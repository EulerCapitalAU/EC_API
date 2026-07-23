#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May  4 07:14:21 2026

@author: dexter
"""
import pytest
import asyncio
import logging
from EC_API.ext.WebAPI.webapi_2_pb2 import ServerMsg
from EC_API.connect.cqg.base import ConnectCQG
from EC_API.connect.enums import ConnectionState
from EC_API.ordering.cqg.trade_session import TradeSessionCQG
from EC_API.utility.symbol_registry import SymbolRegistry
from EC_API.exceptions import TradeSessionRequestError
from tests.unit.fixtures.proxy_clients import FakeCQGClient, FakeTransport
from tests.unit.fixtures.server_msg_builders_CQG import build_symbol_resolution_report_server_msg

# --- Happy Paths aenter/aexit
@pytest.mark.asyncio
async def test_context_manager_normal_enter_exit_valid() -> None:
    conn = ConnectCQG(
        "host_name", 
        "user_name", 
        "password", 
        immediate_connect= False, 
        client = FakeCQGClient()
        )
    
    async with TradeSessionCQG(conn) as TS:
        assert isinstance(TS, TradeSessionCQG)
        assert TS.state == ConnectionState.CONNECTED_DEFAULT

    # __aexit__ ran stop(), so connection should be fully torn down
    assert TS.state == ConnectionState.CLOSED

# --- Sad Paths aenter/aexit
@pytest.mark.asyncio
async def test_aenter_raises_on_conn_failure(mocker):
    conn = ConnectCQG(
        "host_name", "user_name", "password",
        immediate_connect=False, client=FakeCQGClient(),
    )
    mocker.patch.object(conn, "start", return_value=False)  # connection refuses to come up

    ts = TradeSessionCQG(conn)

    with pytest.raises(TradeSessionRequestError):
        await ts.__aenter__()

    assert ts._tracker_task is None   # nothing was left running
    
@pytest.mark.asyncio
async def test_aexit_conn_stopped_even_if_cleanup_raises(mocker):
    conn = ConnectCQG(
        "host_name", "user_name", "password",
        immediate_connect=False, client=FakeCQGClient(),
    )
    ts = TradeSessionCQG(conn)
    await ts.__aenter__()

    ts._cleanup = mocker.AsyncMock(side_effect=RuntimeError("unexpected"))
    spy_stop = mocker.spy(conn, "stop")

    with pytest.raises(RuntimeError):
        await ts.__aexit__(None, None, None)

    spy_stop.assert_awaited_once()          # teardown happened despite the failure
    assert ts.state == ConnectionState.CLOSED

@pytest.mark.asyncio
async def test_aexit_normal(mocker):
    conn = ConnectCQG(
        "host_name", "user_name", "password",
        immediate_connect=False, client=FakeCQGClient(),
    )
    ts = TradeSessionCQG(conn)
    await ts.__aenter__()

    ts._cleanup = mocker.AsyncMock()

    result = await ts.__aexit__(None, None, None)

    ts._cleanup.assert_awaited_once()
    assert result is False
    assert ts.state == ConnectionState.CLOSED
    assert ts._tracker_task.done()
    
    
# --- Cleanup Success ---
@pytest.mark.asyncio
async def test_context_manager_automatic_cleanup_upon_exit_success() -> None:
    ft = FakeTransport()
    conn = ConnectCQG(
        "host_name", 
        "user_name", 
        "password", 
        immediate_connect= False, 
        client = FakeCQGClient(),
        transport = ft
        )
    
    sr = SymbolRegistry()
    TS = TradeSessionCQG(conn)
    TS._symbol_registry = sr
    
    # First register a symbol info
    TS._symbol_registry.register("asset_A", {'contract_id':1})
    TS._symbol_registry.register("asset_B", {'contract_id':2})
    TS._symbol_registry.register("asset_C", {'contract_id':3})
    
    async def run_TS():
        async with TS:
            assert isinstance(TS, TradeSessionCQG)

    async def grab_and_respond(n: int) -> None:
        loop = asyncio.get_running_loop()
        for _ in range(n):
            client_msg = await loop.run_in_executor(None, ft.out_q.get)
            req = client_msg.information_requests[0]
            assert req.subscribe == False
            symbol = req.symbol_resolution_request.symbol   # use whatever came in
            response = build_symbol_resolution_report_server_msg(
                ServerMsg(), report_id=req.id, contract_symbol=symbol
            )
            await ft.in_q.put(response)
    
    await asyncio.gather(run_TS(), grab_and_respond(3))
    
    assert not TS._symbol_registry.active_symbols
    assert not TS._symbol_registry.metatdata
    
# --- Cleanup Fail ---
@pytest.mark.asyncio
async def test_context_manager_automatic_cleanup_upon_exit_fail_timeout(caplog) -> None:
    ft = FakeTransport()
    conn = ConnectCQG(
        "host_name", 
        "user_name", 
        "password", 
        immediate_connect= False, 
        client = FakeCQGClient(),
        transport = ft
        )
    conn._timeout = 0.001
    sr = SymbolRegistry()
    TS = TradeSessionCQG(conn)
    TS._symbol_registry = sr
    
    # First register a symbol info
    TS._symbol_registry.register("asset_A", {'contract_id':1})
    TS._symbol_registry.register("asset_B", {'contract_id':2})
    TS._symbol_registry.register("asset_C", {'contract_id':3})
    
    async def run_TS():
        async with TS:
            assert isinstance(TS, TradeSessionCQG)

    with caplog.at_level(logging.ERROR, logger="EC_API.connect.cqg.base"):
        await run_TS() # no response message -> timeout
        await asyncio.sleep(0)
        
#--- start()/stop()
@pytest.mark.asyncio
async def test_trade_session_start_stop_lifecycle() -> None:
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

    TS = TradeSessionCQG(conn)

    # --- start()
    await TS.start()

    assert TS.state == ConnectionState.CONNECTED_DEFAULT
    assert TS._tracker_task is not None
    assert not TS._tracker_task.done()

    # --- stop()
    await TS.stop()

    assert TS._tracker_task.done()
    assert TS.state == ConnectionState.CLOSED