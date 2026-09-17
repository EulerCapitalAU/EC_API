import asyncio
import pytest
from datetime import datetime, timedelta, timezone
from EC_API.connect.cqg.base import ConnectCQG
from EC_API.ordering.enums import RequestType, SubScope
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
    LiveOrderRequestError,
    MissingSymbolResolutionError,
    TradeSubscriptionMissingError,
    MissingOrderIDError
    )
from tests.unit.fixtures.proxy_clients import FakeCQGClient, FakeTransport
from tests.unit.fixtures.dummy_server_CQG import FakeDataServerCQG

@pytest.fixture
async def conn(timeout = None):
    c = ConnectCQG(
        "host_name", "user_name", "password",
        account_id=10000,
        immediate_connect=False,
        client=FakeCQGClient(),
        transport=FakeTransport(),
    )
    c._timeout = 0.01 if timeout is None else timeout
    return c

# ------ Happy Path 1: Ack  ------
@pytest.mark.asyncio
async def test_new_order_request_send_valid(conn) -> None:
    # ---
    success_decisions = {
        "new_order_request": True,
        "information_requests": True
        }
    loop = asyncio.get_running_loop()
    fake_server = FakeDataServerCQG(
        conn, loop, success_decisions = success_decisions
        )
    # --- Send
    request_details = {
        "symbol_name": "CLE",
        "cl_order_id": "1231314",
        "order_type": OrderType.LMT, 
        "duration": Duration.GTC, 
        "side": Side.BUY,
        "qty": 2,
        "is_manual": False,
        "limit_price": 150,
        "exec_instructions": ExecInstruction.NONE
        }
    metadata = {'CLE': "something", "contract_id": 1}
    
    async def send_order():
        async with TradeSessionCQG(conn) as TS:
            TS._symbol_registry.register('CLE', metadata)        
            TS._active_trade_subs[1] = [SubScope.ORDERS]
                       
            result = await LiveOrderCQG(TS).send(
                                RequestType.NEW_ORDER,
                                request_details=request_details
                            )
            await asyncio.sleep(0.1)
            assert '1231314' in TS.cl_to_chain
            assert len(TS._pending_chain_q) == 0 # Empty pending, tracker loop working
            assert 'chain_order_id_0' in TS._active_order_q 
            assert not conn._trade_work_evt.is_set() # tracker loop unset the work event
            return result

    result, _ = await asyncio.gather(send_order(), fake_server.run(contract_id=1))
    assert isinstance(result, list)
    assert result[0]['order_id'] == 'order_id_0'
    assert result[0]['chain_order_id'] == 'chain_order_id_0'
    
    assert result[0]['order']['account_id'] == conn._account_id
    assert result[0]['order']['contract_id'] == 1
    assert result[0]['order']['cl_order_id'] == '1231314'
    assert result[0]['order']['qty'] == 2
    #assert result[0]['order']['qty']['exponent'] == 0
    assert result[0]['order']['scaled_limit_price'] == 150

@pytest.mark.asyncio
async def test_modify_order_request_send_valid(conn) -> None:
    # ---
    success_decisions = {
        "modify_order_request": True,
        "information_requests": True
        }
    loop = asyncio.get_running_loop()
    fake_server = FakeDataServerCQG(
        conn, loop, success_decisions = success_decisions
        )

    # --- Send
    request_details = {
        "symbol_name": "CLE",
        "order_id": "order_id_1",
        "cl_order_id": "1231314",
        "orig_cl_order_id" : "1313",
        "qty": 12,
        }
    metadata = {'CLE': "something", "contract_id": 0}
    
    async def send_order():
        async with TradeSessionCQG(conn) as TS:
            TS._symbol_registry.register('CLE', metadata)        
            TS._active_trade_subs[1] = [SubScope.ORDERS]
            TS._active_order_q['chain_order_id_1']  = asyncio.Queue()
            
            TS.latest_order_state_by_chain['chain_order_id_1'] = {'order_id': 'order_id_1'}
            TS.active_order_ids_by_chain['chain_order_id_1'] = ('order_id_1', 10101)

            return await LiveOrderCQG(TS).send(
                RequestType.MODIFY_ORDER, 
                request_details=request_details
                )
            
    result, _ = await asyncio.gather(send_order(), fake_server.run(contract_id=0))
    assert isinstance(result, list)
    assert result[0]['order_id'] == 'order_id_1'


@pytest.mark.asyncio
async def test_cancel_order_request_send_valid(conn) -> None:
    # ---
    success_decisions = {
        "cancel_order_request": True,
        "information_requests": True
        }
    loop = asyncio.get_running_loop()
    fake_server = FakeDataServerCQG(
        conn, loop, success_decisions = success_decisions
        )

    # --- Send
    request_details = {
        "symbol_name": "CLE",
        'order_id': "1122",
        'orig_cl_order_id': "1231314",
        'cl_order_id': "1313"
        }
    metadata = {'CLE': "something", "contract_id": 0}
    
    async def send_order():
        async with TradeSessionCQG(conn) as TS:
            TS._symbol_registry.register('CLE', metadata)        
            TS._active_trade_subs[1] = [SubScope.ORDERS]
            TS._active_order_q['chain_order_id_1']  = asyncio.Queue()
            
            TS.latest_order_state_by_chain['chain_order_id_1'] = {'order_id': '1122'}
            TS.active_order_ids_by_chain['chain_order_id_1'] = ('1122', 10101)

            return await LiveOrderCQG(TS).send(
                RequestType.CANCEL_ORDER, 
                request_details=request_details
                )
    result, _ = await asyncio.gather(send_order(), fake_server.run(contract_id=0))
    assert isinstance(result, list)
    assert result[0]['order_id'] == '1122'
            
@pytest.mark.asyncio
async def test_activate_order_request_send_valid(conn) -> None:
    # ---
    success_decisions = {
        "activate_order_request": True,
        "information_requests": True
        }
    loop = asyncio.get_running_loop()
    fake_server = FakeDataServerCQG(
        conn, loop, success_decisions = success_decisions
        )

    # --- Send
    request_details = {
        "symbol_name": "CLE",
        'order_id': "1122",
        'orig_cl_order_id':"1231314",
        'cl_order_id':  "1313",
        "when_utc_timestamp": datetime.now(tz=timezone.utc) + timedelta(minutes=10)
        }
    metadata = {'CLE': "something", "contract_id": 0}
    
    async def send_order():
        async with TradeSessionCQG(conn) as TS:
            TS._symbol_registry.register('CLE', metadata)        
            TS._active_trade_subs[1] = [SubScope.ORDERS]
            TS._active_order_q['chain_order_id_1']  = asyncio.Queue()
            
            TS.latest_order_state_by_chain['chain_order_id_1'] = {'order_id': '1122'}
            TS.active_order_ids_by_chain['chain_order_id_1'] = ('1122', 10101)

            return await LiveOrderCQG(TS).send(
                RequestType.ACTIVATE_ORDER, 
                request_details=request_details
                )
    result, _ = await asyncio.gather(send_order(), fake_server.run(contract_id=0))
    assert isinstance(result, list)
    assert result[0]['order_id'] == '1122'
    
@pytest.mark.asyncio
async def test_cancelall_order_request_send_valid(conn) -> None:
    # ---
    success_decisions = {
        "cancelall_order_request": True,
        "information_requests": True
        }
    loop = asyncio.get_running_loop()
    fake_server = FakeDataServerCQG(
        conn, loop, success_decisions = success_decisions
        )

    # --- Send
    request_details = {
        #"symbol_name": "CLE",
        'cl_order_id': "1313",
        'when_utc_timestamp': datetime.now(tz=timezone.utc)
        }
    metadata = {'CLE': "something", "contract_id": 0}
    
    async def send_order():
        async with TradeSessionCQG(conn) as TS:
            TS._symbol_registry.register('CLE', metadata)        
            TS._active_trade_subs[1] = [SubScope.ORDERS]
            TS._active_order_q['chain_order_id_1']  = asyncio.Queue()
            
            TS.latest_order_state_by_chain['chain_order_id_1'] = {'order_id': '1122'}
            TS.active_order_ids_by_chain['chain_order_id_1'] = ('1122', 10101)

            return await LiveOrderCQG(TS).send(
                RequestType.CANCELALL_ORDER, 
                request_details=request_details
                )
    result, _ = await asyncio.gather(send_order(), fake_server.run(contract_id=0))
    assert isinstance(result, list)
    assert result[0]['request_id'] == 11
    
@pytest.mark.asyncio
async def test_liquidateall_order_request_send_valid(conn) -> None:
    # ---
    success_decisions = {
        "liquidateall_order_request": True,
        "information_requests": True
        }
    loop = asyncio.get_running_loop()
    fake_server = FakeDataServerCQG(
        conn, loop, success_decisions = success_decisions
        )

    # --- Send
    request_details = {
        "symbol_name": "CLE",
        }
    metadata = {'CLE': "something", "contract_id": 0}
    
    async def send_order():
        async with TradeSessionCQG(conn) as TS:
            TS._symbol_registry.register('CLE', metadata)        
            TS._active_trade_subs[1] = [SubScope.ORDERS]
            TS._active_order_q['chain_order_id_1']  = asyncio.Queue()
            
            TS.latest_order_state_by_chain['chain_order_id_1'] = {'order_id': '1122'}
            TS.active_order_ids_by_chain['chain_order_id_1'] = ('1122', 10101)

            return await LiveOrderCQG(TS).send(
                RequestType.LIQUIDATEALL_ORDER, 
                request_details=request_details
                )
    result, _ = await asyncio.gather(send_order(), fake_server.run(contract_id=0))
    assert isinstance(result, list)
    assert result[0]['request_id'] == 11
    
    
@pytest.mark.asyncio
async def test_goflat_order_request_send_valid(conn) -> None:
    success_decisions = {
        "goflat_order_request": True,
        "information_requests": True
        }
    loop = asyncio.get_running_loop()
    fake_server = FakeDataServerCQG(
        conn, loop, success_decisions = success_decisions
        )

    # --- Send
    request_details = {
        #"symbol_name": "CLE",
        'when_utc_timestamp': datetime.now(tz=timezone.utc),
        }
    metadata = {'CLE': "something", "contract_id": 0}
    
    async def send_order():
        async with TradeSessionCQG(conn) as TS:
            TS._symbol_registry.register('CLE', metadata)        
            TS._active_trade_subs[1] = [SubScope.ORDERS]
            TS._active_order_q['chain_order_id_1']  = asyncio.Queue()
            
            TS.latest_order_state_by_chain['chain_order_id_1'] = {'order_id': '1122'}
            TS.active_order_ids_by_chain['chain_order_id_1'] = ('1122', 10101)

            return await LiveOrderCQG(TS).send(
                RequestType.GOFLAT_ORDER, 
                request_details=request_details
                )
    result, _ = await asyncio.gather(send_order(), fake_server.run(contract_id=0))
    assert isinstance(result, list)
    assert result[0]['request_id'] == 11
    
# ------ Happy Path 2: Reject  ------
@pytest.mark.asyncio
async def test_new_order_request_send_valid_reject(conn) -> None:
    # ---
    success_decisions = {
        "new_order_request": True,
        "information_requests": True
        }
    extra_instructions = {
        "new_order_request_reject": True
        }
    loop = asyncio.get_running_loop()
    fake_server = FakeDataServerCQG(
        conn, loop, success_decisions = success_decisions,
        extra_instructions = extra_instructions
        )
    # --- Send
    request_details = {
        "symbol_name": "CLE",
        "cl_order_id": "1231314",
        "order_type": OrderType.LMT, 
        "duration": Duration.GTC, 
        "side": Side.BUY,
        "qty": 2,
        "is_manual": False,
        "limit_price": 150,
        "exec_instructions": ExecInstruction.NONE
        }
    metadata = {'CLE': "something", "contract_id": 1}
    
    async def send_order():
        async with TradeSessionCQG(conn) as TS:
            TS._symbol_registry.register('CLE', metadata)        
            TS._active_trade_subs[1] = [SubScope.ORDERS]
                       
            result = await LiveOrderCQG(TS).send(
                                RequestType.NEW_ORDER,
                                request_details=request_details
                            )
            await asyncio.sleep(0.1)
            assert len(TS.cl_to_chain) == 0 # empty cl_to_chain, no hand-off
            assert len(TS._pending_chain_q) == 0 # Empty pending, tracker loop working
            assert not conn._trade_work_evt.is_set() 
            return result

    result, _ = await asyncio.gather(send_order(), fake_server.run(contract_id=1))
    assert isinstance(result, list)
    assert result[0]['reject_code'] == 1001

# ------ Sad Path
@pytest.mark.asyncio
async def test_order_request_failed_missing_symbol_name() -> None:
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
    
    request_details = {'A':'a'} # <---no 'symbol_name' field

    async with TradeSessionCQG(conn) as TS:
        with pytest.raises(KeyError):
            await LiveOrderCQG(TS).send(
                RequestType.NEW_ORDER, 
                request_details=request_details
                )
            
@pytest.mark.asyncio
async def test_order_request_failed_no_symbol_resolution() -> None:
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
    
    request_details = {'symbol_name':'CLE'} 

    async with TradeSessionCQG(conn) as TS: # <---no symbol resolution
        with pytest.raises(MissingSymbolResolutionError):
            await LiveOrderCQG(TS).send(
                RequestType.NEW_ORDER, 
                request_details=request_details
                )
            
@pytest.mark.asyncio
async def test_order_request_failed_trade_subscription_order_scope() -> None:
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
    
    request_details = {
        'symbol_name':'CLE', 
        } 
    metadata = {'CLE': "something", "contract_id": 0}
    
    async with TradeSessionCQG(conn) as TS: # <---no Trade Subscription
        TS._symbol_registry.register('CLE', metadata)     
        with pytest.raises(TradeSubscriptionMissingError):
            await LiveOrderCQG(TS).send(
                RequestType.MODIFY_ORDER, 
                request_details=request_details
                )
                
@pytest.mark.asyncio
@pytest.mark.parametrize("request_type", [
    RequestType.MODIFY_ORDER,
    RequestType.ACTIVATE_ORDER,
    RequestType.CANCEL_ORDER,
])
async def test_order_request_failed_no_order_id(
    request_type: RequestType,
    conn,
) -> None:
    metadata = {'CLE': "something", "contract_id": 0}

    async with TradeSessionCQG(conn) as TS:
        TS._symbol_registry.register('CLE', metadata)
        TS._active_trade_subs[1] = [SubScope.ORDERS]

        request_details = {
            'symbol_name': 'CLE', 
            'order_id': 'order_id_0'#<== wrong chain_order_id
            }
        with pytest.raises(MissingOrderIDError):
            await LiveOrderCQG(TS).send(
                request_type,
                request_details=request_details
                )
