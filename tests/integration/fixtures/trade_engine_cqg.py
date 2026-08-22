import asyncio
import logging

from EC_API.channel.base import Channel
from EC_API.channel.redis import RedisChannel
from EC_API.connect.base import Connect
from EC_API.connect.cqg.base import ConnectCQG
from EC_API.ordering.trade_session import TradeSession
from EC_API.ordering.cqg.trade_session import TradeSessionCQG
from EC_API.ordering.cqg.live_order import LiveOrderCQG
from EC_API.ordering.enums import RequestType, SubScope
from EC_API.payload.base import Payload, ExecutePayload
from EC_API.payload.safety import PreTradeRiskCheck
from EC_API.recorders.base import SQLSchemaTable, Recorder
from EC_API.recorders.sqlite_recorder import SQLiteRecorder
from EC_API.exceptions import (
    ChannelMissingSettingError, 
    TradeSessionRequestError,
    TradeSessionTimeOutError,
    ControllerInputError
    )

HOST_NAME, USR_NAME, PASSWORD, ACCOUNT_ID = 0, 0, 0, 0

logger = logging.getLogger(__name__)

# Controller class??
from typing import Protocol, Optional, Callable, Any


class Controller(Protocol):...

class TradeEngineController(Controller):
    def __init__(
            self, 
            trade_session: TradeSession, 
            channel: Channel, 
            pretrade_risk_check: PreTradeRiskCheck
        ):
        self._trade_session = trade_session
        self._channel = channel
        self._ptrc = pretrade_risk_check
        self.SCOPE_MAP = {"order_info": SubScope.ORDERS}
        
    async def add_in_stream(
            self, in_stream_name: str,
            callback: Optional[Callable[[Any], None]] = None
        ) -> None:

        if in_stream_name in self._channel.in_streams:
            raise ControllerInputError(
                f"stream_name: {in_stream_name} is already in the channel."
                )

        # Format Check
        if len(in_stream_name.split(":")) !=2:
            raise ControllerInputError("Incorrect format for stream_name input.")
            
        sub_scope = in_stream_name.split(":")[0] 
        symbol_name = in_stream_name.split(":")[1]
        
        if not self.SCOPE_MAP.get(sub_scope):
            raise ControllerInputError("{sub_scope} is an invalid sub_scope.")


        # See if pre-trade risk para is present
        if not self._ptrc.has_symbol(symbol_name):
            raise ControllerInputError(
                f"symbol_name: {symbol_name} is not in pre-trade risk check."
                )
        # Before adding, either ask for pre-trade risk parameters or load a preset
        try:
            # do trade subscription if scope is not present
            match self.SCOPE_MAP[sub_scope]:
                case SubScope.ORDERS:
                    if not self._trade_session.has_orders_scope():
                        next_sub_id = max(self._trade_session._active_trade_subs.keys(), default=0) + 1
                        await self._trade_session.trade_subscription_request(next_sub_id, self.SCOPE_MAP[sub_scope])
            
            # Symbol Resolution
            if not self._trade_session.has_symbol(symbol_name):
                await self._trade_session.resolve_symbol(symbol_name)
                
            # Add in_stream, add symbols, 
            self._channel.in_streams.add(in_stream_name)
                    
            if callback:
                # For each new in_stream added, make a new async task send_loop
                callback(in_stream_name)
                
        except (TradeSessionRequestError, TradeSessionTimeOutError) as e:
            raise ControllerInputError(
                f"Fail to perform a trade session request: {str(e)}."
                )

    async def remove_in_stream(
            self, 
            in_stream_name: str,
            callback: Optional[Callable[[Any], Any]] = None
        ) -> Any:
        if in_stream_name not in self._channel.in_streams:
            raise ControllerInputError(
                f"stream_name: {in_stream_name} is not in the channel."
                )
        # Check if it can be legally removed.
        # !!! Add TTL lock on the strategy side, periodically renew it
        # CHeck it here
        
        
        if callback:
            return callback(in_stream_name)
        else:
            return None
            

class TradeEngineCQG:
    def __init__(
            self, 
            channel_cfg_addr: str, 
            pretraderisk_cfg_addr:str
        ):
        # ---- IPC Channel setting ----
        self.channel: Channel = RedisChannel(channel_cfg_addr)
        self.recorder: Recorder = SQLiteRecorder(
            schema = SQLSchemaTable(
                table_name = "test_trade_engine_audit", 
                columns = (("", "", ""), ("", "", ""),)
                ), 
            db_address = ""
            )
            
        # ---- Sessions setting ----
        self.conn: Connect = ConnectCQG(HOST_NAME, USR_NAME, PASSWORD, ACCOUNT_ID)
        self.trade_session: TradeSession = TradeSessionCQG(self.conn, recorder=self.recorder)
        
        # ---- Risk checks ----
        self.PREC: PreTradeRiskCheck = PreTradeRiskCheck('cqg')
        self.PREC.load(pretraderisk_cfg_addr)
        
        # ---- Engine property ----
        self._stop_evt: asyncio.Event = asyncio.Event()
        self.state = None
        
        # ---- Engine Containers ----
        self._send_order_tasks: dict[str, asyncio.Task] = dict()
        
        # ---- Channel and Control----
        self.CTRL_STREAM: str = "CMD:Trade_Engine"
        self.controller: Controller = TradeEngineController(
            self.trade_session, self.channel, self.PREC
            )
                    
    # ------- Engine functions (Trade)
    async def _package_and_send(
            self, 
            order_type: RequestType, 
            order_info: dict
        ) -> None:
        PL = Payload(
          order_request_type = order_type,
          order_info = order_info,
          risk_check = self.PREC # Static risk check done upon creation
          )
        await ExecutePayload(live_order=LiveOrderCQG(self.trade_session)).unload(PL)
        
        
    def _add_send_task_to_map(self, in_stream_name: str) -> None:
        # For each new in_stream added, make a new async task send_loop
        self._send_order_tasks[in_stream_name] = asyncio.create_task(
            self._send_order_loop(in_stream_name)
            )
        
    def _remove_task(self, in_stream_name: str) -> asyncio.Task:
        task = self._send_order_tasks.pop(in_stream_name, None)
        if task is not None:
            task.cancel()

        self.channel.in_streams.discard(in_stream_name)
        self.channel.last_ids.pop(in_stream_name, None)   
        return task

    async def _send_order_loop(self, in_stream_name: str) -> None:
        while not self._stop_evt.is_set():
            # Continuous listening to the latest order instruction from redis stream
            msg = await self.channel.listen(in_stream_name) 
            
            # If there is something, an event is triggered
            if msg is None:
                continue
            # package and send (fire and forget)
            order_type, order_info = msg
            await self._package_and_send(order_type, order_info)
            
    # ------- Controls
    async def _control_loop(self):
        # cmd: add_stream, remove_stream, start_engine, stop_engine
        while not self._stop_evt.is_set():
            try:
                # Listen to control command and add/remove in_stream
                cmd = await self.channel.listen(stream_name=self.CTRL_STREAM, data_name="")
                if cmd is None:
                    continue
                
                match cmd[0]: # ("ADD", "order_info:WTI")
                    case "ADD":
                        await self.controller.add_in_stream(
                            cmd[1], callback = self._add_send_task_to_map
                            )
                    case "REMOVE":
                        task = await self.controller.remove_in_stream(
                            cmd[1], callback = self._remove_task)
                        try:
                            await task          # await ONLY here — to let CancelledError settle
                        except asyncio.CancelledError:
                            pass
                    case _:
                        logger.warning("[Trade Engine] Unknown Command: %s", cmd[0])
            except ControllerInputError as e:
                logger.error("[Trade Engine] Control_loop error: %s", e)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("[Trade Engine] Control_loop error: %s", e, exc_info=True)

    # -------- Engine LifeCycle
    async def _setup(self):
        # Connect to channel
        try:
            await self.channel.connect()
        except ChannelMissingSettingError as e:
            logger.warning("[Trade Engine]: " + str(e))
            
        # start trade session
        await self.trade_session.start()
        
        # start control loop
        
        # subscribe all the trade subscriptions and pre-resolve symbols
        for stream_name in self.channel.in_streams:
            ...
            #await trade_session.trade_subscription_request(sub_id=1, sub_scope = SubScope.ORDERS)
            #await trade_session.resolve_symbol(order_info['symbol_name'])         
    
    async def start(self):
        await self._setup()

    async def stop(self):...
                
    async def run(self):
        await self.start()
        try:
            await self._stop_evt.wait()
        finally:
            await self.stop()
