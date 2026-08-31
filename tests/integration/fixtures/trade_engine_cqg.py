import asyncio
import logging
from typing import Protocol, Optional, Callable, Any

from EC_API.channel.base import Channel
from EC_API.channel.redis import RedisChannel
from EC_API.connect.base import Connect
from EC_API.connect.enums import ConnectionState
from EC_API.connect.cqg.base import ConnectCQG
from EC_API.ordering.trade_session import TradeSession
from EC_API.ordering.cqg.trade_session import TradeSessionCQG
from EC_API.ordering.cqg.live_order import LiveOrderCQG
from EC_API.ordering.enums import RequestType, SubScope
from EC_API.payload.base import Payload, ExecutePayload
from EC_API.payload.safety import PreTradeRiskCheck
from EC_API.recorders.base import SQLSchemaTable, Recorder
from EC_API.recorders.sqlite_recorder import SQLiteRecorder
from EC_API.utility.state_mgr import StateMgr
from EC_API.exceptions import (
    ConnectRequestError,
    ConnectTimeOutError,
    ChannelMissingSettingError, 
    ChannelListenError,
    TradeSessionRequestError,
    TradeSessionTimeOutError,
    RecorderCriticalError,
    ControllerInputError,
    RiskViolationError
    )
from tests.integration.fixtures.engine_enums import (
    EngineState, ENGINESTATE_LIFECYCLE
    )

HOST_NAME, USR_NAME, PASSWORD, ACCOUNT_ID = 0, 0, 0, 0
PRIVATE_LABEL = 0

logger = logging.getLogger(__name__)

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
        
    async def _subscribe_and_resolve(self, sub_scope: str, symbol_name: str) -> None:
        try:
            match self.SCOPE_MAP[sub_scope]:
                case SubScope.ORDERS:
                    if not self._trade_session.has_orders_scope():
                        next_sub_id = max(self._trade_session._active_trade_subs.keys(), default=0) + 1
                        await self._trade_session.trade_subscription_request(next_sub_id, self.SCOPE_MAP[sub_scope])

            if not self._trade_session.has_symbol(symbol_name):
                await self._trade_session.resolve_symbol(symbol_name)

        except (TradeSessionRequestError, TradeSessionTimeOutError) as e:
            raise ControllerInputError(
                f"Fail to perform a trade session request: {str(e)}."
                )
            
            
    async def add_in_stream(
            self, in_stream_name: str,
            callback: Optional[Callable[[Any], None]] = None,
            auto_sub: bool = True
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
            raise ControllerInputError(f"{sub_scope} is an invalid sub_scope.")


        # See if pre-trade risk para is present
        if not self._ptrc.has_symbol(symbol_name):
            raise ControllerInputError(
                f"symbol_name: {symbol_name} is not in pre-trade risk check."
                )
        if auto_sub:
            await self._subscribe_and_resolve(sub_scope, symbol_name)
            
        # Add in_stream, add symbols
        self._channel.in_streams.add(in_stream_name)

        if callback:
            callback(in_stream_name)

    async def remove_in_stream(
            self, 
            in_stream_name: str,
            callback: Optional[Callable[[Any], Any]] = None,
            auto_unsub: bool = True
        ) -> Optional[Any]:
        if in_stream_name not in self._channel.in_streams:
            raise ControllerInputError(
                f"stream_name: {in_stream_name} is not in the channel."
                )
            
        # Format Check
        if len(in_stream_name.split(":")) !=2:
            raise ControllerInputError("Incorrect format for stream_name input.")
            
        sub_scope = in_stream_name.split(":")[0] 
        symbol_name = in_stream_name.split(":")[1]

        # Check if it can be legally removed.
        # !!! Add TTL lock on the strategy side, periodically renew it
        # CHeck it here
        
        if auto_unsub:
            try:
                await self._trade_session.unsubscribe_symbol(symbol_name)
            except TradeSessionRequestError as e:
                raise ControllerInputError(str(e))
                
        self.channel.in_streams.discard(in_stream_name)
        self.channel.last_ids.pop(in_stream_name, None)   

            
        if callback:
            return callback(in_stream_name)
        else:
            return
        
    async def bootstrap_in_stream(
            self, in_stream_name: str,
            callback: Optional[Callable[[Any], None]] = None
        ) -> None:
        # Re-subscribe an in_stream that's already registered on the channel
        # (e.g. reloaded from config on restart) — skips the channel-membership
        # check and re-registration that add_in_stream does.
        if len(in_stream_name.split(":")) != 2:
            raise ControllerInputError("Incorrect format for stream_name input.")

        sub_scope = in_stream_name.split(":")[0]
        symbol_name = in_stream_name.split(":")[1]

        if not self.SCOPE_MAP.get(sub_scope):
            raise ControllerInputError(f"{sub_scope} is an invalid sub_scope.")

        if not self._ptrc.has_symbol(symbol_name):
            raise ControllerInputError(
                f"symbol_name: {symbol_name} is not in pre-trade risk check."
                )

        await self._subscribe_and_resolve(sub_scope, symbol_name)

        if callback:
            callback(in_stream_name)
            

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
        self.num_logon_trial: int = 10
        self.num_logoff_trial: int = 10
        
        # ---- Risk checks ----
        self.PREC: PreTradeRiskCheck = PreTradeRiskCheck('cqg')
        self.PREC.load(pretraderisk_cfg_addr)
        
        # ---- Engine property ----
        self._stop_evt: asyncio.Event = asyncio.Event()
        self._freeze_evt: asyncio.Event = asyncio.Event()
        
        # ---- Engine Containers ----
        self._send_order_tasks: dict[str, asyncio.Task] = dict()
        
        # ---- Channel and Control----
        self.CTRL_STREAM: str = "CMD:Trade_Engine"
        self.controller: Controller = TradeEngineController(
            self.trade_session, self.channel, self.PREC
            )
        self._control_task: Optional[asyncio.Task] = None
        # ---- State Control ----
        self._state_mgr = StateMgr(
            ENGINESTATE_LIFECYCLE,
            start=EngineState.READY,
            cur=EngineState.READY,
            allowed_starts=[EngineState.READY],
        )

    @property
    def state(self):
        return self._state_mgr.cur
                    
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
            try:
                await self._package_and_send(order_type, order_info)
            except RiskViolationError as e:
                logger.error("[Trade Engine] %s", e)
            
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
                    case "CMD:add_stream":
                        await self.controller.add_in_stream(
                            cmd[1], callback = self._add_send_task_to_map
                            )
                    case "CMD:remove_stream":
                        task = await self.controller.remove_in_stream(
                            cmd[1], callback = self._remove_task)
                        try:
                            await task          # await ONLY here — to let CancelledError settle
                        except asyncio.CancelledError:
                            pass
                    #!!! Freeze Engine
                    #!!! Unfreeze Engine
                    #!!! Cancel_All_request
                    #!!! goflat_request
                    # start engine
                    # shutdown engine
                    case _:
                        logger.warning("[Trade Engine] Unknown Command: %s", cmd[0])
            except ControllerInputError as e:
                logger.error("[Trade Engine] Control_loop error: %s", e)
            except(ChannelMissingSettingError, ChannelListenError) as e:
                logger.error("[Trade Engine] %s", e)
            
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("[Trade Engine] Control_loop error: %s", e, exc_info=True)

    # Add Pause button and state backup for resume
    def _freeze(self):...
    def _unfreeze(self):...

    # -------- Engine LifeCycle
    async def _setup(self) -> bool:
        try: # Connect to channel
            await self.channel.connect()
            
            # start control loop
            self._control_task = asyncio.create_task(self._control_loop())
                
            # start trade session
            trade_session_start = await self.trade_session.start()
            if not trade_session_start:
                logger.warning("[Trade Engine]: Failed to launch Trade Session.")
                return False
            
            for trial in range(self.num_logon_trial):
                logon_res = await self.trade_session._conn.logon(
                    client_app_id = "WebApiTest",
                    client_version = "python-client-test-2-240",
                    protocol_version_major = 2,
                    protocol_version_minor = 240,
                    drop_concurrent_session = False,
                    private_label = PRIVATE_LABEL,
                    )
                logger.info(f"[Trade Engine]: Logon attempt {trial} result: {logon_res.get('result_code')}.")
        
                if self.trade_session.state == ConnectionState.CONNECTED_LOGON:
                    return True
            return False
        except (ConnectRequestError, ConnectTimeOutError) as e:
            logger.warning("[Trade Engine]: %s", e)
            return False

                
    async def start(self) -> bool:
        try:
            setup_is_done = await self._setup()
            if setup_is_done:
                self._state_mgr.transition_to(EngineState.RUNNING)
            else:
                self._state_mgr.transition_to(EngineState.TERMINATED)
                
        except (ChannelMissingSettingError, 
                RecorderCriticalError) as e:
            logger.warning("[Trade Engine]: %s", e)
            return False
        
        try:
            # subscribe all the trade subscriptions and pre-resolve symbols
            for stream_name in self.channel.in_streams:
                await self.controller.bootstrap_in_stream(
                    stream_name, callback=self._add_send_task_to_map
                    )
        except ControllerInputError as e:
            logger.error("[Trade Engine]: %s", e)
            self._state_mgr.transition_to(EngineState.TERMINATED)
            return False
        return True

    async def stop(self) -> bool:
        if self._stop_evt.is_set():
            return 
        self._stop_evt.set()

        try: # logoff
            for trial in range(self.num_logoff_trial):
                logoff_res = await self.trade_session._conn.logoff()
                logger.info(
                    f"[Trade Engine]: Logoff attempt {trial} reason: {logoff_res.get('logoff_reason')}."
                    )
                if self.trade_session.state == ConnectionState.CONNECTED_LOGOFF:
                    break
                
        except (ConnectRequestError, ConnectTimeOutError) as e:
            logger.warning("[Trade Engine]: %s", e)
            return False

        is_stopped = await self.trade_session.stop()
        if not is_stopped:
            logger.error("[Trade Engine] Trade Session is not stopped.")
            return False

        try: # stream cleanup
            for stream_name in list(self.channel.in_streams):
                await self.controller.remove_in_stream(
                    stream_name, callback=self._remove_task,
                    auto_unsub = False
                    )
        except ControllerInputError as e:
            logger.warning("[Trade Engine]: %s", e)
            return False

        # End control loop
        if self._control_task is not None:
            self._control_task.cancel()
            try:
                await self._control_task
            except asyncio.CancelledError:
                pass
            
        try:
            await self.channel.disconnect()
        except ChannelMissingSettingError as e:
            logger.warning("[Trade Engine]: %s", e)
            return False

        self._state_mgr.transition_to(EngineState.TERMINATED)
        return True
        
    # --- Engine request methods ----
    async def request_freeze(self) -> None:
        self._freeze_evt.set()
        
    async def request_unfreeze(self) -> None:
        self._freeze_evt.set()
        
    async def request_stop(self) -> None:
        await self.stop()  
        
    # --- main ---  
    async def run(self):
        try:
            is_started = await self.start()
            
            if not is_started:
                logger.error("[Trade Engine] Engine Start Fail.")
                return

            await self._stop_evt.wait()
            
        finally:
            await self.stop()
