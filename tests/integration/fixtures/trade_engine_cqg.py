import asyncio
import logging
import tomllib
from datetime import datetime, timezone
from typing import Protocol, Optional, Callable, Any

from EC_API.channel.base import Channel
from EC_API.channel.redis import RedisChannel
from EC_API.connect.base import Connect
from EC_API.connect.enums import ConnectionState
from EC_API.connect.session_util import check_if_logoffed
from EC_API.connect.cqg.base import ConnectCQG
from EC_API.ordering.trade_session import TradeSession
from EC_API.ordering.cqg.trade_session import TradeSessionCQG
from EC_API.ordering.cqg.live_order import LiveOrderCQG
from EC_API.ordering.enums import RequestType, SubScope
from EC_API.payload.base import Payload, ExecutePayload
from EC_API.payload.safety import PreTradeRiskCheck
from EC_API.recorders.base import SQLSchemaTable, Recorder
from EC_API.recorders.sqlite_recorder import SQLiteRecorder
from EC_API.protocol.cqg.recorder_util import (
    order_status_to_row_default,
    position_status_to_row_default,
    account_summary_to_row_default,
    ORD_STS_COLS, POS_STS_COLS, ACC_SUMM_COLS
    )
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

logger = logging.getLogger(__name__)

class Controller(Protocol):
    async def add_in_stream(
            self, in_stream_name: str,
            callback: Optional[Callable[[Any], None]] = None,
            auto_sub: bool = True
        ) -> None: ...

    async def remove_in_stream(
            self,
            in_stream_name: str,
            callback: Optional[Callable[[Any], Any]] = None,
            auto_unsub: bool = True
        ) -> Optional[Any]: ...

    async def bootstrap_in_stream(
            self, in_stream_name: str,
            callback: Optional[Callable[[Any], None]] = None
        ) -> None: ...

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
        if self._channel.in_streams is None:
            raise ControllerInputError("Channel's 'in_streams' is not configured.")
            
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
        if self._channel.in_streams is None:
            raise ControllerInputError("Channel's 'in_streams' is not configured.")
        if self._channel.last_ids is None:
            raise ControllerInputError("Channel's 'last_ids' is not configured.")
    
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
        # Check it here
        
        if auto_unsub:
            try:
                await self._trade_session.unsubscribe_symbol(symbol_name)
            except TradeSessionRequestError as e:
                raise ControllerInputError(str(e))
                
        self._channel.in_streams.discard(in_stream_name)
        self._channel.last_ids.pop(in_stream_name, None)   
            
        if callback:
            return callback(in_stream_name)
        else:
            return None
        
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
            usr_cfg: str,
            channel_cfg_addr: str, 
            pretraderisk_cfg_addr:str
        ):
        # ---- user inputs setting ----
        with open(usr_cfg, mode="rb") as f:
            usr_para = tomllib.load(f)
        
        if usr_para.get('credentials') is None:
            logger.warning("'credentials' is missing in the user config.")
            return
        
        if usr_para.get('trade_recorders') is None:
            logger.warning("'trade_recorders' is missing in the user config.")
            return 
    
        self.HOST_NAME: str = usr_para['credentials'].get("HOST_NAME","")
        self.USR_NAME: str = usr_para['credentials'].get("USR_NAME","")
        self.PASSWORD: str = usr_para['credentials'].get("PASSWORD","")
        self.ACCOUNT_ID: int = usr_para['credentials'].get("ACCOUNT_ID","")
        self.PRIVATE_LABEL: str = usr_para['credentials'].get("PRIVATE_LABEL","")
        self.TRADE_LOG_DB_ADDR: str = usr_para["trade_recorders"].get("TRADE_LOG_DB_ADDR", "")
        self.ord_sts_table_name: str = usr_para["trade_recorders"].get("ord_sts_table_name", "")
        self.pos_sts_table_name: str = usr_para["trade_recorders"].get("pos_sts_table_name", "")
        self.acc_summ_table_name: str = usr_para["trade_recorders"].get("acc_summ_table_name","")
        
        # ---- IPC Channel setting ----
        self.channel: Channel = RedisChannel(channel_cfg_addr)
        self.ord_sts_recorder: Recorder = SQLiteRecorder(
            schema = SQLSchemaTable(
                table_name = self.ord_sts_table_name, 
                columns = ORD_STS_COLS
                ), 
            db_address = self.TRADE_LOG_DB_ADDR,
            to_row = order_status_to_row_default
            )
        self.pos_sts_recorder: Recorder = SQLiteRecorder(
            schema = SQLSchemaTable(
                table_name = self.pos_sts_table_name, 
                columns = POS_STS_COLS 
                ), 
            db_address = self.TRADE_LOG_DB_ADDR,
            to_row = position_status_to_row_default
            )
        self.acc_summ_recorder: Recorder = SQLiteRecorder(
            schema = SQLSchemaTable(
                table_name = self.acc_summ_table_name, 
                columns = ACC_SUMM_COLS
                ), 
            db_address = self.TRADE_LOG_DB_ADDR,
            to_row = account_summary_to_row_default
            ) 
            
        # ---- Sessions setting ----
        self.conn: Connect = ConnectCQG(
            self.HOST_NAME, 
            self.USR_NAME, 
            self.PASSWORD, 
            self.ACCOUNT_ID
            )
        self.trade_session: TradeSessionCQG = TradeSessionCQG(
            self.conn,
            ord_sts_recorder=self.ord_sts_recorder,
            pos_sts_recorder=self.pos_sts_recorder,
            acc_summ_recorder=self.acc_summ_recorder
            )
        self.num_logon_trial: int = 10
        self.num_logoff_trial: int = 10
        
        # ---- Risk checks ----
        self.PREC: PreTradeRiskCheck = PreTradeRiskCheck('cqg')
        self.PREC.load(pretraderisk_cfg_addr)
        
        # ---- Engine property ----
        self._stop_evt: asyncio.Event = asyncio.Event()
        self._freeze_evt: asyncio.Event = asyncio.Event()
        self._notify_evt: asyncio.Event = asyncio.Event() # to wake up the main loop
        
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
        #self.is_started: bool = False
        #self.is_stopped: bool = False

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

    def _remove_task(self, in_stream_name: str) -> Optional[asyncio.Task]:
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
            
            if self.state is not EngineState.RUNNING:
                continue
            
            # package and send (fire and forget)
            order_type, order_info = msg
            try:
                await self._package_and_send(order_type, order_info)
            except RiskViolationError as e:
                logger.error("[Trade Engine] %s", e)
            
    # ------- Controls
    async def command_response(self, cmd: tuple[Any, ...]) -> None:
        match cmd[0]: # ("CMD:add_stream", "order_info:WTI")
            case "CMD:add_stream":
                await self.controller.add_in_stream(
                    cmd[1], callback = self._add_send_task_to_map
                    )
            case "CMD:remove_stream":
                task = await self.controller.remove_in_stream(
                    cmd[1], callback = self._remove_task)
                if task is not None: 
                    try:
                        await task          # await ONLY here — to let CancelledError settle
                    except asyncio.CancelledError:
                        pass
                
            case "CMD:freeze_engine":
                await self.request_freeze()
            case "CMD:unfreeze_engine":
                await self.request_wake()
            case "CMD:shutdown_engine":
                await self.request_stop()
            case "CMD:cancel_all_request":
                await LiveOrderCQG(self.trade_session).send(
                    request_type = RequestType.CANCELALL_ORDER, 
                    request_details = {
                        "cl_order_id": "cancel_all_req",
                        "when_utc_timestamp": datetime.now(timezone.utc),
                        })
            case "CMD:goflat_request":
                await LiveOrderCQG(self.trade_session).send(
                    request_type = RequestType.GOFLAT_ORDER, 
                    request_details = {
                        "when_utc_timestamp": datetime.now(timezone.utc),
                        })
            case _:
                logger.warning("[Trade Engine] Unknown Command: %s", cmd[0])
                        
    async def _control_loop(self):
        while not self._stop_evt.is_set():
            try:
                # Listen to control command and add/remove in_stream
                cmd = await self.channel.listen(stream_name=self.CTRL_STREAM, data_name="data")
                if cmd is None:
                    continue
                
                await self.command_response(cmd)
                
            except ControllerInputError as e:
                logger.error("[Trade Engine] Control_loop error: %s", e)
            except(ChannelMissingSettingError, ChannelListenError) as e:
                logger.error("[Trade Engine] %s", e)
            
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("[Trade Engine] Control_loop error: %s", e, exc_info=True)

    async def _freeze(self) -> None:
        # During a freeze. order_info can come in but will be discarded so there
        # is no exection        
        self._state_mgr.transition_to(EngineState.FROZEN)
        return
        
    async def _unfreeze(self) -> None:
        self._state_mgr.transition_to(EngineState.RUNNING)
        return 
    
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
                await self.stop()
                return False
            
            for trial in range(self.num_logon_trial):
                logon_res = await self.trade_session._conn.logon(
                    client_app_id = "WebApiTest",
                    client_version = "python-client-test-2-240",
                    protocol_version_major = 2,
                    protocol_version_minor = 240,
                    drop_concurrent_session = False,
                    private_label = self.PRIVATE_LABEL,
                    )
                if logon_res is not None:
                    logger.info(f"[Trade Engine]: Logon attempt {trial} result: {logon_res.get('result_code')}.")
        
                if self.trade_session.state == ConnectionState.CONNECTED_LOGON:
                    return True
            return False
        except (ConnectRequestError, ConnectTimeOutError) as e:
            logger.warning("[Trade Engine]: %s", e)
            return False

                
    async def start(self) -> bool:
        if self.state is not EngineState.READY:
            logger.warning("[Trade Engine]: start() called from state %s.", self.state)
            return False
        
        try:
            setup_is_done = await self._setup()
            if setup_is_done:
                self._state_mgr.transition_to(EngineState.RUNNING)
            else:
                self._state_mgr.transition_to(EngineState.TERMINATED)
                await self.stop()
                return False
            
        except (ChannelMissingSettingError, 
                RecorderCriticalError) as e:
            logger.warning("[Trade Engine]: %s", e)
            await self.stop()
            return False
        
        try:
            if self.channel.in_streams is None:
                raise ControllerInputError("Channel's 'in_streams' is not configured.")
            # subscribe all the trade subscriptions and pre-resolve symbols
            for stream_name in self.channel.in_streams:
                await self.controller.bootstrap_in_stream(
                    stream_name, callback=self._add_send_task_to_map
                    )
        except ControllerInputError as e:
            logger.error("[Trade Engine]: %s", e)
            self._state_mgr.transition_to(EngineState.TERMINATED)
            await self.stop()
            return False
        return True

    async def stop(self) -> bool:
        if self._stop_evt.is_set():
            return False
        self._stop_evt.set()
        
        logoff_done = True
        if self.trade_session.state == ConnectionState.CONNECTED_LOGON:
            try: # logoff                    
                for trial in range(self.num_logoff_trial):
                    logoff_res = await self.trade_session._conn.logoff()
                    if logoff_res is not None:
                        logger.info(
                            f"[Trade Engine]: Logoff attempt {trial} reason: {logoff_res.get('logoff_reason')}."
                            )
                    if self.trade_session.state == ConnectionState.CONNECTED_LOGOFF:
                        break
                
            except (ConnectRequestError, ConnectTimeOutError) as e:
                logger.warning("[Trade Engine]: %s", e)
                logoff_done = False
        elif not check_if_logoffed(trans_log = self.trade_session._conn._state_mgr.trans_log):
            logger.warning("[Trade Engine]: Trade Session not in a state to be logoff.")
            logoff_done = False
            
        # --- stop trade_session
        trade_session_stopped = await self.trade_session.stop()
        if not trade_session_stopped:
            logger.error("[Trade Engine] Trade Session is not stopped.")

        stream_cleanup_done = True
        try: # stream cleanup
            if self.channel.in_streams is None:
                raise ControllerInputError("Channel's 'in_streams' is not configured.")
            for stream_name in list(self.channel.in_streams):
                await self.controller.remove_in_stream(
                    stream_name, callback=self._remove_task,
                    auto_unsub = False
                    )
        except ControllerInputError as e:
            logger.warning("[Trade Engine]: %s", e)
            stream_cleanup_done = False

        # --- End control loop
        if self._control_task is not None:
            self._control_task.cancel()
            try:
                await self._control_task
            except asyncio.CancelledError:
                pass
            
        channel_disconnected = True
        try: # --- close channel
            await self.channel.disconnect()
        except ChannelMissingSettingError as e:
            logger.warning("[Trade Engine]: %s", e)
            channel_disconnected = False

        self._state_mgr.transition_to(EngineState.TERMINATED)
        return (logoff_done and trade_session_stopped and \
                stream_cleanup_done and channel_disconnected)
        
    # --- Engine request methods ----
    async def request_freeze(self) -> None:
        self._freeze_evt.set()
        self._notify_evt.set()


    async def request_wake(self) -> None:
        self._freeze_evt.clear()
        self._notify_evt.set()
        
    async def request_stop(self) -> None:
        self._stop_evt.set()  
        self._notify_evt.set()

    # --- main ---  
    async def run(self) -> None:
        try:
            is_started = await self.start()
            if not is_started:
                logger.error("[Trade Engine] Engine failed to start.")
                return
            
            # Main loop
            while True:
                await self._notify_evt.wait()
                self._notify_evt.clear()
                
                if self._stop_evt.is_set():
                    break
                                    
                if self._freeze_evt.is_set() and self.state is EngineState.RUNNING:
                    await self._freeze()
                elif not self._freeze_evt.is_set() and self.state is EngineState.FROZEN:
                    await self._unfreeze()
        finally:
            await self.stop()
             
