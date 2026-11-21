import logging
import asyncio
from typing import Protocol, Optional, Any, Callable
from EC_API.channel.base import Channel
from EC_API.channel.redis import RedisChannel
from EC_API.common.tick_buffers_ext import (
    StatType, StatConfig,
    DataExtractionPolicy,
    SlidingWindowBuffer,
    Ringbuffer
    )
from EC_API.common.data_feeds import DataFeed
from EC_API.recorders.base import Recorder
from EC_API.recorders.sqlite_recorder import SQLiteRecorder
from EC_API.utility.state_mgr import StateMgr
from EC_API.exceptions import (
    ControllerInputError, 
    ChannelMissingSettingError, 
    ChannelListenError,
    RecorderCriticalError
)
from tests.integration.fixtures.engine_enums import (
    EngineState, ENGINESTATE_LIFECYCLE
    )

logger = logging.getLogger(__name__)

class Controller(Protocol):
    ...
class MonitorEngineController(Controller):
    def __init__(self):...
    async def add_out_stream(self):...
    async def remove_out_stream(self):...
    async def add_in_stream(self):...
    async def remove_in_stream(self):...

class MonitorEngine:
    """
    create data feed, routinely pull from the buffer, the statistics
    send those statistics to the visualisation media (Control panel)
    
    Collect engine health information and send them out
    
    """
    def __init__(self, channel_cfg_addr: str):
        # DataFeed Mechanism
        # recorder for storing mkt data
        # ---- IPC Channel setting ----
        self.channel: Channel = RedisChannel(channel_cfg_addr)

        self.in_stream = None # take from Data Engine
        self.out_stream = None # send out to either prometheus or control panel
        self.recorder: Optional[Recorder] = None

        # ---- Engine property ----
        self._terminate_evt:  asyncio.Event = asyncio.Event()
        self._start_evt: asyncio.Event = asyncio.Event()
        self._stop_evt: asyncio.Event = asyncio.Event()
        self._freeze_evt: asyncio.Event = asyncio.Event()
        self._notify_evt: asyncio.Event = asyncio.Event() # to wake up the main loop
        
        # ---- Engine Containers ----
        self.datafeeds: dict[str, DataFeed] = dict() # {}
        self.tradefeeds: dict[str, str] = dict()
        self.engine_health: dict[str, dict] = dict() # {"TradeEngine": {}}
        self._tasks: dict = dict()
        
        # Datafeed setting
        self.usr_df_stat_cfg: dict[str, bool] = {
            "cal_ohlcv": True,
            "cal_moments": False,
            "cal_vwap": False,
            "cal_median": False
            }
        self.buffer: Callable = SlidingWindowBuffer
        self.datafeed_stat_cfg = StatConfig(**self.usr_df_stat_cfg)

        # ---- Channel and Control----
        self.CTRL_STREAM: str = "CMD:MontiorEngine"
        self.controller: Controller = MonitorEngineController()
        self._control_task: Optional[asyncio.Task] = None
        
        # ---- State Control ----
        self._state_mgr = StateMgr(
            ENGINESTATE_LIFECYCLE,
            start=EngineState.SETUP,
            cur=EngineState.SETUP,
            allowed_starts=[EngineState.SETUP],
        )
        
    @property
    def state(self):
        return self._state_mgr.cur

    def add_dataFeed(self): 
        # upon receiving a in_stream, we also activate add datafeed
        bf = self.buffer(
            DataExtractionPolicy.ExtractTradeTickCQG,
            300, # 3 seconds
            0.01, #tick size threshold in OHLCV
            self.datafeed_stat_cfg
            )
    def remove_datafeed(self):...
    
    def _calculate_and_push_data(self):...
    
                    
    # ------- Engine functions (Monitor)
    
    # ------- statsnapshot broadcast
    def _send_stat_snapshots_loop(self):...
    
    def _add_send_stat_snapshots_task(self, in_stream_name: str)-> None:...
    
    # health info
    def _send_engine_health_info_loop(self):...

    def _add_send_health_info_task(self):...

    #---
    
    def _remove_task(self, in_stream_name: str) -> Optional[asyncio.Task]:
        task = self._send_order_tasks.pop(in_stream_name, None)
        if task is not None:
            task.cancel()
        return task

    # ------- Control
    async def command_response(self, cmd: tuple[Any, ...]) -> None:
        match cmd[0]: # ("CMD:add_datafeed", "mkt_data:trade_tick:WTI")
            case "CMD:start_engine":
                await self.request_start()
            case "CMD:stop_engine":
                await self.request_stop()
            case "CMD:freeze_engine":
                await self.request_freeze()
            case "CMD:unfreeze_engine":
                await self.request_wake()
            case "CMD:shutdown_engine":
                await self.request_shutdown()

            case _:
                logger.warning("[Montior Engine] Unknown Command: %s", cmd[0])

    async def _control_loop(self):
        # Take commands in the form of # ("CMD:add_stream", "mkt_data:WTI")
        # all data come from channel in_stream "CMD:MonitorEngineCQG"
        # (5) CMD:start_engine, ""
        # (6) CMD:stop_engine, ""
        # (7) CMD:freeze_engine, ""
        # (8) CMD:unfreeze_engine, ""
        # (9) CMD:shutdown_engine, ""
        while not self._terminate_evt.is_set():
            try:
                # Listen to control command and add/remove in_stream
                cmd = await self.channel.listen(stream_name=self.CTRL_STREAM, data_name="data")
                if cmd is None:
                    continue
                
                await self.command_response(cmd)
                
            except ControllerInputError as e:
                logger.error("[Monitor Engine] Control_loop error: %s", e)
            except(ChannelMissingSettingError, ChannelListenError) as e:
                logger.error("[Monitor Engine] %s", e)
            
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("[Monitor Engine] Control_loop error: %s", e, exc_info=True)

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
        # !!! Run some setup, maybe config management
        return True
    
    async def start(self) -> bool:
        if self.state is not EngineState.READY:
            logger.warning("[Monitor Engine]: start() called from state %s.", self.state)
            return False
        
        try:
            setup_is_done = await self._setup()
            if setup_is_done:
                self._state_mgr.transition_to(EngineState.RUNNING)
            else:
                await self.request_stop()
                return False
            
        except (ChannelMissingSettingError, 
                RecorderCriticalError) as e:
            logger.warning("[Monitor Engine]: %s", e)
            await self.request_stop()
            return False
        finally:
            self._start_evt.clear()

        return True
    
    async def stop(self) -> bool:
        # !!! see if we need tratement on stop
        self._stop_evt.clear()
        return True
            
    async def shutdown(self) ->bool:
        # For channel disconnect and control task clean up
        stream_cleanup_done = True
        try: # stream cleanup
            if self.channel.in_streams is None:
                raise ControllerInputError("Channel's 'in_streams' is not configured.")
            
            if self.channel.out_streams is None:
                raise ControllerInputError("Channel's 'out_streams' is not configured.")

            for stream_name in list(self.channel.in_streams):
                await self.controller.remove_in_stream(
                    stream_name, callback=self._remove_task,
                    auto_unsub = False
                    )
            for stream_name in list(self.channel.out_streams):
                await self.controller.remove_out_stream(
                    stream_name, callback=self._remove_task,
                    auto_unsub = False
                    )
                
        except ControllerInputError as e:
            logger.warning("[Monitor Engine]: %s", e)
            stream_cleanup_done = False

        # End control loop
        if self._control_task is not None:
            self._control_task.cancel()
            try:
                await self._control_task
            except asyncio.CancelledError:
                pass
            
        channel_disconnected = True
        try:
            await self.channel.disconnect()
        except ChannelMissingSettingError as e:
            logger.warning("[Monitor Engine]: %s", e)
            channel_disconnected = False

        #self._state_mgr.transition_to(EngineState.TERMINATED)
        return (stream_cleanup_done and channel_disconnected)

    # --- Engine request methods ----
    async def request_freeze(self) -> None:
        self._freeze_evt.set()
        self._notify_evt.set()

    async def request_wake(self) -> None:
        self._freeze_evt.clear()
        self._notify_evt.set()
        
    async def request_start(self) -> None:
        self._start_evt.set()
        self._notify_evt.set()
        
    async def request_stop(self) -> None:
        self._stop_evt.set()  
        self._notify_evt.set()
        
    async def request_shutdown(self) -> None:
        self._terminate_evt.set()
        self._notify_evt.set()

        
    # --- main ---  
    async def run(self) -> None:
        try:
            await self.channel.connect()
            
            self._control_task = asyncio.create_task(self._control_loop())
            
            self._state_mgr.transition_to(EngineState.READY)
            
            while not self._terminate_evt.is_set():
                # Main loop                    
                await self._notify_evt.wait()
                self._notify_evt.clear()

                # ---- Event handling
                if self._start_evt.is_set() and self.state is EngineState.READY: #
                    is_started = await self.start()
                    if not is_started:
                        logger.error("[Monitor Engine] Trade Session failed to start.")
                        
                if self._stop_evt.is_set() and self.state in (
                        EngineState.READY, EngineState.RUNNING, EngineState.FROZEN
                    ):
                    is_stopped = await self.stop()
                    if is_stopped:
                        self._state_mgr.transition_to(EngineState.READY)
                                    
                if self._freeze_evt.is_set() and self.state is EngineState.RUNNING:
                    await self._freeze()
                elif not self._freeze_evt.is_set() and self.state is EngineState.FROZEN:
                    await self._unfreeze()
                
        except ChannelMissingSettingError as e:
            logger.warning("[Monitor Engine]: %s", e)
            
        finally:
            is_shutdown = await self.shutdown()
            if is_shutdown:
                self._state_mgr.transition_to(EngineState.TERMINATED)

