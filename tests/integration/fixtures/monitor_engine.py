import logging
import asyncio
from typing import Protocol, Optional, Any
from EC_API.channel.base import Channel
from EC_API.channel.redis import RedisChannel
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
        self._stop_evt: asyncio.Event = asyncio.Event()
        self._freeze_evt: asyncio.Event = asyncio.Event()
        self._notify_evt: asyncio.Event = asyncio.Event() # to wake up the main loop
        
        # ---- Engine Containers ----
        self.datafeeds: dict[str: DataFeed] = dict() # {}
        self.engine_health: dict[str, dict] = dict() # {"TradeEngine": {}}
        self._tasks: dict = dict()
        
        # ---- Channel and Control----
        self.CTRL_STREAM: str = "CMD:Montior_Engine"
        self.controller: Controller = MonitorEngineController()
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

    def add_dataFeed(self): ...
        # upon receiving a in_stream, we also activate add datafeed
        
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
        match cmd[0]: # ("CMD:add_stream", "order_info:WTI")
            case _:
                logger.warning("[Montior Engine] Unknown Command: %s", cmd[0])

    async def _control_loop(self):
        while not self._stop_evt.is_set():
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
    async def _setup(self) -> bool:...
    
    async def start(self) -> bool:
        if self.state is not EngineState.READY:
            logger.warning("[monitor Engine]: start() called from state %s.", self.state)
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
            logger.warning("[Monitor Engine]: %s", e)
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
            logger.error("[Monoitr Engine]: %s", e)
            self._state_mgr.transition_to(EngineState.TERMINATED)
            await self.stop()
            return False
        return True
    
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
                logger.error("[Monitor Engine] Engine failed to start.")
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
