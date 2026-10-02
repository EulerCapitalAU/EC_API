#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jul  3 21:08:53 2026

@author: dexter
"""
import tomllib
import asyncio
import logging
from typing import Protocol, Optional, Callable, Any

from EC_API.channel.base import Channel
from EC_API.channel.redis import RedisChannel
from EC_API.connect.base import Connect
from EC_API.connect.enums import ConnectionState
from EC_API.connect.session_util import check_if_logoffed
from EC_API.connect.cqg.base import ConnectCQG
from EC_API.monitor.base import Monitor
from EC_API.monitor.cqg.realtime_data import MonitorDataCQG
from EC_API.monitor.enums import MktDataSubLevel
from EC_API.utility.state_mgr import StateMgr
from EC_API.exceptions import (
    ChannelBroadcastError,
    ChannelListenError,
    ControllerInputError,
    ChannelMissingSettingError,
    ConnectRequestError, 
    ConnectTimeOutError,
    ConnectEnterError
    )
from tests.integration.fixtures.engine_enums import (
    EngineState, ENGINESTATE_LIFECYCLE
    )

logger = logging.getLogger(__name__)

class Controller(Protocol):
    SCOPE_MAP: dict[str, MktDataSubLevel]        
    async def add_out_stream(
            self, in_stream_name: str,
            callback: Optional[Callable[[Any], None]] = None,
        ) -> None: ...

    async def remove_out_stream(
            self,
            in_stream_name: str,
            callback: Optional[Callable[[Any], Any]] = None,
        ) -> Optional[Any]: ...


class DataEngineController(Controller):
    def __init__(
            self, 
            monitor: Monitor, 
            channel: Channel 
        ):
        self._monitor = monitor
        self._channel = channel
        self.SCOPE_MAP = {"mkt_data": MktDataSubLevel.LEVEL_TRADES}
            
    async def add_out_stream(
            self, out_stream_name: str,
            callback: Optional[Callable[[Any], None]] = None
        ) -> None:
        if self._channel.out_streams is None:
            raise ControllerInputError("Channel's 'out_streams' is not configured.")

        if out_stream_name in self._channel.out_streams:
            raise ControllerInputError(
                f"stream_name: {out_stream_name} is already in the channel."
                )

        # Format Check
        if len(out_stream_name.split(":")) !=2:
            raise ControllerInputError("Incorrect format for stream_name input.")
            
        sub_scope = out_stream_name.split(":")[0] 
        symbol_name = out_stream_name.split(":")[1]
        
        # Add in_stream, add symbols
        self._channel.out_streams.add(out_stream_name)

        if callback:
            callback(out_stream_name)

    async def remove_out_stream(
            self, 
            out_stream_name: str,
            callback: Optional[Callable[[Any], Any]] = None
        ) -> Optional[Any]:
        if self._channel.out_streams is None:
            raise ControllerInputError("Channel's 'out_streams' is not configured.")
        if self._channel.last_ids is None:
            raise ControllerInputError("Channel's 'last_ids' is not configured.")
    
        if out_stream_name not in self._channel.out_streams:
            raise ControllerInputError(
                f"stream_name: {out_stream_name} is not in the channel."
                )
            
        if len(out_stream_name.split(":")) !=2:
            raise ControllerInputError("Incorrect format for stream_name input.")
            
        self._channel.out_streams.discard(out_stream_name)
        self._channel.last_ids.pop(out_stream_name, None)   

        if callback:
            return callback(out_stream_name)
        else:
            return None
   
class DataEngineCQG:
    def __init__(self,             
            usr_cfg: str,
            channel_cfg_addr: str
            ):
        # ---- user inputs setting ----
        with open(usr_cfg, mode="rb") as f:
            usr_para = tomllib.load(f)
        
        if usr_para.get('credentials') is None:
            logger.warning("'credentials' is missing in the user config.")
            return
        
        self.HOST_NAME: str = usr_para['credentials'].get("HOST_NAME","")
        self.USR_NAME: str = usr_para['credentials'].get("USR_NAME","")
        self.PASSWORD: str = usr_para['credentials'].get("PASSWORD","")
        self.ACCOUNT_ID: int = usr_para['credentials'].get("ACCOUNT_ID","")
        self.PRIVATE_LABEL: str = usr_para['credentials'].get("PRIVATE_LABEL","")

        
        # ---- IPC Channel setting ----
        self.channel: Channel = RedisChannel(channel_cfg_addr)

        # ---- Sessions setting ----
        self.conn: Connect = ConnectCQG(
            self.HOST_NAME, 
            self.USR_NAME, 
            self.PASSWORD, 
            self.ACCOUNT_ID
            )
        self.monitor: MonitorDataCQG = MonitorDataCQG(self.conn)
        self.num_logon_trial: int = 10
        self.num_logoff_trial: int = 10

        # ---- Engine property ----
        self._stop_evt: asyncio.Event = asyncio.Event()
        self._freeze_evt: asyncio.Event = asyncio.Event()
        self._notify_evt: asyncio.Event = asyncio.Event() # to wake up the main loop

        # ---- Engine Containers ----
        self._streaming_tasks: dict[str, asyncio.Task] = dict()
        self._missed_ticks: dict[str, int] = dict()
        
        # ---- Channel and Control----
        self.CTRL_STREAM: str = "CMD:Data_Engine"
        self.controller: Controller = DataEngineController(
            self.monitor, self.channel,
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
                    
    # ------- Engine functions (Monitor)
    async def stream_and_post(
        self, out_stream_name: str, symbol_name: str, level
        ) -> None:
        async for parsed_msg in self.monitor.stream(symbol_name, level):
            if self._stop_evt.is_set():
                break
            try:
                await self.channel.broadcast(parsed_msg, out_stream_name)
            except ChannelBroadcastError:
                pass # no logging, latency senstivie

    def _add_new_data_stream_task(self, out_stream_name: str) -> None:

        self._streaming_tasks[out_stream_name] = asyncio.create_task(
            self._initiate_ingestion_loop(out_stream_name)
        )
        
    def _remove_data_stream_task(self, out_stream_name: str) -> Optional[asyncio.Task]:
        task = self._streaming_tasks.pop(out_stream_name, None)
        if task is not None:
            task.cancel()
        return task
    
    async def _initiate_ingestion_loop(self, out_stream_name: str):
        sub_scope, symbol_name = out_stream_name.split(":")
        level = self.controller.SCOPE_MAP[sub_scope]
        while not self._stop_evt.is_set():
            await self.stream_and_post(out_stream_name, symbol_name, level)

    # ------- Controls
    async def command_response(self, cmd: tuple[Any, ...]) -> None:
        match cmd[0]: # ("CMD:add_stream", "mkt_data:WTI")
            case "CMD:add_stream":
                await self.controller.add_out_stream(
                    cmd[1], callback = self._add_new_data_stream_task
                    )
            case "CMD:remove_stream":
                task = await self.controller.remove_out_stream(
                    cmd[1], callback = self._remove_data_stream_task)
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
            case _:
                logger.warning("[Data Engine] Unknown Command: %s", cmd[0])

    async def _control_loop(self):
        while not self._stop_evt.is_set():
            try:
                # Listen to control command and add/remove out_stream
                cmd = await self.channel.listen(stream_name=self.CTRL_STREAM, data_name="data")
                if cmd is None:
                    continue
                
                await self.command_response(cmd)
                
            except ControllerInputError as e:
                logger.error("[Data Engine] Control_loop error: %s", e)
            except(ChannelMissingSettingError, ChannelListenError) as e:
                logger.error("[Data Engine] %s", e)
            
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("[Data Engine] Control_loop error: %s", e, exc_info=True)

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
                
            # start monitor
            await self.monitor.__aenter__()
            monitor_started = (self.monitor.state in (
                ConnectionState.CONNECTED_DEFAULT,
                ConnectionState.CONNECTED_LOGON,
                ConnectionState.CONNECTED_LOGOFF,
                ConnectionState.RECONNECTING
                ))
            if not monitor_started:
                logger.warning("[Data Engine]: Failed to launch Monitor.")
                await self.stop()
                return False
            
            for trial in range(self.num_logon_trial):
                logon_res = await self.monitor._conn.logon(
                    client_app_id = "WebApiTest",
                    client_version = "python-client-test-2-240",
                    protocol_version_major = 2,
                    protocol_version_minor = 240,
                    drop_concurrent_session = False,
                    private_label = self.PRIVATE_LABEL,
                    )
                if logon_res is not None:
                    logger.info(f"[Data Engine]: Logon attempt {trial} result: {logon_res.get('result_code')}.")
        
                if self.monitor.state == ConnectionState.CONNECTED_LOGON:
                    return True
            return False
        
        except (ConnectRequestError, ConnectTimeOutError) as e:
            logger.warning("[Data Engine]: %s", e)
            return False
        except ConnectEnterError as e:
            logger.warning("[Data Engine]: Failed to launch Monitor: %s", e)
            await self.stop()
            return False

    async def start(self) -> bool:
        try:
            setup_is_done = await self._setup()
            if setup_is_done:
                self._state_mgr.transition_to(EngineState.RUNNING)
            else:
                self._state_mgr.transition_to(EngineState.TERMINATED)
                await self.stop()
                return False

        except (ChannelMissingSettingError) as e:
            logger.warning("[Data Engine]: %s", e)
            await self.stop()
            return False
        
        return True
    
    async def stop(self) -> bool:
        if self._stop_evt.is_set():
            return False
        self._stop_evt.set()

        logoff_done = True
        if self.monitor.state == ConnectionState.CONNECTED_LOGON:
            try: # logoff
                for trial in range(self.num_logoff_trial):
                    logoff_res = await self.monitor._conn.logoff()
                    if logoff_res is not None:
                        logger.info(
                            f"[Data Engine]: Logoff attempt {trial} reason: {logoff_res.get('logoff_reason')}."
                            )
                    if self.monitor.state == ConnectionState.CONNECTED_LOGOFF:
                        break
                    
            except (ConnectRequestError, ConnectTimeOutError) as e:
                logger.warning("[Data Engine]: %s", e)
                logoff_done = False
        elif not check_if_logoffed(trans_log = self.monitor._conn._state_mgr.trans_log):
            logger.warning("[Data Engine]: Trade Session not in a state to be logoff.")
            logoff_done = False

        try:
            await self.monitor.__aexit__(None, None, None)
        except (ConnectRequestError, ConnectTimeOutError) as e:
            logger.warning("[Data Engine]: %s", e)
        monitor_stopped = self.monitor.state == ConnectionState.CLOSED
        if not monitor_stopped:
            logger.error("[Data Engine] Monitor is not stopped.")
            
        stream_cleanup_done = True
        try: # stream cleanup
            if self.channel.out_streams is None:
                raise ControllerInputError("Channel's 'out_streams' is not configured.")
            for stream_name in list(self.channel.out_streams):
                await self.controller.remove_out_stream(
                    stream_name, callback=self._remove_data_stream_task,
                    )
        except ControllerInputError as e:
            logger.warning("[Data Engine]: %s", e)
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
            logger.warning("[Data Engine]: %s", e)
            channel_disconnected = False

        self._state_mgr.transition_to(EngineState.TERMINATED)
        return (logoff_done and monitor_stopped and \
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
                logger.error("[Data Engine] Engine failed to start.")
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
    