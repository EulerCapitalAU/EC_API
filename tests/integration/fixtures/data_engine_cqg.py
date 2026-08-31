#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jul  3 21:08:53 2026

@author: dexter
"""
import asyncio
import logging
from typing import Protocol, Optional, Callable, Any

from EC_API.channel.base import Channel
from EC_API.channel.redis import RedisChannel
from EC_API.connect.base import Connect
from EC_API.connect.enums import ConnectionState
from EC_API.connect.cqg.base import ConnectCQG
from EC_API.monitor.base import Monitor
from EC_API.monitor.cqg.realtime_data import MonitorDataCQG
from EC_API.monitor.enums import MktDataSubLevel

from EC_API.utility.state_mgr import StateMgr
from EC_API.exceptions import (
    ChannelBroadcastError,
    ControllerInputError,
    ChannelMissingSettingError,
    ConnectRequestError, 
    ConnectTimeOutError
    )
from tests.integration.fixtures.engine_enums import (
    EngineState, ENGINESTATE_LIFECYCLE
    )

logger = logging.getLogger(__name__)

HOST_NAME, USR_NAME, PASSWORD, ACCOUNT_ID = 0,0,0,0
PRIVATE_LABEL = 0

class Controller:...
class DataEngineController(Controller):
    def __init__(
            self, 
            monitor: Monitor, 
            channel: Channel 
        ):
        self._monitor = monitor
        self._channel = channel
        self.SCOPE_MAP = {"trade": MktDataSubLevel.LEVEL_TRADES}

    async def add_in_stream(
            self, in_stream_name: str,
            callback: Optional[Callable[[Any], None]] = None
        ) -> None:...
    
    async def remove_in_stream(
            self, 
            in_stream_name: str,
            callback: Optional[Callable[[Any], Any]] = None
        ) -> Optional[Any]:...
    async def bootstrap_in_stream():...
    
class DataEngineCQG:
    def __init__(self, channel_cfg_addr: str):
        # ---- IPC Channel setting ----
        self.channel: Channel = RedisChannel(channel_cfg_addr)

        # ---- Sessions setting ----
        self.conn: Connect = ConnectCQG(HOST_NAME, USR_NAME, PASSWORD, ACCOUNT_ID)
        self.monitor: Monitor = MonitorDataCQG(self.conn)
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
            self.stream_and_post(out_stream_name)
        )
        
    def _remove_data_stream_task(self):...
    
    
    # ------- Controls
    def _control_loop(self):...
        

    # -------- Engine LifeCycle
    async def _setup(self) -> bool:
        try: # Connect to channel
            await self.channel.connect()
            
            # start control loop
            self._control_task = asyncio.create_task(self._control_loop())
                
            # start monitor
            monitor_start = await self.monitor.start()
            if not monitor_start:
                logger.warning("[Data Engine]: Failed to launch Trade Session.")
                return False
            
            for trial in range(self.num_logon_trial):
                logon_res = await self.monitor._conn.logon(
                    client_app_id = "WebApiTest",
                    client_version = "python-client-test-2-240",
                    protocol_version_major = 2,
                    protocol_version_minor = 240,
                    drop_concurrent_session = False,
                    private_label = PRIVATE_LABEL,
                    )
                logger.info(f"[Data Engine]: Logon attempt {trial} result: {logon_res.get('result_code')}.")
        
                if self.monitor.state == ConnectionState.CONNECTED_LOGON:
                    return True
            return False
        except (ConnectRequestError, ConnectTimeOutError) as e:
            logger.warning("[Data Engine]: %s", e)
            return False

    async def start(self) -> bool:
        try:
            setup_is_done = await self._setup()
            if setup_is_done:
                self._state_mgr.transition_to(EngineState.RUNNING)
            else:
                self._state_mgr.transition_to(EngineState.TERMINATED)
                
        except (ChannelMissingSettingError) as e:
            logger.warning("[Data Engine]: %s", e)
            return False
        
        try:
            # subscribe all the trade subscriptions and pre-resolve symbols
            for stream_name in self.channel.in_streams:
                await self.controller.bootstrap_in_stream(
                    stream_name, callback=self._add_new_data_stream_task
                    )
        except ControllerInputError as e:
            logger.error("[Data Engine]: %s", e)
            self._state_mgr.transition_to(EngineState.TERMINATED)
            return False
        return True
    
    async def stop(self) -> bool:
        if self._stop_evt.is_set():
            return False
        self._stop_evt.set()

        try: # logoff
            for trial in range(self.num_logoff_trial):
                logoff_res = await self.monitor._conn.logoff()
                logger.info(
                    f"[Data Engine]: Logoff attempt {trial} reason: {logoff_res.get('logoff_reason')}."
                    )
                if self.trade_session.state == ConnectionState.CONNECTED_LOGOFF:
                    break
                
        except (ConnectRequestError, ConnectTimeOutError) as e:
            logger.warning("[Data Engine]: %s", e)
            return False

        is_stopped = await self.monitor.stop()
        if not is_stopped:
            logger.error("[Data Engine] Trade Session is not stopped.")
            return False

        try: # stream cleanup
            for stream_name in list(self.channel.in_streams):
                await self.controller.remove_in_stream(
                    stream_name, callback=self._remove_task,
                    auto_unsub = False
                    )
        except ControllerInputError as e:
            logger.warning("[Data Engine]: %s", e)
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
            logger.warning("[Data Engine]: %s", e)
            return False

        self._state_mgr.transition_to(EngineState.TERMINATED)
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
                logger.error("[Data Engine] Engine Start Fail.")
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
    