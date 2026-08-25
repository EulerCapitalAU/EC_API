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
from EC_API.connect.cqg.base import ConnectCQG
from EC_API.monitor.base import Monitor
from EC_API.monitor.cqg.realtime_data import MonitorDataCQG

from EC_API.utility.state_mgr import StateMgr
from tests.integration.fixtures.engine_enums import (
    EngineState, ENGINESTATE_LIFECYCLE
    )


HOST_NAME, USR_NAME, PASSWORD, ACCOUNT_ID = 0,0,0,0

class Controller:...
class DataEngineController(Controller):
    ...
    async def add_in_stream(
            self, in_stream_name: str,
            callback: Optional[Callable[[Any], None]] = None
        ) -> None:...
    
    async def remove_in_stream(
            self, 
            in_stream_name: str,
            callback: Optional[Callable[[Any], Any]] = None
        ) -> Optional[Any]:...

    
class DataEngineCQG:
    def __init__(self, channel_cfg_addr: str):
        # ---- IPC Channel setting ----
        self.channel: Channel = RedisChannel(channel_cfg_addr)

        # ---- Sessions setting ----
        self.conn: Connect = ConnectCQG(HOST_NAME, USR_NAME, PASSWORD, ACCOUNT_ID)
        self.monitor: Monitor = MonitorDataCQG(self.conn)
        
        # ---- Engine property ----
        self._stop_evt: asyncio.Event = asyncio.Event()
        
        # ---- Engine Containers ----
        self._streaming_tasks: dict[str, asyncio.Task] = dict()

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
    def _add_new_data_stream_task(self):...
    def _remove_data_stream_task(self):...
    
    
    # ------- Controls
    def _control_loop(self):...
    # -------- Engine LifeCycle
    def request_stop(self) -> None:
        self._stop_evt.set()


    