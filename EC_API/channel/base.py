#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 13 19:44:46 2026

@author: dexter
"""

from typing import Protocol, Callable, Any, Optional

class Channel(Protocol):
    #on_data: Callable[[Any], None]  # wired to DataBus.push at engine construction
    in_streams: Optional[set[str]]
    last_ids: Optional[dict[str, str]]

    async def connect(self) -> None: ...
    async def disconnect(self) -> None: ...

    async def broadcast(self, parsed_msg: tuple, stream_name: str, data_name: str ='data') -> None: ...
    async def listen(self, stream_name: str, data_name: str = 'data') -> tuple | None: ...