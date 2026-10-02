#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 20 00:31:25 2026

@author: dexter
"""
from EC_API.channel.base import Channel

# Unix-Domain Socket Channel
class UDSChannel(Channel):
    def __init__(self):
        ...

    async def connect(self) -> None:
        raise NotImplementedError

    async def disconnect(self) -> None:
        raise NotImplementedError

    async def broadcast(self, parsed_msg: tuple, stream_name: str, data_name: str = 'data') -> None:
        raise NotImplementedError

    async def listen(self, stream_name: str, data_name: str = 'data') -> tuple | None:
        raise NotImplementedError