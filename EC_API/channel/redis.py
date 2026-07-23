#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 18 00:29:07 2026

@author: dexter
"""
import tomllib
from typing import Optional
import msgpack
import redis.asyncio as aioredis
from redis.asyncio.client import PubSub, Pipeline
from EC_API.channel.base import Channel
from EC_API.utility.error_handlers import toml_loader_error_handler
from EC_API.exceptions import (
    ChannelMissingSettingError,
    ChannelSubscriptionError,
    ChannelBroadcastError,
    ChannelListenError,
    ConfigInputError,
    ConfigFormatError
    )

class RedisChannel(Channel):
    def __init__(self, path: str = ""):        
        
        # --- global settings
        self.r: Optional[aioredis.Redis] = None
        self.pipeline: Optional[Pipeline] = None
        self._pubsub: Optional[PubSub] = None
                
        # --- Redis Stream settings
        self.host_name: Optional[dict[str, str]] = None

        self.out_streams: Optional[set[str]] = None
        self.in_streams: Optional[set[str]] = None
        
        self.maxlen_out_stream: int = 50_000
        self.xread_block: int = 5000
        # for redis stream message tracking
        self.last_ids: Optional[dict[str, str]] = None
        
        # We only allow one active listener to stream at a time
        self._active_listeners: set[str] = set()
        
        if path:
            self.load(path)

    def load(self, path: str) -> None:
        with toml_loader_error_handler(ConfigInputError, ConfigFormatError):
            with open(path, mode="rb") as f:
                para = tomllib.load(f)
        
                self.host_name = para.get("host_name", {})
                if not self.host_name:
                    raise KeyError("Field 'host_name' is missing in the toml file.")
                
                if not para.get("streams"):
                    raise KeyError("Field: 'streams' is missing in the toml file.")
                    
                self.in_streams = set(para["streams"].get("in_streams", []))
                self.out_streams = set(para["streams"].get("out_streams", []))
                
                if not (self.in_streams or self.out_streams):
                    raise KeyError("In field 'streams', at least one of these must be present: 'in_streams' and 'out_streams'.")
                
                self.last_ids = {name: '0' for name in self.in_streams}

    async def connect(self):
        if not self.host_name:
            raise ChannelMissingSettingError("Missing URL to start a redis server.")
            
        self.r = aioredis.Redis.from_url(self.host_name['URL'], decode_responses=False)
        
        self._pubsub = self.r.pubsub()
        self.pipeline = self.r.pipeline()
        
        for stream_name in self.in_streams:
            last = await self.r.xrevrange(stream_name, count=1)
            self.last_ids[stream_name] = last[0][0] if last else "0"


    async def disconnect(self):
        if not self.r:
            raise ChannelMissingSettingError("Redis server was not initiated.")
        assert self._pubsub is not None
        await self._pubsub.aclose()
        await self.r.aclose()
        self.r = None
        self._pubsub = None
        self.pipeline = None

    # --- Regular method. Redis default to stream
    async def broadcast(self, parsed_msg: tuple, stream_name:str, data_name: str='data') -> None:
        if not self.r:
            raise ChannelMissingSettingError("Redis server was not initiated.")

        try:
           await self.r.xadd(stream_name, {data_name: msgpack.packb(parsed_msg)},
                             maxlen=self.maxlen_out_stream, approximate=True)
        except Exception as e:
            raise ChannelBroadcastError(str(e))

    async def listen(self, stream_name: str, data_name: str='data') -> tuple | None:
        if not self.r:
            raise ChannelMissingSettingError("Redis server was not initiated.")

        if self.last_ids is None:
            raise ChannelMissingSettingError("Streams not configured.")

        if stream_name in self._active_listeners:
            raise ChannelListenError(f"{stream_name} already has an active listener")
        self._active_listeners.add(stream_name)

        try:
            #!!! Fix the design of count = 1 and [-1] read. I should change this
            # to allow batch read and loop through the batch to return to caller
            # [stream_name, [(id, fields_dict),...]]
            l = await self.r.xread(count=1, block=self.xread_block, streams = {stream_name: self.last_ids[stream_name]})
            if not l:
                return None
            self.last_ids[stream_name] = l[0][1][-1][0]       
            return tuple(msgpack.unpackb(l[0][1][-1][1][data_name.encode()], raw=False))
                    
        except Exception as e:
            raise ChannelListenError(str(e))
        
        finally:
            self._active_listeners.discard(stream_name)

    # --- Special method. For heartbeat or transient messages
    async def subscribe_pubsub(self, channel: str) -> None:   
        if not self._pubsub:
            raise ChannelMissingSettingError("Redis server was not initiated.")
        try:
            await self._pubsub.subscribe(channel)
        except Exception as e:
            raise ChannelSubscriptionError(str(e))

    async def unsubscribe_pubsub(self, channel: str) -> None:
        if not self._pubsub:
            raise ChannelMissingSettingError("Redis server was not initiated.")
            
        if channel.encode() not in self._pubsub.channels.keys():
            raise ChannelSubscriptionError(f"Not subscribed to '{channel}'")

        try:
            await self._pubsub.unsubscribe(channel.encode())
            #!!! add our own tracker
        except Exception as e:
            raise ChannelSubscriptionError(str(e))

    async def broadcast_pubsub(self, parsed_msg: tuple, stream_name: str, data_name: str='data') -> None:
        if not self.r:
            raise ChannelMissingSettingError("Redis server was not initiated.")

        try:
            await self.r.publish(stream_name, msgpack.packb(parsed_msg))
        except Exception as e:
            raise ChannelBroadcastError(str(e))
    
    async def listen_pubsub(self):
        if not self._pubsub:
            raise ChannelMissingSettingError("Redis server was not initiated.")

        try:
          async for message in self._pubsub.listen():
              if message['type'] == 'message':
                  return tuple(msgpack.unpackb(message['data'], raw=False))
        except Exception as e:
            raise ChannelListenError(str(e))
