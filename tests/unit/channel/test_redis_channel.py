#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 21 16:52:59 2026

@author: dexter
"""
import time
import asyncio
import subprocess
from pathlib import Path
import pytest
from unittest.mock import AsyncMock, MagicMock
import msgpack
import redis.asyncio as aioredis
from EC_API.channel.redis import RedisChannel
from EC_API.exceptions import (
    ConfigInputError, 
    ConfigFormatError,
    ChannelMissingSettingError,
    ChannelBroadcastError,
    ChannelListenError,
    ChannelSubscriptionError
    )

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
TEST_TOML_TCP_SOCKET = FIXTURES_DIR / "test_redis_channel_setup_tcp_socket.toml"
TEST_TOML_UDS = FIXTURES_DIR / "test_redis_channel_setup_uds.toml"

# ---- Lifecycle ----
def test_redis_channel_load_tcp_valid() -> None:
    RC = RedisChannel()
    RC.load(TEST_TOML_TCP_SOCKET)
    assert RC.host_name['URL'] == "redis://localhost:16379"
    assert RC.in_streams == {"mkt_data:cqg", "mkt_data:fix"}
    assert RC.out_streams == {"processed_data"}
    assert list(RC.last_ids.keys()) == ["mkt_data:cqg", "mkt_data:fix"]
    assert list(RC.last_ids.values()) == ["0", "0"]
    
def test_redis_channel_load_uds_valid() -> None:
    RC = RedisChannel()
    RC.load(TEST_TOML_UDS)
    assert RC.host_name['URL'] == "unix:///tmp/redis.sock"
    assert RC.in_streams == {"mkt_data:cqg", "mkt_data:fix"}
    assert RC.out_streams == {"processed_data"}
    assert list(RC.last_ids.keys()) == ["mkt_data:cqg", "mkt_data:fix"]
    assert list(RC.last_ids.values()) == ["0", "0"]
    
BAD_CONFIGS = [
    # (toml_bytes, expected_exception)
    (b"this is not valid toml ===", ConfigInputError),
    (b'[streams]\nin_streams = ["s1"]', ConfigFormatError),  # no host_name
    (b'[host_name]\nURL = "redis://localhost:16379"', ConfigFormatError),  # no streams
    (b'[host_name]\nURL = "redis://localhost:16379"\n[streams]', ConfigFormatError),  # streams empty
    (b'[host_name]\nURL = "redis://localhost:16379"\n[streams]\nin_streams=[]\nout_streams=[]', ConfigFormatError) # at least one of the streams has to be present
]

@pytest.mark.parametrize("content, exc", BAD_CONFIGS)
def test_redis_channel_load_invalid_config(tmp_path, content, exc):
    f = tmp_path / "config.toml"
    f.write_bytes(content)
    with pytest.raises(exc):
        RedisChannel(path=str(f))

def test_redis_channel_load_invalid_missing_file():
    with pytest.raises(ConfigInputError):
        RedisChannel(path="/nonexistent/config.toml")
 
@pytest.mark.asyncio
async def test_redis_channel_connect_valid() -> None:
    RC = RedisChannel(TEST_TOML_TCP_SOCKET)
    await RC.connect()
    assert isinstance(RC.r, aioredis.Redis)
    assert isinstance(RC.pipeline, aioredis.client.Pipeline)
    assert isinstance(RC._pubsub, aioredis.client.PubSub)
    
@pytest.mark.asyncio
async def test_redis_channel_connect_last_ids_from_existing_stream(redis_client) -> None:
    await redis_client.xadd("mkt_data:cqg", {b"data": b"x"})
    await redis_client.xadd("mkt_data:fix", {b"data": b"x"})

    RC = RedisChannel(TEST_TOML_TCP_SOCKET)
    await RC.connect()

    assert RC.last_ids["mkt_data:cqg"] != "0"
    assert RC.last_ids["mkt_data:fix"] != "0"

@pytest.mark.asyncio   
async def test_redis_channel_connect_invalid_hostname_empty() -> None:
    RC = RedisChannel()
    with pytest.raises(ChannelMissingSettingError):
        await RC.connect()
        
@pytest.mark.asyncio
async def test_redis_channel_disconnect_valid() -> None:
    redis_client = aioredis.Redis()
    
    RC = RedisChannel()
    RC.r = redis_client
    RC._pubsub = redis_client.pubsub()
    await RC.disconnect()
    
    assert RC.r == None
    assert RC.pipeline == None
    assert RC._pubsub == None
    
@pytest.mark.asyncio  
async def test_redis_channel_disconnect_invalid_empty() -> None:     
    RC = RedisChannel()
    with pytest.raises(ChannelMissingSettingError):
        await RC.disconnect()

# ---- Function calls ----
@pytest.mark.asyncio  
async def test_redis_channel_broadcast_valid(redis_client) -> None:
    redis_client.from_url("redis://localhost:16379")
    
    RC = RedisChannel()
    RC.r = redis_client
    RC.host_name = {'URL': "redis://localhost:16379"}
    RC.out_streams = {"processed_data"}
    RC.in_streams = {"mkt_data:cqg", "mkt_data:fix"}
    RC.last_ids = {"mkt_data:cqg": "$", "mkt_data:fix": "$"}

    parsed_msg = ('1', 2, 3.0, True)
    byte_msg = msgpack.packb(parsed_msg)
    await RC.broadcast(parsed_msg, "processed_data")
    data = await redis_client.xread(streams={"processed_data": "0"}, count=1)
    assert data[0][1][0][1][b'data'] == byte_msg
    
@pytest.mark.asyncio  
async def test_redis_channel_broadcast_invalid_no_redis() -> None:
    RC = RedisChannel()

    parsed_msg = ('1', 2, 3.0, True)
    with pytest.raises(ChannelMissingSettingError):
        await RC.broadcast(parsed_msg, "processed_data")
        
@pytest.mark.asyncio  
async def test_redis_channel_broadcast_invalid_exception(redis_client) -> None:
    redis_client.from_url("redis://localhost:16379")
    
    RC = RedisChannel()
    RC.r = redis_client
    RC.host_name = {'URL': "redis://localhost:16379"}
    RC.out_streams = {"processed_data"}
    RC.in_streams = {"mkt_data:cqg", "mkt_data:fix"}
    RC.last_ids = {"mkt_data:cqg": "$", "mkt_data:fix": "$"}
    
    unpacked_msg = (object(),)
    with pytest.raises(ChannelBroadcastError):
        await RC.broadcast(unpacked_msg, "processed_data") #<--non -existent stream
        

@pytest.mark.asyncio      
async def test_redis_channel_listen_valid(redis_client) -> None:
    RC = RedisChannel()
    RC.r = redis_client
    RC.host_name = {'URL': "redis://localhost:16379"}
    RC.out_streams = {"processed_data"}
    RC.in_streams = {"mkt_data:cqg", "mkt_data:fix"}
    RC.last_ids = {"mkt_data:cqg": "0", "mkt_data:fix": "0"}

    parsed_msg = ('1', 2, 3.0, True)
    # Not that the b'data' here is intensionaly because msgpack send only bytes
    await RC.r.xadd("mkt_data:cqg", {b'data': msgpack.packb(parsed_msg)})
    await asyncio.sleep(0)
    
    data = await RC.listen("mkt_data:cqg",'data')
    assert RC.last_ids

    assert data == parsed_msg
    
@pytest.mark.asyncio      
async def test_redis_channel_listen_invalid_no_redis() -> None:
    RC = RedisChannel()
    with pytest.raises(ChannelMissingSettingError):
        await RC.listen('stream_name')
    
@pytest.mark.asyncio      
async def test_redis_channel_listen_invalid_no_last_ids(redis_client) -> None:
    RC = RedisChannel()
    RC.r = redis_client

    with pytest.raises(ChannelMissingSettingError):
        await RC.listen('stream_name')
 
@pytest.mark.asyncio      
async def test_redis_channel_listen_invalid_exceed_one_listener(redis_client) -> None:
    RC = RedisChannel()
    RC.r = redis_client
    RC.host_name = {'URL': "redis://localhost:16379"}
    RC.out_streams = {"processed_data"}
    RC.in_streams = {"mkt_data:cqg", "mkt_data:fix"}
    RC.last_ids = {"mkt_data:cqg": "0", "mkt_data:fix": "0"}
    
    RC._active_listeners.add("mkt_data:cqg")
    
    with pytest.raises(ChannelListenError):
        await RC.listen("mkt_data:cqg")

@pytest.mark.asyncio      
async def test_redis_channel_listen_invalid_empty_msg(redis_client) -> None:
    RC = RedisChannel()
    RC.r = redis_client
    RC.xread_block = 100
    RC.host_name = {'URL': "redis://localhost:16379"}
    RC.out_streams = {"processed_data"}
    RC.in_streams = {"mkt_data:cqg", "mkt_data:fix"}
    RC.last_ids = {"mkt_data:cqg": "0", "mkt_data:fix": "0"}
    
    data = await RC.listen("mkt_data:cqg",'data')
    await asyncio.sleep(0.1)
    assert data is None
    
@pytest.mark.asyncio
async def test_redis_channel_listen_invalid_bad_data(redis_client) -> None:
    RC = RedisChannel()
    RC.r = redis_client
    RC.last_ids = {"mkt_data:cqg": "0"}

    await redis_client.xadd("mkt_data:cqg", {b"data": b"\xff\xff\xff"})  # not valid msgpack

    with pytest.raises(ChannelListenError):
        await RC.listen("mkt_data:cqg")

# --- Pubsub sub/unsub
@pytest.mark.asyncio
async def test_redis_channel_subscribe_valid(redis_client) -> None:
    RC = RedisChannel()
    RC.r = redis_client
    RC._pubsub = redis_client.pubsub()
    
    await RC.subscribe_pubsub("channel_1")
    await RC.subscribe_pubsub("channel_2")
    assert "channel_1".encode() in RC._pubsub.channels.keys()
    assert "channel_2".encode() in RC._pubsub.channels.keys()
 
@pytest.mark.asyncio
async def test_redis_channel_subscribe_invalid_no_pubsub(redis_client) -> None:
    RC = RedisChannel()
    RC.r = redis_client
    with pytest.raises(ChannelMissingSettingError):
        await RC.subscribe_pubsub("channel_1")
        
@pytest.mark.asyncio
async def test_redis_channel_subscribe_invalid_subscription(redis_client) -> None:
    RC = RedisChannel()
    RC.r = redis_client
    RC._pubsub = redis_client.pubsub()

    with pytest.raises((ChannelSubscriptionError, TypeError)):
        await RC.subscribe_pubsub(None) # <-- invalid, unhashable
        
@pytest.mark.asyncio
async def test_redis_channel_unsubscribe_valid(redis_client) -> None:
    RC = RedisChannel()
    RC.r = redis_client
    RC._pubsub = redis_client.pubsub()
    
    await RC.subscribe_pubsub("channel_1")
    await RC.subscribe_pubsub("channel_2")
      
    await RC.unsubscribe_pubsub("channel_1")
    counts = await redis_client.pubsub_numsub("channel_1", "channel_2")

    assert counts[0] == (b'channel_1', 0)
    assert counts[1] == (b'channel_2', 1)

@pytest.mark.asyncio
async def test_redis_channel_unsubscribe_invalid_no_pubsub(redis_client) -> None:
    RC = RedisChannel()
    RC.r = redis_client

    with pytest.raises(ChannelMissingSettingError):
        await RC.unsubscribe_pubsub("channel_1")
        
@pytest.mark.asyncio
async def test_redis_channel_unsubscribe_invalid_channel_name_non_exist(redis_client) -> None:
    RC = RedisChannel()
    RC.r = redis_client
    RC._pubsub = redis_client.pubsub()

    with pytest.raises(ChannelSubscriptionError):
        await RC.unsubscribe_pubsub("channel_1")

@pytest.mark.asyncio
async def test_redis_channel_unsubscribe_invalid_exception(redis_client) -> None:
    RC = RedisChannel()
    RC.r = redis_client

    mock_pubsub = MagicMock()
    mock_pubsub.channels = {b"channel_1": MagicMock()}   # passes the line-143 guard
    mock_pubsub.unsubscribe = AsyncMock(side_effect=Exception("forced error"))
    RC._pubsub = mock_pubsub

    with pytest.raises(ChannelSubscriptionError):
        await RC.unsubscribe_pubsub("channel_1")

# --- Pubsub function calls
@pytest.mark.asyncio
async def test_redis_channel_pubsub_round_trip_valid(redis_client) -> None:
    RC_sub = RedisChannel()
    RC_sub.r = redis_client
    RC_sub._pubsub = redis_client.pubsub()
    await RC_sub.subscribe_pubsub("test_channel")

    RC_pub = RedisChannel()
    RC_pub.r = redis_client

    # Start listener as a background task
    listen_task = asyncio.create_task(RC_sub.listen_pubsub())

    # Yield to the event loop so listen_task actually enters .listen()
    await asyncio.sleep(0.05)

    # Now publish — the listener is already waiting
    await RC_pub.broadcast_pubsub(('1', 2, 3.0, True), "test_channel")

    result = await listen_task
    assert result == ('1', 2, 3.0, True)
    
@pytest.mark.asyncio
async def test_redis_channel_listen_pubsub_invalid_no_pubsub(redis_client) -> None:
    RC_sub = RedisChannel()
    RC_sub.r = redis_client

    with pytest.raises(ChannelMissingSettingError):
        await RC_sub.listen_pubsub()
        
@pytest.mark.asyncio
async def test_redis_channel_broadcast_pubsub_invalid_no_redis(redis_client) -> None:
    RC_pub = RedisChannel()
    with pytest.raises(ChannelMissingSettingError):
        await RC_pub.broadcast_pubsub(('1', 2, 3.0, True), "test_channel")

@pytest.mark.asyncio
async def test_redis_channel_broadcast_pubsub_invalid_exception() -> None:
    RC = RedisChannel()
    RC.r = MagicMock()
    RC.r.publish = AsyncMock(side_effect=Exception("forced error"))

    with pytest.raises(ChannelBroadcastError):
        await RC.broadcast_pubsub(('1', 2, 3.0), "test_channel")

@pytest.mark.asyncio
async def test_redis_channel_listen_pubsub_invalid_exception() -> None:
    async def failing_listen():
        raise Exception("forced error")
        yield   # never reached, but makes this an async generator

    RC = RedisChannel()
    RC._pubsub = MagicMock()
    RC._pubsub.listen = failing_listen  # assign the function, not a call

    with pytest.raises(ChannelListenError):
        await RC.listen_pubsub()
        
@pytest.mark.asyncio
async def test_redis_channel_listen_pubsub_invalid_bad_data() -> None:
    async def bad_data_listen():
        yield {'type': 'message', 'data': b'\xff\xff\xff'}  # corrupt msgpack

    RC = RedisChannel()
    RC._pubsub = MagicMock()
    RC._pubsub.listen = bad_data_listen

    with pytest.raises(ChannelListenError):
        await RC.listen_pubsub()
    assert "mkt_data:cqg" not in RC._active_listeners

