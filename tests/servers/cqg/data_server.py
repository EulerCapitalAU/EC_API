import asyncio
import websockets
from tests.servers.ws_server_base import WSserver


class MsgFormatterCQG:
    def __init__(self):...
HOST, PORT = 0, 0
class FakeExchangeDataServerCQG:
    
    def __init__(self, ws_server: WSserver):
        self.ws_server: WSserver = WSserver()
        self.stop_evt: asyncio.Event = asyncio.Event()
    
    def response_logic(self):...
    
    async def handler(self, conn):
        while True:
            try:
               msg = await conn.recv()
            except websockets.exceptions.ConnectionClosed:
                break