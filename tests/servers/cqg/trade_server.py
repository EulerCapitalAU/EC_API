import websockets
from websockets.asyncio.server import ServerConnection, OPEN
from EC_API.ext.WebAPI.webapi_2_pb2 import ServerMsg, ClientMsg
from tests.servers.tape import SourceTape, GBMTape
from tests.servers.ws_server_base import WSserver
from tests.servers.brains import ServerBrain
from tests.servers.cqg.data_server_brain import FakeTradeServerBrainCQG



class FakeExchangeTradeServerCQG:
    """ """
    def __init__(
            self,
            host: int, port: int,
            brain: ServerBrain
        ):
        self.brain: ServerBrain = brain
            
        # --- Web Socket server setup
        self.ws_server: WSserver = WSserver(host, port, self.handler)
        
    # ---- properties
    @property
    def connections(self) -> set[ServerConnection]:
        return {c for c in self.all_connections if c.protocol.state is OPEN}

    # ---- Websocket related functions
    async def handler(self, conn: ServerConnection):
        # !!! rework the split 
        while not self.stop_evt.is_set():
            try:
               msg = await conn.recv()
               server_msg = await self.response_logic(msg)
               await conn.send(server_msg.SerializeToString())
               
            except websockets.exceptions.ConnectionClosed:
                break
            
    # ---- Brain related functions
    async def response_logic(self, msg: ClientMsg, **kwargs):
        return await self.brain.response_logic(msg, **kwargs)
