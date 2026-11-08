import asyncio
from typing import Optional, Literal, AsyncIterator, Any
import websockets
from websockets.asyncio.server import ServerConnection, OPEN
from EC_API.ext.WebAPI.webapi_2_pb2 import ServerMsg, ClientMsg
from EC_API.channel.base import Channel
from EC_API.transport.routers import StreamRouter
from tests.servers.tape import SourceTape, GBMTape
from tests.servers.ws_server_base import WSserver
from tests.servers.brains import ServerBrain
from tests.servers.cqg.server_brain import FakeServerBrainCQG

class MsgFormatterCQG:
    def __init__(self):...
    
RpcAction = tuple[Literal["rpc"], ServerMsg]
StreamAction = tuple[Literal["stream"], AsyncIterator[ServerMsg]]
BrainAction = RpcAction | StreamAction

class FakeExchangeServerCQG:
    """ 
    
    Data Streaming: 
    Trade Handling
    """
    def __init__(
            self, 
            data_host: int, data_port: int,
            trade_host: int, trade_port: int,
            #tape: SourceTape,
            brain: ServerBrain,
            data_params: dict[str, Any]
        ):
        
        # --- Brain parameters
        self.brain: Optional[ServerBrain] = brain
        
        self.tapes: dict[int, SourceTape] = dict()
        #self.tape: Optional[SourceTape] = tape
        self.data_params: dict[str, Any] = data_params

        # --- Websocket server 
        self.ws_data_server: WSserver = WSserver(data_host, data_port, self.handler)
        self.ws_trade_server: WSserver = WSserver(trade_host, trade_port, self.handler)
        self.all_connections: list[ServerConnection] = list()
        
        # Server Engine container
        self.router = StreamRouter()
        self._producers_registry: dict[int, asyncio.Task] = dict()
        self.stop_evt: asyncio.Event = asyncio.Event()
        self.interval: int = 10
        
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
       
    # -------- Data Server Streaming --------
    async def _stream_producer_task(self, contract_id:int, tape: SourceTape):
        # read tape, put the data into the stream queue (_subs)
        for tick in tape.replay():
            await self.router.publish(contract_id, tick)
            await asyncio.sleep(self.interval)                 # <-- yields control here
            if contract_id not in self.router._subs:      # checked AFTER an await
                break
        del self._producers_registry[contract_id]
        
    async def _stream_consumer_send_task(self, sub_id):...
        # read the sub_id, send it out to subscribers 
        #await conn.send()
        
    def spawn_stream(self, contract_id: int): 
        gmb_tape = GBMTape(
            1034, 
            self.data_params['s'], 
            self.data_params['scale'], 
            self.data_params['mu'], 
            self.data_params['sigma']
        )
        
        self._producers_registry[contract_id] = asyncio.create_task(self.stream_producer_task(contract_id, gmb_tape))
        asyncio.create_task(self.stream_consumer_send_task(contract_id))
    
    def despawn_stream(self, contract_id:int, queue:asyncio.Queue):
        self.router.unsubscribe(contract_id, queue)          
        # sync — removes queue, deletes 
        if contract_id not in self.router._subs:
            task = self._producers.pop(contract_id, None)
            if task is not None:
                task.cancel()
                
    # -------- Trade Server --------

    # --- Lifecycle    
    async def start(self):
        # initialise WS server layer
        await self.ws_server.start()
        
        # inintialise Server Brain
        
        # setup Data Stream parameters
        
    async def stop(self):
        await self.ws_server.stop()