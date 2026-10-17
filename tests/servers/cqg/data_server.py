import asyncio
from typing import Optional, Literal, AsyncIterator, Any
import websockets
from websockets.asyncio.server import ServerConnection, OPEN
from EC_API.ext.WebAPI.webapi_2_pb2 import ServerMsg, ClientMsg
from EC_API.transport.routers import StreamRouter
from tests.servers.tape import SourceTape, GBMTape
from tests.servers.ws_server_base import WSserver
from tests.servers.brains import ServerBrain
from tests.servers.cqg.data_server_brain import FakeDataServerBrainCQG

class MsgFormatterCQG:
    def __init__(self):...
    
RpcAction = tuple[Literal["rpc"], ServerMsg]
StreamAction = tuple[Literal["stream"], AsyncIterator[ServerMsg]]
BrainAction = RpcAction | StreamAction

class FakeExchangeDataServerCQG:
    def __init__(
            self, 
            host: int, port: int,
            #ws_server: WSserver,
            tape: SourceTape,
            brain: ServerBrain,
            data_params: dict[str, Any]
        ):
        self.tape: Optional[SourceTape] = tape
        self.brain: Optional[ServerBrain] = brain
        self.data_params: dict[str, Any] = data_params

        self.ws_server: WSserver = WSserver(host, port, self.handler)
        
        self.router = StreamRouter()
        self.producers_registry: dict[int, asyncio.Task] = dict()
        self.stop_evt: asyncio.Event = asyncio.Event()
        self.interval: int = 10
        
    async def response_logic(self, msg: ClientMsg, **kwargs):
        return await self.brain.response_logic(msg, **kwargs)
    
    async def handler(self, conn: ServerConnection):
        # !!! rework the split 
        while not self.stop_evt.is_set():
            try:
               msg = await conn.recv()
               server_msg = await self.response_logic(msg)
               await conn.send(server_msg.SerializeToString())
               
            except websockets.exceptions.ConnectionClosed:
                break
            
    # --- Streaming
    def spawn_stream(self, contract_id: int): 
        gmb_tape = GBMTape(
            1034, 
            self.data_params['s'], 
            self.data_params['scale'], 
            self.data_params['mu'], 
            self.data_params['sigma']
        )
        
        asyncio.create_task(self.stream_producer_task(contract_id, gmb_tape))
        asyncio.create_task(self.stream_consumer_send_task(contract_id))
    
    def despawn_stream(self, contract_id:int, queue:asyncio.Queue):
        self.router.unsubscribe(contract_id, queue)          
        # sync — removes queue, deletes 
        if contract_id not in self.router._subs:
            task = self._producers.pop(contract_id, None)
            if task is not None:
                task.cancel()
                
    async def stream_producer_task(self, contract_id:int, tape: SourceTape):
        # read tape, put the data into the stream queue
        for tick in tape.replay():
            await self.router.publish(contract_id, tick)
            await asyncio.sleep(self.interval)                 # <-- yields control here
            if contract_id not in self.router._subs:      # checked AFTER an await
                break
        del self._producers[contract_id]
        
    async def stream_consumer_send_task(self, sub_id):...
        # read the sub_id, send it out to subscribers 
        #await conn.send()


    # --- Lifecycle
    @property
    def connections(self) -> set[ServerConnection]:
        return {c for c in self.all_connections if c.protocol.state is OPEN}
    
    async def start(self):
        # initialise WS server layer
        await self.ws_server.start()
        
        # inintialise Server Brain
        
    async def stop(self):
        await self.ws_server.stop()