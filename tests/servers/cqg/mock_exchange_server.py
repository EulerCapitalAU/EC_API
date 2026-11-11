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
    One connection, one pair of send+recv loop
    
    One symbol subscription, one drain task.
    
    Drain task read the queue in the stream router and put the data in the out_queue
    
    Data Streaming: 
    Trade Handling:
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
        
        # Server Engine containers
        self.data_server_out_queue: asyncio.Queue[ServerMsg] = asyncio.Queue()
        self.trade_server_out_queue: asyncio.Queue[ServerMsg] = asyncio.Queue()

        #self.router = StreamRouter()
        #self._producers_registry: dict[int, asyncio.Task] = dict()
        
        # async management
        self.data_tg: asyncio.TaskGroup = asyncio.TaskGroup()
        self.trade_tg: asyncio.TaskGroup = asyncio.TaskGroup()
        
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
    async def data_handle(self): ...
             
    async def _drain_data(self, contract_id, out_queue):
        # Put data from stream router to out_queue
        ...
    # -------- Trade Server --------
    async def _drain_trade(self, contract_id, out_queue):
        # Put trade from stream router to out_queue
        ...

    # --------- Control ---------
    # ----- Data Server ----
    async def _data_server_recv_loop(
        self, 
        conn, 
        out_queue, 
        tg, 
        writer_task, 
        session_id
    ): 
        tasks: dict[int, asyncio.Task] = {}
        try:
           async for raw in conn:               
               # WS recv with raw formats
               raw = await conn.recv() # WS recv()
               
               msg = ClientMsg() # make it into a client msg format
               msg.ParseFromString(raw)
               
               server_msg = await self.brain.response_logic(msg)
               
               if server_msg is not None:
                   await self.data_server_out_queue.put(
                       server_msg.SeralizeToString()
                       )
               # Check for market_subscription
        except: ...
        finally:...
           
    async def _data_server_send_loop(self, conn):
        tasks: dict[int, asyncio.Task] = {}
        while not self.stop_evt():
            try:
                if not self.data_server_out_queue.empty():
                    msg = await self.data_server_out_queue.get()
                    await conn.send(msg)                
            except:...
            
    # ----- Trade Server ----
    async def _trade_server_recv_loop(
        self, conn, out_queue, tg, writer_task, session_id
    ):
        try:
           async for raw in conn:
               ...
        finally:...
        
    async def _trade_server_send_loop(self):...


    # --------- Lifecycle ---------
    async def start(self):
        # initialise WS server layer
        await self.ws_server.start()
        
        # inintialise Server Brain
        
        # setup Data Stream parameters
        
        # spawn loops
        
    async def stop(self):
        await self.ws_server.stop()
        
        # cancel all tasks