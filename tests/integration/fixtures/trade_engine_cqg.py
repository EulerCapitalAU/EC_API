import asyncio
from EC_API.channel.redis import RedisChannel
from EC_API.connect.cqg.base import ConnectCQG
from EC_API.ordering.cqg.trade_session import TradeSessionCQG
from EC_API.ordering.cqg.live_order import LiveOrderCQG
from EC_API.ordering.enums import RequestType, SubScope
from EC_API.payload.base import Payload, ExecutePayload
from EC_API.payload.safety import PreTradeRiskCheck
from EC_API.recorders.base import SQLSchemaTable
from EC_API.recorders.sqlite_recorder import SQLiteRecorder
from EC_API.exceptions import ChannelMissingSettingError

HOST_NAME, USR_NAME, PASSWORD, ACCOUNT_ID = 0, 0, 0, 0

class TradeEngineCQG:
    def __init__(self, channel_cfg_addr: str, pretraderisk_cfg_addr:str):
        # ---- IPC Channel setting ----
        self.channel = RedisChannel(channel_cfg_addr)
        self.recorder = SQLiteRecorder(
            schema = SQLSchemaTable(
                table_name = "test_trade_engine_audit", 
                columns = (("", "", ""), ("", "", ""),)
                ), 
            db_address = ""
            )
            
        # ---- Sessions setting ----
        self.conn = ConnectCQG(HOST_NAME, USR_NAME, PASSWORD, ACCOUNT_ID)
        self.trade_session = TradeSessionCQG(self.conn, recorder=self.recorder)
        
        # ---- Risk checks ----
        self.PREC = PreTradeRiskCheck('cqg')
        self.PREC.load(pretraderisk_cfg_addr)
        
        # ---- Engine property ----
        self._stop_evt = asyncio.Event()
        
        # ---- Engine Containers ----
        self._send_order_tasks: dict[str, asyncio.Task] = dict()
     
    # ------- Manual control methods 
    def add_in_stream(self, in_stream_name: str):
        if in_stream_name in self.channel.in_streams:
            return
        # BEfore adding, either ask for pre-trade risk parameters or load a preset
        # Add in_stream, add symbols, do trade subscription
        # ...
        self.channel.in_streams.add(in_stream_name)
                
        self._send_order_tasks[in_stream_name] = asyncio.create_task(
            self._send_order_loop(in_stream_name)
            )

    def remove_in_stream(self, in_stream_name: str):
        # unsub trade, remove out_Stream
        
        
        task = self._tasks.pop(in_stream_name, None)
        if task is not None:
            task.cancel()
            try:
                await task          # await ONLY here — to let CancelledError settle
            except asyncio.CancelledError:
                pass
        self.channel.in_streams.discard(in_stream_name)
        self.channel.last_ids.pop(in_stream_name, None)   
        
    # ------- Engine Controls
    async def control_loop(self):
        # Listen to control command and add/remove in_stream
        
        # cmd: add_stream, remove_stream, start_engine, stop_engine
        ...
    
    # ------- Engine functions
    async def _package_and_send(
        self, 
        trade_session: TradeSessionCQG, 
        order_type: RequestType, order_info: dict):
        PL = Payload(
          order_request_type = order_type,
          order_info = order_info,
          check_method = self.PREC # Static risk check done upon creation
          )
        await ExecutePayload(live_order=LiveOrderCQG(trade_session)).unload(PL)

    async def _send_order_loop(self, in_stream_name: str):
        while not self._stop_evt:
            # Continuous listening to the latest order instruction from redis stream
            msg = await self.channel.listen(in_stream_name) 
            
            # If there is something, an event is triggered
            if msg is None:
                continue
            # package and send (fire and forget)
            order_type, order_info = msg
            await self._package_and_send(order_type, order_info)
            
        
    # -------- Engine LifeCycle
    async def _setup(self):
        # Connect to channel
        try:
            self.channel.connect()
        except ChannelMissingSettingError as e:
            print("")
            
        # start trade session
        self.trade_session.start()
        # subscribe all the trade subscriptions and pre-resolve symbols
        for stream_name in self.channel.in_streams:
            ...
            #await trade_session.trade_subscription_request(sub_id=1, sub_scope = SubScope.ORDERS)
            #await trade_session.resolve_symbol(order_info['symbol_name'])         
    
    async def start(self):
        await self._setup()

    async def stop(self):...
                
    async def run(self):
        await self.start()
        try:
            await self._stop_evt.wait()
        finally:
            await self.stop()
