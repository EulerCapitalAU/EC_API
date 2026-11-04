import logging
from EC_API.common.data_feeds import DataFeed

logger = logging.getLogger(__name__)

class MonitorController:
    
    async def add_out_stream(self):...
    async def remove_out_stream(self):...
    async def add_in_stream(self):...
    async def remove_in_stream(self):...


class MonitorEngine:
    def __init__(self):
        # DataFeed Mechanism
        # recorder for storing mkt data
        self.in_channel = None # take from Data Engine
        self.out_channel = None # send out to either prometheus or control panel
        self.datafeeds: dict[str: DataFeed] = dict()
        
    def add_dataFeed(self): ...
        # upon receiving a in_stream, we also activate add datafeed
        
    def remove_datafeed(self):...
    
    def _calculate_and_push_data(self):...
    
    @property
    def state(self):
        return self._state_mgr.cur
                    
    # ------- Engine functions (Monitor)
