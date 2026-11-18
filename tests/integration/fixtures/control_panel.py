


# Fast API control panel
# inputs
# (1) DataEngine: add/remove streams
# (2) TradeEngine: add/remove streams, emergency escape
# (3) MonitorEngine: add/remove streams, datafeed settings
# ------
# (4) StragtegyEngine: ..
import os, tempfile, time, json
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import (
    FastAPI, Request, Depends,
    Form, Header, HTTPException, status
)
from EC_API.channel.base import Channel
from EC_API.channel.redis import RedisChannel

require_api_key = 0


app = FastAPI(title="Control Channel", version="0.1.0")
base_dir = os.path.dirname(__file__)

templates = Jinja2Templates(directory=os.path.join(base_dir, "templates"))

STORE = None
# =============================================================================
# 
# def write_snapshot(path: str, data: dict) -> None:
#     data["updated_at"] = time.time()
#     dir_ = os.path.dirname(path)
#     fd, tmp_path = tempfile.mkstemp(dir=dir_)
#     try:
#         with os.fdopen(fd, "w") as f:
#             json.dump(data, f)
#         os.replace(tmp_path, path)   # atomic rename, same filesystem
#     except Exception:
#         os.unlink(tmp_path)
#         raise
# 
# def read_snapshot(path: str, max_age_secs: float) -> dict | None:
#     with open(path) as f:
#         data = json.load(f)
#     if time.time() - data["updated_at"] > max_age_secs:
#         return None  # stale — writer may be dead
#     return data
# =============================================================================

# ----- Control Functions
class ControlPanel:
    def __init__(self, channel_cfg_addr:str):
        self.channel: Channel = RedisChannel(
            channel_cfg_addr, 
            service_tag="control_panel_streams"
            )
        #self.out_streams: list[str] = None
        
        # --- stream tags
        self.CMD_CRTL_trade_engine: str = "CMD:TradeEngineCQG"
        self.CMD_CRTL_data_engine: str = "CMD:DataEngineCQG"
        self.CMD_CRTL_monitor_engine: str = "CMD:MonitorEngine"
        
    # ------ Control Panel internal functions ------
    # --- the dispatch function 
    async def publish_event(self, kind: str, content: dict) -> None:
        await self.channel.broadcast( 
            {"type": kind, "data": json.dumps(content)}, 
            self.out_stream
        )
    def _write_datafeed_snapshot(self) -> None:...
    def _read_datafeed_snapshot(self) -> None:...
    
    # ------ Trade Engine functions ------
    async def add_stream_trade_engine(self, stream_name: str) -> None: 
        await self.channel.broadcast(
            ("CMD:add_stream", stream_name), 
            self.CMD_CRTL_trade_engine
            )
        
    async def remove_stream_trade_engine(self, stream_name:str) -> None: 
        await self.channel.broadcast(
            ("CMD:remove_stream", stream_name), 
            self.CMD_CRTL_trade_engine
            )
        
    async def freeze_trade_engine(self) -> None:
        await self.channel.broadcast(
            ("CMD:freeze_engine", ""), 
            self.CMD_CRTL_trade_engine
            )

    async def wake_trade_engine(self) -> None:
        await self.channel.broadcast(
            ("CMD:unfreeze_engine", ""), 
            self.CMD_CRTL_trade_engine
            )

    async def shutdown_trade_engine(self) -> None:
        await self.channel.broadcast(
            ("CMD:shutdown_engine", ""), 
            self.CMD_CRTL_trade_engine
            )

    async def send_cancelall_order(self) -> None:
        await self.channel.broadcast(
            ("CMD:cancel_all_request", ""), 
            self.CMD_CRTL_trade_engine
            )

    async def send_goflat_order(self) -> None:
        await self.channel.broadcast(
            ("CMD:goflat_request", ""), 
            self.CMD_CRTL_trade_engine
            )
    
    # ------ Data Engine functions ------
    async def add_stream_data_engine(self, stream_name: str) -> None:
        await self.channel.broadcast(
            ("CMD:add_stream", stream_name), 
            self.CMD_CRTL_data_engine
            )
        
    async def remove_stream_data_engine(self, stream_name: str) -> None:
        await self.channel.broadcast(
            ("CMD:remove_stream", stream_name), 
            self.CMD_CRTL_data_engine
            )
    
    async def freeze_data_engine(self) -> None:
        await self.channel.broadcast(
            ("CMD:freeze_engine", ""), 
            self.CMD_CRTL_data_engine
            )
        
    async def wake_data_engine(self) -> None:
        await self.channel.broadcast(
            ("CMD:unfreeze_engine", ""), 
            self.CMD_CRTL_data_engine
            )
        
    async def shutdown_data_engine(self) -> None:
        await self.channel.broadcast(
            ("CMD:shutdown_engine", ""), 
            self.CMD_CRTL_data_engine
            )
        
    async def add_datafeed(self, symbol: str) -> None: ...
    async def remove_datafeed(self, symbol: str) -> None: ...
    async def remove_all_datafeeds(self): ...
    
    async def list_datafeeds(self) -> list[str]:...
        # Read backup file
        #members = await self.r.smembers(STREAM_CONTORL_FEEDS)
        #print("members", members)
        #return sorted(members)

    async def add_datafeed_in_use(self, symbol: str) -> None: ...
    async def remove_datafeed_in_use(self, symbol: str) -> None: ...
    async def remove_all_datafeeds_in_use(self):...
    async def list_datafeeds_in_use(self) -> list[str]:...
    
    # ------ Monitor Engine functions ------
    async def freeze_monitor_engine(self) -> None:...
    async def wake_monitor_engine(self) -> None:...
    async def run_monitor_engine(self) -> None:...
    
    async def set_datafeeds_config(self) -> None:...

CrtlPanel = ControlPanel()
# ----- Control Panel Layout
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    #active_strategies = await STORE.list_strategies()
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            #"api_key_prefill": os.getenv("CONTROL_API_KEY", "dev-secret"),
            #"strategy_catalog": STRATEGY_CATALOG,
            #"active_strategies": active_strategies,
            #"active_signals": ACTIVE_SIGNALS
        }
    )

@app.get("/ui/feeds", response_class=HTMLResponse, dependencies=[Depends(require_api_key)])
async def ui_feeds(request: Request):
    feeds = await STORE.list_feeds()
    return templates.TemplateResponse(
        "partials/feeds.html",
        {"request": request, "symbols": feeds}
    )
# ----- Trade Engine -----
@app.post("/action/cmd/freeze_trade_engine", response_class=HTMLResponse, dependencies=[Depends(require_api_key)])
async def action_freeze_trade_engine(request: Request):
    await CrtlPanel.freeze_trade_engine()

@app.post("/action/cmd/unfreeze_trade_engine", response_class=HTMLResponse, dependencies=[Depends(require_api_key)])
async def action_unfreeze_trade_engine(request: Request):
    await CrtlPanel.unfreeze_trade_engine()

@app.post("/action/cmd/shutdown_trade_engine", response_class=HTMLResponse, dependencies=[Depends(require_api_key)])
async def action_shutdown_trade_engine(request: Request):
    await CrtlPanel.shutdown_data_engine()

@app.post("/action/cmd/goflat", response_class=HTMLResponse, dependencies=[Depends(require_api_key)])
async def action_goflat(request: Request):
    # Send Command to redis DB here
    # Establish new connection and then send the go flat order
    # Doing so will automatically reset the conenction in the trade server, 
    # But the retry function the retry functions there should restart the service immediately

    # make this later-> publish to bus (Redis/Kafka): {"cmd":"GO_FLAT"}
    # check engine states -> then send
    await STORE.send_goflat_order()
    
    await CrtlPanel.send_goflat_order()
    
    return templates.TemplateResponse("partials/flash.html", {"request": request, "msg": "GoFlat sent"})

@app.post("/action/cmd/cancelall", response_class=HTMLResponse, dependencies=[Depends(require_api_key)])
async def action_cancelall(request: Request):
    # Send Command to redis DB here
    #await STORE.send_cancelall_order()
    
    await CrtlPanel.send_cancelall_order()

    return templates.TemplateResponse("partials/flash.html", {"request": request, "msg": "CancelAll sent"})

# ----- Data Engine -----
@app.post("/action/cmd/freeze_data_engine", response_class=HTMLResponse, dependencies=[Depends(require_api_key)])
async def action_freeze_data_engine(request: Request):
    await CrtlPanel.freeze_data_engine()

@app.post("/action/cmd/unfreeze_data_engine", response_class=HTMLResponse, dependencies=[Depends(require_api_key)])
async def action_unfreeze_data_engine(request: Request):
    await CrtlPanel.unfreeze_data_engine()

@app.post("/action/cmd/shutdown_data_engine", response_class=HTMLResponse, dependencies=[Depends(require_api_key)])
async def action_unfreeze_data_engine(request: Request):
    await CrtlPanel.shutdown_data_engine()

@app.post("/action/datafeeds/add", response_class=HTMLResponse, dependencies=[Depends(require_api_key)])
async def action_datafeeds_add(request: Request, symbols: str = Form("")):
    # Add symbols to in-process memory
    for s in [x.strip().upper() for x in symbols.split(",") if x.strip()]:
        # Check if the symbol is already in the list, if so, do nothing
        feeds = await STORE.list_feeds()
        if s not in feeds:
            #FEEDS.add(s)
            # Add to in-memory database 
            #await STORE.add_feed(s)
            await CrtlPanel.add_datafeed(s)
            
    # return updated feeds partial (HTMX swaps it in place)
    return await ui_feeds(request)

@app.post("/action/fatafeeds/remove", response_class=HTMLResponse, dependencies=[Depends(require_api_key)])
async def action_datafeeds_remove(request: Request, symbols: str = Form("")):

    for s in [x.strip().upper() for x in symbols.split(",") if x.strip()]:
        #print(s, FEEDS_IN_USE)
        feeds_in_use = await STORE.list_feeds_in_use()
        # Check if there are strategies currently using these symbols'        
        if s in feeds_in_use:
            print(f"'{s}' is currently in used. ")
            raise HTTPException(400, f"'{s} is currently in used.")

        await STORE.remove_feed(s)  
    return await ui_feeds(request)

@app.post("/action/datafeeds/remove_all", response_class=HTMLResponse, dependencies=[Depends(require_api_key)])
async def action_datafeeds_remove_all(request: Request):
    
    feeds_in_use = await STORE.list_feeds_in_use()
    # Check if there are strategies currently using these symbols'        
    if len(feeds_in_use) != 0:
        raise HTTPException(400, "Some Data Feeds are still in-use.")

    await STORE.remove_all_feeds()
    
    return await ui_feeds(request)

# ----- Monitor Engine -----
## Choose the buffer parameter you want
## Choose what Stat you want