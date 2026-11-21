from typing import Optional
import os, json
from contextlib import asynccontextmanager
import uvicorn
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi import (
    FastAPI, Request, Depends,
    APIRouter,
    Form, Header, HTTPException, status
)
from EC_API.channel.base import Channel
from EC_API.channel.redis import RedisChannel

class ControlPanel:
    def __init__(self, channel_cfg_addr:str):
        self.channel: Channel = RedisChannel(
            channel_cfg_addr, 
            service_tag="control_panel_streams"
            )
        
        # --- 
        self.feeds_in_use_data_engine: set = set()
        self.feeds_in_use_trade_engine: set = set()
        
        # ------ stream tags ------
        self.CMD_CTRL_trade_engine: str = "CMD:TradeEngineCQG"
        self.CMD_CTRL_data_engine: str = "CMD:DataEngineCQG"
        self.CMD_CTRL_monitor_engine: str = "CMD:MonitorEngine"
        
    async def start(self):
        await self.channel.connect()
        
    async def stop(self):
        self.channel.disconnect()

    # ------ observation ------
    def list_active_datafeeds(self) -> list[str]:
        return list(self.feeds_in_use_data_engine)

    def list_active_trade_symbols(self) -> list[str]:
        return list(self.feeds_in_use_trade_engine)

    # ------ Control Panel internal functions ------
    # --- the dispatch function 
    async def publish_event(self, kind: str, content: dict) -> None:
        event_stream: str = ""
        await self.channel.broadcast( 
            {"type": kind, "data": json.dumps(content)}, 
            event_stream
        )
        
    # WS datafeeds
    def _write_datafeed_snapshot(self) -> None:...
    def _read_datafeed_snapshot(self) -> None:...
    
    # ------ Control Panel Global Functions ------
    # Data + Monitor Engines
    async def add_datafeed(self, symbol: str) -> None: 
        stream_name = "mkt_data:trade_tick:"+symbol
        self.feeds_in_use_data_engine.add(stream_name)
        
        await self._add_stream_data_engine(stream_name)
        
        # !!! Setup Monitor DataFeed later
        
    async def remove_datafeed(self, symbol: str) -> None:
        stream_name = "mkt_data:trade_tick:"+symbol
        
        # !!! check if any servive is actively using it
        
        self.feeds_in_use_data_engine.remove(stream_name)
        await self._remove_stream_data_engine(stream_name)

    async def remove_all_datafeeds(self): 
        for feed in self.list_active_datafeeds():
            # !!! check if any servive is actively using it

            await self._remove_stream_data_engine(feed)

        self.feeds_in_use_data_engine.clear()
    
    # Strategy Engine
    async def add_datafeed_in_use(self, symbol: str) -> None: ...
    async def remove_datafeed_in_use(self, symbol: str) -> None: ...
    async def remove_all_datafeeds_in_use(self):...
    async def list_datafeeds_in_use(self) -> list[str]:...

    # ------ Trade Engine functions ------
    async def _add_stream_trade_engine(self, stream_name: str) -> None: 
        await self.channel.broadcast(
            ("CMD:add_stream", stream_name), 
            self.CMD_CTRL_trade_engine
            )
        
    async def _remove_stream_trade_engine(self, stream_name:str) -> None: 
        await self.channel.broadcast(
            ("CMD:remove_stream", stream_name), 
            self.CMD_CTRL_trade_engine
            )
      
    async def send_cancelall_order(self) -> None:
        await self.channel.broadcast(
            ("CMD:cancel_all_request", ""), 
            self.CMD_CTRL_trade_engine
            )

    async def send_goflat_order(self) -> None:
        await self.channel.broadcast(
            ("CMD:goflat_request", ""), 
            self.CMD_CTRL_trade_engine
            )
        
    async def start_trade_engine(self) -> None:
        await self.channel.broadcast(
            ("CMD:start_engine", ""), 
            self.CMD_CTRL_trade_engine
            )
        
    async def stop_trade_engine(self) -> None:
        await self.channel.broadcast(
            ("CMD:stop_engine", ""), 
            self.CMD_CTRL_trade_engine
            )
        
    async def freeze_trade_engine(self) -> None:
        await self.channel.broadcast(
            ("CMD:freeze_engine", ""), 
            self.CMD_CTRL_trade_engine
            )

    async def unfreeze_trade_engine(self) -> None:
        await self.channel.broadcast(
            ("CMD:unfreeze_engine", ""), 
            self.CMD_CTRL_trade_engine
            )

    async def shutdown_trade_engine(self) -> None:
        await self.channel.broadcast(
            ("CMD:shutdown_engine", ""), 
            self.CMD_CTRL_trade_engine
            )


    
    # ------ Data Engine functions ------
    async def _add_stream_data_engine(self, stream_name: str) -> None:
        await self.channel.broadcast(
            ("CMD:add_stream", stream_name), 
            self.CMD_CTRL_data_engine
            )
        
    async def _remove_stream_data_engine(self, stream_name: str) -> None:
        await self.channel.broadcast(
            ("CMD:remove_stream", stream_name), 
            self.CMD_CTRL_data_engine
            )
    
    async def start_data_engine(self) -> None:
        await self.channel.broadcast(
            ("CMD:start_engine", ""), 
            self.CMD_CTRL_data_engine
            )
        
    async def stop_data_engine(self) -> None:
        await self.channel.broadcast(
            ("CMD:stop_engine", ""), 
            self.CMD_CTRL_data_engine
            )
        
    async def freeze_data_engine(self) -> None:
        await self.channel.broadcast(
            ("CMD:freeze_engine", ""), 
            self.CMD_CTRL_data_engine
            )
        
    async def unfreeze_data_engine(self) -> None:
        await self.channel.broadcast(
            ("CMD:unfreeze_engine", ""), 
            self.CMD_CTRL_data_engine
            )
        
    async def shutdown_data_engine(self) -> None:
        await self.channel.broadcast(
            ("CMD:shutdown_engine", ""), 
            self.CMD_CTRL_data_engine
            )
        
    # ------ Monitor Engine functions ------
    async def start_monitor_engine(self) -> None:
        await self.channel.broadcast(
            ("CMD:start_engine", ""), 
            self.CMD_CTRL_monitor_engine
            )
    async def stop_monitor_engine(self) -> None:
        await self.channel.broadcast(
            ("CMD:stop_engine", ""), 
            self.CMD_CTRL_monitor_engine
            )
        
    async def freeze_monitor_engine(self) -> None:
        await self.channel.broadcast(
            ("CMD:freeze_engine", ""), 
            self.CMD_CTRL_monitor_engine
            )
        
    async def unfreeze_monitor_engine(self) -> None:
        await self.channel.broadcast(
            ("CMD:unfreeze_engine", ""), 
            self.CMD_CTRL_monitor_engine
            )
    async def shutdown_monitor_engine(self) -> None:
        await self.channel.broadcast(
            ("CMD:shutdown_engine", ""), 
            self.CMD_CTRL_monitor_engine
            )
    
    async def set_datafeeds_config(self) -> None:...

# ----- Control Panel Setup
def make_lifespan(cfg_path: str):
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        #cfg_path = os.environ["CONTROL_PANEL_CHANNEL_CFG"]   # or argv, or wherever you want it from
        
        control_panel = ControlPanel(cfg_path)
        await control_panel.start()
        app.state.control_panel = control_panel
        try:
            yield
        finally:
            await control_panel.stop()

    return lifespan
    
def get_control_panel(request: Request) -> ControlPanel:
    return request.app.state.control_panel

def create_app(cfg_path: str) -> FastAPI:
    app = FastAPI(title="Control Channel", version="0.1.0", lifespan=make_lifespan(cfg_path))
    app.include_router(router)   # routes defined on an APIRouter() at module scope, see below
    #app.mount("/static", StaticFiles(directory=os.path.join(base_dir, "static")), name="static")

    return app

API_KEY = "" #os.getenv("CONTROL_API_KEY", "dev-secret")

def require_api_key(x_api_key: Optional[str] = Header(None)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")

router = APIRouter()
base_dir = os.path.dirname(__file__)
templates = Jinja2Templates(directory=os.path.join(base_dir, "templates_ctrl_panel"))

# ----- Control Panel Layout
@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    #active_strategies = await STORE.list_strategies()
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            #"api_key_prefill": os.getenv("CONTROL_API_KEY", "dev-secret"),
            #"strategy_catalog": STRATEGY_CATALOG,
            #"active_strategies": active_strategies,
            #"active_signals": ACTIVE_SIGNALS
        }
    )

async def _feeds_partial(request: Request, control_panel: ControlPanel):
    feeds = control_panel.list_active_datafeeds()
    return templates.TemplateResponse(
        request,
        "partials/feeds.html",
        {"symbols": feeds}
    )

@router.get("/ui/feeds", response_class=HTMLResponse, dependencies=[Depends(require_api_key)])
async def ui_feeds(request: Request, control_panel: ControlPanel = Depends(get_control_panel)):
    return await _feeds_partial(request, control_panel)

# ----- Trade Engine -----
@router.post("/action/cmd/start_trade_engine", response_class=HTMLResponse, dependencies=[Depends(require_api_key)])
async def action_start_trade_engine(
    request: Request, control_panel: ControlPanel = Depends(get_control_panel)
    ) -> None:
    await control_panel.start_trade_engine()
    
@router.post("/action/cmd/stop_trade_engine", response_class=HTMLResponse, dependencies=[Depends(require_api_key)])
async def action_stop_trade_engine(
    request: Request, control_panel: ControlPanel = Depends(get_control_panel)
    ) -> None:
    await control_panel.stop_trade_engine()

@router.post("/action/cmd/freeze_trade_engine", response_class=HTMLResponse, dependencies=[Depends(require_api_key)])
async def action_freeze_trade_engine(
    request: Request, control_panel: ControlPanel = Depends(get_control_panel)
    ) -> None:
    await control_panel.freeze_trade_engine()

@router.post("/action/cmd/unfreeze_trade_engine", response_class=HTMLResponse, dependencies=[Depends(require_api_key)])
async def action_unfreeze_trade_engine(
        request: Request, control_panel: ControlPanel = Depends(get_control_panel)
    ) -> None:
    await control_panel.unfreeze_trade_engine()

@router.post("/action/cmd/shutdown_trade_engine", response_class=HTMLResponse, dependencies=[Depends(require_api_key)])
async def action_shutdown_trade_engine(
        request: Request, control_panel: ControlPanel = Depends(get_control_panel)
    ) -> None:
    await control_panel.shutdown_trade_engine()

@router.post("/action/cmd/goflat", response_class=HTMLResponse, dependencies=[Depends(require_api_key)])
async def action_goflat(
        request: Request, control_panel: ControlPanel = Depends(get_control_panel)
    ) -> None:
    # Send Command to redis DB here
    # Establish new connection and then send the go flat order
    # Doing so will automatically reset the conenction in the trade server, 
    # But the retry function the retry functions there should restart the service immediately

    # make this later-> publish to bus (Redis/Kafka): {"cmd":"GO_FLAT"}
    # check engine states -> then send    
    await control_panel.send_goflat_order()
    
    return templates.TemplateResponse(request, "partials/flash.html", {"msg": "GoFlat sent"})

@router.post("/action/cmd/cancelall", response_class=HTMLResponse, dependencies=[Depends(require_api_key)])
async def action_cancelall(
        request: Request, control_panel: ControlPanel = Depends(get_control_panel)
    ) -> None:
    await control_panel.send_cancelall_order()

    return templates.TemplateResponse(request, "partials/flash.html", {"msg": "CancelAll sent"})

# ----- Data Engine -----
@router.post("/action/cmd/start_data_engine", response_class=HTMLResponse, dependencies=[Depends(require_api_key)])
async def action_start_data_engine(
    request: Request, control_panel: ControlPanel = Depends(get_control_panel)
    ) -> None:
    await control_panel.start_data_engine()
    
@router.post("/action/cmd/stop_data_engine", response_class=HTMLResponse, dependencies=[Depends(require_api_key)])
async def action_stop_data_engine(
    request: Request, control_panel: ControlPanel = Depends(get_control_panel)
    ) -> None:
    await control_panel.stop_data_engine()

@router.post("/action/cmd/freeze_data_engine", response_class=HTMLResponse, dependencies=[Depends(require_api_key)])
async def action_freeze_data_engine(
        request: Request,
        control_panel: ControlPanel = Depends(get_control_panel)
    ) -> None:
    await control_panel.freeze_data_engine()

@router.post("/action/cmd/unfreeze_data_engine", response_class=HTMLResponse, dependencies=[Depends(require_api_key)])
async def action_unfreeze_data_engine(
        request: Request,
        control_panel: ControlPanel = Depends(get_control_panel)
    ) -> None:
    await control_panel.unfreeze_data_engine()

@router.post("/action/cmd/shutdown_data_engine", response_class=HTMLResponse, dependencies=[Depends(require_api_key)])
async def action_shutdown_data_engine(
        request: Request,
        control_panel: ControlPanel = Depends(get_control_panel)
    ) -> None:
    await control_panel.shutdown_data_engine()

@router.post("/action/datafeeds/add", response_class=HTMLResponse, dependencies=[Depends(require_api_key)])
async def action_datafeeds_add(
        request: Request, symbols: str = Form(""),
        control_panel: ControlPanel = Depends(get_control_panel)
    ):
    # add both datafeed for monitor engine, and stream in data engine
    
    # Add symbols to in-process memory
    for s in [x.strip().upper() for x in symbols.split(",") if x.strip()]:
        # Check if the symbol is already in the list, if so, do nothing
        if s not in control_panel.list_active_datafeeds():
            # Add to in-memory database 
            await control_panel.add_datafeed(s)
            
    # return updated feeds partial (HTMX swaps it in place)
    return await _feeds_partial(request, control_panel)

@router.post("/action/datafeeds/remove", response_class=HTMLResponse, dependencies=[Depends(require_api_key)])
async def action_datafeeds_remove(
        request: Request, symbols: str = Form(""),
        control_panel: ControlPanel = Depends(get_control_panel)
    ) -> None:
    # remove both datafeed for monitor engine, and stream in data engine

    for s in [x.strip().upper() for x in symbols.split(",") if x.strip()]:
        # Check if there are strategies currently using these symbols'        
        
        #if s in control_panel.list_active_datafeeds():
        #    print(f"'{s}' is currently in used. ")
        #    raise HTTPException(400, f"'{s} is currently in used.")

        await control_panel.remove_datafeed(s)
        
    return await _feeds_partial(request, control_panel)

@router.post("/action/datafeeds/remove_all", response_class=HTMLResponse, dependencies=[Depends(require_api_key)])
async def action_datafeeds_remove_all(
        request: Request, control_panel: ControlPanel = Depends(get_control_panel)
    ) -> None:
    
    # Check if there are strategies currently using these symbols'        
    #if len(control_panel.list_active_datafeeds()) != 0:
    #    raise HTTPException(400, "Some Data Feeds are still in-use.")

    await control_panel.remove_all_datafeeds()
    
    return await _feeds_partial(request, control_panel)

# ----- Monitor Engine -----
## Choose the buffer parameter you want
## Choose what Stat you want

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


if __name__ == "__main__":
    app = create_app(cfg_path="tests/integration/fixtures/test_redis_channel.toml")
    uvicorn.run(app, host="0.0.0.0", port=8080)