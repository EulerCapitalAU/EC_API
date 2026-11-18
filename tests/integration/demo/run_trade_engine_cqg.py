import asyncio
from tests.integration.fixtures.trade_engine_cqg import TradeEngineCQG

async def main():
    engine = TradeEngineCQG(
        usr_cfg = "tests/integration/demo/configs/demo_trade_engine_cqg_cfg.toml", 
        channel_cfg_addr = "tests/integration/demo/configs/demo_redis_channel_docker.toml",
        pretraderisk_cfg_addr = "tests/integration/demo/configs/pretraderisk.toml"
    )
    await engine.run()
    
if __name__ == "__main__":
    asyncio.run(main())