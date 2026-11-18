import asyncio
from tests.integration.fixtures.trade_engine_cqg import TradeEngineCQG

if __name__ == "__main__":
    engine = TradeEngineCQG(
        usr_cfg = "tests/integration/demo/configs/demo_trade_engine_cqg_cfg.toml", 
        channel_cfg_addr = "tests/integration/demo/configs/demo_redis_channel_docker.toml"
    )
    asyncio.run(engine.run())