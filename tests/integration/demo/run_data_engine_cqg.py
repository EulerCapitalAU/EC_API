#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 18 18:58:18 2026

@author: dexter
"""
import asyncio
from tests.integration.fixtures.data_engine_cqg import DataEngineCQG

if __name__ == "__main__":
    engine = DataEngineCQG(
        usr_cfg = "tests/integration/demo/configs/demo_data_engine_cqg_cfg.toml", 
        channel_cfg_addr = "tests/integration/demo/configs/demo_redis_channel_docker.toml"
    )
    asyncio.run(engine.run())