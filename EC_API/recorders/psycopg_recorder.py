#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from typing import Optional, Any, Callable
import asyncpg
from EC_API.recorders.base import SQLSchemaTable, Recorder

class PostgresRecorder(Recorder):
    def __init__(
            self,
            schema: SQLSchemaTable,
            db_address: str, 
            batch_size: int = 100, 
            flush_interval: float=5.0,
            to_row: Optional[Callable[[Any], tuple[Any]]] = None
        ):
        ...
        self._db = None
        
    @property
    def schema(self) ->SQLSchemaTable:
        return
    
    async def start(self):
        ...
        
    async def stop(self):...
    
    async def record(self, msg: Any):
        ...
        
    async def _flush(self):...
    
    