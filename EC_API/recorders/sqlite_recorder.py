#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 17 03:11:13 2026

@author: dexter
"""
import time
import json
from typing import Optional, Any, Callable
import sqlite3
import aiosqlite
from EC_API.recorders.base import (
    SQLSchemaTable, 
    Recorder, 
    _from_dict_to_row
    )
from EC_API.recorders.error_policies import RecorderErrorPolicy
from EC_API.exceptions import (
    RowConversionError, 
    RecorderOperationalError, 
    RecorderCriticalError
    )



class SQLiteRecorder(Recorder):
    def __init__(
            self, 
            schema: SQLSchemaTable,
            db_address: str, 
            policy: RecorderErrorPolicy = RecorderErrorPolicy.DROP,
            batch_size: int = 100, 
            flush_interval: float=5.0,
            to_row: Optional[Callable[[dict[str, Any], SQLSchemaTable], tuple[Any]]] = None
        ):
        # DB setup_insert_rejected_query
        self._schema: SQLSchemaTable = schema
        self._db_address: str = db_address
        self._db: Optional[aiosqlite.Connection] = None
        self._rejected_schema: Optional[SQLSchemaTable] = None
        
        # Recorder Config
        self._policy: RecorderErrorPolicy = policy
        self._batch_size: int = batch_size
        self._flush_interval: float = flush_interval #seconds
        
        # message format for logging
        # Conversion from raw message to DB format
        self._to_row: Callable[[dict[str, Any], SQLSchemaTable], tuple[Any]] = to_row if to_row is not None else _from_dict_to_row
        
        # SQL commands
        self._insert_query: str = self._schema.insert_query('sqlite3')
        
        # Containers
        self._buf: list[Any] = list()
        self._rejected: list[Any] = list()
        
    @property
    def schema(self) -> SQLSchemaTable:
        return self._schema
    
    def _rejected_schema_init(self) -> None:
        reject_cols = (
            ("seq", "INTEGER", "PRIMARY KEY AUTOINCREMENT"),
            ("ts_ns", "INTEGER", "NOT NULL"), 
            ("raw", "TEXT", "NOT NULL")
            )
        self._rejected_schema = SQLSchemaTable(
            f"{self.schema.table_name}_rejected", reject_cols, strict=True
            )

    async def start(self) -> None:
        try:
            self._db = await aiosqlite.connect(self._db_address)
            await self._db.execute("PRAGMA journal_mode=WAL") # WAL
            await self._db.execute("PRAGMA synchronous=NORMAL") 
            await self._db.execute(self._schema.create_query())
            
            if self._policy is RecorderErrorPolicy.DROP:
                self._rejected_schema_init()
                if self._rejected_schema is None:
                    raise RecorderCriticalError("Failed to initialize the rejected-rows schema.")
                    
                await self._db.execute(self._rejected_schema.create_query())
            await self._db.commit()
            
        except sqlite3.Error as e:
            raise RecorderCriticalError(str(e)) from e
            
        self._last_flush = time.monotonic()

    async def stop(self) -> None:
        if self._db is None:
            return
        try:
            await self._flush()
        finally:
            try:
                await self._db.close()
            except sqlite3.Error as e:
                raise RecorderCriticalError(str(e)) from e

    async def record(self, msg: Any) -> None:
        if self._db is None:
            raise RecorderCriticalError("DB connection is not established.")
        
        try:
            row = self._to_row(msg, self._schema)
            self._buf.append(row)
        except RowConversionError:
            row = (time.time_ns(), json.dumps(msg))
            self._rejected.append(row)
            if self._policy is RecorderErrorPolicy.PROPAGATE:
                raise
        # Note that this is a lazy check. Only upon a call does the recorder
        # flush the messages in the buffer.
        if (len(self._buf)>=self._batch_size or 
            time.monotonic() - self._last_flush >= self._flush_interval or
            len(self._rejected)>=self._batch_size
            ):
            await self._flush()
        
    async def _flush(self) -> None:
        if self._db is None:
            raise RecorderOperationalError("_flush() called without an established DB connection.") 
        
        try:
            if self._buf:
                await self._db.executemany(self._insert_query, self._buf)
            
            if self._policy is RecorderErrorPolicy.DROP:
                
                if self._rejected_schema is  None:
                    raise RecorderOperationalError("_flush() called before start() initialized the rejected-rows schema.")
                
                if self._rejected:
                    await self._db.executemany(
                        self._rejected_schema.insert_query('sqlite3'), self._rejected
                        )
                
            await self._db.commit()
            self._rejected.clear()
            self._buf.clear()

        except sqlite3.OperationalError as e:
            await self._db.rollback()
            raise RecorderOperationalError(str(e)) from e
        except sqlite3.IntegrityError as e:
            await self._db.rollback()
            raise RecorderCriticalError(str(e)) from e
        except sqlite3.ProgrammingError as e:
            await self._db.rollback()
            raise RecorderOperationalError(str(e)) from e
        except (OverflowError, sqlite3.Error) as e:
            await self._db.rollback()
            raise RecorderCriticalError(str(e)) from e
        finally:
            self._last_flush = time.monotonic()

            
    