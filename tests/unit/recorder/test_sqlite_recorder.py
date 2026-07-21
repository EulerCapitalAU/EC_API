# -*- coding: utf-8 -*-
import time
import asyncio
import pytest
import aiosqlite
from EC_API.recorders.base import SQLSchemaTable
from EC_API.recorders.sqlite_recorder import SQLiteRecorder

@pytest.fixture
def schema() -> SQLSchemaTable:
    schema = SQLSchemaTable(
        table_name = "test_table",
        columns = (("field_0", "INTEGER"), ("field_1", "TEXT"))
        )
    return schema

@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "audit.db")

async def _count_rows(db_path: str):
    async with aiosqlite.connect(db_path) as db:
        res = await db.execute("SELECT COUNT(*) FROM test_table")
        return (await res.fetchone())[0]


def test_sqlite_recorder_init_valid(schema, db_path) -> None:
    recorder = SQLiteRecorder(
        schema, 
        db_address = db_path,
        batch_size = 2, 
        flush_interval = 0.5
        )

    assert recorder.schema is schema

@pytest.mark.asyncio
async def test_flush_triggers_at_batch_size(schema, db_path) -> None:
    recorder = SQLiteRecorder(schema, db_address=db_path, batch_size=3)
    await recorder.start()

    await recorder.record({"field_0": 1, "field_1": "a"})
    await recorder.record({"field_0": 2, "field_1": "b"})
    assert await _count_rows(db_path) == 0  # below threshold, not flushed yet

    await recorder.record({"field_0": 3, "field_1": "c"})
    assert await _count_rows(db_path) == 3  # threshold crossed and flushed
    await recorder.stop()
   
@pytest.mark.asyncio
async def test_flush_triggers_at_time_interval(schema, db_path) -> None:
    recorder = SQLiteRecorder(
        schema, db_address=db_path, 
        batch_size=10, flush_interval=0.05) # 2 seconds flush
    await recorder.start()
    await recorder.record({"field_0": 1, "field_1": "a"})
    await recorder.record({"field_0": 2, "field_1": "b"})
    assert await _count_rows(db_path) == 0  
    
    time.sleep(0.2)
    await recorder.record({"field_0": 3, "field_1": "c"})

    assert await _count_rows(db_path) == 3
    await recorder.stop()
    
@pytest.mark.asyncio
async def test_flush_triggers_at_stop(schema, db_path) -> None:
    recorder = SQLiteRecorder(
        schema, db_address=db_path, 
        batch_size=10, flush_interval=0.05) # 2 seconds flush
    await recorder.start()
    await recorder.record({"field_0": 1, "field_1": "a"})
    await recorder.record({"field_0": 2, "field_1": "b"})
    assert await _count_rows(db_path) == 0  
    await recorder.stop()
    assert await _count_rows(db_path) == 2
    
# ---- Vendor-specific data types tests
