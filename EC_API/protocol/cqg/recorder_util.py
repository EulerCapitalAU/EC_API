#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 17 23:57:10 2026

@author: dexter
"""
from typing import Any
from EC_API.recorders.base import SQLSchemaTable, _from_dict_to_row
from EC_API.utility.symbol_registry import SymbolRegistry
from EC_API.exceptions import (
    RowConversionError, 
    SymbolNotInRegistryError
    )

ORD_STS_COLS = (
    # ---- order statuses fields
    ("account_id", "INTEGER", "NOT NULL"),
    ("order_id", "TEXT", "NOT NULL"),
    ("chain_order_id", "TEXT", "NOT NULL"),
    ("status", "TEXT", "NOT NULL"),
    ("status_utc_timestamp","INTEGER", "NOT NULL"),
    ("submission_utc_timestamp", "INTEGER", "NOT NULL"),
    ("fill_cnt", "INTEGER", "NOT NULL"),
    ("scaled_avg_fill_price", "INTEGER","NOT NULL"),
    ("avg_fill_price_correct","REAL","NOT NULL"),
    # ---- order fields
    ("cl_order_id", "TEXT", ""),
    ("contract_id", "INTEGER", ""),
    ("symbol_name", "TEXT", ""),
    ("order_type", "TEXT", ""),
    ("duration", "TEXT", ""),
    ("side", "TEXT", ""),
    ("scaled_limit_price", "INTEGER", ""),
    ("scaled_stop_price","INTEGER",""),
    ("qty","INTEGER",""),
    )

POS_STS_COLS = (
    ("account_id", "INTEGER", "NOT NULL"),         
    ("contract_id", "INTEGER", "NOT NULL"),
    ("symbol_name", "TEXT", ""),
    # ---- open positions fields
    ("id", "INTEGER", "NOT NULL"),                   
    ("price_correct", "REAL", "NOT NULL"),
    ("trade_date", "INTEGER", "NOT NULL"),
    ("statement_date", "INTEGER", "NOT NULL"),
    ("is_aggregated", "INTEGER", "NOT NULL"),       
    ("is_short", "INTEGER", "NOT NULL"),            
    ("qty", "REAL", ""),                            
    )

ACC_SUMM_COLS = (
    ("account_id","INTEGER", "NOT NULL"),
    ("currency","TEXT",""),
    ("purchasing_power", "REAL", "")
    )

# Message transformation functions
def flatten_order_status(
        msg: dict[str, Any], symbol_registry: SymbolRegistry
        ) -> dict[str, Any]:
    order_sub = msg.get("order") or {}
    contract_id = order_sub.get("contract_id")
    try:
        symbol_name = (
            symbol_registry.get_symbol_name(contract_id) if contract_id is not None else None
        )
    except SymbolNotInRegistryError:
        symbol_name = None

    row_msg = dict(msg)
    row_msg.pop("order", None)
    row_msg.update(order_sub)
    if row_msg.get("qty") is not None:
        row_msg["qty"] = int(row_msg["qty"])
    row_msg["symbol_name"] = symbol_name
    return row_msg

def flatten_position_status(
        msg: dict[str, Any], symbol_registry: SymbolRegistry
    ) -> list[dict[str, Any]]:
    open_positions = msg.get("open_positions") or []
    if not open_positions:
        return []

    contract_id = msg.get("contract_id")
    try:
        symbol_name = (
            symbol_registry.get_symbol_name(contract_id) if contract_id is not None else None
        )
    except SymbolNotInRegistryError:
        symbol_name = None

    account_id = msg.get("account_id")
    return [
        {"account_id": account_id, 
         "contract_id": contract_id, 
         "symbol_name": symbol_name, 
         **op_pos,
         "qty": int(op_pos["qty"])}
        for op_pos in open_positions
    ]

# to_row functions flatten nested message and extract conddensed info, output a flat dict
def order_status_to_row_default(
        msg: dict[str, Any], schema: SQLSchemaTable
        ) -> tuple[Any]:
    
    #res = []
    #for col_name, col_typ, col_extra in schema.columns:
    #    row = msg.get(col_name)
        
    return _from_dict_to_row(msg, schema)            

def position_status_to_row_default(
        msg: dict[str, Any], schema: SQLSchemaTable
        ) -> tuple[Any]:
    return _from_dict_to_row(msg, schema)

def account_summary_to_row_default(
        msg: dict[str, Any], schema: SQLSchemaTable
        ) -> tuple[Any]:
    return _from_dict_to_row(msg, schema)