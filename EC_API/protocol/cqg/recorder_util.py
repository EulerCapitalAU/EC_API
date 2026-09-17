#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 17 23:57:10 2026

@author: dexter
"""
from typing import Any
from EC_API.recorders.base import SQLSchemaTable
from EC_API.recorders.sqlite_recorder import _from_dict_to_row
           
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
    ("qty_significand","INTEGER",""),
    ("qty_exponent","INTEGER",""),
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


# to_row functions flatten nested message and extract conddensed info, output a flat dict
def order_status_to_row_default(
        msg: dict[str, Any], schema: SQLSchemaTable
        ) -> tuple[Any]:
    
    res = []
    for col_name, col_typ, col_extra in schema.columns:
        row = msg.get(col_name)
            

def position_status_to_row_default(
        msg: dict[str, Any], schema: SQLSchemaTable
        ) -> tuple[Any]:...


def account_summary_to_row_default(
        msg: dict[str, Any], schema: SQLSchemaTable
        ) -> tuple[Any]:
    return _from_dict_to_row(msg, schema)