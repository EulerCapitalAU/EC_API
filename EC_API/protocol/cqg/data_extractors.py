#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 26 00:41:15 2026

@author: dexter
"""
from typing import Callable, Any, Sequence
from EC_API._typing import (
    Extractor_func, KeyHit, 
    ParsedRTMDCQG,
    QuotesValueTypeCQG,
    Q_CONTRACT_ID, # [0]  contract_id              int
    Q_TYPE,  # [1]  type                     int
    Q_UTC_TIME,  # [2]  quote_utc_time           int   (microseconds since epoch)
    Q_SCALED_PRICE,  # [3]  scaled_price             int
    Q_SCALED_SOURCE_PRICE,  # [4]  scaled_source_price      int
    Q_PRICE_YIELD, # [5]  price_yield              float
    Q_VOL_SIGNIFICAND,  # [6]  vol_significand          int
    Q_VOL_EXPONENT,  # [7]  vol_exponent             int
    Q_INDICATORS,  # [8]  indicators               tuple[int, ...]
    Q_SALES_CONDITION,  # [9]  sales_condition          int
    Q_TRADE_ATTRS,  # [10] trade_attributes         tuple (TradeAttrsCQG)
    Q_SCALED_CURRENCY_RATE_PRICE,  # [11] scaled_currency_rate     int
    Q_SCALED_PREMIUM,  # [12] scaled_premium           int
    Q_MARKET_STATE,  # [13] market_state             tuple (MarketStateCQG)
    Q_CORRECT_PRICE_SCALE,  # [14] correct_price_scale      float
    Q_IS_SNAPSHOT  # [15] is_snapshot              bool
)

data_extractors: dict[str, Extractor_func] = {}

def register_extractor(policy_name: str):
    # decorator for registering extractors functions
    def decorator(func: Callable[..., None]):
        data_extractors[policy_name] = func
        return func
    return decorator


# --- Parsed Real-Time Market Data ---
#@register_extractor("simple_trade")
#def extract_trade_data(parsed_rtmd: ParsedRTMDCQG) -> Sequence[tuple[Any]]:
#    ...
    
    