#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 11 20:40:59 2025

@author: dexter
"""

import tomllib
from EC_API.payload.mapping import PRETRADE_RISKCHECK_VENDORS_MAP
from EC_API.exceptions import MissingVendorError


class PreTradeRiskCheck:
    def __init__(self, vedor_name: str):
        if not PRETRADE_RISKCHECK_VENDORS_MAP.get(vedor_name):
            raise MissingVendorError(f"Missing vendor parameter mapping for: {vedor_name}")
        # From universal field name -> Vendor field name
        self.field_map: dict[str, str] = PRETRADE_RISKCHECK_VENDORS_MAP[vedor_name]
        self.global_limits: dict[str, dict] = dict()
        self.symbol_limits: dict[str, dict] = dict()
        self._aliases: dict[str, str] = dict()

    def load(self, path: str) -> None:
        # Disk config loading
        with open(path, mode="rb") as f:
            para = tomllib.load(f)
            self.global_limits = para.get("global_limits", {})
            self._aliases = para.get("aliases", {})
            self.symbol_limits = para.get("symbol_limits", {})

            # make sure aliases can translate from the real name to itself
            for sym in set(self._aliases.values()):
                self._aliases[sym] = sym

    def reload(self, path: str) -> None:
        self.load(path)

    def update(self, symbol: str, para: dict) -> None:
        # In-memory update
        if not isinstance(para, dict):
            raise ValueError(f"Input: {para} has to be a dict to be a valid input.")
        if not self._aliases.get(symbol):
            raise ValueError(f"{symbol} is not found in the aliases list.")

        sym = self._aliases[symbol]
        if sym not in self.symbol_limits.keys():
            raise ValueError(f"Symbol: {symbol} is not found in the setting file.")

        self.symbol_limits[sym] = para

    def static_validate(self, order_info: dict) -> None:
        if "symbol_name" not in order_info:
            raise KeyError("symbol_name not found in the order_info.")

        # check global limit first then symbol_limits
        if self.global_limits:
            for key, val in self.global_limits.items():
                if self.field_map.get(key) is None:
                    raise KeyError(f"Unknown Field in field_map: {key}")
                order_value = order_info.get(self.field_map[key])
                if order_value is None:
                    continue

                if key.endswith("_max"):
                    if order_value > val:
                        raise ValueError(f"{self.field_map[key]}={order_value} exceeds max {val}")
                elif key.endswith("_min"):
                    if order_value < val:
                        raise ValueError(f"{self.field_map[key]}={order_value} below min {val}")

        if not self._aliases.get(order_info["symbol_name"]):
            raise KeyError(
                f"Symbol: {order_info['symbol_name']} is missing in the pre-trade\
                    risk check list"
            )

        sym = self._aliases[order_info["symbol_name"]]
        if self.symbol_limits.get(sym):
            sym_table = self.symbol_limits[sym]
            for key, val in sym_table.items():
                if self.field_map.get(key) is None:
                    raise KeyError(f"Unknown Field in field_map: {key}")

                order_value = order_info.get(self.field_map[key])
                if order_value is None:
                    continue

                if key.endswith("_max"):
                    if order_value > val:
                        raise ValueError(f"{self.field_map[key]}={order_value} exceeds max {val}")
                elif key.endswith("_min"):
                    if order_value < val:
                        raise ValueError(f"{self.field_map[key]}={order_value} below min {val}")
        
    def has_symbol(self, symbol_name: str) -> bool:
        return symbol_name in self._aliases.keys()

class InSessionRiskCheck:
    def __init__(self): ...
