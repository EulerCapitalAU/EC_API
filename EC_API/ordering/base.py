#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 29 13:19:48 2025

@author: dexter
"""

from typing import Protocol, Any, Optional
from EC_API.ordering.enums import RequestType
from EC_API.ordering.trade_session import TradeSession


class LiveOrder(Protocol):
    _trade_session: TradeSession

    @property
    def timeout(self) -> float | int: ...

    def rid(self) -> int: ...

    async def send(self, request_type: RequestType, request_details: dict, **kwargs) -> Optional[list[dict[str,Any]]]: ...
