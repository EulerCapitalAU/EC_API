#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct  3 01:57:36 2026

@author: dexter
"""

from EC_API.connect.enums import ConnectionState

def check_if_logoffed(trans_log: list[tuple[ConnectionState, ConnectionState]]) -> bool:
    for prev, nxt in reversed(trans_log):
        if prev == ConnectionState.CONNECTED_LOGON:
            return nxt == ConnectionState.CONNECTED_LOGOFF
    return True  # no logon ever recorded — nothing to log off, not a failure