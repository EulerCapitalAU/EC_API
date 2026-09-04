#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jul  3 21:08:40 2026

@author: dexter
"""

class DummyDataServer:
    def __init__(self):
        self.response_logic = None
        self.ws_server = None
    # tape import task
    # streaming ticks base on tape
    # transfer those thorugh the websocket