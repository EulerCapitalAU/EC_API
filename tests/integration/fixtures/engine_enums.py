#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Aug 23 05:19:28 2026

@author: dexter
"""
from enum import Enum

class EngineState(Enum):
    READY = "Ready"
    RUNNING = "Running"
    FROZEN = "Frozen"
    TERMINATED = "Terminated"
    
ENGINESTATE_LIFECYCLE = {
    EngineState.READY: [
        EngineState.RUNNING,
        EngineState.TERMINATED
        ],
    EngineState.RUNNING: [
        EngineState.FROZEN, 
        EngineState.TERMINATED
        ],
    EngineState.FROZEN: [
        EngineState.RUNNING,
        EngineState.TERMINATED
        ],
    EngineState.TERMINATED: [] # End State    
    }