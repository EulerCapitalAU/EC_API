#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 15 05:02:43 2026

@author: dexter
"""

import cProfile
import pstats
import pytest

with cProfile.Profile() as pr:
    pytest.main(["tests/unit/"])  # -x stops on first fail

stats = pstats.Stats(pr)
stats.sort_stats("tottime")
stats.print_stats("EC_API/EC_API")