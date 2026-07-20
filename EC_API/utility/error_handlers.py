#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 10 22:13:49 2026

@author: dexter
"""
import tomllib
from typing import Iterator
import asyncio
from contextlib import contextmanager
from EC_API.exceptions import (EC_APIError, MsgBuilderError, MsgParserError)


@contextmanager
def msg_io_error_handler(
    output_error: type[EC_APIError],
    timeout_error: type[EC_APIError] = EC_APIError,
) -> Iterator[None]:
    try:
        yield
    except (MsgBuilderError, MsgParserError, TypeError) as e:
        raise output_error(str(e)) from e
    except asyncio.TimeoutError as e:
        raise timeout_error(str(e)) from e
        
@contextmanager 
def toml_loader_error_handler(
    output_error: type[EC_APIError],
    format_error: type[EC_APIError]
) -> Iterator[None]:
    try:
        yield
    except (FileNotFoundError, tomllib.TOMLDecodeError) as e:
        raise output_error(str(e)) from e
    except (KeyError, ValueError) as e:
        raise format_error(str(e)) from e
        
        

