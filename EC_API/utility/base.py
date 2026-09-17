#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug  7 09:44:04 2025

@author: dexter
"""

import random
import time
import pickle
from decimal import Decimal
from typing import Callable


def random_string(length: int = 16) -> str:
    """
    Generate a random x-digits alphanumerical string

    Parameters
    ----------
    length : int
        The number of digits.

    Returns
    -------
    Shortcode.

    """
    base = "0123456789"

    # Pick a random x (6) digits string from base
    # The amount to (56,800,235,584) possibilities
    code = "".join([random.choice(base) for i in range(length)])
    return code


def float_to_significand_exponent():
    pass


def foo(value: Decimal):
    # Normalize removes trailing zeros (e.g., 1.2500 -> 1.25)
    # which keeps the significand as small as possible.
    normalized = value.normalize()

    # as_tuple() returns (sign, digits_tuple, exponent)
    # sign: 0 for +, 1 for -
    # digits: e.g., (1, 2, 5)
    # exponent: e.g., -2
    sign, digits, exponent = normalized.as_tuple()

    # Reconstruct the integer significand from the digits tuple
    significand = int("".join(map(str, digits)))

    # Apply the sign
    if sign:
        significand = -significand

    return significand, exponent


def to_significand_sint64_exponent_sint32(n: int | float):
    """
    Converts an integer into an integer significand (sint64)
    and an exponent (sint32) such that: n = significand * 10^exponent.
    """
    d = Decimal(str(n)).normalize()
    sign, digits, exponent = d.as_tuple()

    # Checks
    if not isinstance(exponent, int):
        raise ValueError(f"Unexpected special Decimal value: exponent={exponent}")

    if not (-(2**31) <= exponent < 2**31):
        raise ValueError("Exponent exceeds sint32 range")

    # Reconstruct integer significand from the digits tuple
    significand = int("".join(map(str, digits)))
    if sign:  # 1 is negative in Decimal tuple
        significand = -significand

    # Your Bounds Checks (Keep these, they are great!)
    if not (-(2**63) <= significand < 2**63):
        raise ValueError("Significand exceeds sint64 range")
    if not (-(2**31) <= exponent < 2**31):
        raise ValueError("Exponent exceeds sint32 range")

    return significand, exponent

def significand_exponent_to_decimal(significand: int, exponent: int) -> Decimal:
    # inverse of to_significand_sint64_exponent_sint32: value = significand * 10**exponent
    return Decimal(significand).scaleb(exponent)

def time_it(func: Callable) -> Callable:
    # simple timing function
    def wrapper(*args, **kwargs):
        t1 = time.time()
        result = func(*args, **kwargs)
        t2 = time.time() - t1
        print(f"{func.__name__!r} ran in {t2:.4f} seconds.")
        return result

    return wrapper


def save_csv(savefilename: str, save_or_not: bool = True) -> Callable:
    def decorator(func):
        def wrapper(*args, **kwargs):
            data = func(*args, **kwargs)
            if save_or_not:
                data.to_csv(savefilename, index=False)
                return data
            elif not save_or_not:
                return data

        return wrapper

    return decorator


def pickle_save(savefilename: str, save_or_not: bool = True) -> Callable:
    def decorator(func):
        def wrapper(*args, **kwargs):
            data = func(*args, **kwargs)
            if save_or_not:
                with open(savefilename, "wb") as file:
                    pickle.dump(data, file)
            return data

        return wrapper

    return decorator


def load_pkl(filename: str):  # test function
    output = open(filename, "rb")
    my_pkl = pickle.load(output)
    print("File:{} is loaded.".format(filename))
    output.close()
    return my_pkl
