#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  6 19:40:48 2026

@author: dexter
"""

from enum import Enum
from typing import TypeVar, Generic, Sequence
from EC_API.exceptions import (
    InvalidCurrentStateError,
    InvalidNextStateError,
    StartStateError,
)

T = TypeVar("T", bound=Enum)


class StateMgr(Generic[T]):
    def __init__(
        self,
        trans_map: dict[T, list[T]],
        start: T,
        cur: T | None = None,
        allowed_starts: Sequence[T | None] = [],
    ):
        self.trans_map: dict[T, list[T]] = trans_map
        self.trans_log: list[tuple[T,T]] = list() # !!! Transition log can grow indefinintely, in the next iteration, implement this with maxlen and flushing functions
        # Allowed start pts. If empty, all nodes are allowed
        self._allowed_starts: Sequence[T | None] = allowed_starts
        self.finalised: bool = False
        self.start: Enum = start

        if cur is None:
            self.cur = self.start
        else:
            self.cur = cur

        if self.cur not in self.trans_map:
            raise InvalidCurrentStateError(f"Invalid Current State: {str(self.cur)}.")

        if self._allowed_starts:
            if self.start not in self._allowed_starts:
                raise StartStateError(f"Start State: {str(self.start)} not allowed.")

    def transition_to(self, next_state: T) -> bool:
        if self.finalised:
            return False

        if next_state not in self.trans_map:
            raise InvalidNextStateError(f"Invalid Next State: {str(next_state)}.")

        if next_state not in self.trans_map[self.cur]:
            raise InvalidNextStateError(
                f"State {str(next_state)} is not a valid next state for {str(self.cur)}."
            )

        if not self.trans_map[next_state]:
            self.finalised = True

        self.trans_log.append((self.cur, next_state))
        self.cur = next_state  # assign transition
        return True
