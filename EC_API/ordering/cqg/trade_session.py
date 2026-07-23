#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 31 05:01:41 2026

@author: dexter
"""

import asyncio
import logging
from typing import Optional
from datetime import datetime
from EC_API.transport.cqg.base import TransportCQG
from EC_API.connect.cqg.base import ConnectCQG
from EC_API.ordering.enums import OrderStatus, SubScope
from EC_API.ordering.cqg.enum_mapping import TRADE_SUB_SUCCESS
from EC_API.ordering.cqg.builders import (
    build_trade_subscription_msg,
    build_trade_historical_orders_request_msg,
)
from EC_API.ordering.cqg.parsers import ordering_parsers
from EC_API.recorders.base import Recorder
from EC_API.recorders.null_recorder import NullRecorder
from EC_API.protocol.cqg.parser_util import parse_server_msg
from EC_API.utility.symbol_registry import SymbolRegistry
from EC_API.utility.error_handlers import msg_io_error_handler
from EC_API.exceptions import (
    TradeSessionRequestError,
    TradeSessionTimeOutError,
    TradeSubscriptionMissingError,
    SymbolNotInRegistryError,
    MetaDataMissingError,
    ConnectTimeOutError,
    SymbolResolutionError,
    TransportConnectError,
)
from EC_API._typing import (
    OrderStatusTypeCQG,
    PositionStatusTypeCQG,
    AccountSummaryTypeCQG,
    ContractMetaDataType,
)
from EC_API._typing import (
    OS_CL_ORDER_ID,
    OS_ORDER_ID,
    OS_CHAIN_ORDER_ID,
    PS_CONTRACT_ID,
    ChainOrderId,
    ClOrderId,
    OrderId,
    ContractId,
)

logger = logging.getLogger(__name__)


class TradeSessionCQG:
    def __init__(
        self,
        conn: ConnectCQG,
        recorder: Recorder = NullRecorder()
    ):
        # --- Connect ---
        self._conn = conn
        self._transport: TransportCQG = self._conn.transport

        # --- Event Loop ---
        self._tracker_task: Optional[asyncio.Task] = None
        self._stop_evt: asyncio.Event = self._conn._stop_evt
        
        # --- Logging/Recorder (Audit Log) ---
        self._recorder: Recorder = recorder
        
        # --- Routers ---
        self._exec_stream_router = self._conn._exec_stream_router
        self._pos_status_stream_router = self._conn._pos_status_stream_router
        self._acc_summary_stream_router = self._conn._acc_summary_stream_router

        self._msg_router = self._conn._msg_router

        # --- Symbol Registry ---
        self._symbol_registry: SymbolRegistry = SymbolRegistry()

        # --- Lookup Dictionary ---
        self._active_trade_subs: dict[int, list[SubScope]] = dict()  # trade_sub_id -> scope

        # Logging event pairing
        self.cl_to_chain: dict[ClOrderId, ChainOrderId] = dict()
        self.active_order_ids_by_chain: dict[ChainOrderId, tuple[OrderId, int]] = dict()

        # --- Containers ---
        self._pending_chain_q: list[tuple[ChainOrderId, asyncio.Queue]] = list()

        self._active_order_q: dict[ChainOrderId, asyncio.Queue] = dict()
        self._active_pos_q: dict[ContractId, asyncio.Queue] = dict()
        self._active_acc_summary_q: dict[int, asyncio.Queue] = dict()

        # Snapshots for the latest order_state. overwrite on every event
        self.latest_order_state_by_chain: dict[ChainOrderId, OrderStatusTypeCQG] = (
            dict()
        )  # latest status, overwritten everytime
        self.latest_pos_status_by_contract_id: dict[ContractId, PositionStatusTypeCQG] = dict()
        self.latest_account_summaries: dict[int, AccountSummaryTypeCQG] = dict()

        # Settings
        self.order_statuses_TTL: int = 10  # (Time-to-live in Seconds)

        # States
        self._started: bool = False
        self._stopped: bool = False
        
    # --- Property ---
    @property
    def conn(self):
        return self._conn

    @property
    def state(self):
        return self._conn._state_mgr.cur

    @property
    def timeout(self):
        return self._conn._timeout

    def rid(self) -> int:
        return self.conn.rid()

    # ---- Dunder meothos ---
    async def __aenter__(self):
        if not await self.start():
            await self.stop()
            raise TradeSessionRequestError("Failed to start TradeSessionCQG.")
        return self

    async def __aexit__(self, *args) -> bool:
        await self.stop()
        return False

    # --- Checks
    def has_orders_scope(self) -> bool:
        return any(SubScope.ORDERS in scopes for scopes in self._active_trade_subs.values())

    def has_positions_scope(self) -> bool:
        return any(SubScope.POSITIONS in scopes for scopes in self._active_trade_subs.values())

    # --- Getters
    def get_order_status(self, chain_order_id: str) -> OrderStatusTypeCQG | None:
        return self.latest_order_state_by_chain.get(chain_order_id)

    def get_position_status(self, symbol_name: str) -> PositionStatusTypeCQG | None:
        contract_id = self._symbol_registry.get_contract_ids(symbol_name)
        return self.latest_pos_status_by_contract_id.get(contract_id)

    def get_account_summay(self) -> AccountSummaryTypeCQG | None:
        return self.latest_account_summaries.get(self._conn._account_id)

    # --- status tracker
    async def _tracker_loop(self):
        TERMINAL_STATES = {
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
        }
        # subscribes to exec_stream_router, updates _order_state
        while not self._stop_evt.is_set():
            try:
                await self._conn._trade_work_evt.wait()
                self._conn._trade_work_evt.clear()

                # ---- Order Statuses ----
                while self._pending_chain_q:
                    chain_order_id, q = self._pending_chain_q.pop(0)
                    self._active_order_q[chain_order_id] = q

                # flush updates in queues
                done_ord = set()
                for chain_order_id, ord_q in self._active_order_q.items():
                    while not ord_q.empty():
                        parsed_ord_sts = parse_server_msg(ord_q.get_nowait(), ordering_parsers)

                        for p_ord_sts in parsed_ord_sts:
                            # update cl_order_id for order changes
                            if p_ord_sts.get("order", {}).get("cl_order_id"):
                                chain_order_id = self.cl_to_chain[p_ord_sts["order"]["cl_order_id"]]

                            self.latest_order_state_by_chain[chain_order_id] = p_ord_sts
                            self.active_order_ids_by_chain[chain_order_id] = (
                                p_ord_sts["order_id"],
                                p_ord_sts["status_utc_timestamp"].ToMilliseconds(),
                            )

                            if p_ord_sts.get("status") in TERMINAL_STATES:
                                done_ord.add(chain_order_id)
                                break

                # ---- Position Statuses ----
                done_pos = set()
                for contract_id, pos_q in self._active_pos_q.items():
                    while not pos_q.empty():
                        parsed_pos_sts = parse_server_msg(pos_q.get_nowait(), ordering_parsers)
                        all_done = None
                        for p_pos_sts in parsed_pos_sts:
                            self.latest_pos_status_by_contract_id[contract_id] = p_pos_sts
                            if p_pos_sts.get("open_positions") is None:
                                all_done = True
                            elif len(p_pos_sts["open_positions"]) == 0:
                                all_done = True
                            else:
                                all_done = all(
                                    op_pos["qty"] == 0 for op_pos in p_pos_sts["open_positions"]
                                )

                            if all_done and all_done is not None:
                                done_pos.add(contract_id)
                                break

                # ---- Account Summary ----
                for account_id, acc_summary_q in self._active_acc_summary_q.items():
                    while not acc_summary_q.empty():
                        acc_summary = parse_server_msg(acc_summary_q.get_nowait(), ordering_parsers)
                        for p_acc_summ in acc_summary:
                            self.latest_account_summaries[account_id] = p_acc_summ

                # ---- Cleanup ----
                for chain_order_id in done_ord:
                    q = self._active_order_q.pop(chain_order_id)
                    self._exec_stream_router.unsubscribe(chain_order_id, q)
                    self.active_order_ids_by_chain.pop(chain_order_id)
                    logger.info("Order %s reached terminal state", chain_order_id)

                for contract_id in done_pos:
                    q = self._active_pos_q.pop(contract_id)
                    self._pos_status_stream_router.unsubscribe(contract_id, q)
                    logger.info("Position for contract_id: %s reached terminal state", contract_id)

                #!!! TODO: TTL retirement for snapshots if the scale demands it

            except asyncio.CancelledError as e:
                logger.error(str(e))
                raise
            except TransportConnectError as e:
                logger.error(f"TransportConnectError, reconenction initiated: {str(e)}.")
                # await asyncio.create_task(self._reconnect_loop)
            except Exception as e:
                logger.error("tracker_loop error: %s", e, exc_info=True)

    # --- CQG function calls ---
    async def resolve_symbol(self, symbol_name: str) -> Optional[list[ContractMetaDataType]]:
        try:
            if symbol_name not in self._symbol_registry.sym_to_contract_ids.keys():
                metadatas = await self._conn.resolve_symbol(symbol_name)
                for metadata in metadatas:
                    self._symbol_registry.register(symbol_name, metadata["contract_metadata"])
                return metadatas

            else:
                metadata = self._symbol_registry.get_metadata(symbol_name)
                return [metadata]
        except (SymbolResolutionError, ConnectTimeOutError) as e:
            raise TradeSessionRequestError(str(e))

    async def unsubscribe_symbol(self, symbol_name: str) -> Optional[list[ContractMetaDataType]]:
        try:
            if symbol_name not in self._symbol_registry.sym_to_contract_ids.keys():
                raise SymbolNotInRegistryError(f"Symbol: {symbol_name} is not in the registry.")

            result = await self._conn.unsubscribe_symbol(symbol_name)
            self._symbol_registry.remove_symbol(symbol_name)

            return result
        except (SymbolResolutionError, ConnectTimeOutError) as e:
            raise TradeSessionRequestError(str(e))

    async def trade_subscription_request(
        self, sub_id: int, sub_scope: SubScope
    ) -> tuple[list[dict], list[dict]] | None:
        if sub_id in self._active_trade_subs.keys():
            logger.warning("Sub_id: {sub_id} is already in-use.")
            return None

        with msg_io_error_handler(TradeSessionRequestError, timeout_error=TradeSessionTimeOutError):
            msg = build_trade_subscription_msg(
                sub_id, subscribe=True, sub_scope=sub_scope, skip_orders_snapshot=False
            )

            # wait for three response, trade_sub_Status, snapshot, orderstatus
            sub_status_key = ("sub", "trade_subscription_statuses", "id", sub_id)
            snapshot_key = ("sub", "trade_snapshot_completions", "subscription_id", sub_id)
            fut_sub_status = self._msg_router.register_key(sub_status_key)
            fut_snapshot = self._msg_router.register_key(snapshot_key)

            await self._transport.send(msg)

            sub_status_msg = await asyncio.wait_for(fut_sub_status, timeout=self.timeout)
            snapshot_msg = await asyncio.wait_for(fut_snapshot, timeout=self.timeout)
            parsed_sub_status = parse_server_msg(sub_status_msg, ordering_parsers)
            parsed_snapshot = parse_server_msg(snapshot_msg, ordering_parsers)

            if parsed_sub_status and parsed_sub_status[0].get("status_code") == TRADE_SUB_SUCCESS:
                self._active_trade_subs.setdefault(sub_id, []).append(sub_scope)
            return parsed_sub_status, parsed_snapshot

    async def unsubscribe_trade_request(self, sub_id: int, sub_scope: SubScope) -> list[dict]:
        with msg_io_error_handler(TradeSessionRequestError, timeout_error=TradeSessionTimeOutError):
            if sub_id not in self._active_trade_subs.keys():
                raise TradeSubscriptionMissingError(
                    f"sub_id: {sub_id} is not in the active subscription list"
                )

            msg = build_trade_subscription_msg(
                sub_id, subscribe=False, sub_scope=sub_scope, skip_orders_snapshot=False
            )
            sub_status_key = ("sub", "trade_subscription_statuses", "id", sub_id)
            fut_sub_status = self._msg_router.register_key(sub_status_key)

            await self._transport.send(msg)

            sub_status_msg = await asyncio.wait_for(fut_sub_status, timeout=self.timeout)

            parsed_sub_status = parse_server_msg(sub_status_msg, ordering_parsers)

            if parsed_sub_status and parsed_sub_status[0].get("status_code") == TRADE_SUB_SUCCESS:
                self._active_trade_subs[sub_id].remove(sub_scope)

            return parsed_sub_status

    async def request_historical_orders(
        self, from_date: datetime, to_date: datetime
    ) -> dict[str, str]:
        from_date_timestamp = from_date.timestamp()
        to_date_timestamp = to_date.timestamp()
        rid = self.rid()
        with msg_io_error_handler(TradeSessionRequestError, timeout_error=TradeSessionTimeOutError):
            client_msg = build_trade_historical_orders_request_msg(
                self._conn._account_id, rid, from_date_timestamp, to_date_timestamp
            )
            key = ("info", "information_reports:historical_orders_report", "id", rid)
            fut = self._msg_router.register_key(key)
            await self._transport.send(client_msg)
            server_msg = await asyncio.wait_for(fut, timeout=self.timeout)

            return server_msg

    # --- Lifecycle ---
    async def start(self) -> bool:
        if self._started:
            logger.warning("TradeSessionCQG.start() called in an active session.")
            return True
        
        start_success = self._conn.start()
        
        if not start_success:
            await self._recorder.stop()
            return False
            
        self._tracker_task = asyncio.create_task(self._tracker_loop())
        if start_success:
            await self._recorder.start()
            self._started = True
        return start_success

    async def stop(self) -> bool:
        if (not self._started) or (self._stopped):
            logger.warning("TradeSessionCQG.stop() called without an active session.")
            return False
        
        if self._tracker_task and not self._tracker_task.done():
            self._tracker_task.cancel()
            try:
                await self._tracker_task
            except asyncio.CancelledError:
                pass
        try:    
            await self._cleanup()
        finally:
            stop_success = await self._conn.stop()
            await self._recorder.stop()
            self._stopped, self._started = True, False
            
        return stop_success

    async def _cleanup(self) -> None:
        # Automatically unsubscirbe and destroy the queue copy of the
        # symbol in stream
        active_symbols = self._symbol_registry.active_symbols
        if not active_symbols:
            return

        for sym in list(active_symbols):
            try:
                await self._conn.unsubscribe_symbol(sym)
                self._symbol_registry.remove_symbol(sym)
                self._symbol_registry.remove_metadata(sym)
            except (SymbolNotInRegistryError, MetaDataMissingError, ConnectTimeOutError) as e:
                logger.warning(str(e))
