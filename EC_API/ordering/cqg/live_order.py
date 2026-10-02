#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 30 10:01:16 2025

@author: dexter
"""

import asyncio
from typing import Any, Optional

from EC_API.transport.cqg.base import TransportCQG
from EC_API.transport.routers import MessageRouter, StreamRouter
from EC_API.connect.cqg.base import ConnectCQG
from EC_API.ordering.enums import RequestType
from EC_API.ordering.base import LiveOrder
from EC_API.ordering.cqg.trade_session import TradeSessionCQG
from EC_API.ordering.cqg.builders import (
    build_new_order_request_msg,
    build_modify_order_request_msg,
    build_cancel_order_request_msg,
    build_activate_order_request_msg,
    build_liquidateall_order_request_msg,
    build_cancelall_order_request_msg,
    build_goflat_order_request_msg,
)
from EC_API.ordering.cqg.parsers import ordering_parsers
from EC_API.protocol.cqg.parser_util import parse_server_msg
from EC_API.utility.error_handlers import msg_io_error_handler
from EC_API.exceptions import (
    TradeSubscriptionMissingError,
    MissingSymbolResolutionError,
    LiveOrderRequestError,
    LiveOrderTimeOutError,
    MissingOrderIDError,
)


class LiveOrderCQG(LiveOrder):
    # a class that control the ordering action to the exchange
    # This object is specific for CQG type request
    def __init__(
        self,
        trade_session: TradeSessionCQG,
        timeout: int | float = 1,
    ):
        # Message Routing
        self._trade_session: TradeSessionCQG = trade_session
        self._conn: ConnectCQG = self._trade_session._conn
        self._transport: TransportCQG = self._conn.transport

        self.msg_router: MessageRouter = self._conn._msg_router
        self.stream_router: StreamRouter = self._conn._exec_stream_router

    # ---
    @property
    def timeout(self):
        return self._conn._timeout

    def rid(self):
        return self._conn.rid()

    # --- CQG function calls
    async def _new_order_request(self, request_details: dict[str, Any]) -> list[dict]:
        with msg_io_error_handler(LiveOrderRequestError, timeout_error=LiveOrderTimeOutError):
            client_msg = build_new_order_request_msg(**request_details)
            confirm_key = (
                "order_confirm",
                "order_statuses",
                "cl_order_id",
                request_details["cl_order_id"],
            )
            reject_key = (
                "rpc_reqid",
                "order_request_rejects",
                "request_id",
                request_details["request_id"],
            )

            fut = self.msg_router.register_racing_keys([confirm_key, reject_key])
            await self._transport.send(client_msg)

            server_msg = await asyncio.wait_for(fut, timeout=self.timeout)
            return parse_server_msg(server_msg, ordering_parsers)

    async def _modify_order_request(
        self,
        request_details: dict[str, Any],
    ) -> list[dict]:
        with msg_io_error_handler(LiveOrderRequestError, timeout_error=LiveOrderTimeOutError):
            client_msg = build_modify_order_request_msg(**request_details)

            reject_key = (
                "rpc_reqid",
                "order_request_rejects",
                "request_id",
                request_details["request_id"],
            )
            confirm_key = (
                "order_confirm",
                "order_statuses",
                "order_id",
                request_details["order_id"],
            )

            fut = self.msg_router.register_racing_keys([confirm_key, reject_key])
            await self._transport.send(client_msg)

            server_msg = await asyncio.wait_for(fut, timeout=self.timeout)
            return parse_server_msg(server_msg, ordering_parsers)

    async def _cancel_order_request(self, request_details: dict[str, Any], **kwargs) -> list[dict]:
        with msg_io_error_handler(LiveOrderRequestError, timeout_error=LiveOrderTimeOutError):
            client_msg = build_cancel_order_request_msg(**request_details)

            reject_key = (
                "rpc_reqid",
                "order_request_rejects",
                "request_id",
                request_details["request_id"],
            )
            confirm_key = (
                "order_confirm",
                "order_statuses",
                "order_id",
                request_details["order_id"],
            )

            fut = self.msg_router.register_racing_keys([confirm_key, reject_key])
            await self._transport.send(client_msg)

            server_msg = await asyncio.wait_for(fut, timeout=self.timeout)
            return parse_server_msg(server_msg, ordering_parsers)

    async def _activate_order_request(
        self,
        request_details: dict[str, Any],
    ) -> list[dict]:
        with msg_io_error_handler(LiveOrderRequestError, timeout_error=LiveOrderTimeOutError):
            client_msg = build_activate_order_request_msg(**request_details)

            reject_key = (
                "rpc_reqid",
                "order_request_rejects",
                "request_id",
                request_details["request_id"],
            )
            confirm_key = (
                "order_confirm",
                "order_statuses",
                "order_id",
                request_details["order_id"],
            )

            fut = self.msg_router.register_racing_keys([confirm_key, reject_key])

            await self._transport.send(client_msg)
            server_msg = await asyncio.wait_for(fut, timeout=self.timeout)
            return parse_server_msg(server_msg, ordering_parsers)

    async def _cancelall_order_request(
        self,
        request_details: dict[str, Any],
    ) -> list[dict]:
        with msg_io_error_handler(LiveOrderRequestError, timeout_error=LiveOrderTimeOutError):
            client_msg = build_cancelall_order_request_msg(**request_details)

            ack_key = (
                "rpc_reqid",
                "order_request_acks",
                "request_id",
                request_details["request_id"],
            )
            reject_key = (
                "rpc_reqid",
                "order_request_rejects",
                "request_id",
                request_details["request_id"],
            )

            fut = self.msg_router.register_racing_keys([ack_key, reject_key])

            await self._transport.send(client_msg)
            server_msg = await asyncio.wait_for(fut, timeout=self.timeout)
            return parse_server_msg(server_msg, ordering_parsers)

    async def _liquidateall_order_request(
        self,
        request_details: dict[str, Any],
    ) -> list[dict]:
        with msg_io_error_handler(LiveOrderRequestError, timeout_error=LiveOrderTimeOutError):
            client_msg = build_liquidateall_order_request_msg(**request_details)

            ack_key = (
                "rpc_reqid",
                "order_request_acks",
                "request_id",
                request_details["request_id"],
            )
            reject_key = (
                "rpc_reqid",
                "order_request_rejects",
                "request_id",
                request_details["request_id"],
            )

            fut = self.msg_router.register_racing_keys([ack_key, reject_key])

            await self._transport.send(client_msg)
            server_msg = await asyncio.wait_for(fut, timeout=self.timeout)
            return parse_server_msg(server_msg, ordering_parsers)

    async def _goflat_order_request(
        self,
        request_details: dict[str, Any],
    ) -> list[dict]:
        with msg_io_error_handler(LiveOrderRequestError, timeout_error=LiveOrderTimeOutError):
            client_msg = build_goflat_order_request_msg(**request_details)

            goflat_key = (
                "rpc_reqid",
                "go_flat_statuses",
                "request_id",
                request_details["request_id"],
            )
            reject_key = (
                "rpc_reqid",
                "order_request_rejects",
                "request_id",
                request_details["request_id"],
            )

            fut = self.msg_router.register_racing_keys([goflat_key, reject_key])
            await self._transport.send(client_msg)

            server_msg = await asyncio.wait_for(fut, timeout=self.timeout)
            return parse_server_msg(server_msg, ordering_parsers)

    # --- Function call ---
    async def send(
        self, request_type: RequestType, request_details: dict, **kwargs
    ) -> Optional[list[dict[str,Any]]]:
        
        # --- Symbol checks
        if request_type not in (
            RequestType.CANCELALL_ORDER,
            RequestType.GOFLAT_ORDER,
            ):
            # Get the Inputs
            if not request_details.get("symbol_name"):
                raise KeyError('"symbol_name" is missing in the request_details.')
            symbol = request_details["symbol_name"]
    
            # Check Symbol resolution
            if not self._trade_session._symbol_registry.has_symbol(symbol):
                raise MissingSymbolResolutionError(f"Symbol: {symbol} is not in the registry.")
    
            contract_id = self._trade_session._symbol_registry.get_contract_ids(symbol)
            request_details.pop("symbol_name")

        # --- Check Trade ID
        if not self._trade_session.has_orders_scope():
            raise TradeSubscriptionMissingError(
                "Trade subscription has to be done before sending order."
            )

        # --- OrderID checks
        if request_type not in (
            RequestType.NEW_ORDER,
            RequestType.CANCELALL_ORDER,
            RequestType.LIQUIDATEALL_ORDER,
            RequestType.GOFLAT_ORDER,
        ):
            if not any(
                oid == request_details["order_id"]
                for oid, _ in self._trade_session.active_order_ids_by_chain.values()
            ):
                raise MissingOrderIDError(
                    f"Order ID: {request_details['order_id']} is not in active orders."
                )
        # --- message building
        details = {
            "account_id": self._trade_session._conn._account_id,
            "request_id": self.rid(),
            # "contract_id": contract_id,
            **request_details,
        }

        match request_type:
            case RequestType.NEW_ORDER:
                details["contract_id"] = contract_id

                parsed_server_msg = await self._new_order_request(details)
                for p_s_msg in parsed_server_msg:
                    if p_s_msg.get("chain_order_id"):
                        chain_order_id = p_s_msg["chain_order_id"]
                        self._trade_session.cl_to_chain[request_details["cl_order_id"]] = (
                            chain_order_id
                        )

                        q = self.stream_router.subscribe(chain_order_id)

                        self._trade_session._pending_chain_q.append((chain_order_id, q))
                        self._conn._trade_work_evt.set()
            # ---
            case RequestType.MODIFY_ORDER:
                parsed_server_msg = await self._modify_order_request(details)

            case RequestType.CANCEL_ORDER:
                parsed_server_msg = await self._cancel_order_request(details)

            case RequestType.ACTIVATE_ORDER:
                parsed_server_msg = await self._activate_order_request(details)

            # ---
            case RequestType.CANCELALL_ORDER:
                parsed_server_msg = await self._cancelall_order_request(details)

            case RequestType.LIQUIDATEALL_ORDER:
                details["contract_id"] = contract_id
                parsed_server_msg = await self._liquidateall_order_request(details)

            case RequestType.GOFLAT_ORDER:
                parsed_server_msg = await self._goflat_order_request(details)
        return parsed_server_msg
