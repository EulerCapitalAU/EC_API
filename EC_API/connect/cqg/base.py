#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 18 11:34:22 2025

@author: dexter
"""

import asyncio
from collections import deque
from typing import Optional, Any, cast
import warnings
import logging
from datetime import datetime, timezone

from EC_API.ext.WebAPI import webapi_client
from EC_API.connect.base import Connect
from EC_API.connect.enums import ConnectionState
from EC_API.connect.latency_util import (
    latency_above_threshold, 
    jitter_above_threshold
)
from EC_API.transport.cqg.base import TransportCQG
from EC_API.transport.routers import MessageRouter, StreamRouter
from EC_API.protocol.cqg.router_util import (
    extract_router_keys,
    server_msg_type,
    is_realtime_tick,
    is_order_update_stream,
    is_position_statuses_stream,
    is_account_summary_statuses_stream,
    is_ping,
    split_server_msg,
)
from EC_API.protocol.cqg.parser_util import parse_server_msg
from EC_API.connect.cqg.builders import (
    build_logon_msg,
    build_logoff_msg,
    build_ping_msg,
    build_pong_msg,
    build_resolve_symbol_msg,
    build_restore_msg,
)
from EC_API.connect.enums import CONNECT_STATES_LIFECYCLE
from EC_API.connect.cqg.enum_mapping import (
    CONN_LOGON_RESCODE_CQG2INT,
    CONN_RESTORE_RESCODE_CQG2INT,
    CONN_LOGOFF_RESCODE_CQG2INT,
)
from EC_API.connect.cqg.parsers import (
    parse_logon_result,
    parse_restore_or_join_session_result,
    parse_logged_off,
    parse_pong,
    connect_parsers,
)
from EC_API.utility.state_mgr import StateMgr
from EC_API.ext.WebAPI.webapi_2_pb2 import ServerMsg
from EC_API.utility.error_handlers import msg_io_error_handler
from EC_API.exceptions import (
    TransportConnectError,
    TransportDisconnectError,
    ConnectRequestError,
    ConnectEnterError,
    # ConnectCancelledError,
    ConnectTimeOutError,
    KeyExtractorError,
    SymbolResolutionError,
)
from EC_API._typing import (
    PongType, 
    PONG_PINGTIME, 
    PONG_PONGTIME
    )


logger = logging.getLogger(__name__)


class ConnectCQG(Connect):
    """
    The connection Layer fasciliate communications between our local
    applications and the CQG Server.

    It uses a TransportCQG object that
    manage synchronous messainging via websocket. ConnectCQG the extract
    the server messages asynchronously from the in-queue and place
    client messages schronously in the out-queue within the transport
    layer.

    High-level Services, such as MonotorData or TradeSession build
    on top of the connection layer to perform specific functions.

    Session-specific function calls to CQG are also in this class.
    """

    def __init__(
        self,
        host_name: str,
        user_name: str,
        password: str,
        account_id: int = 0,
        immediate_connect: bool = False,
        client: Optional[webapi_client.WebApiClient] = None,
        transport: Optional[TransportCQG] = None,
    ):
        # Inputs
        self._host_name = host_name
        self._user_name = user_name
        self._password = password
        self._account_id = account_id

        # ---- Sessions para ----
        self.session_token: Optional[str] = None
        self.client_app_id: Optional[str] = None
        self.protocol_version_major: Optional[int] = None
        self.protocol_version_minor: Optional[int] = None

        # ---- Event Loop ----
        self._loop = asyncio.get_running_loop()
        self._router_task: Optional[asyncio.Task] = None
        self._timeout: float | int = 0.2  # make make this a ping based decision

        # ---- async events ----
        self._stop_evt: asyncio.Event = asyncio.Event()
        self._trade_work_evt: asyncio.Event = asyncio.Event()
        self._dust_bin_evt: asyncio.Event = asyncio.Event()

        self._ntwk_degrade_evt: asyncio.Event = asyncio.Event()
        self._high_jitter_evt: asyncio.Event = asyncio.Event()

        # ---- State Control ----
        self._state_mgr = StateMgr(
            CONNECT_STATES_LIFECYCLE,
            start=ConnectionState.UNKNOWN,
            cur=ConnectionState.UNKNOWN,
            allowed_starts=[ConnectionState.UNKNOWN],
        )

        # ---- Generic Message parameter ----
        self._rid: int = 10  # initial request ID

        # Define client and transport layer
        self._client = webapi_client.WebApiClient() if client is None else client
        self._transport = (
            TransportCQG(self._host_name, loop=self._loop, client=self._client)
            if transport is None
            else transport
        )

        # ---- Routers. Use queues inside for message storage ----
        self._mkt_data_stream_router = StreamRouter()

        self._exec_stream_router = StreamRouter(on_publish=self._trade_work_evt.set)
        self._pos_status_stream_router = StreamRouter(on_publish=self._trade_work_evt.set)
        self._acc_summary_stream_router = StreamRouter(on_publish=self._trade_work_evt.set)

        self._msg_router = MessageRouter()

        # ---- queues for dead-letter server messages ----
        self._misc_queue: asyncio.Queue = asyncio.Queue()

        # ---- Latency/reaction attributes ----
        self.is_heartbeat_task_on: bool = False
        self.ping_time_interval: int = 30 # seconds
        self.server_heartbeat_rtts: deque = deque(maxlen=20) # in mili-seconds
        self.client_heartbeat_rtts: deque = deque(maxlen=20) # in seconds
        self.inactivity_timeout: float = 100.0

        if immediate_connect:
            self.start()

    def rid(self) -> int:  # request
        self._rid += 1
        if self._rid > 2_000_000_000:  # Hard limit set by CQG
            self._rid = 1
        return self._rid

    # ---- Dunder methods override
    async def __aenter__(self):
        start_is_success = self.start()
        if start_is_success:
            return self
        else:
            raise ConnectEnterError("Fail to enter connection via context manager.")

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        if self.state in (
            ConnectionState.UNKNOWN,
            ConnectionState.CLOSING,
            ConnectionState.CLOSED,
        ):  # Move to Logoff(if needed) and then Disconnect
            logger.warning(
                "Connect object not properly stopped.\
                Procced to automatic connection shutdown."
            )
            return False
        else:
            stop_is_success = await self.stop()

            if not stop_is_success:
                logger.warning("Unable to go through with standard stop() procedure to exit.")
                return False
            else:
                return False

    def __del__(self):
        # Guard against partial init
        state_mgr = getattr(self, "_state_mgr", None)
        if state_mgr is None:  # __init__ never completed
            return

        # =============================================================================
        #         if self.state not in (
        #                 ConnectionState.UNKNOWN,
        #                 ConnectionState.CLOSING,
        #                 ConnectionState.CLOSED
        #                 ): # Move to Logoff(if needed) and then Disconnect
        #             return
        # =============================================================================

        warnings.warn(
            f"Unclosed {self}. Call await stop() or use async with caluse.",
            ResourceWarning,
            source=self,
        )

        if self.state == ConnectionState.CONNECTED_LOGON:
            try:  # Emergency Sync logoff
                logoff_msg = build_logoff_msg("logoff")
                self._client.send_client_message(logoff_msg)
            except Exception:
                logger.warning("Fail to logoff cleanly.")

        try:
            self._transport.disconnect()
        except TransportDisconnectError:
            logger.warning("Transport Layer not properly disconnect.")

        if getattr(self, "_stop_evt", None):
            self._stop_evt.set()
        try:
            self._transport.stop()
        except Exception:
            logger.warning("Transport Layer not properly stopped.")

        router_task = getattr(self, "_router_task", None)
        if router_task and not router_task.done():
            router_task.cancel()  # schedules cancellation only — cannot await

    # ---- Internal Getter Methods ----
    @property
    def state(self) -> ConnectionState:
        return self._state_mgr.cur

    @property
    def client(self):
        # return client connection object
        return self._client

    @property
    def transport(self) -> TransportCQG:
        return self._transport

    # ---- Live cycle --------
    def start(self) -> bool:
        # (1) unknown -> connecting -> connected_default, or
        # (2) Disconnected -> reconnecting -> connected_default
        if self.state not in (ConnectionState.UNKNOWN, ConnectionState.DISCONNECTED):
            logger.warning("Invalid state: %s to run start() function call.", self.state)
            return False

        if self.state == ConnectionState.UNKNOWN:
            self._state_mgr.transition_to(ConnectionState.CONNECTING)
        elif self.state == ConnectionState.DISCONNECTED:
            self._state_mgr.transition_to(ConnectionState.RECONNECTING)

        try:
            self._transport.connect()
        except TransportConnectError as e:
            logger.warning(str(e))
            self._state_mgr.transition_to(ConnectionState.DISCONNECTED)
            return False
        else:  # Only start transport threads when connection is established.
            self._state_mgr.transition_to(ConnectionState.CONNECTED_DEFAULT)
            self._transport.start()
            self._router_task = asyncio.create_task(self._router_loop())
        return True

    async def stop(self) -> bool:
        # Logon -> Logoff -> Disconnected -> closing -> closed, or
        # connected_default -> disconnected -> closing -> closed
        if self.state in (
            ConnectionState.UNKNOWN,
            ConnectionState.CLOSING,
            ConnectionState.CLOSED,
        ):  # These are terminal states
            logger.warning(f"Current State:{self.state} cannot initiate disconnection.")
            return False

        # --- DISCONNECTION STATE
        # (1) ensure logoff is done before stopping
        if self.state == ConnectionState.CONNECTED_LOGON:
            # Successful Logoff automatically move state to LOGOFF
            try:
                await self.logoff()
            except (ConnectRequestError, ConnectTimeOutError) as e:
                logger.warning(str(e))  # proceed to ungraceful disconnection
                return False

        try:  # Try Disconnect first before joining the threads
            self._transport.disconnect()
        except TransportDisconnectError as e:
            logger.warning(str(e))
            return False
        else:
            self._state_mgr.transition_to(ConnectionState.DISCONNECTED)

        # --- CLOSING STATE
        if self.state == ConnectionState.DISCONNECTED:
            self._state_mgr.transition_to(ConnectionState.CLOSING)

        # Stop the Router loop
        self._stop_evt.set()

        try:  # This stops the send and recv loops and join the threads
            self._transport.stop()
        except TransportDisconnectError as e:
            logger.warning("Transport Dosconnect Failed: %s.", str(e))
            return False
        finally:
            if self._router_task:
                self._router_task.cancel()
                try:
                    await self._router_task
                except (asyncio.CancelledError, Exception) as e:
                    if not isinstance(e, asyncio.CancelledError):
                        logger.warning("Router task raised on shutdown: %s", e)
                self._state_mgr.transition_to(ConnectionState.CLOSED)
        return True

    # ---- Router Loop ------
    async def _router_loop(self) -> None:
        while not self._stop_evt.is_set():
            try:
                msg: ServerMsg = await self._transport.recv()
                # print("[router_loop] msg", msg)

                if not msg or not isinstance(msg, ServerMsg):
                    logger.warning("Invalid Message Type, msg: %s", msg)
                    continue

                # (0) check if it is a composite message
                top_unique_fields = server_msg_type(msg)

                if len(top_unique_fields) > 1:
                    msgs = split_server_msg(msg, top_unique_fields)
                elif len(top_unique_fields) == 0:
                    logger.warning("Empty ServerMsg: %s", msg)
                    continue
                else:
                    msgs = [msg]

                for msg, top_unique_field in zip(msgs, top_unique_fields):
                    # Cheapest check first,
                    # --- (1) Ping/Pong dispatch
                    if is_ping(top_unique_field):
                        utc_time = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
                        await self.pong(msg.ping.token, msg.ping.ping_utc_time, utc_time)
                        logger.info(f"Responded to Server Ping at {utc_time}")
                        continue

                    # --- (2) streaming realtime Data dispatch ---
                    if is_realtime_tick(top_unique_field):
                        for rtmd in msg.real_time_market_data:
                            await self._mkt_data_stream_router.publish(rtmd.contract_id, rtmd)
                        continue

                    # --- (3) order update-related dispatch ---
                    if is_order_update_stream(top_unique_field):
                        for ord_sts in msg.order_statuses:
                            await self._exec_stream_router.publish(ord_sts.chain_order_id, msg)
                            # one-shot future resolution — silent no-op if nobody registered

                            for key in [
                                (
                                    "order_confirm",
                                    "order_statuses",
                                    "cl_order_id",
                                    ord_sts.order.cl_order_id,
                                ),
                                ("order_confirm", "order_statuses", "order_id", ord_sts.order_id),
                                # ('order_confirm', 'order_statuses', 'chain_order_id', ord_sts.chain_order_id)
                            ]:
                                self._msg_router.on_message(key, msg)

                    if is_position_statuses_stream(top_unique_field):
                        for pos_sts in msg.position_statuses:
                            await self._pos_status_stream_router.publish(pos_sts.contract_id, msg)
                        continue

                    if is_account_summary_statuses_stream(top_unique_field):
                        for acc_summ in msg.account_summary_statuses:
                            await self._acc_summary_stream_router.publish(acc_summ.account_id, msg)
                        continue

                    # --- (4) Expensive RPC type key matching ---
                    try:
                        keys = extract_router_keys(msg)
                    except KeyExtractorError as e:
                        logger.warning("Key Extraction Failed: %s.", str(e))
                    else:
                        if len(keys) > 1:
                            logger.debug("multiple keys: %s", keys)
                        for key in keys:
                            if key is not None:
                                key_type, msg_type, msg_id_type, msg_id = key
                                # 3) RPC routing (futures),
                                if key_type in {"rpc_reqid", "session", "sub", "info"}:
                                    ticket_exist = self._msg_router.on_message(key, msg)

                                    if not ticket_exist:
                                        # (5) --- Unsolicited Messages ---
                                        await self._misc_queue.put(msg)
                                        self._dust_bin_evt.set()

            except asyncio.CancelledError as e:
                logger.error(str(e))
                raise
            except TransportConnectError as e:
                logger.error(f"TransportConnectError, reconenction initiated: {str(e)}")
                # await asyncio.create_task(self._reconnect_loop)
                # return
            except Exception as e:
                logger.error("Router Loop Error: %s", e, exc_info=True)

    # ----- DustBin -----------------------
    async def _dust_bin_handle(self) -> None: ...

        # --- (5) Dust Bin/Unsolicited Message Collections ---
        # !!! For server-side
        # 1. logged_off
        # 2. concurrent_connection_join_results
        # 3. information_reports:symbol_resolution_report
        # 4. market_data_subscription_statuses

    
    # ---- Failure Mode -------------------
    async def _on_transport_failure(self, exc: Exception) -> None:
        pass

    async def _reconnect_loop(self, num_attempt: int = 10):
        while not self._stop_evt.is_set():
            ...

    async def _update_timeout_loop(self):
        while not self._stop_evt.is_set():
            ...

    async def _heartbeat_loop(self):
        while not self._stop_evt.is_set():
            start = self._loop.time()
            token = str(self.rid())
            client_rtt = 0.0
            try:
                parsed_pong = await self.ping(token)
                
                if parsed_pong is None:
                    continue
                server_rtt = parsed_pong[PONG_PONGTIME] - parsed_pong[PONG_PINGTIME] 
                client_rtt = self._loop.time() - start
                
                self.server_heartbeat_rtts.append(server_rtt)
                self.client_heartbeat_rtts.append(client_rtt)
                
                # --- Latnecy checks
                if latency_above_threshold():...
                
                if jitter_above_threshold():...
                
            except ConnectRequestError as e:
                logger.warning(f"Ping message of token: {token} failed due to {str(e)}.")
            except ConnectTimeOutError as e:
                logger.warning(f"Ping message of token: {token} failed due to Timeout Error:{str(e)}.")
            finally:
                await asyncio.sleep(max(0, self.ping_time_interval - client_rtt))

    # ---- CQG session messages function calls ----
    async def logon(
        self,
        client_app_id: str = "WebApiTest",
        client_version: str = "python-client-test-2-240",
        protocol_version_major: int = 2,
        protocol_version_minor: int = 240,
        drop_concurrent_session: bool = False,
        private_label: str = "WebApiTest",
        **kwargs,
    ) -> dict[str, Any] | None:
        with msg_io_error_handler(ConnectRequestError, timeout_error=ConnectTimeOutError):
            client_msg = build_logon_msg(
                self._user_name,
                self._password,
                client_app_id=client_app_id,
                protocol_version_major=protocol_version_major,
                protocol_version_minor=protocol_version_minor,
                drop_concurrent_session=drop_concurrent_session,
                private_label=private_label,
                **kwargs,
            )

            msg_key = ("session", "logon_result", "single", 0)
            fut = self._msg_router.register_key(msg_key)

            await self._transport.send(client_msg)
            server_msg = await asyncio.wait_for(fut, timeout=self._timeout)

            int_msg = parse_logon_result(server_msg)  # internal message

            if CONN_LOGON_RESCODE_CQG2INT.get(int_msg["result_code"]):
                next_state = CONN_LOGON_RESCODE_CQG2INT[int_msg["result_code"]]
                self._state_mgr.transition_to(next_state)
            return int_msg

    async def logoff(self) -> dict[str, Any] | None:
        with msg_io_error_handler(ConnectRequestError, timeout_error=ConnectTimeOutError):
            client_msg = build_logoff_msg("logoff")

            msg_key = ("session", "logged_off", "single", 0)
            fut = self._msg_router.register_key(msg_key)

            await self._transport.send(client_msg)
            server_msg = await asyncio.wait_for(fut, timeout=self._timeout)

            int_msg = parse_logged_off(server_msg)

            # Only accept specific path
            if CONN_LOGOFF_RESCODE_CQG2INT.get(int_msg["logoff_reason"]):
                next_state = CONN_LOGOFF_RESCODE_CQG2INT[int_msg["logoff_reason"]]
                self._state_mgr.transition_to(next_state)
            return int_msg

    async def restore_request(
        self,
        session_token: Optional[str] = None,
        client_app_id: Optional[str] = None,
        protocol_version_major: Optional[int] = None,
        protocol_version_minor: Optional[int] = None,
        **kwargs,
    ) -> dict[str, Any] | None:
        with msg_io_error_handler(ConnectRequestError, timeout_error=ConnectTimeOutError):
            self._state_mgr.transition_to(ConnectionState.RECONNECTING)

            restore_msg = build_restore_msg(
                cast(str, client_app_id if client_app_id is not None else self.client_app_id),
                cast(
                    int,
                    protocol_version_major
                    if protocol_version_major is not None
                    else self.protocol_version_major,
                ),
                cast(
                    int,
                    protocol_version_minor
                    if protocol_version_minor is not None
                    else self.protocol_version_minor,
                ),
                cast(str, session_token if session_token is not None else self.session_token),
                **kwargs,
            )

            msg_key = ("session", "restore_or_join_session_result", "single", 0)
            fut = self._msg_router.register_key(msg_key)
            await self._transport.send(restore_msg)
            server_msg = await asyncio.wait_for(fut, timeout=self._timeout)

            int_msg = parse_restore_or_join_session_result(server_msg)

            if CONN_RESTORE_RESCODE_CQG2INT.get(int_msg["result_code"]):
                next_state = CONN_RESTORE_RESCODE_CQG2INT[int_msg["result_code"]]
                self._state_mgr.transition_to(next_state)
            return int_msg

    async def ping(self, token: str | None = None) -> PongType | None:
        if not token:
            token = str(self.rid())
        utc_time = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

        with msg_io_error_handler(ConnectRequestError, timeout_error=ConnectTimeOutError):
            ping_msg = build_ping_msg(token, ping_utc_time=utc_time)

            msg_key = ("session", "pong", "token", token)
            fut = self._msg_router.register_key(msg_key)
            await self._transport.send(ping_msg)
            server_msg = await asyncio.wait_for(fut, timeout=self._timeout)

            return parse_pong(server_msg)

    async def pong(self, token: str, ping_utc_time: int, pong_utc_time: int) -> None:
        with msg_io_error_handler(ConnectRequestError, timeout_error=ConnectTimeOutError):
            pong_msg = build_pong_msg(token, ping_utc_time, pong_utc_time)

            await self._transport.send(pong_msg)
            return

    # ---- CQG metadata messages function calls ----
    async def resolve_symbol(
        self,
        symbol: str,
    ) -> list[dict[str, Any]]:
        # symbol Resolution
        with msg_io_error_handler(SymbolResolutionError, timeout_error=ConnectTimeOutError):
            rid = self.rid()
            msg = build_resolve_symbol_msg(symbol, rid, subscribe=True)

            msg_key = ("info", "information_reports:symbol_resolution_report", "id", rid)
            fut = self._msg_router.register_key(msg_key)
            await self._transport.send(msg)
            server_msg = await asyncio.wait_for(fut, timeout=self._timeout)

            return parse_server_msg(server_msg, connect_parsers)

    async def unsubscribe_symbol(self, symbol: str) -> list[dict[str, Any]]:
        with msg_io_error_handler(SymbolResolutionError, timeout_error=ConnectTimeOutError):
            rid = self.rid()
            msg = build_resolve_symbol_msg(symbol, rid, subscribe=False)
            msg_key = ("info", "information_reports:symbol_resolution_report", "id", rid)
            fut = self._msg_router.register_key(msg_key)
            await self._transport.send(msg)
            server_msg = await asyncio.wait_for(fut, timeout=self._timeout)
            return parse_server_msg(server_msg, connect_parsers)
