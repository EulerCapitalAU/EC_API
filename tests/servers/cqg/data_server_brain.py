#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 12 20:03:21 2026

@author: dexter
"""

from EC_API.ext.WebAPI.webapi_2_pb2 import ServerMsg, ClientMsg
from EC_API.ext.WebAPI.market_data_2_pb2 import MarketDataSubscription as MktDSub
from EC_API.ext.common.shared_1_pb2 import OrderStatus
from EC_API.ext.WebAPI.user_session_2_pb2 import LogonResult as LgRes
from EC_API.ext.WebAPI.user_session_2_pb2 import RestoreOrJoinSessionResult as RstJoinSessRes
from EC_API.ext.WebAPI.user_session_2_pb2 import LoggedOff as LOff
from EC_API.ext.WebAPI.webapi_2_pb2 import InformationReport as InfoRp
from EC_API.connect.cqg.base import ConnectCQG
from tests.unit.fixtures.server_msg_builders_CQG import (
    build_logon_result_server_msg,
    build_logged_off_server_msg,
    build_restore_or_join_session_result_server_msg,
    build_symbol_resolution_report_server_msg,
    build_market_data_subscription_statuses_server_msg,
    build_real_time_market_data_server_msg,
    build_order_statuses_server_msg,
    build_order_request_rejects_server_msg,
    build_order_request_acks_server_msg,
    build_go_flat_statuses_server_msg
    )
from tests.servers.brains import ServerBrain

def client_msg_type(client_msg: ClientMsg) -> list[str]:
    return [fd.name for fd, val in client_msg.ListFields()]

class FakeDataServerBrainCQG(ServerBrain):
    def __init__(
            self, 
            success_decisions: dict = dict(),
            extra_instructions: dict = dict()
            ):
        # --- Settings
        self.success_decisions = success_decisions # Determine if a response return success or not
        self.extra_instructions = extra_instructions
                
    async def response_logic(self, msg: ClientMsg, **kwargs) -> ServerMsg:
        msg_name = client_msg_type(msg)[0] 
        # we assume client dispatch message one at a time
        match msg_name:
            # ---- Session ----
            case "logon":
                return await self._logon_response(msg)
            case "logoff":
                return await self._logoff_response(msg)
            # ---- Metadata ----
            case "information_requests": # In this context
                return await self._sym_res_response(msg, **kwargs)
                 
            # ---- Realtime Data ----
            # !!! wip
            case "market_data_subscriptions":
                await self._mkt_data_status_response(msg)
                count = 0
                #while not self.conn._stop_evt():
                if self.success_decisions["market_data_subscriptions"]:
                    while count < 100:
                        await self._mkt_data_stream_responses(msg)
                        count+=1
            case _:
                
                pass
            
    # ---- Sessions ----
    async def _logon_response(self, client_msg: ClientMsg) -> ServerMsg | None:
        if self.success_decisions.get("logon") is not None:
            if self.success_decisions['logon']:
                server_msg = build_logon_result_server_msg(
                    ServerMsg(),
                    res_code = LgRes.ResultCode.RESULT_CODE_SUCCESS
                    )           
                return server_msg
            elif not self.success_decisions['logon']:
                server_msg = build_logon_result_server_msg(
                    ServerMsg(),
                    res_code = LgRes.ResultCode.RESULT_CODE_FAILURE
                    )
                return server_msg
        return

            #await self.transport.in_q.put(server_msg)
        
    async def _restore_response(self, client_msg: ClientMsg) -> ServerMsg | None:
        if self.success_decisions.get("restore") is not None:
            if self.success_decisions['restore']:
                server_msg = build_restore_or_join_session_result_server_msg(
                    ServerMsg()                    
                    )
                return server_msg
        return

    async def _logoff_response(self, client_msg: ClientMsg) -> ServerMsg | None:
        if self.success_decisions.get("logoff") is not None:
            if self.success_decisions['logoff']:
                server_msg = build_logged_off_server_msg(
                    ServerMsg(),
                    res = LOff.LogoffReason.LOGOFF_REASON_BY_REQUEST
                    )
                return server_msg
            else:
                server_msg = build_logged_off_server_msg(
                    ServerMsg(),
                    res = LOff.LogoffReason.LOGOFF_REASON_BY_REQUEST
                    )
                return server_msg
        return

            #await self.transport.in_q.put(server_msg)
        
    # ---- Metadata ----
    async def _sym_res_response(self, client_msg: ClientMsg, **kwargs) -> ServerMsg | None:
        if self.success_decisions.get("information_requests") is not None:
            sym = client_msg.information_requests[0].symbol_resolution_request.symbol

            if self.success_decisions["information_requests"]:
                
                if client_msg.information_requests[0].subscribe is None:
                    success_code = InfoRp.StatusCode.STATUS_CODE_SUCCESS
                else:
                    if client_msg.information_requests[0].subscribe:
                        success_code = InfoRp.StatusCode.STATUS_CODE_SUBSCRIBED
                    elif not client_msg.information_requests[0].subscribe:
                        success_code = InfoRp.StatusCode.STATUS_CODE_DROPPED
                    
                sym_rp = build_symbol_resolution_report_server_msg(
                    ServerMsg(), 
                    report_id = client_msg.information_requests[0].id,
                    cotract_id = kwargs['contract_id'] if 'contract_id' in kwargs else int(sym.split("_")[-1]),
                    contract_symbol = sym,
                    res = success_code
                    )
                return sym_rp
                
            elif not self.success_decisions["information_requests"]:
                sym_rp = build_symbol_resolution_report_server_msg(
                    ServerMsg(), 
                    report_id = client_msg.information_requests[0].id,
                    cotract_id = kwargs['contract_id'] if 'contract_id' in kwargs else int(sym.split("_")[-1]),
                    contract_symbol = sym,
                    res = InfoRp.StatusCode.STATUS_CODE_FAILURE
                    )
                return sym_rp
        return
        #await self.transport.in_q.put(sym_rp)
        
    # ---- Realtime Market ----
    async def _mkt_data_status_response(self, client_msg: ClientMsg) -> ServerMsg | None:
        if self.success_decisions.get("market_data_subscriptions") is not None:
            if self.success_decisions["market_data_subscriptions"]:
                for mkt_sub in client_msg.market_data_subscriptions:
                    mkt_status = build_market_data_subscription_statuses_server_msg(
                        ServerMsg(),
                        contract_id= mkt_sub.contract_id,
                        level = mkt_sub.level
                        )
                    return mkt_status
            elif not self.success_decisions["market_data_subscriptions"]:
                for mkt_sub in client_msg.market_data_subscriptions:

                    mkt_status = build_market_data_subscription_statuses_server_msg(
                        ServerMsg(),
                        contract_id= mkt_sub.contract_id,
                        level = mkt_sub.level
                        )
                    return mkt_status
        return
            
    async def _mkt_data_stream_responses(self, client_msg: ClientMsg):
        for mkt_sub in client_msg.market_data_subscriptions:
            server_msg = build_real_time_market_data_server_msg(
                ServerMsg(),
                contract_id = mkt_sub.contract_id
                )
            #await self.transport.in_q.put(server_msg)
            
    async def _unsubscribe_mkt_data_response(self, client_msg: ClientMsg) -> ServerMsg | None:
        if self.success_decisions.get("unsubscribe_mkt_data") is not None:
            ...
        return

