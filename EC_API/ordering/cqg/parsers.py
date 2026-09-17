#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 11 03:05:48 2026

@author: dexter
"""

from typing import Any, Callable
from google.protobuf.json_format import MessageToDict
from EC_API.ordering.cqg.enum_mapping import OrderStatus_MAP_CQG2INT
from EC_API.ext.WebAPI.webapi_2_pb2 import ServerMsg
from EC_API.ext.WebAPI.trade_routing_2_pb2 import PositionStatus
from EC_API.exceptions import MsgParserError
from EC_API._typing import Parser_func

ordering_parsers: dict[str, Parser_func] = dict()


def register_parser(name: str):
    def decorator(func: Callable):
        ordering_parsers[name] = func
        return func

    return decorator


@register_parser("order_request_rejects")
def parse_order_request_rejects(msg: ServerMsg) -> list[dict[str, Any]]:
    try:
        res = []
        for ele in msg.order_request_rejects:
            res.append({"request_id": ele.request_id, "reject_code": ele.reject_code})
        return res
    except Exception:
        raise MsgParserError("Failed to parse order_request_rejects.")


@register_parser("order_request_acks")
def parse_order_request_acks(msg: ServerMsg) -> list[dict[str, Any]]:
    try:
        res = []
        for ele in msg.order_request_acks:
            res.append({"request_id": ele.request_id, "when": ele.when})
        return res
    except Exception:
        raise MsgParserError("Failed to parse order_request_acks.")


@register_parser("trade_subscription_statuses")
def parse_trade_subscription_statuses(msg: ServerMsg) -> list[dict[str, Any]]:
    try:
        res = []
        for ele in msg.trade_subscription_statuses:
            res.append({"sub_id": ele.id, "status_code": ele.status_code})
        return res
    except Exception:
        raise MsgParserError("Failed to parse trade_subscription_statuses.")


@register_parser("trade_snapshot_completions")
def parse_trade_snapshot_completions(msg: ServerMsg) -> list[dict[str, Any]]:
    try:
        res = []
        for ele in msg.trade_snapshot_completions:
            res.append({"sub_id": ele.subscription_id, "sub_scopes": list(ele.subscription_scopes)})
        return res

    except Exception:
        raise MsgParserError("Failed to parse trade_snapshot_completions.")


@register_parser("order_statuses")
def parse_order_statuses(msg: ServerMsg, in_detail: bool = False) -> list[dict[str, Any]]:
    try:
        res = []
        for i, ele in enumerate(msg.order_statuses):
            # parsing required fields
            D = {
                "sub_ids": list(ele.subscription_ids),
                "status": OrderStatus_MAP_CQG2INT[ele.status],
                "order_id": ele.order_id,
                "chain_order_id": ele.chain_order_id,
                "status_utc_timestamp": ele.status_utc_timestamp,
                "submission_utc_timestamp": ele.submission_utc_timestamp,
                "fill_cnt": ele.fill_cnt,
                "scaled_avg_fill_price": ele.scaled_avg_fill_price,
                "avg_fill_price_correct": ele.avg_fill_price_correct,
                "account_id": ele.account_id,
                "entered_by_user": ele.entered_by_user,
            }

            # missiion critical optional fields parsing
            if ele.HasField("order"):
                D["order"] = {
                    "account_id": ele.order.account_id,
                    "cl_order_id": ele.order.cl_order_id,
                    "contract_id": ele.order.contract_id,
                    "order_type": ele.order.order_type,
                    "duration": ele.order.duration,
                    "side": ele.order.side,
                    "qty": {
                        "significand": ele.order.qty.significand,
                        "exponent": ele.order.qty.exponent,
                    },
                    "scaled_limit_price": ele.order.scaled_limit_price,
                    "scaled_stop_price": ele.order.scaled_stop_price,
                }

            # Parse all the detailed optional field in the message. Slow Method.
            if in_detail:
                # Note: int32/float/double stays numeric, anything int64/uint64
                # becomes a string.  A flaw in JSON formatting. Fix later.
                detail = MessageToDict(ele, preserving_proto_field_name=True)
                D.update({k: v for k, v in detail.items() if k not in D})
            res.append(D)
        return res

    except Exception as e:
        raise MsgParserError(f"Failed to parse order_statuses: {e}") from e


@register_parser("position_statuses")
def parse_position_statuses(msg: ServerMsg, in_detail: bool = False) -> list[dict[str, Any]]:
    try:
        res = []
        for i, ele in enumerate(msg.position_statuses):
            D = {
                "sub_ids": list(ele.subscription_ids),
                "account_id": ele.account_id,
                "contract_id": ele.contract_id,
                "is_short_open_position": ele.is_short_open_position,
                "is_snapshot": ele.is_snapshot
            }
            if ele.open_positions:
                poss = parse_open_position(ele)
                D["open_positions"] = poss

            # Parse all the detailed field in the message. Slow.
            if in_detail:
                # Note: int32/float/double stays numeric, anything int64/uint64
                # becomes a string. A flaw in JSON formatting. Fix later.
                detail = MessageToDict(ele, preserving_proto_field_name=True)
                D.update({k: v for k, v in detail.items() if k not in D})

            res.append(D)
        return res

    except Exception as e:
        raise MsgParserError(f"Failed to parse position_statuses: {e}") from e


def parse_open_position(msg: PositionStatus) -> list:  # special helper for fast execution
    poss = []
    for pos in msg.open_positions:
        poss.append(
            {
                "id": pos.id,
                "price_correct": pos.price_correct,
                "trade_date": pos.trade_date,
                "statement_date": pos.statement_date,
                "is_aggregated": pos.is_aggregated,
                "is_short": pos.is_short,
                "qty": pos.qty.significand,
            }
        )
    return poss


@register_parser("account_summary_statuses")
def parse_account_summary_statuses(msg: ServerMsg) -> list[dict[str, Any]]:
    try:
        res = []
        for ele in msg.account_summary_statuses:
            res.append(
                {
                    "subscription_ids": ele.subscription_ids,
                    "account_id": ele.account_id,
                    "currency": ele.currency,
                    "purchasing_power": ele.purchasing_power,
                }
            )
        return res
    except Exception as e:
        raise MsgParserError(f"Failed to parse account_summary_statuses: {e}") from e


@register_parser("go_flat_statuses")
def parse_go_flat_statuses(msg: ServerMsg) -> list[dict[str, Any]]:
    try:
        res = []
        for ele in msg.go_flat_statuses:
            res.append(
                {
                    "request_id": ele.request_id,
                    "account_id": ele.account_id,
                    "status_code": ele.status_code,
                }
            )
        return res
    except Exception as e:
        raise MsgParserError(f"Failed to parse go_flat_statuses: {e}") from e
