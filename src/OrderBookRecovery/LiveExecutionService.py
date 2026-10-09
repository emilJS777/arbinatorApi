import json
import logging
import os
from types import SimpleNamespace
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_DOWN

import ccxt
import requests

from src.Exchange.ExchangeModel import Exchange


class LiveExecutionError(Exception):
    pass


class SubmissionUnknown(LiveExecutionError):
    """A durable client ID must be reconciled; never resubmit automatically."""
    pass


class OrderNotFilled(LiveExecutionError):
    pass


logger = logging.getLogger(__name__)


class LiveExecutionService:
    def __init__(self, client_factory=None, requests_client=None):
        self.client_factory = client_factory
        self.requests = requests_client or requests

    def hard_disabled(self):
        return str(os.environ.get("LIVE_TRADING_HARD_DISABLED", "true")).lower() == "true"

    def immutable_config(self, config, trade):
        payload = json.loads(trade.execution_config_json or "{}")
        if not payload:
            raise LiveExecutionError("legacy_trade_requires_execution_config_review")
        payload.update(exchange=trade.exchange, symbol=trade.symbol, leverage=trade.leverage)
        return SimpleNamespace(**payload)

    def exchange_record(self, config):
        if config.exchange_id:
            return Exchange.query.filter_by(id=config.exchange_id).first()
        return Exchange.query.filter(Exchange.title.ilike(config.exchange)).first()

    def validate_enabled(self, config, margin):
        if self.hard_disabled():
            return "live_trading_hard_disabled"
        if config.execution_mode != "live":
            return None
        if not config.live_enabled_confirmation:
            return "live_confirmation_required"
        if config.live_kill_switch:
            return "live_kill_switch_enabled"
        exchange = self.exchange_record(config)
        if not exchange or not exchange.api_key or not exchange.api_secret:
            return "live_exchange_credentials_required"
        if float(margin) > float(config.live_max_margin_usdt):
            return "live_margin_exceeds_limit"
        return None

    def margin_limit_debug(self, config, margin, leverage):
        current_margin = float(margin or 0)
        current_notional = current_margin * float(leverage or 1)
        live_max_margin = float(config.live_max_margin_usdt)
        return {
            "current_margin": current_margin,
            "current_notional": current_notional,
            "live_max_margin_usdt": live_max_margin,
            "margin_limit_reason": "live_margin_exceeds_limit" if current_margin > live_max_margin else None,
        }

    def client(self, config):
        exchange = self.exchange_record(config)
        if not exchange:
            raise LiveExecutionError("live_exchange_not_found")
        if self.client_factory:
            return self.client_factory(exchange)
        exchange_id = str(exchange.title or "").strip().lower().replace(".", "")
        klass = getattr(ccxt, exchange_id, None)
        if not klass:
            raise LiveExecutionError("live_exchange_not_supported")
        exchange_options = {
            "defaultType": "swap",
            "defaultSubType": "linear",
        }
        if exchange_id == "mexc":
            exchange_options.update({
                "defaultType": "swap",
                "defaultSettle": "USDT",
            })
        options = {
            "apiKey": exchange.api_key,
            "secret": exchange.api_secret,
            "password": exchange.password or None,
            "enableRateLimit": True,
            "options": exchange_options,
        }
        client = klass({key: value for key, value in options.items() if value is not None})
        client.timeout = 5000
        if exchange_id == "mexc" and hasattr(client, "urls"):
            client.urls["api"]["contract"]["public"] = "https://api.mexc.com/api/v1/contract"
            client.urls["api"]["contract"]["private"] = "https://api.mexc.com/api/v1/private"
        return client

    def split_symbol(self, symbol):
        normalized = str(symbol or "").upper().split(":", 1)[0].replace("-", "/").replace("_", "/")
        if "/" in normalized:
            base, quote = normalized.split("/", 1)
        else:
            base, quote = normalized[:-4], normalized[-4:]
        return base, quote

    def is_live_futures_market(self, market):
        return bool(market and (market.get("swap") or market.get("future") or market.get("contract")))

    def is_usdt_linear_market(self, market):
        settle = str(market.get("settle") or market.get("settleId") or "").upper()
        return (market.get("linear") is not False) and (not settle or settle == "USDT")

    def market_info(self, market=None, error=None, configured_symbol=None):
        return {
            "configured_symbol": configured_symbol,
            "resolved_live_symbol": (market or {}).get("symbol"),
            "live_market_type": (market or {}).get("type"),
            "live_market_valid": bool(market and not error),
            "live_market_error": error,
            "swap": bool((market or {}).get("swap")),
            "future": bool((market or {}).get("future")),
            "spot": bool((market or {}).get("spot")),
            "linear": (market or {}).get("linear"),
            "settle": (market or {}).get("settle") or (market or {}).get("settleId"),
            "contract_size": (market or {}).get("contractSize") or (market or {}).get("contract_size"),
        }

    def resolve_live_futures_market(self, client, configured_symbol):
        markets = client.load_markets()
        base, quote = self.split_symbol(configured_symbol)
        preferred_symbols = [
            configured_symbol,
            f"{base}/{quote}:USDT",
            f"{base}/{quote}",
        ]
        candidates = []
        for symbol in preferred_symbols:
            market = markets.get(symbol)
            if market:
                candidates.append(market)
        for market in markets.values():
            if str(market.get("base") or "").upper() != base:
                continue
            if str(market.get("quote") or "").upper() != quote:
                continue
            if str(market.get("settle") or market.get("settleId") or "USDT").upper() != "USDT":
                continue
            candidates.append(market)

        unique = []
        seen = set()
        for market in candidates:
            key = market.get("symbol") or id(market)
            if key in seen:
                continue
            seen.add(key)
            unique.append(market)

        for market in unique:
            if self.is_live_futures_market(market) and self.is_usdt_linear_market(market):
                logger.info(
                    "Resolved live futures market configured_symbol=%s resolved_live_symbol=%s market_type=%s swap=%s future=%s spot=%s settle=%s contract_size=%s",
                    configured_symbol,
                    market.get("symbol"),
                    market.get("type"),
                    market.get("swap"),
                    market.get("future"),
                    market.get("spot"),
                    market.get("settle") or market.get("settleId"),
                    market.get("contractSize") or market.get("contract_size"),
                )
                return market
        raise LiveExecutionError("live_futures_market_not_found")

    def market(self, client, symbol):
        return self.resolve_live_futures_market(client, symbol)

    def fee_cost(self, order):
        fee = order.get("fee") if isinstance(order, dict) else None
        if isinstance(fee, dict):
            return float(fee.get("cost") or 0)
        fees = order.get("fees") if isinstance(order, dict) else None
        if isinstance(fees, list):
            return sum(float(item.get("cost") or 0) for item in fees)
        return 0

    def order_id(self, order):
        if not isinstance(order, dict):
            return ""
        value = order.get("id") or order.get("orderId") or order.get("data")
        if isinstance(value, dict):
            value = value.get("orderId") or value.get("id")
        return str(value or "")

    def average_price(self, order, fallback_price):
        if not isinstance(order, dict):
            return fallback_price
        return float(order.get("average") or order.get("price") or fallback_price)

    def filled_amount(self, order, fallback_amount):
        if not isinstance(order, dict):
            return fallback_amount
        return float(order.get("filled") or order.get("amount") or fallback_amount)

    def is_mexc_client(self, client):
        client_id = str(getattr(client, "id", "") or "").lower()
        class_name = client.__class__.__name__.lower()
        return client_id == "mexc" or "mexc" in class_name

    def contract_amount(self, market, base_amount):
        contract_size = market.get("contractSize") or market.get("contract_size")
        if contract_size:
            return float(base_amount) / float(contract_size)
        return float(base_amount)

    def fetch_mexc_contract_detail(self, market):
        symbol_id = market.get("id") or market.get("symbol")
        response = self.requests.get(
            "https://contract.mexc.com/api/v1/contract/detail",
            params={"symbol": symbol_id},
            timeout=10,
        )
        if not response.ok:
            raise LiveExecutionError(f"live_mexc_contract_detail_failed:{response.status_code}:{response.text[:300]}")
        payload = response.json()
        data = payload.get("data")
        if isinstance(data, list):
            detail = next((item for item in data if item.get("symbol") == symbol_id), data[0] if data else None)
        else:
            detail = data
        if not isinstance(detail, dict):
            raise LiveExecutionError("live_mexc_contract_detail_missing")
        return detail

    def decimal_places(self, value):
        decimal = Decimal(str(value))
        return max(0, -decimal.as_tuple().exponent)

    def quantize_vol(self, value, vol_scale):
        step = Decimal("1") if int(vol_scale or 0) <= 0 else Decimal("1").scaleb(-int(vol_scale))
        return Decimal(str(value)).quantize(step, rounding=ROUND_DOWN)

    def quantize_price(self, value, contract_detail=None, direction="down"):
        detail = contract_detail or {}
        price_unit = Decimal(str(detail.get("priceUnit") or "0"))
        if price_unit <= 0:
            price_scale = int(detail.get("priceScale", 8))
            price_unit = Decimal("1").scaleb(-price_scale)
        decimal_value = Decimal(str(value))
        units = decimal_value / price_unit
        rounding = ROUND_DOWN
        rounded = units.to_integral_value(rounding=rounding) * price_unit
        return float(rounded)

    def normalize_mexc_contract_volume(self, market, base_amount, contract_detail=None):
        detail = contract_detail or {}
        contract_size = Decimal(str(detail.get("contractSize") or market.get("contractSize") or market.get("contract_size") or "1"))
        raw_vol = Decimal(str(base_amount)) / contract_size
        vol_scale = int(detail.get("volScale", self.decimal_places(detail.get("volUnit", 1))))
        vol_unit = Decimal(str(detail.get("volUnit") or "1"))
        min_vol = Decimal(str(detail.get("minVol") or "0"))
        max_vol = Decimal(str(detail.get("maxVol") or "0"))
        rounded = self.quantize_vol(raw_vol, vol_scale)
        if vol_unit > 0:
            units = (rounded / vol_unit).to_integral_value(rounding=ROUND_DOWN)
            rounded = units * vol_unit
            rounded = self.quantize_vol(rounded, vol_scale)
        below_min = bool(min_vol and rounded < min_vol)
        above_max = bool(max_vol and rounded > max_vol)
        if below_min:
            raise LiveExecutionError(f"live_mexc_order_below_min_vol:calculated_vol={rounded}:minVol={min_vol}")
        if above_max:
            raise LiveExecutionError(f"live_mexc_order_above_max_vol:calculated_vol={rounded}:maxVol={max_vol}")
        return (int(rounded) if rounded == rounded.to_integral_value() else float(rounded)), {
            "raw_vol": float(raw_vol),
            "rounded_vol": float(rounded),
            "contract_size": float(contract_size),
            "vol_scale": vol_scale,
            "vol_unit": float(vol_unit),
            "min_vol": float(min_vol),
            "max_vol": float(max_vol) if max_vol else None,
            "below_min_vol": below_min,
            "above_max_vol": above_max,
            "api_allowed": detail.get("apiAllowed"),
        }

    def amount_to_precision(self, client, symbol, amount):
        if hasattr(client, "amount_to_precision"):
            return float(client.amount_to_precision(symbol, amount))
        return float(amount)

    def price_to_precision(self, client, symbol, price):
        if hasattr(client, "price_to_precision"):
            return float(client.price_to_precision(symbol, price))
        return float(price)

    def mexc_private_post_request(self, client, path, body):
        signed = client.sign(path, api=["contract", "private"], method="POST", params=body)
        endpoint = signed["url"]
        old_base = "https://contract.mexc.com/api/v1/private"
        new_base = "https://api.mexc.com/api/v1/private"
        if endpoint.startswith(old_base):
            endpoint = new_base + endpoint[len(old_base):]
        return {
            "endpoint": endpoint,
            "method": signed.get("method") or "POST",
            "headers": signed.get("headers") or {},
            "body": body,
            "serialized_body": signed.get("body"),
            "signature_payload_preview": f"ApiKey + Request-Time + {signed.get('body')}",
            "request_time": (signed.get("headers") or {}).get("Request-Time"),
            "endpoint_path": path,
        }

    def mexc_swap_side(self, side, reduce_only):
        if reduce_only:
            return 2 if side == "buy" else 4
        return 1 if side == "buy" else 3

    def mexc_order_type(self, order_type):
        if order_type == "market":
            return 5
        if order_type == "limit":
            return 1
        return order_type

    def ensure_mexc_response_ok(self, response):
        if not isinstance(response, dict):
            return
        code = response.get("code")
        success = response.get("success")
        message = str(response.get("message") or response.get("msg") or "")
        if str(code) == "6026":
            raise LiveExecutionError("mexc_risk_control_verification_required")
        if str(code) == "2009" or "Position is nonexistent or closed" in message:
            raise LiveExecutionError("mexc_position_already_closed")
        if success is False or (code not in (None, 0, 200, "0", "200")):
            raise LiveExecutionError(f"live_mexc_order_failed:{response}")

    def is_position_already_closed_error(self, error):
        text = str(error)
        return "mexc_position_already_closed" in text or "Position is nonexistent or closed" in text or "code': 2009" in text or '"code":2009' in text

    def build_mexc_order_submit_request(self, client, market, order_type, side, amount, price=None, reduce_only=False, leverage=None, contract_detail=None, client_order_id=None):
        symbol = market.get("symbol")
        request_type = self.mexc_order_type(order_type)
        detail = contract_detail or self.fetch_mexc_contract_detail(market)
        vol, volume_details = self.normalize_mexc_contract_volume(market, amount, detail)
        body = {
            "symbol": market.get("id") or symbol,
            "vol": vol,
            "type": request_type,
            "openType": 1,
            "side": self.mexc_swap_side(side, reduce_only),
        }
        if leverage:
            body["leverage"] = int(leverage)
        if request_type == 5:
            body["price"] = 0
        elif price is not None:
            body["price"] = self.price_to_precision(client, symbol, price)
        if client_order_id:
            body["externalOid"] = client_order_id

        signed = self.mexc_private_post_request(client, "order/create", body)
        headers = signed.get("headers") or {}
        serialized_body = signed.get("serialized_body")
        return {
            "endpoint": signed["endpoint"],
            "method": signed.get("method") or "POST",
            "headers": headers,
            "body": body,
            "serialized_body": serialized_body,
            "signature_payload_preview": f"ApiKey + Request-Time + {serialized_body}",
            "request_time": headers.get("Request-Time"),
            "resolved_symbol": symbol,
            "side_mapping": {
                "input_side": side,
                "reduce_only": bool(reduce_only),
                "mexc_side": body["side"],
            },
            "order_type_mapping": {
                "input_order_type": order_type,
                "mexc_type": request_type,
            },
            "endpoint_path": "order/create",
            "contract_detail": detail,
            "volume_details": volume_details,
        }

    def sanitize_signed_order_request(self, signed_request):
        return {
            "endpoint": signed_request["endpoint"],
            "method": signed_request["method"],
            "sanitized_headers": {key: "<redacted>" for key in (signed_request.get("headers") or {}).keys()},
            "headers_names_used": sorted((signed_request.get("headers") or {}).keys()),
            "body": signed_request["body"],
            "serialized_body": signed_request["serialized_body"],
            "signature_payload_preview": signed_request["signature_payload_preview"],
            "request_time": signed_request["request_time"],
            "resolved_symbol": signed_request.get("resolved_symbol"),
            "side_mapping": signed_request.get("side_mapping"),
            "order_type_mapping": signed_request.get("order_type_mapping"),
            "endpoint_path": signed_request.get("endpoint_path"),
            "contract_detail": signed_request.get("contract_detail"),
            "volume_details": signed_request.get("volume_details"),
        }

    def submit_mexc_signed_order(self, signed_request):
        response = self.requests.request(
            signed_request["method"],
            signed_request["endpoint"],
            headers=signed_request["headers"],
            data=signed_request["serialized_body"],
            timeout=10,
        )
        if response.status_code >= 500:
            raise SubmissionUnknown("submit_server_error_outcome_unknown")
        try:
            payload = response.json()
        except Exception:
            payload = {"status_code": response.status_code, "text": response.text}
        self.ensure_mexc_response_ok(payload)
        if not response.ok:
            raise LiveExecutionError(f"live_mexc_order_failed:{response.status_code}:{response.text[:300]}")
        return payload

    def submit_mexc_private_post(self, signed_request):
        response = self.requests.request(
            signed_request["method"],
            signed_request["endpoint"],
            headers=signed_request["headers"],
            data=signed_request["serialized_body"],
            timeout=10,
        )
        try:
            payload = response.json()
        except Exception:
            payload = {"status_code": response.status_code, "text": response.text}
        self.ensure_mexc_response_ok(payload)
        if not response.ok:
            raise LiveExecutionError(f"live_mexc_private_post_failed:{response.status_code}:{response.text[:300]}")
        return payload

    def calculate_tpsl_prices(self, side, entry_price, margin, leverage, take_profit_percent, stop_loss_percent, contract_detail=None):
        notional = Decimal(str(margin)) * Decimal(str(leverage))
        if notional <= 0:
            raise LiveExecutionError("invalid_tpsl_notional")
        entry = Decimal(str(entry_price))
        tp_pnl = Decimal(str(margin)) * Decimal(str(take_profit_percent)) / Decimal("100")
        sl_pnl = Decimal(str(margin)) * Decimal(str(stop_loss_percent)) / Decimal("100")
        tp_move = tp_pnl / notional
        sl_move = sl_pnl / notional
        if side == "long":
            raw_tp = entry * (Decimal("1") + tp_move)
            raw_sl = entry * (Decimal("1") - sl_move)
        else:
            raw_tp = entry * (Decimal("1") - tp_move)
            raw_sl = entry * (Decimal("1") + sl_move)
        return {
            "tp_price": self.quantize_price(raw_tp, contract_detail),
            "sl_price": self.quantize_price(raw_sl, contract_detail),
            "raw_tp_price": float(raw_tp),
            "raw_sl_price": float(raw_sl),
            "tp_pnl": float(tp_pnl),
            "sl_pnl": float(sl_pnl),
            "price_move_tp_pct": float(tp_move * Decimal("100")),
            "price_move_sl_pct": float(sl_move * Decimal("100")),
        }

    def build_mexc_tpsl_requests(self, client, market, side, amount, entry_price, margin, leverage, take_profit_percent, stop_loss_percent, contract_detail=None):
        symbol = market.get("symbol")
        detail = contract_detail or self.fetch_mexc_contract_detail(market)
        vol, volume_details = self.normalize_mexc_contract_volume(market, amount, detail)
        prices = self.calculate_tpsl_prices(side, entry_price, margin, leverage, take_profit_percent, stop_loss_percent, detail)
        close_side = "sell" if side == "long" else "buy"
        close_side_value = self.mexc_swap_side(close_side, True)
        tp_trigger_type = 1 if side == "long" else 2
        sl_trigger_type = 2 if side == "long" else 1

        def body(trigger_price, trigger_type, label):
            return {
                "symbol": market.get("id") or symbol,
                "vol": int(vol) if float(vol).is_integer() else vol,
                "openType": 1,
                "side": close_side_value,
                "leverage": int(leverage),
                "triggerPrice": trigger_price,
                "triggerType": trigger_type,
                "executeCycle": 1,
                "trend": 1,
                "orderType": 5,
            }

        tp_request = self.mexc_private_post_request(client, "planorder/place/v2", body(prices["tp_price"], tp_trigger_type, "tp"))
        sl_request = self.mexc_private_post_request(client, "planorder/place/v2", body(prices["sl_price"], sl_trigger_type, "sl"))
        return {
            "tp_request": tp_request,
            "sl_request": sl_request,
            "prices": prices,
            "volume_details": volume_details,
            "contract_detail": detail,
        }

    def create_mexc_tpsl_orders(self, client, market, side, amount, entry_price, margin, leverage, take_profit_percent, stop_loss_percent, contract_detail=None, on_protection=None):
        requests_payload = self.build_mexc_tpsl_requests(
            client,
            market,
            side,
            amount,
            entry_price,
            margin,
            leverage,
            take_profit_percent,
            stop_loss_percent,
            contract_detail,
        )
        result = {
            "tp_order_id": None,
            "sl_order_id": None,
            "tp_price": requests_payload["prices"]["tp_price"],
            "sl_price": requests_payload["prices"]["sl_price"],
            "created_at": datetime.utcnow(),
            "error": None,
        }
        # Place the stop first and retain every successful ID, even on partial failure.
        for label in ("sl", "tp"):
            try:
                response = self.submit_mexc_private_post(requests_payload[f"{label}_request"])
                result[f"{label}_order_id"] = self.order_id(response)
                if not result[f"{label}_order_id"]:
                    raise SubmissionUnknown("tpsl_missing_order_id")
                if on_protection:
                    on_protection(dict(result))
            except Exception as error:
                result["error"] = f"{label}_protection_unconfirmed:{type(error).__name__}"
                break
        return result

    def cancel_mexc_plan_order(self, client, order_id, symbol):
        if not order_id:
            return None
        signed = self.mexc_private_post_request(client, "planorder/cancel", {"orders": [{"symbol": symbol, "orderId": order_id}]})
        return self.submit_mexc_private_post(signed)

    def cancel_exchange_tpsl_orders(self, config, trade):
        config = self.immutable_config(config, trade)
        if not self.is_mexc_client(self.client(config)):
            return None
        client = self.client(config)
        market = self.market(client, config.symbol)
        cancelled = []
        errors = []
        for order_id in [trade.exchange_tp_order_id, trade.exchange_sl_order_id]:
            if not order_id:
                continue
            try:
                cancelled.append(self.cancel_mexc_plan_order(client, order_id, market["id"]))
            except Exception as error:
                errors.append(str(error))
        return {"cancelled": cancelled, "errors": errors}

    def position_is_open(self, config, trade):
        config = self.immutable_config(config, trade)
        client = self.client(config)
        if not hasattr(client, "fetch_positions"):
            return True
        market = self.market(client, config.symbol)
        symbol = market.get("symbol") or config.symbol
        positions = client.fetch_positions([symbol])
        if not isinstance(positions, list):
            raise SubmissionUnknown("invalid_position_response")
        for position in positions or []:
            position_symbol = position.get("symbol") or (position.get("info") or {}).get("symbol")
            if not position_symbol or str(position_symbol) not in {symbol, market.get("id"), config.symbol}:
                continue
            side = position.get("side")
            if side is None:
                position_type = (position.get("info") or {}).get("positionType")
                side = {1: "long", 2: "short", "1": "long", "2": "short"}.get(position_type)
            if side != trade.side:
                continue
            contracts = position.get("contracts")
            if contracts is None:
                contracts = position.get("amount") or (position.get("info") or {}).get("holdVol")
            try:
                if abs(float(contracts or 0)) > 0:
                    return True
            except (TypeError, ValueError):
                continue
        return False

    def mexc_order_read(self, client, market, client_order_id=None, order_id=None):
        path = f"order/get/{order_id}" if order_id else f"order/external/{market['id']}/{client_order_id}"
        signed = client.sign(path, api=["contract", "private"], method="GET", params={})
        url = signed["url"].replace("https://contract.mexc.com/api/v1/private", "https://api.mexc.com/api/v1/private")
        response = self.requests.request("GET", url, headers=signed.get("headers") or {}, timeout=5)
        if not response.ok:
            raise SubmissionUnknown(f"order_reconciliation_http_{response.status_code}")
        payload = response.json()
        self.ensure_mexc_response_ok(payload)
        data = payload.get("data")
        if not isinstance(data, dict):
            raise SubmissionUnknown("order_reconciliation_missing_data")
        return data

    def private_read(self, client, path, params):
        signed = client.sign(path, api=["contract", "private"], method="GET", params=params)
        url = signed["url"].replace("https://contract.mexc.com", "https://api.mexc.com")
        response = self.requests.request("GET", url, headers=signed.get("headers") or {}, timeout=5)
        if not response.ok:
            raise SubmissionUnknown(f"private_read_http_{response.status_code}")
        payload = response.json()
        self.ensure_mexc_response_ok(payload)
        return payload.get("data")

    def protection_state(self, config, trade, now=None):
        """Read-only verification; never recreate ambiguous or expired plans automatically."""
        now = now or datetime.utcnow()
        client = self.client(self.immutable_config(config, trade))
        if not self.is_mexc_client(client):
            raise SubmissionUnknown("protection_monitor_adapter_unavailable")
        market = self.market(client, trade.symbol)
        ids = {str(trade.exchange_tp_order_id), str(trade.exchange_sl_order_id)}
        if None in (trade.exchange_tp_order_id, trade.exchange_sl_order_id) or len(ids) != 2:
            return {"status": "missing_plan_ids", "protected": False, "expires_at": None}
        since = trade.tp_sl_created_at or trade.opened_at
        rows = []
        for page in range(1, 11):
            data = self.private_read(client, "planorder/list/orders", {
                "symbol": market["id"], "start_time": int(since.replace(tzinfo=timezone.utc).timestamp() * 1000) - 1000,
                "end_time": int(now.replace(tzinfo=timezone.utc).timestamp() * 1000), "page_num": page, "page_size": 100})
            items = data.get("resultList") if isinstance(data, dict) else data
            if not isinstance(items, list):
                raise SubmissionUnknown("invalid_plan_list")
            rows.extend(items)
            if len(items) < 100:
                break
        else:
            raise SubmissionUnknown("plan_list_page_limit")
        found = {str(row.get("id")): row for row in rows if str(row.get("id")) in ids}
        if set(found) != ids:
            return {"status": "plan_missing", "protected": False, "expires_at": None}
        expiries = []
        for label, order_id in (("tp", trade.exchange_tp_order_id), ("sl", trade.exchange_sl_order_id)):
            row = found[str(order_id)]
            if row.get("symbol") != market["id"] or int(row.get("side", 0)) != (4 if trade.side == "long" else 2):
                return {"status": "plan_incompatible", "protected": False, "expires_at": None}
            if int(row.get("state", 0)) != 1:
                return {"status": f"{label}_state_{row.get('state')}", "protected": False, "expires_at": None}
            volume = float(row.get("vol") or 0) * float(market.get("contractSize") or 1)
            expected_price = getattr(trade, f"exchange_{label}_price")
            if abs(volume - trade.amount) > trade.amount * 1e-7 or not expected_price or abs(float(row.get("triggerPrice") or 0) - expected_price) > expected_price * 1e-8:
                return {"status": "plan_parameters_mismatch", "protected": False, "expires_at": None}
            # List response documents executeCycle in hours (not place-request enum).
            created = row.get("createTime")
            cycle = row.get("executeCycle")
            if not created or not cycle or float(cycle) <= 0:
                raise SubmissionUnknown("plan_expiry_unconfirmed")
            expiries.append(datetime.utcfromtimestamp(float(created) / 1000) + timedelta(hours=float(cycle)))
        expiry = min(expiries)
        return {"status": "active" if expiry > now else "expired", "protected": expiry > now, "expires_at": expiry}

    def funding_records(self, config, trade, now=None):
        client = self.client(self.immutable_config(config, trade))
        if not self.is_mexc_client(client):
            raise SubmissionUnknown("funding_adapter_unavailable")
        market = self.market(client, trade.symbol)
        opened = self.mexc_order_read(client, market, order_id=trade.live_exchange_order_id)
        position_id = opened.get("positionId")
        if not position_id:
            raise SubmissionUnknown("funding_position_id_unknown")
        end = trade.closed_at or now or datetime.utcnow()
        since = int(trade.opened_at.replace(tzinfo=timezone.utc).timestamp() * 1000)
        until = int(end.replace(tzinfo=timezone.utc).timestamp() * 1000)
        rows = []
        for page in range(1, 101):
            data = self.private_read(client, "position/funding_records", {"symbol": market["id"],
                "position_id": position_id, "position_type": 1 if trade.side == "long" else 2,
                "start_time": since, "end_time": until, "page_num": page, "page_size": 100})
            if not isinstance(data, dict) or not isinstance(data.get("resultList"), list):
                raise SubmissionUnknown("funding_response_shape_unconfirmed")
            items = data["resultList"]
            for row in items:
                at = int(row.get("settleTime") or 0)
                if row.get("symbol") != market["id"] or int(row.get("positionType", 0)) != (1 if trade.side == "long" else 2) or not since <= at <= until:
                    raise SubmissionUnknown("funding_record_scope_mismatch")
                if row.get("id") is None or row.get("funding") is None:
                    raise SubmissionUnknown("funding_record_incomplete")
                amount = float(row["funding"])
                if not __import__("math").isfinite(amount):
                    raise SubmissionUnknown("funding_record_invalid_amount")
                rows.append({"id": str(row["id"]), "timestamp": at, "amount": amount})
            if len(items) < 100:
                return rows
        raise SubmissionUnknown("funding_page_limit")

    def verified_mexc_order(self, market, data):
        state = int(data.get("state") or 0)
        contracts = float(data.get("dealVol") or 0)
        price = float(data.get("dealAvgPrice") or 0)
        if state in (4, 5) and contracts == 0:
            raise OrderNotFilled("exchange_confirmed_no_fill")
        if state not in (3, 4, 5) or contracts <= 0 or price <= 0:
            raise SubmissionUnknown("order_fill_not_confirmed")
        fee = data.get("totalFee")
        if fee is None and data.get("takerFee") is not None and data.get("makerFee") is not None:
            fee = float(data["takerFee"]) + float(data["makerFee"])
        currency = data.get("feeCurrency")
        if fee is None or (currency and currency != "USDT"):
            raise SubmissionUnknown("order_fee_not_confirmed_in_usdt")
        base_amount = contracts * float(market.get("contractSize") or 1)
        return {"id": str(data["orderId"]), "filled": base_amount, "average": price,
                "status": "closed", "fee": {"cost": float(fee), "currency": "USDT"},
                "raw": data, "partial_fill": contracts < float(data.get("vol") or contracts)}

    def external_close_order(self, config, trade):
        config = self.immutable_config(config, trade)
        client = self.client(config)
        market = self.market(client, trade.symbol)
        opened = self.mexc_order_read(client, market, order_id=trade.live_exchange_order_id)
        position_id = opened.get("positionId")
        if not position_id:
            raise SubmissionUnknown("missing_position_id")
        rows = []
        since = int(trade.opened_at.replace(tzinfo=timezone.utc).timestamp() * 1000)
        for page in range(1, 11):
            signed = client.sign("order/list/history_orders", api=["contract", "private"], method="GET",
                params={"symbol": market["id"], "states": "3,4", "start_time": since, "page_num": page, "page_size": 100})
            response = self.requests.request("GET", signed["url"].replace("https://contract.mexc.com", "https://api.mexc.com"),
                                             headers=signed.get("headers") or {}, timeout=5)
            if not response.ok:
                raise SubmissionUnknown("external_history_unavailable")
            payload = response.json()
            self.ensure_mexc_response_ok(payload)
            data = payload.get("data")
            items = data.get("resultList") if isinstance(data, dict) else data
            if not isinstance(items, list):
                raise SubmissionUnknown("invalid_external_history")
            rows.extend(items)
            if len(items) < 100:
                break
        else:
            raise SubmissionUnknown("external_history_page_limit")
        close_side = 4 if trade.side == "long" else 2
        orders = [self.verified_mexc_order(market, row) for row in rows
                  if str(row.get("positionId")) == str(position_id) and int(row.get("side") or 0) == close_side
                  and row.get("symbol") == market["id"] and float(row.get("dealVol") or 0) > 0
                  and int(row.get("createTime") or 0) >= since]
        amount = sum(order["filled"] for order in orders)
        if not orders or abs(amount - trade.amount) > trade.amount * 1e-7:
            raise SubmissionUnknown("external_close_quantity_unconfirmed")
        return {"id": ",".join(order["id"] for order in orders), "filled": amount,
                "average": sum(order["average"] * order["filled"] for order in orders) / amount,
                "fee": {"cost": sum(order["fee"]["cost"] for order in orders)},
                "raw": [order["raw"] for order in orders],
                "realized_pnl": sum(float(order["raw"]["profit"]) for order in orders) if all("profit" in order["raw"] for order in orders) else None}

    def create_mexc_swap_order(self, client, market, order_type, side, amount, price=None, reduce_only=False, leverage=None, client_order_id=None):
        if not client_order_id:
            raise LiveExecutionError("durable_client_order_id_required")
        signed_request = self.build_mexc_order_submit_request(client, market, order_type, side, amount, price, reduce_only, leverage, client_order_id=client_order_id)
        try:
            response = self.submit_mexc_signed_order(signed_request)
        except (requests.Timeout, requests.ConnectionError) as error:
            raise SubmissionUnknown("submit_outcome_unknown") from error
        order_id = self.order_id(response)
        try:
            data = self.mexc_order_read(client, market, client_order_id, order_id or None)
            return self.verified_mexc_order(market, data)
        except OrderNotFilled:
            raise
        except Exception as error:
            raise SubmissionUnknown("submit_accepted_fill_unconfirmed") from error

    def create_futures_order(self, client, market, order_type, side, amount, price=None, reduce_only=False, leverage=None, client_order_id=None):
        symbol = market.get("symbol")
        if self.is_mexc_client(client) and self.is_live_futures_market(market):
            return self.create_mexc_swap_order(client, market, order_type, side, amount, price, reduce_only, leverage, client_order_id)
        if not client_order_id:
            raise LiveExecutionError("durable_client_order_id_required")
        contracts = float(self.amount_to_precision(client, symbol, self.contract_amount(market, amount)))
        if contracts <= 0:
            raise LiveExecutionError("order_below_contract_precision")
        params = {"reduceOnly": bool(reduce_only), "clientOrderId": client_order_id}
        if leverage:
            params["leverage"] = int(leverage)
        try:
            order = client.create_order(symbol, order_type, side, contracts, price, params)
        except (ccxt.NetworkError, requests.Timeout, requests.ConnectionError) as error:
            raise SubmissionUnknown("submit_outcome_unknown") from error
        if order.get("status") != "closed" and order.get("id") and getattr(client, "has", {}).get("fetchOrder"):
            order = client.fetch_order(order["id"], symbol)
        if order.get("status") != "closed" or not order.get("filled") or not order.get("average"):
            raise SubmissionUnknown("order_fill_not_confirmed")
        if not isinstance(order.get("fee"), dict) or order["fee"].get("cost") is None:
            raise SubmissionUnknown("order_fee_not_confirmed")
        if order["fee"].get("currency") not in (None, "USDT"):
            raise SubmissionUnknown("order_fee_currency_unconfirmed")
        order = dict(order)
        order["filled"] = float(order["filled"]) * float(market.get("contractSize") or 1)
        return order

    def open_position(self, config, side, margin, leverage, entry_price, client_order_id=None, verified_order=None, on_fill=None, on_protection=None):
        client = self.client(config)
        market = self.market(client, config.symbol)
        symbol = market.get("symbol") or config.symbol
        if not verified_order:
            positions = client.fetch_positions([symbol])
            if not isinstance(positions, list):
                raise LiveExecutionError("live_position_check_unavailable")
            for position in positions:
                if position.get("symbol") not in {symbol, market.get("id")}:
                    continue
                amount_open = position.get("contracts")
                if amount_open is None:
                    amount_open = (position.get("info") or {}).get("holdVol")
                if amount_open is None:
                    raise LiveExecutionError("live_position_quantity_unknown")
                if abs(float(amount_open)) > 0:
                    raise LiveExecutionError("exchange_position_already_exists")
        if not self.is_mexc_client(client) and hasattr(client, "set_leverage"):
            try:
                client.set_leverage(int(leverage), symbol)
            except Exception as error:
                raise LiveExecutionError("live_leverage_configuration_failed") from error
        notional = float(margin) * float(leverage)
        amount = notional / float(entry_price)
        order_side = "buy" if side == "long" else "sell"
        order = verified_order or self.create_futures_order(client, market, config.live_order_type, order_side, amount, entry_price, False, leverage, client_order_id)
        average = self.average_price(order, entry_price)
        warning = None
        filled = self.filled_amount(order, amount)
        fee = self.fee_cost(order)
        if on_fill:
            on_fill({"order_id": self.order_id(order), "average_fill_price": average, "filled_amount": filled,
                     "fee": fee, "raw_response": order, "status": "tp_sl_unprotected", "tpsl_error": "protection_pending"})
        tpsl = None
        tpsl_error = None
        if self.is_mexc_client(client) and self.is_live_futures_market(market):
            try:
                tpsl = self.create_mexc_tpsl_orders(
                    client,
                    market,
                    side,
                    filled,
                    average,
                    average * filled / float(leverage),
                    leverage,
                    config.take_profit_percent_of_margin,
                    config.stop_loss_percent_of_margin,
                    on_protection=on_protection,
                )
                tpsl_error = tpsl.get("error")
            except Exception as error:
                tpsl_error = str(error)
                warning = "exchange_tpsl_not_created"
        else:
            tpsl_error = "exchange_protection_adapter_unavailable"
        return {
            "order_id": self.order_id(order),
            "average_fill_price": average,
            "filled_amount": filled,
            "fee": fee,
            "raw_response": order,
            "status": ("tp_sl_unprotected" if tpsl_error else "open"),
            "warning": tpsl_error or warning,
            "tpsl": tpsl,
            "tpsl_error": tpsl_error,
        }

    def close_position(self, config, trade, current_price):
        config = self.immutable_config(config, trade)
        client = self.client(config)
        market = self.market(client, config.symbol)
        symbol = market.get("symbol") or config.symbol
        order_side = "sell" if trade.side == "long" else "buy"
        tpsl_cancel = None
        order = self.create_futures_order(
            client,
            market,
            config.live_order_type,
            order_side,
            trade.live_filled_amount or trade.amount,
            current_price,
            bool(config.live_reduce_only_close),
            trade.leverage,
            trade.live_close_client_order_id,
        )
        average = self.average_price(order, current_price)
        fee = self.fee_cost(order)
        filled = float(order.get("filled") or 0)
        if filled < float(trade.live_filled_amount or trade.amount) * (1 - 1e-8):
            raise SubmissionUnknown("close_partial_fill_requires_reconciliation")
        # Local verified-close commit owns cleanup; never cancel twice or before that commit.
        return {
            "order_id": self.order_id(order),
            "average_fill_price": average,
            "fee": fee,
            "raw_response": order,
            "status": order.get("status", "closed") if isinstance(order, dict) else "closed",
            "warning": None,
            "tpsl_cancel": tpsl_cancel,
        }

    @staticmethod
    def raw_json(payload):
        return json.dumps(payload, default=str)
