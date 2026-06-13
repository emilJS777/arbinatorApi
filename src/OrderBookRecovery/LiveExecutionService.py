import json
import logging
import os
from datetime import datetime
from decimal import Decimal, ROUND_DOWN

import ccxt
import requests

from src.Exchange.ExchangeModel import Exchange


class LiveExecutionError(Exception):
    pass


logger = logging.getLogger(__name__)


class LiveExecutionService:
    def __init__(self, client_factory=None, requests_client=None):
        self.client_factory = client_factory
        self.requests = requests_client or requests

    def hard_disabled(self):
        return str(os.environ.get("LIVE_TRADING_HARD_DISABLED", "false")).lower() == "true"

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
        return klass({key: value for key, value in options.items() if value is not None})

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
        return str(order.get("id") or order.get("orderId") or order.get("data") or "") if isinstance(order, dict) else ""

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
        return float(rounded), {
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
            return 6
        if order_type == "limit":
            return 1
        return order_type

    def ensure_mexc_response_ok(self, response):
        if not isinstance(response, dict):
            return
        code = response.get("code")
        success = response.get("success")
        if str(code) == "6026":
            raise LiveExecutionError("mexc_risk_control_verification_required")
        if success is False or (code not in (None, 0, 200, "0", "200")):
            raise LiveExecutionError(f"live_mexc_order_failed:{response}")

    def build_mexc_order_submit_request(self, client, market, order_type, side, amount, price=None, reduce_only=False, leverage=None, contract_detail=None):
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
        if request_type not in (6, "6") and price is not None:
            body["price"] = self.price_to_precision(client, symbol, price)

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
                "externalOid": f"arbinator_{label}_{int(datetime.utcnow().timestamp() * 1000)}",
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

    def create_mexc_tpsl_orders(self, client, market, side, amount, entry_price, margin, leverage, take_profit_percent, stop_loss_percent, contract_detail=None):
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
        tp_response = self.submit_mexc_private_post(requests_payload["tp_request"])
        sl_response = self.submit_mexc_private_post(requests_payload["sl_request"])
        return {
            "tp_order_id": self.order_id(tp_response),
            "sl_order_id": self.order_id(sl_response),
            "tp_price": requests_payload["prices"]["tp_price"],
            "sl_price": requests_payload["prices"]["sl_price"],
            "raw_tp_response": tp_response,
            "raw_sl_response": sl_response,
            "requests": {
                "tp": self.sanitize_signed_order_request(requests_payload["tp_request"]),
                "sl": self.sanitize_signed_order_request(requests_payload["sl_request"]),
            },
            "created_at": datetime.utcnow(),
        }

    def cancel_mexc_plan_order(self, client, order_id):
        if not order_id:
            return None
        signed = self.mexc_private_post_request(client, "planorder/cancel", {"orderId": order_id})
        return self.submit_mexc_private_post(signed)

    def cancel_exchange_tpsl_orders(self, config, trade):
        if not self.is_mexc_client(self.client(config)):
            return None
        client = self.client(config)
        cancelled = []
        errors = []
        for order_id in [trade.exchange_tp_order_id, trade.exchange_sl_order_id]:
            if not order_id:
                continue
            try:
                cancelled.append(self.cancel_mexc_plan_order(client, order_id))
            except Exception as error:
                errors.append(str(error))
        return {"cancelled": cancelled, "errors": errors}

    def position_is_open(self, config, trade):
        client = self.client(config)
        if not hasattr(client, "fetch_positions"):
            return True
        market = self.market(client, config.symbol)
        symbol = market.get("symbol") or config.symbol
        positions = client.fetch_positions()
        for position in positions or []:
            position_symbol = position.get("symbol") or (position.get("info") or {}).get("symbol")
            if position_symbol and str(position_symbol) not in {symbol, market.get("id"), config.symbol}:
                continue
            contracts = position.get("contracts")
            if contracts is None:
                contracts = position.get("contractSize") or position.get("amount") or (position.get("info") or {}).get("holdVol")
            try:
                if abs(float(contracts or 0)) > 0:
                    return True
            except (TypeError, ValueError):
                continue
        return False

    def create_mexc_swap_order(self, client, market, order_type, side, amount, price=None, reduce_only=False, leverage=None):
        signed_request = self.build_mexc_order_submit_request(client, market, order_type, side, amount, price, reduce_only, leverage)
        response = self.submit_mexc_signed_order(signed_request)
        order_id = self.order_id(response)
        symbol = market.get("symbol")

        return {
            "id": order_id,
            "symbol": symbol,
            "type": order_type,
            "side": side,
            "amount": amount,
            "filled": amount,
            "average": price,
            "status": "open",
            "fee": {"cost": 0},
            "raw": response,
            "request": self.sanitize_signed_order_request(signed_request),
        }

    def create_futures_order(self, client, market, order_type, side, amount, price=None, reduce_only=False, leverage=None):
        symbol = market.get("symbol")
        if self.is_mexc_client(client) and self.is_live_futures_market(market):
            return self.create_mexc_swap_order(client, market, order_type, side, amount, price, reduce_only, leverage)
        params = {"reduceOnly": bool(reduce_only)}
        if leverage:
            params["leverage"] = int(leverage)
        return client.create_order(symbol, order_type, side, amount, price, params)

    def open_position(self, config, side, margin, leverage, entry_price):
        client = self.client(config)
        market = self.market(client, config.symbol)
        symbol = market.get("symbol") or config.symbol
        if hasattr(client, "set_leverage"):
            try:
                client.set_leverage(int(leverage), symbol)
            except Exception:
                pass
        notional = float(margin) * float(leverage)
        amount = notional / float(entry_price)
        order_side = "buy" if side == "long" else "sell"
        order = self.create_futures_order(client, market, config.live_order_type, order_side, amount, entry_price, False, leverage)
        average = self.average_price(order, entry_price)
        warning = None
        if average == entry_price:
            warning = "entry_price_fallback_used"
        filled = self.filled_amount(order, amount)
        fee = self.fee_cost(order)
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
                    margin,
                    leverage,
                    config.take_profit_percent_of_margin,
                    config.stop_loss_percent_of_margin,
                )
            except Exception as error:
                tpsl_error = str(error)
                warning = "exchange_tpsl_not_created"
        return {
            "order_id": self.order_id(order),
            "average_fill_price": average,
            "filled_amount": filled,
            "fee": fee,
            "raw_response": order,
            "status": ("tp_sl_unprotected" if tpsl_error else (order.get("status", "open") if isinstance(order, dict) else "open")),
            "warning": tpsl_error or warning or (None if fee else "fee_not_returned"),
            "tpsl": tpsl,
            "tpsl_error": tpsl_error,
        }

    def close_position(self, config, trade, current_price):
        client = self.client(config)
        market = self.market(client, config.symbol)
        symbol = market.get("symbol") or config.symbol
        order_side = "sell" if trade.side == "long" else "buy"
        tpsl_cancel = None
        if trade.exchange_tp_order_id or trade.exchange_sl_order_id:
            tpsl_cancel = self.cancel_exchange_tpsl_orders(config, trade)
        order = self.create_futures_order(
            client,
            market,
            config.live_order_type,
            order_side,
            trade.live_filled_amount or trade.amount,
            current_price,
            bool(config.live_reduce_only_close),
            trade.leverage,
        )
        average = self.average_price(order, current_price)
        fee = self.fee_cost(order)
        return {
            "order_id": self.order_id(order),
            "average_fill_price": average,
            "fee": fee,
            "raw_response": order,
            "status": order.get("status", "closed") if isinstance(order, dict) else "closed",
            "warning": None if fee else "fee_not_returned",
            "tpsl_cancel": tpsl_cancel,
        }

    @staticmethod
    def raw_json(payload):
        return json.dumps(payload, default=str)
