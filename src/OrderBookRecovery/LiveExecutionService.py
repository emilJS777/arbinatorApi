import json
import logging
import os

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

    def amount_to_precision(self, client, symbol, amount):
        if hasattr(client, "amount_to_precision"):
            return float(client.amount_to_precision(symbol, amount))
        return float(amount)

    def price_to_precision(self, client, symbol, price):
        if hasattr(client, "price_to_precision"):
            return float(client.price_to_precision(symbol, price))
        return float(price)

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
        if success is False or (code not in (None, 0, 200, "0", "200")):
            raise LiveExecutionError(f"live_mexc_order_failed:{response}")

    def build_mexc_order_submit_request(self, client, market, order_type, side, amount, price=None, reduce_only=False, leverage=None):
        symbol = market.get("symbol")
        request_type = self.mexc_order_type(order_type)
        vol = self.amount_to_precision(client, symbol, self.contract_amount(market, amount))
        body = {
            "symbol": market.get("id") or symbol,
            "vol": vol,
            "type": request_type,
            "openType": 1,
            "side": self.mexc_swap_side(side, reduce_only),
        }
        if leverage:
            body["leverage"] = int(leverage)
        if price is not None:
            body["price"] = self.price_to_precision(client, symbol, price)

        signed = client.sign("order/submit", api=["contract", "private"], method="POST", params=body)
        headers = signed.get("headers") or {}
        serialized_body = signed.get("body")
        return {
            "endpoint": signed["url"],
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
            "resolved_symbol": signed_request["resolved_symbol"],
            "side_mapping": signed_request["side_mapping"],
            "order_type_mapping": signed_request["order_type_mapping"],
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
        if not response.ok:
            raise LiveExecutionError(f"live_mexc_order_failed:{response.status_code}:{response.text[:300]}")
        self.ensure_mexc_response_ok(payload)
        return payload

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
        filled = self.filled_amount(order, amount)
        fee = self.fee_cost(order)
        return {
            "order_id": self.order_id(order),
            "average_fill_price": average,
            "filled_amount": filled,
            "fee": fee,
            "raw_response": order,
            "status": order.get("status", "open") if isinstance(order, dict) else "open",
            "warning": None if fee else "fee_not_returned",
        }

    def close_position(self, config, trade, current_price):
        client = self.client(config)
        market = self.market(client, config.symbol)
        symbol = market.get("symbol") or config.symbol
        order_side = "sell" if trade.side == "long" else "buy"
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
        }

    @staticmethod
    def raw_json(payload):
        return json.dumps(payload, default=str)
