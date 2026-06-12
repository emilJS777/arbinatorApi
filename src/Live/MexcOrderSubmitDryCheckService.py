import logging
import time
import hmac
import hashlib
from urllib.parse import urlencode

from src.Exchange.ExchangeModel import Exchange
from src.OrderBookRecovery.LiveExecutionService import LiveExecutionService
from src.__Parents.Response import Response


logger = logging.getLogger(__name__)


class MexcOrderSubmitDryCheckService(Response):
    ORDER_SUBMIT_FORMATS = [
        "ccxt_json",
        "json_price_omitted",
        "json_price_zero",
        "json_price_current",
        "json_integer_vol",
        "json_type6_price_omitted",
        "json_type6_price_zero",
        "json_type6_price_current",
        "json_type6_integer_vol",
        "form_urlencoded_type6",
        "json_type6_user_agent",
        "json_new_base_order_submit_type6",
        "json_new_base_order_create_type6",
        "json_old_base_order_create_type6",
        "json_new_base_order_place_type6",
        "json_new_base_submit_batch_type6",
        "json_new_base_planorder_place_v2_type6",
        "json_no_source",
        "json_user_agent",
        "form_urlencoded",
        "form_urlencoded_no_source",
        "ccxt_raw_request",
    ]

    def __init__(self, live_execution_service=None):
        self.live_execution_service = live_execution_service or LiveExecutionService()

    def exchange_record(self):
        return Exchange.query.filter(Exchange.title.ilike("mexc")).order_by(Exchange.id.asc()).first()

    def log_request(self, endpoint, method, status_code=None, error=None, duration_ms=None):
        logger.info(
            "MEXC order submit drycheck endpoint=%s method=%s status_code=%s error_class=%s duration_ms=%s",
            endpoint,
            method,
            status_code,
            error.__class__.__name__ if error else None,
            duration_ms,
        )

    def log_submit_attempt(self, variant, response=None, error=None, duration_ms=None):
        response_headers = dict(getattr(response, "headers", {}) or {}) if response is not None else {}
        request_headers = variant.get("headers") or {}
        logger.info(
            "MEXC order submit diagnostic endpoint=%s method=%s status_code=%s error_class=%s duration_ms=%s request_headers=%s content_type=%s request_time=%s response_headers=%s akamai_headers=%s serialized_body=%s",
            variant.get("endpoint"),
            variant.get("method"),
            getattr(response, "status_code", None),
            error.__class__.__name__ if error else None,
            duration_ms,
            sorted(request_headers.keys()),
            request_headers.get("Content-Type"),
            request_headers.get("Request-Time"),
            sorted(response_headers.keys()),
            {key: value for key, value in response_headers.items() if "akamai" in key.lower() or key.lower().startswith("x-")},
            variant.get("serialized_body"),
        )

    def client(self, exchange):
        class Config:
            pass

        Config.exchange_id = exchange.id
        Config.exchange = exchange.title
        return self.live_execution_service.client(Config)

    def order_inputs(self, body):
        margin = float(body.get("margin_usdt", 1))
        leverage = int(body.get("leverage", 1))
        price = float(body.get("price", body.get("current_price", 100)))
        current_market_price = float(
            body.get(
                "current_mark_price",
                body.get("mark_price", body.get("last_price", body.get("current_price", price))),
            )
        )
        side = body.get("side", "long")
        order_type = body.get("order_type", "market")
        symbol = body.get("symbol", "BTC/USDT")
        if side not in {"long", "short"}:
            raise ValueError("invalid_side")
        if order_type not in {"market", "limit"}:
            raise ValueError("invalid_order_type")
        if margin <= 0 or leverage <= 0 or price <= 0:
            raise ValueError("invalid_margin_leverage_or_price")
        notional = margin * leverage
        amount = notional / price
        order_side = "buy" if side == "long" else "sell"
        return {
            "symbol": symbol,
            "side": side,
            "order_side": order_side,
            "order_type": order_type,
            "margin": margin,
            "leverage": leverage,
            "price": price,
            "current_market_price": current_market_price,
            "notional": notional,
            "amount": amount,
        }

    def sanitize_headers(self, headers):
        return {key: "<redacted>" for key in (headers or {}).keys()}

    def response_header_details(self, response):
        headers = dict(getattr(response, "headers", {}) or {})
        return {
            "headers": headers,
            "akamai_headers": {key: value for key, value in headers.items() if "akamai" in key.lower() or key.lower().startswith("x-")},
            "content_type": headers.get("Content-Type") or headers.get("content-type"),
        }

    def integerize_order_body(self, body):
        next_body = dict(body)
        for key in ("vol", "type", "openType", "side", "leverage", "price"):
            value = next_body.get(key)
            if isinstance(value, float) and value.is_integer():
                next_body[key] = int(value)
        return next_body

    def body_without_price(self, body):
        next_body = dict(body)
        next_body.pop("price", None)
        return next_body

    def body_with_price(self, body, price):
        next_body = dict(body)
        next_body["price"] = price
        return next_body

    def body_with_type(self, body, order_type):
        next_body = dict(body)
        next_body["type"] = order_type
        return next_body

    def direct_signed_request(
        self,
        client,
        body,
        content_type="application/json",
        include_source=True,
        user_agent=None,
        path="order/submit",
        endpoint_base_url=None,
        base_url_family=None,
    ):
        signed = client.sign(path, api=["contract", "private"], method="POST", params=body)
        headers = dict(signed.get("headers") or {})
        if not include_source:
            headers.pop("source", None)
        if user_agent:
            headers["User-Agent"] = user_agent
        if content_type == "application/x-www-form-urlencoded":
            serialized_body = urlencode(body)
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            # Re-sign against the exact form-encoded payload for this diagnostic variant.
            request_time = headers.get("Request-Time")
            auth = f"{client.apiKey}{request_time}{serialized_body}"
            headers["Signature"] = hmac.new(str(client.secret).encode(), auth.encode(), hashlib.sha256).hexdigest()
        else:
            serialized_body = signed.get("body")
            headers["Content-Type"] = "application/json"
        endpoint = signed["url"]
        if endpoint_base_url:
            endpoint = endpoint_base_url.rstrip("/") + "/" + path
        return {
            "endpoint": endpoint,
            "method": signed.get("method") or "POST",
            "headers": headers,
            "body": body,
            "serialized_body": serialized_body,
            "signature_payload_preview": f"ApiKey + Request-Time + {serialized_body}",
            "request_time": headers.get("Request-Time"),
            "content_type": headers.get("Content-Type"),
            "endpoint_path": path,
            "base_url_family": base_url_family or "installed_ccxt_contract_private",
        }

    def alternative_submit_requests(self, client, signed, current_market_price=None):
        body = signed["body"]
        integer_body = self.integerize_order_body(body)
        current_price_body = self.body_with_price(integer_body, current_market_price) if current_market_price else integer_body
        type6_body = self.body_with_type(integer_body, 6)
        new_contract_private_base = "https://api.mexc.com/api/v1/private"
        old_contract_private_base = "https://contract.mexc.com/api/v1/private"
        browser_user_agent = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/126.0.0.0 Safari/537.36"
        )
        variants = {
            "ccxt_json": signed,
            "json_price_omitted": self.direct_signed_request(client, self.body_without_price(integer_body)),
            "json_price_zero": self.direct_signed_request(client, self.body_with_price(integer_body, 0)),
            "json_price_current": self.direct_signed_request(client, current_price_body),
            "json_integer_vol": self.direct_signed_request(client, integer_body),
            "json_type6_price_omitted": self.direct_signed_request(
                client,
                self.body_without_price(type6_body),
            ),
            "json_type6_price_zero": self.direct_signed_request(
                client,
                self.body_with_price(type6_body, 0),
            ),
            "json_type6_price_current": self.direct_signed_request(
                client,
                self.body_with_type(current_price_body, 6),
            ),
            "json_type6_integer_vol": self.direct_signed_request(
                client,
                type6_body,
            ),
            "form_urlencoded_type6": self.direct_signed_request(
                client,
                type6_body,
                content_type="application/x-www-form-urlencoded",
            ),
            "json_type6_user_agent": self.direct_signed_request(
                client,
                type6_body,
                user_agent=browser_user_agent,
            ),
            "json_new_base_order_submit_type6": self.direct_signed_request(
                client,
                self.body_without_price(type6_body),
                path="order/submit",
                endpoint_base_url=new_contract_private_base,
                base_url_family="latest_ccxt_api_mexc_contract_private",
            ),
            "json_new_base_order_create_type6": self.direct_signed_request(
                client,
                self.body_without_price(type6_body),
                path="order/create",
                endpoint_base_url=new_contract_private_base,
                base_url_family="latest_ccxt_api_mexc_contract_private",
            ),
            "json_old_base_order_create_type6": self.direct_signed_request(
                client,
                self.body_without_price(type6_body),
                path="order/create",
                endpoint_base_url=old_contract_private_base,
                base_url_family="legacy_contract_mexc_private",
            ),
            "json_new_base_order_place_type6": self.direct_signed_request(
                client,
                self.body_without_price(type6_body),
                path="order/place",
                endpoint_base_url=new_contract_private_base,
                base_url_family="latest_ccxt_api_mexc_contract_private",
            ),
            "json_new_base_submit_batch_type6": self.direct_signed_request(
                client,
                [self.body_without_price(type6_body)],
                path="order/submit_batch",
                endpoint_base_url=new_contract_private_base,
                base_url_family="latest_ccxt_api_mexc_contract_private",
            ),
            "json_new_base_planorder_place_v2_type6": self.direct_signed_request(
                client,
                self.body_without_price(type6_body),
                path="planorder/place/v2",
                endpoint_base_url=new_contract_private_base,
                base_url_family="latest_ccxt_api_mexc_contract_private",
            ),
            "ccxt_raw_request": signed,
            "json_no_source": self.direct_signed_request(client, body, include_source=False),
            "json_user_agent": self.direct_signed_request(client, integer_body, user_agent=browser_user_agent),
            "form_urlencoded": self.direct_signed_request(client, integer_body, content_type="application/x-www-form-urlencoded"),
            "form_urlencoded_no_source": self.direct_signed_request(client, integer_body, content_type="application/x-www-form-urlencoded", include_source=False),
        }
        sanitized = {}
        for name, item in variants.items():
            sanitized[name] = {
                "endpoint": item["endpoint"],
                "method": item["method"],
                "sanitized_headers": self.sanitize_headers(item.get("headers")),
                "headers_names_used": sorted((item.get("headers") or {}).keys()),
                "serialized_body": item["serialized_body"],
                "body": item["body"],
                "content_type": (item.get("headers") or {}).get("Content-Type"),
                "request_time": (item.get("headers") or {}).get("Request-Time"),
                "signature_payload_preview": item["signature_payload_preview"],
                "source_header_used": "source" in (item.get("headers") or {}),
                "user_agent_used": (item.get("headers") or {}).get("User-Agent"),
                "endpoint_path": item.get("endpoint_path"),
                "base_url_family": item.get("base_url_family"),
            }
        return variants, sanitized

    def submit_variant(self, variant):
        return self.live_execution_service.requests.request(
            variant["method"],
            variant["endpoint"],
            headers=variant["headers"],
            data=variant["serialized_body"],
            timeout=10,
        )

    def run(self, body):
        exchange = self.exchange_record()
        if not exchange or not exchange.api_key or not exchange.api_secret:
            return self.response(False, {"msg": "mexc_credentials_not_configured"}, 404)

        try:
            inputs = self.order_inputs(body or {})
            client = self.client(exchange)
            market = self.live_execution_service.resolve_live_futures_market(client, inputs["symbol"])
            contract_detail = self.live_execution_service.fetch_mexc_contract_detail(market)
            signed = self.live_execution_service.build_mexc_order_submit_request(
                client,
                market,
                inputs["order_type"],
                inputs["order_side"],
                inputs["amount"],
                inputs["price"],
                False,
                inputs["leverage"],
                contract_detail=contract_detail,
            )
            payload = self.live_execution_service.sanitize_signed_order_request(signed)
            variants, sanitized_variants = self.alternative_submit_requests(
                client,
                signed,
                current_market_price=inputs["current_market_price"],
            )
            payload.update({
                "dry_run": not bool(body.get("confirm_real_order_test")),
                "confirm_real_order_test": bool(body.get("confirm_real_order_test")),
                "available_submit_formats": self.ORDER_SUBMIT_FORMATS,
                "submit_format": body.get("submit_format") or "ccxt_json",
                "submit_format_requests": sanitized_variants,
                "margin_usdt": inputs["margin"],
                "leverage": inputs["leverage"],
                "notional": inputs["notional"],
                "amount": inputs["amount"],
                "price_inputs": {
                    "requested_price": inputs["price"],
                    "current_market_price": inputs["current_market_price"],
                },
                "api_format_notes": {
                    "endpoint_expected": "https://api.mexc.com/api/v1/private/order/create",
                    "old_endpoint": "https://contract.mexc.com/api/v1/private/order/submit",
                    "signature_method": "ApiKey + Request-Time + exact serialized body",
                    "body_serialization": "diagnostics include JSON and form-urlencoded alternatives",
                    "mexc_docs_market_order_type": 5,
                    "ccxt_mexc_swap_market_order_type": 6,
                    "market_price_variants": ["price omitted", "price=0", "price=current mark/last/current"],
                    "type6_hypothesis": "ccxt maps MEXC swap market orders to type=6 (convert market price to current price)",
                    "latest_ccxt_contract_private_base": "https://api.mexc.com/api/v1/private",
                    "installed_ccxt_contract_private_base": "https://contract.mexc.com/api/v1/private",
                    "latest_ccxt_observed_trade_paths": [
                        "order/create",
                        "order/submit",
                        "order/submit_batch",
                        "planorder/place/v2",
                    ],
                    "volume_format": "diagnostics include integer contract vol when integral",
                    "side_values": {
                        "open_long": 1,
                        "close_short": 2,
                        "open_short": 3,
                        "close_long": 4,
                    },
                },
            })
            if not body.get("confirm_real_order_test"):
                return self.response_ok(payload)

            submit_format = body.get("submit_format") or "ccxt_json"
            if submit_format not in variants:
                return self.response(False, {"msg": "invalid_submit_format", "available_submit_formats": self.ORDER_SUBMIT_FORMATS}, 400)
            start = time.time()
            try:
                variant = variants[submit_format]
                response = self.submit_variant(variant)
                duration_ms = round((time.time() - start) * 1000, 2)
                self.log_request(variant["endpoint"], variant["method"], status_code=response.status_code, duration_ms=duration_ms)
                self.log_submit_attempt(variant, response=response, duration_ms=duration_ms)
                payload["real_order_sent"] = True
                payload["real_order_submit_format"] = submit_format
                payload["real_order_status_code"] = response.status_code
                payload["real_order_response_headers"] = self.response_header_details(response)
                payload["real_order_request"] = sanitized_variants[submit_format]
                payload["real_order_response_preview"] = (response.text or "")[:500]
                if not response.ok:
                    return self.response(False, payload, 400)
                return self.response_ok(payload)
            except Exception as error:
                duration_ms = round((time.time() - start) * 1000, 2)
                self.log_request(signed["endpoint"], signed["method"], error=error, duration_ms=duration_ms)
                self.log_submit_attempt(signed, error=error, duration_ms=duration_ms)
                payload["real_order_sent"] = True
                payload["real_order_error"] = f"{error.__class__.__name__}: {error}"
                return self.response(False, payload, 400)
        except Exception as error:
            return self.response(False, {"msg": f"{error.__class__.__name__}: {error}"}, 400)
