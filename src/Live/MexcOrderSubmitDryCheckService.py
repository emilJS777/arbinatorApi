import logging
import time

from src.Exchange.ExchangeModel import Exchange
from src.OrderBookRecovery.LiveExecutionService import LiveExecutionService
from src.__Parents.Response import Response


logger = logging.getLogger(__name__)


class MexcOrderSubmitDryCheckService(Response):
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
            "notional": notional,
            "amount": amount,
        }

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
            payload.update({
                "dry_run": not bool(body.get("confirm_real_order_test")),
                "confirm_real_order_test": bool(body.get("confirm_real_order_test")),
                "margin_usdt": inputs["margin"],
                "leverage": inputs["leverage"],
                "notional": inputs["notional"],
                "amount": inputs["amount"],
                "api_format_notes": {
                    "endpoint_expected": "https://contract.mexc.com/api/v1/private/order/submit",
                    "signature_method": "ApiKey + Request-Time + exact JSON body",
                    "body_serialization": "application/json",
                    "market_order_type": 5,
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

            start = time.time()
            try:
                result = self.live_execution_service.submit_mexc_signed_order(signed)
                duration_ms = round((time.time() - start) * 1000, 2)
                self.log_request(signed["endpoint"], signed["method"], status_code=200, duration_ms=duration_ms)
                payload["real_order_sent"] = True
                payload["real_order_response_preview"] = str(result)[:500]
                return self.response_ok(payload)
            except Exception as error:
                duration_ms = round((time.time() - start) * 1000, 2)
                self.log_request(signed["endpoint"], signed["method"], error=error, duration_ms=duration_ms)
                payload["real_order_sent"] = True
                payload["real_order_error"] = f"{error.__class__.__name__}: {error}"
                return self.response(False, payload, 400)
        except Exception as error:
            return self.response(False, {"msg": f"{error.__class__.__name__}: {error}"}, 400)
