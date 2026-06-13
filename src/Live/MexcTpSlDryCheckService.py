from src.Exchange.ExchangeModel import Exchange
from src.OrderBookRecovery.LiveExecutionService import LiveExecutionService
from src.__Parents.Response import Response


class MexcTpSlDryCheckService(Response):
    def __init__(self, live_execution_service=None):
        self.live_execution_service = live_execution_service or LiveExecutionService()

    def exchange_record(self):
        return Exchange.query.filter(Exchange.title.ilike("mexc")).order_by(Exchange.id.asc()).first()

    def client(self, exchange):
        class Config:
            pass

        Config.exchange_id = exchange.id
        Config.exchange = exchange.title
        return self.live_execution_service.client(Config)

    def sanitize(self, signed_request):
        return self.live_execution_service.sanitize_signed_order_request(signed_request)

    def run(self, body):
        exchange = self.exchange_record()
        if not exchange or not exchange.api_key or not exchange.api_secret:
            return self.response(False, {"msg": "mexc_credentials_not_configured"}, 404)
        try:
            symbol = body.get("symbol", "BTC/USDT")
            side = body.get("side", "long")
            if side not in {"long", "short"}:
                return self.response(False, {"msg": "invalid_side"}, 400)
            margin = float(body.get("margin_usdt", body.get("margin", 10)))
            leverage = float(body.get("leverage", 2))
            entry_price = float(body.get("entry_price", body.get("price", 100)))
            tp_percent = float(body.get("take_profit_percent_of_margin", body.get("tp_percent", 0.25)))
            sl_percent = float(body.get("stop_loss_percent_of_margin", body.get("sl_percent", 0.3)))
            notional = margin * leverage
            amount = notional / entry_price
            client = self.client(exchange)
            market = self.live_execution_service.resolve_live_futures_market(client, symbol)
            contract_detail = self.live_execution_service.fetch_mexc_contract_detail(market)
            payload = self.live_execution_service.build_mexc_tpsl_requests(
                client,
                market,
                side,
                amount,
                entry_price,
                margin,
                leverage,
                tp_percent,
                sl_percent,
                contract_detail,
            )
            return self.response_ok({
                "dry_run": True,
                "symbol": symbol,
                "resolved_symbol": market.get("symbol"),
                "entry_price": entry_price,
                "margin": margin,
                "leverage": leverage,
                "notional": notional,
                "amount": amount,
                "calculated": payload["prices"],
                "tp_request": self.sanitize(payload["tp_request"]),
                "sl_request": self.sanitize(payload["sl_request"]),
                "contract_detail": contract_detail,
                "volume_details": payload["volume_details"],
            })
        except Exception as error:
            return self.response(False, {"msg": f"{error.__class__.__name__}: {error}"}, 400)
