from src.Exchange.ExchangeRepositoryInterface import ExchangeRepositoryInterface
from src.__Parents.Response import Response
from src.__Parents.ExchangeGetter import ExchangeGetter
from src.config import LIVE_TRADING_ENABLED


LIVE_TRADING_DISABLED_MESSAGE = "Live trading is disabled. Use paper trading mode."

class OrderService(Response):
    def __init__(self, exchange_repository: ExchangeRepositoryInterface):
        self.exchange_repository = exchange_repository

    def create(self, body: dict):
        if not LIVE_TRADING_ENABLED:
            return self.response_forbidden(LIVE_TRADING_DISABLED_MESSAGE)

        exchange = self.exchange_repository.get_by_id(body['exchange_id'])
        if not exchange:
            return self.response_not_found('Exchange not found')
        ccxt_exchange = ExchangeGetter.get_exchange(exchange=exchange.title, api_key=exchange.api_key, api_secret=exchange.api_secret, password=exchange.password)
        if not ccxt_exchange:
            return self.response_not_found('Exchange not found')
        try:
            if body['type'] == 'buy':
                response = ccxt_exchange.create_limit_buy_order(symbol=body['pair'], amount=body['amount'], price=body['price'])
                return self.response_ok({})
            if body['type'] == 'sell':
                response = ccxt_exchange.create_limit_sell_order(symbol=body['pair'], amount=body['amount'], price=body['price'])
                return self.response_ok({})
            return self.response_not_found('Method not supported')
        except Exception as e:
            return self.response_err_msg(str(ValueError(e)))

    def cancel(self, exchange_id: int, order_id: str, pair: str) -> dict:
        if not LIVE_TRADING_ENABLED:
            return self.response_forbidden(LIVE_TRADING_DISABLED_MESSAGE)

        exchange = self.exchange_repository.get_by_id(exchange_id)
        if not exchange:
            return self.response_not_found('Exchange not found')
        ccxt_exchange = ExchangeGetter.get_exchange(exchange=exchange.title, api_key=exchange.api_key, api_secret=exchange.api_secret, password=exchange.password)
        if not ccxt_exchange:
            return self.response_not_found('Exchange not found')
        try:
            response = ccxt_exchange.cancel_active_order(order_id=order_id, symbol=pair)
            return self.response_ok({})
        except Exception as e:
            return self.response_err_msg(str(ValueError(e)))
