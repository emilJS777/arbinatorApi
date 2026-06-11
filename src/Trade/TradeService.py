from src.TradingPair.TradingPairRepositoryInterface import TradingPairRepositoryInterface
from src.__Parents.Response import Response
from src.__Parents.ExchangeGetter import ExchangeGetter

class TradeService(Response):
    def __init__(self, trading_pair_repository: TradingPairRepositoryInterface):
        self.trading_pair_repository = trading_pair_repository

    def get_trades(self, trading_pair_id):
        trading_pair = self.trading_pair_repository.get_by_id(trading_pair_id)
        if not trading_pair:
            return self.response_not_found('Trading pair not found')
        ccxt_exchange = ExchangeGetter.get_exchange(exchange=trading_pair.exchange.title, api_key=trading_pair.exchange.api_key, api_secret=trading_pair.exchange.api_secret, password=trading_pair.exchange.password)
        if not ccxt_exchange:
            return self.response_not_found('Trading not found')
        try:
            trades = ccxt_exchange.get_trades(trading_pair.pair)
            return self.response_ok(trades)
        except Exception as e:
            return self.response_err_msg(str(ValueError(e)))