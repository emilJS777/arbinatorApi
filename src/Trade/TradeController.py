from .TradeService import TradeService
from src.__Parents.Controller import Controller
from ..TradingPair.TradingPairRepository import TradingPairRepository


class TradeController(Controller):
    trade_service = TradeService(TradingPairRepository())

    def get(self):
        res: dict = self.trade_service.get_trades(trading_pair_id=int(self.arguments.get('trading_pair_id')))
        return res