from .TradingPairService import TradingPairService
from src.__Parents.Controller import Controller
from .TradingPairRepository import TradingPairRepository

class TradingPairController(Controller):
    trading_pair_service = TradingPairService(TradingPairRepository())

    def post(self):
        res: dict = self.trading_pair_service.create(body=self.request.get_json())
        return res

    def put(self):
        res: dict = self.trading_pair_service.update(trading_pair_id=int(self.arguments.get('id')), body=self.request.get_json())
        return res

    def delete(self):
        res: dict = self.trading_pair_service.delete(trading_pair_id=int(self.arguments.get('id')))
        return res

    def get(self):
        if(self.arguments.get('id')):
            res: dict = self.trading_pair_service.get_by_id(trading_pair_id=int(self.arguments.get('id')))
        else:
            res: dict = self.trading_pair_service.get_all(exchange_id=int(self.arguments.get('exchange_id')) if self.arguments.get('exchange_id') else None)
        return res