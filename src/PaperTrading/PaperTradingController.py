from src.PaperTrading.PaperTradingService import PaperTradingService
from src.__Parents.Controller import Controller


class PaperOrderController(Controller):
    paper_trading_service = PaperTradingService()

    def get(self):
        return self.paper_trading_service.get_orders()


class PaperPositionController(Controller):
    paper_trading_service = PaperTradingService()

    def get(self):
        return self.paper_trading_service.get_positions()


class PaperPositionCloseController(Controller):
    paper_trading_service = PaperTradingService()

    def post(self, position_id: int):
        return self.paper_trading_service.close_position(position_id, self.request.get_json() or {})
