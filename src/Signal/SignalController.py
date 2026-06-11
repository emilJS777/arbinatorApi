from src.PaperTrading.PaperTradingService import PaperTradingService
from src.__Parents.Controller import Controller


class SignalController(Controller):
    paper_trading_service = PaperTradingService()

    def get(self):
        return self.paper_trading_service.get_signals()


class PaperSignalController(Controller):
    paper_trading_service = PaperTradingService()

    def post(self):
        return self.paper_trading_service.execute_paper_signal(self.request.get_json() or {})
