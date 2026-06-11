from src.Arbitrage.ArbitrageStrategyService import ArbitrageStrategyService
from src.__Parents.Controller import Controller


class ArbitrageConfigController(Controller):
    service = ArbitrageStrategyService()

    def get(self):
        return self.service.get_config()

    def patch(self):
        return self.service.patch_config(self.request.get_json() or {})


class ArbitrageOpportunityController(Controller):
    service = ArbitrageStrategyService()

    def get(self):
        return self.service.get_opportunities()


class ArbitrageSignalController(Controller):
    service = ArbitrageStrategyService()

    def get(self):
        return self.service.get_signals()


class ArbitrageRunOnceController(Controller):
    service = ArbitrageStrategyService()

    def post(self):
        return self.service.run_once_response()
