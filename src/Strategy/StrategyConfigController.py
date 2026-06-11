from src.Strategy.StrategyConfigService import StrategyConfigService
from src.__Parents.Controller import Controller


class StrategyConfigController(Controller):
    strategy_config_service = StrategyConfigService()

    def get(self):
        return self.strategy_config_service.get_all()

    def post(self):
        return self.strategy_config_service.create(self.request.get_json() or {})


class StrategyConfigItemController(Controller):
    strategy_config_service = StrategyConfigService()

    def patch(self, strategy_config_id: int):
        return self.strategy_config_service.patch(strategy_config_id, self.request.get_json() or {})
