from src.Risk.RiskManager import RiskManager
from src.__Parents.Controller import Controller
from src.__Parents.Response import Response


class RiskStatusController(Controller, Response):
    risk_manager = RiskManager()

    def get(self):
        return self.response_ok(self.risk_manager.status())
