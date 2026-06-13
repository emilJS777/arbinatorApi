from src.Live.MexcDiagnosticsService import MexcDiagnosticsService
from src.Live.MexcOrderSubmitDryCheckService import MexcOrderSubmitDryCheckService
from src.Live.MexcTpSlDryCheckService import MexcTpSlDryCheckService
from src.__Parents.Controller import Controller


class MexcDiagnosticsController(Controller):
    def get(self):
        return MexcDiagnosticsService().run()


class MexcOrderSubmitDryCheckController(Controller):
    def post(self):
        return MexcOrderSubmitDryCheckService().run(self.request.get_json() or {})


class MexcTpSlDryCheckController(Controller):
    def post(self):
        return MexcTpSlDryCheckService().run(self.request.get_json() or {})
