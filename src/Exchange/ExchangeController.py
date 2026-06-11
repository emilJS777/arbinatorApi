from .ExchangeService import ExchangeService
from src.__Parents.Controller import Controller
from .ExchangeRepository import ExchangeRepository

class ExchangeController(Controller):
    exchange_service = ExchangeService(ExchangeRepository())

    def post(self):
        res: dict = self.exchange_service.create(body=self.request.get_json())
        return res

    def put(self):
        res: dict = self.exchange_service.update(exchange_id=int(self.arguments.get('id')), body=self.request.get_json())
        return res

    def delete(self):
        res: dict = self.exchange_service.delete(exchange_id=int(self.arguments.get('id')))
        return res

    def get(self):
        if(self.arguments.get('id')):
            res: dict = self.exchange_service.get_by_id(exchange_id=int(self.arguments.get('id')))
        else:
            res: dict = self.exchange_service.get_all()
        return res