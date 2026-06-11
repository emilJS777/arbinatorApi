from .OrderService import OrderService
from src.__Parents.Controller import Controller
from src.Exchange.ExchangeRepository import ExchangeRepository

class OrderController(Controller):
    order_service = OrderService(ExchangeRepository())

    def post(self):
        res: dict = self.order_service.create(body=self.request.get_json())
        return res

    def delete(self):
        res: dict = self.order_service.cancel(exchange_id=int(self.arguments.get('exchange_id')), pair=self.arguments.get('pair'), order_id=self.arguments.get('order_id'))
        return res
