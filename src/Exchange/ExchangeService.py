from src.Exchange.ExchangeRepositoryInterface import ExchangeRepositoryInterface
from src.__Parents.Response import Response


def mask_secret(value: str | None) -> str:
    if not value:
        return ""
    if len(value) <= 4:
        return "****"
    return f"{value[:2]}****{value[-2:]}"


class ExchangeService(Response):
    def __init__(self, exchange_repository: ExchangeRepositoryInterface):
        self.exchange_repository = exchange_repository

    def create(self, body: dict) -> dict:
        if self.exchange_repository.get_by_title(title=body['title']):
            return self.response_conflict('Exchange already exists')
        self.exchange_repository.create(body)
        return self.response_created('Exchange created')

    def update(self, exchange_id: int, body: dict) -> dict:
        exchange = self.exchange_repository.get_by_id(exchange_id)
        if not exchange:
            return self.response_not_found('Exchange does not exist')
        self.exchange_repository.update(exchange, body)
        return self.response_updated('Exchange updated')

    def delete(self, exchange_id: int) -> dict:
        exchange = self.exchange_repository.get_by_id(exchange_id)
        if not exchange:
            return self.response_not_found('Exchange does not exist')
        self.exchange_repository.delete(exchange)
        return self.response_deleted('Exchange deleted')

    def get_all(self) -> dict:
        exchanges = self.exchange_repository.get_all()
        return self.response_ok([{
            "id": exchange.id,
            "title": exchange.title,
            "icon_path": exchange.icon_path,
            "index": exchange.index,
            "enabled": exchange.enabled,
            "api_key": mask_secret(exchange.api_key),
            "has_api_key": bool(exchange.api_key),
            "has_secret": bool(exchange.api_secret),
            "has_password": bool(exchange.password),
        } for exchange in exchanges])

    def get_by_id(self, exchange_id: int) -> dict:
        exchange = self.exchange_repository.get_by_id(exchange_id)
        if not exchange:
            return self.response_not_found('Exchange does not exist')
        return self.response_ok({
            "id": exchange.id,
            "title": exchange.title,
            "icon_path": exchange.icon_path,
            "index": exchange.index,
            "enabled": exchange.enabled,
            "api_key": mask_secret(exchange.api_key),
            "has_api_key": bool(exchange.api_key),
            "has_secret": bool(exchange.api_secret),
            "has_password": bool(exchange.password),
            "trading_pairs": [{
                "id": trading_pair.id,
                "pair": trading_pair.pair,
                "index": trading_pair.index,
                "icon_path": trading_pair.icon_path,
                "max_purchase_price": trading_pair.max_purchase_price,
                "order_limit": trading_pair.order_limit,
                "enabled": trading_pair.enabled,
            } for trading_pair in exchange.trading_pairs]
        })
