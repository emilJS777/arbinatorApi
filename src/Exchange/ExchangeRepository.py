from src.Exchange.ExchangeModel import Exchange
from src.Exchange.ExchangeRepositoryInterface import ExchangeRepositoryInterface


def should_update_secret(value):
    return value is not None and "****" not in str(value)


class ExchangeRepository(ExchangeRepositoryInterface):

    def create(self, body: dict):
        exchange = Exchange()
        exchange.title = body['title']
        exchange.icon_path = body.get('icon_path', "")
        exchange.enabled = body.get('enabled', False)
        exchange.index = body.get('index', 1)
        exchange.api_key = body.get('api_key', "")
        exchange.api_secret = body.get('api_secret', "")
        exchange.password = body.get('password', "")
        exchange.save_db()

    def update(self, exchange: Exchange, body: dict):
        exchange.title = body['title']
        exchange.icon_path = body.get('icon_path', exchange.icon_path)
        exchange.enabled = body.get('enabled', exchange.enabled)
        exchange.index = body.get('index', exchange.index)
        if should_update_secret(body.get('api_key')):
            exchange.api_key = body.get('api_key')
        if should_update_secret(body.get('api_secret')):
            exchange.api_secret = body.get('api_secret')
        if should_update_secret(body.get('password')):
            exchange.password = body.get('password')
        exchange.update_db()

    def delete(self, exchange: Exchange):
        exchange.delete_db()

    def get_by_id(self, exchange_id: int) -> Exchange:
        exchange = Exchange.query.filter_by(id=exchange_id).first()
        return exchange

    def get_all(self, enabled: bool or None = None) -> list[Exchange]:
        if enabled is None:
            exchanges = Exchange.query.order_by(Exchange.index.asc()).all()
        else:
            exchanges = Exchange.query.filter_by(enabled=enabled).order_by(Exchange.index.asc()).all()
        return exchanges

    def get_by_title(self, title: str) -> Exchange:
        exchange = Exchange.query.filter_by(title=title).first()
        return exchange
