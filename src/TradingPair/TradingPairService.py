from src.TradingPair.TradingPairRepositoryInterface import TradingPairRepositoryInterface
from src.__Parents.Response import Response
from src.__Parents.ExchangeGetter import ExchangeGetter
from src.Exchange.ExchangeService import mask_secret


class TradingPairService(Response):
    def __init__(self, trading_pair_repository: TradingPairRepositoryInterface):
        self.trading_pair_repository = trading_pair_repository

    def create(self, body: dict) -> dict:
        if self.trading_pair_repository.get_by_pair_exchange_id(pair=body['pair'], exchange_id=body['exchange_id']):
            return self.response_conflict('Pair already exists')
        self.trading_pair_repository.create(body)
        return self.response_created('Pair created')

    def update(self, trading_pair_id: int, body: dict) -> dict:
        trading_pair = self.trading_pair_repository.get_by_id(trading_pair_id)
        if not trading_pair:
            return self.response_not_found('Pair does not exist')
        self.trading_pair_repository.update(trading_pair, body)
        return self.response_updated('Pair updated')

    def delete(self, trading_pair_id: int) -> dict:
        trading_pair = self.trading_pair_repository.get_by_id(trading_pair_id)
        if not trading_pair:
            return self.response_not_found('Exchange does not exist')
        self.trading_pair_repository.delete(trading_pair)
        return self.response_deleted('Pair deleted')

    def get_all(self, exchange_id: int or None) -> dict:
        if exchange_id:
            trading_pairs = self.trading_pair_repository.get_all_by_exchange_id(exchange_id=exchange_id)
        else:
            trading_pairs = self.trading_pair_repository.get_all()



        return self.response_ok([{
                "id": trading_pair.id,
                "pair": trading_pair.pair,
                "icon_path": trading_pair.icon_path,
                "index": trading_pair.index,
                "order_limit": trading_pair.order_limit,
                "max_purchase_price": trading_pair.max_purchase_price,
                "enabled": trading_pair.enabled,
                "exchange_id": trading_pair.exchange.id,

                "exchange": {
                    "id": trading_pair.exchange.id,
                    "title": trading_pair.exchange.title,
                    "icon_path": trading_pair.exchange.icon_path,
                    "enabled": trading_pair.exchange.enabled,
                    "api_key": mask_secret(trading_pair.exchange.api_key),
                    "has_api_key": bool(trading_pair.exchange.api_key),
                    "has_secret": bool(trading_pair.exchange.api_secret),
                }
            } for trading_pair in trading_pairs])

    def get_by_id(self, trading_pair_id: int) -> dict:
        trading_pair = self.trading_pair_repository.get_by_id(trading_pair_id)
        if not trading_pair:
            return self.response_not_found('Pair does not exist')
        return self.response_ok({
                "id": trading_pair.id,
                "pair": trading_pair.pair,
                "icon_path": trading_pair.icon_path,
                "index": trading_pair.index,
                "order_limit": trading_pair.order_limit,
                "max_purchase_price": trading_pair.max_purchase_price,
                "enabled": trading_pair.enabled,
                "exchange_id": trading_pair.exchange.id,
                "exchange": {
                    "id": trading_pair.exchange.id,
                    "title": trading_pair.exchange.title,
                    "icon_path": trading_pair.exchange.icon_path,
                    "enabled": trading_pair.exchange.enabled,
                    "api_key": mask_secret(trading_pair.exchange.api_key),
                    "has_api_key": bool(trading_pair.exchange.api_key),
                    "has_secret": bool(trading_pair.exchange.api_secret),
                }
            })
