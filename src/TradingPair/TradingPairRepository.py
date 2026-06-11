from .TradingPairModel import TradingPair
from .TradingPairRepositoryInterface import TradingPairRepositoryInterface
from .. import db


class TradingPairRepository(TradingPairRepositoryInterface):
    def create(self, body: dict):
        trading_pair = TradingPair()
        trading_pair.pair = body['pair']
        trading_pair.order_limit = body['order_limit']
        trading_pair.index = body['index']
        trading_pair.exchange_id = body['exchange_id']
        trading_pair.icon_path = body['icon_path']
        trading_pair.max_purchase_price = body['max_purchase_price']
        trading_pair.enabled = body['enabled']
        trading_pair.save_db()

    def update(self, trading_pair: TradingPair, body: dict):
        trading_pair.pair = body['pair']
        trading_pair.order_limit = body['order_limit']
        trading_pair.index = body['index']
        trading_pair.exchange_id = body['exchange_id']
        trading_pair.icon_path = body['icon_path']
        trading_pair.max_purchase_price = body['max_purchase_price']
        trading_pair.enabled = body['enabled']
        trading_pair.update_db()

    def delete(self, trading_pair: TradingPair):
        trading_pair.delete_db()

    def get_by_id(self, trading_pair_id: int) -> TradingPair:
        trading_pair = TradingPair.query.filter_by(id=trading_pair_id).first()
        return trading_pair

    def get_all(self, exchange_id: int or None = None, enabled: bool or None = None) -> list[TradingPair]:
        if exchange_id:
            trading_pairs = (TradingPair.query.filter_by(exchange_id=exchange_id)
                             .filter(TradingPair.enabled == enabled if not enabled == None else TradingPair.id.isnot(None))
                             .order_by(TradingPair.index.asc()).all())
        else:
            trading_pairs = TradingPair.query.order_by(TradingPair.index.asc()).all()
        return trading_pairs

    def get_all_by_exchange_id(self, exchange_id) -> list[TradingPair]:
        trading_pairs = TradingPair.query.filter_by(exchange_id=exchange_id).all()
        return trading_pairs

    def get_by_pair_exchange_id(self, exchange_id: int, pair: str) -> TradingPair:
        trading_pair = TradingPair.query.filter_by(pair=pair, exchange_id=exchange_id).first()
        return trading_pair

