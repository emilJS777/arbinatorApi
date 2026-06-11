from abc import abstractmethod, ABC
from .TradingPairModel import TradingPair


class TradingPairRepositoryInterface(ABC):
    @abstractmethod
    def create(self, body: dict):
        pass

    @abstractmethod
    def update(self, trading_pair: TradingPair, body: dict):
        pass

    @abstractmethod
    def delete(self, trading_pair: TradingPair):
        pass

    @abstractmethod
    def get_all(self, exchange_id: int or None = None, enabled: bool or None = None) -> list[TradingPair]:
        pass

    @abstractmethod
    def get_all_by_exchange_id(self, exchange_id) -> list[TradingPair]:
        pass

    @abstractmethod
    def get_by_pair_exchange_id(self, exchange_id: int, pair: str) -> TradingPair:
        pass

    @abstractmethod
    def get_by_id(self, trading_pair_id: int) -> TradingPair:
        pass
