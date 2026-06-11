from abc import abstractmethod, ABC
from .ExchangeModel import Exchange

class ExchangeRepositoryInterface(ABC):
    @abstractmethod
    def create(self, body: dict):
        pass

    @abstractmethod
    def update(self, exchange: Exchange, body: dict):
        pass

    @abstractmethod
    def delete(self, exchange: Exchange):
        pass

    @abstractmethod
    def get_all(self, enabled: bool or None = None) -> list[Exchange]:
        pass

    @abstractmethod
    def get_by_title(self, title: str) -> Exchange:
        pass

    @abstractmethod
    def get_by_id(self, exchange_id: int) -> Exchange:
        pass