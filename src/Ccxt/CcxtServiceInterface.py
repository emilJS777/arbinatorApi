from abc import ABC, abstractmethod
from typing import Dict, List, Any

class CcxtServiceInterface(ABC):
    @abstractmethod
    def create_limit_sell_order(self, symbol: str, amount: float, price: float):
        pass

    @abstractmethod
    def create_limit_buy_order(self, symbol: str, amount: float, price: float):
        pass

    @abstractmethod
    def get_order_book(self, pair: str, limit: int) -> Dict[str, List[Any]]:
        pass

    @abstractmethod
    def get_balance(self, symbol: str) -> Dict[str, float]:
        pass

    @abstractmethod
    def get_account_active_orders(self, symbol: str) -> list:
        pass

    @staticmethod
    def cancel_active_order(self, order_id: str, symbol: str) -> bool:
        pass

    @abstractmethod
    def get_trades(self, symbol: str) -> list:
        pass
