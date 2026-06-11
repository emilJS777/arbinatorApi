from datetime import datetime
from threading import Lock


class OrderBookSnapshotStore:
    _lock = Lock()
    _snapshots = {}

    @classmethod
    def update(cls, exchange: str, symbol: str, order_book: dict, metadata: dict | None = None):
        with cls._lock:
            if exchange not in cls._snapshots:
                cls._snapshots[exchange] = {}
            cls._snapshots[exchange][symbol] = {
                "exchange": exchange,
                "symbol": symbol,
                "order_book": order_book,
                "metadata": metadata or {},
                "updated_at": datetime.utcnow(),
            }

    @classmethod
    def all(cls) -> dict:
        with cls._lock:
            return {
                exchange: dict(symbols)
                for exchange, symbols in cls._snapshots.items()
            }

    @classmethod
    def clear(cls):
        with cls._lock:
            cls._snapshots = {}
