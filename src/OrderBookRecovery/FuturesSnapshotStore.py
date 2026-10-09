from src.Arbitrage.OrderBookSnapshotStore import OrderBookSnapshotStore
from datetime import datetime


class FuturesSnapshotStore(OrderBookSnapshotStore):
    """Keep spot scanner/arbitrage books separate from USDT perpetual signals."""
    _snapshots = {}

    @classmethod
    def update(cls, exchange, symbol, order_book, metadata=None, received_at=None):
        with cls._lock:
            cls._snapshots.setdefault(exchange, {})[symbol] = {
                "exchange": exchange, "symbol": symbol, "order_book": order_book,
                "metadata": metadata or {}, "updated_at": received_at or datetime.utcnow(),
            }
