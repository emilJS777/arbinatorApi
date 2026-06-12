import asyncio
import time
from types import SimpleNamespace

from src.Arbitrage.OrderBookSnapshotStore import OrderBookSnapshotStore
from src.OrderBookRecovery.OrderBookRecoveryService import OrderBookRecoveryService
from src.Scanner.ScannerService import ScannerService


class FakeSocket:
    def __init__(self):
        self.messages = []

    async def send(self, topic, payload):
        self.messages.append((topic, payload))


class FakeCcxtClient:
    def __init__(self, delay=0, payload=None, error=None):
        self.delay = delay
        self.payload = payload or {
            "purchases": [{"price": 100, "amount": 2}],
            "sales": [{"price": 101, "amount": 1}],
        }
        self.error = error

    def get_order_book(self, pair, limit):
        if self.delay:
            time.sleep(self.delay)
        if self.error:
            raise self.error
        return self.payload


def exchange(exchange_id, title):
    return SimpleNamespace(id=exchange_id, title=title, icon_path="", api_key="")


def pair(pair_id, value="BTC/USDT"):
    return SimpleNamespace(
        id=pair_id,
        pair=value,
        order_limit=10,
        icon_path="",
        max_purchase_price="",
    )


def scanner():
    service = ScannerService(None, None, None, FakeSocket())
    service.arbitrage_strategy_service.run_once = lambda ignore_enabled=False: None
    service.order_book_recovery_service.on_order_book_snapshot = lambda *args, **kwargs: None
    service.order_book_fetch_timeout_seconds = 0.05
    service.slow_exchange_cooldown_seconds = 0.1
    return service


def test_slow_exchange_does_not_block_fast_exchange(client):
    service = scanner()
    fast_exchange = exchange(1, "Mexc")
    slow_exchange = exchange(2, "SlowEx")
    trading_pair = pair(1)
    clients = {
        1: FakeCcxtClient(delay=0),
        2: FakeCcxtClient(delay=0.2),
    }
    service._public_exchange_client = lambda ex: clients[ex.id]

    async def run_fetches():
        await asyncio.gather(
            service.fetch_pair_order_book(fast_exchange, trading_pair),
            service.fetch_pair_order_book(slow_exchange, trading_pair),
        )

    asyncio.run(run_fetches())

    assert OrderBookSnapshotStore.all()["Mexc"]["BTC/USDT"]
    diagnostics = ScannerService.diagnostics_payload()
    statuses = {(row["exchange"], row["symbol"]): row["status"] for row in diagnostics}
    assert statuses[("Mexc", "BTC/USDT")] == "active"
    assert statuses[("SlowEx", "BTC/USDT")] == "timeout"


def test_timeout_marks_exchange_failed(client):
    service = scanner()
    slow_exchange = exchange(2, "SlowEx")
    trading_pair = pair(1)
    service._public_exchange_client = lambda ex: FakeCcxtClient(delay=0.2)

    asyncio.run(service.fetch_pair_order_book(slow_exchange, trading_pair))

    row = ScannerService.diagnostics_payload()[0]
    assert row["status"] == "timeout"
    assert row["fail_count"] == 1
    assert row["last_error_at"]
    assert row["cooldown_until"]


def test_snapshot_updated_independently(client):
    service = scanner()
    fast_exchange = exchange(1, "Mexc")
    trading_pair = pair(1, "TON/USDT")
    service._public_exchange_client = lambda ex: FakeCcxtClient(payload={
        "purchases": [{"price": 2.1, "amount": 20}],
        "sales": [{"price": 2.11, "amount": 10}],
    })

    asyncio.run(service.fetch_pair_order_book(fast_exchange, trading_pair))

    snapshot = OrderBookSnapshotStore.all()["Mexc"]["TON/USDT"]
    assert snapshot["metadata"]["fetch_latency_ms"] >= 0
    assert snapshot["metadata"]["snapshot_timestamp"]


def test_stale_seconds_calculated_correctly(client):
    service = scanner()
    ex = exchange(1, "Mexc")
    trading_pair = pair(1)
    service._diagnostic_payload(
        ex,
        trading_pair,
        status="active",
        last_success_at=service._datetime_from_ts(time.time() - 3),
        last_success_ts=time.time() - 3,
    )

    row = ScannerService.diagnostics_payload()[0]
    assert row["stale_seconds"] >= 2.5


def test_diagnostics_endpoint_works(client):
    ScannerService._diagnostics = {
        "1:BTC/USDT": {
            "exchange": "Mexc",
            "exchange_id": 1,
            "symbol": "BTC/USDT",
            "status": "active",
            "latency_ms": 12.5,
            "success_count": 3,
            "fail_count": 0,
            "active": True,
            "last_success_at": "2026-06-12T00:00:00Z",
        }
    }

    response = client.get("/api/scanner/diagnostics")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["success"] is True
    assert payload["obj"][0]["exchange"] == "Mexc"
    assert payload["obj"][0]["latency_ms"] == 12.5


def test_orderbook_recovery_feature_includes_snapshot_latency(client):
    service = OrderBookRecoveryService()
    config = service.get_or_create_config()
    snapshot = {
        "exchange": "Mexc",
        "symbol": "BTC/USDT",
        "updated_at": __import__("datetime").datetime.utcnow(),
        "metadata": {"fetch_latency_ms": 9.2},
        "order_book": {
            "purchases": [{"price": 100, "amount": 3}],
            "sales": [{"price": 101, "amount": 1}],
        },
    }

    row = service.exchange_feature(config, snapshot, __import__("datetime").datetime.utcnow())

    assert row["fetch_latency_ms"] == 9.2
    assert row["source_snapshot_time"] == snapshot["updated_at"]
    assert row["stale_seconds"] >= 0
