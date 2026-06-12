from src import db
from src.config import build_sqlalchemy_engine_options
from src.Exchange.ExchangeModel import Exchange
from src.OrderBookRecovery.LiveExecutionService import LiveExecutionService
from src.OrderBookRecovery.OrderBookRecoveryService import OrderBookRecoveryService


def test_healthz_has_no_db_access(client, monkeypatch):
    def fail_execute(*_args, **_kwargs):
        raise AssertionError("healthz must not touch the database")

    monkeypatch.setattr(db.session, "execute", fail_execute)

    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.get_json() == {"status": "ok"}


def test_readyz_performs_lightweight_select(client, monkeypatch):
    calls = []
    original_execute = db.session.execute

    def spy_execute(statement, *args, **kwargs):
        calls.append(str(statement))
        return original_execute(statement, *args, **kwargs)

    monkeypatch.setattr(db.session, "execute", spy_execute)

    response = client.get("/readyz")

    assert response.status_code == 200
    assert response.get_json() == {"status": "ok"}
    assert any("SELECT 1" in statement for statement in calls)


def test_request_teardown_removes_scoped_session(client, monkeypatch):
    calls = []
    original_remove = db.session.remove

    def spy_remove(*args, **kwargs):
        calls.append(True)
        return original_remove(*args, **kwargs)

    monkeypatch.setattr(db.session, "remove", spy_remove)

    response = client.get("/healthz")

    assert response.status_code == 200
    assert calls


def test_orderbook_read_endpoints_do_not_call_exchange_apis(client, monkeypatch):
    service = OrderBookRecoveryService()
    config = service.get_or_create_config()
    exchange = Exchange(title="Mexc", enabled=True, api_key="key", api_secret="secret", password="", index=1)
    db.session.add(exchange)
    db.session.flush()
    config.exchange = "Mexc"
    config.exchange_id = exchange.id
    config.symbol = "BTC/USDT"
    config.execution_mode = "live"
    config.live_enabled_confirmation = True
    config.live_kill_switch = False
    db.session.commit()

    def fail_client(*_args, **_kwargs):
        raise AssertionError("read endpoints must not call exchange APIs")

    monkeypatch.setattr(LiveExecutionService, "client", fail_client)

    for path in (
        "/api/orderbook-recovery/state",
        "/api/orderbook-recovery/debug",
        "/api/orderbook-recovery/metrics",
        "/api/orderbook-recovery/trades",
    ):
        response = client.get(path)
        assert response.status_code == 200


def test_repeated_orderbook_polling_releases_sessions(client, monkeypatch):
    calls = []
    original_remove = db.session.remove

    def spy_remove(*args, **kwargs):
        calls.append(True)
        return original_remove(*args, **kwargs)

    monkeypatch.setattr(db.session, "remove", spy_remove)

    for _ in range(20):
        assert client.get("/api/orderbook-recovery/metrics").status_code == 200
        assert client.get("/api/orderbook-recovery/state").status_code == 200
        assert client.get("/api/orderbook-recovery/debug").status_code == 200

    assert len(calls) >= 60


def test_postgres_pool_options_are_configured():
    options = build_sqlalchemy_engine_options("postgresql+psycopg2://user:pass@host:5432/db")

    assert options["pool_size"] == 20
    assert options["max_overflow"] == 40
    assert options["pool_timeout"] == 5
    assert options["pool_pre_ping"] is True
    assert options["pool_recycle"] == 1800
