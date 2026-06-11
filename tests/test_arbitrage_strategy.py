from src import db
from src.Arbitrage.ArbitrageConfigModel import ArbitrageConfig
from src.Arbitrage.ArbitrageOpportunityModel import ArbitrageOpportunity
from src.Arbitrage.OrderBookSnapshotStore import OrderBookSnapshotStore
from src.PaperTrading.PaperOrderModel import PaperOrder
from src.Signal.TradeSignalModel import TradeSignal


def seed_config(**overrides):
    values = {
        "enabled": True,
        "symbols_allowlist": [],
        "exchanges_allowlist": [],
        "min_spread_percent": 0,
        "min_net_profit_percent": 0,
        "min_profit_usdt": 0,
        "max_order_margin_usdt": 100,
        "max_leverage": 1,
        "taker_fee_buffer_percent": 0,
        "slippage_buffer_percent": 0,
        "cooldown_seconds_per_symbol": 60,
        "paper_execute_enabled": False,
    }
    values.update(overrides)
    config = ArbitrageConfig(**values)
    db.session.add(config)
    db.session.commit()
    return config


def seed_snapshots(ask=100, bid=102):
    OrderBookSnapshotStore.update("binance", "BTC/USDT", {
        "sales": [{"price": ask, "amount": 1, "commission": 0}],
        "purchases": [{"price": ask - 1, "amount": 1, "commission": 0}],
    })
    OrderBookSnapshotStore.update("bybit", "BTC/USDT", {
        "sales": [{"price": bid + 1, "amount": 1, "commission": 0}],
        "purchases": [{"price": bid, "amount": 1, "commission": 0}],
    })


def test_detects_arbitrage_when_sell_bid_above_buy_ask_plus_buffers(client):
    seed_config(min_net_profit_percent=1, min_profit_usdt=1)
    seed_snapshots(ask=100, bid=103)

    response = client.post("/api/arbitrage/run-once")
    payload = response.get_json()["obj"]

    assert response.status_code == 200
    assert len(payload["opportunities"]) == 1
    assert payload["opportunities"][0]["buy_exchange"] == "binance"
    assert payload["opportunities"][0]["sell_exchange"] == "bybit"
    assert TradeSignal.query.filter_by(strategy_type="arbitrage").count() == 1


def test_rejects_opportunity_below_min_net_profit_percent(client):
    seed_config(min_net_profit_percent=5)
    seed_snapshots(ask=100, bid=102)

    response = client.post("/api/arbitrage/run-once")
    payload = response.get_json()["obj"]

    assert response.status_code == 200
    assert payload["opportunities"] == []
    assert ArbitrageOpportunity.query.count() == 0


def test_cooldown_prevents_duplicate_signals(client):
    seed_config(cooldown_seconds_per_symbol=300)
    seed_snapshots(ask=100, bid=103)

    first = client.post("/api/arbitrage/run-once").get_json()["obj"]
    second = client.post("/api/arbitrage/run-once").get_json()["obj"]

    assert len(first["opportunities"]) == 1
    assert second["opportunities"] == []
    assert TradeSignal.query.filter_by(strategy_type="arbitrage").count() == 1


def test_run_once_returns_opportunities(client):
    seed_config()
    seed_snapshots(ask=100, bid=103)

    response = client.post("/api/arbitrage/run-once")

    assert response.status_code == 200
    assert response.get_json()["obj"]["opportunities"]


def test_paper_execute_disabled_does_not_create_paper_order(client):
    seed_config(paper_execute_enabled=False)
    seed_snapshots(ask=100, bid=103)

    client.post("/api/arbitrage/run-once")

    assert TradeSignal.query.filter_by(strategy_type="arbitrage").count() == 1
    assert PaperOrder.query.count() == 0


def test_paper_execute_enabled_creates_paper_order_after_risk_approval(client):
    seed_config(paper_execute_enabled=True, max_order_margin_usdt=1000)
    seed_snapshots(ask=100, bid=103)

    response = client.post("/api/arbitrage/run-once")
    payload = response.get_json()["obj"]

    assert response.status_code == 200
    assert len(payload["executed"]) == 1
    assert PaperOrder.query.count() == 1
    assert PaperOrder.query.first().status == "filled"
