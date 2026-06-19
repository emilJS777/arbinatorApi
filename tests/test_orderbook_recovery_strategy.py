from datetime import datetime, timedelta
import csv
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys

from src import db
from src.Arbitrage.OrderBookSnapshotStore import OrderBookSnapshotStore
from src.Exchange.ExchangeModel import Exchange
from src.OrderBookRecovery.LiveExecutionService import LiveExecutionService
from src.OrderBookRecovery.OrderBookRecoveryModel import MLFeatureSnapshot, MLMarketPriceHistory, MLMarketSnapshot, MLMarketSnapshotExchangeLabel, StrategyRunTrade
from src.OrderBookRecovery.OrderBookNormalizer import OrderBookNormalizer
from src.OrderBookRecovery.OrderBookRecoveryService import OrderBookRecoveryService


def make_config(service):
    OrderBookRecoveryService._pending_entries.clear()
    OrderBookRecoveryService._last_confirmation_results.clear()
    config = service.get_or_create_config()
    config.exchange = "binance"
    config.symbol = "BTC/USDT"
    config.base_margin_usdt = 7
    config.leverage = 2
    config.max_recovery_steps = 2
    config.recovery_multiplier = 2
    config.take_profit_percent_of_margin = 10
    config.stop_loss_percent_of_margin = 5
    config.max_daily_loss_usdt = 100
    config.max_total_loss_usdt = 1000
    config.max_open_positions = 1
    config.cooldown_after_loss_seconds = 0
    config.cooldown_after_win_seconds = 0
    config.max_spread_percent = 0.05
    config.enabled = True
    db.session.commit()
    return config


def open_trade(service, config, side="long", entry=100):
    state = service.get_or_create_state(config)
    return service.open_position(
        config,
        state,
        {
            "mid_price": entry,
            "imbalance": 2,
            "short_momentum": 1,
            "spread_percent": 0.01,
        },
        side,
        datetime.utcnow(),
    )


def closed_trade(config, pnl=1, result=None, closed_at=None, side="long", signal_valid_exchanges_count=None, signal_momentum=None):
    trade = StrategyRunTrade(
        strategy_config_id=config.id,
        exchange=config.exchange,
        symbol=config.symbol,
        side=side,
        margin=7,
        leverage=2,
        notional=14,
        amount=0.14,
        entry_price=100,
        exit_price=101 if pnl >= 0 else 99,
        pnl=pnl,
        result=result or ("win" if pnl > 0 else "loss"),
        recovery_step=0,
        reason_close="take_profit" if pnl > 0 else "stop_loss",
        opened_at=(closed_at or datetime.utcnow()) - timedelta(minutes=5),
        closed_at=closed_at or datetime.utcnow(),
        signal_valid_exchanges_count=signal_valid_exchanges_count,
        signal_average_momentum=signal_momentum,
        signal_configured_exchange_momentum=signal_momentum,
    )
    db.session.add(trade)
    db.session.commit()
    return trade


def add_snapshot(exchange, symbol="TON/USDT", bid=100, ask=100.01, bid_amount=10, ask_amount=5):
    OrderBookSnapshotStore.update(exchange, symbol, {
        "bids": [[bid, bid_amount], [bid - 0.01, bid_amount], [bid - 0.02, bid_amount], [bid - 0.03, bid_amount], [bid - 0.04, bid_amount]],
        "asks": [[ask, ask_amount], [ask + 0.01, ask_amount], [ask + 0.02, ask_amount], [ask + 0.03, ask_amount], [ask + 0.04, ask_amount]],
    }, metadata={"source_exchange_title": exchange, "source_pair": symbol})


def seed_live_exchange(title="Mexc", api_key="key", api_secret="secret"):
    exchange = Exchange(title=title, enabled=True, api_key=api_key, api_secret=api_secret, password="", index=1)
    db.session.add(exchange)
    db.session.flush()
    return exchange


class MockLiveClient:
    def __init__(self, fail_close=False, fail_open=False, markets=None, exchange_id="mock", fail_contract_open=False, positions=None):
        self.id = exchange_id
        self.orders = []
        self.contract_orders = []
        self.fail_close = fail_close
        self.fail_open = fail_open
        self.fail_contract_open = fail_contract_open
        self.markets = markets or {"TON/USDT": {"symbol": "TON/USDT", "swap": True, "contract": True, "base": "TON", "quote": "USDT", "settle": "USDT", "linear": True}}
        self.positions = positions

    def load_markets(self):
        return self.markets

    def set_leverage(self, leverage, symbol):
        self.leverage = leverage

    def amount_to_precision(self, symbol, amount):
        return amount

    def price_to_precision(self, symbol, price):
        return price

    def fetch_positions(self):
        if self.positions is not None:
            return self.positions
        symbol = next(iter(self.markets.keys()))
        return [{"symbol": symbol, "contracts": 1}]

    def sign(self, path, api=None, method="GET", params=None):
        body = json.dumps(params or {}, separators=(",", ":")) if method == "POST" else None
        return {
            "url": f"https://contract.mexc.com/api/v1/private/{path}",
            "method": method,
            "body": body,
            "headers": {
                "ApiKey": "mock-key",
                "Request-Time": "123",
                "Signature": f"signed:{body}",
                "Content-Type": "application/json",
                "source": "CCXT",
            },
        }

    def contractPrivatePostOrderSubmit(self, request):
        if self.fail_contract_open:
            return {"code": 500, "message": "contract open failed"}
        order = {
            "code": 200,
            "data": f"contract-order-{len(self.contract_orders) + 1}",
            "request": request,
        }
        self.contract_orders.append(order)
        return order

    def create_order(self, symbol, order_type, side, amount, price=None, params=None):
        params = params or {}
        if params.get("reduceOnly") and self.fail_close:
            raise Exception("close failed")
        if not params.get("reduceOnly") and self.fail_open:
            raise Exception("open failed")
        order = {
            "id": f"order-{len(self.orders) + 1}",
            "symbol": symbol,
            "type": order_type,
            "side": side,
            "amount": amount,
            "filled": amount,
            "average": 100,
            "status": "closed",
            "fee": {"cost": 0.01},
            "params": params,
        }
        self.orders.append(order)
        return order


class MockMexcSubmitRequests:
    def __init__(self, status_code=200, text='{"code":200,"data":"contract-order-1"}'):
        self.status_code = status_code
        self.text = text
        self.calls = []

    class Response:
        def __init__(self, status_code, text):
            self.status_code = status_code
            self.text = text
            self.ok = 200 <= status_code < 300

        def json(self):
            return json.loads(self.text)

    def request(self, method, url, headers=None, data=None, timeout=None):
        self.calls.append({
            "method": method,
            "url": url,
            "headers": headers or {},
            "data": data,
            "timeout": timeout,
        })
        return self.Response(self.status_code, self.text)

    def get(self, url, params=None, timeout=None):
        self.calls.append({
            "method": "GET",
            "url": url,
            "params": params or {},
            "timeout": timeout,
        })
        return self.Response(200, '{"success":true,"code":0,"data":{"symbol":"BTC_USDT","contractSize":0.001,"minVol":1,"maxVol":1000000,"volScale":0,"volUnit":1,"priceScale":2,"priceUnit":0.01,"apiAllowed":true}}')


class FailingTpSlRequests(MockMexcSubmitRequests):
    def request(self, method, url, headers=None, data=None, timeout=None):
        self.calls.append({
            "method": method,
            "url": url,
            "headers": headers or {},
            "data": data,
            "timeout": timeout,
        })
        if "planorder/place" in url:
            return self.Response(200, '{"success":false,"code":500,"message":"plan failed"}')
        return self.Response(self.status_code, self.text)


class AlreadyClosedOnCloseRequests(MockMexcSubmitRequests):
    def request(self, method, url, headers=None, data=None, timeout=None):
        body = json.loads(data or "{}")
        self.calls.append({
            "method": method,
            "url": url,
            "headers": headers or {},
            "data": data,
            "timeout": timeout,
        })
        if "order/create" in url and body.get("side") in (2, 4):
            return self.Response(200, '{"success":false,"code":2009,"message":"Position is nonexistent or closed"}')
        return self.Response(self.status_code, self.text)


def prime_momentum(service, config, exchange, symbol="TON/USDT", old_bid=99, old_ask=99.01):
    snapshot = {
        "exchange": exchange,
        "symbol": symbol,
        "order_book": {
            "bids": [[old_bid, 10]],
            "asks": [[old_ask, 5]],
        },
        "updated_at": datetime.utcnow(),
    }
    service.features(config, snapshot)


def test_long_pnl_calculation(client):
    service = OrderBookRecoveryService()
    assert round(service.calculate_pnl("long", 100, 105, 14), 2) == 0.7


def test_short_pnl_calculation(client):
    service = OrderBookRecoveryService()
    assert round(service.calculate_pnl("short", 100, 95, 14), 2) == 0.7


def test_take_profit_closes_by_margin_percent(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    open_trade(service, config, "long", 100)
    trade = StrategyRunTrade.query.first()

    closed = service.evaluate(config, snapshot={
        "exchange": "binance",
        "symbol": "BTC/USDT",
        "order_book": {"bids": [[105, 5]], "asks": [[105, 5]]},
    })

    assert closed["reason_close"] == "take_profit"
    assert trade.result == "win"
    assert round(trade.pnl, 2) == 0.7


def test_stop_loss_closes_by_margin_percent(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    open_trade(service, config, "long", 100)
    trade = StrategyRunTrade.query.first()

    closed = service.evaluate(config, snapshot={
        "exchange": "binance",
        "symbol": "BTC/USDT",
        "order_book": {"bids": [[97.5, 5]], "asks": [[97.5, 5]]},
    })

    assert closed["reason_close"] == "stop_loss"
    assert trade.result == "loss"
    assert round(trade.pnl, 2) == -0.35


def test_manual_close_long_calculates_pnl_correctly(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    open_trade(service, config, "long", 100)
    trade = StrategyRunTrade.query.first()
    OrderBookSnapshotStore.update("binance", "BTC/USDT", {
        "bids": [[105, 5]],
        "asks": [[105, 5]],
    })

    response = client.post(f"/api/orderbook-recovery/positions/{trade.id}/close-manual", json={"reason": "manual_close"})

    data = response.get_json()
    assert data["success"] is True
    assert data["obj"]["reason_close"] == "manual_close"
    assert round(data["obj"]["pnl"], 2) == 0.7
    assert trade.closed_at is not None


def test_manual_close_short_calculates_pnl_correctly(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    open_trade(service, config, "short", 100)
    trade = StrategyRunTrade.query.first()
    OrderBookSnapshotStore.update("binance", "BTC/USDT", {
        "bids": [[95, 5]],
        "asks": [[95, 5]],
    })

    response = client.post(f"/api/orderbook-recovery/positions/{trade.id}/close-manual", json={"reason": "manual_close"})

    data = response.get_json()
    assert data["success"] is True
    assert data["obj"]["reason_close"] == "manual_close"
    assert round(data["obj"]["pnl"], 2) == 0.7
    assert trade.result == "win"


def test_manual_close_win_resets_recovery(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    state = service.get_or_create_state(config)
    service.apply_recovery_after_close(state, config, "loss")
    open_trade(service, config, "long", 100)
    trade = StrategyRunTrade.query.filter_by(closed_at=None).first()
    OrderBookSnapshotStore.update("binance", "BTC/USDT", {
        "bids": [[105, 5]],
        "asks": [[105, 5]],
    })

    client.post(f"/api/orderbook-recovery/positions/{trade.id}/close-manual", json={"reason": "manual_close"})

    assert state.current_step == 0
    assert state.current_margin == config.base_margin_usdt
    assert state.consecutive_losses == 0


def test_manual_close_loss_increases_recovery_step(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    state = service.get_or_create_state(config)
    open_trade(service, config, "long", 100)
    trade = StrategyRunTrade.query.first()
    OrderBookSnapshotStore.update("binance", "BTC/USDT", {
        "bids": [[97.5, 5]],
        "asks": [[97.5, 5]],
    })

    client.post(f"/api/orderbook-recovery/positions/{trade.id}/close-manual", json={"reason": "manual_close"})

    assert trade.result == "loss"
    assert round(trade.pnl, 2) == -0.35
    assert state.current_step == 1
    assert state.current_margin == 14


def test_manual_close_without_valid_market_price_returns_error(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    open_trade(service, config, "long", 100)
    trade = StrategyRunTrade.query.first()

    response = client.post(f"/api/orderbook-recovery/positions/{trade.id}/close-manual", json={"reason": "manual_close"})

    data = response.get_json()
    assert data["success"] is False
    assert data["obj"]["msg"] == "cannot_close_without_valid_market_price"
    assert trade.closed_at is None


def test_archive_closed_trade(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    trade = closed_trade(config, pnl=1.5)

    response = client.post(f"/api/orderbook-recovery/trades/{trade.id}/archive", json={"reason": "old_batch"})

    data = response.get_json()
    assert data["success"] is True
    assert data["obj"]["is_archived"] is True
    assert data["obj"]["archive_reason"] == "old_batch"
    assert trade.archived_at is not None


def test_cannot_archive_open_trade(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    open_trade(service, config, "long", 100)
    trade = StrategyRunTrade.query.first()

    response = client.post(f"/api/orderbook-recovery/trades/{trade.id}/archive", json={})

    data = response.get_json()
    assert data["success"] is False
    assert data["obj"]["msg"] == "cannot_archive_open_trade"
    assert trade.is_archived is False


def test_archive_all_closed_trades(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    closed_trade(config, pnl=1)
    closed_trade(config, pnl=-0.5)
    open_trade(service, config, "long", 100)

    response = client.post("/api/orderbook-recovery/trades/archive-all-closed", json={})

    data = response.get_json()
    assert data["success"] is True
    assert data["obj"]["archived_count"] == 2
    assert StrategyRunTrade.query.filter_by(is_archived=True).count() == 2
    assert StrategyRunTrade.query.filter(StrategyRunTrade.closed_at.is_(None), StrategyRunTrade.is_archived.is_(False)).count() == 1


def test_archived_trades_excluded_from_metrics(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    archived = closed_trade(config, pnl=10)
    archived.is_archived = True
    archived.archived_at = datetime.utcnow()
    active = closed_trade(config, pnl=-2)
    db.session.commit()

    metrics = client.get("/api/orderbook-recovery/metrics").get_json()["obj"]

    assert metrics["total_trades"] == 1
    assert metrics["net_pnl"] == active.pnl
    assert metrics["total_pnl"] == active.pnl
    assert metrics["archived_trades_count"] == 1


def test_archived_pnl_shown_separately(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    archived = closed_trade(config, pnl=3)
    archived.is_archived = True
    archived.archived_at = datetime.utcnow()
    closed_trade(config, pnl=2)
    db.session.commit()

    metrics = client.get("/api/orderbook-recovery/metrics").get_json()["obj"]

    assert metrics["net_pnl"] == 2
    assert metrics["archived_pnl"] == 3
    assert metrics["archived_trades_count"] == 1


def test_unarchive_all_restores_metrics(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    archived = closed_trade(config, pnl=3)
    archived.is_archived = True
    archived.archived_at = datetime.utcnow()
    db.session.commit()

    before = client.get("/api/orderbook-recovery/metrics").get_json()["obj"]
    response = client.post("/api/orderbook-recovery/trades/unarchive-all", json={})
    after = client.get("/api/orderbook-recovery/metrics").get_json()["obj"]

    assert before["total_trades"] == 0
    assert response.get_json()["obj"]["unarchived_count"] == 1
    assert after["total_trades"] == 1
    assert after["net_pnl"] == 3
    assert after["archived_trades_count"] == 0


def test_delete_archived_trade(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    trade = closed_trade(config, pnl=3)
    trade.is_archived = True
    trade.archived_at = datetime.utcnow()
    db.session.commit()

    response = client.post(f"/api/orderbook-recovery/trades/{trade.id}/delete-archived", json={})

    assert response.status_code == 200
    assert response.get_json()["obj"]["deleted_trade_id"] == trade.id
    assert db.session.get(StrategyRunTrade, trade.id) is None


def test_cannot_delete_non_archived_trade(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    trade = closed_trade(config, pnl=3)

    response = client.post(f"/api/orderbook-recovery/trades/{trade.id}/delete-archived", json={})
    data = response.get_json()

    assert response.status_code == 400
    assert data["obj"]["msg"] == "cannot_delete_non_archived_trade"
    assert db.session.get(StrategyRunTrade, trade.id) is not None


def test_delete_all_archived_trades(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    archived_one = closed_trade(config, pnl=3)
    archived_two = closed_trade(config, pnl=-1)
    active = closed_trade(config, pnl=2)
    for trade in [archived_one, archived_two]:
        trade.is_archived = True
        trade.archived_at = datetime.utcnow()
    db.session.commit()

    response = client.post("/api/orderbook-recovery/trades/delete-all-archived", json={})

    assert response.status_code == 200
    assert response.get_json()["obj"]["deleted_count"] == 2
    assert db.session.get(StrategyRunTrade, archived_one.id) is None
    assert db.session.get(StrategyRunTrade, archived_two.id) is None
    assert db.session.get(StrategyRunTrade, active.id) is not None


def test_recovery_doubles_after_loss(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    state = service.get_or_create_state(config)

    service.apply_recovery_after_close(state, config, "loss")
    db.session.commit()

    assert state.current_step == 1
    assert state.current_margin == 14


def test_loss_on_step_one_moves_to_step_two(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    state = service.get_or_create_state(config)

    service.apply_recovery_after_close(state, config, "loss")
    service.apply_recovery_after_close(state, config, "loss")
    db.session.commit()

    assert state.current_step == 2
    assert state.current_margin == 28
    assert state.is_stopped is False


def test_recovery_resets_after_win(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    state = service.get_or_create_state(config)
    service.apply_recovery_after_close(state, config, "loss")
    service.apply_recovery_after_close(state, config, "win")
    db.session.commit()

    assert state.current_step == 0
    assert state.current_margin == 7
    assert state.consecutive_losses == 0


def test_win_on_any_step_resets_to_step_zero(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    state = service.get_or_create_state(config)
    service.apply_recovery_after_close(state, config, "loss")
    service.apply_recovery_after_close(state, config, "loss")

    service.apply_recovery_after_close(state, config, "win")
    db.session.commit()

    assert state.current_step == 0
    assert state.current_margin == config.base_margin_usdt
    assert state.consecutive_losses == 0


def test_manual_recovery_reset_sets_step_zero_and_base_margin(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    state = service.get_or_create_state(config)
    state.current_step = 2
    state.current_margin = 28
    state.consecutive_losses = 2
    state.last_trade_result = "loss"
    db.session.commit()

    response = client.post("/api/orderbook-recovery/recovery/reset", json={})
    state = service.get_or_create_state(config)

    assert response.status_code == 200
    assert state.current_step == 0
    assert state.current_margin == config.base_margin_usdt
    assert state.consecutive_losses == 0
    assert state.last_trade_result is None
    assert state.last_manual_recovery_reset_at is not None


def test_manual_set_current_margin_validates_live_max_margin(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.execution_mode = "live"
    config.live_max_margin_usdt = 5
    db.session.commit()

    response = client.post("/api/orderbook-recovery/recovery/set-current-margin", json={"current_margin": 10})

    assert response.status_code == 400
    assert response.get_json()["obj"]["msg"] == "live_margin_exceeds_limit"


def test_manual_recovery_change_blocked_while_position_open(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    open_trade(service, config)

    reset_response = client.post("/api/orderbook-recovery/recovery/reset", json={})
    margin_response = client.post("/api/orderbook-recovery/recovery/set-current-margin", json={"current_margin": 10})

    assert reset_response.status_code == 400
    assert reset_response.get_json()["obj"]["msg"] == "cannot_change_margin_with_open_position"
    assert margin_response.status_code == 400
    assert margin_response.get_json()["obj"]["msg"] == "cannot_change_margin_with_open_position"


def test_manual_recovery_reset_clears_margin_related_stop_reason(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    state = service.get_or_create_state(config)
    state.is_stopped = True
    state.stop_reason = "current_margin_exceeds_available_paper_equity"
    state.paused_until = datetime.utcnow() + timedelta(minutes=5)
    state.current_step = 2
    state.current_margin = 28
    db.session.commit()

    response = client.post("/api/orderbook-recovery/recovery/reset", json={})
    state = service.get_or_create_state(config)

    assert response.status_code == 200
    assert state.is_stopped is False
    assert state.stop_reason is None
    assert state.paused_until is None


def test_manual_set_current_margin_updates_audit_and_resets_losses(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    state = service.get_or_create_state(config)
    state.current_step = 2
    state.current_margin = 28
    state.consecutive_losses = 2
    db.session.commit()

    response = client.post("/api/orderbook-recovery/recovery/set-current-margin", json={"current_margin": 10})
    state = service.get_or_create_state(config)

    assert response.status_code == 200
    assert state.current_step == 0
    assert state.current_margin == 10
    assert state.consecutive_losses == 0
    assert state.stop_reason == "manual_margin_override"
    assert state.last_manual_margin_override_at is not None
    assert state.last_manual_margin_override_value == 10


def test_strategy_stops_after_max_recovery_steps(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    state = service.get_or_create_state(config)

    service.apply_recovery_after_close(state, config, "loss")
    service.apply_recovery_after_close(state, config, "loss")
    service.apply_recovery_after_close(state, config, "loss")
    db.session.commit()

    assert state.is_stopped is True
    assert state.stop_reason == "max_recovery_pause"
    assert state.paused_until is not None
    assert state.current_step == 0
    assert state.current_margin == config.base_margin_usdt
    assert state.consecutive_losses == 0
    assert config.enabled is False


def test_strategy_does_not_open_if_spread_too_high(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    state = service.get_or_create_state(config)

    reason = service.risk_rejection(
        config,
        state,
        {"spread_percent": 0.2},
        datetime.utcnow(),
    )

    assert reason == "spread_too_high"


def test_strategy_does_not_open_if_daily_loss_exceeded(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.max_daily_loss_usdt = 1
    trade = StrategyRunTrade(
        strategy_config_id=config.id,
        exchange=config.exchange,
        symbol=config.symbol,
        side="long",
        margin=7,
        leverage=2,
        notional=14,
        amount=0.14,
        entry_price=100,
        exit_price=95,
        pnl=-2,
        result="loss",
        recovery_step=0,
        opened_at=datetime.utcnow() - timedelta(minutes=5),
        closed_at=datetime.utcnow(),
    )
    db.session.add(trade)
    db.session.commit()

    reason = service.risk_rejection(
        config,
        service.get_or_create_state(config),
        {"spread_percent": 0.01},
        datetime.utcnow(),
    )

    assert reason == "daily_loss_exceeded"


def test_strategy_stop_prevents_new_trades(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    service.stop()

    result = service.evaluate(config, snapshot={
        "exchange": "binance",
        "symbol": "BTC/USDT",
        "order_book": {"bids": [[100, 20]], "asks": [[100.01, 5]]},
    })

    assert result is None
    assert StrategyRunTrade.query.count() == 0


def test_invalid_orderbook_snapshot_rejected(client):
    service = OrderBookRecoveryService()
    config = make_config(service)

    result = service.evaluate(config, snapshot={
        "exchange": "binance",
        "symbol": "BTC/USDT",
        "order_book": {"bids": [], "asks": []},
    })

    assert result["reason"] == "empty_bids"
    assert StrategyRunTrade.query.count() == 0


def test_orderbook_normalizer_ccxt_bids_asks_list_format_works(client):
    normalized, error = OrderBookNormalizer.normalize({
        "bids": [[100, 2], [99, 3]],
        "asks": [[101, 4], [102, 5]],
    })

    assert error is None
    assert normalized["bids"][0]["price"].to_eng_string() == "100"
    assert normalized["asks"][0]["amount"].to_eng_string() == "4"


def test_orderbook_normalizer_dict_bids_asks_format_works(client):
    normalized, error = OrderBookNormalizer.normalize({
        "bids": [{"price": "100", "amount": "2"}],
        "asks": [{"price": "101", "amount": "4"}],
    })

    assert error is None
    assert normalized["bids"][0]["amount"].to_eng_string() == "2"


def test_orderbook_normalizer_purchases_sales_format_works(client):
    normalized, error = OrderBookNormalizer.normalize({
        "purchases": [{"price": "100", "amount": "2"}],
        "sales": [{"price": "101", "amount": "4"}],
    })

    assert error is None
    assert normalized["bids"][0]["price"].to_eng_string() == "100"
    assert normalized["asks"][0]["price"].to_eng_string() == "101"


def test_orderbook_normalizer_invalid_empty_snapshot_gives_empty_bids_or_asks(client):
    normalized, error = OrderBookNormalizer.normalize({"bids": [], "asks": [{"price": 1, "amount": 1}]})
    assert normalized is None
    assert error == "empty_bids"

    normalized, error = OrderBookNormalizer.normalize({"bids": [{"price": 1, "amount": 1}], "asks": []})
    assert normalized is None
    assert error == "empty_asks"


def test_ton_mexc_sample_like_format_produces_valid_top_5_volumes(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    db.session.commit()
    snapshot = {
        "exchange": "Mexc",
        "symbol": "TON/USDT",
        "order_book": {
            "purchases": [
                {"price": "3.000", "amount": "10"},
                {"price": "2.999", "amount": "9"},
                {"price": "2.998", "amount": "8"},
                {"price": "2.997", "amount": "7"},
                {"price": "2.996", "amount": "6"},
            ],
            "sales": [
                {"price": "3.001", "amount": "5"},
                {"price": "3.002", "amount": "5"},
                {"price": "3.003", "amount": "5"},
                {"price": "3.004", "amount": "5"},
                {"price": "3.005", "amount": "5"},
            ],
        },
    }

    features, error = service.features(config, snapshot)

    assert error is None
    assert features["bid_volume_top_5"] == 40
    assert features["ask_volume_top_5"] == 25
    assert features["imbalance"] == 1.6


def test_consensus_long_opens_when_majority_exchanges_confirm_long(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.consensus_enabled = True
    config.min_confirming_exchanges = 2
    config.min_consensus_ratio = 0.6
    config.require_configured_exchange_signal = True
    db.session.commit()
    for exchange in ["Mexc", "Binance", "Bybit"]:
        prime_momentum(service, config, exchange)
        add_snapshot(exchange, bid=100, ask=100.01, bid_amount=10, ask_amount=5)

    result = service.evaluate(config)
    trade = StrategyRunTrade.query.first()

    assert result["side"] == "long"
    assert trade.exchange == "Mexc"
    assert trade.symbol == "TON/USDT"


def test_consensus_short_opens_when_majority_exchanges_confirm_short(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.consensus_enabled = True
    config.min_confirming_exchanges = 2
    config.min_consensus_ratio = 0.6
    config.require_configured_exchange_signal = True
    db.session.commit()
    for exchange in ["Mexc", "Binance", "Bybit"]:
        prime_momentum(service, config, exchange, old_bid=101, old_ask=101.01)
        add_snapshot(exchange, bid=100, ask=100.01, bid_amount=3, ask_amount=10)

    result = service.evaluate(config)

    assert result["side"] == "short"


def test_median_imbalance_equal_short_threshold_opens_short(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.consensus_enabled = True
    config.short_imbalance_threshold = 0.77
    config.min_confirming_exchanges = 2
    config.min_consensus_ratio = 0.6
    config.require_configured_exchange_signal = True
    db.session.commit()
    for exchange in ["Mexc", "Binance", "Bybit"]:
        prime_momentum(service, config, exchange, old_bid=101, old_ask=101.01)
        add_snapshot(exchange, bid=100, ask=100.01, bid_amount=7.7, ask_amount=10)

    result = service.evaluate(config)
    last = service.signal_diagnostics_for(config)["last_100"][-1]

    assert result["side"] == "short"
    assert last["short_threshold_hit"] is True
    assert last["final_side"] == "short"


def test_after_long_loss_strategy_can_open_short_when_signal_flips(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.consensus_enabled = True
    config.min_confirming_exchanges = 2
    config.min_consensus_ratio = 0.6
    config.require_configured_exchange_signal = True
    config.feedback_enabled = False
    db.session.commit()
    setup_long_consensus(service, config)
    first = service.evaluate(config)
    long_trade = db.session.get(StrategyRunTrade, first["id"])
    state = service.get_or_create_state(config)
    service.close_trade(long_trade, 90, -1, "stop_loss", state, config, datetime.utcnow())
    OrderBookSnapshotStore.clear()
    service._mid_price_history.clear()
    setup_short_consensus(service, config)

    second = service.evaluate(config)

    assert long_trade.result == "loss"
    assert state.current_step == 1
    assert second["side"] == "short"


def test_feedback_long_loss_streak_does_not_block_short(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.consensus_enabled = True
    config.feedback_enabled = True
    config.side_loss_streak_limit = 3
    config.side_cooldown_seconds = 600
    config.min_confirming_exchanges = 2
    config.min_consensus_ratio = 0.6
    config.require_configured_exchange_signal = True
    db.session.commit()
    for _ in range(3):
        closed_trade(config, pnl=-1, side="long")
    setup_short_consensus(service, config)

    result = service.evaluate(config)
    last = service.signal_diagnostics_for(config)["last_100"][-1]

    assert result["side"] == "short"
    assert last["short_blocked_by_feedback"] is False


def test_require_configured_signal_allows_configured_short_without_preferring_long(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.consensus_enabled = True
    config.min_confirming_exchanges = 2
    config.min_consensus_ratio = 0.6
    config.require_configured_exchange_signal = True
    db.session.commit()
    setup_short_consensus(service, config)

    result = service.evaluate(config)
    last = service.signal_diagnostics_for(config)["last_100"][-1]

    assert result["side"] == "short"
    assert last["configured_exchange_short_signal"] is True
    assert last["configured_exchange_long_signal"] is False
    assert last["final_side"] == "short"


def test_signal_diagnostics_tracks_long_and_short_decisions(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.consensus_enabled = True
    config.min_confirming_exchanges = 2
    config.min_consensus_ratio = 0.6
    config.require_configured_exchange_signal = True
    db.session.commit()
    setup_short_consensus(service, config)

    result = service.evaluate(config)
    diagnostics = service.signal_diagnostics_for(config)
    last = diagnostics["last_100"][-1]

    assert result["side"] == "short"
    assert diagnostics["counters"]["short_signals_count"] == 1
    assert diagnostics["counters"]["short_opened_count"] == 1
    assert diagnostics["counters"]["long_signals_count"] == 0
    assert last["proposed_side"] == "short"
    assert last["final_side"] == "short"
    assert last["short_confirms"] >= 2
    assert last["short_ratio"] >= 0.6
    assert last["long_confirms"] == 0
    assert last["why_short_rejected"] is None
    assert last["short_consensus_passed"] is True


def test_debug_returns_signal_diagnostics_and_counters(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.consensus_enabled = True
    config.min_confirming_exchanges = 2
    config.min_consensus_ratio = 0.6
    config.require_configured_exchange_signal = True
    db.session.commit()
    setup_long_consensus(service, config)

    service.evaluate(config)
    debug = service.debug_payload(config, service.get_or_create_state(config))

    assert debug["long_signals_count"] == 1
    assert debug["long_opened_count"] == 1
    assert debug["short_signals_count"] == 0
    assert debug["short_opened_count"] == 0
    assert debug["raw_long_threshold_hits"] == 1
    assert debug["long_consensus_passed_count"] == 1
    assert debug["final_long_count"] == 1
    assert len(debug["signal_diagnostics_last_100"]) == 1
    assert debug["signal_diagnostics_last_100"][0]["proposed_side"] == "long"
    assert debug["signal_diagnostics_last_100"][0]["final_side"] == "long"
    assert "long_threshold_hit" in debug["signal_diagnostics_last_100"][0]["why_long_selected"]
    assert debug["signal_diagnostics_last_100"][0]["short_threshold_hit"] is False


def test_signal_diagnostics_history_trimmed_to_config_max_rows(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.signal_diagnostics_max_rows = 20
    db.session.commit()

    for index in range(25):
        service.record_signal_diagnostic(
            config,
            features={"short_momentum": index},
            proposed_side="long",
            final_side="none",
            evaluated_at=datetime.utcnow() + timedelta(seconds=index),
        )

    diagnostics = service.signal_diagnostics_for(config)

    assert len(diagnostics["last_100"]) == 20
    assert diagnostics["last_100"][0]["momentum"] == 5
    assert diagnostics["counters"]["long_signals_count"] == 25


def test_signal_diagnostics_clear_endpoint_clears_history_and_counters(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    service.record_signal_diagnostic(config, proposed_side="long", final_side="none")
    service.record_signal_diagnostic(config, proposed_side="short", final_side="none")
    service.record_opened_side(config, "long")
    service.record_opened_side(config, "short")

    response = client.post("/api/orderbook-recovery/diagnostics/clear")
    payload = response.get_json()["obj"]

    assert response.status_code == 200
    assert payload["last_100"] == []
    assert payload["counters"] == {
        "long_signals_count": 0,
        "short_signals_count": 0,
        "long_opened_count": 0,
        "short_opened_count": 0,
        "raw_long_threshold_hits": 0,
        "raw_short_threshold_hits": 0,
        "long_consensus_passed_count": 0,
        "short_consensus_passed_count": 0,
        "long_blocked_count": 0,
        "short_blocked_count": 0,
        "final_long_count": 0,
        "final_short_count": 0,
    }


def test_signal_diagnostics_config_max_rows_is_clamped(client):
    service = OrderBookRecoveryService()
    config = make_config(service)

    service.update_config({"signal_diagnostics_max_rows": 5})
    assert service.get_or_create_config().signal_diagnostics_max_rows == 20

    service.update_config({"signal_diagnostics_max_rows": 999})
    assert service.get_or_create_config().signal_diagnostics_max_rows == 500


def test_consensus_no_trade_when_configured_exchange_snapshot_missing(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.consensus_enabled = True
    db.session.commit()
    for exchange in ["Binance", "Bybit", "Gate"]:
        prime_momentum(service, config, exchange)
        add_snapshot(exchange, bid=100, ask=100.01, bid_amount=10, ask_amount=5)

    result = service.evaluate(config, snapshot={"exchange": "Mexc", "symbol": "TON/USDT", "order_book": {"bids": [[100, 10]], "asks": [[100.01, 5]]}, "updated_at": datetime.utcnow()})

    assert result is None
    assert StrategyRunTrade.query.count() == 0
    assert service.last_evaluation_for(config)["reject_reason"] == "configured_exchange_snapshot_missing"


def test_consensus_no_trade_when_ratio_below_threshold(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.consensus_enabled = True
    config.min_confirming_exchanges = 2
    config.min_consensus_ratio = 0.8
    db.session.commit()
    prime_momentum(service, config, "Mexc")
    add_snapshot("Mexc", bid=100, ask=100.01, bid_amount=10, ask_amount=5)
    prime_momentum(service, config, "Binance")
    add_snapshot("Binance", bid=100, ask=100.01, bid_amount=4, ask_amount=5)
    prime_momentum(service, config, "Bybit")
    add_snapshot("Bybit", bid=100, ask=100.01, bid_amount=10, ask_amount=5)

    result = service.evaluate(config)

    assert result is None
    assert StrategyRunTrade.query.count() == 0
    assert service.last_evaluation_for(config)["reject_reason"] == "no_consensus"


def test_consensus_stale_snapshots_ignored(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.consensus_enabled = True
    config.max_snapshot_age_seconds = 1
    db.session.commit()
    prime_momentum(service, config, "Mexc")
    add_snapshot("Mexc", bid=100, ask=100.01, bid_amount=10, ask_amount=5)
    OrderBookSnapshotStore._snapshots["Mexc"]["TON/USDT"]["updated_at"] = datetime.utcnow() - timedelta(seconds=10)

    result = service.evaluate(config)
    consensus = service.last_evaluation_for(config)["consensus"]

    assert result is None
    assert consensus["per_exchange_features"][0]["reject_reason"] == "stale_snapshot"


def test_consensus_high_spread_exchange_ignored(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.consensus_enabled = True
    db.session.commit()
    prime_momentum(service, config, "Mexc")
    add_snapshot("Mexc", bid=100, ask=101, bid_amount=10, ask_amount=5)

    result = service.evaluate(config)
    consensus = service.last_evaluation_for(config)["consensus"]

    assert result is None
    assert consensus["per_exchange_features"][0]["reject_reason"] == "spread_too_high"


def test_imbalance_above_anomaly_max_excluded_from_consensus(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    db.session.commit()
    prime_momentum(service, config, "Mexc")
    add_snapshot("Mexc", bid_amount=10, ask_amount=0.5)

    consensus = service.consensus_snapshot(config)
    row = consensus["per_exchange_features"][0]

    assert row["is_imbalance_anomaly"] is True
    assert row["reject_reason"] == "imbalance_anomaly"
    assert consensus["valid_exchanges_count"] == 0
    assert consensus["confirming_long_count"] == 0


def test_imbalance_below_anomaly_min_excluded_from_consensus(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    db.session.commit()
    prime_momentum(service, config, "Mexc")
    add_snapshot("Mexc", bid_amount=1, ask_amount=20)

    consensus = service.consensus_snapshot(config)
    row = consensus["per_exchange_features"][0]

    assert row["is_imbalance_anomaly"] is True
    assert row["reject_reason"] == "imbalance_anomaly"
    assert consensus["valid_exchanges_count"] == 0


def test_median_imbalance_calculated_from_valid_non_anomalous_exchanges(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.require_configured_exchange_signal = False
    db.session.commit()
    for exchange, bid_amount, ask_amount in [
        ("Mexc", 10, 5),
        ("Binance", 15, 5),
        ("Bybit", 5, 5),
        ("Gate", 10, 0.5),
    ]:
        prime_momentum(service, config, exchange)
        add_snapshot(exchange, bid_amount=bid_amount, ask_amount=ask_amount)

    consensus = service.consensus_snapshot(config)

    assert consensus["valid_exchanges_count"] == 3
    assert consensus["anomalous_exchanges_count"] == 1
    assert consensus["median_imbalance"] == 2
    assert "Gate:TON/USDT" in consensus["excluded_anomalous_imbalance_exchanges"]


def test_decision_uses_median_imbalance_not_raw_average(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.min_confirming_exchanges = 1
    config.min_consensus_ratio = 0.3
    config.require_configured_exchange_signal = False
    config.exclude_anomalous_imbalance = False
    db.session.commit()
    for exchange, bid_amount, ask_amount in [
        ("Mexc", 11, 10),
        ("Binance", 11, 10),
        ("Bybit", 100, 1),
    ]:
        prime_momentum(service, config, exchange)
        add_snapshot(exchange, bid_amount=bid_amount, ask_amount=ask_amount)

    result = service.evaluate(config)
    consensus = service.last_evaluation_for(config)["consensus"]

    assert result is None
    assert consensus["raw_average_imbalance"] > config.long_imbalance_threshold
    assert consensus["median_imbalance"] < config.long_imbalance_threshold
    assert consensus["reject_reason"] == "no_consensus"
    assert StrategyRunTrade.query.count() == 0


def test_all_anomalous_imbalances_do_not_open_trade(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    db.session.commit()
    for exchange in ["Mexc", "Binance", "Bybit"]:
        prime_momentum(service, config, exchange)
        add_snapshot(exchange, bid_amount=100, ask_amount=1)

    result = service.evaluate(config)
    consensus = service.last_evaluation_for(config)["consensus"]

    assert result is None
    assert consensus["valid_exchanges_count"] == 0
    assert consensus["median_imbalance"] is None
    assert StrategyRunTrade.query.count() == 0


def test_debug_returns_imbalance_anomaly_fields(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    db.session.commit()
    for exchange in ["Mexc", "Binance", "Bybit"]:
        prime_momentum(service, config, exchange)
        add_snapshot(exchange, bid_amount=10, ask_amount=5)
    add_snapshot("Gate", bid_amount=100, ask_amount=1)

    response = client.get("/api/orderbook-recovery/debug")
    payload = response.get_json()["obj"]

    assert "median_imbalance" in payload
    assert "raw_average_imbalance" in payload
    assert payload["anomalous_exchanges_count"] == 1
    assert payload["per_exchange_features"][0]["raw_imbalance"] is not None


def test_consensus_configured_exchange_remains_execution_venue(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.consensus_enabled = True
    db.session.commit()
    for exchange in ["Mexc", "Binance", "Bybit"]:
        prime_momentum(service, config, exchange)
        add_snapshot(exchange, bid=100, ask=100.01, bid_amount=10, ask_amount=5)

    service.evaluate(config)
    trade = StrategyRunTrade.query.first()

    assert trade.exchange == "Mexc"


def test_decision_snapshot_saved_on_open_trade(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.feedback_enabled = False
    db.session.commit()
    setup_long_consensus(service, config)

    service.evaluate(config)
    trade = StrategyRunTrade.query.first()
    snapshot = json.loads(trade.decision_snapshot_json)

    assert snapshot["selected_side"] == "long"
    assert snapshot["entry_price"] == trade.entry_price
    assert snapshot["current_recovery_step"] == 0
    assert snapshot["current_margin"] == config.base_margin_usdt
    assert snapshot["consensus_decision"]["direction"] == "long"
    assert trade.entry_reason
    assert trade.consensus_direction == "long"


def test_decision_per_exchange_features_saved(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.feedback_enabled = False
    db.session.commit()
    setup_long_consensus(service, config)

    service.evaluate(config)
    trade = StrategyRunTrade.query.first()
    rows = json.loads(trade.per_exchange_features_json)

    assert len(rows) == 3
    assert {"exchange", "symbol", "bid_volume_top_5", "ask_volume_top_5", "imbalance", "spread_percent", "momentum", "snapshot_age_seconds", "valid", "long_signal", "short_signal", "reject_reason"}.issubset(rows[0].keys())


def test_decision_details_endpoint_returns_details(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.feedback_enabled = False
    db.session.commit()
    setup_long_consensus(service, config)
    service.evaluate(config)
    trade = StrategyRunTrade.query.first()

    response = client.get(f"/api/orderbook-recovery/trades/{trade.id}/decision-details")
    payload = response.get_json()["obj"]

    assert response.status_code == 200
    assert payload["summary"]["id"] == trade.id
    assert payload["decision_snapshot"]["selected_side"] == "long"
    assert payload["consensus"]["direction"] == "long"
    assert len(payload["per_exchange_features"]) == 3


def test_archived_trade_still_shows_decision_details(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.feedback_enabled = False
    db.session.commit()
    setup_long_consensus(service, config)
    service.evaluate(config)
    trade = StrategyRunTrade.query.first()
    trade.closed_at = datetime.utcnow()
    trade.result = "win"
    trade.is_archived = True
    trade.archived_at = datetime.utcnow()
    db.session.commit()

    response = client.get(f"/api/orderbook-recovery/trades/{trade.id}/decision-details")
    payload = response.get_json()["obj"]

    assert response.status_code == 200
    assert payload["summary"]["is_archived"] is True
    assert payload["decision_snapshot"]["selected_side"] == "long"


def open_closed_decision_trade(service, config, pnl=1, archived=False):
    setup_long_consensus(service, config)
    service.evaluate(config)
    trade = StrategyRunTrade.query.order_by(StrategyRunTrade.id.desc()).first()
    trade.closed_at = datetime.utcnow()
    trade.exit_price = trade.entry_price + 1
    trade.pnl = pnl
    trade.result = "win" if pnl > 0 else "loss"
    trade.reason_close = "manual_close"
    trade.holding_seconds = 60
    trade.is_archived = archived
    trade.archived_at = datetime.utcnow() if archived else None
    db.session.commit()
    return trade


def csv_rows(response):
    text = response.data.decode("utf-8")
    return list(csv.DictReader(io.StringIO(text)))


def test_export_csv_excludes_archived_trades(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.feedback_enabled = False
    db.session.commit()
    active = open_closed_decision_trade(service, config, pnl=1, archived=False)
    archived = closed_trade(config, pnl=2)
    archived.is_archived = True
    archived.archived_at = datetime.utcnow()
    db.session.commit()

    response = client.get("/api/orderbook-recovery/trades/export")
    rows = csv_rows(response)

    assert response.status_code == 200
    assert [int(row["id"]) for row in rows] == [active.id]


def test_export_csv_excludes_open_trades(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.feedback_enabled = False
    db.session.commit()
    closed = open_closed_decision_trade(service, config, pnl=1, archived=False)
    open_trade(service, config, "long", 100)

    response = client.get("/api/orderbook-recovery/trades/export")
    rows = csv_rows(response)

    assert [int(row["id"]) for row in rows] == [closed.id]


def test_export_csv_includes_decision_snapshot_fields(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.feedback_enabled = False
    db.session.commit()
    open_closed_decision_trade(service, config, pnl=1, archived=False)

    response = client.get("/api/orderbook-recovery/trades/export")
    row = csv_rows(response)[0]

    assert "decision_snapshot_json" in row
    assert "per_exchange_features_json" in row
    assert "consensus_direction" in row
    assert "median_imbalance" in row
    assert "raw_average_imbalance" in row
    assert "anomalous_exchanges_count" in row
    assert "excluded_anomalous_imbalance_exchanges" in row
    assert "execution_mode" in row
    assert row["execution_mode"] == "paper"
    assert json.loads(row["decision_snapshot_json"])["selected_side"] == "long"


def test_export_json_returns_valid_json(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.feedback_enabled = False
    db.session.commit()
    trade = open_closed_decision_trade(service, config, pnl=1, archived=False)

    response = client.get("/api/orderbook-recovery/trades/export?format=json")
    payload = json.loads(response.data.decode("utf-8"))

    assert response.status_code == 200
    assert payload[0]["id"] == trade.id
    assert payload[0]["decision_snapshot_json"]


def test_long_blocked_when_consensus_is_short(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.consensus_enabled = True
    config.require_configured_exchange_signal = True
    db.session.commit()
    for exchange in ["Mexc", "Binance", "Bybit"]:
        prime_momentum(service, config, exchange, old_bid=101, old_ask=101.01)
        add_snapshot(exchange, bid=100, ask=100.01, bid_amount=3, ask_amount=10)

    result = service.evaluate(config, snapshot={
        "exchange": "Mexc",
        "symbol": "TON/USDT",
        "order_book": {"bids": [[100, 20]], "asks": [[100.01, 5]]},
        "updated_at": datetime.utcnow(),
    })

    assert result["side"] == "short"
    assert StrategyRunTrade.query.first().side != "long"


def test_short_blocked_when_consensus_is_long(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.consensus_enabled = True
    config.require_configured_exchange_signal = True
    db.session.commit()
    for exchange in ["Mexc", "Binance", "Bybit"]:
        prime_momentum(service, config, exchange)
        add_snapshot(exchange, bid=100, ask=100.01, bid_amount=10, ask_amount=5)

    result = service.evaluate(config, snapshot={
        "exchange": "Mexc",
        "symbol": "TON/USDT",
        "order_book": {"bids": [[100, 3]], "asks": [[100.01, 10]]},
        "updated_at": datetime.utcnow(),
    })

    assert result["side"] == "long"
    assert StrategyRunTrade.query.first().side != "short"


def test_no_trade_when_valid_exchanges_count_below_min_valid_exchanges(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.consensus_enabled = True
    config.min_valid_exchanges = 3
    db.session.commit()
    for exchange in ["Mexc", "Binance"]:
        prime_momentum(service, config, exchange)
        add_snapshot(exchange, bid=100, ask=100.01, bid_amount=10, ask_amount=5)

    result = service.evaluate(config)

    assert result is None
    assert StrategyRunTrade.query.count() == 0
    assert service.last_evaluation_for(config)["reject_reason"] == "not_enough_valid_exchanges"


def test_no_trade_when_configured_exchange_is_stale(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.consensus_enabled = True
    config.max_snapshot_age_seconds = 1
    db.session.commit()
    for exchange in ["Mexc", "Binance", "Bybit"]:
        prime_momentum(service, config, exchange)
        add_snapshot(exchange, bid=100, ask=100.01, bid_amount=10, ask_amount=5)
    OrderBookSnapshotStore._snapshots["Mexc"]["TON/USDT"]["updated_at"] = datetime.utcnow() - timedelta(seconds=10)

    result = service.evaluate(config)

    assert result is None
    assert StrategyRunTrade.query.count() == 0
    assert service.last_evaluation_for(config)["reject_reason"] == "stale_snapshot"


def test_no_trade_when_consensus_direction_none(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.consensus_enabled = True
    config.min_confirming_exchanges = 2
    config.min_consensus_ratio = 0.7
    db.session.commit()
    prime_momentum(service, config, "Mexc")
    add_snapshot("Mexc", bid=100, ask=100.01, bid_amount=10, ask_amount=5)
    prime_momentum(service, config, "Binance", old_bid=101, old_ask=101.01)
    add_snapshot("Binance", bid=100, ask=100.01, bid_amount=3, ask_amount=10)
    prime_momentum(service, config, "Bybit")
    add_snapshot("Bybit", bid=100, ask=100.01, bid_amount=4, ask_amount=5)

    result = service.evaluate(config)
    consensus = service.last_evaluation_for(config)["consensus"]

    assert result is None
    assert consensus["consensus_direction"] == "none"
    assert service.last_evaluation_for(config)["reject_reason"] == "no_consensus"


def test_pause_after_max_recovery_steps(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.cooldown_after_max_recovery_seconds = 600
    state = service.get_or_create_state(config)

    service.apply_recovery_after_close(state, config, "loss", datetime.utcnow())
    service.apply_recovery_after_close(state, config, "loss", datetime.utcnow())
    service.apply_recovery_after_close(state, config, "loss", datetime.utcnow())
    db.session.commit()

    assert state.is_stopped is True
    assert state.stop_reason == "max_recovery_pause"
    assert state.paused_until is not None
    assert state.current_step == 0
    assert state.current_margin == config.base_margin_usdt
    assert state.consecutive_losses == 0
    assert config.enabled is False


def test_recovery_resets_after_pause(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    state = service.get_or_create_state(config)
    state.is_stopped = True
    state.stop_reason = "max_recovery_pause"
    state.paused_until = datetime.utcnow() - timedelta(seconds=1)
    state.current_step = 3
    state.current_margin = 56
    state.consecutive_losses = 3
    state.last_trade_result = "loss"
    config.enabled = False
    db.session.commit()

    resumed = service.resume_after_recovery_pause(config, state, datetime.utcnow())

    assert resumed is True
    assert state.is_stopped is False
    assert state.stop_reason is None
    assert state.paused_until is None
    assert state.current_step == 0
    assert state.current_margin == config.base_margin_usdt
    assert state.consecutive_losses == 0
    assert config.enabled is True


def setup_long_consensus(service, config, exchanges=None):
    for exchange in exchanges or ["Mexc", "Binance", "Bybit"]:
        prime_momentum(service, config, exchange)
        add_snapshot(exchange, bid=100, ask=100.01, bid_amount=10, ask_amount=5)


def setup_short_consensus(service, config, exchanges=None, bid=98, ask=98.01):
    for exchange in exchanges or ["Mexc", "Binance", "Bybit"]:
        prime_momentum(service, config, exchange, old_bid=100, old_ask=100.01)
        add_snapshot(exchange, bid=bid, ask=ask, bid_amount=4, ask_amount=10)


def test_instant_entry_mode_opens_immediately(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.entry_mode = "instant"
    config.feedback_enabled = False
    db.session.commit()
    setup_long_consensus(service, config)

    result = service.evaluate(config)

    assert result["side"] == "long"
    assert StrategyRunTrade.query.count() == 1
    assert service.pending_entry_for(config) is None


def test_paper_execution_mode_unchanged(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.execution_mode = "paper"
    config.feedback_enabled = False
    db.session.commit()
    setup_long_consensus(service, config)

    service.evaluate(config)
    trade = StrategyRunTrade.query.first()

    assert trade.execution_mode == "paper"
    assert trade.live_exchange_order_id is None


def test_live_mode_blocked_when_confirmation_false(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.execution_mode = "live"
    config.live_enabled_confirmation = False
    config.live_kill_switch = False
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    db.session.commit()

    response = service.start_paper()

    assert response.status_code == 400
    assert response.get_json()["obj"]["msg"] == "live_confirmation_required"


def test_live_mode_blocked_when_kill_switch_true(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.execution_mode = "live"
    config.live_enabled_confirmation = True
    config.live_kill_switch = True
    db.session.commit()

    response = service.start_paper()

    assert response.status_code == 400
    assert response.get_json()["obj"]["msg"] == "live_kill_switch_enabled"


def test_live_mode_blocked_without_api_keys(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    exchange = seed_live_exchange("Mexc", api_key="", api_secret="")
    config.exchange = "Mexc"
    config.exchange_id = exchange.id
    config.symbol = "TON/USDT"
    config.execution_mode = "live"
    config.live_enabled_confirmation = True
    config.live_kill_switch = False
    db.session.commit()

    response = service.start_paper()

    assert response.status_code == 400
    assert response.get_json()["obj"]["msg"] == "live_exchange_credentials_required"


def test_live_mode_blocked_when_margin_exceeds_limit(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    exchange = seed_live_exchange("Mexc")
    config.exchange = "Mexc"
    config.exchange_id = exchange.id
    config.symbol = "TON/USDT"
    config.execution_mode = "live"
    config.live_enabled_confirmation = True
    config.live_kill_switch = False
    config.live_max_margin_usdt = 1
    state = service.get_or_create_state(config)
    state.current_margin = 7
    db.session.commit()

    response = service.start_paper()

    assert response.status_code == 400
    assert response.get_json()["obj"]["msg"] == "live_margin_exceeds_limit"


def test_live_margin_limit_uses_margin_not_notional(client):
    mock_client = MockLiveClient()
    service = OrderBookRecoveryService(live_execution_service=LiveExecutionService(client_factory=lambda exchange: mock_client))
    config = make_config(service)
    exchange = seed_live_exchange("Mexc")
    config.exchange = "Mexc"
    config.exchange_id = exchange.id
    config.symbol = "TON/USDT"
    config.base_margin_usdt = 5
    config.leverage = 2
    config.execution_mode = "live"
    config.live_enabled_confirmation = True
    config.live_kill_switch = False
    config.live_max_margin_usdt = 5
    state = service.get_or_create_state(config)
    state.current_margin = 5
    db.session.commit()
    add_snapshot("Mexc", bid=100, ask=100.01, bid_amount=10, ask_amount=5)

    response = service.start_paper()
    debug = service.debug_payload(config, state)

    assert response.status_code == 200
    assert debug["current_margin"] == 5
    assert debug["current_notional"] == 10
    assert debug["live_max_margin_usdt"] == 5
    assert debug["margin_limit_reason"] is None


def test_live_config_fields_roundtrip_through_api(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    db.session.commit()

    response = client.patch("/api/orderbook-recovery/config", json={
        "execution_mode": "live",
        "live_enabled_confirmation": True,
        "live_kill_switch": False,
        "live_max_margin_usdt": 9,
        "live_max_daily_loss_usdt": 4,
        "live_max_total_loss_usdt": 8,
        "live_order_type": "market",
        "live_fee_filter_enabled": True,
        "live_fee_filter_taker_fee_percent": 0.2,
        "momentum_confirmation_enabled": True,
        "side_quality_filter_enabled": True,
        "side_quality_lookback_trades": 4,
        "side_quality_cooldown_seconds": 120,
    })
    payload = response.get_json()["obj"]
    refreshed = client.get("/api/orderbook-recovery/config").get_json()["obj"]

    assert response.status_code == 200
    assert payload["execution_mode"] == "live"
    assert payload["live_enabled_confirmation"] is True
    assert payload["live_kill_switch"] is False
    assert payload["live_max_margin_usdt"] == 9
    assert payload["live_fee_filter_enabled"] is True
    assert payload["live_fee_filter_taker_fee_percent"] == 0.2
    assert payload["momentum_confirmation_enabled"] is True
    assert payload["side_quality_filter_enabled"] is True
    assert payload["side_quality_lookback_trades"] == 4
    assert payload["side_quality_cooldown_seconds"] == 120
    assert refreshed["execution_mode"] == "live"
    assert refreshed["live_enabled_confirmation"] is True
    assert refreshed["live_kill_switch"] is False


def test_live_fee_filter_blocks_entry_before_order_submit(client):
    mock_client = MockLiveClient()
    service = OrderBookRecoveryService(live_execution_service=LiveExecutionService(client_factory=lambda exchange: mock_client))
    config = make_config(service)
    exchange = seed_live_exchange("Mexc")
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.exchange_id = exchange.id
    config.execution_mode = "live"
    config.live_enabled_confirmation = True
    config.live_kill_switch = False
    config.live_fee_filter_enabled = True
    config.live_fee_filter_taker_fee_percent = 5
    config.feedback_enabled = False
    db.session.commit()
    setup_long_consensus(service, config)

    result = service.evaluate(config)
    diagnostics = service.signal_diagnostics_for(config)["last_100"][-1]

    assert result is None
    assert mock_client.orders == []
    assert StrategyRunTrade.query.count() == 0
    assert service.last_evaluation_for(config)["reject_reason"] == "fee_filter"
    assert diagnostics["skip_reason"] == "fee_filter"


def test_side_quality_filter_blocks_recent_net_negative_side(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.feedback_enabled = False
    config.side_quality_filter_enabled = True
    config.side_quality_lookback_trades = 3
    config.side_quality_cooldown_seconds = 600
    db.session.commit()
    for _ in range(3):
        closed_trade(config, pnl=-1, side="long")
    setup_long_consensus(service, config)

    result = service.evaluate(config)
    diagnostics = service.signal_diagnostics_for(config)["last_100"][-1]

    assert result is None
    assert StrategyRunTrade.query.count() == 3
    assert service.last_evaluation_for(config)["reject_reason"] == "side_quality_block"
    assert diagnostics["skip_reason"] == "side_quality_block"


def test_live_close_uses_net_pnl_after_fees(client):
    class FeeCloseService:
        def close_position(self, config, trade, current_price):
            return {
                "order_id": "close-1",
                "average_fill_price": 110,
                "fee": 0.02,
                "raw_response": {"id": "close-1"},
            }

        def raw_json(self, value):
            return json.dumps(value)

        def is_position_already_closed_error(self, error):
            return False

    service = OrderBookRecoveryService(live_execution_service=FeeCloseService())
    config = make_config(service)
    state = service.get_or_create_state(config)
    trade = StrategyRunTrade(
        strategy_config_id=config.id,
        exchange=config.exchange,
        symbol=config.symbol,
        side="long",
        margin=5,
        leverage=2,
        notional=10,
        amount=0.1,
        entry_price=100,
        live_entry_price=100,
        live_entry_fee=0.01,
        execution_mode="live",
        live_status="open",
        opened_at=datetime.utcnow() - timedelta(minutes=1),
    )
    db.session.add(trade)
    db.session.commit()

    closed = service.close_trade(trade, 110, 1, "take_profit", state, config, datetime.utcnow())

    assert round(closed["gross_pnl"], 6) == 1
    assert round(closed["total_fee"], 6) == 0.03
    assert round(closed["net_pnl"], 6) == 0.97
    assert round(closed["pnl"], 6) == 0.97
    assert closed["result"] == "win"


def test_ml_disabled_mode_does_not_store_feature_snapshots(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.ml_mode = "disabled"
    config.feedback_enabled = False
    db.session.commit()
    setup_long_consensus(service, config)

    result = service.evaluate(config)

    assert result["side"] == "long"
    assert MLFeatureSnapshot.query.count() == 0


def test_config_patch_ml_mode_shadow_then_get_returns_shadow(client):
    service = OrderBookRecoveryService()
    make_config(service)

    response = client.patch("/api/orderbook-recovery/config", json={"ml_mode": "shadow"})
    payload = response.get_json()["obj"]
    refreshed = client.get("/api/orderbook-recovery/config").get_json()["obj"]

    assert response.status_code == 200
    assert payload["ml_mode"] == "shadow"
    assert refreshed["ml_mode"] == "shadow"


def test_config_raw_reports_db_and_serialized_ml_mode(client):
    service = OrderBookRecoveryService()
    make_config(service)

    client.patch("/api/orderbook-recovery/config", json={"ml_mode": "shadow"})
    raw = client.get("/api/orderbook-recovery/config/raw").get_json()["obj"]

    assert raw["ml_mode_column_exists"] is True
    assert raw["db_ml_mode"] == "shadow"
    assert raw["serialized_ml_mode"] == "shadow"


def test_start_does_not_reset_ml_mode(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.ml_mode = "shadow"
    db.session.commit()

    response = client.post("/api/orderbook-recovery/start-paper")
    refreshed = client.get("/api/orderbook-recovery/config").get_json()["obj"]
    state = client.get("/api/orderbook-recovery/state").get_json()["obj"]

    assert response.status_code == 200
    assert refreshed["ml_mode"] == "shadow"
    assert state["config"]["ml_mode"] == "shadow"


def test_ml_shadow_mode_stores_feature_snapshot(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.ml_mode = "shadow"
    config.feedback_enabled = False
    db.session.commit()
    setup_long_consensus(service, config)

    result = service.evaluate(config)
    snapshots = MLFeatureSnapshot.query.order_by(MLFeatureSnapshot.id.asc()).all()

    assert result["side"] == "long"
    assert len(snapshots) >= 1
    assert snapshots[0].symbol == "TON/USDT"
    assert snapshots[0].exchange == "Mexc"
    assert snapshots[0].proposed_side == "long"
    assert snapshots[0].final_side == "long"
    assert snapshots[0].ml_score is None
    assert snapshots[0].ml_reason == "model_file_not_found"


def test_ml_shadow_mode_does_not_affect_trading_decision(client):
    class BearishShadowModel:
        def predict(self, features):
            return {
                "ml_score": 0.01,
                "ml_decision": "reject",
                "ml_reason": "shadow_prediction_only",
                "ml_model_version": "test-model",
            }

    service = OrderBookRecoveryService(ml_prediction_service=BearishShadowModel())
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.ml_mode = "shadow"
    config.feedback_enabled = False
    db.session.commit()
    setup_long_consensus(service, config)

    result = service.evaluate(config)
    diagnostics = service.signal_diagnostics_for(config)["last_100"][-1]

    assert result["side"] == "long"
    assert diagnostics["ml_score"] == 0.01
    assert diagnostics["ml_decision"] == "reject"
    assert StrategyRunTrade.query.count() == 1


def test_ml_dataset_export_returns_json(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.ml_mode = "shadow"
    config.feedback_enabled = False
    db.session.commit()
    setup_long_consensus(service, config)
    service.evaluate(config)

    response = client.get("/api/orderbook-recovery/ml/dataset/export?format=json")
    payload = json.loads(response.data.decode("utf-8"))

    assert response.status_code == 200
    assert payload
    assert payload[0]["symbol"] == "TON/USDT"
    assert "ml_score" in payload[0]


def test_ml_market_snapshot_saved_in_shadow_mode(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.ml_mode = "shadow"
    config.feedback_enabled = False
    db.session.commit()
    setup_long_consensus(service, config)

    result = service.evaluate(config)
    snapshots = MLMarketSnapshot.query.order_by(MLMarketSnapshot.id.asc()).all()

    assert result["side"] == "long"
    assert len(snapshots) >= 1
    assert snapshots[0].symbol == "TON/USDT"
    assert snapshots[0].exchange == "Mexc"
    assert snapshots[0].reference_price > 0
    assert snapshots[0].label_status == "pending"


def test_ml_market_snapshot_not_saved_when_disabled(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.ml_mode = "disabled"
    config.feedback_enabled = False
    db.session.commit()
    setup_long_consensus(service, config)

    service.evaluate(config)

    assert MLMarketSnapshot.query.count() == 0
    assert MLMarketSnapshotExchangeLabel.query.count() == 0
    assert MLMarketPriceHistory.query.count() == 0


def add_ml_label_and_history(snapshot, exchange, symbol, reference_price, prices, start_time):
    label = MLMarketSnapshotExchangeLabel(
        snapshot_id=snapshot.id,
        exchange=exchange,
        symbol=symbol,
        reference_price=reference_price,
        label_status="pending",
    )
    db.session.add(label)
    for index, price in enumerate(prices, start=1):
        db.session.add(MLMarketPriceHistory(
            timestamp=start_time + timedelta(seconds=index * 10),
            exchange=exchange,
            symbol=symbol,
            mid_price=price,
            bid=price - 0.01,
            ask=price + 0.01,
            spread=0.02,
            snapshot_age_sec=0,
        ))
    db.session.commit()
    return label


def test_ml_market_labeling_calculates_future_return(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.ml_mode = "shadow"
    config.take_profit_percent_of_margin = 1
    config.live_fee_filter_taker_fee_percent = 0
    now = datetime.utcnow()
    snapshot = MLMarketSnapshot(
        timestamp=now - timedelta(seconds=61),
        exchange="Mexc",
        symbol="TON/USDT",
        reference_price=100,
        label_status="pending",
    )
    db.session.add(snapshot)
    db.session.commit()
    add_ml_label_and_history(snapshot, "Mexc", "TON/USDT", 100, [101.01, 101.5, 100.5], snapshot.timestamp)

    result = service.label_pending_market_snapshots(config, now)
    snapshot = db.session.get(MLMarketSnapshot, snapshot.id)

    assert result["updated"] >= 1
    assert snapshot.label_status == "labeled"
    assert round(snapshot.future_price_10s, 2) == 101.01
    assert round(snapshot.future_return_10s, 4) == 0.0101
    assert round(snapshot.max_price_30s, 2) == 101.5
    assert round(snapshot.min_price_30s, 2) == 100.5
    assert round(snapshot.mfe_long_30s, 4) == 0.015
    assert round(snapshot.mae_short_30s, 4) == -0.015


def test_ml_market_long_short_labels_calculated(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.ml_mode = "shadow"
    config.take_profit_percent_of_margin = 1
    config.live_fee_filter_taker_fee_percent = 0
    now = datetime.utcnow()
    first_snapshot = MLMarketSnapshot(
        timestamp=now - timedelta(seconds=61),
        exchange="Mexc",
        symbol="TON/USDT",
        reference_price=100,
        label_status="pending",
    )
    second_snapshot = MLMarketSnapshot(
        timestamp=now - timedelta(seconds=61),
        exchange="Mexc",
        symbol="TON/USDT",
        reference_price=103,
        label_status="pending",
    )
    db.session.add(first_snapshot)
    db.session.add(second_snapshot)
    db.session.commit()
    add_ml_label_and_history(first_snapshot, "Mexc", "TON/USDT", 100, [101.01, 101.01, 101.01], first_snapshot.timestamp)
    add_ml_label_and_history(second_snapshot, "Mexc", "TON/USDT", 103, [101.01, 101.01, 101.01], second_snapshot.timestamp)

    service.label_pending_market_snapshots(config, now)
    first, second = MLMarketSnapshot.query.order_by(MLMarketSnapshot.id.asc()).all()

    assert first.long_would_win_10s is True
    assert first.short_would_win_10s is False
    assert second.long_would_win_10s is False
    assert second.short_would_win_10s is True


def test_ml_market_per_exchange_and_aggregated_labels_are_created(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.ml_mode = "shadow"
    config.feedback_enabled = False
    db.session.commit()
    setup_long_consensus(service, config)

    service.evaluate(config)

    snapshot = MLMarketSnapshot.query.first()
    labels = MLMarketSnapshotExchangeLabel.query.filter_by(snapshot_id=snapshot.id).all()

    assert snapshot is not None
    assert MLMarketPriceHistory.query.count() >= 5
    assert {label.exchange for label in labels} >= {"Mexc", "Binance", "Bybit", "__median__", "__average__"}


def test_ml_market_aggregated_median_labels_calculated(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.ml_mode = "shadow"
    config.take_profit_percent_of_margin = 1
    config.live_fee_filter_taker_fee_percent = 0
    now = datetime.utcnow()
    snapshot = MLMarketSnapshot(
        timestamp=now - timedelta(seconds=61),
        exchange="Mexc",
        symbol="TON/USDT",
        reference_price=100,
        label_status="pending",
    )
    db.session.add(snapshot)
    db.session.commit()
    add_ml_label_and_history(snapshot, "Mexc", "TON/USDT", 100, [101, 102, 101], snapshot.timestamp)
    add_ml_label_and_history(snapshot, "__median__", "TON/USDT", 100, [100.5, 101.5, 101], snapshot.timestamp)

    service.label_pending_market_snapshots(config, now)
    snapshot = db.session.get(MLMarketSnapshot, snapshot.id)

    assert round(snapshot.median_future_return_10s, 3) == 0.005
    assert round(snapshot.median_mfe_long_30s, 3) == 0.015
    assert round(snapshot.median_mae_short_30s, 3) == -0.015


def test_ml_market_snapshot_export_returns_json(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    db.session.add(MLMarketSnapshot(
        timestamp=datetime.utcnow(),
        exchange="Mexc",
        symbol="TON/USDT",
        reference_price=100,
        label_status="pending",
    ))
    db.session.commit()

    response = client.get("/api/orderbook-recovery/ml/market-snapshots/export?format=json")
    payload = json.loads(response.data.decode("utf-8"))

    assert response.status_code == 200
    assert payload[0]["symbol"] == "TON/USDT"
    assert "future_return_10s" in payload[0]
    assert "mfe_long_10s" in payload[0]
    assert "median_future_return_10s" in payload[0]
    assert "exchange_labels" in payload[0]


def test_ml_exchange_labels_export_returns_csv(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    snapshot = MLMarketSnapshot(
        timestamp=datetime.utcnow(),
        exchange="Mexc",
        symbol="TON/USDT",
        reference_price=100,
        label_status="pending",
    )
    db.session.add(snapshot)
    db.session.commit()
    add_ml_label_and_history(snapshot, "Mexc", "TON/USDT", 100, [101], snapshot.timestamp)

    response = client.get("/api/orderbook-recovery/ml/exchange-labels/export?format=csv")
    payload = response.data.decode("utf-8")

    assert response.status_code == 200
    assert "snapshot_id" in payload
    assert "mfe_long_10s" in payload
    assert "Mexc" in payload


def test_ml_feature_snapshot_pagination_and_filters(client):
    db.session.add(MLFeatureSnapshot(
        evaluation_id="eval-1",
        timestamp=datetime.utcnow(),
        symbol="TON/USDT",
        exchange="Mexc",
        proposed_side="long",
        final_side="long",
        result="win",
        ml_score=0.7,
    ))
    db.session.add(MLFeatureSnapshot(
        evaluation_id="eval-2",
        timestamp=datetime.utcnow(),
        symbol="BTC/USDT",
        exchange="Binance",
        proposed_side="short",
        final_side="none",
        result="loss",
        ml_score=None,
    ))
    db.session.commit()

    response = client.get("/api/orderbook-recovery/ml/feature-snapshots?page=1&page_size=1&exchange=Mexc&side=long&has_ml_score=true")
    payload = response.get_json()["obj"]

    assert response.status_code == 200
    assert payload["page"] == 1
    assert payload["page_size"] == 1
    assert payload["total"] == 1
    assert payload["items"][0]["exchange"] == "Mexc"


def test_ml_market_snapshot_detail_endpoint(client):
    snapshot = MLMarketSnapshot(
        timestamp=datetime.utcnow(),
        exchange="Mexc",
        symbol="TON/USDT",
        reference_price=100,
        label_status="pending",
    )
    db.session.add(snapshot)
    db.session.commit()

    response = client.get(f"/api/orderbook-recovery/ml/market-snapshots/{snapshot.id}")
    payload = response.get_json()["obj"]

    assert response.status_code == 200
    assert payload["id"] == snapshot.id
    assert payload["reference_price"] == 100


def test_ml_price_history_pagination_filters(client):
    db.session.add(MLMarketPriceHistory(
        timestamp=datetime.utcnow(),
        exchange="Mexc",
        symbol="TON/USDT",
        mid_price=2.5,
        bid=2.49,
        ask=2.51,
    ))
    db.session.add(MLMarketPriceHistory(
        timestamp=datetime.utcnow(),
        exchange="Binance",
        symbol="BTC/USDT",
        mid_price=100,
    ))
    db.session.commit()

    response = client.get("/api/orderbook-recovery/ml/price-history?symbol=TON&exchange=Mexc")
    payload = response.get_json()["obj"]

    assert response.status_code == 200
    assert payload["total"] == 1
    assert payload["items"][0]["mid_price"] == 2.5


def test_ml_exchange_label_export_respects_filters(client):
    first = MLMarketSnapshot(
        timestamp=datetime.utcnow(),
        exchange="Mexc",
        symbol="TON/USDT",
        reference_price=100,
        label_status="pending",
    )
    second = MLMarketSnapshot(
        timestamp=datetime.utcnow(),
        exchange="Binance",
        symbol="BTC/USDT",
        reference_price=100,
        label_status="pending",
    )
    db.session.add(first)
    db.session.add(second)
    db.session.commit()
    db.session.add(MLMarketSnapshotExchangeLabel(snapshot_id=first.id, exchange="Mexc", symbol="TON/USDT", reference_price=100, label_status="labeled"))
    db.session.add(MLMarketSnapshotExchangeLabel(snapshot_id=second.id, exchange="Binance", symbol="BTC/USDT", reference_price=100, label_status="pending"))
    db.session.commit()

    response = client.get("/api/orderbook-recovery/ml/exchange-labels/export?format=json&exchange=Mexc&label_status=labeled")
    payload = json.loads(response.data.decode("utf-8"))

    assert response.status_code == 200
    assert len(payload) == 1
    assert payload[0]["exchange"] == "Mexc"


def test_ml_stats_endpoint_returns_numeric_shape(client):
    response = client.get("/api/orderbook-recovery/ml/stats")
    payload = response.get_json()["obj"]

    assert response.status_code == 200
    assert payload["ml_market_snapshots_count"] == 0
    assert payload["ml_market_snapshots_pending_count"] == 0
    assert payload["ml_market_snapshots_labeled_count"] == 0
    assert payload["ml_exchange_labels_count"] == 0
    assert payload["ml_exchange_labels_pending_count"] == 0
    assert payload["ml_exchange_labels_labeled_count"] == 0


def test_live_open_order_uses_mocked_ccxt(client):
    mock_client = MockLiveClient()
    live_service = LiveExecutionService(client_factory=lambda exchange: mock_client)
    service = OrderBookRecoveryService(live_execution_service=live_service)
    config = make_config(service)
    exchange = seed_live_exchange("Mexc")
    config.exchange = "Mexc"
    config.exchange_id = exchange.id
    config.symbol = "TON/USDT"
    config.execution_mode = "live"
    config.live_enabled_confirmation = True
    config.live_kill_switch = False
    config.feedback_enabled = False
    db.session.commit()
    setup_long_consensus(service, config)

    result = service.evaluate(config)
    trade = StrategyRunTrade.query.first()

    assert result["execution_mode"] == "live"
    assert trade.live_exchange_order_id == "order-1"
    assert mock_client.orders[0]["side"] == "buy"
    assert mock_client.orders[0]["params"]["reduceOnly"] is False


def test_live_close_order_uses_reduce_only_mocked_ccxt(client):
    mock_client = MockLiveClient()
    service = OrderBookRecoveryService(live_execution_service=LiveExecutionService(client_factory=lambda exchange: mock_client))
    config = make_config(service)
    exchange = seed_live_exchange("Mexc")
    config.exchange = "Mexc"
    config.exchange_id = exchange.id
    config.symbol = "TON/USDT"
    config.execution_mode = "live"
    config.live_enabled_confirmation = True
    config.live_kill_switch = False
    config.feedback_enabled = False
    db.session.commit()
    setup_long_consensus(service, config)
    service.evaluate(config)
    trade = StrategyRunTrade.query.first()

    closed = service.close_trade(trade, 101, 1, "manual_close", service.get_or_create_state(config), config, datetime.utcnow())

    assert closed["live_close_order_id"] == "order-2"
    assert mock_client.orders[1]["params"]["reduceOnly"] is True
    assert trade.closed_at is not None


def test_live_errors_are_stored(client):
    mock_client = MockLiveClient(fail_open=True)
    service = OrderBookRecoveryService(live_execution_service=LiveExecutionService(client_factory=lambda exchange: mock_client))
    config = make_config(service)
    exchange = seed_live_exchange("Mexc")
    config.exchange = "Mexc"
    config.exchange_id = exchange.id
    config.symbol = "TON/USDT"
    config.execution_mode = "live"
    config.live_enabled_confirmation = True
    config.live_kill_switch = False
    config.feedback_enabled = False
    db.session.commit()
    setup_long_consensus(service, config)

    service.evaluate(config)
    trade = StrategyRunTrade.query.first()

    assert trade.live_status == "open_failed"
    assert "open failed" in trade.live_error


def test_mexc_open_failed_does_not_create_open_position(client):
    mock_client = MockLiveClient(exchange_id="mexc")
    requests_client = MockMexcSubmitRequests(status_code=403, text="Access Denied")
    service = OrderBookRecoveryService(live_execution_service=LiveExecutionService(client_factory=lambda exchange: mock_client, requests_client=requests_client))
    config = make_config(service)
    exchange = seed_live_exchange("Mexc")
    config.exchange = "Mexc"
    config.exchange_id = exchange.id
    config.symbol = "TON/USDT"
    config.execution_mode = "live"
    config.live_enabled_confirmation = True
    config.live_kill_switch = False
    config.feedback_enabled = False
    db.session.commit()
    setup_long_consensus(service, config)

    result = service.evaluate(config)
    trade = StrategyRunTrade.query.first()
    metrics = service.metrics()

    assert result["live_status"] == "open_failed"
    assert trade.closed_at is not None
    assert service.open_trade(config) is None
    assert service.open_positions_count(config) == 0
    assert metrics["open_position"] is None


def test_open_failed_excluded_from_win_loss_metrics(client):
    mock_client = MockLiveClient(fail_open=True)
    service = OrderBookRecoveryService(live_execution_service=LiveExecutionService(client_factory=lambda exchange: mock_client))
    config = make_config(service)
    exchange = seed_live_exchange("Mexc")
    config.exchange = "Mexc"
    config.exchange_id = exchange.id
    config.symbol = "TON/USDT"
    config.execution_mode = "live"
    config.live_enabled_confirmation = True
    config.live_kill_switch = False
    config.feedback_enabled = False
    db.session.commit()
    setup_long_consensus(service, config)

    service.evaluate(config)
    metrics = service.metrics()

    assert metrics["total_trades"] == 0
    assert metrics["win_trades"] == 0
    assert metrics["loss_trades"] == 0
    assert metrics["net_pnl"] == 0


def test_live_failed_attempt_cooldown_prevents_duplicate_attempts(client):
    mock_client = MockLiveClient(fail_open=True)
    service = OrderBookRecoveryService(live_execution_service=LiveExecutionService(client_factory=lambda exchange: mock_client))
    config = make_config(service)
    exchange = seed_live_exchange("Mexc")
    config.exchange = "Mexc"
    config.exchange_id = exchange.id
    config.symbol = "TON/USDT"
    config.execution_mode = "live"
    config.live_enabled_confirmation = True
    config.live_kill_switch = False
    config.live_open_failed_cooldown_seconds = 60
    config.feedback_enabled = False
    db.session.commit()
    setup_long_consensus(service, config)

    now = datetime.utcnow()
    service.evaluate(config, current_time=now)
    service.evaluate(config, current_time=now + timedelta(seconds=10))

    assert StrategyRunTrade.query.count() == 1
    assert service.last_evaluation_for(config)["reject_reason"] == "live_open_failed_cooldown"


def test_manual_close_live_failed_does_not_mark_trade_closed(client):
    mock_client = MockLiveClient(fail_close=True)
    service = OrderBookRecoveryService(live_execution_service=LiveExecutionService(client_factory=lambda exchange: mock_client))
    config = make_config(service)
    exchange = seed_live_exchange("Mexc")
    config.exchange = "Mexc"
    config.exchange_id = exchange.id
    config.symbol = "TON/USDT"
    config.execution_mode = "live"
    config.live_enabled_confirmation = True
    config.live_kill_switch = False
    config.feedback_enabled = False
    db.session.commit()
    setup_long_consensus(service, config)
    service.evaluate(config)
    trade = StrategyRunTrade.query.first()

    response = service.close_manual(trade.id, {"reason": "manual_close"})

    assert response.status_code == 400
    assert trade.closed_at is None
    assert trade.live_status == "close_failed"


def test_manual_close_mexc_2009_reconciles_as_closed(client):
    mock_client = MockLiveClient(exchange_id="mexc", markets={
        "BTC/USDT:USDT": {
            "id": "BTC_USDT",
            "symbol": "BTC/USDT:USDT",
            "type": "swap",
            "swap": True,
            "contract": True,
            "linear": True,
            "base": "BTC",
            "quote": "USDT",
            "settle": "USDT",
            "contractSize": 0.001,
        },
    })
    requests_client = AlreadyClosedOnCloseRequests()
    service = OrderBookRecoveryService(live_execution_service=LiveExecutionService(client_factory=lambda exchange: mock_client, requests_client=requests_client))
    config = make_config(service)
    exchange = seed_live_exchange("Mexc")
    config.exchange = "Mexc"
    config.exchange_id = exchange.id
    config.symbol = "BTC/USDT"
    config.execution_mode = "live"
    config.live_enabled_confirmation = True
    config.live_kill_switch = False
    config.feedback_enabled = False
    db.session.commit()
    for source_exchange in ["Mexc", "Binance", "Bybit"]:
        prime_momentum(service, config, source_exchange, symbol="BTC/USDT")
        add_snapshot(source_exchange, symbol="BTC/USDT", bid=100, ask=100.01, bid_amount=10, ask_amount=5)
    service.evaluate(config)
    trade = StrategyRunTrade.query.first()
    add_snapshot("Mexc", symbol="BTC/USDT", bid=101, ask=101.01, bid_amount=10, ask_amount=5)

    response = service.close_manual(trade.id, {"reason": "manual_close"})

    assert response.status_code == 200
    assert trade.closed_at is not None
    assert trade.live_status == "closed"
    assert trade.reason_close == "exchange_position_already_closed"
    assert trade.exit_price_fallback_used is True
    assert trade.exit_price_warning == "position already closed on exchange; used latest market price"
    assert trade.pnl_source == "fallback_market_price"
    assert "position already closed" in trade.live_error


def test_external_exchange_close_reconciliation_closes_local_trade(client):
    mock_client = MockLiveClient(exchange_id="mexc", positions=[], markets={
        "BTC/USDT:USDT": {
            "id": "BTC_USDT",
            "symbol": "BTC/USDT:USDT",
            "type": "swap",
            "swap": True,
            "contract": True,
            "linear": True,
            "base": "BTC",
            "quote": "USDT",
            "settle": "USDT",
            "contractSize": 0.001,
        },
    })
    requests_client = MockMexcSubmitRequests()
    service = OrderBookRecoveryService(live_execution_service=LiveExecutionService(client_factory=lambda exchange: mock_client, requests_client=requests_client))
    config = make_config(service)
    exchange = seed_live_exchange("Mexc")
    config.exchange = "Mexc"
    config.exchange_id = exchange.id
    config.symbol = "BTC/USDT"
    config.execution_mode = "live"
    config.live_enabled_confirmation = True
    config.live_kill_switch = False
    config.feedback_enabled = False
    db.session.commit()
    for source_exchange in ["Mexc", "Binance", "Bybit"]:
        prime_momentum(service, config, source_exchange, symbol="BTC/USDT")
        add_snapshot(source_exchange, symbol="BTC/USDT", bid=100, ask=100.01, bid_amount=10, ask_amount=5)
    service.evaluate(config)
    trade = StrategyRunTrade.query.first()
    trade.tp_sl_protected = False
    db.session.commit()

    result = service.evaluate(config, current_time=datetime.utcnow() + timedelta(seconds=1))

    assert result["reason_close"] == "exchange_position_closed_external"
    assert trade.closed_at is not None
    assert trade.live_status == "closed"
    assert trade.exit_price_fallback_used is True
    assert trade.pnl_source == "fallback_market_price"


def test_live_resolves_configured_symbol_to_swap_symbol(client):
    service = LiveExecutionService()
    client = MockLiveClient(markets={
        "BTC/USDT": {"symbol": "BTC/USDT", "spot": True, "base": "BTC", "quote": "USDT"},
        "BTC/USDT:USDT": {"symbol": "BTC/USDT:USDT", "swap": True, "contract": True, "linear": True, "base": "BTC", "quote": "USDT", "settle": "USDT", "type": "swap"},
    })

    market = service.resolve_live_futures_market(client, "BTC/USDT")

    assert market["symbol"] == "BTC/USDT:USDT"


def test_live_spot_market_is_rejected(client):
    service = LiveExecutionService()
    client = MockLiveClient(markets={
        "BTC/USDT": {"symbol": "BTC/USDT", "spot": True, "base": "BTC", "quote": "USDT"},
    })

    try:
        service.resolve_live_futures_market(client, "BTC/USDT")
    except Exception as error:
        assert str(error) == "live_futures_market_not_found"
    else:
        assert False, "spot market should not be accepted for live futures"


def test_live_swap_market_is_accepted(client):
    service = LiveExecutionService()
    client = MockLiveClient(markets={
        "BTC/USDT:USDT": {"symbol": "BTC/USDT:USDT", "swap": True, "linear": True, "base": "BTC", "quote": "USDT", "settle": "USDT"},
    })

    market = service.resolve_live_futures_market(client, "BTC/USDT")

    assert market["swap"] is True


def test_live_create_order_uses_resolved_futures_symbol(client):
    mock_client = MockLiveClient(markets={
        "BTC/USDT": {"symbol": "BTC/USDT", "spot": True, "base": "BTC", "quote": "USDT"},
        "BTC/USDT:USDT": {"symbol": "BTC/USDT:USDT", "swap": True, "contract": True, "linear": True, "base": "BTC", "quote": "USDT", "settle": "USDT"},
    })
    service = LiveExecutionService(client_factory=lambda exchange: mock_client)
    config = make_config(OrderBookRecoveryService())
    exchange = seed_live_exchange("Mexc")
    config.exchange = "Mexc"
    config.exchange_id = exchange.id
    config.symbol = "BTC/USDT"
    config.live_order_type = "market"
    db.session.commit()

    service.open_position(config, "long", 7, 2, 100)

    assert mock_client.orders[0]["symbol"] == "BTC/USDT:USDT"


def test_successful_mocked_mexc_futures_order_opens_trade(client):
    mock_client = MockLiveClient(exchange_id="mexc", markets={
        "BTC/USDT": {"symbol": "BTC/USDT", "spot": True, "base": "BTC", "quote": "USDT"},
        "BTC/USDT:USDT": {
            "id": "BTC_USDT",
            "symbol": "BTC/USDT:USDT",
            "type": "swap",
            "swap": True,
            "contract": True,
            "linear": True,
            "base": "BTC",
            "quote": "USDT",
            "settle": "USDT",
            "contractSize": 0.001,
        },
    })
    requests_client = MockMexcSubmitRequests()
    service = OrderBookRecoveryService(live_execution_service=LiveExecutionService(client_factory=lambda exchange: mock_client, requests_client=requests_client))
    config = make_config(service)
    exchange = seed_live_exchange("Mexc")
    config.exchange = "Mexc"
    config.exchange_id = exchange.id
    config.symbol = "BTC/USDT"
    config.execution_mode = "live"
    config.live_enabled_confirmation = True
    config.live_kill_switch = False
    config.feedback_enabled = False
    db.session.commit()
    for source_exchange in ["Mexc", "Binance", "Bybit"]:
        prime_momentum(service, config, source_exchange, symbol="BTC/USDT")
        add_snapshot(source_exchange, symbol="BTC/USDT", bid=100, ask=100.01, bid_amount=10, ask_amount=5)

    result = service.evaluate(config)
    trade = StrategyRunTrade.query.first()

    assert result["live_status"] == "open"
    assert trade.closed_at is None
    assert trade.live_exchange_order_id == "contract-order-1"
    sent_body = json.loads(requests_client.calls[1]["data"])
    assert requests_client.calls[0]["url"] == "https://contract.mexc.com/api/v1/contract/detail"
    assert requests_client.calls[0]["params"] == {"symbol": "BTC_USDT"}
    assert requests_client.calls[1]["url"] == "https://api.mexc.com/api/v1/private/order/create"
    assert sent_body["symbol"] == "BTC_USDT"
    assert sent_body["side"] == 1
    assert sent_body["openType"] == 1
    assert sent_body["type"] == 6
    assert "price" not in sent_body


def test_mexc_risk_control_code_has_clear_error(client):
    mock_client = MockLiveClient(exchange_id="mexc", markets={
        "BTC/USDT:USDT": {
            "id": "BTC_USDT",
            "symbol": "BTC/USDT:USDT",
            "type": "swap",
            "swap": True,
            "contract": True,
            "linear": True,
            "base": "BTC",
            "quote": "USDT",
            "settle": "USDT",
            "contractSize": 0.001,
        },
    })
    requests_client = MockMexcSubmitRequests(status_code=200, text='{"success":false,"code":6026,"message":"risk control"}')
    service = LiveExecutionService(client_factory=lambda exchange: mock_client, requests_client=requests_client)
    config = make_config(OrderBookRecoveryService())
    exchange = seed_live_exchange("Mexc")
    config.exchange = "Mexc"
    config.exchange_id = exchange.id
    config.symbol = "BTC/USDT"
    db.session.commit()

    try:
        service.open_position(config, "long", 7, 2, 100)
    except Exception as error:
        assert str(error) == "mexc_risk_control_verification_required"
    else:
        assert False, "MEXC 6026 should be mapped to a clear error"


def test_mexc_long_tpsl_price_calculation(client):
    service = LiveExecutionService()

    prices = service.calculate_tpsl_prices("long", 100, 10, 2, 0.25, 0.3, {"priceUnit": 0.01})

    assert prices["tp_price"] == 100.12
    assert prices["sl_price"] == 99.85
    assert round(prices["price_move_tp_pct"], 3) == 0.125
    assert round(prices["price_move_sl_pct"], 3) == 0.15


def test_mexc_short_tpsl_price_calculation(client):
    service = LiveExecutionService()

    prices = service.calculate_tpsl_prices("short", 100, 10, 2, 0.25, 0.3, {"priceUnit": 0.01})

    assert prices["tp_price"] == 99.87
    assert prices["sl_price"] == 100.15


def test_mexc_tpsl_price_rounding_by_price_unit(client):
    service = LiveExecutionService()

    prices = service.calculate_tpsl_prices("long", 123.456, 10, 2, 0.25, 0.3, {"priceUnit": 0.05})

    assert prices["tp_price"] == 123.6
    assert prices["sl_price"] == 123.25


def test_mexc_tpsl_creation_success_stores_order_ids(client):
    mock_client = MockLiveClient(exchange_id="mexc", markets={
        "BTC/USDT:USDT": {
            "id": "BTC_USDT",
            "symbol": "BTC/USDT:USDT",
            "type": "swap",
            "swap": True,
            "contract": True,
            "linear": True,
            "base": "BTC",
            "quote": "USDT",
            "settle": "USDT",
            "contractSize": 0.001,
        },
    })
    requests_client = MockMexcSubmitRequests()
    service = OrderBookRecoveryService(live_execution_service=LiveExecutionService(client_factory=lambda exchange: mock_client, requests_client=requests_client))
    config = make_config(service)
    exchange = seed_live_exchange("Mexc")
    config.exchange = "Mexc"
    config.exchange_id = exchange.id
    config.symbol = "BTC/USDT"
    config.execution_mode = "live"
    config.live_enabled_confirmation = True
    config.live_kill_switch = False
    config.take_profit_percent_of_margin = 0.25
    config.stop_loss_percent_of_margin = 0.3
    config.live_fee_filter_enabled = False
    config.feedback_enabled = False
    db.session.commit()
    for source_exchange in ["Mexc", "Binance", "Bybit"]:
        prime_momentum(service, config, source_exchange, symbol="BTC/USDT")
        add_snapshot(source_exchange, symbol="BTC/USDT", bid=100, ask=100.01, bid_amount=10, ask_amount=5)

    result = service.evaluate(config)
    trade = StrategyRunTrade.query.first()
    plan_calls = [call for call in requests_client.calls if "planorder/place/v2" in call["url"]]

    assert result["tp_sl_protected"] is True
    assert trade.tp_sl_protected is True
    assert trade.exchange_tp_order_id == "contract-order-1"
    assert trade.exchange_sl_order_id == "contract-order-1"
    assert trade.exchange_tp_price is not None
    assert trade.exchange_sl_price is not None
    assert len(plan_calls) == 2
    assert all(json.loads(call["data"])["side"] == 4 for call in plan_calls)


def test_mexc_tpsl_creation_failure_marks_trade_unprotected(client):
    mock_client = MockLiveClient(exchange_id="mexc", markets={
        "BTC/USDT:USDT": {
            "id": "BTC_USDT",
            "symbol": "BTC/USDT:USDT",
            "type": "swap",
            "swap": True,
            "contract": True,
            "linear": True,
            "base": "BTC",
            "quote": "USDT",
            "settle": "USDT",
            "contractSize": 0.001,
        },
    })
    requests_client = FailingTpSlRequests()
    service = OrderBookRecoveryService(live_execution_service=LiveExecutionService(client_factory=lambda exchange: mock_client, requests_client=requests_client))
    config = make_config(service)
    exchange = seed_live_exchange("Mexc")
    config.exchange = "Mexc"
    config.exchange_id = exchange.id
    config.symbol = "BTC/USDT"
    config.execution_mode = "live"
    config.live_enabled_confirmation = True
    config.live_kill_switch = False
    config.feedback_enabled = False
    db.session.commit()
    for source_exchange in ["Mexc", "Binance", "Bybit"]:
        prime_momentum(service, config, source_exchange, symbol="BTC/USDT")
        add_snapshot(source_exchange, symbol="BTC/USDT", bid=100, ask=100.01, bid_amount=10, ask_amount=5)

    result = service.evaluate(config)
    trade = StrategyRunTrade.query.first()

    assert result["live_status"] == "tp_sl_unprotected"
    assert trade.closed_at is None
    assert trade.tp_sl_protected is False
    assert "live_mexc_order_failed" in trade.tp_sl_error


def test_manual_close_cancels_mexc_tpsl_orders(client):
    mock_client = MockLiveClient(exchange_id="mexc", markets={
        "BTC/USDT:USDT": {
            "id": "BTC_USDT",
            "symbol": "BTC/USDT:USDT",
            "type": "swap",
            "swap": True,
            "contract": True,
            "linear": True,
            "base": "BTC",
            "quote": "USDT",
            "settle": "USDT",
            "contractSize": 0.001,
        },
    })
    requests_client = MockMexcSubmitRequests()
    service = OrderBookRecoveryService(live_execution_service=LiveExecutionService(client_factory=lambda exchange: mock_client, requests_client=requests_client))
    config = make_config(service)
    exchange = seed_live_exchange("Mexc")
    config.exchange = "Mexc"
    config.exchange_id = exchange.id
    config.symbol = "BTC/USDT"
    config.execution_mode = "live"
    config.live_enabled_confirmation = True
    config.live_kill_switch = False
    config.feedback_enabled = False
    db.session.commit()
    for source_exchange in ["Mexc", "Binance", "Bybit"]:
        prime_momentum(service, config, source_exchange, symbol="BTC/USDT")
        add_snapshot(source_exchange, symbol="BTC/USDT", bid=100, ask=100.01, bid_amount=10, ask_amount=5)
    service.evaluate(config)
    trade = StrategyRunTrade.query.first()

    response = service.close_manual(trade.id, {"reason": "manual_close"})
    cancel_calls = [call for call in requests_client.calls if "planorder/cancel" in call["url"]]

    assert response.status_code == 200
    assert len(cancel_calls) == 2


def test_tpsl_does_not_affect_paper_mode(client):
    service = OrderBookRecoveryService()
    config = make_config(service)

    trade_payload = open_trade(service, config, "long", 100)
    trade = StrategyRunTrade.query.first()

    assert trade_payload["execution_mode"] == "paper"
    assert trade.tp_sl_protected is False
    assert trade.exchange_tp_order_id is None


def test_mexc_client_uses_swap_options(client):
    captured = {}

    class FakeMexc:
        def __init__(self, options):
            captured.update(options)

    original = getattr(__import__("ccxt"), "mexc")
    setattr(__import__("ccxt"), "mexc", FakeMexc)
    try:
        service = LiveExecutionService()
        config = make_config(OrderBookRecoveryService())
        exchange = seed_live_exchange("Mexc")
        config.exchange = "Mexc"
        config.exchange_id = exchange.id
        db.session.commit()
        service.client(config)
    finally:
        setattr(__import__("ccxt"), "mexc", original)

    assert captured["options"]["defaultType"] == "swap"
    assert captured["options"]["defaultSettle"] == "USDT"


def test_debug_returns_live_market_fields(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    exchange = seed_live_exchange("Mexc")
    config.exchange = "Mexc"
    config.exchange_id = exchange.id
    config.symbol = "BTC/USDT"
    config.execution_mode = "live"
    db.session.commit()
    OrderBookRecoveryService._live_market_infos[service.debug_key(config)] = {
        "configured_symbol": "BTC/USDT",
        "resolved_live_symbol": "BTC/USDT:USDT",
        "live_market_type": "swap",
        "live_market_valid": True,
        "live_market_error": None,
    }

    debug = service.debug_payload(config, service.get_or_create_state(config))

    assert debug["resolved_live_symbol"] == "BTC/USDT:USDT"
    assert debug["live_market_type"] == "swap"
    assert debug["live_market_valid"] is True
    assert debug["live_market_error"] is None


def test_two_step_mode_creates_pending_entry_without_opening(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.entry_mode = "two_step_confirmation"
    config.feedback_enabled = False
    db.session.commit()
    setup_long_consensus(service, config)

    result = service.evaluate(config)

    assert result is None
    assert StrategyRunTrade.query.count() == 0
    assert service.pending_entry_for(config)["side"] == "long"
    assert service.last_evaluation_for(config)["reject_reason"] == "confirmation_pending"


def test_two_step_long_opens_after_confirmation(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.entry_mode = "two_step_confirmation"
    config.feedback_enabled = False
    db.session.commit()
    setup_long_consensus(service, config)
    now = datetime.utcnow()

    service.evaluate(config, current_time=now)
    result = service.evaluate(config, current_time=now + timedelta(seconds=2))
    trade = StrategyRunTrade.query.first()

    assert result["side"] == "long"
    assert trade.entry_mode == "two_step_confirmation"
    assert trade.confirmation_result == "confirmed"
    assert trade.confirmation_delay_actual_seconds == 2
    assert service.pending_entry_for(config) is None


def test_two_step_short_opens_after_confirmation(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.entry_mode = "two_step_confirmation"
    config.feedback_enabled = False
    db.session.commit()
    setup_short_consensus(service, config)
    now = datetime.utcnow()

    service.evaluate(config, current_time=now)
    result = service.evaluate(config, current_time=now + timedelta(seconds=2))

    assert result["side"] == "short"
    assert StrategyRunTrade.query.first().entry_mode == "two_step_confirmation"


def test_two_step_cancels_if_direction_flips(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.entry_mode = "two_step_confirmation"
    config.feedback_enabled = False
    db.session.commit()
    setup_long_consensus(service, config)
    now = datetime.utcnow()
    service.evaluate(config, current_time=now)
    setup_short_consensus(service, config)

    result = service.evaluate(config, current_time=now + timedelta(seconds=2))

    assert result is None
    assert StrategyRunTrade.query.count() == 0
    assert service.pending_entry_for(config) is None
    assert service.last_evaluation_for(config)["reject_reason"] == "confirmation_failed_direction_changed"


def test_two_step_cancels_if_consensus_becomes_invalid(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.entry_mode = "two_step_confirmation"
    config.feedback_enabled = False
    config.confirmation_max_wait_seconds = 10
    db.session.commit()
    setup_long_consensus(service, config)
    now = datetime.utcnow()

    service.evaluate(config, current_time=now)
    result = service.evaluate(config, current_time=now + timedelta(seconds=6))

    assert result is None
    assert StrategyRunTrade.query.count() == 0
    assert service.pending_entry_for(config) is None
    assert service.last_evaluation_for(config)["reject_reason"] == "confirmation_failed_configured_exchange_invalid"


def test_two_step_cancels_if_momentum_weakens(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.entry_mode = "two_step_confirmation"
    config.feedback_enabled = False
    db.session.commit()
    setup_long_consensus(service, config)
    now = datetime.utcnow()
    service.evaluate(config, current_time=now)
    for exchange in ["Mexc", "Binance", "Bybit"]:
        add_snapshot(exchange, bid=99.5, ask=99.51, bid_amount=10, ask_amount=5)

    result = service.evaluate(config, current_time=now + timedelta(seconds=2))

    assert result is None
    assert StrategyRunTrade.query.count() == 0
    assert service.pending_entry_for(config) is None
    assert service.last_evaluation_for(config)["reject_reason"] == "confirmation_failed_momentum_not_improved"


def test_two_step_expires_after_max_wait(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.entry_mode = "two_step_confirmation"
    config.feedback_enabled = False
    config.max_snapshot_age_seconds = 60
    db.session.commit()
    setup_long_consensus(service, config)
    now = datetime.utcnow()

    service.evaluate(config, current_time=now)
    result = service.evaluate(config, current_time=now + timedelta(seconds=6))

    assert result is None
    assert StrategyRunTrade.query.count() == 0
    assert service.pending_entry_for(config) is None
    assert service.last_evaluation_for(config)["reject_reason"] == "confirmation_expired"


def test_export_includes_confirmation_fields(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.entry_mode = "two_step_confirmation"
    config.feedback_enabled = False
    db.session.commit()
    setup_long_consensus(service, config)
    now = datetime.utcnow()
    service.evaluate(config, current_time=now)
    service.evaluate(config, current_time=now + timedelta(seconds=2))
    trade = StrategyRunTrade.query.first()
    trade.closed_at = datetime.utcnow()
    trade.exit_price = trade.entry_price + 1
    trade.pnl = 1
    trade.result = "win"
    db.session.commit()

    row = csv_rows(client.get("/api/orderbook-recovery/trades/export"))[0]

    assert row["entry_mode"] == "two_step_confirmation"
    assert row["confirmation_delay_actual_seconds"] == "2.0"
    assert row["first_signal_snapshot_json"]
    assert row["confirmation_snapshot_json"]


def test_strategy_does_not_open_during_recovery_pause(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.feedback_enabled = False
    state = service.get_or_create_state(config)
    state.is_stopped = True
    state.stop_reason = "max_recovery_pause"
    state.paused_until = datetime.utcnow() + timedelta(minutes=10)
    state.current_step = 0
    state.current_margin = config.base_margin_usdt
    config.enabled = False
    db.session.commit()
    setup_long_consensus(service, config)

    result = service.evaluate(config)

    assert result is None
    assert StrategyRunTrade.query.count() == 0


def test_after_pause_expires_next_trade_starts_from_step_zero(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.feedback_enabled = False
    config.max_snapshot_age_seconds = 1000
    state = service.get_or_create_state(config)
    state.is_stopped = True
    state.stop_reason = "max_recovery_pause"
    state.paused_until = datetime.utcnow() - timedelta(seconds=1)
    state.current_step = 0
    state.current_margin = config.base_margin_usdt
    config.enabled = False
    db.session.commit()
    setup_long_consensus(service, config)

    result = service.evaluate(config, current_time=datetime.utcnow())
    trade = StrategyRunTrade.query.first()

    assert result["side"] == "long"
    assert trade.recovery_step == 0
    assert trade.margin == config.base_margin_usdt
    assert state.current_step == 0
    assert state.current_margin == config.base_margin_usdt


def test_feedback_blocks_side_after_three_consecutive_losses(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.feedback_enabled = True
    config.side_loss_streak_limit = 3
    config.side_cooldown_seconds = 600
    db.session.commit()
    closed_trade(config, pnl=-1, side="long")
    closed_trade(config, pnl=-1, side="long")
    closed_trade(config, pnl=-1, side="long")
    setup_long_consensus(service, config)

    result = service.evaluate(config)
    debug = service.debug_payload(config, service.get_or_create_state(config))

    assert result is None
    assert StrategyRunTrade.query.filter(StrategyRunTrade.closed_at.is_(None)).count() == 0
    assert debug["blocked_side"] == "long"
    assert debug["feedback_reject_reason"] == "long_loss_streak_cooldown"


def test_feedback_adaptive_consensus_increases_when_win_rate_low(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.feedback_enabled = True
    config.min_consensus_ratio = 0.6
    config.adaptive_consensus_boost = 0.1
    config.min_side_win_rate = 40
    db.session.commit()
    closed_trade(config, pnl=-1, side="long")
    closed_trade(config, pnl=-1, side="long")
    closed_trade(config, pnl=0.1, side="long")
    prime_momentum(service, config, "Mexc")
    add_snapshot("Mexc", bid=100, ask=100.01, bid_amount=10, ask_amount=5)
    prime_momentum(service, config, "Binance")
    add_snapshot("Binance", bid=100, ask=100.01, bid_amount=10, ask_amount=5)
    prime_momentum(service, config, "Bybit")
    add_snapshot("Bybit", bid=100, ask=100.01, bid_amount=4, ask_amount=5)

    result = service.evaluate(config)
    debug = service.debug_payload(config, service.get_or_create_state(config))

    assert result is None
    assert round(debug["adaptive_min_consensus_ratio"], 2) == 0.7
    assert debug["feedback_reject_reason"] == "adaptive_consensus_ratio_not_met"


def test_feedback_adaptive_min_valid_exchanges_increases_after_weak_confirmation_losses(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.feedback_enabled = True
    config.min_valid_exchanges = 2
    config.adaptive_min_valid_exchanges_boost = 1
    config.min_side_win_rate = 0
    config.adaptive_consensus_boost = 0
    db.session.commit()
    closed_trade(config, pnl=-1, side="long", signal_valid_exchanges_count=2, signal_momentum=1)
    setup_long_consensus(service, config, ["Mexc", "Binance"])

    result = service.evaluate(config)
    debug = service.debug_payload(config, service.get_or_create_state(config))

    assert result is None
    assert debug["adaptive_min_valid_exchanges"] == 3
    assert debug["feedback_reject_reason"] == "adaptive_min_valid_exchanges_not_met"


def test_feedback_disabled_keeps_old_behavior(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.feedback_enabled = False
    db.session.commit()
    closed_trade(config, pnl=-1, side="long")
    closed_trade(config, pnl=-1, side="long")
    closed_trade(config, pnl=-1, side="long")
    setup_long_consensus(service, config)

    result = service.evaluate(config)

    assert result["side"] == "long"


def test_feedback_winning_trade_resets_side_loss_streak(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    config.feedback_enabled = True
    config.min_side_win_rate = 0
    config.adaptive_consensus_boost = 0
    config.adaptive_min_valid_exchanges_boost = 0
    db.session.commit()
    now = datetime.utcnow()
    closed_trade(config, pnl=-1, side="long", closed_at=now - timedelta(minutes=4))
    closed_trade(config, pnl=-1, side="long", closed_at=now - timedelta(minutes=3))
    closed_trade(config, pnl=-1, side="long", closed_at=now - timedelta(minutes=2))
    closed_trade(config, pnl=1, side="long", closed_at=now - timedelta(minutes=1))
    setup_long_consensus(service, config)

    result = service.evaluate(config)
    debug = service.debug_payload(config, service.get_or_create_state(config))

    assert debug["long_loss_streak"] == 0
    assert result["side"] == "long"


def test_forward_test_creates_run_and_status(client):
    response = client.post("/api/orderbook-recovery/run-forward-test", json={
        "duration_minutes": 0.01,
        "exchange": "binance",
        "symbol": "BTC/USDT",
        "config": {"base_margin_usdt": 7},
    })

    assert response.status_code == 200
    run_id = response.get_json()["obj"]["run_id"]
    status = client.get(f"/api/orderbook-recovery/forward-tests/{run_id}")

    assert status.status_code == 200
    assert status.get_json()["obj"]["id"] == run_id
    assert status.get_json()["obj"]["status"] == "running"


def test_start_changes_state_to_running(client):
    response = client.post("/api/orderbook-recovery/start-paper", json={})

    assert response.status_code == 200
    payload = response.get_json()["obj"]
    assert payload["status"] == "running"
    assert payload["enabled"] is True


def test_debug_endpoint_returns_reason_when_no_snapshot(client):
    client.post("/api/orderbook-recovery/start-paper", json={})

    response = client.get("/api/orderbook-recovery/debug")

    assert response.status_code == 200
    payload = response.get_json()["obj"]
    assert payload["status"] == "running"
    assert payload["latest_snapshot"] is None
    assert payload["reason_if_not_trading"] == "No order book snapshots received for this exchange/symbol"


def test_scanner_hook_updates_last_evaluation(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    state = service.get_or_create_state(config)
    state.current_margin = config.base_margin_usdt
    OrderBookSnapshotStore.update("binance", "BTC/USDT", {
        "bids": [[100.00, 20], [99.99, 10], [99.98, 10], [99.97, 10], [99.96, 10]],
        "asks": [[100.01, 10], [100.02, 10], [100.03, 10], [100.04, 10], [100.05, 10]],
    })
    service.on_order_book_snapshot("binance", "BTC/USDT")

    debug = service.debug_payload(config, state)

    assert debug["scanner_hook_active"] is True
    assert debug["latest_snapshot"]["updated_at"] is not None
    assert debug["last_evaluation"]["imbalance"] > 1
    assert debug["last_evaluation"]["evaluated_at"] is not None


def test_config_binance_and_hook_binance_title_match(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    match = service.hook_match(config, "Binance", "BTC/USDT", {"exchange_title": "Binance", "raw_pair": "BTC/USDT"})

    assert match["exchange_match"] is True


def test_config_slash_symbol_and_hook_plain_symbol_match(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    match = service.hook_match(config, "binance", "BTCUSDT", {"raw_pair": "BTCUSDT"})

    assert match["symbol_match"] is True
    assert match["normalized_config_symbol"] == "BTCUSDT"
    assert match["normalized_hook_symbol"] == "BTCUSDT"


def test_config_slash_symbol_and_hook_futures_symbol_match(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    match = service.hook_match(config, "binance", "BTC/USDT:USDT", {"raw_pair": "BTC/USDT:USDT"})

    assert match["symbol_match"] is True
    assert match["normalized_hook_symbol"] == "BTCUSDT"


def test_mismatch_symbol_rejected_with_visible_reason(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    service.on_order_book_snapshot("binance", "ETH/USDT", {"exchange_title": "Binance", "raw_pair": "ETH/USDT"})

    debug = service.debug_payload(config, service.get_or_create_state(config))

    assert debug["exchange_match"] is True
    assert debug["symbol_match"] is False
    assert debug["last_evaluation"]["reject_reason"] == "Snapshot received but ignored because symbol mismatch"
    assert debug["reason_if_not_trading"] == "Snapshot received but ignored because symbol mismatch"


def test_valid_normalized_match_updates_last_evaluation(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "binance"
    config.symbol = "BTC/USDT"
    db.session.commit()
    OrderBookSnapshotStore.update("Binance", "BTCUSDT", {
        "bids": [[100.00, 20], [99.99, 10], [99.98, 10], [99.97, 10], [99.96, 10]],
        "asks": [[100.01, 10], [100.02, 10], [100.03, 10], [100.04, 10], [100.05, 10]],
    })

    service.on_order_book_snapshot("Binance", "BTCUSDT", {"exchange_title": "Binance", "raw_pair": "BTCUSDT"})
    debug = service.debug_payload(config, service.get_or_create_state(config))

    assert debug["exchange_match"] is True
    assert debug["symbol_match"] is True
    assert debug["latest_snapshot"]["updated_at"] is not None
    assert debug["last_evaluation"]["imbalance"] > 1


def test_later_same_symbol_other_exchange_does_not_override_matching_snapshot(client):
    service = OrderBookRecoveryService()
    config = make_config(service)
    config.exchange = "Mexc"
    config.symbol = "TON/USDT"
    db.session.commit()
    OrderBookSnapshotStore.update("Mexc", "TON/USDT", {
        "bids": [[3.00, 20], [2.99, 10], [2.98, 10], [2.97, 10], [2.96, 10]],
        "asks": [[3.001, 10], [3.01, 10], [3.02, 10], [3.03, 10], [3.04, 10]],
    }, metadata={"source_exchange_title": "Mexc", "source_exchange_id": 10, "source_pair": "TON/USDT"})
    service.on_order_book_snapshot("Mexc", "TON/USDT", {"exchange_id": 10, "exchange_title": "Mexc", "raw_pair": "TON/USDT"})
    service.on_order_book_snapshot("AscendEX", "TON/USDT", {"exchange_id": 11, "exchange_title": "AscendEX", "raw_pair": "TON/USDT"})

    debug = service.debug_payload(config, service.get_or_create_state(config))

    assert debug["last_snapshot_source_exchange"] == "Mexc"
    assert debug["last_snapshot_source_pair"] == "TON/USDT"
    assert debug["last_hook_exchange"] == "Mexc"
    assert debug["exchange_match"] is True
    assert debug["symbol_match"] is True
    assert debug["last_evaluation"]["imbalance"] > 1


def test_frontend_api_has_debug_method():
    frontend_api = Path("/Users/emilhambardzumyan/WebstormProjects/arbinator/src/api/orderBookRecovery.js").read_text()

    assert "getDebug()" in frontend_api
    assert "/orderbook-recovery/debug" in frontend_api


def test_frontend_close_position_button_visible_only_with_open_position():
    frontend_view = Path("/Users/emilhambardzumyan/WebstormProjects/arbinator/src/views/orderBookRecovery/v-order-book-recovery.vue").read_text()
    frontend_api = Path("/Users/emilhambardzumyan/WebstormProjects/arbinator/src/api/orderBookRecovery.js").read_text()

    assert 'v-if="openPosition"' in frontend_view
    assert "Close Position" in frontend_view
    assert "Close current paper position manually?" in frontend_view
    assert "CLOSE_MANUAL" in frontend_view
    assert "/close-manual" in frontend_api


def test_frontend_archive_controls_exist():
    frontend_view = Path("/Users/emilhambardzumyan/WebstormProjects/arbinator/src/views/orderBookRecovery/v-order-book-recovery.vue").read_text()
    frontend_api = Path("/Users/emilhambardzumyan/WebstormProjects/arbinator/src/api/orderBookRecovery.js").read_text()

    assert "Net PnL" in frontend_view
    assert "Show archived trades" in frontend_view
    assert "Archive all closed trades" in frontend_view
    assert "Unarchive all" in frontend_view
    assert "Delete archived trade permanently?" in frontend_view
    assert "Delete all archived trades" in frontend_view
    assert "Delete all archived trades permanently?" in frontend_view
    assert "DELETE_ARCHIVED_TRADE" in frontend_view
    assert "DELETE_ALL_ARCHIVED_TRADES" in frontend_view
    assert "archive-all-closed" in frontend_api
    assert "unarchive-all" in frontend_api
    assert "delete-archived" in frontend_api
    assert "delete-all-archived" in frontend_api
    assert "deleteArchivedTrade" in frontend_api
    assert "deleteAllArchivedTrades" in frontend_api
    assert "include_archived" in frontend_api


def test_frontend_decision_details_controls_exist():
    frontend_view = Path("/Users/emilhambardzumyan/WebstormProjects/arbinator/src/views/orderBookRecovery/v-order-book-recovery.vue").read_text()
    frontend_api = Path("/Users/emilhambardzumyan/WebstormProjects/arbinator/src/api/orderBookRecovery.js").read_text()

    assert "View Details" in frontend_view
    assert "Decision Snapshot" in frontend_view
    assert "Per-exchange order book" in frontend_view
    assert "decision-details" in frontend_api


def test_frontend_export_api_method_exists():
    frontend_view = Path("/Users/emilhambardzumyan/WebstormProjects/arbinator/src/views/orderBookRecovery/v-order-book-recovery.vue").read_text()
    frontend_api = Path("/Users/emilhambardzumyan/WebstormProjects/arbinator/src/api/orderBookRecovery.js").read_text()

    assert "Export non-archived trades" in frontend_view
    assert "Export JSON" in frontend_view
    assert "No non-archived closed trades to export" in frontend_view
    assert "exportTrades" in frontend_api
    assert "/orderbook-recovery/trades/export" in frontend_api


def test_frontend_exchange_tpsl_fields_visible():
    frontend_view = Path("/Users/emilhambardzumyan/WebstormProjects/arbinator/src/views/orderBookRecovery/v-order-book-recovery.vue").read_text()

    assert "TP/SL protection" in frontend_view
    assert "Exchange TP/SL not created" in frontend_view
    assert "Exchange position already closed" in frontend_view
    assert "Closed externally on exchange" in frontend_view
    assert "exit_price_warning" in frontend_view
    assert "pnl_source" in frontend_view
    assert "tp_sl_protected" in frontend_view
    assert "exchange_tp_price" in frontend_view
    assert "exchange_sl_price" in frontend_view
    assert "exchange_tp_order_id" in frontend_view
    assert "exchange_sl_order_id" in frontend_view
    assert "tp_sl_error" in frontend_view


def test_frontend_entry_mode_controls_exist():
    frontend_view = Path("/Users/emilhambardzumyan/WebstormProjects/arbinator/src/views/orderBookRecovery/v-order-book-recovery.vue").read_text()
    frontend_api = Path("/Users/emilhambardzumyan/WebstormProjects/arbinator/src/api/orderBookRecovery.js").read_text()
    frontend_store = Path("/Users/emilhambardzumyan/WebstormProjects/arbinator/src/store/modules/orderBookRecovery.js").read_text()

    assert "Entry mode" in frontend_view
    assert "two_step_confirmation" in frontend_view
    assert "Confirmation delay sec" in frontend_view
    assert "Require same direction" in frontend_view
    assert "Pending Entry" in frontend_view
    assert "pending_entry_exists" in frontend_view
    assert "Execution mode" in frontend_view
    assert "Live enabled confirmation" in frontend_view
    assert "Live kill switch" in frontend_view
    assert "WARNING: Live mode places real orders on the selected exchange." in frontend_view
    assert "You are enabling LIVE trading. Real orders may be placed on the exchange." in frontend_view
    assert "Live max margin USDT" in frontend_view
    assert "Resolved live symbol" in frontend_view
    assert "Live market type" in frontend_view
    assert "Live market valid" in frontend_view
    assert "Live market error" in frontend_view
    assert "Signal diagnostics max rows" in frontend_view
    assert "Clear diagnostics" in frontend_view
    assert "/orderbook-recovery/diagnostics/clear" in frontend_api
    assert "CLEAR_DIAGNOSTICS" in frontend_store
    assert "Reset recovery to base margin" in frontend_view
    assert "Set current margin" in frontend_view
    assert "/orderbook-recovery/recovery/reset" in frontend_api
    assert "/orderbook-recovery/recovery/set-current-margin" in frontend_api
    assert "RESET_RECOVERY" in frontend_store
    assert "SET_CURRENT_MARGIN" in frontend_store


def test_alembic_upgrade_head_succeeds_and_keeps_required_tables(tmp_path):
    db_path = tmp_path / "alembic_safety.db"
    env = dict(os.environ)
    env["DB_CONNECTION_STRING"] = f"sqlite:///{db_path}"
    env["FLASK_APP"] = "src"
    env["LIVE_TRADING_ENABLED"] = "false"
    result = subprocess.run(
        [sys.executable, "-m", "flask", "db", "upgrade", "head"],
        cwd=str(Path(__file__).resolve().parents[1]),
        env=env,
        text=True,
        capture_output=True,
        timeout=60,
    )

    assert result.returncode == 0, result.stderr
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("select name from sqlite_master where type='table'").fetchall()
    finally:
        conn.close()
    tables = {row[0] for row in rows}
    required_tables = {
        "exchange",
        "trading_pair",
        "paper_order",
        "paper_position",
        "trade_signal",
        "arbitrage_opportunity",
        "backtest_run",
        "backtest_trade",
        "strategy_candidate",
        "order_book_pattern_strategy_config",
        "recovery_state",
        "strategy_run",
        "strategy_run_trade",
    }

    assert required_tables.issubset(tables)
