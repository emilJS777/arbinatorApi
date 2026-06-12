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
from src.OrderBookRecovery.OrderBookRecoveryModel import StrategyRunTrade
from src.OrderBookRecovery.OrderBookNormalizer import OrderBookNormalizer
from src.OrderBookRecovery.OrderBookRecoveryService import OrderBookRecoveryService


def make_config(service):
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
