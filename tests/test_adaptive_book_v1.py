from datetime import datetime, timedelta
from types import SimpleNamespace
import json

import pytest
from src import db
from src.OrderBookRecovery.AdaptiveBookV1 import AdaptiveBookV1, costs, exit_decision, settings, VERSION
from src.OrderBookRecovery.OrderBookRecoveryModel import StrategyRunTrade
from test_execution_safety import setup_service, book, store


def inputs():
    config = SimpleNamespace(id=90, exchange="mexc", symbol="VELVET/USDT", execution_mode="paper",
        experiment_settings={}, max_snapshot_age_seconds=5, consensus_enabled=True, min_valid_exchanges=2,
        leverage=1, paper_taker_fee_percent=.1, take_profit_percent_of_margin=1.8)
    consensus = {"configured_exchange_valid": True, "valid_exchanges_count": 2, "consensus_direction": "long",
                 "configured_exchange_long_signal": True, "configured_exchange_short_signal": True, "average_momentum": .01}
    AdaptiveBookV1.reset(config.id)
    return config, consensus


@pytest.mark.parametrize("side", ["long", "short"])
def test_persistence_new_timestamps_and_costs(side):
    config, consensus = inputs()
    consensus.update(consensus_direction=side, average_momentum=.01 if side == 'long' else -.01)
    now = datetime.utcnow()
    for seconds in (0, 0, 1, 3):
        at = now + timedelta(seconds=seconds)
        result, report = AdaptiveBookV1.entry(config, side, consensus, book(at=at), at, 7)
        assert (result == side) == (seconds == 3)
    assert report['distinct_books'] == 3
    assert report['trade_flow'] == 'unavailable_not_used'
    result, _ = AdaptiveBookV1.entry(config, side, consensus, book(at=now), now + timedelta(seconds=20), 7)
    assert result is None


def test_cost_components_not_double_counted():
    result = costs({'asks': [[101, 1], [102, 1]], 'bids': [[99, 2]]}, 'long', 2, .1, 7, 1.8, 5)
    assert result['spread_depth_usdt'] == 5
    assert result['roundtrip_fees_usdt'] == pytest.approx(.401)
    assert result['funding_reserve_usdt'] == pytest.approx(.1015)
    assert result['estimated_cost_usdt'] == pytest.approx(5.5025)
    assert result['gross_tp_usdt'] == pytest.approx(.126)


def test_cost_hurdle_and_live_rejection():
    config, consensus = inputs()
    config.take_profit_percent_of_margin = .1
    now = datetime.utcnow()
    for t in (0, 1, 3):
        at = now + timedelta(seconds=t)
        side, report = AdaptiveBookV1.entry(config, 'long', consensus, book(at=at), at, 7)
    assert side is None and report['reject_reason'] == 'experimental_cost_hurdle'
    config.execution_mode = 'live'
    assert AdaptiveBookV1.entry(config, 'long', consensus, book(at=at), at, 7)[1]['reject_reason'] == 'experimental_paper_only'


def test_exit_requires_sustained_new_books_and_max_hold():
    now = datetime.utcnow()
    state = {}
    for seconds in (0, 0, 1, 3):
        at = now + timedelta(seconds=seconds)
        reason, state = exit_decision({}, state, 'long', 'short', at, at, now, -.01, 7)
        assert bool(reason) == (seconds == 3)
    assert exit_decision({}, state, 'long', 'long', now, now + timedelta(seconds=121), now, 0, 7)[0] == 'experimental_max_hold'
    assert exit_decision({}, state, 'long', 'long', now + timedelta(seconds=4), now + timedelta(seconds=4), now, 0, 7)[1]['weak_count'] == 0


def test_bad_settings_rejected():
    for value in ({'cost_hurdle': 0}, {'max_hold_seconds': float('nan')}, {'unexpected': 2}):
        with pytest.raises(ValueError):
            settings(value)


def test_service_paper_lifecycle_immutable_pause_and_delayed_exit(client):
    service, config, state = setup_service()
    AdaptiveBookV1.reset(config.id)
    config.strategy_version = VERSION
    config.experiment_settings = settings({'max_hold_seconds': 5})
    config.consensus_enabled = True
    config.paper_latency_ms = 250
    config.take_profit_percent_of_margin = 1.8
    config.stop_loss_percent_of_margin = .9
    config.leverage = 1
    service.create_paper_session(config)
    db.session.commit()
    now = datetime.utcnow()
    for seconds in (0, 1, 3, 4, 5):
        at = now + timedelta(seconds=seconds)
        for venue in ('mexc', 'bybit'):
            snapshot = book(exchange=venue, price=100 + seconds * .001, at=at)
            # The store uses wall time by default; replay must retain historical receipt.
            from src.OrderBookRecovery.FuturesSnapshotStore import FuturesSnapshotStore
            FuturesSnapshotStore.update(venue, config.symbol, snapshot['order_book'], snapshot['metadata'], received_at=at)
        service.evaluate(config, current_time=at)
    trade = service.open_trade(config)
    assert trade and trade.live_status != 'paper_pending'
    frozen = service.trade_config(config, trade)
    assert frozen.strategy_version == VERSION
    config.experiment_settings = settings({'max_hold_seconds': 3600})
    config.take_profit_percent_of_margin = 50
    db.session.commit()
    service.stop()
    at = now + timedelta(seconds=10)
    snapshot = book(at=at)
    FuturesSnapshotStore.update('mexc', config.symbol, snapshot['order_book'], snapshot['metadata'], received_at=at)
    service.evaluate(config, current_time=at)
    assert trade.closed_at is None
    assert trade.paper_close_reason == 'experimental_max_hold'
    service.evaluate(config, current_time=at + timedelta(seconds=1))
    assert trade.closed_at is None  # No post-delay source book, no fabricated fill.
    snapshot = book(at=at + timedelta(seconds=1))
    FuturesSnapshotStore.update('mexc', config.symbol, snapshot['order_book'], snapshot['metadata'], received_at=at + timedelta(seconds=1))
    service.evaluate(config, current_time=at + timedelta(seconds=1))
    assert trade.closed_at and trade.net_pnl is not None
    assert not config.enabled
    assert frozen.take_profit_percent_of_margin == 1.8


def test_baseline_defaults_and_live_config_guard(client):
    service, config, state = setup_service()
    assert config.strategy_version == 'baseline'
    assert service.live_start_rejection(SimpleNamespace(strategy_version=VERSION)) == 'experimental_paper_only'
    with pytest.raises(ValueError, match='paper_only'):
        service.apply_config_overrides(config, {'strategy_version': VERSION, 'execution_mode': 'live'})
    db.session.rollback()


def test_reversal_and_restart_require_new_persistence():
    config, consensus = inputs()
    now = datetime.utcnow()
    for seconds in (0, 1, 3):
        at = now + timedelta(seconds=seconds)
        AdaptiveBookV1.entry(config, 'long', consensus, book(at=at), at, 7)
    consensus['configured_exchange_long_signal'] = False
    assert AdaptiveBookV1.entry(config, 'long', consensus, book(at=at), at, 7)[0] is None
    consensus['configured_exchange_long_signal'] = True
    assert AdaptiveBookV1.entry(config, 'long', consensus, book(at=at), at, 7)[1]['distinct_books'] == 1
    AdaptiveBookV1.reset(config.id)  # Process restart loses warmup, never authorizes immediate entry.
    assert AdaptiveBookV1.entry(config, 'long', consensus, book(at=at), at, 7)[0] is None


def test_hard_stop_precedes_max_hold_and_persists_after_restart(client):
    from test_paper_sessions_pending import reserve
    from src.OrderBookRecovery.OrderBookRecoveryService import OrderBookRecoveryService
    from src.OrderBookRecovery.FuturesSnapshotStore import FuturesSnapshotStore
    service, config, state = setup_service()
    now = datetime.utcnow()
    config.paper_latency_ms = 250
    trade = reserve(service, config, state, now)
    # Persisted filled-position fixture; no exchange calls or simulated live orders.
    frozen = service.parse_json(trade.execution_config_json)
    frozen.update(strategy_version=VERSION, experiment_settings=settings({'max_hold_seconds': 5}),
        stop_loss_percent_of_margin=.9, take_profit_percent_of_margin=1.8)
    trade.execution_config_json = json.dumps(frozen)
    trade.live_status = None
    trade.live_entry_fee = .007
    db.session.commit()
    service.stop()
    at = now + timedelta(seconds=6)
    snapshot = book(price=98, at=at)
    FuturesSnapshotStore.update('mexc', config.symbol, snapshot['order_book'], snapshot['metadata'], received_at=at)
    service.evaluate(config, current_time=at)
    assert trade.paper_close_reason == 'stop_loss'
    assert trade.closed_at is None
    replacement = OrderBookRecoveryService(publisher=service.publisher)
    snapshot = book(price=97, at=at + timedelta(seconds=1))
    FuturesSnapshotStore.update('mexc', config.symbol, snapshot['order_book'], snapshot['metadata'], received_at=at + timedelta(seconds=1))
    replacement.evaluate(config, current_time=at + timedelta(seconds=1))
    assert trade.closed_at and trade.reason_close == 'stop_loss'
    assert trade.exit_price == pytest.approx(96.99)


def test_experiment_settings_validation_is_4xx(client):
    response = client.patch('/api/orderbook-recovery/config', json={'experiment_settings': {'cost_hurdle': -1}})
    assert response.status_code == 400
