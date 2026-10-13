from datetime import datetime, timedelta
import json
import pytest
from src import db
from src.OrderBookRecovery.PaperShadowMeasurement import observe
from src.OrderBookRecovery.FuturesSnapshotStore import FuturesSnapshotStore
from test_execution_safety import setup_service, book
from test_paper_sessions_pending import reserve


def fixture_trade():
    service, config, state = setup_service()
    now = datetime.utcnow()
    config.paper_latency_ms = 250
    trade = reserve(service, config, state, now)
    frozen = json.loads(trade.execution_config_json)
    frozen.update(strategy_version='adaptive_book_v2', experiment_settings={'max_hold_seconds': 120},
                  take_profit_percent_of_margin=1.8, stop_loss_percent_of_margin=.9)
    trade.execution_config_json = json.dumps(frozen)
    db.session.commit()
    return service, config, state, trade, now


def snapshot(config, at, price=100):
    snap = book(price=price, at=at)
    FuturesSnapshotStore.update('mexc', config.symbol, snap['order_book'], snap['metadata'], received_at=at)


def test_pending_exit_observes_120_from_fill_not_request(client):
    service, config, state, trade, now = fixture_trade()
    trade.live_status = None
    trade.live_entry_fee = .007
    # Actual fill is later than the reservation, and latency remains authoritative.
    trade.opened_at = now + timedelta(seconds=1)
    db.session.commit()
    service.stop()
    at = trade.opened_at + timedelta(seconds=120)
    snapshot(config, at)
    service.evaluate(config, current_time=at)
    assert trade.closed_at is None and trade.paper_close_requested_at
    later = at + timedelta(seconds=1)
    snapshot(config, later)
    service.evaluate(config, current_time=later)
    assert trade.closed_at == later
    data = json.loads(trade.decision_snapshot_json)
    assert data['entry_quality_measurement']['horizons']['120']['status'] == 'observed'
    assert data['paper_shadow_prices']['phase'] == 'post_exit'


def test_pending_failure_preserves_exact_reasons(client, monkeypatch):
    service, config, state, trade, now = fixture_trade()
    at = now + timedelta(seconds=1)
    snapshot(config, at)
    monkeypatch.setattr(service, 'signal', lambda *args, **kwargs: (None, {
        'reject_reason': 'no_consensus', 'valid_exchanges_count': 1,
        'configured_exchange_valid': True, 'average_momentum': 0}))
    service.evaluate_open_trade(trade, 100, state, service.trade_config(config, trade), at)
    data = json.loads(trade.decision_snapshot_json)
    assert trade.result == 'cancelled'
    assert 'momentum_wrong_sign_or_zero' in data['pending_revalidation']['direction_rejections'][trade.side]
    assert data['pending_revalidation']['final_reject_reason'] == 'no_consensus'
    assert data['pending_cancellation']['reason'] == 'no_consensus'


def test_cancel_shadow_survives_restart_without_ledger_changes(client):
    from src.OrderBookRecovery.OrderBookRecoveryService import OrderBookRecoveryService
    service, config, state, trade, now = fixture_trade()
    service.cancel_paper_entry(trade, 'expired', now)
    ledger = (trade.pnl, trade.result, trade.entry_price, trade.exit_price, trade.total_fee)
    at = now + timedelta(seconds=1)
    snapshot(config, at)
    service.observe_recent_paper_shadows(at)
    at = now + timedelta(seconds=16)
    snapshot(config, at, 101)
    replacement = OrderBookRecoveryService(publisher=service.publisher)
    replacement.observe_recent_paper_shadows(at)
    db.session.refresh(trade)
    data = json.loads(trade.decision_snapshot_json)['paper_shadow_prices']
    assert data['horizons']['15']['status'] == 'observed'
    assert (trade.pnl, trade.result, trade.entry_price, trade.exit_price, trade.total_fee) == ledger
    before = json.loads(trade.decision_snapshot_json)
    replacement.observe_recent_paper_shadows(now + timedelta(seconds=130))
    assert json.loads(trade.decision_snapshot_json) == before


def test_shadow_missing_reference_future_duplicates_and_cap():
    now = datetime(2026, 10, 13)
    state = {'event_at': now.isoformat()}
    assert observe(state, now + timedelta(seconds=1), now, 100, 'short') == state
    missing = observe(state, now + timedelta(seconds=6), now + timedelta(seconds=6), 100, 'short')
    assert missing['status'] == 'missing_reference_window' and 'reference_price' not in missing
    data = observe(state, now, now, 100, 'short')
    assert observe(data, now, now, 99, 'short') == data
    data = observe(data, now + timedelta(seconds=15), now + timedelta(seconds=15), 99, 'short')
    assert data['horizons']['15']['signed_return'] == pytest.approx(.01)
    assert observe(data, now + timedelta(seconds=126), now + timedelta(seconds=126), 98, 'short') == data


def test_measurement_failure_cannot_block_close(client, monkeypatch):
    service, config, state, trade, now = fixture_trade()
    trade.live_status = None
    trade.live_entry_fee = .007
    db.session.commit()
    def broken(*args, **kwargs):
        raise RuntimeError('measurement unavailable')
    monkeypatch.setattr(service, 'measure_pending_exit', broken)
    snapshot(config, now)
    service.close_trade(trade, 100, 0, 'stop_loss', state, config, now)
    assert trade.closed_at is None
    at = now + timedelta(seconds=1)
    snapshot(config, at)
    service.close_trade(trade, 100, 0, 'stop_loss', state, config, at)
    assert trade.closed_at and trade.reason_close == 'stop_loss'
