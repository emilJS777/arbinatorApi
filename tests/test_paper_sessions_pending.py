from datetime import datetime, timedelta
import pytest
from src import db
from src.OrderBookRecovery.OrderBookRecoveryModel import StrategyRunTrade, ExecutionSlot, PaperSession
from src.OrderBookRecovery.OrderBookRecoveryService import OrderBookRecoveryService
from src.OrderBookRecovery.FuturesSnapshotStore import FuturesSnapshotStore
from test_execution_safety import setup_service, book, store


def reserve(service, config, state, now):
    service.exchange_feature(config, book(price=99, at=now - timedelta(seconds=1)), now)
    snapshot = book(at=now)
    FuturesSnapshotStore.update(snapshot['exchange'], snapshot['symbol'], snapshot['order_book'], snapshot['metadata'], received_at=now)
    result = service.open_position(config, state, {'mid_price': 100, 'imbalance': 2, 'short_momentum': .01, 'spread_percent': .02}, 'long', now)
    return db.session.get(StrategyRunTrade, result['id'])


def closed(config, mode, pnl, session_id=None):
    trade = StrategyRunTrade(strategy_config_id=config.id, execution_mode=mode, paper_session_id=session_id,
        exchange='mexc', symbol='VELVET/USDT', side='long', margin=7, leverage=1, notional=7,
        amount=.07, entry_price=100, pnl=pnl, closed_at=datetime.utcnow(), result='win' if pnl > 0 else 'loss')
    db.session.add(trade)
    db.session.commit()
    return trade


def test_mode_and_session_accounting_preserves_history(client):
    service, config, state = setup_service()
    old = closed(config, 'paper', -10)
    closed(config, 'live', 9000)
    assert service.available_equity(config) == config.paper_equity_usdt - 10
    service.stop()
    response = client.post('/api/orderbook-recovery/paper-sessions')
    assert response.status_code == 200
    assert config.paper_session_id
    assert service.available_equity(config) == config.paper_equity_usdt
    assert service.metrics()['total_trades'] == 0
    trade = closed(config, 'paper', -2, config.paper_session_id)
    trade.is_archived = True
    db.session.commit()
    assert service.daily_loss(config) == -2
    assert service.total_loss(config) == -2
    assert service.available_equity(config) == config.paper_equity_usdt - 2
    assert db.session.get(StrategyRunTrade, old.id) is not None
    config.paper_equity_usdt = 50000
    db.session.commit()
    assert service.available_equity(config) == 9998  # Initial session capital is immutable.


def test_restart_retains_session_and_expires_pending_without_book(client):
    service, config, state = setup_service()
    config.pending_entry_ttl_seconds = 1
    now = datetime.utcnow()
    trade = reserve(service, config, state, now)
    session_id = config.paper_session_id
    replacement = OrderBookRecoveryService(publisher=service.publisher)
    replacement.snapshot_for = lambda *_: None
    replacement.evaluate(config, current_time=now + timedelta(seconds=2))
    assert trade.live_status == 'paper_cancelled'
    assert trade.reason_close == 'pending_entry_expired'
    assert db.session.get(ExecutionSlot, config.id) is None
    assert replacement.metrics()['total_trades'] == 0
    assert config.paper_session_id == session_id


def test_pause_wins_before_fill_and_stale_worker_cannot_resurrect(client):
    service, config, state = setup_service()
    now = datetime.utcnow()
    trade = reserve(service, config, state, now)
    frozen = service.trade_config(config, trade)
    service.stop()
    store(book(price=100.1, at=now + timedelta(milliseconds=300)))
    replacement = OrderBookRecoveryService(publisher=service.publisher)
    replacement.evaluate_open_trade(trade, 100.1, state, frozen, now + timedelta(milliseconds=300))
    assert trade.live_status == 'paper_cancelled'
    assert trade.result == 'cancelled'
    assert trade.live_entry_fee is None
    assert service.open_trade(config) is None


@pytest.mark.parametrize('change', ['side', 'risk'])
def test_pending_revalidates_direction_and_emergency_block(client, change):
    service, config, state = setup_service()
    now = datetime.utcnow()
    trade = reserve(service, config, state, now)
    if change == 'risk':
        config.emergency_entry_block = True
        db.session.commit()
    store(book(price=98, imbalance=.5, at=now + timedelta(milliseconds=300)))
    service.evaluate(config, current_time=now + timedelta(milliseconds=300))
    assert trade.live_status == 'paper_cancelled'


def test_wide_spread_stop_works_after_pause(client):
    service, config, state = setup_service()
    config.stop_loss_percent_of_margin = .1
    now = datetime.utcnow()
    trade = reserve(service, config, state, now)
    service.evaluate(config, current_time=now)
    assert trade.live_status is None
    service.stop()
    snapshot = book(at=now + timedelta(seconds=1))
    snapshot['order_book'] = {'bids': [[98, 10]], 'asks': [[102, 10]]}
    store(snapshot)
    service.evaluate(config, current_time=now + timedelta(seconds=1))
    assert trade.closed_at is not None
    assert trade.reason_close == 'stop_loss'
    assert trade.exit_price == 98


def test_manual_exit_missing_depth_persists_until_executable(client):
    service, config, state = setup_service()
    now = datetime.utcnow()
    trade = reserve(service, config, state, now)
    service.evaluate(config, current_time=now)
    frozen = service.trade_config(config, trade)
    service.snapshot_for = lambda *_: None
    service.close_trade(trade, 999, 999, 'manual_close', state, frozen, now)
    assert trade.closed_at is None and trade.exit_price is None
    assert trade.paper_close_reason == 'manual_close'
    assert trade.paper_exit_status.startswith('unresolved')
    snapshot = book(at=now + timedelta(seconds=1))
    snapshot['order_book'] = {'bids': [[99, 10]], 'asks': [[103, 10]]}
    service.snapshot_for = lambda *_: snapshot
    service.evaluate(config, current_time=now + timedelta(seconds=1))
    assert trade.closed_at is not None and trade.exit_price == 99
    assert trade.paper_exit_status == 'filled'


def test_session_cannot_reset_active_position(client):
    service, config, state = setup_service()
    now = datetime.utcnow()
    trade = reserve(service, config, state, now)
    service.evaluate(config, current_time=now)
    service.stop()
    before = PaperSession.query.count()
    service.new_paper_session()
    assert PaperSession.query.count() == before
    assert trade.closed_at is None


def test_state_distinguishes_pending_intent_from_filled_position(client):
    service, config, state = setup_service()
    trade = reserve(service, config, state, datetime.utcnow())
    payload = client.get('/api/orderbook-recovery/state').get_json()['obj']
    assert payload['open_position'] is None
    assert payload['pending_order']['id'] == trade.id
    assert payload['pending_order']['pending_entry_expires_at']
    assert payload['paper_execution_model'] == 'fixed_latency_full_depth_no_partial_fills_no_queue'
    service.stop()
    payload = client.get('/api/orderbook-recovery/state').get_json()['obj']
    assert payload['pending_order'] is None
