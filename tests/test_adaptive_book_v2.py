from datetime import datetime, timedelta
from types import SimpleNamespace
import pytest
from src import db
from src.OrderBookRecovery.AdaptiveBookV2 import AdaptiveBookV2, exit_decision, VERSION
from src.OrderBookRecovery.SignalFeedbackService import SignalFeedbackService
from src.OrderBookRecovery.FuturesSnapshotStore import FuturesSnapshotStore
from test_adaptive_book_v1 import inputs
from test_execution_safety import setup_service, book
from test_paper_sessions_pending import closed


def test_fifteen_losses_block_during_cooldown_not_forever(client):
    service, config, state = setup_service()
    config.feedback_enabled = True
    config.side_loss_streak_limit = 3
    config.side_cooldown_seconds = 600
    now = datetime.utcnow()
    for i in range(15):
        trade = closed(config, 'paper', -.03, config.paper_session_id)
        trade.side = 'short'
        trade.closed_at = now - timedelta(seconds=15-i)
    db.session.commit()
    feedback = SignalFeedbackService()
    assert feedback.side_stats(config, 'short')['loss_streak'] == 15
    consensus = {'consensus_ratio_short': 1, 'valid_exchanges_count': 4, 'average_momentum': -.01}
    allowed, report = feedback.evaluate(config, 'short', consensus, now)
    assert not allowed and report['feedback_reject_reason'] == 'short_loss_streak_cooldown'
    assert report['cooldown_until']
    assert feedback.evaluate(config, 'short', consensus, now + timedelta(seconds=601))[0]


def test_runtime_loss_limit_stops_entries(client):
    service, config, state = setup_service()
    config.max_consecutive_losses = 3
    for _ in range(3):
        service.apply_recovery_after_close(state, config, 'loss')
    assert state.is_stopped and not config.enabled
    assert state.consecutive_losses == 0  # Different from historical side loss streak.


def test_candidate_neutral_unavailable_do_not_equal_reversal():
    config, consensus = inputs()
    now = datetime.utcnow()
    state = {}
    for t in (0, 2, 4, 6, 8):
        reason, state = exit_decision(config, state, 'short', None, consensus, now+timedelta(seconds=t), now+timedelta(seconds=t), now, -.03, 7)
        assert reason is None and state['classification'] == 'neutral'
    for t in (10, 12, 14, 16):
        reason, state = exit_decision(config, state, 'short', 'long', consensus, now+timedelta(seconds=t), now+timedelta(seconds=t), now, -.03, 7)
    assert reason == 'candidate_confirmed_reversal'  # Loss allowed; no profit requirement.
    consensus['configured_exchange_valid'] = False
    reason, state = exit_decision(config, state, 'short', 'long', consensus, now+timedelta(seconds=17), now+timedelta(seconds=17), now, -.03, 7)
    assert reason is None and state['count'] == 0 and state['classification'] == 'unavailable'


def test_renewal_and_restart_fail_closed():
    config, consensus = inputs()
    AdaptiveBookV2._history.clear()
    AdaptiveBookV2._renewal.clear()
    now = datetime.utcnow()
    previous = SimpleNamespace(id=1, side='long', closed_at=now-timedelta(seconds=1))
    for t in (0, 2, 4, 6, 8):
        at = now+timedelta(seconds=t)
        assert AdaptiveBookV2.entry(config, 'long', consensus, book(at=at), at, 7, previous)[0] is None
    consensus['consensus_direction'] = 'none'
    for t in (10, 12, 14, 16):
        at = now+timedelta(seconds=t)
        assert AdaptiveBookV2.entry(config, None, consensus, book(at=at), at, 7, previous)[0] is None
    consensus['consensus_direction'] = 'long'
    for t in (18, 20, 22, 24):
        at = now+timedelta(seconds=t)
        side, _ = AdaptiveBookV2.entry(config, 'long', consensus, book(at=at), at, 7, previous)
    assert side == 'long'
    AdaptiveBookV2._renewal.clear()
    assert AdaptiveBookV2.entry(config, 'long', consensus, book(at=at), at, 7, previous)[0] is None


def test_candidate_service_delayed_fill_pause_and_hard_stop(client):
    service, config, state = setup_service()
    config.strategy_version = VERSION
    config.consensus_enabled = True
    config.paper_latency_ms = 250
    config.take_profit_percent_of_margin = 1.8
    config.stop_loss_percent_of_margin = .9
    service.create_paper_session(config)
    db.session.commit()
    AdaptiveBookV2._history.clear()
    AdaptiveBookV2._renewal.clear()
    now = datetime.utcnow()
    for t in (0, 2, 4, 6, 8, 10):
        at = now+timedelta(seconds=t)
        for venue in ('mexc','bybit'):
            snap = book(exchange=venue, price=100+t*.001, at=at)
            FuturesSnapshotStore.update(venue, config.symbol, snap['order_book'], snap['metadata'], received_at=at)
        service.evaluate(config, current_time=at)
    trade = service.open_trade(config)
    assert trade and trade.live_status != 'paper_pending'
    service.stop()
    for t in (12, 13):
        at = now+timedelta(seconds=t)
        snap = book(price=98, at=at)
        FuturesSnapshotStore.update('mexc', config.symbol, snap['order_book'], snap['metadata'], received_at=at)
        service.evaluate(config, current_time=at)
    assert trade.closed_at and trade.reason_close == 'stop_loss'
    assert trade.total_fee > 0 and trade.net_pnl < 0
    assert service.live_start_rejection(SimpleNamespace(strategy_version=VERSION)) == 'experimental_paper_only'
