from datetime import datetime, timedelta
from types import SimpleNamespace
import json
import pytest
from src import db
from src.OrderBookRecovery.AdaptiveTrendV3 import AdaptiveTrendV3, VERSION
from src.OrderBookRecovery.FuturesSnapshotStore import FuturesSnapshotStore
from test_adaptive_book_v1 import inputs
from test_execution_safety import book, setup_service


@pytest.mark.parametrize('side,sign', [('long', 1), ('short', -1)])
def test_sixty_second_gate_symmetric_and_no_early_entry(side, sign):
    config, consensus = inputs()
    config.exchange, config.symbol = 'mexc', 'VELVET/USDT'
    AdaptiveTrendV3.clear_config(config.id)
    now = datetime.utcnow()
    consensus.update(consensus_direction=side, average_momentum=sign*.01)
    for t in range(0, 61, 2):
        at = now + timedelta(seconds=t)
        selected, report = AdaptiveTrendV3.entry(config, side, consensus, book(at=at, price=100+sign*t*.001), at, 7)
        assert (selected == side) == (t == 60)
    assert report['version'] == VERSION and report['trend60']['elapsed_seconds'] == 60
    # Trend turns against the candidate at delayed revalidation, despite same consensus.
    at = now + timedelta(seconds=61)
    selected, report = AdaptiveTrendV3.entry(config, side, consensus, book(at=at, price=100-sign*.1), at, 7)
    assert selected is None and report['reject_reason'] == 'trend60_direction_not_confirmed'


def test_gap_future_duplicate_restart_and_session_isolation():
    config, _ = inputs()
    config.exchange, config.symbol = 'mexc', 'VELVET/USDT'
    AdaptiveTrendV3.clear_config(config.id)
    now = datetime.utcnow()
    for t in (0, 60):
        at = now + timedelta(seconds=t)
        assert AdaptiveTrendV3.trend(config, 'long', book(at=at), at)['reject_reason'] == 'trend60_history_unavailable'
    at = now + timedelta(seconds=60)
    assert AdaptiveTrendV3.trend(config, 'long', book(at=at), now)['reject_reason'] == 'trend60_stale_or_future_book'
    AdaptiveTrendV3.clear_config(config.id)
    for t in range(61):
        at = now + timedelta(seconds=t)
        result = AdaptiveTrendV3.trend(config, 'long', book(at=at, price=100+t*.001), at)
    assert result['reject_reason'] is None
    assert AdaptiveTrendV3.trend(config, 'long', book(at=at, price=100.06), at)['source_at'] == result['source_at']
    config.paper_session_id = 'new-session'
    assert AdaptiveTrendV3.trend(config, 'long', book(at=at), at)['reject_reason'] == 'trend60_history_unavailable'
    AdaptiveTrendV3.clear_config(config.id)
    assert AdaptiveTrendV3.trend(config, 'long', book(at=at), at)['reject_reason'] == 'trend60_history_unavailable'


def test_paper_only_and_default_unchanged(client):
    service, config, state = setup_service()
    assert config.strategy_version == 'baseline'
    assert service.live_start_rejection(SimpleNamespace(strategy_version=VERSION)) == 'experimental_paper_only'
    with pytest.raises(ValueError, match='paper_only'):
        service.apply_config_overrides(config, {'strategy_version': VERSION, 'execution_mode': 'live'})
    db.session.rollback()


def test_actual_v3_fill_pause_hard_sl_and_fees(client):
    service, config, state = setup_service()
    config.strategy_version = VERSION
    config.consensus_enabled = True
    config.paper_latency_ms = 250
    config.take_profit_percent_of_margin = 1.8
    config.stop_loss_percent_of_margin = .9
    service.create_paper_session(config)
    db.session.commit()
    AdaptiveTrendV3.clear_config(config.id)
    now = datetime.utcnow()
    for t in range(0, 65, 2):
        at = now + timedelta(seconds=t)
        for venue in ('mexc', 'bybit'):
            snap = book(exchange=venue, price=100+t*.001, at=at)
            FuturesSnapshotStore.update(venue, config.symbol, snap['order_book'], snap['metadata'], received_at=at)
        service.evaluate(config, current_time=at)
        if t < 60:
            assert service.open_trade(config) is None
    trade = service.open_trade(config)
    assert trade and trade.live_status != 'paper_pending'
    assert json.loads(trade.decision_snapshot_json)['experiment']['trend60']['elapsed_seconds'] >= 60
    service.stop()
    for t in (66, 67):
        at = now + timedelta(seconds=t)
        snap = book(price=98, at=at)
        FuturesSnapshotStore.update('mexc', config.symbol, snap['order_book'], snap['metadata'], received_at=at)
        service.evaluate(config, current_time=at)
    assert trade.closed_at and trade.reason_close == 'stop_loss'
    assert trade.total_fee > 0 and trade.net_pnl < 0
