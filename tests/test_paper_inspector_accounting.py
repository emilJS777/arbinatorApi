from datetime import datetime, timedelta
import pytest
from src import db
from src.OrderBookRecovery.SignalFeedbackService import SignalFeedbackService
from src.OrderBookRecovery.FuturesSnapshotStore import FuturesSnapshotStore
from test_execution_safety import setup_service, book
from test_paper_sessions_pending import reserve, closed


def test_paper_close_stores_two_fees_and_inspector_does_not_abandon(client):
    service, config, state = setup_service()
    now = datetime.utcnow()
    trade = reserve(service, config, state, now)
    trade.live_status = None
    trade.live_entry_fee = .014
    db.session.commit()
    snap = book(at=now + timedelta(seconds=1))
    FuturesSnapshotStore.update('mexc', config.symbol, snap['order_book'], snap['metadata'], received_at=now + timedelta(seconds=1))
    service.close_trade(trade, 100, 0, 'manual_close', state, config, now + timedelta(seconds=1))
    assert trade.live_exit_fee is not None
    assert trade.total_fee == pytest.approx(trade.live_entry_fee + trade.live_exit_fee)
    assert trade.net_pnl == pytest.approx(trade.gross_pnl - trade.total_fee + float(trade.funding_pnl or 0))
    details = service.decision_details_payload(trade)
    assert details['trade']['abandoned_at'] is None
    assert details['summary']['accounting_status'] == 'active_history'
    assert details['trade']['closed_at'] is not None
    assert details['execution_config']['execution_mode'] == 'paper'


def test_session_accounting_feedback_and_abandonment_remain_separate(client):
    service, config, state = setup_service()
    service.create_paper_session(config)
    db.session.commit()
    normal = closed(config, 'paper', -.03, config.paper_session_id)
    legacy = closed(config, 'paper', -9, None)
    abandoned = closed(config, 'paper', -8, config.paper_session_id)
    abandoned.abandoned_at = datetime.utcnow()
    db.session.commit()
    assert service.metrics()['total_trades'] == 1
    assert [row.id for row in SignalFeedbackService().recent_trades(config)] == [normal.id]
    assert service.decision_details_payload(abandoned)['summary']['accounting_status'] == 'abandoned_unverified'
    assert service.decision_details_payload(legacy)['summary']['accounting_status'] == 'active_history'
