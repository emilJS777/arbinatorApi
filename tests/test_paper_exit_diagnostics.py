from datetime import datetime, timedelta
from types import SimpleNamespace
from src import db
from src.OrderBookRecovery.PaperExitDiagnostics import exit_diagnostics
from test_execution_safety import setup_service, book, store
from test_paper_sessions_pending import reserve


def test_deadline_requires_new_book_and_depth():
    now = datetime.utcnow()
    trade = SimpleNamespace(paper_close_requested_at=now-timedelta(seconds=2), side='short', amount=1)
    config = SimpleNamespace(paper_latency_ms=250, max_snapshot_age_seconds=5)
    assert exit_diagnostics(trade, config, None, now)['exit_block_reason'] == 'missing_execution_book'
    assert exit_diagnostics(trade, config, book(at=now-timedelta(seconds=2)), now)['exit_block_reason'] == 'no_new_book_after_latency'
    snapshot = book(at=now)
    snapshot['order_book']['asks'] = [[100, .01]]
    assert exit_diagnostics(trade, config, snapshot, now)['exit_block_reason'] == 'insufficient_exit_depth'
    result = exit_diagnostics(trade, config, book(at=now), now, now)
    assert result['exit_block_reason'] == 'awaiting_management_tick'
    assert result['pending_age_seconds'] == 2
    assert result['worker_age_seconds'] == 0


def test_periodic_worker_recovers_paused_paper_exit_without_hook(client):
    service, config, state = setup_service()
    now = datetime.utcnow()
    trade = reserve(service, config, state, now)
    service.evaluate_open_trade(trade, 100, state, config, datetime.utcnow())
    assert trade.live_status != 'paper_pending'
    service.stop()
    trade.paper_close_requested_at = datetime.utcnow()-timedelta(seconds=2)
    trade.paper_close_reason = 'manual_close'
    trade.paper_exit_status = 'pending_fixed_latency'
    db.session.commit()
    from src.OrderBookRecovery.FuturesSnapshotStore import FuturesSnapshotStore
    FuturesSnapshotStore.clear()
    service.reconcile_existing_positions()
    assert trade.closed_at is None
    assert trade.paper_exit_status == 'unresolved_no_fresh_valid_book'
    store(book())
    service.reconcile_existing_positions()
    assert trade.closed_at is not None
    assert trade.reason_close == 'manual_close'
