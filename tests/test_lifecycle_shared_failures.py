import json
import pytest
from src import db
from src.OrderBookRecovery.OrderBookRecoveryService import OrderBookRecoveryService
from src.OrderBookRecovery.OrderBookRecoveryController import OrderBookRecoveryStartController, OrderBookRecoveryStopController
from src.OrderBookRecovery.OrderBookRecoveryModel import StrategyRun, StrategyRunTrade, PaperSession
from test_execution_safety import setup_service, store, book
from test_legacy_paper_close import legacy_position


@pytest.mark.parametrize('snapshot', [None, '[]', '{broken', '{}', '{"config":{"paper_latency_ms":null}}'])
def test_save_start_pause_state_with_unusable_persisted_legacy_snapshot(client, snapshot):
    service, config, state = setup_service()
    trade = legacy_position(service, config)
    trade_id = trade.id
    trade.decision_snapshot_json = snapshot
    trade.execution_config_json = None
    db.session.commit()
    response = client.patch('/api/orderbook-recovery/config', json={'emergency_entry_block':False})
    assert response.status_code == 200
    started = client.post('/api/orderbook-recovery/start-paper')
    assert started.status_code == 200
    assert started.json['obj']['paper_exit_diagnostics']['exit_block_reason'] == 'legacy_paper_execution_config_review_required'
    assert started.json['obj']['open_position']['id'] == trade_id
    assert client.post('/api/orderbook-recovery/stop', json={}).status_code == 200
    assert client.get('/api/orderbook-recovery/state').status_code == 200
    result = client.post(f'/api/orderbook-recovery/positions/{trade_id}/close-manual', json={})
    assert result.status_code == 409
    assert result.json['obj']['code'] == 'legacy_paper_execution_config_review_required'
    assert db.session.get(StrategyRunTrade, trade_id).closed_at is None
    assert client.post('/api/orderbook-recovery/paper-sessions').status_code == 409
    restarted = OrderBookRecoveryService(publisher=service.publisher)
    restarted.reconcile_existing_positions()
    assert db.session.get(StrategyRunTrade, trade_id).closed_at is None


@pytest.mark.parametrize('path,method', [('/config','update_config'),('/start-paper','start_paper'),
    ('/stop','stop'),('/paper-sessions','new_paper_session'),('/positions/1/close-manual','close_manual')])
def test_lifecycle_unexpected_errors_have_safe_incident_and_trace(client, monkeypatch, caplog, path, method):
    setup_service()
    def fail(*args, **kwargs):
        raise RuntimeError('PRIVATE_SENTINEL_NEVER_LOG')
    monkeypatch.setattr(OrderBookRecoveryService, method, fail)
    response = getattr(client, 'patch' if path == '/config' else 'post')('/api/orderbook-recovery'+path, json={})
    assert response.status_code == 500
    assert response.json['obj']['incident_id'] in caplog.text
    assert 'traceback=' in caplog.text
    assert 'PRIVATE_SENTINEL_NEVER_LOG' not in caplog.text + response.get_data(as_text=True)


def test_committed_start_pause_survive_notification_and_labeler_failure(client, monkeypatch):
    service, config, state = setup_service()
    def fail(*args, **kwargs):
        raise RuntimeError('event_or_thread_failure')
    monkeypatch.setattr(OrderBookRecoveryStartController.service.publisher,'publish',fail)
    monkeypatch.setattr(OrderBookRecoveryStopController.service.publisher,'publish',fail)
    monkeypatch.setattr(OrderBookRecoveryService,'start_ml_market_labeler',fail)
    first = client.post('/api/orderbook-recovery/start-paper')
    assert first.status_code == 200
    assert first.json['obj']['enabled'] is True
    session_id = first.json['obj']['config']['paper_session_id']
    repeated = client.post('/api/orderbook-recovery/start-paper')
    assert repeated.status_code == 200
    assert StrategyRun.query.filter_by(status='running').count() == 1
    assert PaperSession.query.count() == 1
    assert repeated.json['obj']['config']['paper_session_id'] == session_id
    assert client.post('/api/orderbook-recovery/stop', json={}).status_code == 200
    assert service.get_or_create_config().enabled is False


def test_expected_bad_save_and_invalid_json_roll_back(client):
    service, config, state = setup_service()
    old_margin = config.base_margin_usdt
    result = client.patch('/api/orderbook-recovery/config', json={'base_margin_usdt':-1,'ml_mode':'shadow'})
    assert result.status_code == 400
    assert service.get_or_create_config().base_margin_usdt == old_margin
    assert service.get_or_create_config().ml_mode == 'disabled'
    assert client.patch('/api/orderbook-recovery/config', json=[]).status_code == 400
    assert client.post('/api/orderbook-recovery/start-paper',data='{broken',content_type='application/json').status_code == 400
    for payload in ({'enabled':'true'}, {'execution_mode':[]}, {'paper_latency_ms':None}, {'max_daily_loss_usdt':'bad'}):
        assert client.patch('/api/orderbook-recovery/config',json=payload).status_code == 400


def test_null_legacy_mode_repeated_close_remains_paper(client):
    service, config, state = setup_service()
    trade = legacy_position(service,config)
    trade_id = trade.id
    trade.execution_mode = None
    db.session.commit()
    store(book(price=99))
    assert client.get('/api/orderbook-recovery/state').status_code == 200
    assert client.post(f'/api/orderbook-recovery/positions/{trade_id}/close-manual',json={}).status_code == 200
    assert client.post(f'/api/orderbook-recovery/positions/{trade_id}/close-manual',json={}).status_code == 200


def test_start_blocked_by_emergency_does_not_create_session(client):
    service, config, state = setup_service()
    config.emergency_entry_block = True
    config.enabled = False
    db.session.commit()
    response = client.post('/api/orderbook-recovery/start-paper')
    assert response.status_code == 400
    assert response.json['obj']['msg'] == 'emergency_entry_block'
    assert PaperSession.query.count() == 0


@pytest.mark.parametrize('operation', ['start', 'pause', 'session'])
def test_serialization_error_rolls_back_entire_lifecycle_transition(client, monkeypatch, operation):
    service, config, state = setup_service()
    config.enabled = operation == 'pause'
    state.is_stopped = operation != 'pause'
    db.session.commit()
    config_id, old_enabled = config.id, config.enabled
    def unserializable(*args, **kwargs):
        return {'invalid': object()}
    monkeypatch.setattr(OrderBookRecoveryService,'state_payload',unserializable)
    path = {'start':'start-paper','pause':'stop','session':'paper-sessions'}[operation]
    result = client.post('/api/orderbook-recovery/'+path, json={})
    assert result.status_code == 500
    assert result.json['obj']['incident_id']
    current = service.get_or_create_config()
    assert current.enabled is old_enabled
    assert current.paper_session_id is None
    assert PaperSession.query.count() == 0
    assert StrategyRun.query.count() == 0


def test_worker_query_failure_has_sanitized_incident(client, monkeypatch, caplog):
    import asyncio
    from unittest.mock import Mock
    from src.Scanner.ScannerService import ScannerService
    worker = object.__new__(ScannerService)
    worker.order_book_recovery_service = Mock()
    worker.order_book_recovery_service.reconcile_existing_positions.side_effect = RuntimeError('PRIVATE_WORKER_SENTINEL')
    asyncio.run(worker._reconcile_positions())
    assert 'operation=position_worker' in caplog.text
    assert 'incident_id=' in caplog.text
    assert 'PRIVATE_WORKER_SENTINEL' not in caplog.text
