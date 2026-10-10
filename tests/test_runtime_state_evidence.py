from src.OrderBookRecovery.OrderBookRecoveryService import OrderBookRecoveryService
from src.OrderBookRecovery.FuturesSnapshotStore import FuturesSnapshotStore
from src.Arbitrage.OrderBookSnapshotStore import OrderBookSnapshotStore
from test_execution_safety import setup_service, book, store
from test_legacy_paper_close import legacy_position


def test_state_contract_and_process_evidence(client, monkeypatch):
    monkeypatch.setenv('BUILD_REVISION', 'test-build')
    setup_service()
    response = client.get('/api/orderbook-recovery/state')
    assert response.status_code == 200
    payload = response.json['obj']
    assert payload['config']['execution_mode'] == 'paper'
    assert 'open_position' in payload and 'paper_exit_diagnostics' in payload
    evidence = payload['runtime_diagnostics']
    assert evidence['build_revision'] == 'test-build'
    assert evidence['process_id'] > 0
    assert evidence['snapshot_scope'] == 'current_process'
    assert evidence['state_contract_version'] == 'paper-lifecycle-state-v2'


def test_state_failure_has_incident_not_partial_success(client, monkeypatch, caplog):
    setup_service()
    def fail(*args):
        raise RuntimeError('DO_NOT_EXPOSE_PRIVATE_PAYLOAD')
    monkeypatch.setattr(OrderBookRecoveryService, 'state_payload', fail)
    response = client.get('/api/orderbook-recovery/state')
    assert response.status_code == 500
    assert response.json['success'] is False
    assert response.json['obj']['incident_id'] in caplog.text
    assert 'DO_NOT_EXPOSE_PRIVATE_PAYLOAD' not in caplog.text + response.text


def test_spot_connectivity_does_not_prove_execution_futures_book(client):
    service, config, state = setup_service()
    trade = legacy_position(service, config)
    FuturesSnapshotStore.clear()
    OrderBookSnapshotStore.update(trade.exchange, trade.symbol, book()['order_book'])
    payload = client.get('/api/orderbook-recovery/state').json['obj']
    assert payload['paper_exit_diagnostics']['exit_block_reason'] == 'missing_execution_book'
    store(book())
    payload = client.get('/api/orderbook-recovery/state').json['obj']
    diagnostics = payload['paper_exit_diagnostics']
    assert diagnostics['execution_exchange'] == trade.exchange
    assert diagnostics['execution_symbol'] == trade.symbol
    assert diagnostics['book_market_type'] == 'swap'
    assert diagnostics['book_source_at'] and diagnostics['book_received_at']
