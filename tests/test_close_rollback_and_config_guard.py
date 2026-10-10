import json
from datetime import datetime
import pytest
from src import db
from src.Exchange.ExchangeModel import Exchange
from src.TradingPair.TradingPairModel import TradingPair
from src.OrderBookRecovery.OrderBookRecoveryModel import StrategyRunTrade, ExecutionSlot
from src.OrderBookRecovery.OrderBookRecoveryService import OrderBookRecoveryService
from src.OrderBookRecovery.OrderBookRecoveryController import OrderBookRecoveryManualCloseController
from test_execution_safety import setup_service, store, book
from test_legacy_paper_close import legacy_position
from test_paper_sessions_pending import reserve


def selection(config):
    exchange = Exchange(title='mexc', enabled=True)
    db.session.add(exchange)
    db.session.flush()
    pair = TradingPair(exchange_id=exchange.id, pair='VELVET/USDT', enabled=True)
    db.session.add(pair)
    db.session.flush()
    config.exchange_id, config.trading_pair_id = exchange.id, pair.id
    db.session.commit()


def test_full_form_unchanged_market_allows_emergency_toggle_without_start(client):
    service, config, state = setup_service()
    selection(config)
    trade = legacy_position(service, config)
    payload = json.loads(json.dumps(service.config_to_dict(config), default=str))
    payload.update(emergency_entry_block=False, enabled=True)
    payload['exchange_id'] = str(payload['exchange_id'])
    result = client.patch('/api/orderbook-recovery/config', json=payload)
    assert result.status_code == 200
    assert result.json['obj']['emergency_entry_block'] is False
    assert result.json['obj']['enabled'] is False
    assert db.session.get(StrategyRunTrade, trade.id).closed_at is None
    assert client.patch('/api/orderbook-recovery/config', json={'execution_mode':'live'}).status_code == 400
    assert client.patch('/api/orderbook-recovery/config', json={'symbol':'BTC/USDT'}).status_code == 400


def test_block_cancels_pending_and_off_does_not_restart(client):
    service, config, state = setup_service()
    trade = reserve(service, config, state, datetime.utcnow())
    result = client.patch('/api/orderbook-recovery/config', json={'emergency_entry_block':True})
    assert result.status_code == 200
    pending = db.session.get(StrategyRunTrade, trade.id)
    assert pending.live_status == 'paper_cancelled'
    assert db.session.get(ExecutionSlot, config.id) is None
    result = client.patch('/api/orderbook-recovery/config', json={'emergency_entry_block':False})
    assert result.json['obj']['enabled'] is False
    current = service.get_or_create_config()
    recovery = service.get_or_create_state(current)
    assert recovery.is_stopped is True
    assert recovery.stop_reason == 'manual_stop'


def test_pending_market_change_stays_blocked(client):
    service, config, state = setup_service()
    reserve(service, config, state, datetime.utcnow())
    assert client.patch('/api/orderbook-recovery/config', json={'symbol':'BTC/USDT'}).status_code == 400


def test_close_failure_rolls_back_fill_but_preserves_intent_and_history(client, monkeypatch, caplog):
    service, config, state = setup_service()
    trade = legacy_position(service, config)
    trade_id = trade.id
    store(book(price=99))
    def fail(*args, **kwargs):
        raise RuntimeError('SECRET_MUST_NOT_APPEAR')
    monkeypatch.setattr(OrderBookRecoveryService, 'apply_recovery_after_close', fail)
    result = client.post(f'/api/orderbook-recovery/positions/{trade.id}/close-manual', json={})
    assert result.status_code == 500
    assert result.json['obj']['incident_id']
    assert 'SECRET_MUST_NOT_APPEAR' not in result.get_data(as_text=True)
    assert 'SECRET_MUST_NOT_APPEAR' not in caplog.text
    assert 'close_trade' in caplog.text
    persisted = db.session.get(StrategyRunTrade, trade_id)
    assert persisted.closed_at is None
    assert persisted.exit_price is None
    assert persisted.pnl == 0
    assert persisted.paper_close_requested_at is not None
    assert StrategyRunTrade.query.count() == 1


def test_committed_paper_close_not_failed_by_notification(client, monkeypatch):
    service, config, state = setup_service()
    trade = legacy_position(service, config)
    store(book(price=99))
    def fail(*args, **kwargs):
        raise RuntimeError('notification_unavailable')
    monkeypatch.setattr(OrderBookRecoveryManualCloseController.service.publisher, 'publish', fail)
    result = client.post(f'/api/orderbook-recovery/positions/{trade.id}/close-manual', json={})
    assert result.status_code == 200
    persisted = db.session.get(StrategyRunTrade, trade.id)
    assert persisted.closed_at is not None
    pnl = persisted.pnl
    assert client.post(f'/api/orderbook-recovery/positions/{trade.id}/close-manual', json={}).status_code == 200
    assert db.session.get(StrategyRunTrade, trade.id).pnl == pnl
