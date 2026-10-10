import json
from datetime import datetime, timedelta
from src import db
from src.OrderBookRecovery.OrderBookRecoveryModel import StrategyRunTrade
from test_execution_safety import setup_service, store, book


def legacy_position(service, config):
    saved = service.config_to_dict(config)
    for key in ('paper_session_id', 'risk_per_trade_percent', 'max_position_margin_usdt', 'max_consecutive_losses'):
        saved.pop(key, None)
    trade = StrategyRunTrade(strategy_config_id=config.id, execution_mode='paper',
        exchange=config.exchange, symbol=config.symbol, side='short', margin=7, leverage=1,
        notional=7, amount=.07, entry_price=100, pnl=0, paper_session_id=None,
        execution_config_json=None, decision_snapshot_json=json.dumps({'config': saved}, default=str),
        paper_close_requested_at=datetime.utcnow()-timedelta(seconds=2),
        paper_close_reason='manual_close', paper_exit_status='pending_fixed_latency',
        opened_at=datetime.utcnow()-timedelta(minutes=5))
    config.enabled = False
    config.emergency_entry_block = True
    db.session.add(trade)
    db.session.commit()
    return trade


def test_legacy_paper_close_paused_restart_and_repeat(client):
    service, config, state = setup_service()
    trade = legacy_position(service, config)
    state.is_stopped, state.stop_reason = True, 'manual_stop'
    db.session.commit()
    store(book(price=99))
    response = client.post(f'/api/orderbook-recovery/positions/{trade.id}/close-manual', json={'reason': 'manual_close'})
    assert response.status_code == 200
    trade = db.session.get(StrategyRunTrade, trade.id)
    assert trade.closed_at is not None
    assert trade.paper_session_id is None
    config = service.get_or_create_config()
    state = service.get_or_create_state(config)
    assert config.enabled is False
    assert state.is_stopped is True
    assert client.post(f'/api/orderbook-recovery/positions/{trade.id}/close-manual', json={}).status_code == 200
