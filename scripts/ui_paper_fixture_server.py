"""Isolated UI integration server. Never import this from production routes.

Requires migrated arbinator_safety_test_ui_lifecycle on /tmp:55439.
No real market observations, exchange credentials or network execution.
"""
import os
from pathlib import Path
import sys
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
URL = 'postgresql:///arbinator_safety_test_ui_lifecycle?host=/tmp&port=55439'
os.environ.update(DB_CONNECTION_STRING=URL, LIVE_TRADING_ENABLED='false', LIVE_TRADING_HARD_DISABLED='true',
                  CORS_ALLOWED_ORIGINS='http://127.0.0.1:5186')

import requests
def deny_network(*args, **kwargs):
    raise AssertionError('external_network_forbidden_in_ui_fixture')
requests.sessions.Session.request = deny_network

from flask import request, jsonify
from src import app, db
from src.Exchange.ExchangeModel import Exchange
from src.TradingPair.TradingPairModel import TradingPair
from src.OrderBookRecovery.OrderBookRecoveryModel import StrategyRunTrade
from src.OrderBookRecovery.OrderBookRecoveryService import OrderBookRecoveryService
from src.OrderBookRecovery.FuturesSnapshotStore import FuturesSnapshotStore

service = OrderBookRecoveryService(publisher=type('Silent', (), {'publish': lambda *a, **k: None})())


def seed():
    config = service.get_or_create_config()
    if StrategyRunTrade.query.count() or Exchange.query.count():
        raise RuntimeError('fixture_requires_empty_test_database; no automatic data reset')
    for title in ['Mexc', 'Binance', 'Bybit']:
        exchange = Exchange(title=title, enabled=True)
        db.session.add(exchange)
        db.session.flush()
        pair = TradingPair(exchange_id=exchange.id, pair='BTC/USDT', enabled=True)
        db.session.add(pair)
        db.session.flush()
        if title == 'Mexc':
            config.exchange_id, config.trading_pair_id = exchange.id, pair.id
    # Only disposable test data; working settings and research protocols are untouched.
    config.exchange, config.symbol = 'Mexc', 'BTC/USDT'
    config.execution_mode, config.enabled, config.emergency_entry_block = 'paper', False, True
    config.live_kill_switch, config.live_enabled_confirmation = True, False
    config.paper_latency_ms, config.pending_entry_ttl_seconds = 1500, 30
    config.base_margin_usdt, config.max_position_margin_usdt, config.leverage = 7, 10, 1
    config.paper_taker_fee_percent = .1
    config.take_profit_percent_of_margin, config.stop_loss_percent_of_margin = 10, 5
    config.ml_mode = 'disabled'
    for mode, pnl in [('paper', 50), ('live', 1000)]:
        db.session.add(StrategyRunTrade(strategy_config_id=config.id, execution_mode=mode,
            exchange='Mexc', symbol='BTC/USDT', side='long', margin=7, leverage=1, notional=7,
            amount=.07, entry_price=100, exit_price=101, pnl=pnl, net_pnl=pnl,
            result='win', reason_open='SYNTHETIC_UI_FIXTURE_NOT_MARKET_DATA', closed_at=datetime.utcnow()))
    db.session.commit()
    service.get_or_create_state(config)
    service.stop()


@app.post('/__fixture__/book')
def fixture_book():
    body = request.get_json() or {}
    price = float(body.get('price', 100))
    spread = float(body.get('spread', .01))
    depth = float(body.get('depth', 100))
    now = datetime.utcnow()
    config = service.get_or_create_config()
    for exchange in ['Mexc', 'Binance', 'Bybit']:
        metadata = dict(source_timestamp=now, market_type='swap', linear=True, settle='USDT', contract_size=.001)
        raw = {'bids': [[price - spread / 2, depth * 2]], 'asks': [[price + spread / 2, depth]]}
        FuturesSnapshotStore.update(exchange, 'BTC/USDT', raw, metadata, received_at=now)
    service.evaluate(config, current_time=now)
    return jsonify(success=True, fixture=True, observed_at=now.isoformat(), obj=service.state_payload(config, service.get_or_create_state(config)))


@app.get('/__fixture__/identity')
def fixture_identity():
    return jsonify(fixture=True, live_hard_disabled=os.environ['LIVE_TRADING_HARD_DISABLED'],
                   database='arbinator_safety_test_ui_lifecycle', project=str(ROOT))


if __name__ == '__main__':
    with app.app_context():
        assert db.engine.url.database == 'arbinator_safety_test_ui_lifecycle'
        seed()
    app.run(host='127.0.0.1', port=5587, debug=False, use_reloader=False, threaded=True)
