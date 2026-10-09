"""Opt-in process tests, exclusively a dedicated local PostgreSQL test DB.

Run after migrating arbinator_safety_test_pending on /tmp port 55439.
No exchange network or live execution is permitted.
"""
import os
from pathlib import Path
import subprocess
import sys
from datetime import datetime, timedelta

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
URL = "postgresql:///arbinator_safety_test_pending?host=/tmp&port=55439"
os.environ.update(DB_CONNECTION_STRING=URL, LIVE_TRADING_ENABLED="false", LIVE_TRADING_HARD_DISABLED="true")

import requests
requests.sessions.Session.request = lambda *a, **k: (_ for _ in ()).throw(AssertionError("network_forbidden"))
from src import app, db
from src.OrderBookRecovery.OrderBookRecoveryModel import OrderBookPatternStrategyConfig, StrategyRunTrade, ExecutionSlot
from src.OrderBookRecovery.OrderBookRecoveryService import OrderBookRecoveryService
from src.OrderBookRecovery.FuturesSnapshotStore import FuturesSnapshotStore


def service_for(config_id):
    service = OrderBookRecoveryService(publisher=type('Silent', (), {'publish': lambda *a, **k: None})())
    service.get_or_create_config = lambda: db.session.get(OrderBookPatternStrategyConfig, config_id)
    return service


def books(service, config, now):
    for at, price in ((now - timedelta(seconds=1), 99), (now, 100)):
        metadata = dict(source_timestamp=at, market_type='swap', linear=True, settle='USDT')
        raw = {'bids': [[price - .01, 20]], 'asks': [[price + .01, 10]]}
        FuturesSnapshotStore.update('mexc', 'VELVET/USDT', raw, metadata, received_at=at)
        service.exchange_feature(config, service.snapshot_for('mexc', 'VELVET/USDT'), now)


def worker(action, config_id):
    with app.app_context():
        service = service_for(config_id)
        config = service.get_or_create_config()
        state = service.get_or_create_state(config)
        now = datetime.utcnow()
        books(service, config, now)
        if action == 'crash-reserve':
            from sqlalchemy import event
            event.listen(db.session.session_factory.class_, 'after_commit', lambda session: os._exit(71))
            service.open_position(config, state, {'mid_price': 100, 'imbalance': 2, 'short_momentum': .01, 'spread_percent': .02}, 'long', now)
        elif action == 'pause':
            service.stop()
        else:
            trade = StrategyRunTrade.query.filter_by(strategy_config_id=config_id).order_by(StrategyRunTrade.id.desc()).first()
            service.evaluate_open_trade(trade, 100, state, service.trade_config(config, trade), now)
        db.session.remove()


def launch(action, config_id):
    return subprocess.Popen([sys.executable, __file__, action, str(config_id)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def wait(process, expected=0):
    out, err = process.communicate(timeout=30)
    assert process.returncode == expected, (process.returncode, err.decode()[-2000:])


def main():
    with app.app_context():
        # Unique configs preserve earlier test audit rows on repeated runs.
        for index in range(6):
            config = OrderBookPatternStrategyConfig(exchange='mexc', symbol='VELVET/USDT', enabled=True,
                emergency_entry_block=False, execution_mode='paper', consensus_enabled=False, feedback_enabled=False,
                paper_latency_ms=0, pending_entry_ttl_seconds=60)
            db.session.add(config)
            db.session.commit()
            config_id = config.id
            service = service_for(config_id)
            service.get_or_create_state(config)
            wait(launch('crash-reserve', config_id), expected=71)
            db.session.expire_all()
            trade = StrategyRunTrade.query.filter_by(strategy_config_id=config_id).one()
            assert trade.live_status == 'paper_pending' and trade.live_entry_fee is None
            assert db.session.get(ExecutionSlot, config_id).status == 'paper_pending'
            db.session.commit()
            if index == 0:
                wait(launch('pause', config_id))
                wait(launch('fill', config_id))
            else:
                first, second = launch('fill', config_id), launch('pause', config_id)
                wait(first)
                wait(second)
            db.session.expire_all()
            trade = StrategyRunTrade.query.filter_by(strategy_config_id=config_id).one()
            assert not db.session.get(OrderBookPatternStrategyConfig, config_id).enabled
            if trade.live_status == 'paper_cancelled':
                assert trade.live_entry_fee is None
                assert db.session.get(ExecutionSlot, config_id) is None
            else:
                assert index > 0 and trade.live_status is None and trade.live_entry_fee > 0
                assert trade.closed_at is None and db.session.get(ExecutionSlot, config_id).status == 'active'
            db.session.commit()
        print('PASS: 6 crash-after-commit recoveries, pause-before-fill, 5 independent-process pause/fill races; no live/network')


if __name__ == '__main__':
    if len(sys.argv) > 1:
        worker(sys.argv[1], int(sys.argv[2]))
    else:
        main()
