"""Controlled persisted lifecycle test on a fixed separate local PostgreSQL DB."""
import json
import os
from pathlib import Path
import subprocess
import sys
from datetime import datetime, timedelta

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.update(DB_CONNECTION_STRING='postgresql:///arbinator_safety_test_lifecycle_shared?host=/tmp&port=55439',
                  LIVE_TRADING_ENABLED='false', LIVE_TRADING_HARD_DISABLED='true')
import requests
def deny_network(*args, **kwargs):
    raise AssertionError('external_network_forbidden')
requests.sessions.Session.request = deny_network
from src import app, db
from src.OrderBookRecovery.OrderBookRecoveryModel import OrderBookPatternStrategyConfig, StrategyRunTrade
from src.OrderBookRecovery.OrderBookRecoveryService import OrderBookRecoveryService
from src.OrderBookRecovery.OrderBookRecoveryController import (
    OrderBookRecoveryConfigController, OrderBookRecoveryStartController, OrderBookRecoveryStopController,
    OrderBookRecoveryManualCloseController, OrderBookRecoveryPaperSessionController, OrderBookRecoveryStateController,
)
from src.OrderBookRecovery.FuturesSnapshotStore import FuturesSnapshotStore


def bind(config_id):
    service = OrderBookRecoveryService(publisher=type('Silent', (), {'publish': lambda *args: None})())
    service.get_or_create_config = lambda: db.session.get(OrderBookPatternStrategyConfig, config_id)
    for controller in (OrderBookRecoveryConfigController, OrderBookRecoveryStartController,
                       OrderBookRecoveryStopController, OrderBookRecoveryManualCloseController,
                       OrderBookRecoveryPaperSessionController, OrderBookRecoveryStateController):
        controller.service = service
    return service


def worker(stage, config_id, trade_id):
    with app.app_context():
        service = bind(config_id)
        client = app.test_client()
        prefix = '/api/orderbook-recovery'
        close = prefix + f'/positions/{trade_id}/close-manual'
        if stage == 'start':
            assert client.patch(prefix+'/config', json={'emergency_entry_block':False}).status_code == 200
            response = client.post(prefix+'/start-paper', json={})
            assert response.status_code == 200
            assert response.json['obj']['open_position']['id'] == trade_id
            assert client.post(prefix+'/stop',json={}).status_code == 200
        elif stage == 'close':
            assert not FuturesSnapshotStore.all()
            response = client.post(close,json={})
            assert response.status_code == 200 and response.json['obj']['closed_at'] is None
            now = datetime.utcnow()
            FuturesSnapshotStore.update('mexc','VELVET/USDT',{'bids':[[98.99,20]],'asks':[[99.01,20]]},
                {'source_timestamp':now,'market_type':'swap','linear':True,'settle':'USDT'},received_at=now)
            service.reconcile_existing_positions()
            response = client.post(close,json={})
            assert response.status_code == 200 and response.json['obj']['closed_at']
            assert client.post(close,json={}).status_code == 200
            assert client.post(prefix+'/paper-sessions',json={}).status_code == 200
        else:
            assert client.post(close,json={}).status_code == 200
            assert db.session.get(StrategyRunTrade,trade_id).paper_session_id is None
            response = client.get(prefix+'/state')
            assert response.status_code == 200
            assert response.json['obj']['metrics']['total_trades'] == 0
        db.session.remove()


def main():
    with app.app_context():
        assert db.engine.url.database == 'arbinator_safety_test_lifecycle_shared'
        config = OrderBookPatternStrategyConfig(exchange='mexc',symbol='VELVET/USDT',execution_mode='paper',
            enabled=False,emergency_entry_block=True,paper_latency_ms=250,ml_mode='disabled')
        db.session.add(config)
        db.session.commit()
        service = bind(config.id)
        service.get_or_create_state(config)
        saved = service.config_to_dict(config)
        for key in ('paper_session_id','risk_per_trade_percent','max_position_margin_usdt','max_consecutive_losses'):
            saved.pop(key,None)
        trade = StrategyRunTrade(strategy_config_id=config.id,execution_mode='paper',exchange='mexc',symbol='VELVET/USDT',
            side='short',margin=7,leverage=1,notional=7,amount=.07,entry_price=100,pnl=0,
            opened_at=datetime.utcnow()-timedelta(minutes=5),
            decision_snapshot_json=json.dumps({'config':saved},default=str),execution_config_json=None,
            paper_close_requested_at=datetime.utcnow()-timedelta(seconds=2),paper_close_reason='manual_close',
            paper_exit_status='pending_fixed_latency')
        db.session.add(trade)
        db.session.commit()
        config_id, trade_id = config.id, trade.id
        for stage in ('start','close','history'):
            process = subprocess.run([sys.executable,__file__,stage,str(config_id),str(trade_id)],
                                     capture_output=True,text=True,timeout=30)
            if process.returncode:
                raise AssertionError(f'test_stage_failed:{stage}; inspect local test output')
        print(json.dumps({'passed':True,'database':'arbinator_safety_test_lifecycle_shared',
                          'process_stages':['start','close','history'],'synthetic_books':True}))


if __name__ == '__main__':
    if len(sys.argv) == 4:
        worker(sys.argv[1],int(sys.argv[2]),int(sys.argv[3]))
    else:
        main()
