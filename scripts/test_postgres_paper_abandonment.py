"""No-network concurrency/restart regression on a fixed isolated local database."""
import os
import sys
import json
import threading
import subprocess
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
os.environ.update(DB_CONNECTION_STRING='postgresql:///arbinator_safety_test_abandon?host=/tmp&port=55439',
    LIVE_TRADING_ENABLED='false', LIVE_TRADING_HARD_DISABLED='true', AUTO_RUN_MIGRATIONS='false')
import requests
def deny_network(*args,**kwargs):
    raise AssertionError('external_network_forbidden')
requests.sessions.Session.request=deny_network
from src import app, db
from src.OrderBookRecovery.OrderBookRecoveryModel import OrderBookPatternStrategyConfig, StrategyRunTrade, ExecutionSlot
from src.OrderBookRecovery.OrderBookRecoveryService import OrderBookRecoveryService
from src.OrderBookRecovery.OrderBookRecoveryController import OrderBookRecoveryAbandonPaperController


def service_for(config_id):
    service=OrderBookRecoveryService(publisher=type('Silent',(),{'publish':lambda *args:None})())
    service.get_or_create_config=lambda: db.session.get(OrderBookPatternStrategyConfig,config_id)
    return service


def verify_restart(config_id,trade_id):
    with app.app_context():
        service=service_for(config_id)
        service.reconcile_existing_positions()
        trade=db.session.get(StrategyRunTrade,trade_id)
        assert trade.abandoned_at and trade.closed_at is None and trade.exit_price is None and trade.pnl==-123
        assert db.session.get(ExecutionSlot,config_id) is None
        assert service.open_trade(service.get_or_create_config()) is None
        db.session.remove()


def main():
    with app.app_context():
        assert db.engine.url.database=='arbinator_safety_test_abandon'
        config=OrderBookPatternStrategyConfig(exchange='mexc',symbol='VELVET/USDT',execution_mode='paper',
            enabled=False,emergency_entry_block=True,take_profit_percent_of_margin=1.8,stop_loss_percent_of_margin=.9,
            ml_mode='disabled')
        db.session.add(config)
        db.session.flush()
        trade=StrategyRunTrade(strategy_config_id=config.id,execution_mode='paper',exchange='mexc',symbol='VELVET/USDT',
            side='short',entry_price=100,amount=.07,margin=7,leverage=1,notional=7,pnl=-123,
            paper_close_requested_at=datetime.utcnow(),paper_close_reason='manual_close',paper_exit_status='pending_fixed_latency')
        db.session.add(trade)
        db.session.flush()
        config_id,trade_id=config.id,trade.id
        db.session.add(ExecutionSlot(strategy_config_id=config_id,trade_id=trade_id,client_order_id=f'paper_{trade_id}',status='active'))
        db.session.commit()
        db.session.remove()
    service=service_for(config_id)
    OrderBookRecoveryAbandonPaperController.service=service
    selected=threading.Event()
    execute_stale=threading.Event()
    start=threading.Barrier(2)
    def stale_worker():
        with app.app_context():
            trade=db.session.get(StrategyRunTrade,trade_id)
            config=db.session.get(OrderBookPatternStrategyConfig,config_id)
            state=service.get_or_create_state(config)
            db.session.commit()
            # Row is selected before abandonment; refresh must make it terminal.
            selected.set()
            assert execute_stale.wait(10)
            service.evaluate_open_trade(trade,101,state,config,datetime.utcnow())
            service.close_trade(trade,101,123,'manual_close',state,config,datetime.utcnow())
            db.session.commit()
            assert trade.abandoned_at and trade.closed_at is None and trade.exit_price is None
            db.session.remove()
    def request():
        assert selected.wait(10)
        start.wait(10)
        with app.test_client() as client:
            response=client.post(f'/api/orderbook-recovery/positions/{trade_id}/abandon-legacy-paper',
                json={'confirm_abandon':True,'position_id':trade_id})
            assert response.status_code==200
            return response.json['obj']['abandoned_at']
    with ThreadPoolExecutor(max_workers=3) as pool:
        worker=pool.submit(stale_worker)
        first,second=pool.submit(request),pool.submit(request)
        try:
            assert first.result(15)==second.result(15)
        finally:
            execute_stale.set()
        worker.result(15)
    result=subprocess.run([sys.executable,__file__,'verify',str(config_id),str(trade_id)],capture_output=True,text=True,timeout=30)
    assert result.returncode==0, 'restart_verification_failed'
    print(json.dumps({'passed':True,'database':'arbinator_safety_test_abandon',
        'concurrent_abandon_requests':2,'stale_worker_terminal':True,'restart_verified':True,'external_network_disabled':True}))


if __name__=='__main__':
    if len(sys.argv)==4:
        verify_restart(int(sys.argv[2]),int(sys.argv[3]))
    else:
        main()
