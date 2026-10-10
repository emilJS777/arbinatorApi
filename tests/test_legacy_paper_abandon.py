from datetime import datetime
import pytest
from src import db
from src.OrderBookRecovery.OrderBookRecoveryModel import StrategyRunTrade, ExecutionSlot, StrategyRun
from src.OrderBookRecovery.OrderBookRecoveryService import OrderBookRecoveryService
from test_execution_safety import setup_service
from test_legacy_paper_close import legacy_position


def blocked_position():
    service, config, state = setup_service()
    config.take_profit_percent_of_margin = 1.8
    config.stop_loss_percent_of_margin = .9
    trade = legacy_position(service, config)
    trade.decision_snapshot_json = None
    trade.pnl = -123  # Preserve old unverified ledger value, never account for it.
    db.session.add(ExecutionSlot(strategy_config_id=config.id, trade_id=trade.id, client_order_id='paper-test-slot', status='active'))
    db.session.commit()
    return service, config, state, trade


def abandon(client, trade_id, **override):
    return client.post(f'/api/orderbook-recovery/positions/{trade_id}/abandon-legacy-paper',
        json={'position_id':trade_id, 'confirm_abandon':True, **override})


def test_abandon_preserves_evidence_cancels_actions_releases_slot_and_is_idempotent(client):
    service, config, state, trade = blocked_position()
    position_id = trade.id
    config_id = config.id
    before = {key: getattr(trade,key) for key in ('entry_price','exit_price','pnl','gross_pnl','net_pnl','total_fee','closed_at','opened_at','decision_snapshot_json','execution_config_json')}
    payload = client.get('/api/orderbook-recovery/state').json['obj']
    assert payload['paper_exit_diagnostics']['abandon_allowed'] is True
    response = abandon(client,position_id)
    assert response.status_code == 200
    assert response.json['obj']['pnl'] is None
    assert response.json['obj']['result'] == 'abandoned'
    stamp = response.json['obj']['abandoned_at']
    db.session.expire_all()
    trade = db.session.get(StrategyRunTrade,position_id)
    assert {key:getattr(trade,key) for key in before} == before
    assert trade.paper_close_requested_at is None and trade.paper_close_reason is None
    assert trade.pending_entry_expires_at is None
    assert db.session.get(ExecutionSlot,config.id) is None
    assert service.open_trade(config) is None and service.open_positions_count(config) == 0
    assert config.take_profit_percent_of_margin == 1.8 and config.stop_loss_percent_of_margin == .9
    assert config.enabled is False and config.emergency_entry_block is True
    assert abandon(client,position_id).json['obj']['abandoned_at'] == stamp
    assert client.post(f'/api/orderbook-recovery/positions/{position_id}/close-manual',json={}).status_code == 200
    restarted = OrderBookRecoveryService(publisher=service.publisher)
    restarted.reconcile_existing_positions()
    trade = db.session.get(StrategyRunTrade,position_id)
    config = restarted.lock_config(config_id)
    state = restarted.get_or_create_state(config)
    # A previously selected worker row must also remain terminal after refresh.
    restarted.evaluate_open_trade(trade, 101, state, config, datetime.utcnow())
    restarted.close_trade(trade,101,100,'manual_close',state,config,datetime.utcnow())
    assert trade.closed_at is None and trade.exit_price is None and trade.pnl == before['pnl']
    history = client.get('/api/orderbook-recovery/trades').json['obj']
    assert next(row for row in history if row['id']==position_id)['accounting_status']=='abandoned_unverified'
    details = client.get(f'/api/orderbook-recovery/trades/{position_id}/decision-details')
    assert details.status_code == 200 and details.json['obj']['summary']['pnl'] is None
    assert service.available_equity(config)==service.paper_initial_equity(config)
    assert service.feedback_service.recent_trades(config)==[]
    metrics = service.calculate_metrics([trade], 100, trade, [trade])
    assert metrics['total_trades']==0 and metrics['total_pnl']==0 and metrics['archived_trades_count']==0


@pytest.mark.parametrize('mode',[None,'live','unknown'])
def test_ambiguous_or_live_mode_rejected(client,mode):
    _,_,_,trade=blocked_position()
    trade_id=trade.id
    trade.execution_mode=mode
    db.session.commit()
    assert abandon(client,trade_id).status_code==409
    assert db.session.get(StrategyRunTrade,trade_id).abandoned_at is None


@pytest.mark.parametrize('field,value',[
    ('live_exchange_order_id','exchange-id'),('live_close_order_id','close-id'),
    ('live_filled_amount',.1),('live_raw_open_response_json','{}'),
    ('exchange_sl_order_id','sl'),('tp_sl_protected',True),('live_status','open_unknown'),
    ('pnl_source','exchange_realized_pnl'),('live_client_order_id','arbi_unknown'),
    ('pnl_source','verified_fills_funding_pending'),('live_exit_fee',0),
    ('execution_config_json','{"execution_mode":"live"}'),
])
def test_any_live_evidence_rejected_without_exchange_calls(client,field,value):
    _,_,_,trade=blocked_position()
    trade_id=trade.id
    setattr(trade,field,value)
    db.session.commit()
    assert abandon(client,trade_id).status_code==409
    assert db.session.get(StrategyRunTrade,trade_id).abandoned_at is None


def test_confirmation_and_pause_are_required(client):
    _,config,_,trade=blocked_position()
    trade_id,config_id=trade.id,config.id
    assert abandon(client,trade_id,confirm_abandon=False).status_code==400
    assert abandon(client,trade_id,position_id=trade_id+1).status_code==400
    config=service_config(config_id)
    config.enabled=True
    db.session.commit()
    assert abandon(client,trade_id).json['obj']['code']=='pause_entries_before_abandon'
    assert db.session.get(StrategyRunTrade,trade_id).abandoned_at is None


def test_complete_execution_evidence_not_abandonable_and_foreign_slot_preserved(client):
    service,config,state=setup_service()
    trade=legacy_position(service,config)
    trade_id,config_id=trade.id,config.id
    assert abandon(client,trade_id).status_code==409
    trade=db.session.get(StrategyRunTrade,trade_id)
    config=service_config(config_id)
    trade.decision_snapshot_json=None
    other=StrategyRunTrade(strategy_config_id=config.id,exchange=config.exchange,symbol=config.symbol,
        execution_mode='paper',side='long',margin=7,leverage=1,notional=7,amount=.07,entry_price=100)
    db.session.add(other)
    db.session.flush()
    db.session.add(ExecutionSlot(strategy_config_id=config.id,trade_id=other.id,client_order_id='unrelated-slot',status='active'))
    db.session.commit()
    other_id=other.id
    assert abandon(client,trade_id).status_code==200
    assert db.session.get(ExecutionSlot,config_id).trade_id==other_id


def test_serialization_failure_rolls_back_abandonment_and_slot_release(client,monkeypatch):
    service,config,_,trade=blocked_position()
    trade_id,config_id=trade.id,config.id
    def fail(*args): raise RuntimeError('serialization failure')
    monkeypatch.setattr(OrderBookRecoveryService,'trade_to_dict',fail)
    response=abandon(client,trade_id)
    assert response.status_code==500 and response.json['obj']['incident_id']
    db.session.expire_all()
    assert db.session.get(StrategyRunTrade,trade_id).abandoned_at is None
    assert db.session.get(ExecutionSlot,config_id) is not None


def service_config(config_id):
    from src.OrderBookRecovery.OrderBookRecoveryModel import OrderBookPatternStrategyConfig
    return db.session.get(OrderBookPatternStrategyConfig,config_id)


def test_unknown_slot_prevents_abandonment(client):
    _,config,_,trade=blocked_position()
    trade_id,config_id=trade.id,config.id
    db.session.get(ExecutionSlot,config_id).status='open_unknown'
    db.session.commit()
    response=abandon(client,trade_id)
    assert response.status_code==409
    assert response.json['obj']['code']=='abandon_rejected_unknown_execution_slot'
    assert db.session.get(StrategyRunTrade,trade_id).abandoned_at is None
