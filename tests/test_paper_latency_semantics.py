from datetime import datetime, timedelta

import pytest
from src import db
from src.OrderBookRecovery.OrderBookRecoveryModel import StrategyRunTrade
from test_execution_safety import setup_service, book, store


@pytest.mark.parametrize('reason,price', [('manual_close', 100), ('take_profit', 102), ('stop_loss', 98)])
def test_entry_and_exit_wait_for_post_delay_book(client, reason, price):
    service, config, state = setup_service()
    config.paper_latency_ms = 250
    config.paper_taker_fee_percent = .1
    config.take_profit_percent_of_margin = .1
    config.stop_loss_percent_of_margin = .1
    now = datetime.utcnow()
    snapshot = book(at=now)
    store(snapshot)
    service.snapshot_for = lambda *_: snapshot
    payload = service.open_position(config, state, {'mid_price': 100, 'imbalance': 2, 'short_momentum': .01, 'spread_percent': .01}, 'long', now)
    trade = db.session.get(StrategyRunTrade, payload['id'])
    frozen = service.trade_config(config, trade)
    config.paper_latency_ms = 0
    config.paper_taker_fee_percent = 10
    db.session.commit()
    assert frozen.paper_latency_ms == 250
    assert frozen.paper_taker_fee_percent == .1
    service.evaluate_open_trade(trade, 100, state, frozen, now + timedelta(milliseconds=249))
    assert trade.live_status == 'paper_pending'
    service.evaluate_open_trade(trade, 100, state, frozen, now + timedelta(milliseconds=300))
    assert trade.live_status == 'paper_pending'  # Same pre-delay book cannot fill.
    snapshot = book(price=101, at=now + timedelta(milliseconds=300))
    service.evaluate_open_trade(trade, 101, state, frozen, now + timedelta(milliseconds=300))
    assert trade.live_status is None
    assert trade.entry_price == pytest.approx(101.01)
    assert trade.live_entry_fee == pytest.approx(trade.amount * trade.entry_price * .001)
    request_time = now + timedelta(seconds=1)
    snapshot = book(price=price, at=request_time)
    if reason == 'manual_close':
        service.close_trade(trade, price, 0, reason, state, frozen, request_time)
    else:
        service.evaluate_open_trade(trade, price, state, frozen, request_time)
    assert trade.paper_close_reason == reason
    assert trade.closed_at is None
    service.evaluate_open_trade(trade, price, state, frozen, request_time + timedelta(milliseconds=249))
    assert trade.closed_at is None
    service.evaluate_open_trade(trade, price, state, frozen, request_time + timedelta(milliseconds=300))
    assert trade.closed_at is None
    snapshot = book(price=price - .1, at=request_time + timedelta(milliseconds=300))
    service.evaluate_open_trade(trade, price, state, frozen, request_time + timedelta(milliseconds=300))
    assert trade.closed_at is not None
    assert trade.exit_price == pytest.approx(price - .11)
    assert trade.total_fee == pytest.approx(trade.amount * (trade.entry_price + trade.exit_price) * .001)
    assert trade.net_pnl == pytest.approx(trade.gross_pnl - trade.total_fee)


@pytest.mark.parametrize('milliseconds', [1, 500, 2000])
def test_future_book_cannot_fill_even_inside_clock_skew_tolerance(client, milliseconds):
    service, config, _ = setup_service()
    now = datetime.utcnow()
    assert service.paper_fill(book(at=now + timedelta(milliseconds=milliseconds)), 'long', .1, config, now) is None


def test_future_receipt_cannot_fill(client):
    service, config, _ = setup_service()
    now = datetime.utcnow()
    snapshot = book(at=now)
    snapshot['updated_at'] = now + timedelta(milliseconds=1)
    assert service.paper_fill(snapshot, 'long', .1, config, now) is None


def test_gross_tp_can_close_at_net_loss(client):
    service, config, state = setup_service()
    config.paper_taker_fee_percent = .1
    config.take_profit_percent_of_margin = .1
    config.leverage = 1
    now = datetime.utcnow()
    service.exchange_feature(config, book(price=99, at=now - timedelta(seconds=1)), now)
    snapshot = book(at=now)
    store(snapshot)
    service.snapshot_for = lambda *_: snapshot
    payload = service.open_position(config, state, {'mid_price': 100, 'imbalance': 2, 'short_momentum': .01, 'spread_percent': .01}, 'long', now)
    trade = db.session.get(StrategyRunTrade, payload['id'])
    frozen = service.trade_config(config, trade)
    service.evaluate_open_trade(trade, 100, state, frozen, now)
    target = trade.entry_price * 1.00101
    snapshot = book(price=target + .01, at=now + timedelta(seconds=1))
    service.evaluate_open_trade(trade, target, state, frozen, now + timedelta(seconds=1))
    assert trade.reason_close == 'take_profit'
    assert trade.gross_pnl > 0
    assert trade.net_pnl < 0
    assert trade.result == 'loss'
