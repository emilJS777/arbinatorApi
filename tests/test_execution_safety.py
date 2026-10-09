from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from src import db
from src.OrderBookRecovery.DepthExecution import consume_book
from src.OrderBookRecovery.FuturesSnapshotStore import FuturesSnapshotStore
from src.OrderBookRecovery.LiveExecutionService import LiveExecutionService, LiveExecutionError, SubmissionUnknown
from src.OrderBookRecovery.OrderBookRecoveryModel import ExecutionSlot, StrategyRunTrade
from src.OrderBookRecovery.OrderBookRecoveryService import OrderBookRecoveryService


def book(exchange="mexc", price=100, at=None, imbalance=2):
    at = at or datetime.utcnow()
    return {"exchange": exchange, "symbol": "VELVET/USDT", "updated_at": at,
            "metadata": {"source_timestamp": at, "market_type": "swap", "linear": True, "settle": "USDT"},
            "order_book": {"bids": [[price - .01, imbalance * 10]], "asks": [[price + .01, 10]]}}


def setup_service():
    FuturesSnapshotStore.clear()
    service = OrderBookRecoveryService(publisher=Mock())
    config = service.get_or_create_config()
    config.exchange = "mexc"
    config.symbol = "VELVET/USDT"
    config.emergency_entry_block = False
    config.paper_latency_ms = 0
    config.enabled = True
    config.consensus_enabled = False
    config.feedback_enabled = False
    db.session.commit()
    state = service.get_or_create_state(config)
    return service, config, state


def store(snapshot):
    FuturesSnapshotStore.update(snapshot["exchange"], snapshot["symbol"], snapshot["order_book"], snapshot["metadata"])


def test_fill_uses_depth_and_fees():
    fill = consume_book({"asks": [[101, 1], [102, 2]]}, "long", 2, .1)
    assert fill["price"] == 101.5
    assert fill["fee"] == pytest.approx(.203)
    assert consume_book({"asks": [[101, 1]]}, "long", 2) is None


def test_replay_store_preserves_historical_receipt_time():
    at = datetime(2026, 1, 1)
    FuturesSnapshotStore.update("mexc", "BTC/USDT", {"bids": [], "asks": []}, received_at=at)
    assert FuturesSnapshotStore.all()["mexc"]["BTC/USDT"]["updated_at"] == at


def test_mexc_verified_fill_contracts_and_fee():
    service = LiveExecutionService()
    order = service.verified_mexc_order({"contractSize": .01},
        {"state": 3, "dealVol": 10, "vol": 10, "dealAvgPrice": 100, "orderId": "123", "totalFee": .01, "feeCurrency": "USDT"})
    assert order["filled"] == pytest.approx(.1)
    assert order["average"] == 100
    assert order["fee"]["cost"] == .01


@pytest.mark.parametrize("data", [
    {"state": 2, "dealVol": 0, "dealAvgPrice": 0},
    {"state": 3, "dealVol": 1, "dealAvgPrice": 100, "orderId": "1"},
])
def test_unconfirmed_fills_or_fees_never_invented(data):
    with pytest.raises(SubmissionUnknown):
        LiveExecutionService().verified_mexc_order({"contractSize": 1}, data)


def test_nested_mexc_order_id_is_scalar():
    assert LiveExecutionService().order_id({"data": {"orderId": "123", "ts": 123456}}) == "123"


def test_plan_cancel_matches_documented_body():
    service = LiveExecutionService()
    service.mexc_private_post_request = Mock(return_value={"signed": True})
    service.submit_mexc_private_post = Mock(return_value={"success": True})
    client = SimpleNamespace(id="mexc")
    service.cancel_mexc_plan_order(client, "123", "BTC_USDT")
    service.mexc_private_post_request.assert_called_once_with(
        client, "planorder/cancel", {"orders": [{"symbol": "BTC_USDT", "orderId": "123"}]})


def test_partial_tpsl_preserves_stop_id():
    service = LiveExecutionService()
    service.build_mexc_tpsl_requests = Mock(return_value={"tp_request": {}, "sl_request": {}, "prices": {"tp_price": 101, "sl_price": 99}})
    service.submit_mexc_private_post = Mock(side_effect=[{"data": "sl-1"}, LiveExecutionError("rejected")])
    result = service.create_mexc_tpsl_orders(None, {}, "long", 1, 100, 10, 2, 10, 5)
    assert result["sl_order_id"] == "sl-1"
    assert result["tp_order_id"] is None
    assert result["error"]


def test_close_failure_does_not_cancel_protection(client):
    service, config, state = setup_service()
    store(book())
    trade_id = service.open_position(config, state, {"mid_price": 100, "imbalance": 2, "short_momentum": .01, "spread_percent": .02}, "long", datetime.utcnow())["id"]
    trade = db.session.get(StrategyRunTrade, trade_id)
    trade.execution_mode = "live"
    trade.exchange_tp_order_id = "tp"
    trade.exchange_sl_order_id = "sl"
    live = LiveExecutionService()
    live.client = Mock(return_value=SimpleNamespace(id="mexc"))
    live.market = Mock(return_value={"symbol": "VELVET/USDT:USDT"})
    live.create_futures_order = Mock(side_effect=SubmissionUnknown("timeout"))
    live.cancel_exchange_tpsl_orders = Mock()
    with pytest.raises(SubmissionUnknown):
        live.close_position(config, trade, 100)
    live.cancel_exchange_tpsl_orders.assert_not_called()


def test_unknown_submit_blocks_duplicates_and_reconciles(client):
    service, config, state = setup_service()
    config.execution_mode = "live"
    db.session.commit()
    service.live_execution_service.open_position = Mock(side_effect=SubmissionUnknown("timeout"))
    service.live_execution_service.client = Mock(return_value=SimpleNamespace())
    service.live_execution_service.market = Mock(return_value={"id": "VELVET_USDT"})
    service.live_execution_service.mexc_order_read = Mock(side_effect=SubmissionUnknown("not yet visible"))
    features = {"mid_price": 100, "imbalance": 2, "short_momentum": .01, "spread_percent": .02}
    first = service.open_position(config, state, features, "long", datetime.utcnow())
    assert first["live_status"] == "open_unknown"
    assert db.session.get(ExecutionSlot, config.id)
    store(book())
    service.evaluate(config)
    service.live_execution_service.open_position.assert_called_once()
    service.live_execution_service.mexc_order_read.assert_called_once()


def test_stop_still_manages_open_paper_position(client):
    service, config, state = setup_service()
    store(book())
    trade_id = service.open_position(config, state, {"mid_price": 100, "imbalance": 2, "short_momentum": .01, "spread_percent": .02}, "long", datetime.utcnow())["id"]
    service.evaluate(config)
    service.stop()
    store(book(price=110))
    service.evaluate(config)
    trade = db.session.get(StrategyRunTrade, trade_id)
    assert trade.closed_at is not None
    assert not config.enabled


def test_immutable_tp_sl_and_symbol_after_config_changes(client):
    service, config, state = setup_service()
    store(book())
    trade_id = service.open_position(config, state, {"mid_price": 100, "imbalance": 2, "short_momentum": .01, "spread_percent": .02}, "long", datetime.utcnow())["id"]
    trade = db.session.get(StrategyRunTrade, trade_id)
    old = config.stop_loss_percent_of_margin
    config.stop_loss_percent_of_margin = 90
    config.symbol = "BTC/USDT"
    frozen = service.trade_config(config, trade)
    assert frozen.symbol == "VELVET/USDT"
    assert frozen.stop_loss_percent_of_margin == old


def test_loss_never_increases_margin(client):
    service, config, state = setup_service()
    state.current_margin = service.bounded_margin(config)
    original = state.current_margin
    service.apply_recovery_after_close(state, config, "loss")
    assert state.current_margin <= original
    assert state.current_step == 0


def test_emergency_entry_block(client):
    service, config, state = setup_service()
    config.emergency_entry_block = True
    assert service.risk_rejection(config, state, {"spread_percent": .01}, datetime.utcnow()) == "emergency_entry_block"


def test_momentum_only_new_timestamps_and_percent(client):
    service, config, state = setup_service()
    now = datetime.utcnow()
    first = book(at=now)
    service.features(config, first)
    for _ in range(20):
        service.features(config, first)
    assert len(service._mid_price_history["mexc:VELVET/USDT"]) == 1
    result, _ = service.features(config, book(price=101, at=now + timedelta(seconds=1)))
    assert result["short_momentum"] == pytest.approx(.01)


def test_stale_and_spot_snapshots_excluded(client):
    service, config, state = setup_service()
    now = datetime.utcnow()
    assert service.exchange_feature(config, book(at=now - timedelta(seconds=10)), now)["reject_reason"] == "stale_snapshot"
    spot = book()
    spot["metadata"]["market_type"] = "spot"
    assert service.exchange_feature(config, spot, now)["reject_reason"] == "incompatible_futures_snapshot"


def test_risk_budget_includes_fees(client):
    service, config, state = setup_service()
    config.paper_equity_usdt = 100
    config.risk_per_trade_percent = .25
    config.base_margin_usdt = 100
    config.max_position_margin_usdt = 100
    margin = service.bounded_margin(config)
    assert margin * (.05 + 2 * .001 * 2) <= .25 + 1e-12


def test_durable_slot_prevents_second_open(client):
    service, config, state = setup_service()
    store(book())
    features = {"mid_price": 100, "imbalance": 2, "short_momentum": .01, "spread_percent": .02}
    service.open_position(config, state, features, "long", datetime.utcnow())
    service.open_position(config, state, features, "short", datetime.utcnow())
    assert StrategyRunTrade.query.count() == 1
    assert ExecutionSlot.query.count() == 1


def test_max_leverage_blocked(client):
    service, config, state = setup_service()
    config.leverage = 3
    assert service.risk_rejection(config, state, {"spread_percent": .01}, datetime.utcnow()) == "max_leverage_exceeded"


def test_paper_entry_latency_uses_next_observed_book(client):
    service, config, state = setup_service()
    config.paper_latency_ms = 250
    db.session.commit()
    now = datetime.utcnow()
    store(book(price=100, at=now))
    result = service.open_position(config, state, {"mid_price": 100, "imbalance": 2, "short_momentum": .01, "spread_percent": .02}, "long", now)
    trade = db.session.get(StrategyRunTrade, result["id"])
    frozen = service.trade_config(config, trade)
    service.evaluate_open_trade(trade, 100, state, frozen, now + timedelta(milliseconds=100))
    assert trade.live_status == "paper_pending"
    store(book(price=101, at=now + timedelta(milliseconds=300)))
    service.evaluate_open_trade(trade, 101, state, frozen, now + timedelta(milliseconds=300))
    assert trade.entry_price == pytest.approx(101.01)
    assert trade.live_entry_fee > 0


def test_confirmation_cannot_bypass_rejected_entry_filters(client):
    service, config, state = setup_service()
    config.confirmation_require_same_direction = False
    consensus = {"configured_exchange_valid": True, "consensus_direction": "long", "reject_reason": "fee_filter"}
    assert service.confirmation_reject_reason(config, {"side": "long"}, None, consensus) == "fee_filter"


def test_verified_external_close_uses_real_fees_and_pnl(client):
    service, config, state = setup_service()
    store(book())
    features = {"mid_price": 100, "imbalance": 2, "short_momentum": .01, "spread_percent": .02}
    result = service.open_position(config, state, features, "long", datetime.utcnow())
    trade = db.session.get(StrategyRunTrade, result["id"])
    trade.execution_mode = "live"
    trade.live_entry_fee = .02
    service.live_execution_service.cancel_exchange_tpsl_orders = Mock(return_value={})
    service.finalize_verified_close(trade, {"id": "close-1", "average": 101, "realized_pnl": .1,
        "fee": {"cost": .03}}, "exchange_position_closed_external", state, config, datetime.utcnow())
    assert trade.pnl == pytest.approx(.05)
    assert trade.pnl_source == "exchange_order_details_excluding_funding"
    assert db.session.get(ExecutionSlot, config.id) is None
