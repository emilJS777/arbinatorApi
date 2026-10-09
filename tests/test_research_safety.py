from datetime import datetime, timedelta
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from src import db
from src.OrderBookRecovery.OrderBookRecoveryModel import StrategyRunTrade, TradeFundingEvent
from src.OrderBookRecovery.PaperContractRules import executable_amount
from src.OrderBookRecovery.PositionGuardian import PositionGuardian
from src.OrderBookRecovery.SignalFunnel import SignalFunnel
from src.OrderBookRecovery.SignalRules import direction_rejections
from test_execution_safety import setup_service, book, store


def load_script(name):
    path = Path(__file__).resolve().parents[1] / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("price,requested,minimum,reason", [(100, .01, 50, "below_contract_min_notional"), (100, .0001, 0, "below_contract_min_amount")])
def test_contract_minimums_never_round_risk_upwards(price, requested, minimum, reason):
    metadata = {"contract_size": .001, "precision": {"amount": 1}, "precision_mode": 4,
                "limits": {"amount": {"min": 1}, "cost": {"min": minimum}}}
    assert executable_amount(requested, price, metadata, strict=True)[1] == reason


def test_executable_contract_quantity_rounds_down():
    metadata = {"contract_size": .001, "precision": {"amount": 1}, "precision_mode": 4, "limits": {"amount": {"min": 1}}}
    amount, error = executable_amount(.0199, 100, metadata, 2, strict=True)
    assert amount == .019 and error is None


def test_missing_contract_data_explicit_not_zero():
    assert executable_amount(.1, 100, {}, strict=True) == (None, "contract_metadata_missing")


def test_funnel_no_threshold_change_and_exact_consensus_reasons(client):
    service, config, state = setup_service()
    config.consensus_enabled = True
    store(book())
    funnel = SignalFunnel()
    service.signal_observer = funnel
    service.evaluate(config)
    assert funnel.counts["evaluation"] == 1
    assert funnel.rejections["consensus_rejected"]["not_enough_valid_exchanges"] == 1
    assert config.min_valid_exchanges == 2
    assert StrategyRunTrade.query.count() == 0


def test_stale_snapshot_cannot_enter_with_consensus_disabled(client):
    service, config, state = setup_service()
    store(book(at=datetime.utcnow() - timedelta(seconds=10)))
    result = service.evaluate(config)
    assert result["reason"] == "stale_snapshot"
    assert StrategyRunTrade.query.count() == 0


def test_future_timestamp_cannot_poison_momentum_history(client):
    service, config, state = setup_service()
    now = datetime.utcnow()
    service.exchange_feature(config, book(at=now - timedelta(seconds=1)), now)
    assert service.exchange_feature(config, book(price=200, at=now + timedelta(hours=1)), now)["reject_reason"] == "future_snapshot_timestamp"
    row = service.exchange_feature(config, book(price=101, at=now), now)
    assert row["momentum"] == pytest.approx(.01)


def test_paper_cannot_fill_unchanged_book_after_latency(client):
    service, config, state = setup_service()
    config.paper_latency_ms = 250
    now = datetime.utcnow()
    store(book(at=now))
    result = service.open_position(config, state, {"mid_price": 100, "imbalance": 2, "short_momentum": .01, "spread_percent": .01}, "long", now)
    trade = db.session.get(StrategyRunTrade, result["id"])
    service.evaluate_open_trade(trade, 100, state, service.trade_config(config, trade), now + timedelta(seconds=1))
    assert trade.live_status == "paper_pending"


@pytest.mark.parametrize("protected,status", [(False, "expired"), (False, "sl_state_2"), (True, "active")])
def test_protection_monitor_persists_state(client, protected, status):
    service, config, state = setup_service()
    store(book())
    result = service.open_position(config, state, {"mid_price": 100, "imbalance": 2, "short_momentum": .01, "spread_percent": .01}, "long", datetime.utcnow())
    trade = db.session.get(StrategyRunTrade, result["id"])
    service.live_execution_service.protection_state = Mock(return_value={"protected": protected, "status": status, "expires_at": datetime.utcnow() + timedelta(hours=1)})
    PositionGuardian(service).protection(config, trade, datetime.utcnow())
    assert trade.tp_sl_protected is protected
    assert trade.protection_status == status
    if not protected:
        assert config.emergency_entry_block


@pytest.mark.parametrize("cashflow", [-.03, .04])
def test_funding_signed_and_idempotent(client, cashflow):
    service, config, state = setup_service()
    store(book())
    result = service.open_position(config, state, {"mid_price": 100, "imbalance": 2, "short_momentum": .01, "spread_percent": .01}, "long", datetime.utcnow() - timedelta(minutes=2))
    trade = db.session.get(StrategyRunTrade, result["id"])
    trade.execution_mode = "live"
    trade.closed_at = datetime.utcnow() - timedelta(minutes=1)
    trade.gross_pnl = 1
    trade.total_fee = .1
    service.live_execution_service.funding_records = Mock(return_value=[{"id": "one", "timestamp": int(trade.opened_at.timestamp() * 1000) + 1000, "amount": cashflow}])
    guardian = PositionGuardian(service)
    guardian.funding(config, trade, datetime.utcnow())
    trade.funding_checked_at = None
    guardian.funding(config, trade, datetime.utcnow())
    assert TradeFundingEvent.query.count() == 1
    assert trade.net_pnl == pytest.approx(.9 + cashflow)
    assert trade.funding_status == "reconciled"


def test_invalid_funding_response_never_becomes_zero_cost(client):
    service, config, state = setup_service()
    store(book())
    result = service.open_position(config, state, {"mid_price": 100, "imbalance": 2, "short_momentum": .01, "spread_percent": .01}, "long", datetime.utcnow())
    trade = db.session.get(StrategyRunTrade, result["id"])
    trade.execution_mode = "live"
    service.live_execution_service.funding_records = Mock(side_effect=ValueError("invalid response"))
    PositionGuardian(service).funding(config, trade, datetime.utcnow())
    assert trade.funding_pnl is None and trade.funding_status == "unavailable"
    assert config.emergency_entry_block


def test_settled_funding_applies_final_loss_state_once(client):
    service, config, state = setup_service()
    store(book())
    opened = datetime.utcnow() - timedelta(minutes=2)
    result = service.open_position(config, state, {"mid_price": 100, "imbalance": 2, "short_momentum": .01, "spread_percent": .01}, "long", opened)
    trade = db.session.get(StrategyRunTrade, result["id"])
    trade.execution_mode = "live"
    trade.closed_at = datetime.utcnow() - timedelta(minutes=1)
    trade.gross_pnl, trade.total_fee = .05, .01
    trade.pnl_source = "verified_fills_funding_pending"
    trade.funding_status = "pending"
    service.live_execution_service.funding_records = Mock(return_value=[{"id": "debit", "timestamp": int(opened.timestamp() * 1000) + 1000, "amount": -.1}])
    guardian = PositionGuardian(service)
    guardian.funding(config, trade, datetime.utcnow())
    assert trade.result == "loss"
    assert state.consecutive_losses == 1
    trade.funding_checked_at = None
    guardian.funding(config, trade, datetime.utcnow())
    assert state.consecutive_losses == 1


def test_resumable_jsonl_repairs_only_interrupted_tail(tmp_path):
    collector = load_script("collect_perpetual_books")
    path = tmp_path / "books.jsonl"
    path.write_bytes(b'{"received_at":1}\n{"broken":')
    collector.recover_tail(path)
    assert path.read_bytes() == b'{"received_at":1}\n'
    path.write_bytes(b'not-json\n{"received_at":1}\n')
    with pytest.raises(ValueError, match="interior_dataset_corruption"):
        collector.recover_tail(path)


def test_disk_replay_sort_is_chronological_and_hash_is_repeatable(tmp_path):
    script = load_script("replay_orderbooks")
    path = tmp_path / "rows.jsonl"
    path.write_text('\n'.join(json.dumps({"received_at": stamp}) for stamp in [3, 1, 2]))
    rows = script.BookRows(path)
    assert [r["received_at"] for r in rows] == [1, 2, 3]
    assert script.protocol_hash(rows) == script.protocol_hash(rows)
    rows.close()


def test_actual_replay_path_executes_lots_depth_fees_and_funding(client):
    script = load_script("replay_orderbooks")
    start = datetime(2026, 1, 1)
    millis = int(start.replace(tzinfo=__import__("datetime").timezone.utc).timestamp() * 1000)
    rows = []
    for index in range(24):
        price = 100 + index * .6
        for exchange in ("mexc", "binance", "bybit"):
            rows.append({"exchange": exchange, "symbol": "BTC/USDT", "received_at": millis + index * 500 + 1,
                "timestamp": millis + index * 500, "market_type": "swap", "linear": True, "settle": "USDT",
                "bids": [[price - .01, 20]], "asks": [[price + .01, 10]], "contract_size": .001,
                "precision": {"amount": 1}, "precision_mode": 4, "limits": {"amount": {"min": 1}}})
    funds = [{"exchange": "mexc", "symbol": "BTC/USDT", "timestamp": millis + 2000, "funding_rate": .001, "mark_price": 102.4}]
    result = script.replay(rows, {"exchange": "mexc"}, "BTC/USDT", start, funding_rows=funds)
    assert result["trades_count"] >= 1
    trade = result["trades"][0]
    assert trade["fee"] > 0
    assert trade["net_pnl"] < trade["gross_pnl"] - trade["fee"]
    assert result["signal_funnel"]["counts"]["paper_filled"] >= 1


def test_replay_without_evaluation_data_is_explicit(client):
    script = load_script("replay_orderbooks")
    result = script.replay([], {"exchange": "mexc"}, "BTC/USDT", datetime(2026, 1, 1))
    assert result["evidence_status"] == "no_evaluation_data"
    assert result["expectancy"] is None


def test_funding_protocol_hash_is_resumable_but_rejects_revisions():
    script = load_script("replay_orderbooks")
    row = {"exchange": "mexc", "symbol": "BTC/USDT", "timestamp": 1000, "funding_rate": .001, "mark_price": None, "received_at": 2000}
    repeat = dict(row, received_at=3000)
    cut = datetime(2026, 1, 1)
    assert script.funding_hash([row], cut) == script.funding_hash([row, repeat], cut)
    with pytest.raises(ValueError, match="conflicting_settled_funding_record"):
        script.funding_hash([row, dict(repeat, funding_rate=.002)], cut)


def test_research_utc_boundary_is_not_local_timezone():
    script = load_script("replay_orderbooks")
    assert script.utc_millis(datetime(1970, 1, 1)) == 0


def test_insufficient_research_evidence_never_selects_winner():
    script = load_script("replay_orderbooks")
    plan = json.loads((Path(__file__).parents[1] / "research/protocols/orderbook-v1.template.json").read_text())
    result = {"trades_count": 0, "open_position_at_end": False, "missing_data": {"settled_funding_input_missing": 1}}
    verdict = script.evidence_verdict(result, {}, plan["data_quality_requirements"], False)
    assert verdict["status"] == "inconclusive"
    assert verdict["winning_pair"] is None
    assert "insufficient_closed_trades" in verdict["reasons"]


def test_dataset_quality_ignores_incompatible_sources():
    quality = load_script("research_quality")
    row = {"exchange": "mexc", "symbol": "BTC/USDT", "timestamp": 1000, "received_at": 1100,
        "market_type": "swap", "linear": True, "settle": "USDT", "resolved_symbol": "BTC/USDT:USDT",
        "contract_size": .001, "precision_mode": 4, "precision": {"amount": 1}, "limits": {"amount": {"min": 1}},
        "bids": [[100, 1]], "asks": [[101, 1]], "depth_amount_unit": "base"}
    config = {"exchange": "mexc", "base_margin_usdt": 10, "leverage": 2, "max_snapshot_age_seconds": 5, "min_valid_exchanges": 2}
    report = quality.dataset_quality([dict(row, exchange="binance", market_type="spot"), row], config, ["BTC/USDT"])
    assert report["rejections"]["incompatible_futures_identity"] == 1
    assert report["pairs"]["BTC/USDT"]["cross_exchange_coverage_fraction"] == 0


def test_collector_graceful_stop_sets_flag_only():
    collector = load_script("collect_perpetual_books")
    collector.request_stop(15, None)
    assert collector.STOP_REQUESTED is True


def test_separate_replay_files_merge_without_sequence_collisions(tmp_path):
    script = load_script("replay_orderbooks")
    dev, evaluation = tmp_path / "dev.jsonl", tmp_path / "evaluation.jsonl"
    dev.write_text('{"received_at":2}\n{"received_at":1}\n')
    evaluation.write_text('{"received_at":4}\n{"received_at":3}\n')
    rows = script.BookRows([dev, evaluation])
    assert [r["received_at"] for r in rows] == [1, 2, 3, 4]
    rows.close()


def test_replay_exposure_and_cost_scenario_are_explicit(client):
    script = load_script("replay_orderbooks")
    start = datetime(2026, 1, 1)
    millis = script.utc_millis(start)
    rows = []
    for index in range(24):
        for venue in ("mexc", "binance", "bybit"):
            price = 100 + index * .6
            rows.append({"exchange": venue, "symbol": "BTC/USDT", "received_at": millis + index * 500 + 1,
                "timestamp": millis + index * 500, "market_type": "swap", "linear": True, "settle": "USDT",
                "resolved_symbol": "BTC/USDT:USDT", "bids": [[price - .01, 20]], "asks": [[price + .01, 10]],
                "contract_size": .001, "precision": {"amount": 1}, "precision_mode": 4, "limits": {"amount": {"min": 1}}})
    result = script.replay(rows, {"exchange": "mexc"}, "BTC/USDT", start, funding_scenario=.3)
    assert result["trades_count"] > 0
    assert result["exposure_seconds"] > 0
    assert 0 <= result["exposure_fraction"] <= 1
    assert result["notional_time_usdt_seconds"] > 0
    assert result["funding_assumption"]["adverse_percent_per_8h_prorated"] == .3
    assert result["funding_assumption"]["exact_account_costs"] is False
    assert result["mark_to_market_max_drawdown_percent"] >= 0


def test_research_plan_requires_paper_and_ordered_untouched_periods():
    script = load_script("replay_orderbooks")
    plan = json.loads((Path(__file__).parents[1] / "research/protocols/orderbook-v1.template.json").read_text())
    script.validate_plan(plan)
    plan["parameters"]["execution_mode"] = "live"
    with pytest.raises(ValueError, match="paper_only"):
        script.validate_plan(plan)
