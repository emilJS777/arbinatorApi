"""Replay recorded books in an isolated in-memory DB; no exchange clients or live submits."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import statistics
import sys
import hashlib
from collections import Counter


def protocol_hash(rows):
    digest = hashlib.sha256()
    for row in rows:
        digest.update((json.dumps(row, sort_keys=True) + "\n").encode())
    return digest.hexdigest()


def funding_hash(rows, cut):
    # Resume re-reads the last settlement. Hash unique economic events, not receipt time.
    events = {}
    for row in rows:
        if row.get("timestamp") is None or datetime.utcfromtimestamp(row["timestamp"] / 1000) >= cut:
            continue
        key = (row["exchange"], row["symbol"], row["timestamp"])
        event = {name: row.get(name) for name in ("exchange", "symbol", "timestamp", "funding_rate", "mark_price")}
        if key in events and events[key] != event:
            raise ValueError("conflicting_settled_funding_record")
        events[key] = event
    return protocol_hash(events[key] for key in sorted(events))

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
os.environ["DB_CONNECTION_STRING"] = "sqlite:///:memory:"
os.environ["LIVE_TRADING_HARD_DISABLED"] = "true"
os.environ["LIVE_TRADING_ENABLED"] = "false"
os.environ["LOG_LEVEL"] = "ERROR"

from src import app, db
from src.OrderBookRecovery.FuturesSnapshotStore import FuturesSnapshotStore
from src.OrderBookRecovery.OrderBookRecoveryModel import StrategyRunTrade
from src.OrderBookRecovery.OrderBookRecoveryService import OrderBookRecoveryService
from src.OrderBookRecovery.SignalFunnel import SignalFunnel
from research_quality import dataset_quality, check_row, BookRows


SOURCE_FILES = ("src/OrderBookRecovery/SignalRules.py", "src/OrderBookRecovery/OrderBookRecoveryService.py",
    "src/OrderBookRecovery/DepthExecution.py", "src/OrderBookRecovery/PaperContractRules.py",
    "src/OrderBookRecovery/OrderBookRecoveryModel.py", "src/OrderBookRecovery/FuturesSnapshotStore.py",
    "src/OrderBookRecovery/OrderBookNormalizer.py", "src/OrderBookRecovery/SignalFeedbackService.py",
    "scripts/replay_orderbooks.py", "scripts/research_quality.py", "scripts/collect_perpetual_books.py")


def source_hashes():
    return {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in SOURCE_FILES}


def utc_millis(value):
    return int(value.replace(tzinfo=timezone.utc).timestamp() * 1000)


def validate_plan(plan):
    dates = [datetime.fromisoformat(plan["chronology"][key]) for key in
        ("development_start_utc", "development_end_utc", "evaluation_start_utc", "evaluation_end_utc")]
    if any(date.tzinfo is not None for date in dates) or not dates[0] < dates[1] < dates[2] < dates[3]:
        raise ValueError("plan_dates_must_be_strictly_ordered_naive_UTC")
    if plan["parameters"].get("execution_mode") != "paper" or plan["parameters"].get("live_kill_switch") is not True:
        raise ValueError("research_plan_must_be_paper_only")
    if not plan["candidate_pairs"] or len(set(plan["candidate_pairs"])) != len(plan["candidate_pairs"]):
        raise ValueError("candidate_pairs_must_be_unique")
    for scenario in plan["cost_scenarios"]:
        if min(scenario["taker_fee_percent_per_side"], scenario["latency_ms"], scenario["adverse_funding_percent_per_8h"]) < 0:
            raise ValueError("negative_cost_assumption")


def evidence_verdict(result, quality, requirements, period_complete):
    reasons = []
    if not period_complete:
        reasons.append("evaluation_period_not_complete")
    if quality.get("observed_duration_days", 0) < requirements["min_observed_evaluation_days"]:
        reasons.append("insufficient_observed_duration")
    if quality.get("configured_fresh_fraction", 0) < requirements["min_configured_fresh_fraction"]:
        reasons.append("configured_fresh_coverage_below_requirement")
    if quality.get("cross_exchange_coverage_fraction", 0) < requirements["min_cross_exchange_coverage_fraction"]:
        reasons.append("cross_exchange_coverage_below_requirement")
    if quality.get("max_configured_gap_seconds", 0) > requirements["max_configured_gap_seconds"]:
        reasons.append("configured_gap_exceeds_requirement")
    if result["trades_count"] < requirements["min_closed_trades_per_pair"]:
        reasons.append("insufficient_closed_trades")
    if result["open_position_at_end"]:
        reasons.append("open_exposure_at_boundary")
    if result["missing_data"]:
        reasons.append("missing_cost_or_contract_inputs")
    return {"status": "inconclusive" if reasons else "eligible_for_statistical_review_not_profitability_claim",
        "reasons": reasons, "winning_pair": None}


class NullPublisher:
    def publish(self, *args, **kwargs):
        pass


class BaselineService(OrderBookRecoveryService):
    """Old snapshot-count/raw-price momentum; identical realistic execution costs."""
    def features(self, config, snapshot, record_history=True):
        features, error = super().features(config, snapshot, record_history=record_history)
        if not features:
            return features, error
        key = f"{snapshot['exchange']}:{snapshot['symbol']}"
        prices = list(self._mid_price_history[key])[-int(config.momentum_window_snapshots):]
        features["short_momentum"] = prices[-1][1] - prices[0][1] if len(prices) > 1 else 0
        return features, error


def replay(rows, config_overrides, symbol, split_time, baseline=False, end_time=None, funding_rows=(), funding_scenario=None):
    with app.app_context():
        if str(db.engine.url) != "sqlite:///:memory:":
            raise RuntimeError("replay_requires_isolated_memory_database")
        # Only this process's in-memory research database is recreated.
        db.drop_all()
        db.create_all()
        FuturesSnapshotStore.clear()
        OrderBookRecoveryService._mid_price_history.clear()
        OrderBookRecoveryService._last_price_timestamp.clear()
        OrderBookRecoveryService._pending_entries.clear()
        service = (BaselineService if baseline else OrderBookRecoveryService)(publisher=NullPublisher())
        service.strict_replay = True
        funnel = SignalFunnel()
        service.signal_observer = funnel
        config = service.get_or_create_config()
        service.apply_config_overrides(config, config_overrides)
        config.symbol = symbol
        config.execution_mode = "paper"
        config.ml_mode = "disabled"
        config.emergency_entry_block = False
        config.enabled = True
        db.session.commit()
        state = service.get_or_create_state(config)
        seen = 0
        missing = Counter()
        if not funding_rows:
            missing["settled_funding_input_missing"] = 1
        funding_seen = set()
        events = sorted((r for r in funding_rows if not r.get("error") and r.get("timestamp") is not None), key=lambda r: r["timestamp"])
        latest_price = {}
        last_at = None
        warmup = 0
        first_evaluation = None
        mtm_peak = config.paper_equity_usdt
        mtm_dd = 0
        last_funding_accrual = None
        marked_closed, closed_pnl = set(), 0.0
        for row in rows:
            if row.get("symbol") != symbol or row.get("error") or row.get("timestamp") is None:
                continue
            at = datetime.utcfromtimestamp(row["received_at"] / 1000)
            if end_time and at >= end_time:
                break
            last_at = at
            metadata = {key: row.get(key) for key in ("market_type", "linear", "settle", "resolved_symbol", "contract_size", "limits", "precision", "precision_mode", "latency_ms")}
            metadata["source_timestamp"] = row["timestamp"]
            FuturesSnapshotStore.update(row["exchange"], symbol, {"bids": row["bids"], "asks": row["asks"]}, metadata, received_at=at)
            snapshot = service.snapshot_for(row["exchange"], symbol)
            if row.get("bids") and row.get("asks"):
                latest_price[row["exchange"]] = (row["timestamp"], (row["bids"][0][0] + row["asks"][0][0]) / 2)
            if at < split_time:
                service.exchange_feature(config, snapshot, at)
                warmup += 1
                continue
            trade = service.open_trade(config)
            if trade and trade.live_status != "paper_pending":
                if funding_scenario is not None:
                    since = max(trade.opened_at, last_funding_accrual or trade.opened_at)
                    seconds = max(0, (at - since).total_seconds())
                    trade.funding_pnl = float(trade.funding_pnl or 0) - trade.notional * funding_scenario / 100 * seconds / 28800
                    trade.funding_status = "assumed_adverse_funding_scenario_not_exchange_cashflow"
                    db.session.commit()
                    last_funding_accrual = at
                for event in events:
                    if funding_scenario is not None:
                        break
                    key = (event["exchange"], event["symbol"], event["timestamp"])
                    if key in funding_seen or event["exchange"] != trade.exchange or event["symbol"] != symbol:
                        continue
                    settled = datetime.utcfromtimestamp(event["timestamp"] / 1000)
                    if not trade.opened_at <= settled <= at:
                        continue
                    mark = event.get("mark_price")
                    if mark is None:
                        missing["funding_settlement_mark_price_missing"] += 1
                        funding_seen.add(key)
                        continue
                    rate = event.get("funding_rate")
                    if rate is None:
                        missing["settled_funding_rate_missing"] += 1
                        continue
                    cashflow = trade.amount * float(mark) * float(rate) * (-1 if trade.side == "long" else 1)
                    trade.funding_pnl = float(trade.funding_pnl or 0) + cashflow
                    trade.funding_status = "public_settlement_simulated"
                    funding_seen.add(key)
                    db.session.commit()
            if row["exchange"].lower() != config.exchange.lower():
                continue
            service.evaluate(config, current_time=at)
            seen += 1
            first_evaluation = first_evaluation or at
            active = service.open_trade(config)
            if trade and trade.closed_at and trade.id not in marked_closed:
                closed_pnl += float(trade.pnl or 0)
                marked_closed.add(trade.id)
            equity_now = config.paper_equity_usdt + closed_pnl
            if active and active.live_status != "paper_pending" and not check_row(row) and 0 <= row["received_at"] - row["timestamp"] <= config.max_snapshot_age_seconds * 1000:
                liquidation = row["bids"][0][0] if active.side == "long" else row["asks"][0][0]
                equity_now += service.calculate_pnl(active.side, active.entry_price, liquidation, active.notional) - float(active.total_fee or 0) - active.amount * liquidation * config.paper_taker_fee_percent / 100 + float(active.funding_pnl or 0)
            mtm_peak = max(mtm_peak, equity_now)
            mtm_dd = max(mtm_dd, (mtm_peak - equity_now) / mtm_peak * 100)
        trades = StrategyRunTrade.query.filter(StrategyRunTrade.closed_at.isnot(None)).order_by(StrategyRunTrade.closed_at).all()
        results = [{"side": t.side, "entry": t.entry_price, "exit": t.exit_price, "gross_pnl": t.gross_pnl,
                    "net_pnl": t.pnl, "fee": t.total_fee, "opened_at": t.opened_at.isoformat(), "closed_at": t.closed_at.isoformat()} for t in trades]
        pnls = [t.pnl for t in trades]
        positive, negative = sum(p for p in pnls if p > 0), abs(sum(p for p in pnls if p < 0))
        equity = peak = config.paper_equity_usdt
        dd = 0
        for pnl in pnls:
            equity += pnl
            peak = max(peak, equity)
            dd = max(dd, (peak - equity) / peak * 100)
        for reason, count in funnel.rejections.get("execution_rejected", {}).items():
            if reason.startswith("contract_"):
                missing[reason] += count
        all_trades = StrategyRunTrade.query.all()
        seconds = max(0, (last_at - first_evaluation).total_seconds()) if last_at and first_evaluation else 0
        filled = [t for t in all_trades if t.live_status not in {"paper_pending", "paper_rejected", "open_failed"} and t.result != "rejected"]
        exposure = sum(max(0, ((t.closed_at or last_at) - max(t.opened_at, split_time)).total_seconds()) for t in filled) if last_at else 0
        if funding_scenario is not None:
            missing.pop("settled_funding_input_missing", None)
        return {"variant": "baseline_raw_price_momentum" if baseline else "timestamp_normalized_momentum",
                "test_evaluations": seen, "trades_count": len(trades), "total_net_pnl": sum(pnls),
                "expectancy": statistics.mean(pnls) if pnls else None, "profit_factor": positive / negative if negative else None,
                "max_drawdown_percent": dd, "open_position_at_end": bool(service.open_trade(config)), "trades": results,
                "mark_to_market_max_drawdown_percent": mtm_dd,
                "exposure_seconds": exposure, "exposure_fraction": exposure / seconds if seconds else 0,
                "notional_time_usdt_seconds": sum(t.notional * max(0, ((t.closed_at or last_at) - max(t.opened_at, split_time)).total_seconds()) for t in filled) if last_at else 0,
                "funding_assumption": {"adverse_percent_per_8h_prorated": funding_scenario, "exact_account_costs": False},
                "warmup_rows": warmup, "signal_funnel": funnel.report(), "missing_data": dict(missing),
                "funding_input_available": bool(funding_rows), "fees_source": "configured_estimate_not_account_verified",
                "evidence_status": "no_evaluation_data" if not seen else "no_closed_trades" if not trades else "research_only_not_statistically_validated",
                "last_test_time": last_at.isoformat() if last_at else None,
                "effective_config": service.config_to_dict(config)}


def chronological_split(rows, fraction=.7):
    times = sorted({r["received_at"] for r in rows if r.get("received_at") and r.get("timestamp") and not r.get("error")})
    if len(times) < 2:
        raise ValueError("insufficient_chronological_orderbook_data")
    return datetime.utcfromtimestamp(times[min(len(times) - 1, max(1, int(len(times) * fraction)))] / 1000)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--evaluation-input", help="Separate untouched evaluation JSONL; development file remains immutable")
    parser.add_argument("--output", required=True)
    parser.add_argument("--symbol", default="VELVET/USDT")
    parser.add_argument("--exchange", default="mexc")
    parser.add_argument("--config")
    parser.add_argument("--protocol")
    parser.add_argument("--prepare-protocol")
    parser.add_argument("--prepare-plan", help="Resolve and freeze a versioned research plan before evaluation")
    parser.add_argument("--research-plan", help="Versioned plan/template; no evaluation-driven configuration changes")
    parser.add_argument("--development-end")
    parser.add_argument("--evaluation-end")
    parser.add_argument("--period", choices=["development", "evaluation"], default="evaluation")
    parser.add_argument("--funding-input")
    parser.add_argument("--evaluation-funding-input")
    parser.add_argument("--all-symbols", action="store_true")
    parser.add_argument("--diagnostic", action="store_true")
    args = parser.parse_args()
    if args.evaluation_input and (not args.protocol or args.period != "evaluation"):
        parser.error("separate evaluation input is allowed only with sealed protocol evaluation")
    rows = BookRows([args.input, args.evaluation_input] if args.evaluation_input else args.input)
    config = json.loads(Path(args.config).read_text()) if args.config else {}
    config["exchange"] = args.exchange
    funds = [json.loads(l) for l in Path(args.funding_input).read_text().splitlines() if l.strip()] if args.funding_input else []
    if args.evaluation_funding_input:
        if not args.evaluation_input:
            parser.error("evaluation funding requires separate evaluation input")
        funds += [json.loads(l) for l in Path(args.evaluation_funding_input).read_text().splitlines() if l.strip()]
    plan = json.loads(Path(args.research_plan).read_text()) if args.research_plan else None
    if plan:
        validate_plan(plan)
        if args.config:
            parser.error("research plan parameters cannot be overridden with --config")
        config = plan["parameters"]
        if plan.get("implementation_sha256") and plan["implementation_sha256"] != source_hashes():
            parser.error("research implementation changed; create a new plan version before evaluation")
    if args.prepare_plan:
        if not plan:
            parser.error("prepare-plan requires --research-plan template")
        if Path(args.prepare_plan).exists():
            parser.error("plan already exists; use a new version, never overwrite")
        if datetime.utcnow() >= datetime.fromisoformat(plan["chronology"]["evaluation_start_utc"]):
            parser.error("cannot freeze this plan after evaluation start")
        plan["parameters"] = replay([], config, args.symbol, datetime.utcnow())["effective_config"]
        plan["implementation_sha256"] = source_hashes()
        plan["frozen_at_utc"] = datetime.utcnow().isoformat()
        Path(args.prepare_plan).write_text(json.dumps(plan, indent=2, default=str))
        print("Versioned plan frozen. No evaluation collected or validated.")
        rows.close()
        sys.exit(0)
    if args.prepare_protocol:
        if Path(args.prepare_protocol).exists():
            parser.error("sealed protocol already exists; never overwrite")
        if plan:
            args.development_end = plan["chronology"]["development_end_utc"]
            args.evaluation_end = plan["chronology"]["evaluation_end_utc"]
            if datetime.utcnow() < datetime.fromisoformat(args.development_end):
                parser.error("development not finished; plan is frozen but data seal must wait")
            if datetime.utcnow() >= datetime.fromisoformat(plan["chronology"]["evaluation_start_utc"]):
                parser.error("data seal must precede evaluation collection")
        if not args.development_end:
            parser.error("development-end required before untouched evaluation starts")
        cut = datetime.fromisoformat(args.development_end)
        if any(datetime.utcfromtimestamp(r["received_at"] / 1000) >= cut for r in rows):
            parser.error("evaluation data already present; protocol must be frozen before collection of evaluation")
        parameters = plan["parameters"] if plan else replay([], config, args.symbol, cut)["effective_config"]
        protocol = {"development_end": cut.isoformat(), "evaluation_end": args.evaluation_end, "parameters": parameters,
            "development_sha256": protocol_hash(rows),
            "development_funding_sha256": funding_hash(funds, cut),
            "implementation_sha256": source_hashes(),
            "research_plan": plan,
            "baseline": "raw_price_momentum_only", "untouched_evaluation_required": True}
        Path(args.prepare_protocol).write_text(json.dumps(protocol, indent=2, default=str))
        print("Protocol frozen; evaluation has not been collected or tested.")
        sys.exit(0)
    if args.protocol:
        protocol = json.loads(Path(args.protocol).read_text())
        if any(hashlib.sha256((ROOT / name).read_bytes()).hexdigest() != digest for name, digest in protocol.get("implementation_sha256", {}).items()):
            parser.error("implementation changed after protocol freeze; evaluation invalid")
        if args.config:
            parser.error("config changes forbidden when using frozen protocol")
        config = protocol["parameters"]
        split = datetime.fromisoformat(protocol["development_end"])
        development = (r for r in rows if datetime.utcfromtimestamp(r["received_at"] / 1000) < split)
        if protocol_hash(development) != protocol["development_sha256"]:
            parser.error("development data changed after protocol freeze")
        if funding_hash(funds, split) != protocol.get("development_funding_sha256"):
            parser.error("development funding inputs changed after protocol freeze")
        end = datetime.fromisoformat(protocol["evaluation_end"]) if protocol.get("evaluation_end") else None
        plan = protocol.get("research_plan")
        if plan and args.period == "evaluation":
            split = datetime.fromisoformat(plan["chronology"]["evaluation_start_utc"])
    elif plan and args.period == "development":
        split = datetime.fromisoformat(plan["chronology"]["development_end_utc"])
        end = split
    elif args.diagnostic:
        split, end = chronological_split(rows), None
    else:
        parser.error("use frozen --protocol or --diagnostic (not an untouched validation)")
    if args.period == "development":
        end = split
        split = datetime.fromisoformat(plan["chronology"]["development_start_utc"]) if plan else datetime.utcfromtimestamp(min(r["received_at"] for r in rows) / 1000 + 10)
    pairs = plan["candidate_pairs"] if plan else ([base + "/USDT" for base in ("BTC", "ETH", "SOL", "XRP", "VELVET")] if args.all_symbols else [args.symbol])
    quality_config = config if plan else replay([], config, args.symbol, split)["effective_config"]
    quality = dataset_quality(rows, quality_config, pairs, utc_millis(split), utc_millis(end) if end else None)
    report = {"chronological_test_start": split.isoformat(), "tuning_performed": False,
              "period": args.period, "untouched_protocol": bool(args.protocol),
              "pairs": {symbol: {"baseline": replay(rows, config, symbol, split, True, end, funds),
                                 "improved": replay(rows, config, symbol, split, False, end, funds)}
                        for symbol in pairs},
              "data_quality": quality,
              "limitations": ["Baseline isolates momentum changes, not a full reconstruction of historical production behavior.",
                  "No candles used. Fill uses observed depth after latency; queue priority and intervening liquidity are unobserved.",
                  "Funding requires settled rates and settlement mark prices; unavailable inputs are not zero costs.",
                  "Diagnostic 70/30 split is exploratory, not evidence of untouched validation.",
                  "Chronological sorting uses a temporary disk-backed SQLite index, not full JSONL in RAM."]}
    if plan:
        report["protocol_version"] = plan["protocol_version"]
        report["pair_selection"] = "none_on_evaluation; all predeclared pairs reported; no winning pair selected"
        report["cost_sensitivity"] = {}
        for symbol in pairs:
            scenarios = {}
            for scenario in plan["cost_scenarios"]:
                settings = dict(config, paper_taker_fee_percent=scenario["taker_fee_percent_per_side"], paper_latency_ms=scenario["latency_ms"])
                scenarios[scenario["name"]] = {variant: replay(rows, settings, symbol, split, baseline, end, funds, scenario["adverse_funding_percent_per_8h"])
                    for variant, baseline in (("baseline", True), ("improved", False))}
            report["cost_sensitivity"][symbol] = scenarios
            last = quality["last_received_at"]
            complete = bool(end and last and last >= utc_millis(end) - plan["data_quality_requirements"]["max_configured_gap_seconds"] * 1000)
            for result in report["pairs"][symbol].values():
                result["evaluation_verdict"] = evidence_verdict(result, quality["pairs"][symbol], plan["data_quality_requirements"], complete)
        report["conclusion"] = "inconclusive"
        report["conclusion_reason"] = "No automatic winner/profitability claim; cost scenarios are assumptions and require statistical review."
    Path(args.output).write_text(json.dumps(report, indent=2, default=str))
    print(json.dumps({"output": args.output, "period": args.period, "pairs": list(report["pairs"]), "untouched_protocol": report["untouched_protocol"]}, indent=2))
    rows.close()
