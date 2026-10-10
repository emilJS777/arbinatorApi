"""Paper-only heuristic overlay. No expected-return or confidence prediction."""
from datetime import datetime
import math
from threading import RLock

from src.OrderBookRecovery.DepthExecution import consume_book
from src.OrderBookRecovery.OrderBookNormalizer import OrderBookNormalizer

VERSION = "adaptive_book_v1"
DEFAULTS = {"persistence_seconds": 3.0, "cost_hurdle": 2.0,
            "funding_reserve_bps": 5.0, "exit_persistence_seconds": 3.0,
            "max_hold_seconds": 120.0, "trailing_margin_percent": 0.0}


def settings(value):
    if value is None:
        value = {}
    if not isinstance(value, dict) or set(value) - set(DEFAULTS):
        raise ValueError("invalid_experiment_settings")
    result = {**DEFAULTS, **value}
    bounds = {"persistence_seconds": (1, 60), "cost_hurdle": (1, 20),
              "funding_reserve_bps": (0, 100), "exit_persistence_seconds": (1, 60),
              "max_hold_seconds": (5, 3600), "trailing_margin_percent": (0, 100)}
    for key, (low, high) in bounds.items():
        number = result[key]
        if isinstance(number, bool) or not isinstance(number, (int, float)) or not math.isfinite(number) or not low <= number <= high:
            raise ValueError("invalid_experiment_" + key)
    return result


def costs(book, side, amount, fee_percent, margin, tp_percent, funding_bps):
    normalized, error = OrderBookNormalizer.normalize(book)
    if error:
        return {"reject_reason": error}
    entry = consume_book(normalized, side, amount, fee_percent)
    exit_fill = consume_book(normalized, "short" if side == "long" else "long", amount, fee_percent)
    if not entry or not exit_fill:
        return {"reject_reason": "experimental_insufficient_depth"}
    # Both VWAPs already contain spread AND depth slippage. Never add spread again.
    crossing = abs(entry["price"] - exit_fill["price"]) * amount
    fees = entry["fee"] + exit_fill["fee"]
    reserve = entry["cost"] * funding_bps / 10000
    target = margin * tp_percent / 100
    return {"roundtrip_fees_usdt": fees, "spread_depth_usdt": crossing,
            "funding_reserve_usdt": reserve, "estimated_cost_usdt": fees + crossing + reserve,
            "gross_tp_usdt": target, "net_target_after_cost_budget_usdt": target - fees - crossing - reserve,
            "funding_source": "heuristic_reserve_not_settled_funding",
            "method": "same_book_roundtrip_VWAP_heuristic_not_expected_return",
            "trade_flow": "unavailable_not_used"}


class AdaptiveBookV1:
    _history = {}
    _lock = RLock()

    @classmethod
    def reset(cls, config_id):
        with cls._lock:
            cls._history.pop(config_id, None)

    @classmethod
    def entry(cls, config, side, consensus, snapshot, now, margin):
        options = settings(getattr(config, "experiment_settings", None))
        report = {"version": VERSION, "trade_flow": "unavailable_not_used"}
        if config.execution_mode != "paper":
            return None, {**report, "reject_reason": "experimental_paper_only"}
        source = (snapshot or {}).get("metadata", {}).get("source_timestamp")
        if isinstance(source, (int, float)):
            source = datetime.utcfromtimestamp(source / 1000 if source > 1e11 else source)
        reason = None
        if not isinstance(source, datetime) or source > now or (now - source).total_seconds() > config.max_snapshot_age_seconds:
            reason = "experimental_stale_book"
        elif not config.consensus_enabled or not consensus.get("configured_exchange_valid") or consensus.get("valid_exchanges_count", 0) < max(2, config.min_valid_exchanges):
            reason = "experimental_cross_exchange_required"
        elif not side or consensus.get("consensus_direction") != side:
            reason = consensus.get("reject_reason") or "experimental_contradictory_signal"
        elif not consensus.get("configured_exchange_" + side + "_signal"):
            reason = "experimental_configured_contradiction"
        elif consensus.get("average_momentum", 0) * (1 if side == "long" else -1) <= 0:
            reason = "experimental_weak_momentum"
        if reason:
            cls.reset(config.id)
            return None, {**report, "reject_reason": reason}
        fingerprint = (config.exchange, config.symbol, getattr(config, "paper_session_id", None), tuple(sorted(options.items())))
        with cls._lock:
            previous = cls._history.get(config.id)
            if not previous or previous["key"] != fingerprint or previous["side"] != side or source < previous["last"] or (source - previous["last"]).total_seconds() > config.max_snapshot_age_seconds:
                previous = {"key": fingerprint, "side": side, "first": source, "last": source, "count": 1}
            elif source > previous["last"]:
                previous = {**previous, "last": source, "count": previous["count"] + 1}
            cls._history[config.id] = previous
            duration = (source - previous["first"]).total_seconds()
        report.update(persistence_seconds=duration, distinct_books=previous["count"])
        normalized, error = OrderBookNormalizer.normalize(snapshot.get("order_book"))
        if error:
            return None, {**report, "reject_reason": error}
        price = float(normalized["asks" if side == "long" else "bids"][0]["price"])
        report.update(costs(snapshot["order_book"], side, margin * config.leverage / price,
                            config.paper_taker_fee_percent, margin, config.take_profit_percent_of_margin,
                            options["funding_reserve_bps"]))
        reason = report.get("reject_reason")
        if not reason and (duration < options["persistence_seconds"] or previous["count"] < 3):
            reason = "experimental_signal_not_persistent"
        if not reason and report["gross_tp_usdt"] < report["estimated_cost_usdt"] * options["cost_hurdle"]:
            reason = "experimental_cost_hurdle"
        report["reject_reason"] = reason
        return (None if reason else side), report


def exit_decision(options, state, side, consensus_side, source_time, now, opened_at, pnl, margin):
    """Persist state in trade decision JSON. Duplicate/out-of-order books cannot confirm an exit."""
    options = settings(options)
    state = dict(state or {})
    if (now - opened_at).total_seconds() >= options["max_hold_seconds"]:
        return "experimental_max_hold", state
    if not isinstance(source_time, datetime) or source_time > now:
        return None, state
    stamp = source_time.timestamp()
    if stamp <= state.get("last", 0):
        return None, state
    if stamp - state.get("last", stamp) > options["exit_persistence_seconds"] + 1:
        state.pop("weak_since", None)
        state["weak_count"] = 0
    state["last"] = stamp
    state["peak_pnl"] = max(state.get("peak_pnl", 0), pnl)
    trailing = options["trailing_margin_percent"]
    weakness = consensus_side != side
    if trailing and state["peak_pnl"] > 0 and pnl <= state["peak_pnl"] - margin * trailing / 100:
        weakness = True
        reason = "experimental_trailing"
    else:
        reason = "experimental_signal_deterioration"
    if not weakness:
        state.pop("weak_since", None)
        state["weak_count"] = 0
        return None, state
    state.setdefault("weak_since", stamp)
    state["weak_count"] = state.get("weak_count", 0) + 1
    confirmed = stamp - state["weak_since"] >= options["exit_persistence_seconds"] and state["weak_count"] >= 3
    return (reason if confirmed else None), state
