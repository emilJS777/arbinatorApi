"""Predeclared paper candidate, not fitted to trade outcomes. V1 stays unchanged."""
from datetime import datetime
from types import SimpleNamespace
from src.OrderBookRecovery.AdaptiveBookV1 import AdaptiveBookV1, settings

VERSION = "adaptive_book_v2"


def classification(config, side, direction, consensus):
    if not consensus.get("configured_exchange_valid") or consensus.get("valid_exchanges_count", 0) < max(2, config.min_valid_exchanges):
        return "unavailable"
    if direction not in {"long", "short"}:
        return "neutral"
    return "same" if side == direction else "reversal"


class AdaptiveBookV2(AdaptiveBookV1):
    _history = {}
    _renewal = {}

    @classmethod
    def entry(cls, config, side, consensus, snapshot, now, margin, previous=None):
        source = (snapshot or {}).get("metadata", {}).get("source_timestamp")
        if isinstance(source, (int, float)):
            source = datetime.utcfromtimestamp(source / 1000 if source > 1e11 else source)
        options = settings(config.experiment_settings)
        duration = max(6, options["persistence_seconds"])
        report = {"version": VERSION, "minimum_persistence_seconds": duration, "minimum_books": 4}
        if config.execution_mode != "paper":
            return None, {**report, "reject_reason": "experimental_paper_only"}
        if previous:
            key = (config.id, getattr(config, "paper_session_id", None))
            with cls._lock:
                current = cls._renewal.get(key, {})
                if current.get("previous_id") != previous.id:
                    current = {"previous_id": previous.id, "count": 0, "armed": False}
                    cls.reset(config.id)
                valid = isinstance(source, datetime) and previous.closed_at < source <= now and (now - source).total_seconds() <= config.max_snapshot_age_seconds
                condition = classification(config, previous.side, consensus.get("consensus_direction"), consensus)
                if not current["armed"]:
                    if not valid or condition in {"same", "unavailable"}:
                        current.update(count=0, first=None, last=None)
                    elif current.get("last") != source:
                        if current.get("last") and (source <= current['last'] or (source - current['last']).total_seconds() > config.max_snapshot_age_seconds):
                            current.update(count=0, first=None)
                        current['first'] = current.get('first') or source
                        current['last'] = source
                        current['count'] += 1
                        current['armed'] = current['count'] >= 4 and (source - current['first']).total_seconds() >= duration
                    cls._renewal[key] = current
                    cls.reset(config.id)
                    return None, {**report, "reject_reason": "candidate_waiting_signal_renewal", "renewal_armed": current['armed']}
        # Use the same cost/depth estimator, with explicit v2 minimum persistence.
        frozen = SimpleNamespace(**{column.name: getattr(config, column.name) for column in config.__table__.columns}) if hasattr(config, '__table__') else SimpleNamespace(**vars(config))
        frozen.experiment_settings = {**options, "persistence_seconds": duration}
        selected, evidence = super().entry(frozen, side, consensus, snapshot, now, margin)
        evidence.update(report)
        if selected and evidence.get('distinct_books', 0) < 4:
            return None, {**evidence, "reject_reason": "experimental_signal_not_persistent"}
        return selected, evidence


def exit_decision(config, state, side, direction, consensus, source, now, opened_at, pnl, margin):
    state = dict(state or {})
    options = settings(config.experiment_settings)
    category = classification(config, side, direction, consensus)
    state['classification'] = category
    if (now - opened_at).total_seconds() >= options['max_hold_seconds']:
        return 'experimental_max_hold', state
    if category == 'unavailable' or not isinstance(source, datetime) or source > now or (now-source).total_seconds() > config.max_snapshot_age_seconds:
        state.update(count=0, since=None, classification='unavailable')
        return None, state
    stamp = source.timestamp()
    if stamp <= state.get('last', 0):
        return None, state
    if stamp-state.get('last', stamp) > config.max_snapshot_age_seconds:
        state.update(count=0, since=None)
    state['last'] = stamp
    state['peak'] = max(state.get('peak', 0), pnl)
    trail = options['trailing_margin_percent']
    triggered = bool(trail and state['peak'] > 0 and pnl <= state['peak'] - margin * trail / 100)
    kind = 'candidate_trailing' if triggered else 'candidate_confirmed_reversal' if category == 'reversal' else None
    if not kind:
        state.update(count=0, since=None, kind=None)
        return None, state
    if state.get('kind') != kind:
        state.update(count=0, since=None)
    state['kind'] = kind
    state['since'] = state.get('since') or stamp
    state['count'] = state.get('count', 0) + 1
    confirmed = state['count'] >= 4 and stamp-state['since'] >= max(6, options['exit_persistence_seconds'])
    return (kind if confirmed else None), state
