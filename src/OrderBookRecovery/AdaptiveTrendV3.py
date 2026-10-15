"""Fixed paper-only 60s trend gate on unchanged v2. Unvalidated, no fitted parameters."""
from collections import OrderedDict, deque
from datetime import datetime
from src.OrderBookRecovery.AdaptiveBookV2 import AdaptiveBookV2
from src.OrderBookRecovery.OrderBookNormalizer import OrderBookNormalizer

VERSION = 'adaptive_trend_v3'


class AdaptiveTrendV3(AdaptiveBookV2):
    _history = {}
    _renewal = {}
    _trend = OrderedDict()

    @classmethod
    def clear_config(cls, config_id):
        with cls._lock:
            cls.reset(config_id)
            for mapping in (cls._trend, cls._renewal):
                for key in list(mapping):
                    if key[0] == config_id:
                        mapping.pop(key, None)

    @classmethod
    def trend(cls, config, side, snapshot, now):
        evidence = {'lookback_seconds': 60, 'anchor_max_age_seconds': 65,
                    'max_gap_seconds': 5, 'method': 'configured_futures_mid_price_sign',
                    'validated': False}
        def rejected(reason):
            return {**evidence, 'reject_reason': reason}
        if config.execution_mode != 'paper':
            return rejected('experimental_paper_only')
        metadata = (snapshot or {}).get('metadata') or {}
        source = metadata.get('source_timestamp')
        if isinstance(source, (int, float)):
            source = datetime.utcfromtimestamp(source / 1000 if source > 1e11 else source)
        receipt = (snapshot or {}).get('updated_at')
        if (not isinstance(source, datetime) or source > now
                or (now-source).total_seconds() > min(5, config.max_snapshot_age_seconds)
                or (receipt and receipt > now)):
            return rejected('trend60_stale_or_future_book')
        if metadata.get('market_type') not in ('swap', 'future') or metadata.get('linear') is not True or metadata.get('settle') != 'USDT':
            return rejected('trend60_incompatible_market')
        normalized, error = OrderBookNormalizer.normalize(snapshot.get('order_book'))
        if error:
            return rejected('trend60_invalid_book')
        bid, ask = float(normalized['bids'][0]['price']), float(normalized['asks'][0]['price'])
        if bid > ask:
            return rejected('trend60_invalid_book')
        mid = (bid + ask) / 2
        key = (config.id, getattr(config, 'paper_session_id', None), config.exchange, config.symbol)
        with cls._lock:
            history = cls._trend.setdefault(key, deque(maxlen=1024))
            cls._trend.move_to_end(key)
            while len(cls._trend) > 32:
                cls._trend.popitem(last=False)
            if history and source < history[-1][0]:
                return rejected('trend60_out_of_order')
            if history and source == history[-1][0]:
                mid = history[-1][1]
            if history and (source-history[-1][0]).total_seconds() > 5:
                history.clear()
            if not history or source > history[-1][0]:
                history.append((source, mid))
            while history and (source-history[0][0]).total_seconds() > 65:
                history.popleft()
            anchors = [(at, value) for at, value in history if 60 <= (source-at).total_seconds() <= 65]
            if not anchors:
                return rejected('trend60_history_unavailable')
            anchor, price = anchors[-1]
        change = mid / price - 1
        evidence.update(anchor_at=anchor.isoformat(), anchor_mid=price,
                        source_at=source.isoformat(), current_mid=mid,
                        elapsed_seconds=(source-anchor).total_seconds(), return_fraction=change)
        if side not in ('long', 'short') or change * (1 if side == 'long' else -1) <= 0:
            return rejected('trend60_direction_not_confirmed')
        return {**evidence, 'reject_reason': None}

    @classmethod
    def entry(cls, config, side, consensus, snapshot, now, margin, previous=None):
        trend = cls.trend(config, side, snapshot, now)
        selected, report = super().entry(config, side, consensus, snapshot, now, margin, previous)
        report.update(version=VERSION, trend60=trend)
        if selected and trend['reject_reason']:
            return None, {**report, 'reject_reason': trend['reject_reason']}
        return selected, report
