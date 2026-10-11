"""Bounded descriptive measurements, never an input to trading decisions."""
from datetime import datetime
import math

HORIZONS = (15, 30, 60, 120)
WINDOW_SECONDS = 5


def observe(previous, *, source, now, opened_at, gross, entry_fee, exit_fee):
    """First fresh executable quote in [h, h+5], not a simulated future fill.

    Call only after the existing futures freshness/depth checks. Duplicate and
    out-of-order books cannot create coverage. No interpolation or backfill.
    """
    previous = previous if isinstance(previous, dict) else {}
    if not all(isinstance(x, datetime) for x in (source, now, opened_at)):
        return previous
    if source < opened_at or source > now:
        return previous
    if any(isinstance(x, bool) or not isinstance(x, (int, float)) or not math.isfinite(x)
           for x in (gross, entry_fee, exit_fee)) or min(entry_fee, exit_fee) < 0:
        return previous
    stamp = source.isoformat()
    if previous.get('last_source_time') and stamp <= previous['last_source_time']:
        return previous
    elapsed = (source - opened_at).total_seconds()
    net = gross - entry_fee - exit_fee
    result = {**previous, 'version': 1, 'last_source_time': stamp,
              'samples': previous.get('samples', 0) + 1,
              'basis': 'observed_depth_exit_quotes_after_two_fees_excluding_funding',
              'scope': 'filled_trades_while_open_only_selection_biased',
              'horizon_window_seconds': WINDOW_SECONDS,
              'funding_included': False, 'latency_execution': False,
              'mfe_net_usdt': max(previous.get('mfe_net_usdt', net), net),
              'mae_net_usdt': min(previous.get('mae_net_usdt', net), net)}
    if net > 0 and result.get('first_positive_net_age_seconds') is None:
        result['first_positive_net_age_seconds'] = elapsed
    horizons = {str(h): dict((previous.get('horizons') or {}).get(str(h)) or
                            {'status': 'unobserved'}) for h in HORIZONS}
    for h in HORIZONS:
        row = horizons[str(h)]
        if row['status'] != 'unobserved' or elapsed < h:
            continue
        if elapsed > h + WINDOW_SECONDS:
            row.update(status='missed_window')
        else:
            row.update(status='observed', source_time=stamp, observed_age_seconds=elapsed,
                       gross_usdt=gross, roundtrip_fees_usdt=entry_fee + exit_fee,
                       net_excluding_funding_usdt=net)
    result['horizons'] = horizons
    return result
