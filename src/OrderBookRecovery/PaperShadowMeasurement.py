"""Price observations only: no hypothetical fills, orders or ledger changes."""
from datetime import datetime
import math


def observe(state, source, now, price, side):
    state = dict(state)
    origin = datetime.fromisoformat(state['event_at'])
    if not isinstance(source, datetime) or source < origin or source > now:
        return state
    if not isinstance(price, (int, float)) or not math.isfinite(price) or price <= 0:
        return state
    elapsed = (source - origin).total_seconds()
    if elapsed > 125 or (state.get('last_source_at') and source.isoformat() <= state['last_source_at']):
        return state
    state.update(last_source_at=source.isoformat(), samples=state.get('samples', 0) + 1,
                 last_reject_reason=None)
    # Reference is the first observed depth quote, never the cancelled reservation price.
    if 'reference_price' not in state:
        if elapsed > 5:
            state['status'] = 'missing_reference_window'
            return state
        state.update(reference_price=price, reference_at=source.isoformat(), status='observing')
    change = (price / state['reference_price'] - 1) * (1 if side == 'long' else -1)
    state.update(mfe_return=max(state.get('mfe_return', change), change),
                 mae_return=min(state.get('mae_return', change), change))
    rows = {str(h): dict((state.get('horizons') or {}).get(str(h), {'status': 'unobserved'}))
            for h in (15, 30, 60, 120)}
    for h, row in rows.items():
        if row['status'] != 'unobserved' or elapsed < int(h):
            continue
        row.update(status='missed_window' if elapsed > int(h) + 5 else 'observed')
        if row['status'] == 'observed':
            row.update(source_at=source.isoformat(), event_age_seconds=elapsed,
                       depth_quote_price=price, signed_return=change)
    state['horizons'] = rows
    return state
