from datetime import datetime, timedelta
from src.OrderBookRecovery.OrderBookNormalizer import OrderBookNormalizer
from src.OrderBookRecovery.DepthExecution import consume_book


def exit_diagnostics(trade, config, snapshot, now, heartbeat=None):
    requested = trade.paper_close_requested_at
    deadline = requested + timedelta(milliseconds=int(config.paper_latency_ms)) if requested else None
    source = (snapshot or {}).get('metadata', {}).get('source_timestamp')
    if isinstance(source, (int, float)):
        source = datetime.utcfromtimestamp(source / 1000 if source > 1e11 else source)
    received = (snapshot or {}).get('updated_at')
    age = max((now - source).total_seconds(), (now - received).total_seconds()) if isinstance(source, datetime) and isinstance(received, datetime) else None
    reason = None
    metadata = (snapshot or {}).get('metadata') or {}
    if not snapshot:
        reason = 'missing_execution_book'
    elif metadata.get('market_type') != 'swap' or metadata.get('linear') is not True or metadata.get('settle') != 'USDT':
        reason = 'incompatible_futures_snapshot'
    elif age is None:
        reason = 'missing_source_timestamp'
    elif source > now or received > now:
        reason = 'future_book_not_available'
    elif age > float(config.max_snapshot_age_seconds):
        reason = 'stale_snapshot'
    else:
        normalized, error = OrderBookNormalizer.normalize(snapshot.get('order_book') or {})
        reason = error
        if not reason and not consume_book(normalized, 'short' if trade.side == 'long' else 'long', float(trade.amount), 0):
            reason = 'insufficient_exit_depth'
    valid_age = age if reason is None else None
    if not reason and deadline and now < deadline:
        reason = 'pending_fixed_latency'
    elif not reason and deadline and source < deadline:
        reason = 'no_new_book_after_latency'
    return {
        'pending_age_seconds': max(0, (now - requested).total_seconds()) if requested else None,
        'latency_deadline': deadline.isoformat() + 'Z' if deadline else None,
        'book_age_seconds': age, 'last_valid_book_age_seconds': valid_age,
        'worker_heartbeat': heartbeat.isoformat() + 'Z' if heartbeat else None,
        'worker_age_seconds': max(0, (now - heartbeat).total_seconds()) if heartbeat else None,
        'worker_scope': 'current_process',
        'exit_block_reason': reason or ('awaiting_management_tick' if requested else 'monitoring'),
    }
