from datetime import datetime, timedelta
import pytest
from src.OrderBookRecovery.PaperEntryMeasurement import observe


def quote(previous=None, t=15, **overrides):
    opened = datetime(2026, 10, 11)
    values = dict(source=opened + timedelta(seconds=t), now=opened + timedelta(seconds=t),
                  opened_at=opened, gross=.01, entry_fee=.014, exit_fee=.014)
    values.update(overrides)
    return observe(previous, **values)


def test_two_fees_and_no_spread_double_count():
    result = quote()
    row = result['horizons']['15']
    assert row['net_excluding_funding_usdt'] == pytest.approx(-.018)
    assert result['mfe_net_usdt'] == pytest.approx(-.018)  # Not fabricated zero.
    assert result['funding_included'] is False
    assert result['latency_execution'] is False


def test_gaps_never_backfill_or_interpolate_horizons():
    result = quote(t=29)
    assert result['horizons']['15']['status'] == 'missed_window'
    assert result['horizons']['30']['status'] == 'unobserved'
    result = quote(result, t=31)
    assert result['horizons']['30']['observed_age_seconds'] == 31
    assert result['horizons']['60']['status'] == 'unobserved'


def test_duplicates_future_and_out_of_order_do_not_change_measurement():
    result = quote()
    assert quote(result) == result
    assert quote(result, t=14) == result
    assert quote(result, t=30, now=datetime(2026, 10, 11)) == result
    assert quote(result, t=-1) == result


@pytest.mark.parametrize('fee', [None, float('nan'), float('inf'), -1, True])
def test_missing_or_invalid_fees_not_assumed_zero(fee):
    assert quote(entry_fee=fee) == {}


def test_persisted_restart_preserves_first_observation():
    import json
    result = json.loads(json.dumps(quote()))
    result = quote(result, t=18, gross=.05)
    assert result['first_positive_net_age_seconds'] == 18
    assert result['mfe_net_usdt'] == pytest.approx(.022)
    assert result['horizons']['15']['observed_age_seconds'] == 15


@pytest.mark.parametrize('version', ['baseline', 'adaptive_book_v1', 'adaptive_book_v2'])
def test_actual_service_persisted_trade_measurement_and_export(client, version):
    import json
    from src import db
    from src.OrderBookRecovery.FuturesSnapshotStore import FuturesSnapshotStore
    from test_execution_safety import setup_service, book
    from test_paper_sessions_pending import reserve
    service, config, state = setup_service()
    now = datetime.utcnow()
    trade = reserve(service, config, state, now)
    frozen = service.parse_json(trade.execution_config_json)
    frozen.update(strategy_version=version, experiment_settings={'max_hold_seconds': 120},
                  take_profit_percent_of_margin=1.8, stop_loss_percent_of_margin=.9)
    trade.execution_config_json = json.dumps(frozen)
    trade.live_status = None
    trade.live_entry_fee = .007
    db.session.commit()
    service.stop()
    at = now + timedelta(seconds=15)
    snapshot = book(price=100, at=at)
    FuturesSnapshotStore.update('mexc', config.symbol, snapshot['order_book'], snapshot['metadata'], received_at=at)
    service.evaluate(config, current_time=at)
    db.session.refresh(trade)
    decision = json.loads(trade.decision_snapshot_json)
    assert trade.closed_at is None
    assert not config.enabled
    if version == 'baseline':
        assert 'entry_quality_measurement' not in decision
        return
    row = decision['entry_quality_measurement']['horizons']['15']
    assert row['status'] == 'observed'
    assert row['net_excluding_funding_usdt'] == pytest.approx(row['gross_usdt'] - row['roundtrip_fees_usdt'])
    assert service.trade_to_dict(trade)['decision_snapshot_json']
    # Stale book cannot invent a 30-second observation or modify prior coverage.
    service.evaluate(config, current_time=at + timedelta(seconds=15))
    assert json.loads(trade.decision_snapshot_json)['entry_quality_measurement'] == decision['entry_quality_measurement']
