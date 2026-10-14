import importlib.util
from pathlib import Path
import json
import pytest

spec = importlib.util.spec_from_file_location('entry_check', Path(__file__).parents[1] / 'scripts/check_unanimous_entry.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def row(count=4):
    return dict(id=1, side='short', execution_mode='paper', result='loss', opened_at='2026-10-14T10:00:01',
                closed_at='2026-10-14T10:02:01', entry_price=100, exit_price=100, notional=14,
                net_pnl=-.028, gross_pnl=0, total_fee=.028,
                decision_snapshot_json=dict(timestamp='2026-10-14T10:00:00',
                    config=dict(strategy_version='adaptive_book_v2', paper_session_id='one'),
                    consensus_decision=dict(valid_exchanges_count=4, confirming_short_count=count)))


def test_filter_uses_only_pre_entry_counts_not_outcome_or_mfe():
    r = row()
    assert module.keep(r)
    r['net_pnl'] = -100
    r['decision_snapshot_json']['observed_excursions'] = {'mfe': 999}
    assert module.keep(r)
    assert not module.keep(row(3))
    r['decision_snapshot_json']['timestamp'] = '2026-10-14T10:00:02'
    with pytest.raises(ValueError, match='after fill'): module.keep(r)


def test_dedupe_and_development_boundary(tmp_path):
    path = tmp_path / 'rows.json'
    r = row()
    path.write_text(json.dumps([r, r]))
    report = module.audit([path])
    assert report['unique_ids'] == 1 and report['duplicate_ids'] == 1
    assert report['groups'][0]['retained']['count'] == 1
    r['closed_at'] = '2026-10-24T00:00:00'
    path.write_text(json.dumps([r]))
    with pytest.raises(ValueError, match='development'): module.audit([path])
