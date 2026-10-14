"""One fixed, retrospective development-only filter. Never imports the trading app."""
import argparse
from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path


def decision(row):
    value = row['decision_snapshot_json']
    return json.loads(value) if isinstance(value, str) else value


def timestamp(value):
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    return parsed.astimezone(timezone.utc).replace(tzinfo=None) if parsed.tzinfo else parsed


def keep(row):
    data = decision(row)
    if timestamp(data['timestamp']) > timestamp(row['opened_at']):
        raise ValueError('decision timestamp after fill')
    consensus = data['consensus_decision']
    valid = consensus['valid_exchanges_count']
    confirmed = consensus[f"confirming_{row['side']}_count"]
    if type(valid) is not int or type(confirmed) is not int or not 0 <= confirmed <= valid or valid < 1:
        raise ValueError('missing/invalid pre-entry counts')
    return confirmed == valid


def metrics(rows):
    cumulative = peak = drawdown = 0
    for row in sorted(rows, key=lambda x: timestamp(x['closed_at'])):
        cumulative += row['net_pnl']
        peak = max(peak, cumulative)
        drawdown = max(drawdown, peak - cumulative)
    return dict(count=len(rows), ids=[r['id'] for r in rows],
                gross=sum(r['gross_pnl'] for r in rows),
                fees=sum(r['total_fee'] for r in rows), net=cumulative,
                expectancy=cumulative / len(rows) if rows else None,
                turnover=sum(r['notional'] * (1 + r['exit_price'] / r['entry_price']) for r in rows),
                closed_trade_drawdown=drawdown)


def audit(paths):
    unique, sources, duplicates = {}, [], 0
    immutable = ('side', 'execution_mode', 'opened_at', 'closed_at', 'result',
                 'entry_price', 'exit_price', 'notional', 'net_pnl', 'total_fee')
    for batch, path in enumerate(paths):
        raw = Path(path).read_bytes()
        sources.append(dict(name=Path(path).name, sha256=hashlib.sha256(raw).hexdigest()))
        for row in json.loads(raw):
            if row['id'] in unique:
                old = unique[row['id']][0]
                if any(old.get(k) != row.get(k) for k in immutable) or decision(old)['config'] != decision(row)['config']:
                    raise ValueError(f"conflicting duplicate {row['id']}")
                duplicates += 1
                continue
            unique[row['id']] = (row, batch)
    groups = defaultdict(list)
    config_manifest = {}
    for row, batch in unique.values():
        if row['result'] not in ('win', 'loss'):
            continue
        if row.get('execution_mode') != 'paper':
            raise ValueError('paper exports only')
        opened = timestamp(row['opened_at'])
        if not datetime(2026, 10, 9) <= opened < datetime(2026, 10, 23) or timestamp(row['closed_at']) >= datetime(2026, 10, 23):
            raise ValueError('outside frozen development window')
        cfg = decision(row)['config']
        # updated_at is bookkeeping, not a changed strategy configuration.
        canonical = {k: v for k, v in cfg.items() if k != 'updated_at'}
        fingerprint = hashlib.sha256(json.dumps(canonical, sort_keys=True).encode()).hexdigest()[:12]
        config_manifest[fingerprint] = {k: cfg.get(k) for k in (
            'strategy_version', 'paper_session_id', 'long_imbalance_threshold',
            'short_imbalance_threshold', 'paper_taker_fee_percent', 'paper_latency_ms',
            'take_profit_percent_of_margin', 'stop_loss_percent_of_margin', 'experiment_settings')}
        groups[(cfg['strategy_version'], cfg['paper_session_id'], fingerprint, batch)].append(row)
    results = []
    for (version, session, fingerprint, batch), rows in groups.items():
        rows.sort(key=lambda r: timestamp(r['opened_at']))
        result = dict(version=version, session=session, config=fingerprint, source_batch=batch,
                      start=rows[0]['opened_at'], end=rows[-1]['closed_at'], baseline=metrics(rows))
        if version == 'adaptive_book_v2':
            result.update(retained=metrics([r for r in rows if keep(r)]),
                          rejected=metrics([r for r in rows if not keep(r)]))
        results.append(result)
    return dict(hypothesis='All valid exchanges confirm selected side at original pre-entry decision',
                scope='Retrospective executed-trade subset; not replay or out-of-sample validation',
                sources=sources, unique_ids=len(unique), duplicate_ids=duplicates,
                config_manifest=config_manifest, groups=sorted(results, key=lambda g: g['start']))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('exports', nargs='+')
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    report = audit(args.exports)
    with open(args.output, 'x') as file:
        json.dump(report, file, indent=2)
