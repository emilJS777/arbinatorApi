"""Retrospective accounting only: exports cannot supply missing pre-entry prices."""
import argparse
import hashlib
import json
from collections import defaultdict
from decimal import Decimal
from pathlib import Path


def snapshot(row):
    value = row.get('decision_snapshot_json') or {}
    return json.loads(value) if isinstance(value, str) else value


def summarize(rows):
    rows = sorted((r for r in rows if r.get('result') in ('win', 'loss') and r.get('closed_at')), key=lambda r: (r['closed_at'], r['id']))
    totals = {k: sum((Decimal(str(r.get(k) or 0)) for r in rows), Decimal(0)) for k in ('gross_pnl', 'total_fee', 'net_pnl')}
    equity = peak = drawdown = turnover = Decimal(0)
    for r in rows:
        equity += Decimal(str(r['net_pnl']))
        peak = max(peak, equity)
        drawdown = max(drawdown, peak-equity)
        turnover += Decimal(str(r['notional'])) * (1+Decimal(str(r['exit_price']))/Decimal(str(r['entry_price'])))
    return {**{k: float(v) for k, v in totals.items()}, 'trades': len(rows), 'wins': sum(r['result'] == 'win' for r in rows),
            'net_expectancy': float(equity/len(rows)) if rows else None, 'closed_trade_drawdown_usdt': float(drawdown),
            'turnover_entry_plus_exit_usdt': float(turnover), 'candidate_retained': None, 'candidate_rejected': None,
            'candidate_result': 'unavailable: no continuous pre-entry 60-second history; not a replay'}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('exports', nargs='+')
    parser.add_argument('--output', required=True)
    parser.add_argument('--config-output', required=True)
    args = parser.parse_args()
    unique, files, duplicates = {}, [], 0
    for name in args.exports:
        path = Path(name)
        rows = json.loads(path.read_text())
        files.append({'file': path.name, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest(), **summarize(rows)})
        for row in rows:
            previous = unique.get(row['id'])
            if previous:
                if any(previous.get(k) != row.get(k) for k in ('opened_at', 'closed_at', 'net_pnl', 'total_fee', 'result')):
                    raise ValueError('conflicting repeated trade id: %s' % row['id'])
                duplicates += 1
            unique[row['id']] = row
    groups = defaultdict(list)
    for row in unique.values():
        config = snapshot(row).get('config', {})
        hashed = {k: v for k, v in config.items() if k not in ('created_at', 'updated_at')}
        digest = hashlib.sha256(json.dumps(hashed, sort_keys=True).encode()).hexdigest()[:12]
        groups[(config.get('strategy_version'), config.get('paper_session_id'), digest)].append(row)
    report = {'status': 'inconclusive', 'duplicate_ids_removed': duplicates, 'unique_ids': len(unique), 'files': files,
              'groups': [{'version': key[0], 'session': key[1], 'config_hash': key[2], **summarize(rows)} for key, rows in groups.items()]}
    latest = max((r for r in unique.values() if r.get('result') in ('win', 'loss')), key=lambda r: r['opened_at'])
    config = snapshot(latest)['config'].copy()
    for key in ('id', 'created_at', 'updated_at', 'exchange_id', 'trading_pair_id', 'paper_session_id'):
        config.pop(key, None)
    config.update(enabled=False, execution_mode='paper', live_kill_switch=True, live_enabled_confirmation=False)
    with open(args.output, 'x') as stream:
        json.dump(report, stream, indent=2)
    with open(args.config_output, 'x') as stream:
        json.dump(config, stream, indent=2)


if __name__ == '__main__':
    main()
