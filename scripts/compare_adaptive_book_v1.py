"""Small development-only comparison. Never reads an evaluation input or frozen protocol."""
import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path

from replay_orderbooks import replay, source_hashes
from src.OrderBookRecovery.AdaptiveBookV1 import AdaptiveBookV1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--development-input', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--functional-smoke', action='store_true', help='Only pre-development 2026-10-08 smoke, NOT performance evidence')
    args = parser.parse_args()
    path = Path(args.development_input)
    rows = []
    start, end = (datetime(2026, 10, 8), datetime(2026, 10, 9)) if args.functional_smoke else (datetime(2026, 10, 9), datetime(2026, 10, 23))
    with path.open() as stream:
        for line in stream:
            row = json.loads(line)
            received = row.get('received_at')
            if received and not start <= datetime.utcfromtimestamp(received / 1000) < end:
                raise SystemExit('Input dates outside selected development/functional-smoke bounds; evaluation is forbidden')
            rows.append(row)
            if len(rows) > 100000:
                raise SystemExit('Small-budget comparison limited to 100000 rows')
    config = {'exchange': 'mexc', 'symbol': 'VELVET/USDT', 'execution_mode': 'paper',
              'live_kill_switch': True, 'live_enabled_confirmation': False,
              'take_profit_percent_of_margin': 1.8, 'stop_loss_percent_of_margin': .9,
              'base_margin_usdt': 7, 'max_position_margin_usdt': 7, 'leverage': 1,
              'ml_mode': 'disabled'}
    results = []
    for fee, latency in ((.1, 250), (.15, 1000)):
        for version in ('baseline', 'adaptive_book_v1'):
            AdaptiveBookV1._history.clear()
            result = replay(rows, {**config, 'strategy_version': version,
                'paper_taker_fee_percent': fee, 'paper_latency_ms': latency}, 'VELVET/USDT', start,
                baseline=False, end_time=end, funding_scenario=.1)
            result['variant'] = version
            results.append(result)
    report = {'status': 'inconclusive', 'input': path.name,
        'input_sha256': hashlib.sha256(path.read_bytes()).hexdigest(), 'input_rows': len(rows),
        'scope': 'pre-development functional smoke ONLY; not performance evidence' if args.functional_smoke else 'development only; no tuning, no evaluation opened',
        'limitations': ['short sample', 'executed flow unavailable', 'account fees assumed',
                        'funding adverse scenario, not exact cashflows', 'fixed latency; no queue/partial fills'],
        'source_hashes': {**source_hashes(), 'src/OrderBookRecovery/AdaptiveBookV1.py':
            hashlib.sha256((Path(__file__).resolve().parents[1] / 'src/OrderBookRecovery/AdaptiveBookV1.py').read_bytes()).hexdigest()},
        'results': results}
    with Path(args.output).open('x') as output:
        json.dump(report, output, indent=2, default=str)
    print(json.dumps([{k: r[k] for k in ('variant', 'trades_count', 'total_net_pnl', 'test_evaluations', 'missing_data')} for r in results]))


if __name__ == '__main__':
    main()
