"""Public dataset checks. No app, account access, tuning, or trading."""
import argparse
from collections import Counter, defaultdict
import json
import math
from pathlib import Path
import runpy
import statistics
import sqlite3
import tempfile

ROOT = Path(__file__).resolve().parents[1]
lot_size = runpy.run_path(str(ROOT / "src/OrderBookRecovery/PaperContractRules.py"))["executable_amount"]
consume = runpy.run_path(str(ROOT / "src/OrderBookRecovery/DepthExecution.py"))["consume_book"]


class BookRows:
    """Disk-backed event-time ordering shared by validation and replay."""
    def __init__(self, paths):
        self.directory = tempfile.TemporaryDirectory(prefix="arbinator-research-")
        self.connection = sqlite3.connect(str(Path(self.directory.name) / "rows.sqlite"))
        self.connection.execute("CREATE TABLE rows (at INTEGER, sequence INTEGER PRIMARY KEY, data TEXT)")
        sequence = 0
        for filename in paths if isinstance(paths, (list, tuple)) else [paths]:
            with Path(filename).open() as stream:
                for line in stream:
                    if not line.strip():
                        continue
                    row = json.loads(line)
                    if row.get("received_at") is None:
                        continue
                    self.connection.execute("INSERT INTO rows VALUES (?, ?, ?)", (row["received_at"], sequence, json.dumps(row)))
                    sequence += 1
                    if sequence % 5000 == 0:
                        self.connection.commit()
        self.connection.execute("CREATE INDEX chronological ON rows(at, sequence)")
        self.connection.commit()

    def __iter__(self):
        for (raw,) in self.connection.execute("SELECT data FROM rows ORDER BY at, sequence"):
            yield json.loads(raw)

    def close(self):
        self.connection.close()
        self.directory.cleanup()


def check_row(row):
    if row.get("error"):
        return "fetch_error"
    if row.get("market_type") != "swap" or row.get("linear") is not True or row.get("settle") != "USDT":
        return "incompatible_futures_identity"
    if not row.get("resolved_symbol") or not row.get("contract_size") or not row.get("limits") or not row.get("precision") or row.get("precision_mode") != 4:
        return "missing_contract_metadata"
    if row.get("timestamp") is None or row.get("received_at") is None:
        return "missing_timestamp"
    if row["timestamp"] > row["received_at"] + 1000:
        return "future_timestamp"
    try:
        for side in ("bids", "asks"):
            if not row.get(side):
                return "empty_" + side
            if any(not math.isfinite(float(p)) or not math.isfinite(float(q)) or p <= 0 or q <= 0 for p, q in row[side]):
                return "invalid_price_amount"
        if row["bids"][0][0] >= row["asks"][0][0]:
            return "crossed_or_locked_book"
        if row["bids"] != sorted(row["bids"], key=lambda x: x[0], reverse=True) or row["asks"] != sorted(row["asks"], key=lambda x: x[0]):
            return "unsorted_depth"
    except (TypeError, ValueError):
        return "invalid_price_amount"
    return None


def dataset_quality(rows, parameters, pairs, start_ms=None, end_ms=None):
    venue_counts, reasons, previous, latest, pair_counts = defaultdict(Counter), Counter(), {}, {}, defaultdict(Counter)
    first = last = None
    pair_first, pair_last = {}, {}
    max_gap = defaultdict(float)
    for row in rows:
        at = row.get("received_at")
        if at is None or (start_ms is not None and at < start_ms) or (end_ms is not None and at >= end_ms):
            continue
        symbol, venue = row.get("symbol"), row.get("exchange")
        if symbol not in pairs:
            continue
        first = at if first is None else min(first, at)
        last = at if last is None else max(last, at)
        key = f"{venue}:{symbol}"
        counts = venue_counts[key]
        counts["rows"] += 1
        error = check_row(row)
        if error:
            reasons[error] += 1
            counts[error] += 1
            latest.pop((venue, symbol), None)
        else:
            counts["valid_identity_contract_depth"] += 1
            age = (at - row["timestamp"]) / 1000
            if 0 <= age <= parameters["max_snapshot_age_seconds"]:
                counts["fresh"] += 1
                latest[venue, symbol] = row
            else:
                counts["stale"] += 1
            if key in previous:
                if row["timestamp"] < previous[key][1]:
                    counts["source_timestamp_regression"] += 1
                max_gap[key] = max(max_gap[key], (at - previous[key][0]) / 1000)
            previous[key] = (at, row["timestamp"])
            mid = (row["bids"][0][0] + row["asks"][0][0]) / 2
            notional = parameters["base_margin_usdt"] * parameters["leverage"]
            amount, lot_error = lot_size(notional / mid, mid, row, notional, strict=True)
            if lot_error:
                counts[lot_error] += 1
            elif all(consume(row, side, amount) for side in ("long", "short")):
                counts["executable_two_sided_size"] += 1
            if row.get("depth_amount_unit") != "base":
                counts["depth_unit_provenance_missing"] += 1
            elif row.get("first_bid_raw") and row.get("first_ask_raw"):
                matches = all(abs(row[side][0][1] - row[raw][1] * row["contract_size"]) <= max(1e-12, abs(row[side][0][1]) * 1e-10)
                    for side, raw in (("bids", "first_bid_raw"), ("asks", "first_ask_raw")))
                counts["depth_conversion_verified" if matches else "depth_conversion_mismatch"] += 1
            if row.get("estimated_taker_fee") is None:
                counts["public_fee_estimate_missing"] += 1
            if (row.get("limits", {}).get("cost") or {}).get("min") is None:
                counts["explicit_min_notional_not_published"] += 1
        if str(venue).lower() == parameters["exchange"].lower():
            pair_first.setdefault(symbol, at)
            pair_last[symbol] = at
            tally = pair_counts[symbol]
            tally["configured_ticks"] += 1
            tally["configured_fresh_ticks"] += int(not error and 0 <= (at - row["timestamp"]) / 1000 <= parameters["max_snapshot_age_seconds"])
            fresh = sum(1 for (v, s), r in latest.items() if s == symbol and 0 <= at - r["timestamp"] <= parameters["max_snapshot_age_seconds"] * 1000)
            tally["cross_exchange_covered_ticks"] += int(fresh >= parameters["min_valid_exchanges"])
    pair_report = {}
    for symbol in pairs:
        c = pair_counts[symbol]
        n = c["configured_ticks"]
        pair_report[symbol] = dict(c, configured_fresh_fraction=c["configured_fresh_ticks"] / n if n else 0,
            cross_exchange_coverage_fraction=c["cross_exchange_covered_ticks"] / n if n else 0,
            max_configured_gap_seconds=max_gap[f"{parameters['exchange']}:{symbol}"],
            observed_duration_days=(pair_last.get(symbol, 0) - pair_first.get(symbol, 0)) / 86400000)
    return {"pairs": pair_report, "venues": {k: dict(v, max_gap_seconds=max_gap[k]) for k, v in venue_counts.items()},
        "rejections": dict(reasons), "first_received_at": first, "last_received_at": last,
        "duration_seconds": (last - first) / 1000 if first is not None else 0,
        "fee_status": "account_tier_unknown_public_metadata_not_verified_fees"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--funding-input")
    parser.add_argument("--plan", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    plan = json.loads(Path(args.plan).read_text())
    rows = BookRows(args.input)
    report = dataset_quality(rows, plan["parameters"], plan["candidate_pairs"])
    rows.close()
    funding = Counter()
    if args.funding_input:
        with Path(args.funding_input).open() as stream:
            for line in stream:
                row = json.loads(line)
                funding["rows"] += 1
                funding["errors"] += int(bool(row.get("error")))
                funding["settled_rate_without_mark"] += int(not row.get("error") and row.get("mark_price") is None)
    report["funding"] = dict(funding)
    size = Path(args.input).stat().st_size
    report["book_bytes"] = size
    seconds = report["duration_seconds"]
    report["estimated_book_bytes_per_day"] = size / seconds * 86400 if seconds else None
    intervals, last_at, row_count = defaultdict(list), {}, 0
    with Path(args.input).open() as stream:
        for line in stream:
            row = json.loads(line)
            row_count += 1
            key = row.get("exchange"), row.get("symbol")
            at = row.get("received_at")
            if at is None:
                continue
            if key in last_at and 0 < at - last_at[key] <= 30000:
                intervals[key].append((at - last_at[key]) / 1000)
            last_at[key] = at
    rate = sum(1 / statistics.median(values) for values in intervals.values() if values)
    report["estimated_continuous_book_bytes_per_day"] = rate * size / row_count * 86400 if row_count and rate else None
    report["disk_estimate_method"] = "median per venue/pair interval <=30s; resume idle gaps excluded; average observed JSONL row size"
    report["disk_estimate_warning"] = "short_sample_extrapolation_only_allow_3x_plus_replay_index_and_funding"
    Path(args.output).write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
