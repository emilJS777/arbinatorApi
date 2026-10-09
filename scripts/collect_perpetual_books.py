"""Public-only USDT perpetual collection. No app import, credentials or order methods."""
import argparse
import json
import math
from pathlib import Path
import statistics
import time
import os
import fcntl
import signal
from collections import deque, Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

import ccxt


BASES = ("BTC", "ETH", "SOL", "XRP", "VELVET")
STOP_REQUESTED = False


def request_stop(signum, frame):
    global STOP_REQUESTED
    STOP_REQUESTED = True


def recover_tail(path):
    """Only an interrupted final JSONL line may be discarded; interior corruption is fatal."""
    if not path.exists():
        return
    with path.open("rb+") as stream:
        valid_end = 0
        while True:
            line = stream.readline()
            if not line:
                break
            try:
                json.loads(line)
            except (ValueError, UnicodeDecodeError):
                if stream.read(1):
                    raise ValueError("interior_dataset_corruption")
                stream.truncate(valid_end)
                return
            valid_end = stream.tell()
        if valid_end:
            stream.seek(valid_end - 1)
            if stream.read(1) != b"\n":
                stream.seek(valid_end)
                stream.write(b"\n")


def collect(output, duration=60, interval=2, exchanges=("mexc", "binance", "bybit"), notional=20, resume=False, funding_interval=300, until_timestamp=None):
    clients, markets, observations = {}, {}, deque(maxlen=5000)
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    lock = output.with_suffix(output.suffix + ".lock").open("a")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    if output.exists() and not resume:
        raise ValueError("dataset_exists_use_resume_or_new_path")
    recover_tail(output)
    funding_path = output.with_suffix(".funding.jsonl")
    recover_tail(funding_path)
    checkpoint_path = output.with_suffix(".checkpoint.json")
    checkpoint = json.loads(checkpoint_path.read_text()) if checkpoint_path.exists() else {"funding_since": {}, "rows": 0}
    for venue in exchanges:
        try:
            client = getattr(ccxt, venue)({"enableRateLimit": True, "timeout": 3000,
                "options": {"defaultType": "swap", "defaultSubType": "linear"}})
            if venue == "mexc":
                # Current documented base. No alternate hosts or restriction bypass.
                client.urls["api"]["contract"]["public"] = "https://api.mexc.com/api/v1/contract"
            clients[venue] = client
            listed = client.load_markets()
            for base in BASES:
                markets[venue, base] = next((m for m in listed.values() if m.get("base") == base
                    and m.get("quote") == "USDT" and m.get("settle") == "USDT"
                    and m.get("swap") is True and m.get("linear") is True and m.get("active") is not False), None)
        except Exception as error:
            observations.append({"exchange": venue, "error": type(error).__name__, "stage": "markets"})
            clients.pop(venue, None)
    funding, next_funding = {}, {}
    counts = Counter()

    def scan_venue(venue):
        result, funding_rows = [], []
        client = clients[venue]
        for base in BASES:
            market = markets.get((venue, base))
            if not market:
                continue
            started = time.monotonic()
            row = {"schema_version": 2, "exchange": venue, "symbol": base + "/USDT", "market_type": "swap",
                   "linear": True, "settle": "USDT", "resolved_symbol": market["symbol"],
                   "limits": market.get("limits"), "precision": market.get("precision"),
                   "precision_mode": client.precisionMode, "estimated_taker_fee": market.get("taker"),
                   "depth_amount_unit": "base", "raw_depth_amount_unit": "contracts",
                   "collector_ccxt_version": ccxt.__version__}
            try:
                book = client.fetch_order_book(market["symbol"], 20)
                size = float(market.get("contractSize") or 1)
                row.update(timestamp=book.get("timestamp"), received_at=int(time.time() * 1000),
                    latency_ms=(time.monotonic() - started) * 1000,
                    bids=[[float(p), float(q) * size] for p, q, *_ in book.get("bids", [])],
                    asks=[[float(p), float(q) * size] for p, q, *_ in book.get("asks", [])], contract_size=size,
                    first_bid_raw=book.get("bids", [None])[0] if book.get("bids") else None,
                    first_ask_raw=book.get("asks", [None])[0] if book.get("asks") else None)
            except Exception as error:
                row.update(received_at=int(time.time() * 1000), error=type(error).__name__)
            key = f"{venue}:{base}"
            if time.monotonic() >= next_funding.get(key, 0):
                next_funding[key] = time.monotonic() + funding_interval
                try:
                    rate = client.fetch_funding_rate(market["symbol"])
                    funding[key] = {k: rate.get(k) for k in ("fundingRate", "fundingTimestamp", "nextFundingTimestamp", "interval")}
                    funding[key]["source"] = "projection_not_settled_cashflow"
                except Exception as error:
                    funding[key] = {"error": type(error).__name__}
                try:
                    since = checkpoint["funding_since"].get(key, int(time.time() * 1000) - 86400000)
                    rates = client.fetch_funding_rate_history(market["symbol"], since, 100)
                    for rate in rates:
                        if rate.get("timestamp") is None:
                            continue
                        funding_rows.append({"exchange": venue, "symbol": base + "/USDT", "timestamp": rate["timestamp"],
                            "funding_rate": rate.get("fundingRate"), "mark_price": rate.get("markPrice"),
                            "received_at": int(time.time() * 1000), "source": "public_settled_rate_history"})
                    if rates:
                        checkpoint["funding_since"][key] = max(r["timestamp"] for r in rates if r.get("timestamp"))
                    if len(rates) >= 100:
                        funding_rows.append({"exchange": venue, "symbol": base + "/USDT", "error": "funding_history_page_limit", "received_at": int(time.time() * 1000)})
                except Exception as error:
                    funding_rows.append({"exchange": venue, "symbol": base + "/USDT", "error": type(error).__name__, "received_at": int(time.time() * 1000)})
            row["funding"] = funding.get(key, {})
            result.append(row)
        return result, funding_rows

    collection_started = time.monotonic()
    deadline = collection_started + duration
    if until_timestamp is not None:
        deadline = min(deadline, time.monotonic() + until_timestamp - time.time())
    with output.open("a", encoding="utf-8") as stream, funding_path.open("a", encoding="utf-8") as fund_stream, ThreadPoolExecutor(max_workers=max(1, len(clients))) as pool:
        while time.monotonic() < deadline and not STOP_REQUESTED:
            for future in as_completed([pool.submit(scan_venue, venue) for venue in clients]):
                rows, funding_rows = future.result()
                for row in rows:
                    if until_timestamp is not None and row.get("received_at", 0) >= until_timestamp * 1000:
                        continue
                    stream.write(json.dumps(row, allow_nan=False) + "\n")
                    counts[row["exchange"]] += 1
                    observations.append(row)
                for row in funding_rows:
                    if until_timestamp is not None and row.get("received_at", 0) >= until_timestamp * 1000:
                        continue
                    fund_stream.write(json.dumps(row, allow_nan=False) + "\n")
            stream.flush()
            fund_stream.flush()
            os.fsync(stream.fileno())
            os.fsync(fund_stream.fileno())
            checkpoint["rows"] += sum(counts.values())
            counts.clear()
            temporary = checkpoint_path.with_suffix(".tmp")
            temporary.write_text(json.dumps(checkpoint))
            temporary.replace(checkpoint_path)
            time.sleep(min(interval, max(0, deadline - time.monotonic())))
    report = compare(observations, notional)
    report["missing_markets"] = [{"exchange": v, "symbol": b + "/USDT"} for (v, b), m in markets.items() if m is None]
    report["summary_scope"] = "last_at_most_5000_rows_of_this_collection_session"
    report["funding_file"] = str(funding_path)
    report["collection_active_seconds"] = time.monotonic() - collection_started
    report["missing_data"] = ["Account-specific fees are estimates, not private fee tiers.", "Historical funding mark price may be unavailable.", "REST snapshots do not observe all book events or queue priority."]
    lock.close()
    output.with_suffix(".comparison.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def compare(rows, notional):
    import runpy
    consume = runpy.run_path(str(Path(__file__).resolve().parents[1] / "src/OrderBookRecovery/DepthExecution.py"))["consume_book"]
    result = []
    for venue, symbol in sorted({(r["exchange"], r.get("symbol", "unknown")) for r in rows}):
        selected = [r for r in rows if r["exchange"] == venue and r.get("symbol", "unknown") == symbol]
        valid = [r for r in selected if not r.get("error") and r.get("timestamp") is not None
                 and 0 <= r["received_at"] - r["timestamp"] <= 5000 and r.get("bids") and r.get("asks")]
        spreads, buy_slip, sell_slip, mids, depths = [], [], [], [], []
        for row in valid:
            bid, ask = row["bids"][0][0], row["asks"][0][0]
            if ask < bid or bid <= 0:
                continue
            mid = (bid + ask) / 2
            mids.append((row["timestamp"], mid))
            spreads.append((ask - bid) / mid * 100)
            buy = consume(row, "long", notional / mid)
            sell = consume(row, "short", notional / mid)
            if buy:
                buy_slip.append((buy["price"] / ask - 1) * 100)
            if sell:
                sell_slip.append((1 - sell["price"] / bid) * 100)
            depths.append(min(sum(p * q for p, q in row["bids"]), sum(p * q for p, q in row["asks"])))
        mids = sorted(dict(mids).items())
        returns = [math.log(b / a) for (_, a), (_, b) in zip(mids, mids[1:])]
        result.append({"exchange": venue, "symbol": symbol, "samples": len(selected), "fresh_samples": len(valid),
            "fresh_fraction": len(valid) / len(selected), "median_spread_percent": statistics.median(spreads) if spreads else None,
            "min_two_sided_depth_usdt": min(depths) if depths else None,
            "buy_fill_fraction": len(buy_slip) / len(valid) if valid else 0,
            "sell_fill_fraction": len(sell_slip) / len(valid) if valid else 0,
            "median_buy_slippage_percent": statistics.median(buy_slip) if buy_slip else None,
            "median_sell_slippage_percent": statistics.median(sell_slip) if sell_slip else None,
            "observed_return_std": statistics.stdev(returns) if len(returns) >= 2 else None,
            "funding": valid[-1].get("funding") if valid else None,
            "contract_limits": valid[-1].get("limits") if valid else None,
            "contract_precision": valid[-1].get("precision") if valid else None})
    coverage = {}
    for base in BASES:
        symbol = base + "/USDT"
        selected = [r for r in rows if r.get("symbol") == symbol and not r.get("error") and r.get("timestamp")]
        counts, dispersions = [], []
        latest = {}
        for row in sorted(selected, key=lambda r: r["received_at"]):
            stamp = row["received_at"]
            latest[row["exchange"]] = row
            latest = {venue: r for venue, r in latest.items() if 0 <= stamp - r["timestamp"] <= 5000 and r.get("bids") and r.get("asks")}
            counts.append(len(latest))
            prices = [(r["bids"][0][0] + r["asks"][0][0]) / 2 for r in latest.values()]
            if len(prices) > 1:
                dispersions.append((max(prices) - min(prices)) / statistics.median(prices) * 100)
        coverage[symbol] = {"median_fresh_exchanges": statistics.median(counts) if counts else 0,
            "minimum_fresh_exchanges": min(counts) if counts else 0,
            "median_price_dispersion_percent": statistics.median(dispersions) if dispersions else None}
    return {"notional_usdt": notional, "results": result, "cross_exchange_quality": coverage, "limitations": [
        "Short sample is liquidity diagnostics, not a profitability test.",
        "Returns are irregularly sampled; observed_return_std is not annualized volatility.",
        "Funding is a point-in-time rate, not a historical funding cost series.",
        "Depth fill is continuous base quantity; exchange lot/min-notional feasibility requires the contract limits and precision.",
        "Missing timestamps, blocked APIs and unavailable markets are not replaced with spot data."]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--interval", type=float, default=2)
    parser.add_argument("--notional", type=float, default=20)
    parser.add_argument("--exchanges", nargs="+", default=["mexc", "binance", "bybit"])
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--funding-interval", type=float, default=300)
    parser.add_argument("--until", help="UTC ISO boundary; stop collection before evaluation/development end")
    args = parser.parse_args()
    until_timestamp = None
    if args.until:
        from datetime import datetime, timezone
        until = datetime.fromisoformat(args.until.replace("Z", "+00:00"))
        if until.tzinfo is None:
            until = until.replace(tzinfo=timezone.utc)
        until_timestamp = until.timestamp()
        args.duration = min(args.duration, until.timestamp() - time.time())
    if args.duration <= 0 or args.interval < 1 or args.notional <= 0 or args.funding_interval < 60:
        parser.error("duration/notional must be positive, interval >= 1 and funding-interval >= 60")
    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    print(json.dumps(collect(args.output, args.duration, args.interval, args.exchanges, args.notional, args.resume, args.funding_interval, until_timestamp), indent=2))
