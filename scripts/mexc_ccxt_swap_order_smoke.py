#!/usr/bin/env python3
import argparse
import json
import os
import sys
from decimal import Decimal, ROUND_DOWN

import ccxt


def decimal_value(value, default=None):
    if value is None:
        return default
    return Decimal(str(value))


def resolve_symbol(exchange, configured_symbol):
    markets = exchange.load_markets()
    if configured_symbol in markets and markets[configured_symbol].get("swap"):
        return configured_symbol, markets[configured_symbol]

    normalized = configured_symbol.replace(":USDT", "").replace("/", "").replace("-", "").upper()
    for symbol, market in markets.items():
        market_normalized = symbol.replace(":USDT", "").replace("/", "").replace("-", "").upper()
        if (
            market_normalized == normalized
            and market.get("swap")
            and market.get("linear")
            and (market.get("settle") == "USDT" or market.get("settleId") == "USDT")
        ):
            return symbol, market

    raise RuntimeError(f"swap_market_not_found:{configured_symbol}")


def ticker_price(exchange, symbol):
    ticker = exchange.fetch_ticker(symbol)
    for key in ("mark", "last", "close", "bid", "ask"):
        value = ticker.get(key)
        if value:
            return Decimal(str(value)), ticker
    raise RuntimeError(f"ticker_price_not_found:{symbol}")


def contracts_for_notional(market, price, notional_usdt):
    contract_size = decimal_value(market.get("contractSize"), Decimal("1"))
    raw_contracts = Decimal(str(notional_usdt)) / (Decimal(str(price)) * contract_size)
    precision = market.get("precision") or {}
    amount_precision = precision.get("amount")
    if isinstance(amount_precision, int):
        quant = Decimal("1").scaleb(-amount_precision)
    else:
        quant = Decimal("1")
    contracts = raw_contracts.quantize(quant, rounding=ROUND_DOWN)
    limits = market.get("limits") or {}
    amount_limits = limits.get("amount") or {}
    min_amount = decimal_value(amount_limits.get("min"))
    if min_amount is not None and contracts < min_amount:
        contracts = min_amount
    return contracts, {
        "contract_size": str(contract_size),
        "raw_contracts": str(raw_contracts),
        "rounded_contracts": str(contracts),
        "amount_precision": amount_precision,
        "min_amount": str(min_amount) if min_amount is not None else None,
    }


def public_market_summary(market):
    return {
        "id": market.get("id"),
        "symbol": market.get("symbol"),
        "type": market.get("type"),
        "swap": market.get("swap"),
        "future": market.get("future"),
        "linear": market.get("linear"),
        "settle": market.get("settle"),
        "contract": market.get("contract"),
        "contractSize": market.get("contractSize"),
        "precision": market.get("precision"),
        "limits": market.get("limits"),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Isolated MEXC USDT-M swap create_order smoke test through native ccxt only."
    )
    parser.add_argument("--symbol", default="BTC/USDT:USDT")
    parser.add_argument("--side", choices=["long", "short"], default="long")
    parser.add_argument("--notional-usdt", type=Decimal, default=Decimal("20"))
    parser.add_argument("--leverage", type=int, default=1)
    parser.add_argument("--api-key", default=os.getenv("MEXC_API_KEY"))
    parser.add_argument("--api-secret", default=os.getenv("MEXC_API_SECRET"))
    parser.add_argument(
        "--confirm-real-order-test",
        action="store_true",
        help="Actually sends one native ccxt market order. Default is read-only dry-run.",
    )
    args = parser.parse_args()

    if not args.api_key or not args.api_secret:
        print("missing MEXC_API_KEY/MEXC_API_SECRET or --api-key/--api-secret", file=sys.stderr)
        return 2

    exchange = ccxt.mexc({
        "apiKey": args.api_key,
        "secret": args.api_secret,
        "enableRateLimit": True,
        "options": {
            "defaultType": "swap",
            "defaultSubType": "linear",
            "defaultSettle": "USDT",
        },
    })

    symbol, market = resolve_symbol(exchange, args.symbol)
    balance = exchange.fetch_balance({"type": "swap"})
    price, ticker = ticker_price(exchange, symbol)
    amount, amount_details = contracts_for_notional(market, price, args.notional_usdt)
    order_side = "buy" if args.side == "long" else "sell"
    params = {
        "leverage": args.leverage,
        "marginMode": "isolated",
    }

    result = {
        "mode": "real_order" if args.confirm_real_order_test else "dry_run",
        "ccxt_version": ccxt.__version__,
        "exchange_id": exchange.id,
        "requested_symbol": args.symbol,
        "resolved_symbol": symbol,
        "market": public_market_summary(market),
        "price_used": str(price),
        "notional_usdt": str(args.notional_usdt),
        "side": args.side,
        "order_side": order_side,
        "order_type": "market",
        "amount_contracts": str(amount),
        "amount_details": amount_details,
        "params": params,
        "balance_total_usdt": (balance.get("total") or {}).get("USDT"),
        "balance_free_usdt": (balance.get("free") or {}).get("USDT"),
        "ticker_keys": sorted(ticker.keys()),
    }

    if not args.confirm_real_order_test:
        result["next_step"] = "Add --confirm-real-order-test to send exactly one native ccxt market order."
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
        return 0

    order = exchange.create_order(symbol, "market", order_side, float(amount), None, params)
    result["order"] = order
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
