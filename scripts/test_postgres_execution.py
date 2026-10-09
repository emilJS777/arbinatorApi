"""Real PostgreSQL/process/HTTP path tests. Exchange is ONLY a loopback emulator."""
import argparse
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import sys
from threading import Lock, Thread
from urllib.parse import urlsplit, parse_qs

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def database_guard(url):
    from sqlalchemy.engine import make_url
    parsed = make_url(url)
    if not parsed.drivername.startswith("postgresql") or not (parsed.database or "").startswith("arbinator_safety_test") or parsed.query.get("host") != "/tmp" or str(parsed.query.get("port")) != "55439":
        raise RuntimeError("requires_dedicated_local_test_cluster")


def execution_service(base):
    import ccxt
    import requests
    from src.OrderBookRecovery.LiveExecutionService import LiveExecutionService
    market = {"id": "BTC_USDT", "symbol": "BTC/USDT:USDT", "base": "BTC", "quote": "USDT", "settle": "USDT",
              "swap": True, "linear": True, "contract": True, "type": "swap", "active": True, "contractSize": .001}

    class LoopbackTransport:
        def __init__(self):
            self.session = requests.Session()
            self.session.trust_env = False

        def request(self, method, url, **kwargs):
            parsed = urlsplit(url)
            if parsed.hostname not in {"api.mexc.com", "contract.mexc.com"}:
                raise RuntimeError("non_emulated_network_forbidden")
            if method == "POST" and "order/create" in parsed.path and os.getenv("CRASH_BEFORE_SUBMIT") == "1":
                os._exit(72)
            response = self.session.request(method, base + parsed.path + ("?" + parsed.query if parsed.query else ""), **kwargs)
            if method == "POST" and "order/create" in parsed.path and os.getenv("CRASH_AFTER_ACCEPT") == "1":
                os._exit(71)
            if method == "POST" and "planorder/place/v2" in parsed.path and os.getenv("CRASH_AFTER_PLAN") == "1":
                os._exit(73)
            return response

        def get(self, url, **kwargs):
            return self.request("GET", url, **kwargs)

    transport = LoopbackTransport()

    class LocalMexc(ccxt.mexc):
        def load_markets(self, *args, **kwargs):
            return {market["symbol"]: market}

        def fetch_positions(self, *args, **kwargs):
            return live.private_read(self, "position/open_positions", {})

    client = LocalMexc({"apiKey": "emulator-key", "secret": "emulator-only-not-real"})
    live = LiveExecutionService(client_factory=lambda exchange: client, requests_client=transport)
    return live


def worker(action, base):
    from src import app, db
    from src.OrderBookRecovery.OrderBookRecoveryService import OrderBookRecoveryService
    from src.OrderBookRecovery.OrderBookRecoveryModel import StrategyRunTrade
    from src.OrderBookRecovery.PositionGuardian import PositionGuardian
    service = OrderBookRecoveryService(live_execution_service=execution_service(base), publisher=type("NoEvents", (), {"publish": lambda *a, **k: None})())
    with app.app_context():
        config = service.get_or_create_config()
        state = service.get_or_create_state(config)
        trade = StrategyRunTrade.query.order_by(StrategyRunTrade.id.desc()).first()
        if action == "open":
            if os.getenv("CRASH_BEFORE_COMMIT") == "1":
                from sqlalchemy import event
                event.listen(db.session.session_factory.class_, "before_commit", lambda session: os._exit(74))
            service.open_position(config, state, {"mid_price": 100, "imbalance": 2, "short_momentum": .01, "spread_percent": .01}, "long", datetime.utcnow())
        elif action == "close":
            service.close_trade(trade, 102, 0, "manual_close", state, config, datetime.utcnow())
        elif action == "funding":
            PositionGuardian(service).funding(config, trade, datetime.utcnow() + timedelta(seconds=31))
        else:
            service.reconcile_existing_positions()
        db.session.remove()


class Emulator:
    def __init__(self):
        self.lock = Lock()
        self.orders = {}
        self.plans = {}
        self.submit_count = 0
        self.position = False
        self.plan_state = 1
        self.expired = False
        self.funding = []
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
                with owner.lock:
                    if self.path.endswith("order/create"):
                        owner.submit_count += 1
                        key = str(owner.submit_count)
                        closing = body["side"] in (2, 4)
                        owner.position = not closing
                        data = {"orderId": key, "positionId": 123, "symbol": "BTC_USDT", "side": body["side"],
                                "state": 3, "vol": body["vol"], "dealVol": body["vol"], "dealAvgPrice": 102 if closing else 100,
                                "totalFee": .01, "feeCurrency": "USDT", "createTime": int(datetime.now(timezone.utc).timestamp() * 1000)}
                        if closing:
                            data["profit"] = body["vol"] * .001 * 2
                        owner.orders[key] = data
                        owner.orders[body["externalOid"]] = data
                        result = {"orderId": key, "ts": data["createTime"]}
                    elif self.path.endswith("planorder/place/v2"):
                        key = str(100 + len(owner.plans))
                        owner.plans[key] = dict(body, id=key, state=1, createTime=int(datetime.now(timezone.utc).timestamp() * 1000), executeCycle=24)
                        result = key
                    elif self.path.endswith("planorder/cancel"):
                        for item in body["orders"]:
                            owner.plans[item["orderId"]]["state"] = 2
                        result = None
                    else:
                        self.send_error(404)
                        return
                self.respond(result)

            def do_GET(self):
                parsed = urlsplit(self.path)
                with owner.lock:
                    if "/contract/detail" in parsed.path:
                        result = {"symbol": "BTC_USDT", "contractSize": .001, "minVol": 1, "volUnit": 1, "volScale": 0, "priceUnit": .01, "apiAllowed": True}
                    elif "order/external/" in parsed.path or "order/get/" in parsed.path:
                        result = owner.orders.get(parsed.path.rsplit("/", 1)[-1])
                    elif "position/open_positions" in parsed.path:
                        result = [{"symbol": "BTC/USDT:USDT", "side": "long", "contracts": 140}] if owner.position else []
                    elif "planorder/list/orders" in parsed.path:
                        result = [dict(plan, state=owner.plan_state, createTime=plan["createTime"] - (25 * 3600000 if owner.expired else 0)) for plan in owner.plans.values()]
                    elif "position/funding_records" in parsed.path:
                        result = {"resultList": owner.funding, "totalPage": 1}
                    elif "history_orders" in parsed.path:
                        result = list({order["orderId"]: order for order in owner.orders.values()}.values())
                    else:
                        self.send_error(404)
                        return
                self.respond(result)

            def respond(self, result):
                data = json.dumps({"success": True, "code": 0, "data": result}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def reset(self):
        with self.lock:
            self.orders.clear()
            self.plans.clear()
            self.submit_count = 0
            self.position = False
            self.expired = False
            self.plan_state = 1
            self.funding = []


def run_suite(url, output):
    from sqlalchemy import text
    from src import app, db
    from src.Exchange.ExchangeModel import Exchange
    from src.OrderBookRecovery.OrderBookRecoveryModel import StrategyRunTrade, ExecutionSlot, TradeFundingEvent
    from src.OrderBookRecovery.OrderBookRecoveryService import OrderBookRecoveryService
    emu = Emulator()
    results = []

    def start(action, **flags):
        env = dict(os.environ, LIVE_TRADING_HARD_DISABLED="false", LIVE_TRADING_ENABLED="false", **flags)
        return subprocess.Popen([sys.executable, "-B", __file__, "--database-url", url, "--worker", action, "--emulator", emu.base], env=env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    def complete(process, expected=0):
        out, err = process.communicate(timeout=30)
        if process.returncode != expected:
            raise AssertionError(f"worker exit {process.returncode}: {err[-2000:]}")

    def reset():
        emu.reset()
        db.session.remove()
        db.session.execute(text("TRUNCATE strategy_run_trade, order_book_pattern_strategy_config, exchange RESTART IDENTITY CASCADE"))
        db.session.commit()
        service = OrderBookRecoveryService()
        config = service.get_or_create_config()
        exchange = Exchange(title="mexc", api_key="emulator-key", api_secret="emulator-only", enabled=True)
        db.session.add(exchange)
        db.session.flush()
        config.exchange_id = exchange.id
        config.exchange = "mexc"
        config.symbol = "BTC/USDT"
        config.execution_mode = "live"
        config.live_enabled_confirmation = True
        config.live_kill_switch = False
        config.emergency_entry_block = False
        db.session.commit()
        db.session.remove()

    def trade():
        db.session.remove()
        return StrategyRunTrade.query.first()

    try:
        with app.app_context():
            reset()
            first, second = start("open"), start("open")
            complete(first)
            complete(second)
            assert emu.submit_count == 1 and StrategyRunTrade.query.count() == 1 and ExecutionSlot.query.count() == 1
            results.append("concurrent_open_single_submit")
            first, second = start("close"), start("close")
            complete(first)
            complete(second)
            assert emu.submit_count == 2 and trade().closed_at and ExecutionSlot.query.count() == 0
            results.append("concurrent_close_single_submit")

            reset()
            complete(start("open", CRASH_AFTER_ACCEPT="1"), 71)
            assert emu.submit_count == 1 and trade().live_status == "open_pending"
            complete(start("reconcile"))
            assert emu.submit_count == 1 and trade().live_status == "tp_sl_unprotected" and trade().live_filled_amount > 0
            results.append("crash_after_accept_read_only_recovery")

            reset()
            complete(start("open", CRASH_BEFORE_SUBMIT="1"), 72)
            complete(start("reconcile"))
            assert emu.submit_count == 0 and ExecutionSlot.query.count() == 1
            results.append("crash_before_submit_no_blind_retry")

            reset()
            complete(start("open", CRASH_BEFORE_COMMIT="1"), 74)
            assert StrategyRunTrade.query.count() == 0 and ExecutionSlot.query.count() == 0 and emu.submit_count == 0
            complete(start("open"))
            assert emu.submit_count == 1
            results.append("crash_before_commit_database_rollback")

            reset()
            complete(start("open", CRASH_AFTER_PLAN="1"), 73)
            complete(start("reconcile"))
            assert emu.submit_count == 1 and len(emu.plans) == 1 and not trade().tp_sl_protected
            results.append("ambiguous_plan_after_crash_not_duplicated")

            reset()
            complete(start("open"))
            item = trade()
            assert item.tp_sl_protected
            item.protection_checked_at = datetime.utcnow() - timedelta(seconds=10)
            db.session.commit()
            emu.expired = True
            complete(start("reconcile"))
            assert trade().protection_status == "expired" and not trade().tp_sl_protected
            results.append("active_plan_expiry_detected")

            reset()
            complete(start("open"))
            item = trade()
            item.protection_checked_at = datetime.utcnow() - timedelta(seconds=10)
            db.session.commit()
            emu.plan_state = 2
            complete(start("reconcile"))
            assert not trade().tp_sl_protected and trade().protection_status in {"tp_state_2", "sl_state_2"}
            results.append("cancelled_protection_detected")

            item = trade()
            item.execution_config_json = None
            item.decision_snapshot_json = None
            db.session.commit()
            complete(start("reconcile"))
            assert trade().legacy_reconciliation_status == "legacy_immutable_evidence_missing" and emu.submit_count == 1
            results.append("legacy_without_evidence_quarantined")

            reset()
            complete(start("open"))
            item = trade()
            item.execution_config_json = None
            db.session.commit()
            complete(start("reconcile"))
            assert trade().legacy_reconciliation_status == "evidence_verified" and emu.submit_count == 1
            results.append("legacy_order_evidence_verified_without_submit")

            item = trade()
            item.funding_checked_at = None
            stamp = int(item.opened_at.replace(tzinfo=timezone.utc).timestamp() * 1000) + 1
            emu.funding = [{"id": 777, "symbol": "BTC_USDT", "positionType": 1, "funding": -.03, "settleTime": stamp}]
            db.session.commit()
            complete(start("close"))
            item = trade()
            item.funding_checked_at = None
            db.session.commit()
            complete(start("funding"))
            item = trade()
            assert item.funding_status == "reconciled" and abs(item.funding_pnl + .03) < 1e-9
            assert abs(item.net_pnl - (item.gross_pnl - item.total_fee - .03)) < 1e-9
            item.funding_checked_at = None
            db.session.commit()
            complete(start("funding"))
            assert TradeFundingEvent.query.count() == 1
            results.append("funding_signed_idempotent_net_accounting")
    finally:
        emu.server.shutdown()
        emu.server.server_close()
        emu.thread.join()
    report = {"passed": results, "count": len(results), "database": "isolated PostgreSQL", "exchange": "loopback HTTP emulator, NOT real MEXC", "real_orders_sent": False}
    Path(output).write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--worker", choices=["open", "close", "reconcile", "funding"])
    parser.add_argument("--emulator")
    parser.add_argument("--output", default="research/postgres-execution-tests.json")
    args = parser.parse_args()
    database_guard(args.database_url)
    os.environ["DB_CONNECTION_STRING"] = args.database_url
    os.environ["LIVE_TRADING_ENABLED"] = "false"
    if args.worker:
        if not args.emulator or urlsplit(args.emulator).hostname != "127.0.0.1":
            raise RuntimeError("only_loopback_exchange_allowed")
        worker(args.worker, args.emulator)
    else:
        os.environ["LIVE_TRADING_HARD_DISABLED"] = "true"
        run_suite(args.database_url, args.output)
