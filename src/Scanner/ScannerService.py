import asyncio
import logging
import os
import time

import ccxt

from src import db
from src.Arbitrage.ArbitrageStrategyService import ArbitrageStrategyService
from src.Arbitrage.OrderBookSnapshotStore import OrderBookSnapshotStore
from src.Exchange.ExchangeRepositoryInterface import ExchangeRepositoryInterface
from src.OrderBookRecovery.OrderBookRecoveryService import OrderBookRecoveryService
from src.Socket.ISocket import ISocket
from src.TradingPair.TradingPairRepositoryInterface import TradingPairRepositoryInterface
from src.__Parents.ExchangeGetter import ExchangeGetter


logger = logging.getLogger(__name__)


class ScannerService:
    def __init__(self, ccxt_service, exchange_repository: ExchangeRepositoryInterface, trading_pair_repository: TradingPairRepositoryInterface, socket: ISocket):
        self.ccxt_service = ccxt_service
        self.exchange_repository = exchange_repository
        self.trading_pair_repository = trading_pair_repository
        self.socket = socket
        self.order_scan_interval = float(os.getenv("ORDER_SCAN_INTERVAL", "0.5"))
        self.fast_order_book_interval_seconds = float(os.getenv("FAST_ORDER_BOOK_INTERVAL_SECONDS", "1"))
        self.order_book_fetch_timeout_seconds = float(os.getenv("ORDER_BOOK_FETCH_TIMEOUT_SECONDS", "2"))
        self.slow_exchange_cooldown_seconds = float(os.getenv("SLOW_EXCHANGE_COOLDOWN_SECONDS", "10"))
        self.failed_exchange_cooldown_seconds = float(os.getenv("FAILED_EXCHANGE_COOLDOWN_SECONDS", "30"))
        self.balance_scan_interval = float(os.getenv("BALANCE_SCAN_INTERVAL", "1.5"))
        self.account_orders_scan_interval = float(os.getenv("ACCOUNT_ORDERS_SCAN_INTERVAL", "1.5"))
        self.max_parallel_tasks = max(1, int(os.getenv("SCANNER_MAX_PARALLEL_TASKS", "10")))
        self.exchange_error_cooldown_seconds = max(5, int(os.getenv("EXCHANGE_ERROR_COOLDOWN_SECONDS", "120")))
        self._semaphore = asyncio.Semaphore(self.max_parallel_tasks)
        self._order_book_semaphore = asyncio.Semaphore(self.max_parallel_tasks)
        self._exchange_cooldowns = {}
        self._exchange_status_cache = {}
        self._order_book_inflight = set()
        self._next_order_book_scan_at = {}
        self._public_exchange_clients = {}
        self.arbitrage_strategy_service = ArbitrageStrategyService()
        self.order_book_recovery_service = OrderBookRecoveryService()

    _diagnostics = {}

    def _cooldown_key(self, exchange, scope: str):
        return f"{exchange.id}:{scope}"

    def _is_on_cooldown(self, exchange, scope: str) -> bool:
        retry_at = self._exchange_cooldowns.get(self._cooldown_key(exchange, scope))
        return bool(retry_at and retry_at > time.time())

    async def _emit_exchange_status(self, exchange, scope: str, status: str, message: str | None = None):
        cache_key = self._cooldown_key(exchange, scope)
        payload = {
            "exchange": exchange.title,
            "exchange_id": exchange.id,
            "scope": scope,
            "status": status,
            "message": message,
        }

        if self._exchange_status_cache.get(cache_key) == payload:
            return

        self._exchange_status_cache[cache_key] = payload
        await self.socket.send("exchange_status", payload)

    async def _mark_exchange_failed(self, exchange, scope: str, error: Exception):
        retry_at = time.time() + self._get_cooldown_seconds(error)
        self._exchange_cooldowns[self._cooldown_key(exchange, scope)] = retry_at
        await self._emit_exchange_status(
            exchange,
            scope,
            "unavailable",
            f"{type(error).__name__}: {error}",
        )

    async def _mark_exchange_recovered(self, exchange, scope: str):
        cache_key = self._cooldown_key(exchange, scope)
        self._exchange_cooldowns.pop(cache_key, None)
        if self._exchange_status_cache.get(cache_key, {}).get("status") == "unavailable":
            await self._emit_exchange_status(exchange, scope, "available", "Exchange recovered")

    def _get_cooldown_seconds(self, error: Exception) -> int:
        if isinstance(error, asyncio.TimeoutError):
            return int(self.slow_exchange_cooldown_seconds)
        if isinstance(error, (ccxt.AuthenticationError, ccxt.PermissionDenied)):
            return self.exchange_error_cooldown_seconds * 5
        return int(self.failed_exchange_cooldown_seconds or self.exchange_error_cooldown_seconds)

    def _pair_key(self, exchange, trading_pair):
        return f"{exchange.id}:{trading_pair.id}"

    def _diagnostic_key(self, exchange, symbol: str):
        return f"{exchange.id}:{symbol}"

    def _public_exchange_client(self, exchange):
        cache_key = str(exchange.id)
        client = self._public_exchange_clients.get(cache_key)
        if client:
            return client
        client = ExchangeGetter.get_exchange(exchange.title, "", "", password="")
        if client:
            self._public_exchange_clients[cache_key] = client
        return client

    def _diagnostic_payload(self, exchange, trading_pair, **updates):
        now = time.time()
        key = self._diagnostic_key(exchange, trading_pair.pair)
        current = self.__class__._diagnostics.get(key, {
            "exchange": exchange.title,
            "exchange_id": exchange.id,
            "symbol": trading_pair.pair,
            "status": "waiting",
            "latency_ms": None,
            "stale_seconds": None,
            "last_success_at": None,
            "last_error_at": None,
            "error_message": None,
            "success_count": 0,
            "fail_count": 0,
            "active": True,
            "cooldown_until": None,
        })
        current.update(updates)
        if current.get("last_success_ts"):
            current["stale_seconds"] = max(0, now - current["last_success_ts"])
        cooldown_until_ts = current.get("cooldown_until_ts")
        current["cooldown_until"] = self._datetime_from_ts(cooldown_until_ts) if cooldown_until_ts else None
        self.__class__._diagnostics[key] = current
        return current

    @staticmethod
    def _datetime_from_ts(value):
        if not value:
            return None
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(value))

    @classmethod
    def diagnostics_payload(cls):
        now = time.time()
        rows = []
        for row in cls._diagnostics.values():
            item = dict(row)
            if item.get("last_success_ts"):
                item["stale_seconds"] = max(0, now - item["last_success_ts"])
            item.pop("last_success_ts", None)
            item.pop("cooldown_until_ts", None)
            rows.append(item)
        return sorted(rows, key=lambda row: (str(row.get("exchange") or ""), str(row.get("symbol") or "")))

    async def run_scanner_cycle(self, name: str, interval: float, coroutine_factory):
        while True:
            try:
                await coroutine_factory()
            except Exception as error:
                logger.exception("Scanner cycle '%s' failed: %s", name, error)
            finally:
                db.session.remove()
                await asyncio.sleep(interval)

    async def fetch_pair_order_book(self, exchange, trading_pair):
        async with self._order_book_semaphore:
            if self._is_on_cooldown(exchange, "order_book"):
                retry_at = self._exchange_cooldowns.get(self._cooldown_key(exchange, "order_book"))
                self._diagnostic_payload(
                    exchange,
                    trading_pair,
                    status="cooldown",
                    active=False,
                    cooldown_until_ts=retry_at,
                )
                return

            ccxt_exchange = self._public_exchange_client(exchange)
            if not ccxt_exchange:
                logger.warning("Exchange %s not found", exchange.title)
                self._diagnostic_payload(exchange, trading_pair, status="disabled", active=False)
                return

            started_at = time.perf_counter()
            try:
                order_book = await asyncio.wait_for(
                    asyncio.to_thread(ccxt_exchange.get_order_book, trading_pair.pair, trading_pair.order_limit),
                    timeout=self.order_book_fetch_timeout_seconds,
                )
                latency_ms = round((time.perf_counter() - started_at) * 1000, 2)
                if not order_book:
                    self._diagnostic_payload(exchange, trading_pair, status="empty", latency_ms=latency_ms)
                    return

                snapshot_ts = time.time()
                logger.info("scanner snapshot exchange=%s exchange_id=%s pair=%s latency_ms=%s", exchange.title, exchange.id, trading_pair.pair, latency_ms)
                OrderBookSnapshotStore.update(
                    exchange=exchange.title,
                    symbol=trading_pair.pair,
                    order_book=order_book,
                    metadata={
                        "source_exchange_title": exchange.title,
                        "source_exchange_id": exchange.id,
                        "source_pair": trading_pair.pair,
                        "snapshot_timestamp": snapshot_ts,
                        "fetch_latency_ms": latency_ms,
                        "exchange_icon_path": exchange.icon_path,
                        "pair_icon_path": trading_pair.icon_path,
                        "pair_max_purchase_price": trading_pair.max_purchase_price or "",
                    },
                )
                self._diagnostic_payload(
                    exchange,
                    trading_pair,
                    status="active",
                    active=True,
                    latency_ms=latency_ms,
                    last_success_at=self._datetime_from_ts(snapshot_ts),
                    last_success_ts=snapshot_ts,
                    error_message=None,
                    cooldown_until_ts=None,
                    success_count=(self.__class__._diagnostics.get(self._diagnostic_key(exchange, trading_pair.pair), {}).get("success_count", 0) + 1),
                )

                await self.socket.send("order_book", {
                    "exchange": exchange.title,
                    "exchange_icon_path": exchange.icon_path,
                    "pair": trading_pair.pair,
                    "pair_max_purchase_price": trading_pair.max_purchase_price or '',
                    "pair_icon_path": trading_pair.icon_path,
                    "order_book": order_book,
                    "snapshot_timestamp": snapshot_ts,
                    "fetch_latency_ms": latency_ms,
                })
                self.arbitrage_strategy_service.run_once(ignore_enabled=False)
                self.order_book_recovery_service.on_order_book_snapshot(
                    exchange.title,
                    trading_pair.pair,
                    metadata={
                        "exchange_id": exchange.id,
                        "exchange_title": exchange.title,
                        "exchange_slug": getattr(exchange, "slug", None) or exchange.title,
                        "raw_pair": trading_pair.pair,
                        "normalized_symbol": self.order_book_recovery_service.normalize_symbol(trading_pair.pair),
                        "snapshot_timestamp": snapshot_ts,
                        "fetch_latency_ms": latency_ms,
                    },
                )
                await self._mark_exchange_recovered(exchange, "order_book")
            except Exception as error:
                latency_ms = round((time.perf_counter() - started_at) * 1000, 2)
                logger.warning("Order book fetch failed for %s on %s after %sms: %s", trading_pair.pair, exchange.title, latency_ms, error)
                await self._mark_exchange_failed(exchange, "order_book", error)
                retry_at = self._exchange_cooldowns.get(self._cooldown_key(exchange, "order_book"))
                current = self.__class__._diagnostics.get(self._diagnostic_key(exchange, trading_pair.pair), {})
                self._diagnostic_payload(
                    exchange,
                    trading_pair,
                    status="timeout" if isinstance(error, asyncio.TimeoutError) else "failed",
                    active=False,
                    latency_ms=latency_ms,
                    last_error_at=self._datetime_from_ts(time.time()),
                    error_message=f"{type(error).__name__}: {error}",
                    cooldown_until_ts=retry_at,
                    fail_count=current.get("fail_count", 0) + 1,
                )

    async def _run_order_book_task(self, exchange, trading_pair):
        key = self._pair_key(exchange, trading_pair)
        try:
            await self.fetch_pair_order_book(exchange, trading_pair)
        except Exception as error:
            logger.exception("order_scanner task failed for %s %s: %s", exchange.title, trading_pair.pair, error)
        finally:
            self._order_book_inflight.discard(key)
            db.session.remove()

    async def order_scanner(self):
        async def cycle():
            now = time.time()
            exchanges = self.exchange_repository.get_all(enabled=True)
            for exchange in exchanges:
                trading_pairs = self.trading_pair_repository.get_all(exchange_id=exchange.id, enabled=True)
                for trading_pair in trading_pairs:
                    key = self._pair_key(exchange, trading_pair)
                    next_scan_at = self._next_order_book_scan_at.get(key, 0)
                    if key in self._order_book_inflight or next_scan_at > now:
                        continue
                    self._order_book_inflight.add(key)
                    self._next_order_book_scan_at[key] = now + self.fast_order_book_interval_seconds
                    asyncio.create_task(self._run_order_book_task(exchange, trading_pair))

        await self.run_scanner_cycle("order_scanner", self.order_scan_interval, cycle)

    async def fetch_balance(self, exchange, pair, symbol):
        async with self._semaphore:
            if self._is_on_cooldown(exchange, "balance"):
                return

            ccxt_exchange = ExchangeGetter.get_exchange(
                exchange.title, exchange.api_key, exchange.api_secret, password=exchange.password
            )
            if not ccxt_exchange:
                logger.warning("Exchange %s not found", exchange.title)
                return

            try:
                balance = await asyncio.to_thread(ccxt_exchange.get_balance, symbol)
                if not balance:
                    return

                await self.socket.send("balance", {
                    "symbol": symbol,
                    "exchange": exchange.title,
                    "pair": pair,
                    "balance": balance
                })
                await self._mark_exchange_recovered(exchange, "balance")
            except Exception as error:
                logger.warning("Balance fetch failed for %s on %s: %s", symbol, exchange.title, error)
                await self._mark_exchange_failed(exchange, "balance", error)

    async def balance_scanner(self):
        async def cycle():
            exchanges = self.exchange_repository.get_all(enabled=True)
            tasks = []
            for exchange in exchanges:
                if exchange.api_key:
                    tasks.append(asyncio.create_task(self.fetch_balance(exchange, "", 'USDT')))
                    trading_pairs = self.trading_pair_repository.get_all(exchange_id=exchange.id, enabled=True)
                    for trading_pair in trading_pairs:
                        tasks.append(asyncio.create_task(self.fetch_balance(exchange, trading_pair.pair, trading_pair.pair.split('/')[0])))
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, Exception):
                        logger.exception("balance_scanner task failed: %s", result)

        await self.run_scanner_cycle("balance_scanner", self.balance_scan_interval, cycle)

    async def fetch_account_active_orders(self, exchange, pair):
        async with self._semaphore:
            if self._is_on_cooldown(exchange, "account_active_orders"):
                return

            ccxt_exchange = ExchangeGetter.get_exchange(
                exchange.title, exchange.api_key, exchange.api_secret, password=exchange.password
            )
            if not ccxt_exchange:
                logger.warning("Exchange %s not found", exchange.title)
                return

            try:
                orders = await asyncio.to_thread(ccxt_exchange.get_account_active_orders, pair)
                await self.socket.send("account_active_orders", {
                    "exchange": exchange.title,
                    "exchange_id": exchange.id,
                    "exchange_icon_path": exchange.icon_path,
                    "pair": pair,
                    "orders": orders,
                })
                await self._mark_exchange_recovered(exchange, "account_active_orders")
            except Exception as e:
                logger.warning("Error fetching orders for %s on %s: %s", pair, exchange.title, e)
                await self._mark_exchange_failed(exchange, "account_active_orders", e)
                orders = []

    async def account_active_orders_scanner(self):
        async def cycle():
            exchanges = self.exchange_repository.get_all(enabled=True)
            tasks = []
            for exchange in exchanges:
                if exchange.api_key:
                    trading_pairs = self.trading_pair_repository.get_all(exchange_id=exchange.id, enabled=True)
                    for trading_pair in trading_pairs:
                        tasks.append(asyncio.create_task(self.fetch_account_active_orders(exchange, trading_pair.pair)))
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, Exception):
                        logger.exception("account_active_orders_scanner task failed: %s", result)

        await self.run_scanner_cycle("account_active_orders_scanner", self.account_orders_scan_interval, cycle)

    # # TRADES FETCH
    # async def fetch_trades(self, exchange, trading_pair):
    #     ccxt_exchange = ExchangeGetter.get_exchange(exchange.title, exchange.api_key, exchange.api_secret, password=exchange.password)
    #     if not ccxt_exchange:
    #         print(f"Exchange {exchange.title} not found")
    #         return
    #
    #     try:
    #         trades = await asyncio.to_thread(ccxt_exchange.get_trades, trading_pair.pair)
    #     except Exception as e:
    #         print(f"Error fetching trades for {trading_pair.pair} on {exchange.title}: {e}")
    #         trades = []
    #
    #     simple_trades = [
    #         {"timestamp": trade["timestamp"], "price": trade["price"]}
    #         for trade in trades
    #     ]
    #
    #     await self.socket.send("trades", {
    #         "exchange": exchange.title,
    #         "pair": trading_pair.pair,
    #         "trades": simple_trades
    #     })
    #
    # async def trades_scanner(self):
    #     while True:
    #         exchanges = self.exchange_repository.get_all(enabled=True)
    #         tasks = []
    #         for exchange in exchanges:
    #             trading_pairs = self.trading_pair_repository.get_all(exchange_id=exchange.id, enabled=True)
    #             for trading_pair in trading_pairs:
    #                 tasks.append(create_task(self.fetch_trades(exchange, trading_pair)))
    #         await gather(*tasks)
    #         db.session.remove()
    #         await asyncio.sleep(5)
