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
        self.balance_scan_interval = float(os.getenv("BALANCE_SCAN_INTERVAL", "1.5"))
        self.account_orders_scan_interval = float(os.getenv("ACCOUNT_ORDERS_SCAN_INTERVAL", "1.5"))
        self.max_parallel_tasks = max(1, int(os.getenv("SCANNER_MAX_PARALLEL_TASKS", "10")))
        self.exchange_error_cooldown_seconds = max(5, int(os.getenv("EXCHANGE_ERROR_COOLDOWN_SECONDS", "120")))
        self._semaphore = asyncio.Semaphore(self.max_parallel_tasks)
        self._exchange_cooldowns = {}
        self._exchange_status_cache = {}
        self.arbitrage_strategy_service = ArbitrageStrategyService()
        self.order_book_recovery_service = OrderBookRecoveryService()

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
        if isinstance(error, (ccxt.AuthenticationError, ccxt.PermissionDenied)):
            return self.exchange_error_cooldown_seconds * 5

        return self.exchange_error_cooldown_seconds

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
        async with self._semaphore:
            if self._is_on_cooldown(exchange, "order_book"):
                return

            # Order books are public; using a public client avoids credential/IP whitelist failures
            # triggered by private market/currency bootstrap on some exchanges.
            ccxt_exchange = ExchangeGetter.get_exchange(
                exchange.title, "", "", password=""
            )
            if not ccxt_exchange:
                logger.warning("Exchange %s not found", exchange.title)
                return

            try:
                order_book = await asyncio.to_thread(
                    ccxt_exchange.get_order_book, trading_pair.pair, trading_pair.order_limit
                )
                if not order_book:
                    return

                logger.info("scanner snapshot exchange=%s exchange_id=%s pair=%s", exchange.title, exchange.id, trading_pair.pair)
                OrderBookSnapshotStore.update(
                    exchange=exchange.title,
                    symbol=trading_pair.pair,
                    order_book=order_book,
                    metadata={
                        "source_exchange_title": exchange.title,
                        "source_exchange_id": exchange.id,
                        "source_pair": trading_pair.pair,
                        "snapshot_timestamp": time.time(),
                        "exchange_icon_path": exchange.icon_path,
                        "pair_icon_path": trading_pair.icon_path,
                        "pair_max_purchase_price": trading_pair.max_purchase_price or "",
                    },
                )

                await self.socket.send("order_book", {
                    "exchange": exchange.title,
                    "exchange_icon_path": exchange.icon_path,
                    "pair": trading_pair.pair,
                    "pair_max_purchase_price": trading_pair.max_purchase_price or '',
                    "pair_icon_path": trading_pair.icon_path,
                    "order_book": order_book,
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
                    },
                )
                await self._mark_exchange_recovered(exchange, "order_book")
            except Exception as error:
                logger.warning("Order book fetch failed for %s on %s: %s", trading_pair.pair, exchange.title, error)
                await self._mark_exchange_failed(exchange, "order_book", error)

    async def order_scanner(self):
        async def cycle():
            exchanges = self.exchange_repository.get_all(enabled=True)
            tasks = []
            for exchange in exchanges:
                trading_pairs = self.trading_pair_repository.get_all(exchange_id=exchange.id, enabled=True)
                for trading_pair in trading_pairs:
                    tasks.append(asyncio.create_task(self.fetch_pair_order_book(exchange, trading_pair)))
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, Exception):
                        logger.exception("order_scanner task failed: %s", result)

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
