import os
import sys

import asyncio
from threading import Thread

from flask_migrate import upgrade

from src import app, logger
from src.Ccxt.CcxtService import CcxtService
from src.Exchange.ExchangeRepository import ExchangeRepository
from src.Scanner.ScannerService import ScannerService
from src.Socket.Socket import Socket
from src.TradingPair.TradingPairRepository import TradingPairRepository


def env_bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def run_startup_migrations():
    if not env_bool("AUTO_RUN_MIGRATIONS", "true"):
        logger.info("Automatic DB migrations are disabled")
        return False

    migration_dir = os.getenv("MIGRATIONS_DIR", "migrations")
    revision = os.getenv("MIGRATIONS_REVISION", "head")
    logger.info("Applying DB migrations before backend startup: revision=%s", revision)
    with app.app_context():
        upgrade(directory=migration_dir, revision=revision)
    logger.info("DB migrations applied successfully")
    return True


def run_flask():
    host = os.getenv("APP_HOST", "0.0.0.0")
    port = int(os.getenv("APP_PORT", "5555"))
    app.run(host=host, port=port)

async def async_main():
    with app.app_context():
        scanner_service = ScannerService(
            ccxt_service=CcxtService(),
            exchange_repository=ExchangeRepository(),
            trading_pair_repository=TradingPairRepository(),
            socket=Socket()
        )
        await asyncio.gather(
            scanner_service.order_scanner(),
            scanner_service.balance_scanner(),
            scanner_service.account_active_orders_scanner(),
            # scanner_service.trades_scanner()
        )

if __name__ == '__main__':
    try:
        run_startup_migrations()
    except Exception as error:
        logger.exception("DB migration failed; backend startup aborted: %s", error)
        sys.exit(1)

    flask_thread = Thread(target=run_flask)
    flask_thread.start()
    debug = os.getenv("ASYNC_DEBUG", "false").lower() == "true"
    asyncio.run(async_main(), debug=debug)
