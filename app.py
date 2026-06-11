import os

import asyncio
from threading import Thread

from src import app
from src.Ccxt.CcxtService import CcxtService
from src.Exchange.ExchangeRepository import ExchangeRepository
from src.Scanner.ScannerService import ScannerService
from src.Socket.Socket import Socket
from src.TradingPair.TradingPairRepository import TradingPairRepository


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
    flask_thread = Thread(target=run_flask)
    flask_thread.start()
    debug = os.getenv("ASYNC_DEBUG", "false").lower() == "true"
    asyncio.run(async_main(), debug=debug)
