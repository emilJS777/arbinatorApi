from typing import Dict, List, Any
import time
import logging

from src.Ccxt.CcxtServiceInterface import CcxtServiceInterface


logger = logging.getLogger(__name__)

class CcxtService(CcxtServiceInterface):
    def __init__(self):
        self.exchange = None
        self._markets_cache = None

    def _markets(self):
        if self._markets_cache is None:
            self._markets_cache = self.exchange.load_markets()
        return self._markets_cache

    def create_limit_sell_order(self, symbol: str, amount: float, price: float):
        order = self.exchange.create_order(
            symbol=symbol,
            type='limit',
            side='sell',
            amount=amount,
            price=price,
            params={
                'timeInForce': 'GTC'  # можно заменить на 'IOC' или 'FOK'
            }
        )
        return order

    def create_limit_buy_order(self, symbol: str, amount: float, price: float):
        order = self.exchange.create_order(
            symbol=symbol,
            type='limit',
            side='buy',
            amount=amount,
            price=price,
            params={
                'timeInForce': 'GTC'  # можно заменить на 'IOC' или 'FOK'
            }
        )
        return order

    def get_order_book(self, pair: str, limit: int) -> Dict[str, List[Any]]:
        try:
            order_book = self.exchange.fetch_order_book(pair)

            market_info = self._markets().get(pair, {})
            taker_fee = market_info.get('taker') or 0

            sales = []
            for price, amount in order_book['asks'][:limit]:
                sales.append({'price': price, 'amount': amount, "commission": (price * amount) * taker_fee})

            purchases = []
            for price, amount in order_book['bids'][:limit]:
                purchases.append({'price': price, 'amount': amount, "commission": (price * amount) * taker_fee})

            return {"purchases": purchases, "sales": sales}
        except Exception:
            logger.warning("Failed to fetch order book for %s", pair)
            raise

    def get_balance(self, symbol) -> Dict[str, float]:
        try:
            balance = self.exchange.fetch_balance()
            return {"total": balance.get('total').get(symbol, 0), "free": balance.get('free').get(symbol, 0), "used": balance.get('used').get(symbol, 0)}
        except Exception:
            logger.warning("Failed to fetch balance for %s", symbol)
            raise

    def get_account_active_orders(self, symbol: str) -> list:
        orders = self.exchange.fetch_open_orders(symbol=symbol)
        simplified_orders = []
        for order in orders:
            simplified_orders.append({
                'id': order['id'],
                'side': order['side'],
                'price': order['price'],
                'amount': order['amount'],
                'filled': order['filled'],
                'remaining': order['remaining'],
                'status': order['status'],
                'datetime': order['datetime'],
                'type': order['type'],
                'timeInForce': order.get('timeInForce'),
            })

        return simplified_orders

    def cancel_active_order(self, order_id: str, symbol: str) -> bool:
        response = self.exchange.cancel_order(order_id, symbol)
        return True

    def get_trades(self, symbol: str) -> list:
        trades = self.exchange.fetch_trades(symbol)
        now = int(time.time() * 1000)
        three_hours_ago = now - 1 * 60 * 60 * 1000  # 1 hour

        recent_trades = [trade for trade in trades if trade['timestamp'] >= three_hours_ago]
        return recent_trades
