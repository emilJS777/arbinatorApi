from src.Ccxt.CcxtService import CcxtService
import ccxt

class ExchangeGetter:
    @staticmethod
    def get_exchange(exchange: str, api_key: str, api_secret: str, password: str = "") -> CcxtService or None:
        exchange_service = None
        if exchange.lower() == 'binance':
            exchange_service = CcxtService()
            exchange_service.exchange = ccxt.binance({'apiKey': api_key, 'secret': api_secret, 'enableRateLimit': True})
        elif exchange.lower() == 'bybit':
            exchange_service = CcxtService()
            exchange_service.exchange = ccxt.bybit({'apiKey': api_key, 'secret': api_secret, 'enableRateLimit': True})
        elif exchange.lower() == 'kraken':
            exchange_service = CcxtService()
            exchange_service.exchange = ccxt.kraken({'apiKey': api_key, 'secret': api_secret, 'enableRateLimit': True})
        elif exchange.lower() == 'apex':
            exchange_service = CcxtService()
            exchange_service.exchange = ccxt.apex({'apiKey': api_key, 'secret': api_secret, 'enableRateLimit': True})
        elif exchange.lower() == 'cex':
            exchange_service = CcxtService()
            exchange_service.exchange = ccxt.cex({'apiKey': api_key, 'secret': api_secret, 'enableRateLimit': True})
        elif exchange.lower() == 'coinbase':
            exchange_service = CcxtService()
            exchange_service.exchange = ccxt.coinbase({'apiKey': api_key, 'secret': api_secret, 'enableRateLimit': True})
        elif exchange.lower() == 'okx':
            exchange_service = CcxtService()
            exchange_service.exchange = ccxt.okx({'apiKey': api_key, 'secret': api_secret, 'enableRateLimit': True})
        elif exchange.lower() == 'bitfinex':
            exchange_service = CcxtService()
            exchange_service.exchange = ccxt.bitfinex({'apiKey': api_key, 'secret': api_secret, 'enableRateLimit': True})

        elif exchange.lower() == 'huobi':
            exchange_service = CcxtService()
            exchange_service.exchange = ccxt.huobi({'apiKey': api_key, 'secret': api_secret, 'enableRateLimit': True})

        elif exchange.lower() == 'bitstamp':
            exchange_service = CcxtService()
            exchange_service.exchange = ccxt.bitstamp({'apiKey': api_key, 'secret': api_secret, 'enableRateLimit': True})

        elif exchange.lower() == 'gate.io':
            exchange_service = CcxtService()
            exchange_service.exchange = ccxt.gateio({'apiKey': api_key, 'secret': api_secret, 'enableRateLimit': True})

        elif exchange.lower() == 'kucoin':
            exchange_service = CcxtService()
            exchange_service.exchange = ccxt.kucoin({'apiKey': api_key, 'secret': api_secret, 'password': password, 'enableRateLimit': True})

        elif exchange.lower() == 'mexc':
            exchange_service = CcxtService()
            exchange_service.exchange = ccxt.mexc({'apiKey': api_key, 'secret': api_secret, 'enableRateLimit': True})

        elif exchange.lower() == 'ascendex':
            exchange_service = CcxtService()
            exchange_service.exchange = ccxt.ascendex({'apiKey': api_key, 'secret': api_secret, 'enableRateLimit': True})

        elif exchange.lower() == 'whitebit':
            exchange_service = CcxtService()
            exchange_service.exchange = ccxt.whitebit({'apiKey': api_key, 'secret': api_secret, 'enableRateLimit': True})

        return exchange_service
