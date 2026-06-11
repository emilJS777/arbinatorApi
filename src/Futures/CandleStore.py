from datetime import datetime

import ccxt

from src import db
from src.Futures.CandleModel import Candle


class CandleStore:
    def __init__(self, exchange_id="binance", timeframe="5m", window_size=500):
        self.exchange_id = exchange_id
        self.timeframe = timeframe
        self.window_size = window_size

    def fetch_ohlcv(self, symbol="BTCUSDT", limit=500):
        exchange_class = getattr(ccxt, self.exchange_id)
        exchange = exchange_class({"enableRateLimit": True})
        ccxt_symbol = self.to_ccxt_symbol(symbol)
        return exchange.fetch_ohlcv(ccxt_symbol, timeframe=self.timeframe, limit=limit)

    def refresh(self, symbol="BTCUSDT", limit=500):
        candles = self.fetch_ohlcv(symbol, limit)
        self.upsert_many(symbol, candles)
        return self.get_window(symbol)

    def upsert_many(self, symbol: str, candles: list):
        for item in candles:
            timestamp = datetime.utcfromtimestamp(item[0] / 1000)
            candle = Candle.query.filter_by(
                exchange=self.exchange_id,
                symbol=symbol,
                timeframe=self.timeframe,
                timestamp=timestamp,
            ).first()
            if not candle:
                candle = Candle(exchange=self.exchange_id, symbol=symbol, timeframe=self.timeframe, timestamp=timestamp)
                db.session.add(candle)
            candle.open = float(item[1])
            candle.high = float(item[2])
            candle.low = float(item[3])
            candle.close = float(item[4])
            candle.volume = float(item[5])
        db.session.commit()
        self.prune(symbol)

    def get_window(self, symbol="BTCUSDT", limit=500):
        return Candle.query.filter_by(
            exchange=self.exchange_id,
            symbol=symbol,
            timeframe=self.timeframe,
        ).order_by(Candle.timestamp.desc()).limit(limit).all()[::-1]

    def prune(self, symbol: str):
        candles = Candle.query.filter_by(
            exchange=self.exchange_id,
            symbol=symbol,
            timeframe=self.timeframe,
        ).order_by(Candle.timestamp.desc()).offset(self.window_size).all()
        for candle in candles:
            db.session.delete(candle)
        db.session.commit()

    def to_ccxt_symbol(self, symbol: str) -> str:
        if "/" in symbol:
            return symbol
        if symbol.endswith("USDT"):
            return f"{symbol[:-4]}/USDT"
        return symbol
