import json

from src import db
from src.Futures.CandleStore import CandleStore
from src.Futures.IndicatorEngine import IndicatorEngine
from src.Signal.TradeSignalModel import TradeSignal
from src.PaperTrading.PaperPositionModel import PaperPosition


FUTURES_TREND_CONFIG = {
    "symbol": "BTCUSDT",
    "timeframe": "5m",
    "leverage": 2,
    "margin_mode": "isolated",
    "risk_per_trade_percent": 1,
    "take_profit_percent": 0.8,
    "stop_loss_percent": 0.5,
    "max_open_positions": 1,
    "pullback_tolerance_percent": 0.15,
}


class FuturesTrendStrategyService:
    strategy_type = "futures_trend_pullback"

    def __init__(self, candle_store=None, indicator_engine=None):
        self.candle_store = candle_store or CandleStore()
        self.indicator_engine = indicator_engine or IndicatorEngine()

    def evaluate(self, symbol="BTCUSDT", candles=None):
        candles = candles or self.candle_store.get_window(symbol)
        if len(candles) < 50:
            return None
        if self.has_open_position(symbol):
            return None

        indicators = self.indicator_engine.latest(candles)
        if not all(indicators.values()):
            return None

        candle = candles[-1]
        ema20 = indicators["ema20"]
        ema50 = indicators["ema50"]
        rsi14 = indicators["rsi14"]
        touches_ema20 = self.touches_ema20(candle, ema20)
        if not touches_ema20:
            return None

        side = None
        if ema20 > ema50 and rsi14 < 60:
            side = "long"
        elif ema20 < ema50 and rsi14 > 40:
            side = "short"
        if not side:
            return None

        return self.create_signal(symbol, side, candle.close, indicators)

    def has_open_position(self, symbol: str) -> bool:
        return PaperPosition.query.filter_by(symbol=symbol, status="open", strategy_type=self.strategy_type).first() is not None

    def touches_ema20(self, candle, ema20: float) -> bool:
        if candle.low <= ema20 <= candle.high:
            return True
        tolerance = candle.close * (FUTURES_TREND_CONFIG["pullback_tolerance_percent"] / 100)
        return abs(candle.close - ema20) <= tolerance

    def create_signal(self, symbol: str, side: str, entry_price: float, indicators: dict):
        take_profit_percent = FUTURES_TREND_CONFIG["take_profit_percent"] / 100
        stop_loss_percent = FUTURES_TREND_CONFIG["stop_loss_percent"] / 100
        if side == "short":
            take_profit = entry_price * (1 - take_profit_percent)
            stop_loss = entry_price * (1 + stop_loss_percent)
        else:
            take_profit = entry_price * (1 + take_profit_percent)
            stop_loss = entry_price * (1 - stop_loss_percent)

        reason = {
            "strategy": "Trend Following + Pullback",
            "timeframe": "5m",
            "ema20": indicators["ema20"],
            "ema50": indicators["ema50"],
            "rsi14": indicators["rsi14"],
            "atr14": indicators["atr14"],
            "config": FUTURES_TREND_CONFIG,
        }
        signal = TradeSignal(
            strategy_type=self.strategy_type,
            symbol=symbol,
            exchange="binance",
            side=side,
            entry_price=entry_price,
            take_profit_price=take_profit,
            stop_loss_price=stop_loss,
            confidence=0.7,
            reason=json.dumps(reason),
            status="created",
            config_snapshot=FUTURES_TREND_CONFIG,
        )
        db.session.add(signal)
        db.session.commit()
        return signal
