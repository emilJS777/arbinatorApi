from datetime import datetime, timedelta

from src import db
from src.Futures.CandleModel import Candle
from src.Futures.EquityCurveModel import EquityCurvePoint
from src.Futures.FuturesPaperTradingService import FuturesPaperTradingService
from src.Futures.FuturesTradeModel import FuturesTrade
from src.Futures.FuturesTrendStrategyService import FuturesTrendStrategyService
from src.Futures.MetricsService import MetricsService
from src.PaperTrading.PaperPositionModel import PaperPosition
from src.Signal.TradeSignalModel import TradeSignal


def make_candles():
    base = datetime.utcnow() - timedelta(minutes=5 * 60)
    candles = []
    price = 100
    for index in range(60):
        price += 0.12
        candles.append(Candle(
            exchange="binance",
            symbol="BTCUSDT",
            timeframe="5m",
            timestamp=base + timedelta(minutes=5 * index),
            open=price - 0.1,
            high=price + 0.2,
            low=price - 0.2,
            close=price,
            volume=100,
        ))
    # Pullback candle: keep uptrend, RSI below extreme, and touch EMA20 area.
    candles[-8].close -= 0.7
    candles[-7].close -= 0.8
    candles[-6].close -= 0.9
    candles[-5].close -= 1.0
    candles[-4].close -= 1.1
    candles[-3].close -= 1.2
    candles[-2].close -= 1.3
    candles[-1].close = candles[-2].close - 0.35
    candles[-1].open = candles[-1].close + 0.2
    candles[-1].high = candles[-1].close + 0.4
    candles[-1].low = candles[-1].close - 1.4
    return candles


def test_futures_trend_signal_generation(client):
    signal = FuturesTrendStrategyService().evaluate("BTCUSDT", make_candles())

    assert signal is not None
    assert signal.strategy_type == "futures_trend_pullback"
    assert signal.side == "long"
    assert signal.take_profit_price > signal.entry_price
    assert signal.stop_loss_price < signal.entry_price


def test_stop_loss_logic(client):
    signal = TradeSignal(symbol="BTCUSDT", exchange="binance", side="long", entry_price=100, stop_loss_price=99.5, take_profit_price=100.8, status="created")
    db.session.add(signal)
    db.session.commit()
    ok, position = FuturesPaperTradingService().open_position(signal, risk_config={"max_position_margin": 50000})
    assert ok

    trade = FuturesPaperTradingService().evaluate_exit(position, high=100.2, low=99.4)

    assert trade.exit_reason == "stop_loss"
    assert trade.realized_pnl < 0


def test_take_profit_logic(client):
    signal = TradeSignal(symbol="BTCUSDT", exchange="binance", side="long", entry_price=100, stop_loss_price=99.5, take_profit_price=100.8, status="created")
    db.session.add(signal)
    db.session.commit()
    ok, position = FuturesPaperTradingService().open_position(signal, risk_config={"max_position_margin": 50000})
    assert ok

    trade = FuturesPaperTradingService().evaluate_exit(position, high=100.9, low=99.8)

    assert trade.exit_reason == "take_profit"
    assert trade.realized_pnl > 0


def test_metrics_calculation(client):
    db.session.add(FuturesTrade(symbol="BTCUSDT", side="long", entry_price=100, exit_price=101, amount=1, realized_pnl=10, fee=0, margin=50))
    db.session.add(FuturesTrade(symbol="BTCUSDT", side="long", entry_price=100, exit_price=99, amount=1, realized_pnl=-5, fee=0, margin=50))
    db.session.commit()

    metrics = MetricsService().metrics()

    assert metrics["total_trades"] == 2
    assert metrics["win_rate"] == 50
    assert metrics["profit_factor"] == 2
    assert metrics["total_pnl"] == 5


def test_drawdown_calculation(client):
    db.session.add(EquityCurvePoint(equity=10000, realized_pnl=0, unrealized_pnl=0))
    db.session.add(EquityCurvePoint(equity=10100, realized_pnl=100, unrealized_pnl=0))
    db.session.add(EquityCurvePoint(equity=9900, realized_pnl=-100, unrealized_pnl=0))
    db.session.add(EquityCurvePoint(equity=10050, realized_pnl=50, unrealized_pnl=0))
    db.session.commit()

    assert MetricsService().max_drawdown() == 200
