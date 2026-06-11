from datetime import datetime, timedelta

from src import db
from src.Futures.BacktestModel import StrategyCandidate
from src.Futures.BacktestService import BacktestCandle
from src.Futures.ExperimentRunnerService import ExperimentRunnerService


def make_experiment_candles(count=180):
    base = datetime(2025, 1, 1)
    candles = []
    price = 100
    for index in range(count):
        cycle = index % 20
        if cycle < 8:
            price += 0.22
        elif cycle < 13:
            price -= 0.35
        else:
            price += 0.42
        candles.append(BacktestCandle(
            timestamp=base + timedelta(minutes=5 * index),
            open=price - 0.1,
            high=price + 0.55,
            low=price - (1.2 if cycle == 12 else 0.45),
            close=price,
            volume=100,
        ))
    return candles


def test_experiment_parameter_grid_normalizes_rsi_short_threshold():
    service = ExperimentRunnerService()
    params = service.normalize_parameters({
        "ema_fast": 20,
        "ema_slow": 50,
        "rsi_threshold": 60,
        "take_profit_percent": 0.8,
        "stop_loss_percent": 0.5,
        "risk_per_trade_percent": 1,
    })

    assert params["rsi_long_max"] == 60
    assert params["rsi_short_min"] == 40


def test_experiment_stability_score_components():
    service = ExperimentRunnerService()
    metrics = {"profit_factor": 1.8, "max_drawdown": 500, "sharpe_ratio": 1.2}
    walk_forward = {"train_profit_factor": 1.5, "test_profit_factor": 1.2}
    score = service.score(metrics, walk_forward, {"initial_equity": 10000})

    assert score["stability_score"] > 0
    assert score["profit_factor_score"] > 0
    assert score["walk_forward_score"] == 80


def test_experiment_rejection_reasons():
    service = ExperimentRunnerService()
    metrics = {"profit_factor": 1.0, "expectancy": -1, "max_drawdown": 2500, "trades_count": 20}
    walk_forward = {"train_profit_factor": 1.8, "test_profit_factor": 0.8}
    reasons = service.rejection_reasons(metrics, walk_forward, {"initial_equity": 10000})

    assert "profit_factor_below_1_2" in reasons
    assert "negative_expectancy" in reasons
    assert "high_drawdown" in reasons
    assert "low_trade_count" in reasons
    assert "overfitted" in reasons


def test_experiment_runner_persists_candidates(client, monkeypatch):
    service = ExperimentRunnerService()
    monkeypatch.setattr(service, "resolve_candles", lambda *args, **kwargs: make_experiment_candles())

    result = service.run(symbols=["BTCUSDT"], timeframes=["5m"], periods=[30], max_combinations=5)

    assert result["runs"] == 5
    assert StrategyCandidate.query.count() == 5
    assert "ema_combinations" in result["heatmaps"]


def test_experiment_candidates_rank_by_stability(client):
    db.session.add(StrategyCandidate(exchange="binance", symbol="BTCUSDT", timeframe="5m", period_days=30, parameters={}, profit_factor=1.3, win_rate=50, expectancy=1, max_drawdown=100, max_drawdown_percent=1, sharpe=1, trades_count=101, stability_score=20, profit_factor_score=1, drawdown_score=1, sharpe_score=1, walk_forward_score=1, rejection_reasons=[]))
    db.session.add(StrategyCandidate(exchange="binance", symbol="BTCUSDT", timeframe="5m", period_days=90, parameters={}, profit_factor=1.6, win_rate=55, expectancy=2, max_drawdown=100, max_drawdown_percent=1, sharpe=1, trades_count=120, stability_score=80, profit_factor_score=1, drawdown_score=1, sharpe_score=1, walk_forward_score=1, rejection_reasons=[]))
    db.session.commit()

    response = client.get("/api/research/candidates")
    rows = response.get_json()["obj"]

    assert rows[0]["stability_score"] == 80
