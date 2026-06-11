from datetime import datetime, timedelta

from src.Futures.BacktestService import BacktestCandle, BacktestService, DEFAULT_BACKTEST_CONFIG


def make_research_candles(count=140):
    base = datetime(2025, 1, 1)
    candles = []
    price = 100
    for index in range(count):
        cycle = index % 18
        if cycle < 10:
            price += 0.18
        elif cycle < 14:
            price -= 0.28
        else:
            price += 0.34
        high = price + 0.35
        low = price - 0.35
        if cycle == 13:
            low = price - 1.2
        candles.append(BacktestCandle(
            timestamp=base + timedelta(minutes=5 * index),
            open=price - 0.05,
            high=high,
            low=low,
            close=price,
            volume=100,
        ))
    return candles


def test_backtest_no_lookahead_bias():
    service = BacktestService()
    candles = make_research_candles()
    config = service.merge_config({"fee_percent": 0, "min_trades": 0})
    result = service.simulate(candles, config)

    for trade in result["trades"]:
        assert trade["exit_time"] > trade["entry_time"]


def test_research_drawdown_calculation():
    service = BacktestService()
    assert service.drawdown_curve([10000, 10100, 9900, 10050]) == [0, 0, 200, 50]


def test_research_expectancy():
    service = BacktestService()
    trades = [
        {"pnl": 10, "r_multiple": 1, "entry_time": datetime(2025, 1, 1), "exit_time": datetime(2025, 1, 1, 0, 5)},
        {"pnl": -4, "r_multiple": -0.4, "entry_time": datetime(2025, 1, 1), "exit_time": datetime(2025, 1, 1, 0, 5)},
    ]
    metrics = service.calculate_metrics(trades, [10000, 10010, 10006])

    assert metrics["expectancy"] == 3


def test_research_monte_carlo():
    service = BacktestService()
    trades = [{"pnl": 10}, {"pnl": -5}, {"pnl": 15}, {"pnl": -3}]
    result = service.monte_carlo(trades, initial_equity=10000, iterations=100)

    assert result["best_equity"] >= result["median_equity"]
    assert 0 <= result["probability_of_ruin"] <= 100


def test_research_walk_forward_split():
    service = BacktestService()
    candles = make_research_candles()
    result = service.walk_forward(candles, service.merge_config({"fee_percent": 0}))

    assert "train_profit_factor" in result
    assert "test_profit_factor" in result
    assert "is_overfitted" in result


def test_research_optimizer():
    service = BacktestService()
    candles = make_research_candles()
    result = service.optimize(candles, service.merge_config({"fee_percent": 0}), max_results=3)

    assert len(result["best_parameter_sets"]) <= 3
    assert result["best_parameter_sets"][0]["config"]["ema_fast"] < result["best_parameter_sets"][0]["config"]["ema_slow"]
