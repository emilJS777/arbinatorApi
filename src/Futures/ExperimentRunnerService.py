from datetime import datetime, timedelta
import itertools
import statistics

from src import db
from src.Futures.BacktestModel import StrategyCandidate
from src.Futures.BacktestService import BacktestService
from src.__Parents.Response import Response


class ExperimentRunnerService(Response):
    symbols = ["BTCUSDT", "ETHUSDT"]
    timeframes = ["5m", "15m"]
    periods = [30, 90, 180, 365]

    def __init__(self, backtest_service=None):
        self.backtest_service = backtest_service or BacktestService()

    def parameter_grid(self):
        ranges = {
            "ema_fast": list(range(10, 31, 5)),
            "ema_slow": list(range(40, 101, 10)),
            "rsi_threshold": list(range(50, 71, 5)),
            "take_profit_percent": [0.3, 0.6, 0.9, 1.2, 1.5],
            "stop_loss_percent": [0.2, 0.4, 0.6, 0.8, 1.0],
            "risk_per_trade_percent": [0.5, 1.0, 1.5, 2.0],
        }
        for values in itertools.product(*ranges.values()):
            raw = dict(zip(ranges.keys(), values))
            if raw["ema_fast"] >= raw["ema_slow"]:
                continue
            yield self.normalize_parameters(raw)

    def normalize_parameters(self, params: dict) -> dict:
        threshold = float(params["rsi_threshold"])
        return {
            "ema_fast": int(params["ema_fast"]),
            "ema_slow": int(params["ema_slow"]),
            "rsi_period": 14,
            "rsi_long_max": threshold,
            "rsi_short_min": 100 - threshold,
            "rsi_threshold": threshold,
            "take_profit_percent": float(params["take_profit_percent"]),
            "stop_loss_percent": float(params["stop_loss_percent"]),
            "risk_per_trade_percent": float(params["risk_per_trade_percent"]),
        }

    def run_from_payload(self, body: dict):
        result = self.run(
            exchange=body.get("exchange", "binance"),
            symbols=body.get("symbols") or self.symbols,
            timeframes=body.get("timeframes") or self.timeframes,
            periods=body.get("periods") or self.periods,
            max_combinations=int(body.get("max_combinations", 2400)),
            candles_by_key=body.get("candles_by_key"),
        )
        return self.response_ok(result)

    def run(self, exchange="binance", symbols=None, timeframes=None, periods=None, max_combinations=2400, candles_by_key=None):
        symbols = symbols or self.symbols
        timeframes = timeframes or self.timeframes
        periods = periods or self.periods
        created = []
        runs = 0
        candidates_query_start = datetime.utcnow()

        for symbol in symbols:
            for timeframe in timeframes:
                for period_days in periods:
                    candles = self.resolve_candles(exchange, symbol, timeframe, period_days, candles_by_key)
                    if len(candles) < 80:
                        continue
                    for params in self.parameter_grid():
                        if runs >= max_combinations:
                            break
                        config = self.backtest_service.merge_config(params)
                        simulation = self.backtest_service.simulate(candles, config)
                        walk_forward = self.backtest_service.walk_forward(candles, config)
                        monte_carlo = self.backtest_service.monte_carlo(simulation["trades"], config["initial_equity"], iterations=200)
                        score = self.score(simulation["metrics"], walk_forward, config)
                        rejection_reasons = self.rejection_reasons(simulation["metrics"], walk_forward, config)
                        candidate = StrategyCandidate(
                            exchange=exchange,
                            symbol=symbol,
                            timeframe=timeframe,
                            period_days=int(period_days),
                            parameters=params,
                            profit_factor=simulation["metrics"]["profit_factor"],
                            win_rate=simulation["metrics"]["win_rate"],
                            expectancy=simulation["metrics"]["expectancy"],
                            max_drawdown=simulation["metrics"]["max_drawdown"],
                            max_drawdown_percent=self.drawdown_percent(simulation["metrics"], config),
                            sharpe=simulation["metrics"]["sharpe_ratio"],
                            trades_count=simulation["metrics"]["trades_count"],
                            stability_score=score["stability_score"],
                            profit_factor_score=score["profit_factor_score"],
                            drawdown_score=score["drawdown_score"],
                            sharpe_score=score["sharpe_score"],
                            walk_forward_score=score["walk_forward_score"],
                            rejection_reasons=rejection_reasons,
                            equity_curve_json=simulation["equity_curve"],
                            drawdown_curve_json=simulation["drawdown_curve"],
                            monte_carlo_json=monte_carlo,
                            walk_forward_json=walk_forward,
                        )
                        db.session.add(candidate)
                        created.append(candidate)
                        runs += 1
                    db.session.commit()

        all_created = StrategyCandidate.query.filter(StrategyCandidate.created_at >= candidates_query_start).all()
        top = sorted(
            [item for item in all_created if not item.rejection_reasons],
            key=lambda item: item.stability_score,
            reverse=True,
        )[:50]
        return {
            "runs": runs,
            "top_candidates": [self.candidate_to_dict(item) for item in top],
            "heatmaps": self.heatmaps(all_created),
            "summary": self.summary(all_created),
        }

    def resolve_candles(self, exchange, symbol, timeframe, period_days, candles_by_key=None):
        key = f"{symbol}:{timeframe}:{period_days}"
        if candles_by_key and key in candles_by_key:
            return self.backtest_service.parse_candles(candles_by_key[key])
        end = datetime.utcnow()
        start = end - timedelta(days=int(period_days))
        return self.backtest_service.fetch_ohlcv(exchange, symbol, timeframe, start.isoformat(), end.isoformat())

    def score(self, metrics: dict, walk_forward: dict, config: dict) -> dict:
        pf = float(metrics["profit_factor"])
        dd_pct = self.drawdown_percent(metrics, config)
        sharpe = float(metrics["sharpe_ratio"])
        train_pf = float(walk_forward.get("train_profit_factor") or 0)
        test_pf = float(walk_forward.get("test_profit_factor") or 0)
        degradation = (test_pf / train_pf) if train_pf > 0 else 0

        profit_factor_score = min(100, max(0, (pf - 1) / 1.5 * 100))
        drawdown_score = max(0, 100 - dd_pct * 5)
        sharpe_score = min(100, max(0, sharpe / 2 * 100))
        walk_forward_score = min(100, max(0, degradation * 100))
        stability_score = (
            profit_factor_score * 0.35 +
            drawdown_score * 0.25 +
            sharpe_score * 0.2 +
            walk_forward_score * 0.2
        )
        return {
            "profit_factor_score": profit_factor_score,
            "drawdown_score": drawdown_score,
            "sharpe_score": sharpe_score,
            "walk_forward_score": walk_forward_score,
            "stability_score": stability_score,
        }

    def rejection_reasons(self, metrics: dict, walk_forward: dict, config: dict) -> list[str]:
        reasons = []
        if metrics["profit_factor"] <= 1.2:
            reasons.append("profit_factor_below_1_2")
        if metrics["expectancy"] <= 0:
            reasons.append("negative_expectancy")
        if self.drawdown_percent(metrics, config) >= 20:
            reasons.append("high_drawdown")
        if metrics["trades_count"] <= 100:
            reasons.append("low_trade_count")
        train_pf = walk_forward.get("train_profit_factor") or 0
        test_pf = walk_forward.get("test_profit_factor") or 0
        if train_pf > 1.2 and (test_pf < 1 or test_pf < train_pf * 0.6):
            reasons.append("overfitted")
        return reasons

    def drawdown_percent(self, metrics: dict, config: dict) -> float:
        initial_equity = float(config.get("initial_equity", 10000))
        return (float(metrics["max_drawdown"]) / initial_equity) * 100 if initial_equity else 0

    def candidates(self, limit=50):
        rows = StrategyCandidate.query.order_by(StrategyCandidate.stability_score.desc()).limit(limit).all()
        return self.response_ok([self.candidate_to_dict(item) for item in rows])

    def heatmap_response(self):
        rows = StrategyCandidate.query.order_by(StrategyCandidate.created_at.desc()).limit(5000).all()
        return self.response_ok(self.heatmaps(rows))

    def heatmaps(self, rows: list[StrategyCandidate]) -> dict:
        return {
            "ema_combinations": self.aggregate(rows, lambda item: f"{item.parameters.get('ema_fast')}/{item.parameters.get('ema_slow')}"),
            "tp_sl_combinations": self.aggregate(rows, lambda item: f"{item.parameters.get('take_profit_percent')}/{item.parameters.get('stop_loss_percent')}"),
            "risk_combinations": self.aggregate(rows, lambda item: str(item.parameters.get("risk_per_trade_percent"))),
        }

    def aggregate(self, rows, key_fn):
        buckets = {}
        for item in rows:
            key = key_fn(item)
            buckets.setdefault(key, []).append(item.stability_score)
        return [{"key": key, "average_stability_score": statistics.mean(values), "count": len(values)} for key, values in buckets.items()]

    def summary(self, rows):
        return {
            "total": len(rows),
            "accepted": len([item for item in rows if not item.rejection_reasons]),
            "rejected": len([item for item in rows if item.rejection_reasons]),
        }

    def candidate_to_dict(self, item: StrategyCandidate) -> dict:
        return {
            "id": item.id,
            "exchange": item.exchange,
            "symbol": item.symbol,
            "timeframe": item.timeframe,
            "period_days": item.period_days,
            "parameters": item.parameters,
            "profit_factor": item.profit_factor,
            "win_rate": item.win_rate,
            "expectancy": item.expectancy,
            "max_drawdown": item.max_drawdown,
            "max_drawdown_percent": item.max_drawdown_percent,
            "sharpe": item.sharpe,
            "trades_count": item.trades_count,
            "stability_score": item.stability_score,
            "profit_factor_score": item.profit_factor_score,
            "drawdown_score": item.drawdown_score,
            "sharpe_score": item.sharpe_score,
            "walk_forward_score": item.walk_forward_score,
            "rejection_reasons": item.rejection_reasons or [],
            "equity_curve": item.equity_curve_json or [],
            "drawdown_curve": item.drawdown_curve_json or [],
            "monte_carlo": item.monte_carlo_json or {},
            "walk_forward": item.walk_forward_json or {},
            "created_at": item.created_at,
        }
