from dataclasses import dataclass
from datetime import datetime
import itertools
import math
import random
import statistics

import ccxt

from src import db
from src.Futures.BacktestModel import BacktestRun, BacktestTrade
from src.Futures.IndicatorEngine import IndicatorEngine
from src.__Parents.Response import Response


DEFAULT_BACKTEST_CONFIG = {
    "ema_fast": 20,
    "ema_slow": 50,
    "rsi_period": 14,
    "rsi_long_max": 60,
    "rsi_short_min": 40,
    "take_profit_percent": 0.8,
    "stop_loss_percent": 0.5,
    "risk_per_trade_percent": 1,
    "leverage": 2,
    "initial_equity": 10000,
    "pullback_tolerance_percent": 0.15,
    "fee_percent": 0.08,
}


@dataclass
class BacktestCandle:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0


class BacktestService(Response):
    def __init__(self, indicator_engine=None):
        self.indicator_engine = indicator_engine or IndicatorEngine()

    def run_from_payload(self, body: dict):
        candles = self.parse_candles(body.get("candles")) if body.get("candles") else self.fetch_ohlcv(
            body.get("exchange", "binance"),
            body.get("symbol", "BTCUSDT"),
            body.get("timeframe", "5m"),
            body.get("start_date"),
            body.get("end_date"),
        )
        config = self.merge_config(body.get("strategy_config") or {})
        result = self.simulate(candles, config)
        run = self.persist_run(body, config, result)
        return self.response_ok(self.run_to_dict(run, include_details=True))

    def merge_config(self, config: dict) -> dict:
        merged = dict(DEFAULT_BACKTEST_CONFIG)
        merged.update(config or {})
        return merged

    def fetch_ohlcv(self, exchange_id: str, symbol: str, timeframe: str, start_date=None, end_date=None):
        exchange_class = getattr(ccxt, exchange_id)
        exchange = exchange_class({"enableRateLimit": True})
        ccxt_symbol = self.to_ccxt_symbol(symbol)
        since = int(datetime.fromisoformat(start_date).timestamp() * 1000) if start_date else None
        end_ms = int(datetime.fromisoformat(end_date).timestamp() * 1000) if end_date else None
        rows = []
        while True:
            batch = exchange.fetch_ohlcv(ccxt_symbol, timeframe=timeframe, since=since, limit=1000)
            if not batch:
                break
            for item in batch:
                if end_ms and item[0] > end_ms:
                    break
                rows.append(item)
            last_ts = batch[-1][0]
            if end_ms and last_ts >= end_ms:
                break
            if since and last_ts <= since:
                break
            since = last_ts + 1
            if len(batch) < 1000:
                break
        return self.parse_candles(rows)

    def to_ccxt_symbol(self, symbol: str) -> str:
        if "/" in symbol:
            return symbol
        if symbol.endswith("USDT"):
            return f"{symbol[:-4]}/USDT"
        return symbol

    def parse_candles(self, rows: list) -> list[BacktestCandle]:
        candles = []
        for item in rows:
            if isinstance(item, dict):
                ts = item["timestamp"]
                timestamp = datetime.fromisoformat(ts) if isinstance(ts, str) else ts
                candles.append(BacktestCandle(timestamp, float(item["open"]), float(item["high"]), float(item["low"]), float(item["close"]), float(item.get("volume", 0))))
            else:
                candles.append(BacktestCandle(datetime.utcfromtimestamp(item[0] / 1000), float(item[1]), float(item[2]), float(item[3]), float(item[4]), float(item[5] if len(item) > 5 else 0)))
        return candles

    def simulate(self, candles: list[BacktestCandle], config: dict) -> dict:
        if len(candles) < max(int(config["ema_slow"]), int(config["rsi_period"])) + 2:
            return self.build_result([], [float(config["initial_equity"])], config)

        equity = float(config["initial_equity"])
        equity_curve = [equity]
        trades = []
        open_position = None

        for index in range(len(candles)):
            candle = candles[index]
            if open_position:
                trade = self.check_exit(open_position, candle, config)
                if trade:
                    equity += trade["pnl"]
                    trades.append(trade)
                    equity_curve.append(equity)
                    open_position = None
                continue

            signal = self.signal_at(candles[:index + 1], config)
            if not signal:
                continue
            open_position = self.open_position(signal, equity, config)

        if open_position:
            last = candles[-1]
            if last.timestamp > open_position["entry_time"]:
                trade = self.close_open_position(open_position, last.close, last.timestamp, "end_of_data", config)
                equity += trade["pnl"]
                trades.append(trade)
                equity_curve.append(equity)

        return self.build_result(trades, equity_curve, config)

    def signal_at(self, visible_candles: list[BacktestCandle], config: dict):
        if len(visible_candles) < max(int(config["ema_slow"]), int(config["rsi_period"]) + 1):
            return None
        closes = [candle.close for candle in visible_candles]
        ema_fast = self.indicator_engine.ema(closes, int(config["ema_fast"]))[-1]
        ema_slow = self.indicator_engine.ema(closes, int(config["ema_slow"]))[-1]
        rsi = self.indicator_engine.rsi(closes, int(config["rsi_period"]))[-1]
        candle = visible_candles[-1]
        if ema_fast is None or ema_slow is None or rsi is None:
            return None
        touches = candle.low <= ema_fast <= candle.high or abs(candle.close - ema_fast) <= candle.close * (float(config["pullback_tolerance_percent"]) / 100)
        if not touches:
            return None
        if ema_fast > ema_slow and rsi < float(config["rsi_long_max"]):
            return {"side": "long", "entry_price": candle.close, "entry_time": candle.timestamp}
        if ema_fast < ema_slow and rsi > float(config["rsi_short_min"]):
            return {"side": "short", "entry_price": candle.close, "entry_time": candle.timestamp}
        return None

    def open_position(self, signal: dict, equity: float, config: dict) -> dict:
        entry = signal["entry_price"]
        sl_percent = float(config["stop_loss_percent"]) / 100
        tp_percent = float(config["take_profit_percent"]) / 100
        if signal["side"] == "short":
            stop_loss = entry * (1 + sl_percent)
            take_profit = entry * (1 - tp_percent)
        else:
            stop_loss = entry * (1 - sl_percent)
            take_profit = entry * (1 + tp_percent)
        risk_amount = equity * (float(config["risk_per_trade_percent"]) / 100)
        stop_distance = abs(entry - stop_loss)
        amount = risk_amount / stop_distance if stop_distance > 0 else 0
        return {**signal, "stop_loss": stop_loss, "take_profit": take_profit, "amount": amount, "risk_amount": risk_amount}

    def check_exit(self, position: dict, candle: BacktestCandle, config: dict):
        if position["side"] == "long":
            if candle.low <= position["stop_loss"]:
                return self.close_open_position(position, position["stop_loss"], candle.timestamp, "stop_loss", config)
            if candle.high >= position["take_profit"]:
                return self.close_open_position(position, position["take_profit"], candle.timestamp, "take_profit", config)
        else:
            if candle.high >= position["stop_loss"]:
                return self.close_open_position(position, position["stop_loss"], candle.timestamp, "stop_loss", config)
            if candle.low <= position["take_profit"]:
                return self.close_open_position(position, position["take_profit"], candle.timestamp, "take_profit", config)
        return None

    def close_open_position(self, position: dict, exit_price: float, exit_time: datetime, reason: str, config: dict):
        direction = -1 if position["side"] == "short" else 1
        gross = (exit_price - position["entry_price"]) * position["amount"] * direction
        notional = (position["entry_price"] + exit_price) * position["amount"]
        fee = notional * (float(config["fee_percent"]) / 100)
        pnl = gross - fee
        pnl_percent = (pnl / max(position["risk_amount"], 1e-9)) * 100
        return {
            "entry_time": position["entry_time"],
            "exit_time": exit_time,
            "side": position["side"],
            "entry_price": position["entry_price"],
            "exit_price": exit_price,
            "pnl": pnl,
            "pnl_percent": pnl_percent,
            "r_multiple": pnl / max(position["risk_amount"], 1e-9),
            "reason": reason,
        }

    def build_result(self, trades: list[dict], equity_curve: list[float], config: dict) -> dict:
        metrics = self.calculate_metrics(trades, equity_curve)
        return {
            "trades": trades,
            "metrics": metrics,
            "equity_curve": equity_curve,
            "drawdown_curve": self.drawdown_curve(equity_curve),
            "monthly_returns": self.monthly_returns(trades),
            "config": config,
        }

    def calculate_metrics(self, trades: list[dict], equity_curve: list[float]) -> dict:
        pnls = [trade["pnl"] for trade in trades]
        wins = [pnl for pnl in pnls if pnl > 0]
        losses = [pnl for pnl in pnls if pnl < 0]
        total_pnl = sum(pnls)
        gross_win = sum(wins)
        gross_loss = abs(sum(losses))
        returns = [pnl / 10000 for pnl in pnls]
        downside = [item for item in returns if item < 0]
        max_dd = max(self.drawdown_curve(equity_curve) or [0])
        return {
            "trades_count": len(trades),
            "total_pnl": total_pnl,
            "win_rate": (len(wins) / len(trades) * 100) if trades else 0,
            "profit_factor": (gross_win / gross_loss) if gross_loss else (gross_win if gross_win else 0),
            "max_drawdown": max_dd,
            "sharpe_ratio": self.ratio(returns),
            "sortino_ratio": self.ratio(returns, downside_only=True),
            "expectancy": (sum(pnls) / len(pnls)) if pnls else 0,
            "recovery_factor": (total_pnl / max_dd) if max_dd else 0,
            "average_holding_minutes": self.average_holding_minutes(trades),
            "longest_losing_streak": self.longest_streak(pnls, win=False),
            "longest_winning_streak": self.longest_streak(pnls, win=True),
            "average_r_multiple": statistics.mean([trade["r_multiple"] for trade in trades]) if trades else 0,
        }

    def ratio(self, returns: list[float], downside_only=False) -> float:
        values = [item for item in returns if item < 0] if downside_only else returns
        if not returns or len(values) < 2:
            return 0
        denom = statistics.pstdev(values)
        return (statistics.mean(returns) / denom * math.sqrt(252)) if denom else 0

    def drawdown_curve(self, equity_curve: list[float]) -> list[float]:
        peak = equity_curve[0] if equity_curve else 0
        result = []
        for equity in equity_curve:
            peak = max(peak, equity)
            result.append(peak - equity)
        return result

    def monthly_returns(self, trades: list[dict]) -> dict:
        result = {}
        for trade in trades:
            key = trade["exit_time"].strftime("%Y-%m")
            result[key] = result.get(key, 0) + trade["pnl"]
        return result

    def average_holding_minutes(self, trades: list[dict]) -> float:
        if not trades:
            return 0
        durations = [(trade["exit_time"] - trade["entry_time"]).total_seconds() / 60 for trade in trades]
        return statistics.mean(durations)

    def longest_streak(self, pnls: list[float], win: bool) -> int:
        best = current = 0
        for pnl in pnls:
            matches = pnl > 0 if win else pnl < 0
            current = current + 1 if matches else 0
            best = max(best, current)
        return best

    def monte_carlo(self, trades: list[dict], initial_equity=10000, iterations=1000, ruin_threshold_percent=30) -> dict:
        pnls = [trade["pnl"] for trade in trades]
        if not pnls:
            return {"worst_drawdown": 0, "median_equity": initial_equity, "probability_of_ruin": 0, "best_equity": initial_equity, "average_equity": initial_equity, "sample_equity_curves": []}
        finals = []
        drawdowns = []
        ruined = 0
        samples = []
        rng = random.Random(42)
        for iteration in range(iterations):
            sequence = pnls[:]
            rng.shuffle(sequence)
            equity = initial_equity
            curve = [equity]
            for pnl in sequence:
                equity += pnl
                curve.append(equity)
            finals.append(equity)
            drawdowns.append(max(self.drawdown_curve(curve)))
            if min(curve) <= initial_equity * (1 - ruin_threshold_percent / 100):
                ruined += 1
            if iteration < 20:
                samples.append(curve)
        return {
            "worst_drawdown": max(drawdowns),
            "median_equity": statistics.median(finals),
            "probability_of_ruin": ruined / iterations * 100,
            "best_equity": max(finals),
            "average_equity": statistics.mean(finals),
            "sample_equity_curves": samples,
        }

    def walk_forward(self, candles: list[BacktestCandle], config: dict) -> dict:
        split = int(len(candles) * 0.7)
        train = self.simulate(candles[:split], config)
        test_seed = candles[max(0, split - int(config["ema_slow"])):]
        test = self.simulate(test_seed, config)
        train_pf = train["metrics"]["profit_factor"]
        test_pf = test["metrics"]["profit_factor"]
        return {
            "train_profit_factor": train_pf,
            "test_profit_factor": test_pf,
            "is_overfitted": train_pf > 1.2 and test_pf < 1,
            "train": train["metrics"],
            "test": test["metrics"],
        }

    def optimize(self, candles: list[BacktestCandle], base_config: dict, max_results=10) -> dict:
        ranges = {
            "ema_fast": [10, 20, 30],
            "ema_slow": [40, 50, 100],
            "rsi_long_max": [50, 60, 70],
            "take_profit_percent": [0.3, 0.8, 1.5],
            "stop_loss_percent": [0.2, 0.5, 1.0],
            "risk_per_trade_percent": [0.5, 1.0, 2.0],
        }
        results = []
        for values in itertools.product(*ranges.values()):
            config = dict(base_config)
            config.update(dict(zip(ranges.keys(), values)))
            if config["ema_fast"] >= config["ema_slow"]:
                continue
            simulation = self.simulate(candles, config)
            metrics = simulation["metrics"]
            results.append({"config": config, "metrics": metrics})
        results.sort(key=lambda item: (item["metrics"]["profit_factor"], item["metrics"]["expectancy"]), reverse=True)
        return {"best_parameter_sets": results[:max_results]}

    def persist_run(self, body: dict, config: dict, result: dict) -> BacktestRun:
        metrics = result["metrics"]
        run = BacktestRun(
            exchange=body.get("exchange", "binance"),
            symbol=body.get("symbol", "BTCUSDT"),
            timeframe=body.get("timeframe", "5m"),
            start_date=datetime.fromisoformat(body["start_date"]) if body.get("start_date") else None,
            end_date=datetime.fromisoformat(body["end_date"]) if body.get("end_date") else None,
            config_json=config,
            equity_curve_json=result["equity_curve"],
            drawdown_curve_json=result["drawdown_curve"],
            monthly_returns_json=result["monthly_returns"],
            monte_carlo_json=self.monte_carlo(result["trades"], config["initial_equity"], int(body.get("monte_carlo_iterations", 1000))),
            walk_forward_json=self.walk_forward(self.parse_candles(body.get("candles") or []), config) if body.get("candles") else {},
            optimization_json=self.optimize(self.parse_candles(body.get("candles") or []), config) if body.get("run_optimization") and body.get("candles") else {},
            **metrics,
        )
        db.session.add(run)
        db.session.flush()
        for trade in result["trades"]:
            db.session.add(BacktestTrade(backtest_run_id=run.id, **trade))
        db.session.commit()
        return run

    def run_to_dict(self, run: BacktestRun, include_details=False) -> dict:
        data = {
            "id": run.id,
            "exchange": run.exchange,
            "symbol": run.symbol,
            "timeframe": run.timeframe,
            "start_date": run.start_date,
            "end_date": run.end_date,
            "trades_count": run.trades_count,
            "total_pnl": run.total_pnl,
            "win_rate": run.win_rate,
            "profit_factor": run.profit_factor,
            "max_drawdown": run.max_drawdown,
            "sharpe_ratio": run.sharpe_ratio,
            "sortino_ratio": run.sortino_ratio,
            "expectancy": run.expectancy,
            "recovery_factor": run.recovery_factor,
            "average_holding_minutes": run.average_holding_minutes,
            "longest_losing_streak": run.longest_losing_streak,
            "longest_winning_streak": run.longest_winning_streak,
            "average_r_multiple": run.average_r_multiple,
            "created_at": run.created_at,
        }
        if include_details:
            trades = BacktestTrade.query.filter_by(backtest_run_id=run.id).order_by(BacktestTrade.entry_time.asc()).all()
            data.update({
                "config": run.config_json or {},
                "equity_curve": run.equity_curve_json or [],
                "drawdown_curve": run.drawdown_curve_json or [],
                "monthly_returns": run.monthly_returns_json or {},
                "monte_carlo": run.monte_carlo_json or {},
                "walk_forward": run.walk_forward_json or {},
                "optimization": run.optimization_json or {},
                "trades": [self.trade_to_dict(trade) for trade in trades],
            })
        return data

    def trade_to_dict(self, trade: BacktestTrade) -> dict:
        return {
            "id": trade.id,
            "entry_time": trade.entry_time,
            "exit_time": trade.exit_time,
            "side": trade.side,
            "entry_price": trade.entry_price,
            "exit_price": trade.exit_price,
            "pnl": trade.pnl,
            "pnl_percent": trade.pnl_percent,
            "r_multiple": trade.r_multiple,
            "reason": trade.reason,
        }
