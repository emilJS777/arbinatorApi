from datetime import datetime

from src import db
from src.Futures.EquityCurveModel import EquityCurvePoint
from src.Futures.FuturesTradeModel import FuturesTrade
from src.Futures.MetricsService import INITIAL_EQUITY, MetricsService
from src.PaperTrading.PaperPositionModel import PaperPosition
from src.Signal.TradeSignalModel import TradeSignal
from src.Risk.RiskManager import RiskManager


class FuturesPaperTradingService:
    def __init__(self, risk_manager=None):
        self.risk_manager = risk_manager or RiskManager()
        self.metrics_service = MetricsService()

    def liquidation_price(self, side: str, entry_price: float, leverage: float) -> float:
        buffer = 1 / max(leverage, 1)
        if side == "short":
            return entry_price * (1 + buffer)
        return entry_price * (1 - buffer)

    def open_position(self, signal: TradeSignal, risk_config: dict | None = None, equity: float | None = None):
        leverage = 2
        risk_per_trade_percent = 1
        equity = equity or self.metrics_service.current_equity() or INITIAL_EQUITY
        risk_usdt = equity * (risk_per_trade_percent / 100)
        stop_distance = abs(signal.entry_price - signal.stop_loss_price)
        amount = risk_usdt / stop_distance if stop_distance > 0 else 0
        margin = (signal.entry_price * amount) / leverage

        ok, reason, limits = self.risk_manager.validate_futures_signal(signal, margin, leverage, risk_config)
        if not ok:
            signal.status = "rejected"
            db.session.commit()
            return False, {"reason": reason, "limits": limits}

        signal.status = "executed"
        position = PaperPosition(
            exchange=signal.exchange,
            symbol=signal.symbol,
            side=signal.side,
            entry_price=signal.entry_price,
            amount=amount,
            leverage=leverage,
            margin=margin,
            margin_mode="isolated",
            liquidation_price=self.liquidation_price(signal.side, signal.entry_price, leverage),
            take_profit_price=signal.take_profit_price,
            stop_loss_price=signal.stop_loss_price,
            strategy_type="futures_trend_pullback",
            status="open",
        )
        db.session.add(position)
        db.session.commit()
        return True, position

    def evaluate_exit(self, position: PaperPosition, high: float, low: float):
        if position.status != "open":
            return None
        if position.side == "long":
            if low <= position.stop_loss_price:
                return self.close_position(position, position.stop_loss_price, "stop_loss")
            if high >= position.take_profit_price:
                return self.close_position(position, position.take_profit_price, "take_profit")
        if position.side == "short":
            if high >= position.stop_loss_price:
                return self.close_position(position, position.stop_loss_price, "stop_loss")
            if low <= position.take_profit_price:
                return self.close_position(position, position.take_profit_price, "take_profit")
        return None

    def close_position(self, position: PaperPosition, exit_price: float, reason: str):
        pnl = self.pnl(position.side, position.entry_price, exit_price, position.amount)
        fee = (position.entry_price * position.amount + exit_price * position.amount) * 0.0004
        realized = pnl - fee
        position.status = "closed"
        position.exit_price = exit_price
        position.exit_reason = reason
        position.realized_pnl = realized
        position.unrealized_pnl = 0
        position.closed_at = datetime.utcnow()
        trade = FuturesTrade(
            position_id=position.id,
            symbol=position.symbol,
            side=position.side,
            entry_price=position.entry_price,
            exit_price=exit_price,
            amount=position.amount,
            leverage=position.leverage,
            margin=position.margin,
            realized_pnl=realized,
            fee=fee,
            exit_reason=reason,
            opened_at=position.opened_at,
            closed_at=position.closed_at,
        )
        db.session.add(trade)
        db.session.flush()
        equity = INITIAL_EQUITY + sum(item.realized_pnl for item in FuturesTrade.query.all())
        db.session.add(EquityCurvePoint(equity=equity, realized_pnl=sum(item.realized_pnl for item in FuturesTrade.query.all()), unrealized_pnl=0))
        db.session.commit()
        return trade

    def pnl(self, side: str, entry: float, exit: float, amount: float) -> float:
        if side == "short":
            return (entry - exit) * amount
        return (exit - entry) * amount
