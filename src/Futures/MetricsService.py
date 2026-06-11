from src.Futures.EquityCurveModel import EquityCurvePoint
from src.Futures.FuturesTradeModel import FuturesTrade
from src.PaperTrading.PaperPositionModel import PaperPosition


INITIAL_EQUITY = 10000


class MetricsService:
    def closed_trades(self):
        return FuturesTrade.query.order_by(FuturesTrade.closed_at.asc()).all()

    def current_equity(self) -> float:
        realized = sum(trade.realized_pnl for trade in self.closed_trades())
        unrealized = sum(position.unrealized_pnl or 0 for position in PaperPosition.query.filter_by(status="open").all())
        return INITIAL_EQUITY + realized + unrealized

    def metrics(self) -> dict:
        trades = self.closed_trades()
        wins = [trade.realized_pnl for trade in trades if trade.realized_pnl > 0]
        losses = [trade.realized_pnl for trade in trades if trade.realized_pnl < 0]
        total_pnl = sum(trade.realized_pnl for trade in trades)
        gross_win = sum(wins)
        gross_loss = abs(sum(losses))
        return {
            "total_trades": len(trades),
            "win_rate": (len(wins) / len(trades) * 100) if trades else 0,
            "average_win": (gross_win / len(wins)) if wins else 0,
            "average_loss": (sum(losses) / len(losses)) if losses else 0,
            "profit_factor": (gross_win / gross_loss) if gross_loss else (gross_win if gross_win else 0),
            "total_pnl": total_pnl,
            "max_drawdown": self.max_drawdown(),
            "current_equity": self.current_equity(),
        }

    def max_drawdown(self) -> float:
        points = EquityCurvePoint.query.order_by(EquityCurvePoint.timestamp.asc()).all()
        if not points:
            return 0
        peak = points[0].equity
        max_dd = 0
        for point in points:
            peak = max(peak, point.equity)
            drawdown = peak - point.equity
            max_dd = max(max_dd, drawdown)
        return max_dd

    def record_equity(self):
        point = EquityCurvePoint(equity=self.current_equity(), realized_pnl=sum(trade.realized_pnl for trade in self.closed_trades()), unrealized_pnl=0)
        from src import db
        db.session.add(point)
        db.session.commit()
        return point
