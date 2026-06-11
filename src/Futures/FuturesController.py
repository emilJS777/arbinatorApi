from src.__Parents.Controller import Controller
from src.__Parents.Response import Response
from src.Futures.EquityCurveModel import EquityCurvePoint
from src.Futures.FuturesTradeModel import FuturesTrade
from src.Futures.MetricsService import MetricsService
from src.PaperTrading.PaperPositionModel import PaperPosition
from src.PaperTrading.PaperTradingService import paper_position_to_dict, signal_to_dict
from src.Signal.TradeSignalModel import TradeSignal


class FuturesMetricsController(Controller, Response):
    def get(self):
        return self.response_ok(MetricsService().metrics())


class FuturesEquityController(Controller, Response):
    def get(self):
        points = EquityCurvePoint.query.order_by(EquityCurvePoint.timestamp.asc()).all()
        return self.response_ok([{
            "id": point.id,
            "equity": point.equity,
            "realized_pnl": point.realized_pnl,
            "unrealized_pnl": point.unrealized_pnl,
            "timestamp": point.timestamp,
        } for point in points])


class FuturesSignalController(Controller, Response):
    def get(self):
        signals = TradeSignal.query.filter_by(strategy_type="futures_trend_pullback").order_by(TradeSignal.id.desc()).all()
        return self.response_ok([signal_to_dict(signal) for signal in signals])


class FuturesPositionController(Controller, Response):
    def get(self):
        positions = PaperPosition.query.filter_by(strategy_type="futures_trend_pullback").order_by(PaperPosition.id.desc()).all()
        return self.response_ok([paper_position_to_dict(position) for position in positions])


class FuturesTradeController(Controller, Response):
    def get(self):
        trades = FuturesTrade.query.order_by(FuturesTrade.closed_at.desc()).all()
        return self.response_ok([{
            "id": trade.id,
            "position_id": trade.position_id,
            "signal_id": trade.signal_id,
            "symbol": trade.symbol,
            "side": trade.side,
            "entry_price": trade.entry_price,
            "exit_price": trade.exit_price,
            "amount": trade.amount,
            "leverage": trade.leverage,
            "margin": trade.margin,
            "realized_pnl": trade.realized_pnl,
            "fee": trade.fee,
            "exit_reason": trade.exit_reason,
            "opened_at": trade.opened_at,
            "closed_at": trade.closed_at,
        } for trade in trades])
