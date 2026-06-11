from datetime import datetime

from src import db
from src.PaperTrading.PaperOrderModel import PaperOrder
from src.PaperTrading.PaperPositionModel import PaperPosition
from src.Signal.TradeSignalModel import TradeSignal
from src.Socket.EventPublisher import EventPublisher
from src.Strategy.StrategyConfigModel import StrategyConfig
from src.Risk.RiskManager import RiskManager
from src.__Parents.Response import Response


def signal_to_dict(signal: TradeSignal) -> dict:
    return {
        "id": signal.id,
        "strategy_config_id": signal.strategy_config_id,
        "symbol": signal.symbol,
        "exchange": signal.exchange,
        "side": signal.side,
        "entry_price": signal.entry_price,
        "take_profit_price": signal.take_profit_price,
        "stop_loss_price": signal.stop_loss_price,
        "confidence": signal.confidence,
        "reason": signal.reason,
        "status": signal.status,
        "strategy_type": signal.strategy_type,
        "buy_exchange": signal.buy_exchange,
        "sell_exchange": signal.sell_exchange,
        "buy_price": signal.buy_price,
        "sell_price": signal.sell_price,
        "gross_spread_percent": signal.gross_spread_percent,
        "net_profit_percent": signal.net_profit_percent,
        "expected_profit_usdt": signal.expected_profit_usdt,
        "config_snapshot": signal.config_snapshot or {},
        "dedupe_key": signal.dedupe_key,
        "created_at": signal.created_at,
    }


def paper_order_to_dict(order: PaperOrder) -> dict:
    return {
        "id": order.id,
        "signal_id": order.signal_id,
        "exchange": order.exchange,
        "symbol": order.symbol,
        "side": order.side,
        "order_type": order.order_type,
        "price": order.price,
        "amount": order.amount,
        "status": order.status,
        "filled_price": order.filled_price,
        "filled_amount": order.filled_amount,
        "fee": order.fee,
        "created_at": order.created_at,
        "filled_at": order.filled_at,
    }


def paper_position_to_dict(position: PaperPosition) -> dict:
    return {
        "id": position.id,
        "strategy_config_id": position.strategy_config_id,
        "exchange": position.exchange,
        "symbol": position.symbol,
        "side": position.side,
        "entry_price": position.entry_price,
        "amount": position.amount,
        "leverage": position.leverage,
        "margin": position.margin,
        "margin_mode": position.margin_mode,
        "liquidation_price": position.liquidation_price,
        "take_profit_price": position.take_profit_price,
        "stop_loss_price": position.stop_loss_price,
        "exit_price": position.exit_price,
        "exit_reason": position.exit_reason,
        "strategy_type": position.strategy_type,
        "unrealized_pnl": position.unrealized_pnl,
        "realized_pnl": position.realized_pnl,
        "status": position.status,
        "opened_at": position.opened_at,
        "closed_at": position.closed_at,
    }


class PaperTradingService(Response):
    def __init__(self, risk_manager=None, publisher=None):
        self.risk_manager = risk_manager or RiskManager()
        self.publisher = publisher or EventPublisher()

    def _create_signal(self, body: dict) -> TradeSignal:
        signal = TradeSignal(
            strategy_config_id=body.get("strategy_config_id"),
            symbol=body["symbol"],
            exchange=body["exchange"],
            side=body.get("side") or "buy",
            entry_price=float(body["entry_price"]),
            take_profit_price=float(body["take_profit_price"]) if body.get("take_profit_price") is not None else None,
            stop_loss_price=float(body["stop_loss_price"]) if body.get("stop_loss_price") is not None else None,
            confidence=float(body.get("confidence", 0)),
            reason=body.get("reason") or "paper signal",
            status="created",
        )
        db.session.add(signal)
        db.session.flush()
        self.publisher.publish("signal.created", signal_to_dict(signal))
        return signal

    def execute_existing_signal(self, signal: TradeSignal, amount: float, leverage: float = 1, body: dict | None = None, strategy_config=None):
        body = body or {}
        approved, message, limits = self.risk_manager.validate_signal(
            signal=signal,
            amount=amount,
            leverage=leverage,
            strategy_config=strategy_config,
            override_config=body.get("risk_config"),
        )

        if not approved:
            signal.status = "rejected"
            db.session.commit()
            payload = {"signal": signal_to_dict(signal), "reason": message, "limits": limits}
            self.publisher.publish("risk.rejected", payload)
            self.publisher.publish("signal.rejected", payload)
            return False, payload

        signal.status = "approved"
        order = PaperOrder(
            signal_id=signal.id,
            exchange=signal.exchange,
            symbol=signal.symbol,
            side=signal.side,
            order_type=body.get("order_type") or "market",
            price=float(body.get("price") or signal.entry_price),
            amount=amount,
            status="created",
        )
        db.session.add(order)
        db.session.flush()
        self.publisher.publish("paper_order.created", paper_order_to_dict(order))

        fill_price = float(body.get("last_price") or order.price or signal.entry_price)
        fee_rate = float(body.get("fee_rate", 0.001))
        order.status = "filled"
        order.filled_price = fill_price
        order.filled_amount = amount
        order.fee = fill_price * amount * fee_rate
        order.filled_at = datetime.utcnow()
        signal.status = "executed"
        self.publisher.publish("paper_order.filled", paper_order_to_dict(order))

        position = PaperPosition(
            strategy_config_id=signal.strategy_config_id,
            exchange=signal.exchange,
            symbol=signal.symbol,
            side=signal.side,
            entry_price=fill_price,
            amount=amount,
            leverage=leverage,
            margin=(fill_price * amount) / max(leverage, 1),
            unrealized_pnl=0,
            realized_pnl=0,
            status="open",
        )
        db.session.add(position)
        db.session.commit()
        self.publisher.publish("paper_position.opened", paper_position_to_dict(position))

        return True, {
            "signal": signal_to_dict(signal),
            "paper_order": paper_order_to_dict(order),
            "paper_position": paper_position_to_dict(position),
        }

    def execute_paper_signal(self, body: dict):
        try:
            amount = float(body.get("amount", 0))
            leverage = float(body.get("leverage", 1))
            if amount <= 0:
                return self.response_err_msg("Amount must be greater than zero")

            strategy_config = None
            if body.get("strategy_config_id"):
                strategy_config = StrategyConfig.query.get(body["strategy_config_id"])

            signal = self._create_signal(body)
            executed, payload = self.execute_existing_signal(signal, amount, leverage, body, strategy_config)
            if not executed:
                return self.response_forbidden(payload["reason"])

            return self.response_ok(payload)
        except KeyError as error:
            db.session.rollback()
            return self.response_err_msg(f"Missing required field: {error}")
        except Exception as error:
            db.session.rollback()
            return self.response_err_msg(str(error))

    def get_signals(self):
        signals = TradeSignal.query.order_by(TradeSignal.id.desc()).all()
        return self.response_ok([signal_to_dict(signal) for signal in signals])

    def get_orders(self):
        orders = PaperOrder.query.order_by(PaperOrder.id.desc()).all()
        return self.response_ok([paper_order_to_dict(order) for order in orders])

    def get_positions(self):
        positions = PaperPosition.query.order_by(PaperPosition.id.desc()).all()
        return self.response_ok([paper_position_to_dict(position) for position in positions])

    def close_position(self, position_id: int, body: dict):
        position = PaperPosition.query.get(position_id)
        if not position:
            return self.response_not_found("Paper position not found")
        if position.status != "open":
            return self.response_err_msg("Paper position is already closed")

        close_price = float(body.get("price") or position.entry_price)
        if position.side in ["sell", "short"]:
            pnl = (position.entry_price - close_price) * position.amount
        else:
            pnl = (close_price - position.entry_price) * position.amount

        position.realized_pnl = pnl
        position.unrealized_pnl = 0
        position.status = "closed"
        position.closed_at = datetime.utcnow()
        db.session.commit()
        payload = paper_position_to_dict(position)
        self.publisher.publish("paper_position.closed", payload)
        return self.response_ok(payload)
