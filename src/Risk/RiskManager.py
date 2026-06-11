from datetime import datetime, time

from src.config import LIVE_TRADING_ENABLED
from src.PaperTrading.PaperPositionModel import PaperPosition


DEFAULT_RISK_CONFIG = {
    "max_order_margin_usdt": 100,
    "max_leverage": 1,
    "max_daily_loss_usdt": 50,
    "allowed_exchanges": [],
    "allowed_symbols": [],
    "min_expected_roi_percent": 0,
}

DEFAULT_FUTURES_RISK_CONFIG = {
    "max_open_positions": 1,
    "max_daily_loss": 100,
    "max_position_margin": 100,
    "max_leverage": 2,
    "cooldown_after_loss": 0,
}


class RiskManager:
    def get_limits(self, strategy_config=None, override_config=None) -> dict:
        config = dict(DEFAULT_RISK_CONFIG)
        stored_config = strategy_config.config_json if strategy_config and strategy_config.config_json else {}
        risk_config = stored_config.get("risk") if isinstance(stored_config, dict) else {}
        if isinstance(risk_config, dict):
            config.update(risk_config)
        if isinstance(override_config, dict):
            config.update(override_config)
        return config

    def get_daily_realized_pnl(self) -> float:
        today_start = datetime.combine(datetime.utcnow().date(), time.min)
        positions = PaperPosition.query.filter(PaperPosition.closed_at >= today_start).all()
        return sum(position.realized_pnl or 0 for position in positions)

    def expected_roi_percent(self, signal) -> float:
        if not signal.entry_price:
            return 0
        if not signal.take_profit_price:
            return 0

        if signal.side in ["sell", "short"]:
            return ((signal.entry_price - signal.take_profit_price) / signal.entry_price) * 100
        return ((signal.take_profit_price - signal.entry_price) / signal.entry_price) * 100

    def validate_signal(self, signal, amount: float, leverage: float = 1, strategy_config=None, override_config=None):
        limits = self.get_limits(strategy_config, override_config)
        margin = (signal.entry_price * amount) / max(leverage, 1)
        daily_pnl = self.get_daily_realized_pnl()
        allowed_exchanges = limits.get("allowed_exchanges") or []
        allowed_symbols = limits.get("allowed_symbols") or []
        expected_roi = self.expected_roi_percent(signal)

        if margin > float(limits["max_order_margin_usdt"]):
            return False, f"Order margin {margin:.4f} USDT exceeds max_order_margin_usdt", limits
        if leverage > float(limits["max_leverage"]):
            return False, f"Leverage {leverage} exceeds max_leverage", limits
        if daily_pnl <= -abs(float(limits["max_daily_loss_usdt"])):
            return False, "Daily loss limit reached", limits
        if allowed_exchanges and signal.exchange not in allowed_exchanges:
            return False, f"Exchange {signal.exchange} is not allowed", limits
        if allowed_symbols and signal.symbol not in allowed_symbols:
            return False, f"Symbol {signal.symbol} is not allowed", limits
        if expected_roi < float(limits["min_expected_roi_percent"]):
            return False, f"Expected ROI {expected_roi:.4f}% is below min_expected_roi_percent", limits

        return True, "approved", limits

    def status(self) -> dict:
        return {
            "limits": DEFAULT_RISK_CONFIG,
            "daily_realized_pnl": self.get_daily_realized_pnl(),
            "live_trading_enabled": LIVE_TRADING_ENABLED,
        }

    def validate_futures_signal(self, signal, margin: float, leverage: float, config: dict | None = None):
        limits = dict(DEFAULT_FUTURES_RISK_CONFIG)
        if config:
            limits.update(config)

        open_positions = PaperPosition.query.filter_by(status="open").all()
        symbol_open = [position for position in open_positions if position.symbol == signal.symbol]
        daily_pnl = self.get_daily_realized_pnl()

        if len(open_positions) >= int(limits["max_open_positions"]):
            return False, "Max open positions reached", limits
        if symbol_open:
            return False, "Position for symbol already open", limits
        if daily_pnl <= -abs(float(limits["max_daily_loss"])):
            return False, "Max daily loss reached", limits
        if margin > float(limits["max_position_margin"]):
            return False, "Position margin exceeds max_position_margin", limits
        if leverage > float(limits["max_leverage"]):
            return False, "Leverage exceeds max_leverage", limits
        if int(limits.get("cooldown_after_loss", 0)) > 0:
            last_loss = PaperPosition.query.filter(
                PaperPosition.status == "closed",
                PaperPosition.realized_pnl < 0,
                PaperPosition.closed_at.isnot(None),
            ).order_by(PaperPosition.closed_at.desc()).first()
            if last_loss:
                seconds_since_loss = (datetime.utcnow() - last_loss.closed_at).total_seconds()
                if seconds_since_loss < int(limits["cooldown_after_loss"]):
                    return False, "Cooldown after loss is active", limits

        return True, "approved", limits
