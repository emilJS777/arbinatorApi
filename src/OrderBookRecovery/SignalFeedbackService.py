from datetime import timedelta

from src.OrderBookRecovery.OrderBookRecoveryModel import StrategyRunTrade


class SignalFeedbackService:
    weak_momentum_threshold = 0.000001

    def recent_trades(self, config, side=None, limit=None):
        query = StrategyRunTrade.query.filter(
            StrategyRunTrade.strategy_config_id == config.id,
            StrategyRunTrade.closed_at.isnot(None),
            StrategyRunTrade.is_archived.is_(False),
        )
        if side:
            query = query.filter_by(side=side)
        return query.order_by(StrategyRunTrade.closed_at.desc()).limit(limit or int(config.feedback_lookback_trades)).all()

    def side_stats(self, config, side):
        trades = self.recent_trades(config, side)
        wins = [trade for trade in trades if trade.pnl > 0]
        losses = [trade for trade in trades if trade.pnl < 0]
        gross_profit = sum(trade.pnl for trade in wins)
        gross_loss = abs(sum(trade.pnl for trade in losses))
        loss_streak = 0
        for trade in trades:
            if trade.pnl < 0:
                loss_streak += 1
                continue
            break
        return {
            "side": side,
            "trades_count": len(trades),
            "win_rate": (len(wins) / len(trades) * 100) if trades else 0,
            "avg_pnl": (sum(trade.pnl for trade in trades) / len(trades)) if trades else 0,
            "loss_streak": loss_streak,
            "profit_factor": (gross_profit / gross_loss) if gross_loss else (gross_profit if gross_profit else 0),
            "average_entry_imbalance": self.average([trade.signal_configured_exchange_imbalance for trade in trades]),
            "average_entry_momentum": self.average([trade.signal_configured_exchange_momentum for trade in trades]),
            "common_losing_conditions": self.common_losing_conditions(losses),
        }

    def summary(self, config):
        return {
            "feedback_enabled": bool(config.feedback_enabled),
            "long": self.side_stats(config, "long"),
            "short": self.side_stats(config, "short"),
        }

    def evaluate(self, config, side, consensus, current_time):
        base_ratio = float(config.min_consensus_ratio)
        base_valid = int(config.min_valid_exchanges)
        result = {
            "feedback_enabled": bool(config.feedback_enabled),
            "adaptive_min_consensus_ratio": base_ratio,
            "adaptive_min_valid_exchanges": base_valid,
            "blocked_side": None,
            "feedback_reject_reason": None,
        }
        if not config.feedback_enabled or not side:
            return True, result

        stats = self.side_stats(config, side)
        recent_side_trades = self.recent_trades(config, side, int(config.feedback_lookback_trades))
        if stats["loss_streak"] >= int(config.side_loss_streak_limit) and recent_side_trades:
            latest_loss = recent_side_trades[0]
            cooldown_until = latest_loss.closed_at + timedelta(seconds=int(config.side_cooldown_seconds))
            if current_time < cooldown_until:
                result["blocked_side"] = side
                result["feedback_reject_reason"] = f"{side}_loss_streak_cooldown"
                return False, result

        side_recent_20 = self.recent_trades(config, side, 20)
        if side_recent_20:
            wins = [trade for trade in side_recent_20 if trade.pnl > 0]
            win_rate = len(wins) / len(side_recent_20) * 100
            if win_rate < float(config.min_side_win_rate):
                result["adaptive_min_consensus_ratio"] = min(1.0, base_ratio + float(config.adaptive_consensus_boost))

        if stats["avg_pnl"] < 0:
            result["adaptive_min_valid_exchanges"] = base_valid + int(config.adaptive_min_valid_exchanges_boost)

        recent_losses = [trade for trade in recent_side_trades if trade.pnl < 0]
        if any((trade.signal_valid_exchanges_count or 0) < 3 for trade in recent_losses[:5]):
            result["adaptive_min_valid_exchanges"] = max(result["adaptive_min_valid_exchanges"], 3)

        weak_loss_seen = any(
            abs(trade.signal_configured_exchange_momentum or trade.signal_average_momentum or 0) <= self.weak_momentum_threshold
            for trade in recent_losses[:5]
        )
        if weak_loss_seen and abs(consensus.get("average_momentum") or 0) <= self.weak_momentum_threshold:
            result["feedback_reject_reason"] = "weak_momentum_after_recent_loss"
            return False, result

        side_ratio = consensus.get("consensus_ratio_long") if side == "long" else consensus.get("consensus_ratio_short")
        if (side_ratio or 0) < result["adaptive_min_consensus_ratio"]:
            result["feedback_reject_reason"] = "adaptive_consensus_ratio_not_met"
            return False, result

        if int(consensus.get("valid_exchanges_count") or 0) < result["adaptive_min_valid_exchanges"]:
            result["feedback_reject_reason"] = "adaptive_min_valid_exchanges_not_met"
            return False, result

        return True, result

    def common_losing_conditions(self, losses):
        return {
            "weak_confirmation_losses": len([trade for trade in losses if (trade.signal_valid_exchanges_count or 0) < 3]),
            "weak_momentum_losses": len([
                trade for trade in losses
                if abs(trade.signal_configured_exchange_momentum or trade.signal_average_momentum or 0) <= self.weak_momentum_threshold
            ]),
        }

    def average(self, values):
        numbers = [value for value in values if value is not None]
        return sum(numbers) / len(numbers) if numbers else 0
