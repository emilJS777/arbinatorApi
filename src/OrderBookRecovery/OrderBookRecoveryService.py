from collections import defaultdict, deque
from datetime import datetime, timedelta
from threading import Thread
from io import BytesIO, StringIO
import csv
import json
import logging
import time

from flask import send_file

from src import db
from src.Arbitrage.OrderBookSnapshotStore import OrderBookSnapshotStore
from src.OrderBookRecovery.OrderBookRecoveryModel import (
    OrderBookPatternStrategyConfig,
    RecoveryState,
    StrategyRun,
    StrategyRunTrade,
)
from src.OrderBookRecovery.OrderBookNormalizer import OrderBookNormalizer
from src.OrderBookRecovery.SignalFeedbackService import SignalFeedbackService
from src.Socket.EventPublisher import EventPublisher
from src.__Parents.Response import Response


logger = logging.getLogger(__name__)


class OrderBookRecoveryService(Response):
    strategy_type = "order_book_pattern_recovery"
    _mid_price_history = defaultdict(lambda: deque(maxlen=200))
    _last_evaluations = {}
    _last_hook_seen_at = None
    _last_hook_snapshot = None
    _last_matching_hooks = {}
    _last_mismatch_hooks = {}

    def __init__(self, publisher=None):
        self.publisher = publisher or EventPublisher()
        self.feedback_service = SignalFeedbackService()

    def get_or_create_config(self):
        config = OrderBookPatternStrategyConfig.query.order_by(OrderBookPatternStrategyConfig.id.asc()).first()
        if config:
            return config
        config = OrderBookPatternStrategyConfig()
        db.session.add(config)
        db.session.flush()
        state = RecoveryState(strategy_config_id=config.id, current_margin=config.base_margin_usdt)
        db.session.add(state)
        db.session.commit()
        return config

    def get_or_create_state(self, config=None):
        config = config or self.get_or_create_config()
        state = RecoveryState.query.filter_by(strategy_config_id=config.id).first()
        if state:
            return state
        state = RecoveryState(strategy_config_id=config.id, current_margin=config.base_margin_usdt)
        db.session.add(state)
        db.session.commit()
        return state

    def config_response(self):
        return self.response_ok(self.config_to_dict(self.get_or_create_config()))

    def apply_config_overrides(self, config, overrides: dict):
        allowed = {
            "exchange",
            "symbol",
            "base_margin_usdt",
            "leverage",
            "max_recovery_steps",
            "recovery_multiplier",
            "take_profit_percent_of_margin",
            "stop_loss_percent_of_margin",
            "max_daily_loss_usdt",
            "max_total_loss_usdt",
            "max_open_positions",
            "cooldown_after_loss_seconds",
            "cooldown_after_win_seconds",
            "long_imbalance_threshold",
            "short_imbalance_threshold",
            "max_spread_percent",
            "momentum_window_snapshots",
            "consensus_enabled",
            "min_valid_exchanges",
            "min_confirming_exchanges",
            "min_consensus_ratio",
            "max_snapshot_age_seconds",
            "require_configured_exchange_signal",
            "cooldown_after_max_recovery_seconds",
            "feedback_enabled",
            "feedback_lookback_trades",
            "side_loss_streak_limit",
            "side_cooldown_seconds",
            "min_side_win_rate",
            "adaptive_consensus_boost",
            "adaptive_min_valid_exchanges_boost",
            "paper_equity_usdt",
        }
        for key in allowed:
            if key in overrides:
                setattr(config, key, overrides[key])
        config.paper_mode_only = True

    def update_config(self, body: dict):
        config = self.get_or_create_config()
        self.apply_config_overrides(config, body)
        if "enabled" in body:
            config.enabled = body["enabled"]
        config.paper_mode_only = True
        state = self.get_or_create_state(config)
        if state.current_margin <= 0:
            state.current_margin = config.base_margin_usdt
        db.session.commit()
        return self.response_ok(self.config_to_dict(config))

    def start_paper(self):
        config = self.get_or_create_config()
        config.enabled = True
        config.paper_mode_only = True
        state = self.get_or_create_state(config)
        state.is_stopped = False
        state.stop_reason = None
        if state.current_margin <= 0:
            state.current_margin = config.base_margin_usdt
        existing_run = self.active_run(config)
        if existing_run:
            existing_run.status = "stopped"
            existing_run.stopped_at = datetime.utcnow()
            existing_run.stop_reason = "restarted"
        run = StrategyRun(strategy_config_id=config.id, status="running")
        db.session.add(run)
        db.session.commit()
        logger.info("OrderBookRecovery strategy started: exchange=%s symbol=%s run_id=%s", config.exchange, config.symbol, run.id)
        payload = self.state_payload(config, state)
        self.publisher.publish("orderbook_recovery.started", payload)
        return self.response_ok(payload)

    def stop(self, reason="manual_stop"):
        config = self.get_or_create_config()
        config.enabled = False
        state = self.get_or_create_state(config)
        state.is_stopped = True
        state.stop_reason = reason
        run = self.active_run(config)
        if run:
            run.status = "stopped"
            run.stopped_at = datetime.utcnow()
            run.stop_reason = reason
        db.session.commit()
        logger.info("OrderBookRecovery strategy stopped: exchange=%s symbol=%s reason=%s", config.exchange, config.symbol, reason)
        payload = self.state_payload(config, state)
        self.publisher.publish("orderbook_recovery.stopped", payload)
        return self.response_ok(payload)

    def state_response(self):
        config = self.get_or_create_config()
        state = self.get_or_create_state(config)
        return self.response_ok(self.state_payload(config, state))

    def trades_response(self, include_archived=False):
        query = StrategyRunTrade.query
        if not include_archived:
            query = query.filter_by(is_archived=False)
        trades = query.order_by(StrategyRunTrade.id.desc()).limit(200).all()
        return self.response_ok([self.trade_to_dict(trade) for trade in trades])

    def metrics_response(self):
        return self.response_ok(self.metrics())

    def debug_response(self):
        config = self.get_or_create_config()
        state = self.get_or_create_state(config)
        return self.response_ok(self.debug_payload(config, state))

    def run_forward_test(self, body: dict):
        config = self.get_or_create_config()
        overrides = dict(body.get("config") or {})
        if body.get("exchange"):
            overrides["exchange"] = body["exchange"]
        if body.get("symbol"):
            overrides["symbol"] = body["symbol"]
        self.apply_config_overrides(config, overrides)
        config.enabled = True
        config.paper_mode_only = True

        state = self.get_or_create_state(config)
        state.is_stopped = False
        state.stop_reason = None
        if state.current_margin <= 0:
            state.current_margin = config.base_margin_usdt

        existing_run = self.active_run(config)
        if existing_run:
            existing_run.status = "stopped"
            existing_run.stopped_at = datetime.utcnow()
            existing_run.stop_reason = "restarted_by_forward_test"

        run = StrategyRun(strategy_config_id=config.id, status="running")
        db.session.add(run)
        db.session.commit()

        duration_minutes = max(0.01, float(body.get("duration_minutes", 30)))
        self.schedule_forward_test_stop(run.id, duration_minutes)
        return self.response_ok({
            "run_id": run.id,
            "status": run.status,
            "duration_minutes": duration_minutes,
            "config": self.config_to_dict(config),
        })

    def schedule_forward_test_stop(self, run_id: int, duration_minutes: float):
        def worker():
            time.sleep(duration_minutes * 60)
            from src import app

            with app.app_context():
                try:
                    run = db.session.get(StrategyRun, run_id)
                    if not run or run.status != "running":
                        return
                    config = db.session.get(OrderBookPatternStrategyConfig, run.strategy_config_id)
                    state = RecoveryState.query.filter_by(strategy_config_id=run.strategy_config_id).first()
                    run.status = "completed"
                    run.stopped_at = datetime.utcnow()
                    run.stop_reason = "forward_test_completed"
                    if config:
                        config.enabled = False
                    if state:
                        state.is_stopped = True
                        state.stop_reason = "forward_test_completed"
                    db.session.commit()
                except Exception as error:
                    db.session.rollback()
                    logger.warning("Forward test auto-stop failed for run %s: %s", run_id, error)

        Thread(target=worker, daemon=True).start()

    def forward_test_status(self, run_id: int):
        run = db.session.get(StrategyRun, run_id)
        if not run:
            return self.response_not_found("Forward test not found")
        return self.response_ok(self.run_to_dict(run))

    def forward_test_metrics(self, run_id: int):
        run = db.session.get(StrategyRun, run_id)
        if not run:
            return self.response_not_found("Forward test not found")
        return self.response_ok(self.metrics_for_run(run))

    def close_manual(self, position_id: int, body: dict):
        config = self.get_or_create_config()
        state = self.get_or_create_state(config)
        trade = db.session.get(StrategyRunTrade, position_id)
        if not trade or trade.strategy_config_id != config.id:
            return self.response_not_found("Paper position not found")
        if trade.closed_at:
            return self.response_err_msg("Paper position is already closed")

        snapshot = self.snapshot_for(config.exchange, config.symbol)
        if not snapshot:
            return self.response_err_msg("cannot_close_without_valid_market_price")
        features, reject_reason = self.features(config, snapshot)
        if not features:
            return self.response_err_msg("cannot_close_without_valid_market_price")

        current_time = datetime.utcnow()
        exit_price = features["mid_price"]
        pnl = self.calculate_pnl(trade.side, trade.entry_price, exit_price, trade.notional)
        reason = (body or {}).get("reason") or "manual_close"
        if reason != "manual_close":
            reason = "manual_close"
        payload = self.close_trade(trade, exit_price, pnl, reason, state, config, current_time)
        self.store_last_evaluation(
            config,
            features,
            False,
            False,
            "manual_close",
            None,
            current_time,
            self.consensus_snapshot(config, current_time) if config.consensus_enabled else {},
        )
        return self.response_ok(payload)

    def archive_trade(self, trade_id: int, body: dict):
        trade = db.session.get(StrategyRunTrade, trade_id)
        if not trade:
            return self.response_not_found("Trade not found")
        if not trade.closed_at:
            return self.response_err_msg("cannot_archive_open_trade")
        trade.is_archived = True
        trade.archived_at = datetime.utcnow()
        trade.archive_reason = (body or {}).get("reason") or "manual_archive"
        db.session.commit()
        return self.response_ok(self.trade_to_dict(trade))

    def archive_all_closed_trades(self, body: dict | None = None):
        reason = (body or {}).get("reason") or "archive_all_closed"
        trades = StrategyRunTrade.query.filter(
            StrategyRunTrade.closed_at.isnot(None),
            StrategyRunTrade.is_archived.is_(False),
        ).all()
        archived_at = datetime.utcnow()
        for trade in trades:
            trade.is_archived = True
            trade.archived_at = archived_at
            trade.archive_reason = reason
        db.session.commit()
        return self.response_ok({"archived_count": len(trades)})

    def unarchive_all_trades(self):
        trades = StrategyRunTrade.query.filter_by(is_archived=True).all()
        for trade in trades:
            trade.is_archived = False
            trade.archived_at = None
            trade.archive_reason = None
        db.session.commit()
        return self.response_ok({"unarchived_count": len(trades)})

    def decision_details(self, trade_id: int):
        trade = db.session.get(StrategyRunTrade, trade_id)
        if not trade:
            return self.response_not_found("Trade not found")
        return self.response_ok(self.decision_details_payload(trade))

    def export_trades(self, include_archived=False, export_format="csv"):
        query = StrategyRunTrade.query.filter(StrategyRunTrade.closed_at.isnot(None))
        if not include_archived:
            query = query.filter_by(is_archived=False)
        trades = query.order_by(StrategyRunTrade.closed_at.asc()).all()
        rows = [self.export_row(trade) for trade in trades]
        stamp = datetime.utcnow().strftime("%Y-%m-%d-%H-%M")
        if export_format == "json":
            payload = json.dumps(rows, default=str, ensure_ascii=False, indent=2)
            buffer = BytesIO(payload.encode("utf-8"))
            return send_file(buffer, mimetype="application/json", as_attachment=True, download_name=f"orderbook-recovery-trades-{stamp}.json")

        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=self.export_fields())
        writer.writeheader()
        writer.writerows(rows)
        buffer = BytesIO(output.getvalue().encode("utf-8"))
        return send_file(buffer, mimetype="text/csv", as_attachment=True, download_name=f"orderbook-recovery-trades-{stamp}.csv")

    def export_fields(self):
        return [
            "id", "side", "recovery_step", "margin", "leverage", "notional", "entry_price", "exit_price",
            "pnl", "result", "close_reason", "opened_at", "closed_at", "holding_seconds",
            "consensus_direction", "valid_exchanges_count", "confirming_long_count", "confirming_short_count",
            "consensus_ratio_long", "consensus_ratio_short", "average_imbalance", "average_momentum",
            "configured_exchange_imbalance", "configured_exchange_spread", "configured_exchange_momentum", "entry_reason",
            "feedback_enabled", "long_recent_win_rate", "short_recent_win_rate", "long_loss_streak", "short_loss_streak",
            "adaptive_min_consensus_ratio", "adaptive_min_valid_exchanges", "blocked_side", "feedback_reject_reason",
            "exchange", "symbol", "base_margin_usdt", "leverage_config", "tp_percent_of_margin", "sl_percent_of_margin",
            "long_imbalance_threshold", "short_imbalance_threshold", "min_valid_exchanges", "min_confirming_exchanges",
            "min_consensus_ratio", "max_spread_percent", "momentum_window",
            "per_exchange_features_json", "decision_snapshot_json",
        ]

    def export_row(self, trade):
        snapshot = self.parse_json(trade.decision_snapshot_json) or {}
        config = snapshot.get("config") or {}
        feedback = snapshot.get("feedback_state") or {}
        return {
            "id": trade.id,
            "side": trade.side,
            "recovery_step": trade.recovery_step,
            "margin": trade.margin,
            "leverage": trade.leverage,
            "notional": trade.notional,
            "entry_price": trade.entry_price,
            "exit_price": trade.exit_price,
            "pnl": trade.pnl,
            "result": trade.result,
            "close_reason": trade.reason_close,
            "opened_at": trade.opened_at,
            "closed_at": trade.closed_at,
            "holding_seconds": trade.holding_seconds,
            "consensus_direction": trade.consensus_direction or trade.signal_consensus_direction,
            "valid_exchanges_count": trade.valid_exchanges_count or trade.signal_valid_exchanges_count,
            "confirming_long_count": trade.confirming_long_count or trade.signal_confirming_long_count,
            "confirming_short_count": trade.confirming_short_count or trade.signal_confirming_short_count,
            "consensus_ratio_long": trade.consensus_ratio_long or trade.signal_consensus_ratio_long,
            "consensus_ratio_short": trade.consensus_ratio_short or trade.signal_consensus_ratio_short,
            "average_imbalance": trade.average_imbalance or trade.signal_average_imbalance,
            "average_momentum": trade.average_momentum or trade.signal_average_momentum,
            "configured_exchange_imbalance": trade.configured_exchange_imbalance or trade.signal_configured_exchange_imbalance,
            "configured_exchange_spread": trade.configured_exchange_spread or trade.signal_configured_exchange_spread,
            "configured_exchange_momentum": trade.configured_exchange_momentum or trade.signal_configured_exchange_momentum,
            "entry_reason": trade.entry_reason or trade.reason_open,
            "feedback_enabled": feedback.get("feedback_enabled"),
            "long_recent_win_rate": feedback.get("long_recent_win_rate"),
            "short_recent_win_rate": feedback.get("short_recent_win_rate"),
            "long_loss_streak": feedback.get("long_loss_streak"),
            "short_loss_streak": feedback.get("short_loss_streak"),
            "adaptive_min_consensus_ratio": feedback.get("adaptive_min_consensus_ratio"),
            "adaptive_min_valid_exchanges": feedback.get("adaptive_min_valid_exchanges"),
            "blocked_side": feedback.get("blocked_side"),
            "feedback_reject_reason": feedback.get("feedback_reject_reason"),
            "exchange": config.get("exchange") or trade.exchange,
            "symbol": config.get("symbol") or trade.symbol,
            "base_margin_usdt": config.get("base_margin_usdt"),
            "leverage_config": config.get("leverage"),
            "tp_percent_of_margin": config.get("take_profit_percent_of_margin"),
            "sl_percent_of_margin": config.get("stop_loss_percent_of_margin"),
            "long_imbalance_threshold": config.get("long_imbalance_threshold"),
            "short_imbalance_threshold": config.get("short_imbalance_threshold"),
            "min_valid_exchanges": config.get("min_valid_exchanges"),
            "min_confirming_exchanges": config.get("min_confirming_exchanges"),
            "min_consensus_ratio": config.get("min_consensus_ratio"),
            "max_spread_percent": config.get("max_spread_percent"),
            "momentum_window": config.get("momentum_window_snapshots"),
            "per_exchange_features_json": trade.per_exchange_features_json or trade.signal_per_exchange_features_json,
            "decision_snapshot_json": trade.decision_snapshot_json,
        }

    def decision_details_payload(self, trade):
        return {
            "trade": self.trade_to_dict(trade),
            "summary": {
                "id": trade.id,
                "exchange": trade.exchange,
                "symbol": trade.symbol,
                "side": trade.side,
                "entry_price": trade.entry_price,
                "exit_price": trade.exit_price,
                "pnl": trade.pnl,
                "result": trade.result,
                "opened_at": trade.opened_at,
                "closed_at": trade.closed_at,
                "is_archived": trade.is_archived,
            },
            "decision_snapshot": self.parse_json(trade.decision_snapshot_json),
            "per_exchange_features": self.parse_json(trade.per_exchange_features_json or trade.signal_per_exchange_features_json) or [],
            "signal": {
                "consensus_direction": trade.consensus_direction or trade.signal_consensus_direction,
                "valid_exchanges_count": trade.valid_exchanges_count or trade.signal_valid_exchanges_count,
                "confirming_long_count": trade.confirming_long_count or trade.signal_confirming_long_count,
                "confirming_short_count": trade.confirming_short_count or trade.signal_confirming_short_count,
                "consensus_ratio_long": trade.consensus_ratio_long or trade.signal_consensus_ratio_long,
                "consensus_ratio_short": trade.consensus_ratio_short or trade.signal_consensus_ratio_short,
                "average_imbalance": trade.average_imbalance or trade.signal_average_imbalance,
                "average_momentum": trade.average_momentum or trade.signal_average_momentum,
                "configured_exchange_imbalance": trade.configured_exchange_imbalance or trade.signal_configured_exchange_imbalance,
                "configured_exchange_spread": trade.configured_exchange_spread or trade.signal_configured_exchange_spread,
                "configured_exchange_momentum": trade.configured_exchange_momentum or trade.signal_configured_exchange_momentum,
                "entry_reason": trade.entry_reason or trade.reason_open,
            },
            "consensus": (self.parse_json(trade.decision_snapshot_json) or {}).get("consensus_decision") or {},
            "feedback": (self.parse_json(trade.decision_snapshot_json) or {}).get("feedback_state") or {},
            "risk": (self.parse_json(trade.decision_snapshot_json) or {}).get("risk_decision") or {},
        }

    def parse_json(self, value):
        if not value:
            return None
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return None

    def on_order_book_snapshot(self, exchange: str, symbol: str, metadata: dict | None = None):
        metadata = metadata or {}
        self.__class__._last_hook_seen_at = datetime.utcnow()
        self.__class__._last_hook_snapshot = {
            "exchange": exchange,
            "symbol": symbol,
            "raw_pair": metadata.get("raw_pair") or symbol,
            "exchange_id": metadata.get("exchange_id"),
            "exchange_title": metadata.get("exchange_title") or exchange,
            "normalized_exchange": self.normalize_exchange(metadata.get("exchange_title") or exchange),
            "normalized_symbol": self.normalize_symbol(metadata.get("raw_pair") or symbol),
            "received_at": self.__class__._last_hook_seen_at,
        }
        logger.info("hook received exchange=%s pair=%s metadata=%s", exchange, symbol, metadata)
        config = self.get_or_create_config()
        match = self.hook_match(config, exchange, symbol, metadata)
        hook_snapshot = dict(self.__class__._last_hook_snapshot)
        hook_snapshot.update(match)
        if not match["exchange_match"] or not match["symbol_match"]:
            reason = "Snapshot received but ignored because exchange mismatch" if not match["exchange_match"] else "Snapshot received but ignored because symbol mismatch"
            hook_snapshot["reject_reason"] = reason
            self.__class__._last_mismatch_hooks[self.debug_key(config)] = hook_snapshot
            if not self.snapshot_for(config.exchange, config.symbol):
                self.store_last_evaluation(config, reject_reason=reason)
            logger.info("OrderBookRecovery snapshot ignored: %s config=%s hook=%s", reason, config.exchange, hook_snapshot)
            return None
        self.__class__._last_matching_hooks[self.debug_key(config)] = hook_snapshot
        return self.evaluate(config)

    def evaluate(self, config=None, snapshot=None, current_time=None):
        config = config or self.get_or_create_config()
        state = self.get_or_create_state(config)
        current_time = current_time or datetime.utcnow()
        self.resume_after_recovery_pause(config, state, current_time)

        if not config.enabled or state.is_stopped:
            self.store_last_evaluation(config, reject_reason=self.reason_if_not_trading(config, state), evaluated_at=current_time)
            return None

        snapshot = snapshot or self.snapshot_for(config.exchange, config.symbol)
        if not snapshot:
            self.store_last_evaluation(config, reject_reason="no_valid_order_book_snapshot", evaluated_at=current_time)
            return self.reject("no_valid_order_book_snapshot", config, state)

        features, reject_reason = self.features(config, snapshot)
        if not features:
            self.store_last_evaluation(config, reject_reason=reject_reason, evaluated_at=current_time)
            return self.reject(reject_reason, config, state)

        long_signal = features["imbalance"] > config.long_imbalance_threshold and features["short_momentum"] > 0
        short_signal = features["imbalance"] < config.short_imbalance_threshold and features["short_momentum"] < 0
        open_trade = self.open_trade(config)
        if open_trade:
            self.store_last_evaluation(config, features, long_signal, short_signal, "none", None, current_time)
            closed = self.evaluate_open_trade(open_trade, features["mid_price"], state, config, current_time)
            if closed:
                logger.info("OrderBookRecovery evaluation result: managed open position trade_id=%s result=closed", open_trade.id)
                return closed
            logger.info("OrderBookRecovery evaluation result: managed open position trade_id=%s result=hold", open_trade.id)
            return self.trade_to_dict(open_trade)

        signal, consensus = self.signal(config, state, features, current_time)
        feedback = self.feedback_snapshot(config, signal, consensus, current_time)
        if signal and feedback.get("feedback_reject_reason"):
            signal = None
            consensus["reject_reason"] = feedback["feedback_reject_reason"]
        consensus["feedback"] = feedback
        reject_reason = consensus.get("reject_reason") or (self.risk_rejection(config, state, features, current_time) if not signal else None)
        self.store_last_evaluation(config, features, long_signal, short_signal, signal or "none", reject_reason, current_time, consensus)
        logger.info(
            "OrderBookRecovery evaluation result: exchange=%s symbol=%s decision=%s imbalance=%s spread=%s momentum=%s",
            config.exchange,
            config.symbol,
            signal or "none",
            features["imbalance"],
            features["spread_percent"],
            features["short_momentum"],
        )
        if not signal:
            return None
        return self.open_position(config, state, features, signal, current_time, consensus)

    def snapshot_for(self, exchange: str, symbol: str):
        normalized_exchange = self.normalize_exchange(exchange)
        normalized_symbol = self.normalize_symbol(symbol)
        for exchange_key, symbols in OrderBookSnapshotStore.all().items():
            if self.normalize_exchange(exchange_key) != normalized_exchange:
                continue
            for symbol_key, snapshot in symbols.items():
                if self.normalize_symbol(symbol_key) == normalized_symbol:
                    return snapshot
        return None

    def normalize_exchange(self, value):
        return str(value or "").strip().lower()

    def normalize_symbol(self, value):
        symbol = str(value or "").strip().upper()
        if ":" in symbol:
            symbol = symbol.split(":", 1)[0]
        return symbol.replace("/", "").replace("-", "").replace("_", "").replace(" ", "")

    def hook_match(self, config, exchange, symbol, metadata=None):
        metadata = metadata or {}
        hook_exchange = metadata.get("exchange_title") or exchange
        hook_symbol = metadata.get("raw_pair") or symbol
        normalized_config_exchange = self.normalize_exchange(config.exchange)
        normalized_hook_exchange = self.normalize_exchange(hook_exchange)
        normalized_config_symbol = self.normalize_symbol(config.symbol)
        normalized_hook_symbol = self.normalize_symbol(hook_symbol)
        return {
            "configured_exchange": config.exchange,
            "configured_symbol": config.symbol,
            "last_hook_exchange": hook_exchange,
            "last_hook_symbol": symbol,
            "last_hook_raw_pair": hook_symbol,
            "normalized_config_exchange": normalized_config_exchange,
            "normalized_config_symbol": normalized_config_symbol,
            "normalized_hook_exchange": normalized_hook_exchange,
            "normalized_hook_symbol": normalized_hook_symbol,
            "exchange_match": normalized_config_exchange == normalized_hook_exchange,
            "symbol_match": normalized_config_symbol == normalized_hook_symbol,
            "exchange_id": metadata.get("exchange_id"),
        }

    def debug_key(self, config):
        return f"{config.exchange}:{config.symbol}"

    def store_last_evaluation(
        self,
        config,
        features=None,
        long_signal=False,
        short_signal=False,
        decision="none",
        reject_reason=None,
        evaluated_at=None,
        consensus=None,
    ):
        evaluated_at = evaluated_at or datetime.utcnow()
        features = features or {}
        self.__class__._last_evaluations[self.debug_key(config)] = {
            "bid_volume_top_5": features.get("bid_volume_top_5"),
            "ask_volume_top_5": features.get("ask_volume_top_5"),
            "imbalance": features.get("imbalance"),
            "spread_percent": features.get("spread_percent"),
            "momentum": features.get("short_momentum"),
            "long_signal": bool(long_signal),
            "short_signal": bool(short_signal),
            "last_decision": decision or "none",
            "reject_reason": reject_reason,
            "evaluated_at": evaluated_at,
            "consensus": consensus or {},
        }

    def last_evaluation_for(self, config):
        return self.__class__._last_evaluations.get(self.debug_key(config))

    def latest_snapshot_for(self, config):
        snapshot = self.snapshot_for(config.exchange, config.symbol)
        if not snapshot:
            return None
        order_book = snapshot.get("order_book") or {}
        normalized, _ = OrderBookNormalizer.normalize(order_book)
        bids = normalized["bids"] if normalized else []
        asks = normalized["asks"] if normalized else []
        raw_bids = order_book.get("bids") or []
        raw_asks = order_book.get("asks") or []
        raw_purchases = order_book.get("purchases") or []
        raw_sales = order_book.get("sales") or []
        return {
            "exchange": snapshot.get("exchange"),
            "symbol": snapshot.get("symbol"),
            "source_exchange": snapshot.get("metadata", {}).get("source_exchange_title") or snapshot.get("exchange"),
            "source_exchange_id": snapshot.get("metadata", {}).get("source_exchange_id"),
            "source_pair": snapshot.get("metadata", {}).get("source_pair") or snapshot.get("symbol"),
            "updated_at": snapshot.get("updated_at"),
            "snapshot_keys": list(order_book.keys()) if isinstance(order_book, dict) else [],
            "bids_count": len(raw_bids),
            "asks_count": len(raw_asks),
            "purchases_count": len(raw_purchases),
            "sales_count": len(raw_sales),
            "first_bid_raw": raw_bids[0] if raw_bids else None,
            "first_ask_raw": raw_asks[0] if raw_asks else None,
            "first_purchase_raw": raw_purchases[0] if raw_purchases else None,
            "first_sale_raw": raw_sales[0] if raw_sales else None,
            "normalized_bids_count": len(bids),
            "normalized_asks_count": len(asks),
            "best_bid": float(bids[0]["price"]) if bids else None,
            "best_ask": float(asks[0]["price"]) if asks else None,
        }

    def reason_if_not_trading(self, config, state):
        if state.is_stopped:
            if state.stop_reason == "max_recovery_pause" and state.paused_until:
                return "max_recovery_pause"
            return state.stop_reason or "strategy_stopped"
        if not config.enabled:
            return "strategy_disabled"
        if not self.snapshot_for(config.exchange, config.symbol):
            mismatch = self.__class__._last_mismatch_hooks.get(self.debug_key(config)) or {}
            return mismatch.get("reject_reason") or "No order book snapshots received for this exchange/symbol"
        last = self.last_evaluation_for(config) or {}
        return last.get("reject_reason")

    def status_for(self, config, state):
        if state.is_stopped:
            return "stopped"
        if config.enabled:
            return "running"
        return "stopped"

    def debug_payload(self, config, state):
        matching_hook = self.__class__._last_matching_hooks.get(self.debug_key(config))
        mismatch_hook = self.__class__._last_mismatch_hooks.get(self.debug_key(config))
        hook = matching_hook or mismatch_hook or self.__class__._last_hook_snapshot or {}
        match = self.hook_match(config, hook.get("exchange"), hook.get("symbol"), {
            "raw_pair": hook.get("raw_pair"),
            "exchange_id": hook.get("exchange_id"),
            "exchange_title": hook.get("exchange_title") or hook.get("exchange"),
        }) if hook else {
            "configured_exchange": config.exchange,
            "configured_symbol": config.symbol,
            "last_hook_exchange": None,
            "last_hook_symbol": None,
            "last_hook_raw_pair": None,
            "normalized_config_exchange": self.normalize_exchange(config.exchange),
            "normalized_config_symbol": self.normalize_symbol(config.symbol),
            "normalized_hook_exchange": None,
            "normalized_hook_symbol": None,
            "exchange_match": False,
            "symbol_match": False,
        }
        consensus = (self.last_evaluation_for(config) or {}).get("consensus") or self.consensus_snapshot(config)
        feedback = consensus.get("feedback") or self.feedback_snapshot(config, None, consensus, datetime.utcnow())
        return {
            "config": self.config_to_dict(config),
            "state": self.state_to_dict(state),
            "status": self.status_for(config, state),
            "last_evaluation": self.last_evaluation_for(config),
            "consensus": consensus,
            "valid_exchanges_count": consensus.get("valid_exchanges_count"),
            "confirming_long_count": consensus.get("confirming_long_count"),
            "confirming_short_count": consensus.get("confirming_short_count"),
            "consensus_ratio_long": consensus.get("consensus_ratio_long"),
            "consensus_ratio_short": consensus.get("consensus_ratio_short"),
            "average_imbalance": consensus.get("average_imbalance"),
            "average_momentum": consensus.get("average_momentum"),
            "consensus_direction": consensus.get("consensus_direction") or "none",
            "entry_blocked_reason": consensus.get("reject_reason") or (self.last_evaluation_for(config) or {}).get("reject_reason"),
            "feedback_enabled": feedback.get("feedback_enabled"),
            "long_recent_win_rate": feedback.get("long_recent_win_rate"),
            "short_recent_win_rate": feedback.get("short_recent_win_rate"),
            "long_loss_streak": feedback.get("long_loss_streak"),
            "short_loss_streak": feedback.get("short_loss_streak"),
            "adaptive_min_consensus_ratio": feedback.get("adaptive_min_consensus_ratio"),
            "adaptive_min_valid_exchanges": feedback.get("adaptive_min_valid_exchanges"),
            "blocked_side": feedback.get("blocked_side"),
            "feedback_reject_reason": feedback.get("feedback_reject_reason"),
            "feedback": feedback,
            "per_exchange_features": consensus.get("per_exchange_features") or [],
            "latest_snapshot": self.latest_snapshot_for(config),
            "last_snapshot_source_exchange": (self.latest_snapshot_for(config) or {}).get("source_exchange"),
            "last_snapshot_source_pair": (self.latest_snapshot_for(config) or {}).get("source_pair"),
            "snapshot_keys": (self.latest_snapshot_for(config) or {}).get("snapshot_keys"),
            "bids_count": (self.latest_snapshot_for(config) or {}).get("bids_count"),
            "asks_count": (self.latest_snapshot_for(config) or {}).get("asks_count"),
            "purchases_count": (self.latest_snapshot_for(config) or {}).get("purchases_count"),
            "sales_count": (self.latest_snapshot_for(config) or {}).get("sales_count"),
            "first_bid_raw": (self.latest_snapshot_for(config) or {}).get("first_bid_raw"),
            "first_ask_raw": (self.latest_snapshot_for(config) or {}).get("first_ask_raw"),
            "first_purchase_raw": (self.latest_snapshot_for(config) or {}).get("first_purchase_raw"),
            "first_sale_raw": (self.latest_snapshot_for(config) or {}).get("first_sale_raw"),
            "scanner_hook_active": self.__class__._last_hook_seen_at is not None,
            "last_scanner_hook_at": self.__class__._last_hook_seen_at,
            "last_scanner_hook_snapshot": self.__class__._last_hook_snapshot,
            "last_matching_hook_snapshot": matching_hook,
            "last_mismatch_hook_snapshot": mismatch_hook,
            **match,
            "reason_if_not_trading": self.reason_if_not_trading(config, state),
        }

    def features(self, config, snapshot):
        order_book = snapshot.get("order_book") or {}
        normalized, error = OrderBookNormalizer.normalize(order_book)
        if error:
            return None, error
        bids = normalized["bids"]
        asks = normalized["asks"]
        best_bid = float(bids[0]["price"])
        best_ask = float(asks[0]["price"])
        if best_bid <= 0 or best_ask <= 0 or best_ask < best_bid:
            return None, "invalid_price_amount"
        bid_volume = sum(float(row["amount"]) for row in bids[:5])
        ask_volume = sum(float(row["amount"]) for row in asks[:5])
        if ask_volume <= 0:
            return None, "empty_asks"
        mid_price = (best_bid + best_ask) / 2
        spread_percent = ((best_ask - best_bid) / mid_price) * 100
        key = f"{snapshot['exchange']}:{snapshot['symbol']}"
        history = self._mid_price_history[key]
        history.append(mid_price)
        window = max(1, int(config.momentum_window_snapshots))
        prices = list(history)[-window:]
        momentum = prices[-1] - prices[0] if len(prices) >= 2 else 0
        return {
            "bid_volume_top_5": bid_volume,
            "ask_volume_top_5": ask_volume,
            "imbalance": bid_volume / ask_volume,
            "spread_percent": spread_percent,
            "mid_price": mid_price,
            "short_momentum": momentum,
        }, None

    def snapshots_for_symbol(self, symbol: str):
        normalized_symbol = self.normalize_symbol(symbol)
        result = []
        for exchange_key, symbols in OrderBookSnapshotStore.all().items():
            for symbol_key, snapshot in symbols.items():
                if self.normalize_symbol(symbol_key) == normalized_symbol:
                    result.append(snapshot)
        return result

    def exchange_feature(self, config, snapshot, current_time):
        exchange = snapshot.get("exchange")
        symbol = snapshot.get("symbol")
        age = None
        if snapshot.get("updated_at"):
            age = max(0, (current_time - snapshot["updated_at"]).total_seconds())
        features, error = self.features(config, snapshot)
        item = {
            "exchange": exchange,
            "symbol": symbol,
            "bid_volume_top_5": None,
            "ask_volume_top_5": None,
            "imbalance": None,
            "spread_percent": None,
            "momentum": None,
            "snapshot_age_seconds": age,
            "long_signal": False,
            "short_signal": False,
            "valid": False,
            "reject_reason": None,
        }
        if error:
            item["reject_reason"] = error
            return item
        item.update({
            "bid_volume_top_5": features["bid_volume_top_5"],
            "ask_volume_top_5": features["ask_volume_top_5"],
            "imbalance": features["imbalance"],
            "spread_percent": features["spread_percent"],
            "momentum": features["short_momentum"],
            "long_signal": features["imbalance"] > config.long_imbalance_threshold,
            "short_signal": features["imbalance"] < config.short_imbalance_threshold,
        })
        if age is not None and age > float(config.max_snapshot_age_seconds):
            item["reject_reason"] = "stale_snapshot"
            return item
        if features["spread_percent"] > config.max_spread_percent:
            item["reject_reason"] = "spread_too_high"
            return item
        item["valid"] = True
        return item

    def consensus_snapshot(self, config, current_time=None):
        current_time = current_time or datetime.utcnow()
        rows = [self.exchange_feature(config, snapshot, current_time) for snapshot in self.snapshots_for_symbol(config.symbol)]
        valid = [row for row in rows if row["valid"]]
        long_count = len([row for row in valid if row["long_signal"]])
        short_count = len([row for row in valid if row["short_signal"]])
        valid_count = len(valid)
        configured_row = next(
            (row for row in rows if self.normalize_exchange(row["exchange"]) == self.normalize_exchange(config.exchange)),
            None,
        )
        return {
            "valid_exchanges_count": valid_count,
            "confirming_long_count": long_count,
            "confirming_short_count": short_count,
            "consensus_ratio_long": (long_count / valid_count) if valid_count else 0,
            "consensus_ratio_short": (short_count / valid_count) if valid_count else 0,
            "average_imbalance": (sum(row["imbalance"] for row in valid) / valid_count) if valid_count else 0,
            "average_momentum": (sum(row["momentum"] for row in valid) / valid_count) if valid_count else 0,
            "configured_exchange_valid": bool(configured_row and configured_row["valid"]),
            "configured_exchange_long_signal": bool(configured_row and configured_row["long_signal"]),
            "configured_exchange_short_signal": bool(configured_row and configured_row["short_signal"]),
            "configured_exchange_imbalance": configured_row.get("imbalance") if configured_row else None,
            "configured_exchange_spread": configured_row.get("spread_percent") if configured_row else None,
            "configured_exchange_momentum": configured_row.get("momentum") if configured_row else None,
            "configured_exchange_reject_reason": configured_row.get("reject_reason") if configured_row else "configured_exchange_snapshot_missing",
            "consensus_direction": "none",
            "per_exchange_features": rows,
        }

    def feedback_snapshot(self, config, signal, consensus, current_time):
        summary = self.feedback_service.summary(config)
        allowed, adaptive = self.feedback_service.evaluate(config, signal, consensus, current_time)
        return {
            "feedback_enabled": summary["feedback_enabled"],
            "long_recent_win_rate": summary["long"]["win_rate"],
            "short_recent_win_rate": summary["short"]["win_rate"],
            "long_loss_streak": summary["long"]["loss_streak"],
            "short_loss_streak": summary["short"]["loss_streak"],
            "adaptive_min_consensus_ratio": adaptive["adaptive_min_consensus_ratio"],
            "adaptive_min_valid_exchanges": adaptive["adaptive_min_valid_exchanges"],
            "blocked_side": adaptive["blocked_side"],
            "feedback_reject_reason": None if allowed else adaptive["feedback_reject_reason"],
            "long": summary["long"],
            "short": summary["short"],
        }

    def consensus_signal(self, config, current_time):
        consensus = self.consensus_snapshot(config, current_time)
        valid_count = consensus["valid_exchanges_count"]
        if valid_count == 0:
            consensus["reject_reason"] = "no_valid_consensus_snapshots"
            return None, consensus
        if valid_count < int(config.min_valid_exchanges):
            consensus["reject_reason"] = "not_enough_valid_exchanges"
            return None, consensus
        if not consensus["configured_exchange_valid"]:
            consensus["reject_reason"] = consensus.get("configured_exchange_reject_reason") or "configured_exchange_snapshot_missing_or_invalid"
            return None, consensus

        min_count = int(config.min_confirming_exchanges)
        min_ratio = float(config.min_consensus_ratio)
        long_ok = (
            consensus["confirming_long_count"] >= min_count
            and consensus["consensus_ratio_long"] >= min_ratio
            and consensus["average_momentum"] > 0
        )
        short_ok = (
            consensus["confirming_short_count"] >= min_count
            and consensus["consensus_ratio_short"] >= min_ratio
            and consensus["average_momentum"] < 0
        )
        if config.require_configured_exchange_signal:
            long_ok = long_ok and consensus["configured_exchange_long_signal"]
            short_ok = short_ok and consensus["configured_exchange_short_signal"]
        if long_ok:
            consensus["consensus_direction"] = "long"
            consensus["reject_reason"] = None
            return "long", consensus
        if short_ok:
            consensus["consensus_direction"] = "short"
            consensus["reject_reason"] = None
            return "short", consensus
        consensus["consensus_direction"] = "none"
        consensus["reject_reason"] = "no_consensus"
        return None, consensus

    def signal(self, config, state, features, current_time):
        risk_reason = self.risk_rejection(config, state, features, current_time)
        if risk_reason:
            consensus = self.consensus_snapshot(config, current_time) if config.consensus_enabled else {"per_exchange_features": []}
            consensus["reject_reason"] = risk_reason
            self.store_last_evaluation(
                config,
                features,
                features["imbalance"] > config.long_imbalance_threshold and features["short_momentum"] > 0,
                features["imbalance"] < config.short_imbalance_threshold and features["short_momentum"] < 0,
                "none",
                risk_reason,
                current_time,
                consensus,
            )
            self.reject(risk_reason, config, state)
            return None, consensus
        if config.consensus_enabled:
            return self.consensus_signal(config, current_time)
        if features["imbalance"] > config.long_imbalance_threshold and features["short_momentum"] > 0:
            return "long", {}
        if features["imbalance"] < config.short_imbalance_threshold and features["short_momentum"] < 0:
            return "short", {}
        return None, {"reject_reason": "no_signal"}

    def risk_rejection(self, config, state, features, current_time):
        if state.is_stopped:
            return "strategy_stopped"
        if features["spread_percent"] > config.max_spread_percent:
            return "spread_too_high"
        if self.open_positions_count(config) >= config.max_open_positions:
            return "max_open_positions_reached"
        if state.current_margin > self.available_equity(config):
            return "current_margin_exceeds_available_paper_equity"
        if abs(self.daily_loss(config, current_time)) >= config.max_daily_loss_usdt:
            return "daily_loss_exceeded"
        if abs(self.total_loss(config)) >= config.max_total_loss_usdt:
            return "total_loss_exceeded"
        if state.last_closed_at and state.last_trade_result == "loss":
            retry_at = state.last_closed_at + timedelta(seconds=int(config.cooldown_after_loss_seconds))
            if current_time < retry_at:
                return "cooldown_after_loss"
        if state.last_closed_at and state.last_trade_result == "win":
            retry_at = state.last_closed_at + timedelta(seconds=int(config.cooldown_after_win_seconds))
            if current_time < retry_at:
                return "cooldown_after_win"
        return None

    def resume_after_recovery_pause(self, config, state, current_time):
        if state.stop_reason != "max_recovery_pause" or not state.paused_until:
            return False
        if current_time < state.paused_until:
            return False
        state.current_step = 0
        state.current_margin = config.base_margin_usdt
        state.consecutive_losses = 0
        state.last_trade_result = None
        state.is_stopped = False
        state.stop_reason = None
        state.paused_until = None
        config.enabled = True
        db.session.commit()
        return True

    def decision_snapshot(self, config, state, features, side, current_time, consensus, margin, notional, entry_price):
        target_profit = margin * (float(config.take_profit_percent_of_margin) / 100)
        max_loss = margin * (float(config.stop_loss_percent_of_margin) / 100)
        return {
            "config": self.config_to_dict(config),
            "timestamp": current_time,
            "selected_side": side,
            "entry_price": entry_price,
            "take_profit_target_pnl": target_profit,
            "stop_loss_target_pnl": -max_loss,
            "current_recovery_step": state.current_step,
            "current_margin": margin,
            "current_notional": notional,
            "feedback_state": consensus.get("feedback") or {},
            "risk_decision": {
                "approved": True,
                "reason": None,
            },
            "consensus_decision": {
                "direction": consensus.get("consensus_direction"),
                "valid_exchanges_count": consensus.get("valid_exchanges_count"),
                "confirming_long_count": consensus.get("confirming_long_count"),
                "confirming_short_count": consensus.get("confirming_short_count"),
                "consensus_ratio_long": consensus.get("consensus_ratio_long"),
                "consensus_ratio_short": consensus.get("consensus_ratio_short"),
                "average_imbalance": consensus.get("average_imbalance"),
                "average_momentum": consensus.get("average_momentum"),
                "configured_exchange_imbalance": consensus.get("configured_exchange_imbalance"),
                "configured_exchange_spread": consensus.get("configured_exchange_spread"),
                "configured_exchange_momentum": consensus.get("configured_exchange_momentum"),
                "reject_reason": consensus.get("reject_reason"),
            },
            "signal": {
                "bid_volume_top_5": features.get("bid_volume_top_5"),
                "ask_volume_top_5": features.get("ask_volume_top_5"),
                "imbalance": features.get("imbalance"),
                "spread_percent": features.get("spread_percent"),
                "momentum": features.get("short_momentum"),
            },
        }

    def open_position(self, config, state, features, side, current_time, consensus=None):
        consensus = consensus or {}
        margin = float(state.current_margin or config.base_margin_usdt)
        notional = margin * float(config.leverage)
        entry_price = features["mid_price"]
        amount = notional / entry_price if entry_price else 0
        entry_reason = f"side={side}, consensus={consensus.get('consensus_direction')}, imbalance={features['imbalance']:.4f}, momentum={features['short_momentum']:.8f}, spread={features['spread_percent']:.4f}"
        per_exchange_features = consensus.get("per_exchange_features") or []
        decision_snapshot = self.decision_snapshot(config, state, features, side, current_time, consensus, margin, notional, entry_price)
        trade = StrategyRunTrade(
            strategy_run_id=self.active_run(config).id if self.active_run(config) else None,
            strategy_config_id=config.id,
            exchange=config.exchange,
            symbol=config.symbol,
            side=side,
            margin=margin,
            leverage=config.leverage,
            notional=notional,
            amount=amount,
            entry_price=entry_price,
            pnl=0,
            recovery_step=state.current_step,
            reason_open=entry_reason,
            opened_at=current_time,
            signal_consensus_direction=consensus.get("consensus_direction"),
            signal_valid_exchanges_count=consensus.get("valid_exchanges_count"),
            signal_confirming_long_count=consensus.get("confirming_long_count"),
            signal_confirming_short_count=consensus.get("confirming_short_count"),
            signal_consensus_ratio_long=consensus.get("consensus_ratio_long"),
            signal_consensus_ratio_short=consensus.get("consensus_ratio_short"),
            signal_average_imbalance=consensus.get("average_imbalance"),
            signal_average_momentum=consensus.get("average_momentum"),
            signal_configured_exchange_imbalance=consensus.get("configured_exchange_imbalance"),
            signal_configured_exchange_spread=consensus.get("configured_exchange_spread"),
            signal_configured_exchange_momentum=consensus.get("configured_exchange_momentum"),
            signal_entry_blocked_reason=consensus.get("reject_reason"),
            signal_per_exchange_features_json=json.dumps(per_exchange_features, default=str),
            decision_snapshot_json=json.dumps(decision_snapshot, default=str),
            per_exchange_features_json=json.dumps(per_exchange_features, default=str),
            consensus_direction=consensus.get("consensus_direction"),
            valid_exchanges_count=consensus.get("valid_exchanges_count"),
            confirming_long_count=consensus.get("confirming_long_count"),
            confirming_short_count=consensus.get("confirming_short_count"),
            consensus_ratio_long=consensus.get("consensus_ratio_long"),
            consensus_ratio_short=consensus.get("consensus_ratio_short"),
            average_imbalance=consensus.get("average_imbalance"),
            average_momentum=consensus.get("average_momentum"),
            configured_exchange_imbalance=consensus.get("configured_exchange_imbalance"),
            configured_exchange_spread=consensus.get("configured_exchange_spread"),
            configured_exchange_momentum=consensus.get("configured_exchange_momentum"),
            entry_reason=entry_reason,
        )
        state.last_opened_at = current_time
        db.session.add(trade)
        db.session.commit()
        payload = self.trade_to_dict(trade)
        logger.info("OrderBookRecovery position opened: trade_id=%s side=%s margin=%s entry=%s", trade.id, side, margin, entry_price)
        self.publisher.publish("orderbook_recovery.position_opened", payload)
        return payload

    def evaluate_open_trade(self, trade, current_price: float, state, config, current_time):
        pnl = self.calculate_pnl(trade.side, trade.entry_price, current_price, trade.notional)
        trade.pnl = pnl
        target_profit = trade.margin * (float(config.take_profit_percent_of_margin) / 100)
        max_loss = trade.margin * (float(config.stop_loss_percent_of_margin) / 100)
        if pnl >= target_profit:
            return self.close_trade(trade, current_price, pnl, "take_profit", state, config, current_time)
        if pnl <= -max_loss:
            return self.close_trade(trade, current_price, pnl, "stop_loss", state, config, current_time)
        db.session.commit()
        return None

    def close_trade(self, trade, exit_price, pnl, reason, state, config, current_time):
        trade.exit_price = exit_price
        trade.pnl = pnl
        trade.result = "win" if pnl > 0 else "loss"
        trade.reason_close = reason
        trade.closed_at = current_time
        trade.holding_seconds = (trade.closed_at - trade.opened_at).total_seconds() if trade.opened_at else None
        self.apply_recovery_after_close(state, config, trade.result, current_time)
        db.session.commit()
        payload = self.trade_to_dict(trade)
        logger.info("OrderBookRecovery position closed: trade_id=%s reason=%s pnl=%s", trade.id, reason, pnl)
        self.publisher.publish("orderbook_recovery.position_closed", payload)
        return payload

    def apply_recovery_after_close(self, state, config, result, current_time=None):
        state.last_trade_result = result
        state.last_closed_at = current_time or datetime.utcnow()
        if result == "win":
            state.current_step = 0
            state.consecutive_losses = 0
            state.current_margin = config.base_margin_usdt
            state.is_stopped = False
            state.stop_reason = None
            state.paused_until = None
            return state

        state.consecutive_losses += 1
        state.current_step += 1
        if state.current_step > config.max_recovery_steps:
            state.is_stopped = True
            state.stop_reason = "max_recovery_pause"
            state.paused_until = (current_time or datetime.utcnow()) + timedelta(seconds=int(config.cooldown_after_max_recovery_seconds))
            state.current_step = 0
            state.current_margin = config.base_margin_usdt
            state.consecutive_losses = 0
            config.enabled = False
            run = self.active_run(config)
            if run:
                run.status = "stopped"
                run.stopped_at = state.last_closed_at
                run.stop_reason = state.stop_reason
            return state
        state.current_margin = config.base_margin_usdt * (float(config.recovery_multiplier) ** state.current_step)
        return state

    def calculate_pnl(self, side: str, entry_price: float, current_price: float, notional: float) -> float:
        if side == "short":
            return ((entry_price - current_price) / entry_price) * notional
        return ((current_price - entry_price) / entry_price) * notional

    def reject(self, reason, config, state):
        payload = {"reason": reason, "state": self.state_payload(config, state)}
        logger.info("OrderBookRecovery rejected with reason: exchange=%s symbol=%s reason=%s", config.exchange, config.symbol, reason)
        self.publisher.publish("orderbook_recovery.rejected", payload)
        return payload

    def open_trade(self, config):
        return StrategyRunTrade.query.filter_by(strategy_config_id=config.id, closed_at=None).order_by(StrategyRunTrade.id.desc()).first()

    def open_positions_count(self, config):
        return StrategyRunTrade.query.filter_by(strategy_config_id=config.id, closed_at=None).count()

    def active_run(self, config):
        return StrategyRun.query.filter_by(strategy_config_id=config.id, status="running").order_by(StrategyRun.id.desc()).first()

    def closed_trades_query(self, config=None):
        query = StrategyRunTrade.query.filter(StrategyRunTrade.closed_at.isnot(None))
        if config:
            query = query.filter_by(strategy_config_id=config.id)
        return query

    def daily_loss(self, config, current_time=None):
        current_time = current_time or datetime.utcnow()
        start = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
        losses = [
            trade.pnl for trade in self.closed_trades_query(config)
            .filter(StrategyRunTrade.closed_at >= start, StrategyRunTrade.pnl < 0)
            .all()
        ]
        return sum(losses)

    def total_loss(self, config):
        losses = [trade.pnl for trade in self.closed_trades_query(config).filter(StrategyRunTrade.pnl < 0).all()]
        return sum(losses)

    def available_equity(self, config):
        realized = sum(trade.pnl for trade in self.closed_trades_query(config).all())
        return float(config.paper_equity_usdt) + realized

    def metrics(self):
        config = self.get_or_create_config()
        trades = self.closed_trades_query(config).filter_by(is_archived=False).all()
        archived_trades = self.closed_trades_query(config).filter_by(is_archived=True).all()
        return self.calculate_metrics(trades, config.paper_equity_usdt, self.open_trade(config), archived_trades)

    def metrics_for_run(self, run):
        config = db.session.get(OrderBookPatternStrategyConfig, run.strategy_config_id)
        trades = StrategyRunTrade.query.filter_by(strategy_run_id=run.id, is_archived=False).all()
        archived_trades = StrategyRunTrade.query.filter_by(strategy_run_id=run.id, is_archived=True).all()
        open_trade = StrategyRunTrade.query.filter_by(strategy_run_id=run.id, closed_at=None).first()
        return self.calculate_metrics(trades, config.paper_equity_usdt if config else 10000, open_trade, archived_trades)

    def calculate_metrics(self, trades, initial_equity, open_trade=None, archived_trades=None):
        archived_trades = archived_trades or []
        pnls = [trade.pnl for trade in trades]
        wins = [pnl for pnl in pnls if pnl > 0]
        losses = [pnl for pnl in pnls if pnl < 0]
        equity = float(initial_equity)
        peak = equity
        max_drawdown = 0
        for pnl in pnls:
            equity += pnl
            peak = max(peak, equity)
            max_drawdown = max(max_drawdown, peak - equity)
        streak_wins, streak_losses = self.current_streaks(trades)
        gross_profit = sum(wins)
        gross_loss = abs(sum(losses))
        return {
            "total_trades": len(trades),
            "win_trades": len(wins),
            "loss_trades": len(losses),
            "total_pnl": sum(pnls),
            "total_win_pnl": gross_profit,
            "total_loss_pnl": sum(losses),
            "net_pnl": sum(pnls),
            "gross_profit": gross_profit,
            "gross_loss": gross_loss,
            "win_rate": (len(wins) / len(trades) * 100) if trades else 0,
            "loss_rate": (len(losses) / len(trades) * 100) if trades else 0,
            "average_win": (gross_profit / len(wins)) if wins else 0,
            "average_loss": (sum(losses) / len(losses)) if losses else 0,
            "profit_factor": (gross_profit / gross_loss) if gross_loss else (gross_profit if gross_profit else 0),
            "max_drawdown": max_drawdown,
            "consecutive_wins": streak_wins,
            "consecutive_losses": streak_losses,
            "archived_trades_count": len(archived_trades),
            "archived_pnl": sum(trade.pnl for trade in archived_trades),
            "open_position": self.trade_to_dict(open_trade) if open_trade else None,
            "backtest": {
                "available": False,
                "reason": "Historical order book snapshots are not stored yet. Forward paper trading is supported.",
            },
        }

    def current_streaks(self, trades):
        consecutive_wins = 0
        consecutive_losses = 0
        for trade in sorted(trades, key=lambda item: item.closed_at or item.opened_at, reverse=True):
            if trade.pnl > 0 and consecutive_losses == 0:
                consecutive_wins += 1
                continue
            if trade.pnl < 0 and consecutive_wins == 0:
                consecutive_losses += 1
                continue
            break
        return consecutive_wins, consecutive_losses

    def state_payload(self, config, state):
        return {
            "config": self.config_to_dict(config),
            "recovery_state": self.state_to_dict(state),
            "status": self.status_for(config, state),
            "enabled": config.enabled,
            "exchange": config.exchange,
            "symbol": config.symbol,
            "open_position": self.trade_to_dict(self.open_trade(config)) if self.open_trade(config) else None,
            "last_evaluation": self.last_evaluation_for(config),
            "latest_snapshot": self.latest_snapshot_for(config),
            "last_order_book_snapshot_time": (self.latest_snapshot_for(config) or {}).get("updated_at"),
            "reason_if_not_trading": self.reason_if_not_trading(config, state),
            "metrics": self.metrics_without_state_query(config),
        }

    def metrics_without_state_query(self, config):
        trades = self.closed_trades_query(config).filter_by(is_archived=False).all()
        archived_trades = self.closed_trades_query(config).filter_by(is_archived=True).all()
        return self.calculate_metrics(trades, config.paper_equity_usdt, self.open_trade(config), archived_trades)

    def config_to_dict(self, config):
        return {
            "id": config.id,
            "exchange": config.exchange,
            "symbol": config.symbol,
            "base_margin_usdt": config.base_margin_usdt,
            "leverage": config.leverage,
            "max_recovery_steps": config.max_recovery_steps,
            "recovery_multiplier": config.recovery_multiplier,
            "take_profit_percent_of_margin": config.take_profit_percent_of_margin,
            "stop_loss_percent_of_margin": config.stop_loss_percent_of_margin,
            "max_daily_loss_usdt": config.max_daily_loss_usdt,
            "max_total_loss_usdt": config.max_total_loss_usdt,
            "max_open_positions": config.max_open_positions,
            "cooldown_after_loss_seconds": config.cooldown_after_loss_seconds,
            "cooldown_after_win_seconds": config.cooldown_after_win_seconds,
            "enabled": config.enabled,
            "paper_mode_only": config.paper_mode_only,
            "long_imbalance_threshold": config.long_imbalance_threshold,
            "short_imbalance_threshold": config.short_imbalance_threshold,
            "max_spread_percent": config.max_spread_percent,
            "momentum_window_snapshots": config.momentum_window_snapshots,
            "consensus_enabled": config.consensus_enabled,
            "min_valid_exchanges": config.min_valid_exchanges,
            "min_confirming_exchanges": config.min_confirming_exchanges,
            "min_consensus_ratio": config.min_consensus_ratio,
            "max_snapshot_age_seconds": config.max_snapshot_age_seconds,
            "require_configured_exchange_signal": config.require_configured_exchange_signal,
            "cooldown_after_max_recovery_seconds": config.cooldown_after_max_recovery_seconds,
            "feedback_enabled": config.feedback_enabled,
            "feedback_lookback_trades": config.feedback_lookback_trades,
            "side_loss_streak_limit": config.side_loss_streak_limit,
            "side_cooldown_seconds": config.side_cooldown_seconds,
            "min_side_win_rate": config.min_side_win_rate,
            "adaptive_consensus_boost": config.adaptive_consensus_boost,
            "adaptive_min_valid_exchanges_boost": config.adaptive_min_valid_exchanges_boost,
            "paper_equity_usdt": config.paper_equity_usdt,
            "created_at": config.created_at,
            "updated_at": config.updated_at,
        }

    def state_to_dict(self, state):
        return {
            "id": state.id,
            "strategy_config_id": state.strategy_config_id,
            "current_step": state.current_step,
            "current_margin": state.current_margin,
            "last_trade_result": state.last_trade_result,
            "consecutive_losses": state.consecutive_losses,
            "is_stopped": state.is_stopped,
            "stop_reason": state.stop_reason,
            "paused_until": state.paused_until,
            "last_closed_at": state.last_closed_at,
            "last_opened_at": state.last_opened_at,
            "updated_at": state.updated_at,
        }

    def trade_to_dict(self, trade):
        if not trade:
            return None
        return {
            "id": trade.id,
            "strategy_run_id": trade.strategy_run_id,
            "strategy_config_id": trade.strategy_config_id,
            "exchange": trade.exchange,
            "symbol": trade.symbol,
            "side": trade.side,
            "margin": trade.margin,
            "leverage": trade.leverage,
            "notional": trade.notional,
            "amount": trade.amount,
            "entry_price": trade.entry_price,
            "exit_price": trade.exit_price,
            "pnl": trade.pnl,
            "result": trade.result,
            "recovery_step": trade.recovery_step,
            "reason_open": trade.reason_open,
            "reason_close": trade.reason_close,
            "opened_at": trade.opened_at,
            "closed_at": trade.closed_at,
            "is_archived": trade.is_archived,
            "archived_at": trade.archived_at,
            "archive_reason": trade.archive_reason,
            "signal_consensus_direction": trade.signal_consensus_direction,
            "signal_valid_exchanges_count": trade.signal_valid_exchanges_count,
            "signal_confirming_long_count": trade.signal_confirming_long_count,
            "signal_confirming_short_count": trade.signal_confirming_short_count,
            "signal_consensus_ratio_long": trade.signal_consensus_ratio_long,
            "signal_consensus_ratio_short": trade.signal_consensus_ratio_short,
            "signal_average_imbalance": trade.signal_average_imbalance,
            "signal_average_momentum": trade.signal_average_momentum,
            "signal_configured_exchange_imbalance": trade.signal_configured_exchange_imbalance,
            "signal_configured_exchange_spread": trade.signal_configured_exchange_spread,
            "signal_configured_exchange_momentum": trade.signal_configured_exchange_momentum,
            "signal_entry_blocked_reason": trade.signal_entry_blocked_reason,
            "signal_per_exchange_features_json": trade.signal_per_exchange_features_json,
            "decision_snapshot_json": trade.decision_snapshot_json,
            "per_exchange_features_json": trade.per_exchange_features_json,
            "consensus_direction": trade.consensus_direction,
            "valid_exchanges_count": trade.valid_exchanges_count,
            "confirming_long_count": trade.confirming_long_count,
            "confirming_short_count": trade.confirming_short_count,
            "consensus_ratio_long": trade.consensus_ratio_long,
            "consensus_ratio_short": trade.consensus_ratio_short,
            "average_imbalance": trade.average_imbalance,
            "average_momentum": trade.average_momentum,
            "configured_exchange_imbalance": trade.configured_exchange_imbalance,
            "configured_exchange_spread": trade.configured_exchange_spread,
            "configured_exchange_momentum": trade.configured_exchange_momentum,
            "entry_reason": trade.entry_reason,
            "holding_seconds": trade.holding_seconds,
        }

    def run_to_dict(self, run):
        return {
            "id": run.id,
            "strategy_config_id": run.strategy_config_id,
            "status": run.status,
            "started_at": run.started_at,
            "stopped_at": run.stopped_at,
            "stop_reason": run.stop_reason,
            "metrics": self.metrics_for_run(run),
        }
