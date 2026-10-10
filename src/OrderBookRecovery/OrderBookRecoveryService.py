from collections import defaultdict, deque
from datetime import datetime, timedelta
from decimal import Decimal
from threading import Thread
from io import BytesIO, StringIO
import csv
import json
import logging
import random
import time
from uuid import uuid4
from types import SimpleNamespace
import math

from flask import jsonify, make_response, send_file
from sqlalchemy import inspect
from sqlalchemy import or_
from sqlalchemy.exc import IntegrityError

from src import db
from src.OrderBookRecovery.FuturesSnapshotStore import FuturesSnapshotStore as OrderBookSnapshotStore
from src.Exchange.ExchangeModel import Exchange
from src.OrderBookRecovery.OrderBookRecoveryModel import (
    MLFeatureSnapshot,
    MLMarketPriceHistory,
    MLMarketSnapshot,
    MLMarketSnapshotExchangeLabel,
    OrderBookPatternStrategyConfig,
    RecoveryState,
    StrategyRun,
    StrategyRunTrade,
    ExecutionSlot,
    PaperSession,
)
from src.OrderBookRecovery.OrderBookNormalizer import OrderBookNormalizer
from src.OrderBookRecovery.LiveExecutionService import LiveExecutionService, LiveExecutionError, SubmissionUnknown, OrderNotFilled
from src.OrderBookRecovery.MLPredictionService import MLPredictionService
from src.OrderBookRecovery.SignalFeedbackService import SignalFeedbackService
from src.OrderBookRecovery.SignalRules import consensus_side, direction_rejections
from src.OrderBookRecovery.DepthExecution import consume_book
from src.OrderBookRecovery.PositionGuardian import PositionGuardian
from src.OrderBookRecovery.PaperContractRules import executable_amount
from src.TradingPair.TradingPairModel import TradingPair
from src.Socket.EventPublisher import EventPublisher
from src.__Parents.Response import Response


logger = logging.getLogger(__name__)


class OrderBookRecoveryService(Response):
    strategy_type = "order_book_pattern_recovery"
    _mid_price_history = defaultdict(lambda: deque(maxlen=200))
    _last_price_timestamp = {}
    _live_equity = {}
    _last_evaluations = {}
    _last_hook_seen_at = None
    _last_hook_snapshot = None
    _last_matching_hooks = {}
    _last_mismatch_hooks = {}
    _pending_entries = {}
    _last_confirmation_results = {}
    _live_market_infos = {}
    _ml_labeler_started = False
    _signal_diagnostics = defaultdict(lambda: deque(maxlen=500))
    _signal_counters = defaultdict(lambda: {
        "long_signals_count": 0,
        "short_signals_count": 0,
        "long_opened_count": 0,
        "short_opened_count": 0,
        "raw_long_threshold_hits": 0,
        "raw_short_threshold_hits": 0,
        "long_consensus_passed_count": 0,
        "short_consensus_passed_count": 0,
        "long_blocked_count": 0,
        "short_blocked_count": 0,
        "final_long_count": 0,
        "final_short_count": 0,
    })

    def __init__(self, publisher=None, live_execution_service=None, ml_prediction_service=None):
        self.publisher = publisher or EventPublisher()
        self.feedback_service = SignalFeedbackService()
        self.live_execution_service = live_execution_service or LiveExecutionService()
        self.ml_prediction_service = ml_prediction_service or MLPredictionService()

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

    def config_raw_response(self):
        config = self.get_or_create_config()
        db.session.flush()
        db.session.expire(config)
        reloaded = db.session.get(OrderBookPatternStrategyConfig, config.id)
        columns = [column["name"] for column in inspect(db.engine).get_columns(OrderBookPatternStrategyConfig.__tablename__)]
        return self.response_ok({
            "config_id": reloaded.id,
            "db_ml_mode": getattr(reloaded, "ml_mode", None),
            "serialized_ml_mode": self.config_to_dict(reloaded).get("ml_mode"),
            "model_default_ml_mode": OrderBookPatternStrategyConfig.ml_mode.default.arg,
            "ml_mode_column_exists": "ml_mode" in columns,
            "known_columns": columns,
        })

    def options_response(self):
        exchanges = Exchange.query.filter_by(enabled=True).order_by(Exchange.index.asc()).all()
        payload = []
        for exchange in exchanges:
            pairs = (
                TradingPair.query
                .filter_by(exchange_id=exchange.id, enabled=True)
                .order_by(TradingPair.index.asc())
                .all()
            )
            payload.append({
                "id": exchange.id,
                "title": exchange.title,
                "name": exchange.title,
                "is_active": bool(exchange.enabled),
                "pairs": [
                    {
                        "id": pair.id,
                        "pair": pair.pair,
                        "normalized_symbol": self.normalize_symbol(pair.pair),
                        "is_active": bool(pair.enabled),
                    }
                    for pair in pairs
                ],
            })
        return self.response_ok({"exchanges": payload})

    def validation_error(self, code: str):
        return make_response(jsonify(success=False, obj={"msg": code, "code": code}), 400)

    def resolve_config_selection(self, body: dict):
        exchange_id = body.get("exchange_id")
        trading_pair_id = body.get("trading_pair_id")
        if exchange_id in (None, "") and trading_pair_id in (None, ""):
            return None, None, None
        if exchange_id in (None, "") or trading_pair_id in (None, ""):
            return None, None, "invalid_pair_for_exchange"
        try:
            exchange_id = int(exchange_id)
            trading_pair_id = int(trading_pair_id)
        except (TypeError, ValueError):
            return None, None, "invalid_pair_for_exchange"
        exchange = Exchange.query.filter_by(id=exchange_id, enabled=True).first()
        if not exchange:
            return None, None, "invalid_exchange"
        trading_pair = TradingPair.query.filter_by(id=trading_pair_id, exchange_id=exchange.id, enabled=True).first()
        if not trading_pair:
            return exchange, None, "invalid_pair_for_exchange"
        return exchange, trading_pair, None

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
            "use_median_imbalance",
            "imbalance_anomaly_min",
            "imbalance_anomaly_max",
            "exclude_anomalous_imbalance",
            "entry_mode",
            "confirmation_delay_seconds",
            "confirmation_max_wait_seconds",
            "confirmation_require_same_direction",
            "confirmation_require_momentum_improvement",
            "confirmation_min_momentum_delta",
            "confirmation_require_consensus_still_valid",
            "execution_mode",
            "live_enabled_confirmation",
            "live_kill_switch",
            "live_max_margin_usdt",
            "live_max_daily_loss_usdt",
            "live_max_total_loss_usdt",
            "live_order_type",
            "live_reduce_only_close",
            "live_open_failed_cooldown_seconds",
            "live_fee_filter_enabled",
            "live_fee_filter_taker_fee_percent",
            "momentum_confirmation_enabled",
            "side_quality_filter_enabled",
            "side_quality_lookback_trades",
            "side_quality_cooldown_seconds",
            "ml_mode",
            "ml_snapshot_capture_enabled",
            "ml_snapshot_sample_rate",
            "ml_label_horizons_seconds",
            "ml_max_snapshots_per_hour",
            "cooldown_after_max_recovery_seconds",
            "feedback_enabled",
            "feedback_lookback_trades",
            "side_loss_streak_limit",
            "side_cooldown_seconds",
            "min_side_win_rate",
            "adaptive_consensus_boost",
            "adaptive_min_valid_exchanges_boost",
            "signal_diagnostics_max_rows",
            "paper_equity_usdt",
            "risk_per_trade_percent", "max_position_margin_usdt", "emergency_entry_block",
            "paper_taker_fee_percent", "paper_latency_ms", "pending_entry_ttl_seconds",
            "max_consecutive_losses",
            "max_leverage",
        }
        for key in allowed:
            if key in overrides:
                setattr(config, key, overrides[key])
        if config.entry_mode not in {"instant", "two_step_confirmation"}:
            config.entry_mode = "instant"
        if config.execution_mode not in {"paper", "live"}:
            config.execution_mode = "paper"
        if config.live_order_type not in {"market", "limit"}:
            config.live_order_type = "market"
        config.live_fee_filter_taker_fee_percent = max(0, float(config.live_fee_filter_taker_fee_percent or 0))
        config.side_quality_lookback_trades = max(1, int(config.side_quality_lookback_trades or 5))
        config.side_quality_cooldown_seconds = max(0, int(config.side_quality_cooldown_seconds or 0))
        if config.ml_mode not in {"disabled", "shadow"}:
            config.ml_mode = "disabled"
        config.ml_snapshot_sample_rate = min(1, max(0, float(config.ml_snapshot_sample_rate if config.ml_snapshot_sample_rate is not None else 1)))
        config.ml_label_horizons_seconds = json.dumps(self.ml_label_horizons(config))
        config.ml_max_snapshots_per_hour = max(1, int(config.ml_max_snapshots_per_hour or 10000))
        config.signal_diagnostics_max_rows = min(500, max(20, int(config.signal_diagnostics_max_rows or 100)))
        config.paper_mode_only = True
        ttl = float(config.pending_entry_ttl_seconds)
        if not math.isfinite(ttl) or not 0.1 <= ttl <= 3600:
            raise ValueError("invalid_pending_entry_ttl_seconds")
        for name in ("risk_per_trade_percent", "max_position_margin_usdt", "base_margin_usdt", "leverage", "stop_loss_percent_of_margin"):
            value = float(getattr(config, name))
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"invalid_{name}")
        config.risk_per_trade_percent = min(float(config.risk_per_trade_percent), 1.0)
        config.max_open_positions = 1
        config.max_consecutive_losses = max(1, int(config.max_consecutive_losses))
        config.max_leverage = min(10, max(1, float(config.max_leverage)))

    def update_config(self, body: dict):
        config = self.get_or_create_config()
        if self.open_trade(config) and any(key in body for key in ("exchange", "symbol", "exchange_id", "trading_pair_id", "execution_mode")):
            return self.validation_error("cannot_change_execution_config_with_open_position")
        logger.info("OrderBookRecovery config PATCH received keys=%s ml_mode=%s", sorted(body.keys()), body.get("ml_mode"))
        exchange, trading_pair, error = self.resolve_config_selection(body)
        if error:
            return self.validation_error(error)
        self.apply_config_overrides(config, body)
        logger.info("OrderBookRecovery config after overrides id=%s ml_mode=%s", config.id, config.ml_mode)
        if exchange and trading_pair:
            config.exchange_id = exchange.id
            config.trading_pair_id = trading_pair.id
            config.exchange = exchange.title
            config.symbol = trading_pair.pair
        if "enabled" in body:
            config.enabled = body["enabled"]
        config.paper_mode_only = True
        state = self.get_or_create_state(config)
        if state.current_margin <= 0:
            state.current_margin = config.base_margin_usdt
        db.session.commit()
        db.session.expire(config)
        reloaded = db.session.get(OrderBookPatternStrategyConfig, config.id)
        logger.info("OrderBookRecovery config committed/reloaded id=%s ml_mode=%s", reloaded.id, reloaded.ml_mode)
        return self.response_ok(self.config_to_dict(reloaded))

    @staticmethod
    def median(values):
        values = sorted(value for value in values if value is not None)
        if not values:
            return None
        middle = len(values) // 2
        if len(values) % 2:
            return values[middle]
        return (values[middle - 1] + values[middle]) / 2

    def start_paper(self):
        config = self.get_or_create_config()
        config = self.lock_config(config.id)
        if config.execution_mode == "paper" and not config.paper_session_id and not self.open_trade(config):
            self.create_paper_session(config)
        if config.execution_mode == "live":
            reason = self.live_start_rejection(config)
            if reason:
                return self.response(False, {"msg": reason}, 400)
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
        self.start_ml_market_labeler()
        payload = self.state_payload(config, state)
        self.publisher.publish("orderbook_recovery.started", payload)
        return self.response_ok(payload)

    def live_start_rejection(self, config):
        state = self.get_or_create_state(config)
        reason = self.live_execution_service.validate_enabled(config, state.current_margin or config.base_margin_usdt)
        if reason:
            return reason
        snapshot = self.snapshot_for(config.exchange, config.symbol)
        if not snapshot:
            return "live_requires_fresh_snapshot"
        features, error = self.features(config, snapshot)
        if not features:
            return error or "live_requires_valid_snapshot"
        row = self.exchange_feature(config, snapshot, datetime.utcnow())
        if not row.get("valid"):
            return row.get("reject_reason") or "live_requires_fresh_snapshot"
        if abs(self.live_daily_loss(config, datetime.utcnow())) >= float(config.live_max_daily_loss_usdt):
            return "live_daily_loss_exceeded"
        if abs(self.live_total_loss(config)) >= float(config.live_max_total_loss_usdt):
            return "live_total_loss_exceeded"
        if self.open_live_trade(config):
            return "live_position_already_open"
        try:
            client = self.live_execution_service.client(config)
            balance = client.fetch_balance({"type": "swap"})
            equity = float((balance.get("free") or {}).get("USDT") or 0)
            self._live_equity[config.id] = (datetime.utcnow(), equity)
            if equity <= 0:
                return "live_balance_unavailable"
            market = self.live_execution_service.market(client, config.symbol)
            market_info = self.live_execution_service.market_info(market, configured_symbol=config.symbol)
            self.__class__._live_market_infos[self.debug_key(config)] = market_info
            logger.info("OrderBookRecovery live startup market resolved: %s", market_info)
        except Exception as error:
            self.__class__._live_market_infos[self.debug_key(config)] = self.live_execution_service.market_info(error=str(error), configured_symbol=config.symbol)
            return str(error)
        return None

    def live_market_debug(self, config):
        return self.__class__._live_market_infos.get(
            self.debug_key(config),
            self.live_execution_service.market_info(configured_symbol=config.symbol),
        )

    def stop(self, reason="manual_stop"):
        config = self.get_or_create_config()
        config = self.lock_config(config.id)
        config.enabled = False
        for trade in StrategyRunTrade.query.filter_by(strategy_config_id=config.id, execution_mode="paper", live_status="paper_pending", closed_at=None).all():
            self.cancel_paper_entry(trade, "paused", datetime.utcnow(), commit=False)
        self.clear_pending_entry(config, "cancelled", "manual_stop")
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

    def lock_config(self, config_id):
        # Serialize Pause, reservation and paper fill across processes (PostgreSQL).
        return OrderBookPatternStrategyConfig.query.filter_by(id=config_id).populate_existing().with_for_update().one()

    def create_paper_session(self, config):
        previous = db.session.get(PaperSession, config.paper_session_id) if config.paper_session_id else None
        if previous:
            previous.ended_at = datetime.utcnow()
        session = PaperSession(id=str(uuid4()), strategy_config_id=config.id,
                               initial_equity_usdt=float(config.paper_equity_usdt))
        db.session.add(session)
        db.session.flush()
        config.paper_session_id = session.id
        state = RecoveryState.query.filter_by(strategy_config_id=config.id).first()
        if state:
            state.current_step, state.current_margin, state.consecutive_losses = 0, config.base_margin_usdt, 0
            state.last_closed_at = state.last_opened_at = state.last_trade_result = state.paused_until = None
        return session

    def new_paper_session(self):
        config = self.lock_config(self.get_or_create_config().id)
        if config.execution_mode != "paper" or config.enabled or self.open_trade(config) or db.session.get(ExecutionSlot, config.id):
            return self.response_err_msg("paper_session_requires_paused_and_flat")
        self.create_paper_session(config)
        state = self.get_or_create_state(config)
        state.current_step, state.current_margin, state.consecutive_losses = 0, config.base_margin_usdt, 0
        state.last_closed_at = state.last_opened_at = state.last_trade_result = state.paused_until = None
        state.is_stopped, state.stop_reason = True, "manual_stop"
        self.clear_pending_entry(config, "cancelled", "new_paper_session")
        db.session.commit()
        return self.response_ok(self.state_payload(config, state))

    def paper_initial_equity(self, config):
        session_id = getattr(config, "paper_session_id", None)
        session = db.session.get(PaperSession, session_id) if session_id else None
        return float(session.initial_equity_usdt if session else config.paper_equity_usdt)

    def cancel_paper_entry(self, trade, reason, now, commit=True):
        if trade.closed_at or trade.live_status != "paper_pending":
            return self.trade_to_dict(trade)
        trade.live_status, trade.result = "paper_cancelled", "cancelled"
        trade.reason_close, trade.closed_at, trade.pnl = f"pending_entry_{reason}", now, 0
        trade.live_error = trade.reason_close
        self.observe_signal("pending_cancelled", trade.reason_close)
        slot = db.session.get(ExecutionSlot, trade.strategy_config_id)
        if slot and slot.trade_id == trade.id:
            db.session.delete(slot)
        if commit:
            db.session.commit()
        return self.trade_to_dict(trade)

    def margin_related_stop_reason(self, reason):
        return reason in {
            "current_margin_exceeds_available_paper_equity",
            "live_margin_exceeds_limit",
            "margin_limit_exceeded",
            "insufficient_balance",
            "live_balance_insufficient",
            "max_recovery_pause",
        }

    def clear_margin_related_pause(self, state):
        if self.margin_related_stop_reason(state.stop_reason):
            state.is_stopped = False
            state.stop_reason = None
            state.paused_until = None

    def reset_recovery(self):
        config = self.get_or_create_config()
        state = self.get_or_create_state(config)
        if self.open_trade(config):
            return self.response(False, {"msg": "cannot_change_margin_with_open_position"}, 400)
        state.current_step = 0
        state.current_margin = config.base_margin_usdt
        state.consecutive_losses = 0
        state.last_trade_result = None
        state.last_manual_recovery_reset_at = datetime.utcnow()
        self.clear_margin_related_pause(state)
        db.session.commit()
        logger.info("OrderBookRecovery recovery reset manually: exchange=%s symbol=%s margin=%s", config.exchange, config.symbol, state.current_margin)
        return self.response_ok(self.state_payload(config, state))

    def set_current_margin(self, body: dict):
        config = self.get_or_create_config()
        state = self.get_or_create_state(config)
        if self.open_trade(config):
            return self.response(False, {"msg": "cannot_change_margin_with_open_position"}, 400)
        try:
            current_margin = float(body.get("current_margin"))
        except (TypeError, ValueError):
            return self.response(False, {"msg": "invalid_current_margin"}, 400)
        if current_margin <= 0:
            return self.response(False, {"msg": "invalid_current_margin"}, 400)
        if config.execution_mode == "live" and current_margin > float(config.live_max_margin_usdt):
            return self.response(False, {"msg": "live_margin_exceeds_limit"}, 400)
        state.current_step = 0
        state.current_margin = current_margin
        state.consecutive_losses = 0
        state.last_trade_result = None
        state.stop_reason = "manual_margin_override"
        state.paused_until = None
        state.is_stopped = False
        state.last_manual_margin_override_at = datetime.utcnow()
        state.last_manual_margin_override_value = current_margin
        db.session.commit()
        logger.info("OrderBookRecovery current margin overridden manually: exchange=%s symbol=%s margin=%s", config.exchange, config.symbol, current_margin)
        return self.response_ok(self.state_payload(config, state))

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
        self.start_ml_market_labeler()
        return self.response_ok({
            "run_id": run.id,
            "status": run.status,
            "duration_minutes": duration_minutes,
            "config": self.config_to_dict(config),
        })

    def start_ml_market_labeler(self):
        if self.__class__._ml_labeler_started:
            return
        self.__class__._ml_labeler_started = True

        def worker():
            from src import app

            while True:
                time.sleep(5)
                with app.app_context():
                    try:
                        configs = OrderBookPatternStrategyConfig.query.filter(
                            OrderBookPatternStrategyConfig.ml_mode == "shadow",
                            OrderBookPatternStrategyConfig.ml_snapshot_capture_enabled.is_(True),
                        ).all()
                        for config in configs:
                            self.label_pending_market_snapshots(config)
                    except Exception as error:
                        db.session.rollback()
                        logger.warning("ML market snapshot labeler failed: %s", error)
                    finally:
                        db.session.remove()

        Thread(target=worker, daemon=True).start()

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
                finally:
                    db.session.remove()

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

        config = self.trade_config(config, trade)
        if trade.execution_mode == "paper":
            # Persist a close request even without executable data; never pretend it filled.
            return self.response_ok(self.close_trade(trade, 0, trade.pnl, "manual_close", state, config, datetime.utcnow()))
        snapshot = self.snapshot_for(trade.exchange, trade.symbol)
        if not snapshot:
            return self.response_err_msg("cannot_close_without_valid_market_price")
        normalized, reject_reason = OrderBookNormalizer.normalize(snapshot.get("order_book") or {})
        if reject_reason:
            return self.response_err_msg("cannot_close_without_valid_market_price")
        if not self.exchange_feature(config, snapshot, datetime.utcnow(), protective=True).get("valid"):
            return self.response_err_msg("cannot_close_without_valid_market_price")

        current_time = datetime.utcnow()
        features, reject_reason = self.features(config, snapshot)
        exit_price = features["mid_price"]
        pnl = self.calculate_pnl(trade.side, trade.entry_price, exit_price, trade.notional)
        reason = (body or {}).get("reason") or "manual_close"
        if reason != "manual_close":
            reason = "manual_close"
        payload = self.close_trade(trade, exit_price, pnl, reason, state, config, current_time)
        if not payload:
            return self.response(False, {"msg": "live_close_failed", "trade": self.trade_to_dict(trade)}, 400)
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

    def delete_archived_trade(self, trade_id: int):
        trade = db.session.get(StrategyRunTrade, trade_id)
        if not trade:
            return self.response_not_found("Trade not found")
        if not trade.is_archived:
            return self.response(False, {"msg": "cannot_delete_non_archived_trade"}, 400)
        db.session.delete(trade)
        db.session.commit()
        return self.response_ok({"deleted_trade_id": trade_id})

    def delete_all_archived_trades(self):
        trades = StrategyRunTrade.query.filter_by(is_archived=True).all()
        deleted_count = len(trades)
        for trade in trades:
            db.session.delete(trade)
        db.session.commit()
        return self.response_ok({"deleted_count": deleted_count})

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
            "pnl", "gross_pnl", "net_pnl", "total_fee", "result", "close_reason", "opened_at", "closed_at", "holding_seconds",
            "consensus_direction", "valid_exchanges_count", "confirming_long_count", "confirming_short_count",
            "consensus_ratio_long", "consensus_ratio_short", "average_imbalance", "average_momentum",
            "median_imbalance", "raw_average_imbalance", "anomalous_exchanges_count",
            "excluded_anomalous_imbalance_exchanges",
            "configured_exchange_imbalance", "configured_exchange_spread", "configured_exchange_momentum", "entry_reason",
            "entry_mode", "confirmation_delay_actual_seconds", "first_signal_snapshot_json", "confirmation_snapshot_json",
            "execution_mode", "live_status", "live_exchange_order_id", "live_close_order_id", "live_entry_fee", "live_exit_fee", "live_error",
            "exchange_tp_order_id", "exchange_sl_order_id", "exchange_tp_price", "exchange_sl_price", "tp_sl_protected", "tp_sl_error", "tp_sl_created_at",
            "exit_price_fallback_used", "exit_price_warning", "pnl_source",
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
            "gross_pnl": trade.gross_pnl,
            "net_pnl": trade.net_pnl,
            "total_fee": trade.total_fee,
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
            "median_imbalance": trade.median_imbalance or trade.signal_median_imbalance or (snapshot.get("consensus_decision") or {}).get("median_imbalance"),
            "raw_average_imbalance": trade.raw_average_imbalance or trade.signal_raw_average_imbalance or (snapshot.get("consensus_decision") or {}).get("raw_average_imbalance"),
            "anomalous_exchanges_count": trade.anomalous_exchanges_count if trade.anomalous_exchanges_count is not None else (
                trade.signal_anomalous_exchanges_count if trade.signal_anomalous_exchanges_count is not None else (snapshot.get("consensus_decision") or {}).get("anomalous_exchanges_count")
            ),
            "excluded_anomalous_imbalance_exchanges": (
                trade.excluded_anomalous_imbalance_exchanges_json
                or trade.signal_excluded_anomalous_imbalance_exchanges_json
                or json.dumps((snapshot.get("consensus_decision") or {}).get("excluded_anomalous_imbalance_exchanges") or [])
            ),
            "average_momentum": trade.average_momentum or trade.signal_average_momentum,
            "configured_exchange_imbalance": trade.configured_exchange_imbalance or trade.signal_configured_exchange_imbalance,
            "configured_exchange_spread": trade.configured_exchange_spread or trade.signal_configured_exchange_spread,
            "configured_exchange_momentum": trade.configured_exchange_momentum or trade.signal_configured_exchange_momentum,
            "entry_reason": trade.entry_reason or trade.reason_open,
            "entry_mode": trade.entry_mode or "instant",
            "confirmation_delay_actual_seconds": trade.confirmation_delay_actual_seconds,
            "first_signal_snapshot_json": trade.first_signal_snapshot_json,
            "confirmation_snapshot_json": trade.confirmation_snapshot_json,
            "execution_mode": trade.execution_mode or "paper",
            "live_status": trade.live_status,
            "live_exchange_order_id": trade.live_exchange_order_id,
            "live_close_order_id": trade.live_close_order_id,
            "live_entry_fee": trade.live_entry_fee,
            "live_exit_fee": trade.live_exit_fee,
            "live_error": trade.live_error,
            "exchange_tp_order_id": trade.exchange_tp_order_id,
            "exchange_sl_order_id": trade.exchange_sl_order_id,
            "exchange_tp_price": trade.exchange_tp_price,
            "exchange_sl_price": trade.exchange_sl_price,
            "tp_sl_protected": trade.tp_sl_protected,
            "tp_sl_error": trade.tp_sl_error,
            "tp_sl_created_at": trade.tp_sl_created_at,
            "exit_price_fallback_used": trade.exit_price_fallback_used,
            "exit_price_warning": trade.exit_price_warning,
            "pnl_source": trade.pnl_source,
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
        ml_snapshots = MLFeatureSnapshot.query.filter_by(trade_id=trade.id).order_by(MLFeatureSnapshot.timestamp.desc()).all()
        latest_ml = self.ml_snapshot_to_dict(ml_snapshots[0]) if ml_snapshots else None
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
                "median_imbalance": trade.median_imbalance or trade.signal_median_imbalance,
                "raw_average_imbalance": trade.raw_average_imbalance or trade.signal_raw_average_imbalance,
                "anomalous_exchanges_count": trade.anomalous_exchanges_count if trade.anomalous_exchanges_count is not None else trade.signal_anomalous_exchanges_count,
                "excluded_anomalous_imbalance_exchanges": self.parse_json(
                    trade.excluded_anomalous_imbalance_exchanges_json
                    or trade.signal_excluded_anomalous_imbalance_exchanges_json
                ) or [],
                "average_momentum": trade.average_momentum or trade.signal_average_momentum,
                "configured_exchange_imbalance": trade.configured_exchange_imbalance or trade.signal_configured_exchange_imbalance,
                "configured_exchange_spread": trade.configured_exchange_spread or trade.signal_configured_exchange_spread,
                "configured_exchange_momentum": trade.configured_exchange_momentum or trade.signal_configured_exchange_momentum,
                "entry_reason": trade.entry_reason or trade.reason_open,
            },
            "consensus": (self.parse_json(trade.decision_snapshot_json) or {}).get("consensus_decision") or {},
            "feedback": (self.parse_json(trade.decision_snapshot_json) or {}).get("feedback_state") or {},
            "risk": (self.parse_json(trade.decision_snapshot_json) or {}).get("risk_decision") or {},
            "ml": latest_ml or (self.parse_json(trade.decision_snapshot_json) or {}).get("ml_prediction") or {},
            "ml_snapshots": [self.ml_snapshot_to_dict(snapshot) for snapshot in ml_snapshots],
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

    def observe_signal(self, stage, reason=None, details=None):
        observer = getattr(self, "signal_observer", None)
        if observer:
            observer(stage, reason, details or {})

    def evaluate(self, config=None, snapshot=None, current_time=None):
        config = config or self.get_or_create_config()
        state = self.get_or_create_state(config)
        current_time = current_time or datetime.utcnow()
        self.observe_signal("evaluation", details={"timestamp": current_time.isoformat()})
        self.resume_after_recovery_pause(config, state, current_time)

        open_trade = self.open_trade(config)
        if open_trade:
            self.observe_signal("position_management")
            if open_trade.execution_mode == "live" and not PositionGuardian(self).prepare_legacy(config, open_trade):
                return self.trade_to_dict(open_trade)
            frozen = self.trade_config(config, open_trade)
            if open_trade.live_status == "paper_pending":
                return self.evaluate_open_trade(open_trade, 0, state, frozen, current_time)
            managed_snapshot = snapshot or self.snapshot_for(open_trade.exchange, open_trade.symbol)
            if managed_snapshot:
                row = self.exchange_feature(frozen, managed_snapshot, current_time, protective=True)
                if row.get("valid"):
                    return self.evaluate_open_trade(open_trade, row["mid_price"], state, frozen, current_time)
            if open_trade.execution_mode == "paper":
                open_trade.paper_exit_status = "unresolved_no_fresh_valid_book"
                db.session.commit()
            return self.trade_to_dict(open_trade)

        if not config.enabled or state.is_stopped:
            reason = self.reason_if_not_trading(config, state)
            self.observe_signal("inactive", reason)
            self.store_last_evaluation(config, reject_reason=reason, evaluated_at=current_time)
            self.record_signal_diagnostic(config, reject_reason=reason, evaluated_at=current_time)
            return None

        snapshot = snapshot or self.snapshot_for(config.exchange, config.symbol)
        if not snapshot:
            self.observe_signal("snapshot_rejected", "no_valid_order_book_snapshot")
            self.store_last_evaluation(config, reject_reason="no_valid_order_book_snapshot", evaluated_at=current_time)
            self.record_signal_diagnostic(config, reject_reason="no_valid_order_book_snapshot", evaluated_at=current_time)
            return self.reject("no_valid_order_book_snapshot", config, state)

        normalized, reject_reason = OrderBookNormalizer.normalize(snapshot.get("order_book") or {})
        if reject_reason:
            self.observe_signal("snapshot_rejected", reject_reason)
            self.store_last_evaluation(config, reject_reason=reject_reason, evaluated_at=current_time)
            self.record_signal_diagnostic(config, reject_reason=reject_reason, evaluated_at=current_time)
            return self.reject(reject_reason, config, state)
        configured = self.exchange_feature(config, snapshot, current_time)
        if not configured.get("valid"):
            reason = configured.get("reject_reason") or "invalid_configured_snapshot"
            self.observe_signal("snapshot_rejected", reason, configured)
            if not config.consensus_enabled:
                self.store_last_evaluation(config, reject_reason=reason, evaluated_at=current_time)
                return self.reject(reason, config, state)
        else:
            self.observe_signal("snapshot_valid", details=configured)
        features, reject_reason = self.features(config, snapshot, record_history=bool(configured.get("valid")))
        if not features:
            self.store_last_evaluation(config, reject_reason=reject_reason, evaluated_at=current_time)
            self.record_signal_diagnostic(config, reject_reason=reject_reason, evaluated_at=current_time)
            return self.reject(reject_reason, config, state)

        long_signal = features["imbalance"] > config.long_imbalance_threshold and features["short_momentum"] > 0
        short_signal = features["imbalance"] <= config.short_imbalance_threshold and features["short_momentum"] < 0
        open_trade = self.open_trade(config)
        if open_trade:
            self.store_last_evaluation(config, features, long_signal, short_signal, "none", None, current_time)
            self.create_ml_market_snapshot(
                config,
                state,
                features,
                self.consensus_snapshot(config, current_time) if config.consensus_enabled else {},
                "open_position",
                open_trade.side,
                None,
                current_time,
            )
            closed = self.evaluate_open_trade(open_trade, features["mid_price"], state, config, current_time)
            if closed:
                logger.info("OrderBookRecovery evaluation result: managed open position trade_id=%s result=closed", open_trade.id)
                return closed
            logger.info("OrderBookRecovery evaluation result: managed open position trade_id=%s result=hold", open_trade.id)
            return self.trade_to_dict(open_trade)

        signal, consensus = self.signal(config, state, features, current_time)
        feedback = self.feedback_snapshot(config, signal, consensus, current_time)
        if signal and feedback.get("feedback_reject_reason"):
            self.observe_signal("feedback_rejected", feedback["feedback_reject_reason"])
            signal = None
            consensus["reject_reason"] = feedback["feedback_reject_reason"]
        consensus["feedback"] = feedback
        if signal:
            self.observe_signal("feedback_passed")
        if signal:
            protection_reject_reason = self.profit_protection_rejection(config, state, features, signal, consensus, current_time)
            if protection_reject_reason:
                self.observe_signal("entry_filter_rejected", protection_reject_reason)
                signal = None
                consensus["reject_reason"] = protection_reject_reason
            else:
                self.observe_signal("entry_filters_passed")
        reject_reason = consensus.get("reject_reason") or (self.risk_rejection(config, state, features, current_time) if not signal else None)
        if config.entry_mode == "two_step_confirmation":
            return self.handle_two_step_entry(config, state, features, long_signal, short_signal, signal, consensus, reject_reason, current_time)
        self.store_last_evaluation(config, features, long_signal, short_signal, signal or "none", reject_reason, current_time, consensus)
        self.record_signal_diagnostic(
            config,
            features,
            long_signal,
            short_signal,
            proposed_side=self.diagnostic_side(long_signal, short_signal, consensus),
            final_side=signal or "none",
            reject_reason=reject_reason,
            evaluated_at=current_time,
            consensus=consensus,
        )
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

    def diagnostic_side(self, long_signal=False, short_signal=False, consensus=None):
        consensus = consensus or {}
        direction = consensus.get("consensus_direction")
        if direction in ("long", "short"):
            return direction
        if long_signal and not short_signal:
            return "long"
        if short_signal and not long_signal:
            return "short"
        return "none"

    def side_decision_audit(self, config, features=None, consensus=None, feedback=None, final_side=None):
        features = features or {}
        consensus = consensus or {}
        feedback = feedback or consensus.get("feedback") or {}
        median = consensus.get("median_imbalance")
        threshold_source = median if median is not None else features.get("imbalance")
        long_threshold_hit = threshold_source is not None and threshold_source > float(config.long_imbalance_threshold)
        short_threshold_hit = threshold_source is not None and threshold_source <= float(config.short_imbalance_threshold)
        min_count = int(config.min_confirming_exchanges)
        min_ratio = float(config.min_consensus_ratio)
        valid_count = int(consensus.get("valid_exchanges_count") or 0)
        average_momentum = consensus.get("average_momentum", features.get("short_momentum") or 0) or 0
        long_consensus_passed = (
            valid_count >= int(config.min_valid_exchanges)
            and (consensus.get("confirming_long_count") or 0) >= min_count
            and (consensus.get("consensus_ratio_long") or 0) >= min_ratio
            and average_momentum > 0
            and long_threshold_hit
        )
        short_consensus_passed = (
            valid_count >= int(config.min_valid_exchanges)
            and (consensus.get("confirming_short_count") or 0) >= min_count
            and (consensus.get("consensus_ratio_short") or 0) >= min_ratio
            and average_momentum < 0
            and short_threshold_hit
        )
        configured_long = bool(consensus.get("configured_exchange_long_signal"))
        configured_short = bool(consensus.get("configured_exchange_short_signal"))
        long_blocked_by_configured = bool(config.require_configured_exchange_signal and long_consensus_passed and not configured_long)
        short_blocked_by_configured = bool(config.require_configured_exchange_signal and short_consensus_passed and not configured_short)
        long_blocked_by_feedback = bool(feedback.get("blocked_side") == "long" or (final_side != "long" and feedback.get("feedback_reject_reason") and consensus.get("consensus_direction") == "long"))
        short_blocked_by_feedback = bool(feedback.get("blocked_side") == "short" or (final_side != "short" and feedback.get("feedback_reject_reason") and consensus.get("consensus_direction") == "short"))
        long_blocked_by_consensus = bool(long_threshold_hit and not long_consensus_passed)
        short_blocked_by_consensus = bool(short_threshold_hit and not short_consensus_passed)
        short_reasons = []
        if not short_threshold_hit:
            short_reasons.append("median_above_short_threshold")
        if short_blocked_by_consensus:
            short_reasons.append("short_consensus_not_met")
        if short_blocked_by_configured:
            short_reasons.append("configured_exchange_not_short")
        if short_blocked_by_feedback:
            short_reasons.append(feedback.get("feedback_reject_reason") or "short_blocked_by_feedback")
        if final_side == "long" and short_threshold_hit:
            short_reasons.append("long_selected")
        long_reasons = []
        if final_side == "long":
            long_reasons.append("long_consensus_selected" if consensus.get("consensus_direction") == "long" else "long_signal_selected")
            if long_threshold_hit:
                long_reasons.append("long_threshold_hit")
            if configured_long:
                long_reasons.append("configured_exchange_long_signal")
        return {
            "why_long_selected": ", ".join(long_reasons) if long_reasons else None,
            "why_short_rejected": ", ".join(short_reasons) if short_reasons else None,
            "configured_exchange_long_signal": configured_long,
            "configured_exchange_short_signal": configured_short,
            "median_imbalance_vs_threshold": f"{threshold_source} vs long>{config.long_imbalance_threshold}, short<={config.short_imbalance_threshold}",
            "long_threshold_hit": bool(long_threshold_hit),
            "short_threshold_hit": bool(short_threshold_hit),
            "short_blocked_by_feedback": short_blocked_by_feedback,
            "short_blocked_by_configured_exchange": short_blocked_by_configured,
            "short_blocked_by_consensus": short_blocked_by_consensus,
            "long_blocked_by_feedback": long_blocked_by_feedback,
            "long_blocked_by_configured_exchange": long_blocked_by_configured,
            "long_blocked_by_consensus": long_blocked_by_consensus,
            "long_consensus_passed": bool(long_consensus_passed and not long_blocked_by_configured),
            "short_consensus_passed": bool(short_consensus_passed and not short_blocked_by_configured),
        }

    def record_signal_diagnostic(
        self,
        config,
        features=None,
        long_signal=False,
        short_signal=False,
        proposed_side=None,
        final_side=None,
        reject_reason=None,
        evaluated_at=None,
        consensus=None,
    ):
        consensus = consensus or {}
        feedback = consensus.get("feedback") or {}
        key = self.debug_key(config)
        proposed_side = proposed_side or self.diagnostic_side(long_signal, short_signal, consensus)
        final_side = final_side or "none"
        state = self.get_or_create_state(config)
        ml_snapshot = self.create_ml_feature_snapshot(config, state, features, consensus, proposed_side, final_side, evaluated_at)
        self.create_ml_market_snapshot(
            config,
            state,
            features,
            consensus,
            proposed_side,
            final_side,
            reject_reason or consensus.get("reject_reason"),
            evaluated_at,
        )
        ml_prediction = consensus.get("ml_prediction") or (
            self.ml_snapshot_to_dict(ml_snapshot) if ml_snapshot else {}
        )
        audit = self.side_decision_audit(config, features, consensus, feedback, final_side)
        row = {
            "timestamp": evaluated_at or datetime.utcnow(),
            "median_imbalance": consensus.get("median_imbalance"),
            "avg_imbalance": consensus.get("average_imbalance"),
            "raw_average_imbalance": consensus.get("raw_average_imbalance"),
            "momentum": consensus.get("average_momentum", (features or {}).get("short_momentum")),
            "long_confirms": consensus.get("confirming_long_count"),
            "short_confirms": consensus.get("confirming_short_count"),
            "long_ratio": consensus.get("consensus_ratio_long"),
            "short_ratio": consensus.get("consensus_ratio_short"),
            "proposed_side": proposed_side,
            "final_side": final_side,
            "reject_reason": reject_reason or consensus.get("reject_reason"),
            "skip_reason": self.entry_skip_reason(reject_reason or consensus.get("reject_reason")),
            "blocked_side": feedback.get("blocked_side"),
            "long_win_rate": feedback.get("long_recent_win_rate"),
            "short_win_rate": feedback.get("short_recent_win_rate"),
            "long_signal": bool(long_signal),
            "short_signal": bool(short_signal),
            "consensus_direction": consensus.get("consensus_direction"),
            "feedback_reject_reason": feedback.get("feedback_reject_reason"),
            "profit_protection": consensus.get("profit_protection") or {},
            "ml_score": ml_prediction.get("ml_score"),
            "ml_decision": ml_prediction.get("ml_decision"),
            "ml_reason": ml_prediction.get("ml_reason"),
            "ml_model_version": ml_prediction.get("ml_model_version"),
            **audit,
        }
        self.__class__._signal_diagnostics[key].append(row)
        self.trim_signal_diagnostics(config)
        if audit["long_threshold_hit"]:
            self.__class__._signal_counters[key]["raw_long_threshold_hits"] += 1
        if audit["short_threshold_hit"]:
            self.__class__._signal_counters[key]["raw_short_threshold_hits"] += 1
        if audit["long_consensus_passed"]:
            self.__class__._signal_counters[key]["long_consensus_passed_count"] += 1
        if audit["short_consensus_passed"]:
            self.__class__._signal_counters[key]["short_consensus_passed_count"] += 1
        if final_side == "long":
            self.__class__._signal_counters[key]["final_long_count"] += 1
        elif final_side == "short":
            self.__class__._signal_counters[key]["final_short_count"] += 1
        if proposed_side == "long" and final_side != "long":
            self.__class__._signal_counters[key]["long_blocked_count"] += 1
        if proposed_side == "short" and final_side != "short":
            self.__class__._signal_counters[key]["short_blocked_count"] += 1
        if proposed_side == "long":
            self.__class__._signal_counters[key]["long_signals_count"] += 1
        elif proposed_side == "short":
            self.__class__._signal_counters[key]["short_signals_count"] += 1
        return row

    def record_opened_side(self, config, side):
        key = self.debug_key(config)
        if side == "long":
            self.__class__._signal_counters[key]["long_opened_count"] += 1
        elif side == "short":
            self.__class__._signal_counters[key]["short_opened_count"] += 1

    def signal_diagnostics_max_rows(self, config):
        try:
            return min(500, max(20, int(config.signal_diagnostics_max_rows or 100)))
        except (TypeError, ValueError):
            return 100

    def trim_signal_diagnostics(self, config):
        key = self.debug_key(config)
        max_rows = self.signal_diagnostics_max_rows(config)
        rows = self.__class__._signal_diagnostics[key]
        while len(rows) > max_rows:
            rows.popleft()

    def signal_diagnostics_for(self, config):
        key = self.debug_key(config)
        self.trim_signal_diagnostics(config)
        return {
            "counters": dict(self.__class__._signal_counters[key]),
            "last_100": list(self.__class__._signal_diagnostics[key]),
        }

    def clear_signal_diagnostics(self, config=None):
        config = config or self.get_or_create_config()
        key = self.debug_key(config)
        self.__class__._signal_diagnostics[key].clear()
        self.__class__._signal_counters[key] = {
            "long_signals_count": 0,
            "short_signals_count": 0,
            "long_opened_count": 0,
            "short_opened_count": 0,
            "raw_long_threshold_hits": 0,
            "raw_short_threshold_hits": 0,
            "long_consensus_passed_count": 0,
            "short_consensus_passed_count": 0,
            "long_blocked_count": 0,
            "short_blocked_count": 0,
            "final_long_count": 0,
            "final_short_count": 0,
        }
        return self.response_ok(self.signal_diagnostics_for(config))

    def entry_skip_reason(self, reject_reason):
        if not reject_reason:
            return None
        if str(reject_reason).startswith("confirmation_failed") or reject_reason == "confirmation_expired":
            return "failed_confirmation"
        if reject_reason in {"fee_filter", "weak_momentum", "side_quality_block"}:
            return reject_reason
        return None

    def live_fee_filter_result(self, config, state):
        margin = float(state.current_margin or config.base_margin_usdt or 0)
        leverage = float(config.leverage or 1)
        notional = margin * leverage
        taker_fee_percent = float(config.live_fee_filter_taker_fee_percent or 0)
        roundtrip_fee = notional * (taker_fee_percent / 100) * 2
        target_profit = margin * (float(config.take_profit_percent_of_margin or 0) / 100)
        return {
            "enabled": bool(config.live_fee_filter_enabled and config.execution_mode == "live"),
            "current_margin": margin,
            "estimated_notional": notional,
            "taker_fee_percent": taker_fee_percent,
            "estimated_roundtrip_fee": roundtrip_fee,
            "target_profit_pnl": target_profit,
            "required_min_target_pnl": roundtrip_fee * 2,
            "passes": target_profit >= (roundtrip_fee * 2),
        }

    def side_quality_filter_result(self, config, side, current_time):
        result = {
            "enabled": bool(config.side_quality_filter_enabled),
            "side": side,
            "lookback": int(config.side_quality_lookback_trades or 5),
            "cooldown_seconds": int(config.side_quality_cooldown_seconds or 0),
            "recent_count": 0,
            "recent_net_pnl": 0,
            "cooldown_until": None,
            "passes": True,
        }
        if not config.side_quality_filter_enabled or not side:
            return result
        trades = (
            self.metrics_trades_query(config).filter(
                StrategyRunTrade.strategy_config_id == config.id,
                StrategyRunTrade.side == side,
                StrategyRunTrade.closed_at.isnot(None),
                StrategyRunTrade.is_archived.is_(False),
                or_(StrategyRunTrade.live_status.is_(None), StrategyRunTrade.live_status != "open_failed"),
            )
            .order_by(StrategyRunTrade.closed_at.desc())
            .limit(result["lookback"])
            .all()
        )
        net_values = [float((trade.net_pnl if trade.net_pnl is not None else trade.pnl) or 0) for trade in trades]
        result["recent_count"] = len(net_values)
        result["recent_net_pnl"] = sum(net_values)
        latest = trades[0] if trades else None
        if len(net_values) >= result["lookback"] and result["recent_net_pnl"] < 0 and latest and latest.closed_at:
            cooldown_until = latest.closed_at + timedelta(seconds=result["cooldown_seconds"])
            result["cooldown_until"] = cooldown_until
            if current_time < cooldown_until:
                result["passes"] = False
        return result

    def profit_protection_rejection(self, config, state, features, signal, consensus, current_time):
        protection = {
            "fee_filter": self.live_fee_filter_result(config, state),
            "momentum_confirmation_enabled": bool(config.momentum_confirmation_enabled),
            "side_quality": self.side_quality_filter_result(config, signal, current_time),
            "reject_reason": None,
        }
        if protection["fee_filter"]["enabled"] and not protection["fee_filter"]["passes"]:
            protection["reject_reason"] = "fee_filter"
        momentum = consensus.get("average_momentum", features.get("short_momentum"))
        if signal and config.momentum_confirmation_enabled:
            if signal == "long" and (momentum is None or momentum <= 0):
                protection["reject_reason"] = "weak_momentum"
            if signal == "short" and (momentum is None or momentum >= 0):
                protection["reject_reason"] = "weak_momentum"
        if signal and not protection["side_quality"]["passes"]:
            protection["reject_reason"] = "side_quality_block"
        consensus["profit_protection"] = protection
        return protection["reject_reason"]

    def ml_enabled(self, config):
        return getattr(config, "ml_mode", "disabled") == "shadow"

    def ml_feature_payload(self, config, state, features=None, consensus=None, proposed_side=None, final_side=None, evaluated_at=None, trade=None):
        features = features or {}
        consensus = consensus or {}
        snapshot = self.latest_snapshot_for(config) or {}
        return {
            "trade_id": trade.id if trade else None,
            "evaluation_id": consensus.get("evaluation_id") or str(uuid4()),
            "timestamp": evaluated_at or datetime.utcnow(),
            "symbol": config.symbol,
            "exchange": config.exchange,
            "proposed_side": proposed_side,
            "final_side": final_side,
            "median_imbalance": consensus.get("median_imbalance"),
            "raw_avg_imbalance": consensus.get("raw_average_imbalance"),
            "spread": features.get("spread_percent") or consensus.get("configured_exchange_spread"),
            "momentum": consensus.get("average_momentum", features.get("short_momentum")),
            "valid_exchanges_count": consensus.get("valid_exchanges_count"),
            "confirming_long_count": consensus.get("confirming_long_count"),
            "confirming_short_count": consensus.get("confirming_short_count"),
            "consensus_ratio_long": consensus.get("consensus_ratio_long"),
            "consensus_ratio_short": consensus.get("consensus_ratio_short"),
            "anomaly_count": consensus.get("anomalous_exchanges_count"),
            "snapshot_age_sec": (snapshot.get("updated_at") and (datetime.utcnow() - snapshot["updated_at"]).total_seconds()),
            "configured_exchange_long_signal": consensus.get("configured_exchange_long_signal"),
            "configured_exchange_short_signal": consensus.get("configured_exchange_short_signal"),
            "current_step": state.current_step if state else None,
            "margin": (state.current_margin if state else None) or config.base_margin_usdt,
            "leverage": config.leverage,
            "tp_percent": config.take_profit_percent_of_margin,
            "sl_percent": config.stop_loss_percent_of_margin,
            "entry_mode": consensus.get("entry_mode") or config.entry_mode,
            "result": trade.result if trade else None,
            "gross_pnl": trade.gross_pnl if trade else None,
            "net_pnl": trade.net_pnl if trade else None,
            "total_fee": trade.total_fee if trade else None,
        }

    def create_ml_feature_snapshot(self, config, state, features=None, consensus=None, proposed_side=None, final_side=None, evaluated_at=None, trade=None):
        if not self.ml_enabled(config):
            return None
        consensus = consensus or {}
        if not consensus.get("evaluation_id"):
            consensus["evaluation_id"] = str(uuid4())
        payload = self.ml_feature_payload(config, state, features, consensus, proposed_side, final_side, evaluated_at, trade)
        prediction = self.ml_prediction_service.predict(payload)
        payload.update(prediction)
        snapshot = MLFeatureSnapshot(**payload)
        db.session.add(snapshot)
        db.session.commit()
        consensus["ml_prediction"] = prediction
        consensus["ml_feature_snapshot_id"] = snapshot.id
        return snapshot

    def ml_label_horizons(self, config):
        value = getattr(config, "ml_label_horizons_seconds", None) or [10, 30, 60]
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except (TypeError, ValueError):
                value = [10, 30, 60]
        allowed = {10, 30, 60}
        horizons = []
        for item in value if isinstance(value, list) else [10, 30, 60]:
            try:
                horizon = int(item)
            except (TypeError, ValueError):
                continue
            if horizon in allowed and horizon not in horizons:
                horizons.append(horizon)
        return horizons or [10, 30, 60]

    def ml_market_capture_enabled(self, config):
        return self.ml_enabled(config) and bool(getattr(config, "ml_snapshot_capture_enabled", True))

    def ml_required_future_return(self, config):
        margin = float(config.base_margin_usdt or 0)
        leverage = float(config.leverage or 1)
        notional = margin * leverage
        if notional <= 0:
            return 0
        target_profit = margin * (float(config.take_profit_percent_of_margin or 0) / 100)
        taker_fee_percent = float(getattr(config, "live_fee_filter_taker_fee_percent", 0) or 0)
        estimated_roundtrip_fee = notional * (taker_fee_percent / 100) * 2
        return (target_profit + estimated_roundtrip_fee) / notional

    def snapshot_mid_price(self, snapshot):
        order_book = (snapshot or {}).get("order_book") or {}
        normalized, error = OrderBookNormalizer.normalize(order_book)
        if error:
            return None, error
        bids = normalized["bids"]
        asks = normalized["asks"]
        best_bid = float(bids[0]["price"])
        best_ask = float(asks[0]["price"])
        if best_bid <= 0 or best_ask <= 0 or best_ask < best_bid:
            return None, "invalid_price_amount"
        return (best_bid + best_ask) / 2, None

    def should_capture_ml_market_snapshot(self, config, evaluated_at):
        if not self.ml_market_capture_enabled(config):
            return False
        sample_rate = min(1, max(0, float(getattr(config, "ml_snapshot_sample_rate", 1) or 0)))
        if sample_rate <= 0 or (sample_rate < 1 and random.random() > sample_rate):
            return False
        cap = max(1, int(getattr(config, "ml_max_snapshots_per_hour", 10000) or 10000))
        since = evaluated_at - timedelta(hours=1)
        count = MLMarketSnapshot.query.filter(
            MLMarketSnapshot.exchange == config.exchange,
            MLMarketSnapshot.symbol == config.symbol,
            MLMarketSnapshot.timestamp >= since,
        ).count()
        return count < cap

    def ml_price_history_rows(self, config, consensus, evaluated_at):
        rows = []
        valid_features = [
            row for row in (consensus or {}).get("per_exchange_features", [])
            if row.get("valid") and row.get("mid_price")
        ]
        for row in valid_features:
            rows.append(MLMarketPriceHistory(
                timestamp=evaluated_at,
                exchange=row.get("exchange"),
                symbol=row.get("symbol") or config.symbol,
                mid_price=float(row["mid_price"]),
                bid=row.get("bid"),
                ask=row.get("ask"),
                spread=row.get("spread_percent"),
                snapshot_age_sec=row.get("snapshot_age_seconds"),
                created_at=datetime.utcnow(),
            ))
        mids = [float(row["mid_price"]) for row in valid_features]
        if mids:
            rows.append(MLMarketPriceHistory(
                timestamp=evaluated_at,
                exchange="__median__",
                symbol=config.symbol,
                mid_price=float(self.median(mids)),
                bid=None,
                ask=None,
                spread=None,
                snapshot_age_sec=None,
                created_at=datetime.utcnow(),
            ))
            rows.append(MLMarketPriceHistory(
                timestamp=evaluated_at,
                exchange="__average__",
                symbol=config.symbol,
                mid_price=sum(mids) / len(mids),
                bid=None,
                ask=None,
                spread=None,
                snapshot_age_sec=None,
                created_at=datetime.utcnow(),
            ))
        return rows

    def create_ml_exchange_labels(self, config, market_snapshot, consensus):
        valid_features = [
            row for row in (consensus or {}).get("per_exchange_features", [])
            if row.get("valid") and row.get("mid_price")
        ]
        labels = []
        seen = set()
        for row in valid_features:
            key = (row.get("exchange"), row.get("symbol") or config.symbol)
            if key in seen:
                continue
            seen.add(key)
            labels.append(MLMarketSnapshotExchangeLabel(
                snapshot_id=market_snapshot.id,
                exchange=row.get("exchange"),
                symbol=row.get("symbol") or config.symbol,
                reference_price=float(row["mid_price"]),
                label_status="pending",
                created_at=datetime.utcnow(),
            ))
        mids = [float(row["mid_price"]) for row in valid_features]
        if mids:
            labels.append(MLMarketSnapshotExchangeLabel(
                snapshot_id=market_snapshot.id,
                exchange="__median__",
                symbol=config.symbol,
                reference_price=float(self.median(mids)),
                label_status="pending",
                created_at=datetime.utcnow(),
            ))
            labels.append(MLMarketSnapshotExchangeLabel(
                snapshot_id=market_snapshot.id,
                exchange="__average__",
                symbol=config.symbol,
                reference_price=sum(mids) / len(mids),
                label_status="pending",
                created_at=datetime.utcnow(),
            ))
        return labels

    def create_ml_market_snapshot(
        self,
        config,
        state,
        features=None,
        consensus=None,
        proposed_side=None,
        final_side=None,
        reject_reason=None,
        evaluated_at=None,
    ):
        evaluated_at = evaluated_at or datetime.utcnow()
        features = features or {}
        consensus = consensus or {}
        reference_price = features.get("mid_price")
        if reference_price is None or not self.ml_market_capture_enabled(config):
            return None
        history_rows = self.ml_price_history_rows(config, consensus, evaluated_at)
        if history_rows:
            db.session.add_all(history_rows)
        if not self.should_capture_ml_market_snapshot(config, evaluated_at):
            db.session.commit()
            return None
        snapshot = self.latest_snapshot_for(config) or {}
        source_time = snapshot.get("updated_at")
        market_snapshot = MLMarketSnapshot(
            timestamp=evaluated_at,
            symbol=config.symbol,
            exchange=config.exchange,
            reference_price=float(reference_price),
            median_imbalance=consensus.get("median_imbalance"),
            raw_avg_imbalance=consensus.get("raw_average_imbalance"),
            spread=features.get("spread_percent") or consensus.get("configured_exchange_spread"),
            momentum=consensus.get("average_momentum", features.get("short_momentum")),
            valid_exchanges_count=consensus.get("valid_exchanges_count"),
            long_confirms=consensus.get("confirming_long_count"),
            short_confirms=consensus.get("confirming_short_count"),
            long_ratio=consensus.get("consensus_ratio_long"),
            short_ratio=consensus.get("consensus_ratio_short"),
            anomaly_count=consensus.get("anomalous_exchanges_count"),
            snapshot_age_sec=(source_time and (evaluated_at - source_time).total_seconds()),
            configured_exchange_long_signal=consensus.get("configured_exchange_long_signal"),
            configured_exchange_short_signal=consensus.get("configured_exchange_short_signal"),
            proposed_side=proposed_side,
            final_side=final_side,
            reject_reason=reject_reason or consensus.get("reject_reason"),
            created_at=datetime.utcnow(),
            label_status="pending",
        )
        db.session.add(market_snapshot)
        db.session.flush()
        labels = self.create_ml_exchange_labels(config, market_snapshot, consensus)
        if labels:
            db.session.add_all(labels)
        db.session.commit()
        return market_snapshot

    def label_pending_market_snapshots(self, config=None, current_time=None):
        config = config or self.get_or_create_config()
        current_time = current_time or datetime.utcnow()
        required_return = self.ml_required_future_return(config)
        horizons = self.ml_label_horizons(config)
        pending_labels = (
            MLMarketSnapshotExchangeLabel.query.filter_by(label_status="pending")
            .join(MLMarketSnapshot, MLMarketSnapshot.id == MLMarketSnapshotExchangeLabel.snapshot_id)
            .filter(MLMarketSnapshot.symbol == config.symbol)
            .order_by(MLMarketSnapshot.timestamp.asc())
            .limit(3000)
            .all()
        )
        labels_updated = 0
        for label in pending_labels:
            if self.label_exchange_label(label, horizons, current_time):
                labels_updated += 1
        pending_snapshots = (
            MLMarketSnapshot.query.filter_by(exchange=config.exchange, symbol=config.symbol, label_status="pending")
            .order_by(MLMarketSnapshot.timestamp.asc())
            .limit(1000)
            .all()
        )
        snapshots_updated = 0
        for item in pending_snapshots:
            if self.sync_market_snapshot_labels(item, horizons, required_return):
                snapshots_updated += 1
        updated = labels_updated + snapshots_updated
        if updated:
            db.session.commit()
        return {"updated": updated, "required_return": required_return}

    def label_values_from_history(self, reference_price, history):
        if not reference_price or not history:
            return None
        prices = [float(row.mid_price) for row in history]
        future_price = prices[-1]
        max_price = max(prices)
        min_price = min(prices)
        reference = float(reference_price)
        return {
            "future_price": future_price,
            "future_return": (future_price - reference) / reference,
            "max_price": max_price,
            "min_price": min_price,
            "mfe_long": (max_price - reference) / reference,
            "mae_long": (min_price - reference) / reference,
            "mfe_short": (reference - min_price) / reference,
            "mae_short": (reference - max_price) / reference,
        }

    def label_exchange_label(self, label, horizons, current_time):
        touched = False
        snapshot_time = label.snapshot.timestamp
        for horizon in horizons:
            if (current_time - snapshot_time).total_seconds() < horizon:
                continue
            if getattr(label, f"future_price_{horizon}s") is not None:
                continue
            end_time = snapshot_time + timedelta(seconds=horizon)
            history = (
                MLMarketPriceHistory.query.filter(
                    MLMarketPriceHistory.exchange == label.exchange,
                    MLMarketPriceHistory.symbol == label.symbol,
                    MLMarketPriceHistory.timestamp > snapshot_time,
                    MLMarketPriceHistory.timestamp <= end_time,
                )
                .order_by(MLMarketPriceHistory.timestamp.asc())
                .all()
            )
            values = self.label_values_from_history(label.reference_price, history)
            if not values:
                continue
            setattr(label, f"future_price_{horizon}s", values["future_price"])
            setattr(label, f"future_return_{horizon}s", values["future_return"])
            setattr(label, f"max_price_{horizon}s", values["max_price"])
            setattr(label, f"min_price_{horizon}s", values["min_price"])
            setattr(label, f"mfe_long_{horizon}s", values["mfe_long"])
            setattr(label, f"mae_long_{horizon}s", values["mae_long"])
            setattr(label, f"mfe_short_{horizon}s", values["mfe_short"])
            setattr(label, f"mae_short_{horizon}s", values["mae_short"])
            touched = True
        if all(getattr(label, f"future_price_{horizon}s") is not None for horizon in horizons):
            label.label_status = "labeled"
            touched = True
        return touched

    def sync_market_snapshot_labels(self, snapshot, horizons, required_return):
        labels = MLMarketSnapshotExchangeLabel.query.filter_by(snapshot_id=snapshot.id).all()
        configured = next((label for label in labels if self.normalize_exchange(label.exchange) == self.normalize_exchange(snapshot.exchange)), None)
        median_label = next((label for label in labels if label.exchange == "__median__"), None)
        average_label = next((label for label in labels if label.exchange == "__average__"), None)
        touched = False
        for horizon in horizons:
            if configured and getattr(configured, f"future_price_{horizon}s") is not None and getattr(snapshot, f"future_price_{horizon}s") is None:
                future_return = getattr(configured, f"future_return_{horizon}s")
                setattr(snapshot, f"future_price_{horizon}s", getattr(configured, f"future_price_{horizon}s"))
                setattr(snapshot, f"future_return_{horizon}s", future_return)
                setattr(snapshot, f"long_would_win_{horizon}s", future_return >= required_return)
                setattr(snapshot, f"short_would_win_{horizon}s", future_return <= -required_return)
                for field in ["max_price", "min_price", "mfe_long", "mae_long", "mfe_short", "mae_short"]:
                    setattr(snapshot, f"{field}_{horizon}s", getattr(configured, f"{field}_{horizon}s"))
                touched = True
            if median_label and getattr(median_label, f"future_price_{horizon}s") is not None:
                setattr(snapshot, f"median_future_price_{horizon}s", getattr(median_label, f"future_price_{horizon}s"))
                setattr(snapshot, f"median_future_return_{horizon}s", getattr(median_label, f"future_return_{horizon}s"))
                for field in ["mfe_long", "mae_long", "mfe_short", "mae_short"]:
                    setattr(snapshot, f"median_{field}_{horizon}s", getattr(median_label, f"{field}_{horizon}s"))
                touched = True
            if average_label and getattr(average_label, f"future_price_{horizon}s") is not None:
                setattr(snapshot, f"avg_future_price_{horizon}s", getattr(average_label, f"future_price_{horizon}s"))
                setattr(snapshot, f"avg_future_return_{horizon}s", getattr(average_label, f"future_return_{horizon}s"))
                touched = True
        if all(getattr(snapshot, f"future_price_{horizon}s") is not None for horizon in horizons):
            snapshot.label_status = "labeled"
            touched = True
        return touched

    def update_ml_snapshots_for_trade(self, trade):
        snapshots = MLFeatureSnapshot.query.filter_by(trade_id=trade.id).all()
        for snapshot in snapshots:
            snapshot.result = trade.result
            snapshot.gross_pnl = trade.gross_pnl
            snapshot.net_pnl = trade.net_pnl
            snapshot.total_fee = trade.total_fee
        return snapshots

    def safe_json_value(self, value):
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, dict):
            return {str(key): self.safe_json_value(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self.safe_json_value(item) for item in value]
        return value

    def safe_json_payload(self, payload):
        return {key: self.safe_json_value(value) for key, value in (payload or {}).items()}

    def safe_json_blob(self, value):
        if value in (None, ""):
            return None
        if isinstance(value, (dict, list)):
            return self.safe_json_value(value)
        if isinstance(value, str):
            try:
                return self.safe_json_value(json.loads(value))
            except (TypeError, ValueError):
                return value
        return self.safe_json_value(value)

    def ml_snapshot_to_dict(self, snapshot):
        return self.safe_json_payload({
            "id": snapshot.id,
            "trade_id": snapshot.trade_id,
            "evaluation_id": snapshot.evaluation_id,
            "timestamp": snapshot.timestamp,
            "symbol": snapshot.symbol,
            "exchange": snapshot.exchange,
            "proposed_side": snapshot.proposed_side,
            "final_side": snapshot.final_side,
            "median_imbalance": snapshot.median_imbalance,
            "raw_avg_imbalance": snapshot.raw_avg_imbalance,
            "spread": snapshot.spread,
            "momentum": snapshot.momentum,
            "valid_exchanges_count": snapshot.valid_exchanges_count,
            "confirming_long_count": snapshot.confirming_long_count,
            "confirming_short_count": snapshot.confirming_short_count,
            "consensus_ratio_long": snapshot.consensus_ratio_long,
            "consensus_ratio_short": snapshot.consensus_ratio_short,
            "anomaly_count": snapshot.anomaly_count,
            "snapshot_age_sec": snapshot.snapshot_age_sec,
            "configured_exchange_long_signal": snapshot.configured_exchange_long_signal,
            "configured_exchange_short_signal": snapshot.configured_exchange_short_signal,
            "current_step": snapshot.current_step,
            "margin": snapshot.margin,
            "leverage": snapshot.leverage,
            "tp_percent": snapshot.tp_percent,
            "sl_percent": snapshot.sl_percent,
            "entry_mode": snapshot.entry_mode,
            "result": snapshot.result,
            "gross_pnl": snapshot.gross_pnl,
            "net_pnl": snapshot.net_pnl,
            "total_fee": snapshot.total_fee,
            "ml_score": snapshot.ml_score,
            "ml_decision": snapshot.ml_decision,
            "ml_reason": snapshot.ml_reason,
            "ml_model_version": snapshot.ml_model_version,
        })

    def ml_dataset_fields(self):
        return [
            "id", "trade_id", "evaluation_id", "timestamp", "symbol", "exchange", "proposed_side", "final_side",
            "median_imbalance", "raw_avg_imbalance", "spread", "momentum", "valid_exchanges_count",
            "confirming_long_count", "confirming_short_count", "consensus_ratio_long", "consensus_ratio_short",
            "anomaly_count", "snapshot_age_sec", "configured_exchange_long_signal", "configured_exchange_short_signal",
            "current_step", "margin", "leverage", "tp_percent", "sl_percent", "entry_mode", "result",
            "gross_pnl", "net_pnl", "total_fee", "ml_score", "ml_decision", "ml_reason", "ml_model_version",
        ]

    def export_ml_dataset(self, export_format="csv"):
        snapshots = MLFeatureSnapshot.query.order_by(MLFeatureSnapshot.timestamp.asc()).all()
        rows = [self.ml_snapshot_to_dict(snapshot) for snapshot in snapshots]
        stamp = datetime.utcnow().strftime("%Y-%m-%d-%H-%M")
        if export_format == "json":
            payload = json.dumps(rows, default=str, ensure_ascii=False, indent=2)
            buffer = BytesIO(payload.encode("utf-8"))
            return send_file(buffer, mimetype="application/json", as_attachment=True, download_name=f"orderbook-recovery-ml-dataset-{stamp}.json")
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=self.ml_dataset_fields())
        writer.writeheader()
        writer.writerows(rows)
        buffer = BytesIO(output.getvalue().encode("utf-8"))
        return send_file(buffer, mimetype="text/csv", as_attachment=True, download_name=f"orderbook-recovery-ml-dataset-{stamp}.csv")

    def latest_ml_snapshot_for_config(self, config):
        snapshot = MLFeatureSnapshot.query.filter_by(exchange=config.exchange, symbol=config.symbol).order_by(MLFeatureSnapshot.timestamp.desc()).first()
        return self.ml_snapshot_to_dict(snapshot) if snapshot else None

    def ml_market_snapshot_to_dict(self, snapshot, include_exchange_labels=False):
        payload = {
            "id": snapshot.id,
            "timestamp": snapshot.timestamp,
            "symbol": snapshot.symbol,
            "exchange": snapshot.exchange,
            "reference_price": snapshot.reference_price,
            "median_imbalance": snapshot.median_imbalance,
            "raw_avg_imbalance": snapshot.raw_avg_imbalance,
            "spread": snapshot.spread,
            "momentum": snapshot.momentum,
            "valid_exchanges_count": snapshot.valid_exchanges_count,
            "long_confirms": snapshot.long_confirms,
            "short_confirms": snapshot.short_confirms,
            "long_ratio": snapshot.long_ratio,
            "short_ratio": snapshot.short_ratio,
            "anomaly_count": snapshot.anomaly_count,
            "snapshot_age_sec": snapshot.snapshot_age_sec,
            "configured_exchange_long_signal": snapshot.configured_exchange_long_signal,
            "configured_exchange_short_signal": snapshot.configured_exchange_short_signal,
            "proposed_side": snapshot.proposed_side,
            "final_side": snapshot.final_side,
            "reject_reason": snapshot.reject_reason,
            "created_at": snapshot.created_at,
            "label_status": snapshot.label_status,
        }
        for horizon in [10, 30, 60]:
            for field in [
                "future_price", "future_return", "long_would_win", "short_would_win",
                "max_price", "min_price", "mfe_long", "mae_long", "mfe_short", "mae_short",
                "median_future_price", "median_future_return", "avg_future_price", "avg_future_return",
                "median_mfe_long", "median_mae_long", "median_mfe_short", "median_mae_short",
            ]:
                key = f"{field}_{horizon}s"
                payload[key] = getattr(snapshot, key, None)
        if include_exchange_labels:
            payload["exchange_labels"] = [
                self.ml_exchange_label_to_dict(label)
                for label in MLMarketSnapshotExchangeLabel.query.filter_by(snapshot_id=snapshot.id).order_by(MLMarketSnapshotExchangeLabel.exchange.asc()).all()
            ]
        return self.safe_json_payload(payload)

    def ml_exchange_label_to_dict(self, label):
        payload = {
            "id": label.id,
            "snapshot_id": label.snapshot_id,
            "exchange": label.exchange,
            "symbol": label.symbol,
            "reference_price": label.reference_price,
            "label_status": label.label_status,
            "created_at": label.created_at,
        }
        for horizon in [10, 30, 60]:
            for field in [
                "future_price", "future_return", "max_price", "min_price",
                "mfe_long", "mae_long", "mfe_short", "mae_short",
            ]:
                key = f"{field}_{horizon}s"
                payload[key] = getattr(label, key, None)
        return self.safe_json_payload(payload)

    def ml_exchange_label_fields(self):
        fields = ["id", "snapshot_id", "exchange", "symbol", "reference_price", "label_status", "created_at"]
        for horizon in [10, 30, 60]:
            fields.extend([
                f"future_price_{horizon}s", f"future_return_{horizon}s",
                f"max_price_{horizon}s", f"min_price_{horizon}s",
                f"mfe_long_{horizon}s", f"mae_long_{horizon}s",
                f"mfe_short_{horizon}s", f"mae_short_{horizon}s",
            ])
        return fields

    def ml_market_dataset_fields(self):
        fields = [
            "id", "timestamp", "symbol", "exchange", "reference_price", "median_imbalance", "raw_avg_imbalance",
            "spread", "momentum", "valid_exchanges_count", "long_confirms", "short_confirms", "long_ratio",
            "short_ratio", "anomaly_count", "snapshot_age_sec", "configured_exchange_long_signal",
            "configured_exchange_short_signal", "proposed_side", "final_side", "reject_reason", "created_at",
        ]
        for horizon in [10, 30, 60]:
            fields.extend([
                f"future_price_{horizon}s", f"future_return_{horizon}s",
                f"long_would_win_{horizon}s", f"short_would_win_{horizon}s",
                f"max_price_{horizon}s", f"min_price_{horizon}s",
                f"mfe_long_{horizon}s", f"mae_long_{horizon}s",
                f"mfe_short_{horizon}s", f"mae_short_{horizon}s",
                f"median_future_price_{horizon}s", f"median_future_return_{horizon}s",
                f"avg_future_price_{horizon}s", f"avg_future_return_{horizon}s",
                f"median_mfe_long_{horizon}s", f"median_mae_long_{horizon}s",
                f"median_mfe_short_{horizon}s", f"median_mae_short_{horizon}s",
            ])
        fields.extend(["label_status", "exchange_labels"])
        return fields

    def ml_market_snapshot_stats(self, config):
        query = MLMarketSnapshot.query.filter_by(exchange=config.exchange, symbol=config.symbol)
        label_query = MLMarketSnapshotExchangeLabel.query.join(MLMarketSnapshot, MLMarketSnapshot.id == MLMarketSnapshotExchangeLabel.snapshot_id).filter(
            MLMarketSnapshot.exchange == config.exchange,
            MLMarketSnapshot.symbol == config.symbol,
        )
        label_total = label_query.count()
        label_pending = label_query.filter(MLMarketSnapshotExchangeLabel.label_status == "pending").count()
        label_labeled = label_query.filter(MLMarketSnapshotExchangeLabel.label_status == "labeled").count()
        return {
            "total": query.count(),
            "pending": query.filter_by(label_status="pending").count(),
            "labeled": query.filter_by(label_status="labeled").count(),
            "exchange_labels_total": label_total,
            "exchange_labels_pending": label_pending,
            "exchange_labels_labeled": label_labeled,
            "exchange_label_completion_percent": (label_labeled / label_total * 100) if label_total else 0,
        }

    def ml_stats_response(self):
        try:
            config = self.get_or_create_config()
            stats = self.ml_market_snapshot_stats(config)
            return self.response_ok({
                "ml_market_snapshots_count": stats["total"],
                "ml_market_snapshots_pending_count": stats["pending"],
                "ml_market_snapshots_labeled_count": stats["labeled"],
                "ml_exchange_labels_count": stats["exchange_labels_total"],
                "ml_exchange_labels_pending_count": stats["exchange_labels_pending"],
                "ml_exchange_labels_labeled_count": stats["exchange_labels_labeled"],
                "ml_exchange_label_completion_percent": stats["exchange_label_completion_percent"],
            })
        except Exception as error:
            logger.exception("ML stats endpoint failed: %s", error)
            return self.response(False, {
                "msg": "ml_stats_unavailable",
                "ml_market_snapshots_count": -1,
                "ml_market_snapshots_pending_count": -1,
                "ml_market_snapshots_labeled_count": -1,
                "ml_exchange_labels_count": -1,
                "ml_exchange_labels_pending_count": -1,
                "ml_exchange_labels_labeled_count": -1,
                "ml_exchange_label_completion_percent": -1,
            }, 500)

    def clear_ml_dataset(self):
        try:
            counts = {
                "exchange_labels_deleted": MLMarketSnapshotExchangeLabel.query.delete(synchronize_session=False),
                "market_snapshots_deleted": MLMarketSnapshot.query.delete(synchronize_session=False),
                "price_history_deleted": MLMarketPriceHistory.query.delete(synchronize_session=False),
                "feature_snapshots_deleted": MLFeatureSnapshot.query.delete(synchronize_session=False),
            }
            db.session.commit()
            logger.warning("OrderBookRecovery ML dataset cleared: %s", counts)
            return self.response_ok({
                **counts,
                "msg": "ml_dataset_cleared",
            })
        except Exception as error:
            db.session.rollback()
            logger.exception("ML dataset clear failed: %s", error)
            return self.response(False, {"msg": "ml_dataset_clear_failed"}, 500)

    def export_ml_market_snapshots(self, export_format="csv"):
        config = self.get_or_create_config()
        self.label_pending_market_snapshots(config)
        snapshots = MLMarketSnapshot.query.order_by(MLMarketSnapshot.timestamp.asc()).all()
        rows = [self.ml_market_snapshot_to_dict(snapshot) for snapshot in snapshots]
        stamp = datetime.utcnow().strftime("%Y-%m-%d-%H-%M")
        if export_format == "json":
            payload = json.dumps(rows, default=str, ensure_ascii=False, indent=2)
            buffer = BytesIO(payload.encode("utf-8"))
            return send_file(buffer, mimetype="application/json", as_attachment=True, download_name=f"orderbook-recovery-ml-market-snapshots-{stamp}.json")
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=self.ml_market_dataset_fields())
        writer.writeheader()
        for row in rows:
            row["exchange_labels"] = json.dumps(row.get("exchange_labels") or [], default=str, ensure_ascii=False)
        writer.writerows(rows)
        buffer = BytesIO(output.getvalue().encode("utf-8"))
        return send_file(buffer, mimetype="text/csv", as_attachment=True, download_name=f"orderbook-recovery-ml-market-snapshots-{stamp}.csv")

    def export_ml_exchange_labels(self, export_format="csv"):
        config = self.get_or_create_config()
        self.label_pending_market_snapshots(config)
        labels = MLMarketSnapshotExchangeLabel.query.order_by(MLMarketSnapshotExchangeLabel.id.asc()).all()
        rows = [self.ml_exchange_label_to_dict(label) for label in labels]
        stamp = datetime.utcnow().strftime("%Y-%m-%d-%H-%M")
        if export_format == "json":
            payload = json.dumps(rows, default=str, ensure_ascii=False, indent=2)
            buffer = BytesIO(payload.encode("utf-8"))
            return send_file(buffer, mimetype="application/json", as_attachment=True, download_name=f"orderbook-recovery-ml-exchange-labels-{stamp}.json")
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=self.ml_exchange_label_fields())
        writer.writeheader()
        writer.writerows(rows)
        buffer = BytesIO(output.getvalue().encode("utf-8"))
        return send_file(buffer, mimetype="text/csv", as_attachment=True, download_name=f"orderbook-recovery-ml-exchange-labels-{stamp}.csv")

    def ml_price_history_to_dict(self, row):
        return self.safe_json_payload({
            "id": row.id,
            "timestamp": row.timestamp,
            "exchange": row.exchange,
            "symbol": row.symbol,
            "mid_price": row.mid_price,
            "bid": row.bid,
            "ask": row.ask,
            "spread": row.spread,
            "snapshot_age_sec": row.snapshot_age_sec,
            "created_at": row.created_at,
        })

    def ml_price_history_fields(self):
        return ["id", "timestamp", "exchange", "symbol", "mid_price", "bid", "ask", "spread", "snapshot_age_sec", "created_at"]

    def parse_bool_filter(self, value):
        if value is None or value == "":
            return None
        return str(value).lower() in {"1", "true", "yes", "y"}

    def parse_datetime_filter(self, value):
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            return None

    def ml_dataset_model_config(self, dataset):
        configs = {
            "feature": {
                "model": MLFeatureSnapshot,
                "serializer": self.ml_snapshot_to_dict,
                "fields": self.ml_dataset_fields,
                "timestamp": MLFeatureSnapshot.timestamp,
                "sort": {
                    "id": MLFeatureSnapshot.id,
                    "timestamp": MLFeatureSnapshot.timestamp,
                    "symbol": MLFeatureSnapshot.symbol,
                    "exchange": MLFeatureSnapshot.exchange,
                    "side": MLFeatureSnapshot.final_side,
                    "result": MLFeatureSnapshot.result,
                    "ml_score": MLFeatureSnapshot.ml_score,
                },
            },
            "market": {
                "model": MLMarketSnapshot,
                "serializer": self.ml_market_snapshot_to_dict,
                "fields": self.ml_market_dataset_fields,
                "timestamp": MLMarketSnapshot.timestamp,
                "sort": {
                    "id": MLMarketSnapshot.id,
                    "timestamp": MLMarketSnapshot.timestamp,
                    "symbol": MLMarketSnapshot.symbol,
                    "exchange": MLMarketSnapshot.exchange,
                    "label_status": MLMarketSnapshot.label_status,
                    "future_return_10s": MLMarketSnapshot.future_return_10s,
                    "median_future_return_10s": MLMarketSnapshot.median_future_return_10s,
                },
            },
            "price_history": {
                "model": MLMarketPriceHistory,
                "serializer": self.ml_price_history_to_dict,
                "fields": self.ml_price_history_fields,
                "timestamp": MLMarketPriceHistory.timestamp,
                "sort": {
                    "id": MLMarketPriceHistory.id,
                    "timestamp": MLMarketPriceHistory.timestamp,
                    "symbol": MLMarketPriceHistory.symbol,
                    "exchange": MLMarketPriceHistory.exchange,
                    "mid_price": MLMarketPriceHistory.mid_price,
                },
            },
            "exchange_label": {
                "model": MLMarketSnapshotExchangeLabel,
                "serializer": self.ml_exchange_label_to_dict,
                "fields": self.ml_exchange_label_fields,
                "timestamp": MLMarketSnapshotExchangeLabel.created_at,
                "sort": {
                    "id": MLMarketSnapshotExchangeLabel.id,
                    "snapshot_id": MLMarketSnapshotExchangeLabel.snapshot_id,
                    "symbol": MLMarketSnapshotExchangeLabel.symbol,
                    "exchange": MLMarketSnapshotExchangeLabel.exchange,
                    "label_status": MLMarketSnapshotExchangeLabel.label_status,
                    "future_return_10s": MLMarketSnapshotExchangeLabel.future_return_10s,
                },
            },
        }
        return configs[dataset]

    def apply_ml_dataset_filters(self, dataset, query, args):
        args = args or {}
        config = self.ml_dataset_model_config(dataset)
        model = config["model"]
        symbol = args.get("symbol")
        exchange = args.get("exchange")
        if symbol and hasattr(model, "symbol"):
            query = query.filter(model.symbol.ilike(f"%{symbol}%"))
        if exchange and hasattr(model, "exchange"):
            query = query.filter(model.exchange.ilike(f"%{exchange}%"))
        date_from = self.parse_datetime_filter(args.get("date_from"))
        date_to = self.parse_datetime_filter(args.get("date_to"))
        timestamp_column = config["timestamp"]
        if date_from:
            query = query.filter(timestamp_column >= date_from)
        if date_to:
            query = query.filter(timestamp_column <= date_to)
        if dataset == "feature":
            side = args.get("side")
            if side:
                query = query.filter(or_(MLFeatureSnapshot.proposed_side == side, MLFeatureSnapshot.final_side == side))
            result = args.get("result")
            if result:
                query = query.filter(MLFeatureSnapshot.result == result)
            has_ml_score = self.parse_bool_filter(args.get("has_ml_score"))
            if has_ml_score is True:
                query = query.filter(MLFeatureSnapshot.ml_score.isnot(None))
            elif has_ml_score is False:
                query = query.filter(MLFeatureSnapshot.ml_score.is_(None))
        if dataset in {"market", "exchange_label"}:
            label_status = args.get("label_status")
            if label_status:
                query = query.filter(model.label_status == label_status)
        return query

    def ml_dataset_query(self, dataset, args):
        try:
            config = self.ml_dataset_model_config(dataset)
            model = config["model"]
            try:
                page = max(1, int(args.get("page") or 1))
                page_size = min(200, max(1, int(args.get("page_size") or args.get("per_page") or 50)))
            except (TypeError, ValueError):
                page, page_size = 1, 50
            query = self.apply_ml_dataset_filters(dataset, model.query, args)
            total = query.count()
            sort_by = args.get("sort_by") or "timestamp"
            sort_column = config["sort"].get(sort_by) or config["sort"].get("timestamp") or model.id
            sort_dir = str(args.get("sort_dir") or "desc").lower()
            query = query.order_by(sort_column.asc() if sort_dir == "asc" else sort_column.desc())
            items = query.offset((page - 1) * page_size).limit(page_size).all()
            return self.response_ok({
                "items": [config["serializer"](item) for item in items],
                "page": page,
                "page_size": page_size,
                "total": total,
                "total_pages": (total + page_size - 1) // page_size if total else 0,
            })
        except Exception as error:
            logger.exception("ML dataset list failed dataset=%s: %s", dataset, error)
            return self.response(False, {
                "msg": "ml_dataset_unavailable",
                "items": [],
                "page": 1,
                "page_size": 50,
                "total": 0,
                "total_pages": 0,
            }, 500)

    def ml_dataset_detail(self, dataset, item_id):
        try:
            config = self.ml_dataset_model_config(dataset)
            item = db.session.get(config["model"], item_id)
            if not item:
                return self.response_not_found("ML dataset item not found")
            if dataset == "market":
                return self.response_ok(self.ml_market_snapshot_to_dict(item, include_exchange_labels=True))
            return self.response_ok(config["serializer"](item))
        except Exception as error:
            logger.exception("ML dataset detail failed dataset=%s id=%s: %s", dataset, item_id, error)
            return self.response(False, {"msg": "ml_dataset_detail_unavailable"}, 500)

    def get_ml_feature_snapshots(self, args):
        return self.ml_dataset_query("feature", args)

    def get_ml_feature_snapshot_detail(self, item_id):
        return self.ml_dataset_detail("feature", item_id)

    def get_ml_market_snapshots(self, args):
        return self.ml_dataset_query("market", args)

    def get_ml_market_snapshot_detail(self, item_id):
        return self.ml_dataset_detail("market", item_id)

    def get_price_history(self, args):
        return self.ml_dataset_query("price_history", args)

    def get_price_history_detail(self, item_id):
        return self.ml_dataset_detail("price_history", item_id)

    def get_ml_exchange_labels(self, args):
        return self.ml_dataset_query("exchange_label", args)

    def get_ml_exchange_label_detail(self, item_id):
        return self.ml_dataset_detail("exchange_label", item_id)

    def export_ml_dataset_filtered(self, dataset, args):
        config = self.ml_dataset_model_config(dataset)
        export_format = str(args.get("format") or "csv").lower()
        rows = [
            self.ml_market_snapshot_to_dict(item, include_exchange_labels=True) if dataset == "market" else config["serializer"](item)
            for item in self.apply_ml_dataset_filters(dataset, config["model"].query, args).order_by(config["timestamp"].asc()).all()
        ]
        stamp = datetime.utcnow().strftime("%Y-%m-%d-%H-%M")
        if export_format == "json":
            payload = json.dumps(rows, default=str, ensure_ascii=False, indent=2)
            buffer = BytesIO(payload.encode("utf-8"))
            return send_file(buffer, mimetype="application/json", as_attachment=True, download_name=f"orderbook-recovery-ml-{dataset}-{stamp}.json")
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=config["fields"]())
        writer.writeheader()
        for row in rows:
            if "exchange_labels" in row:
                row["exchange_labels"] = json.dumps(row.get("exchange_labels") or [], default=str, ensure_ascii=False)
        writer.writerows(rows)
        buffer = BytesIO(output.getvalue().encode("utf-8"))
        return send_file(buffer, mimetype="text/csv", as_attachment=True, download_name=f"orderbook-recovery-ml-{dataset}-{stamp}.csv")

    def pending_key(self, config):
        return self.debug_key(config)

    def pending_entry_for(self, config):
        return self.__class__._pending_entries.get(self.pending_key(config))

    def clear_pending_entry(self, config, status=None, reject_reason=None, current_time=None):
        pending = self.__class__._pending_entries.pop(self.pending_key(config), None)
        result = {
            "status": status or "cleared",
            "reject_reason": reject_reason,
            "at": current_time or datetime.utcnow(),
        }
        if pending:
            result.update({
                "side": pending.get("side"),
                "created_at": pending.get("created_at"),
                "expires_at": pending.get("expires_at"),
            })
        self.__class__._last_confirmation_results[self.pending_key(config)] = result
        return pending

    def consensus_summary_snapshot(self, features, consensus, current_time):
        return {
            "timestamp": current_time,
            "entry_price": features.get("mid_price"),
            "configured_exchange_imbalance": consensus.get("configured_exchange_imbalance"),
            "configured_exchange_spread": consensus.get("configured_exchange_spread"),
            "configured_exchange_momentum": consensus.get("configured_exchange_momentum"),
            "consensus_direction": consensus.get("consensus_direction"),
            "median_imbalance": consensus.get("median_imbalance"),
            "average_momentum": consensus.get("average_momentum"),
            "valid_exchanges_count": consensus.get("valid_exchanges_count"),
            "confirming_long_count": consensus.get("confirming_long_count"),
            "confirming_short_count": consensus.get("confirming_short_count"),
            "consensus_ratio_long": consensus.get("consensus_ratio_long"),
            "consensus_ratio_short": consensus.get("consensus_ratio_short"),
            "configured_exchange_valid": consensus.get("configured_exchange_valid"),
            "reject_reason": consensus.get("reject_reason"),
        }

    def create_pending_entry(self, config, features, side, consensus, current_time):
        confirming_count = consensus.get("confirming_long_count") if side == "long" else consensus.get("confirming_short_count")
        pending = {
            "side": side,
            "created_at": current_time,
            "expires_at": current_time + timedelta(seconds=float(config.confirmation_max_wait_seconds)),
            "status": "pending",
            "first_snapshot": self.consensus_summary_snapshot(features, consensus, current_time),
            "first_consensus_direction": consensus.get("consensus_direction"),
            "first_median_imbalance": consensus.get("median_imbalance"),
            "first_average_momentum": consensus.get("average_momentum") or 0,
            "first_valid_exchanges_count": consensus.get("valid_exchanges_count"),
            "first_confirming_count": confirming_count,
            "first_entry_price": features.get("mid_price"),
            "reason": consensus.get("reject_reason") or "signal_detected",
        }
        self.__class__._pending_entries[self.pending_key(config)] = pending
        self.__class__._last_confirmation_results[self.pending_key(config)] = {
            "status": "pending",
            "reject_reason": None,
            "at": current_time,
        }
        return pending

    def confirmation_reject_reason(self, config, pending, signal, consensus):
        side = pending.get("side")
        if config.confirmation_require_consensus_still_valid and not consensus.get("configured_exchange_valid"):
            return "configured_exchange_invalid"
        if config.confirmation_require_same_direction and signal != side:
            return "direction_changed"
        if signal != side:
            return consensus.get("reject_reason") or "entry_filters_not_passed"
        if consensus.get("consensus_direction") != side:
            return "direction_changed"
        if (consensus.get("valid_exchanges_count") or 0) < int(config.min_valid_exchanges):
            return "not_enough_valid_exchanges"
        if side == "long":
            if (consensus.get("confirming_long_count") or 0) < int(config.min_confirming_exchanges):
                return "not_enough_confirming_exchanges"
            if (consensus.get("consensus_ratio_long") or 0) < float(config.min_consensus_ratio):
                return "consensus_ratio_too_low"
            if (consensus.get("average_momentum") or 0) <= 0:
                return "momentum_not_positive"
            if config.confirmation_require_momentum_improvement:
                required = float(pending.get("first_average_momentum") or 0) + float(config.confirmation_min_momentum_delta)
                if (consensus.get("average_momentum") or 0) < required:
                    return "momentum_not_improved"
        else:
            if (consensus.get("confirming_short_count") or 0) < int(config.min_confirming_exchanges):
                return "not_enough_confirming_exchanges"
            if (consensus.get("consensus_ratio_short") or 0) < float(config.min_consensus_ratio):
                return "consensus_ratio_too_low"
            if (consensus.get("average_momentum") or 0) >= 0:
                return "momentum_not_negative"
            if config.confirmation_require_momentum_improvement:
                required = abs(float(pending.get("first_average_momentum") or 0)) + float(config.confirmation_min_momentum_delta)
                if abs(consensus.get("average_momentum") or 0) < required:
                    return "momentum_not_improved"
        if self.open_positions_count(config) >= config.max_open_positions:
            return "max_open_positions_reached"
        return None

    def handle_two_step_entry(self, config, state, features, long_signal, short_signal, signal, consensus, reject_reason, current_time):
        pending = self.pending_entry_for(config)
        if pending:
            if current_time > pending["expires_at"]:
                self.observe_signal("confirmation_rejected", "confirmation_expired")
                self.clear_pending_entry(config, "expired", "confirmation_expired", current_time)
                consensus["reject_reason"] = "confirmation_expired"
                self.store_last_evaluation(config, features, long_signal, short_signal, "none", "confirmation_expired", current_time, consensus)
                self.record_signal_diagnostic(config, features, long_signal, short_signal, proposed_side=pending.get("side"), final_side="none", reject_reason="confirmation_expired", evaluated_at=current_time, consensus=consensus)
                return None
            ready_at = pending["created_at"] + timedelta(seconds=float(config.confirmation_delay_seconds))
            if current_time < ready_at:
                self.observe_signal("confirmation_waiting")
                consensus["reject_reason"] = "confirmation_waiting"
                self.store_last_evaluation(config, features, long_signal, short_signal, "none", "confirmation_waiting", current_time, consensus)
                self.record_signal_diagnostic(config, features, long_signal, short_signal, proposed_side=pending.get("side"), final_side="none", reject_reason="confirmation_waiting", evaluated_at=current_time, consensus=consensus)
                return None
            reason = self.confirmation_reject_reason(config, pending, signal, consensus)
            if reason:
                reject = f"confirmation_failed_{reason}"
                self.observe_signal("confirmation_rejected", reject)
                self.clear_pending_entry(config, "cancelled", reject, current_time)
                consensus["reject_reason"] = reject
                self.store_last_evaluation(config, features, long_signal, short_signal, "none", reject, current_time, consensus)
                self.record_signal_diagnostic(config, features, long_signal, short_signal, proposed_side=pending.get("side"), final_side="none", reject_reason=reject, evaluated_at=current_time, consensus=consensus)
                return None
            confirmation_snapshot = self.consensus_summary_snapshot(features, consensus, current_time)
            self.observe_signal("confirmation_passed")
            actual_delay = (current_time - pending["created_at"]).total_seconds()
            context = {
                "entry_mode": "two_step_confirmation",
                "first_signal_snapshot": pending["first_snapshot"],
                "confirmation_snapshot": confirmation_snapshot,
                "confirmation_delay_actual_seconds": actual_delay,
                "confirmation_result": "confirmed",
                "first_signal_time": pending["created_at"],
                "confirmation_time": current_time,
            }
            self.clear_pending_entry(config, "confirmed", None, current_time)
            consensus["reject_reason"] = None
            self.store_last_evaluation(config, features, long_signal, short_signal, pending["side"], None, current_time, consensus)
            self.record_signal_diagnostic(config, features, long_signal, short_signal, proposed_side=pending.get("side"), final_side=pending.get("side"), reject_reason=None, evaluated_at=current_time, consensus=consensus)
            return self.open_position(config, state, features, pending["side"], current_time, consensus, context)

        if signal:
            pending = self.create_pending_entry(config, features, signal, consensus, current_time)
            self.observe_signal("confirmation_pending")
            consensus["reject_reason"] = "confirmation_pending"
            self.store_last_evaluation(config, features, long_signal, short_signal, "none", "confirmation_pending", current_time, consensus)
            self.record_signal_diagnostic(config, features, long_signal, short_signal, proposed_side=signal, final_side="none", reject_reason="confirmation_pending", evaluated_at=current_time, consensus=consensus)
            return None

        self.store_last_evaluation(config, features, long_signal, short_signal, "none", reject_reason, current_time, consensus)
        self.record_signal_diagnostic(config, features, long_signal, short_signal, proposed_side=self.diagnostic_side(long_signal, short_signal, consensus), final_side="none", reject_reason=reject_reason, evaluated_at=current_time, consensus=consensus)
        return None

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
            "fetch_latency_ms": snapshot.get("metadata", {}).get("fetch_latency_ms"),
            "snapshot_timestamp": snapshot.get("metadata", {}).get("snapshot_timestamp"),
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
        pending = self.pending_entry_for(config) or {}
        confirmation = self.__class__._last_confirmation_results.get(self.pending_key(config)) or {}
        live_market = self.live_market_debug(config)
        signal_diagnostics = self.signal_diagnostics_for(config)
        self.label_pending_market_snapshots(config)
        ml_market_stats = self.ml_market_snapshot_stats(config)
        latest_ml_snapshot = self.latest_ml_snapshot_for_config(config)
        margin_limit = self.live_execution_service.margin_limit_debug(
            config,
            state.current_margin or config.base_margin_usdt,
            config.leverage,
        )
        now = datetime.utcnow()
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
            "median_imbalance": consensus.get("median_imbalance"),
            "raw_average_imbalance": consensus.get("raw_average_imbalance"),
            "anomalous_exchanges_count": consensus.get("anomalous_exchanges_count"),
            "imbalance_anomaly_min": consensus.get("imbalance_anomaly_min"),
            "imbalance_anomaly_max": consensus.get("imbalance_anomaly_max"),
            "excluded_anomalous_imbalance_exchanges": consensus.get("excluded_anomalous_imbalance_exchanges") or [],
            "average_momentum": consensus.get("average_momentum"),
            "consensus_direction": consensus.get("consensus_direction") or "none",
            "entry_blocked_reason": consensus.get("reject_reason") or (self.last_evaluation_for(config) or {}).get("reject_reason"),
            "entry_skip_reason": self.entry_skip_reason(consensus.get("reject_reason") or (self.last_evaluation_for(config) or {}).get("reject_reason")),
            "profit_protection": consensus.get("profit_protection") or {},
            "ml_mode": config.ml_mode,
            "ml_latest_snapshot": latest_ml_snapshot,
            "ml_score": (latest_ml_snapshot or {}).get("ml_score"),
            "ml_decision": (latest_ml_snapshot or {}).get("ml_decision"),
            "ml_reason": (latest_ml_snapshot or {}).get("ml_reason"),
            "ml_model_version": (latest_ml_snapshot or {}).get("ml_model_version"),
            "ml_market_snapshots_count": ml_market_stats["total"],
            "ml_market_snapshots_pending_count": ml_market_stats["pending"],
            "ml_market_snapshots_labeled_count": ml_market_stats["labeled"],
            "ml_exchange_labels_count": ml_market_stats["exchange_labels_total"],
            "ml_exchange_labels_pending_count": ml_market_stats["exchange_labels_pending"],
            "ml_exchange_labels_labeled_count": ml_market_stats["exchange_labels_labeled"],
            "ml_exchange_label_completion_percent": ml_market_stats["exchange_label_completion_percent"],
            "entry_mode": config.entry_mode,
            "resolved_live_symbol": live_market.get("resolved_live_symbol"),
            "live_market_type": live_market.get("live_market_type"),
            "live_market_valid": live_market.get("live_market_valid"),
            "live_market_error": live_market.get("live_market_error"),
            "live_market": live_market,
            **margin_limit,
            "pending_entry_exists": bool(pending),
            "pending_entry_side": pending.get("side"),
            "pending_entry_created_at": pending.get("created_at"),
            "pending_entry_expires_at": pending.get("expires_at"),
            "pending_entry_age_seconds": ((now - pending["created_at"]).total_seconds() if pending.get("created_at") else None),
            "pending_entry_expires_in_seconds": ((pending["expires_at"] - now).total_seconds() if pending.get("expires_at") else None),
            "pending_entry_first_momentum": pending.get("first_average_momentum"),
            "pending_entry_current_momentum": consensus.get("average_momentum"),
            "pending_entry_first_consensus": pending.get("first_consensus_direction"),
            "pending_entry_current_consensus": consensus.get("consensus_direction"),
            "pending_entry_status": pending.get("status") or confirmation.get("status"),
            "last_confirmation_result": confirmation.get("status"),
            "last_confirmation_reject_reason": confirmation.get("reject_reason"),
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
            "signal_diagnostics": signal_diagnostics["last_100"],
            "signal_diagnostics_last_100": signal_diagnostics["last_100"],
            **signal_diagnostics["counters"],
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

    def features(self, config, snapshot, record_history=True):
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
        source_time = (snapshot.get("metadata") or {}).get("source_timestamp") or snapshot.get("updated_at")
        if isinstance(source_time, (int, float)):
            source_time = datetime.utcfromtimestamp(source_time / 1000 if source_time > 1e11 else source_time)
        if source_time is not None and record_history:
            if not history or source_time > history[-1][0]:
                history.append((source_time, mid_price))
                self._last_price_timestamp[key] = source_time
        window = max(1, int(config.momentum_window_snapshots))
        prices = list(history)[-window:]
        if prices:
            prices = [point for point in prices if (prices[-1][0] - point[0]).total_seconds() <= 10]
        momentum = ((prices[-1][1] / prices[0][1]) - 1) if len(prices) >= 2 else 0
        return {
            "bid_volume_top_5": bid_volume,
            "ask_volume_top_5": ask_volume,
            "best_bid": best_bid,
            "best_ask": best_ask,
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

    def exchange_feature(self, config, snapshot, current_time, protective=False):
        exchange = snapshot.get("exchange")
        symbol = snapshot.get("symbol")
        age = None
        if snapshot.get("updated_at"):
            age = max(0, (current_time - snapshot["updated_at"]).total_seconds())
        error = None
        metadata = snapshot.get("metadata") or {}
        if metadata.get("market_type") != "swap" or metadata.get("linear") is not True or metadata.get("settle") != "USDT":
            error = "incompatible_futures_snapshot"
        source_time = metadata.get("source_timestamp")
        if source_time is None:
            error = "missing_source_timestamp"
        else:
            if isinstance(source_time, (int, float)):
                source_time = datetime.utcfromtimestamp(source_time / 1000 if source_time > 1e11 else source_time)
            age = max(age or 0, max(0, (current_time - source_time).total_seconds()))
            if source_time > current_time + timedelta(seconds=0 if protective else 1):
                error = "future_snapshot_timestamp"
        source_snapshot_time = snapshot.get("updated_at")
        item = {
            "exchange": exchange,
            "symbol": symbol,
            "bid_volume_top_5": None,
            "ask_volume_top_5": None,
            "imbalance": None,
            "raw_imbalance": None,
            "capped_imbalance": None,
            "is_imbalance_anomaly": False,
            "spread_percent": None,
            "momentum": None,
            "snapshot_age_seconds": age,
            "stale_seconds": age,
            "source_snapshot_time": source_snapshot_time,
            "fetch_latency_ms": metadata.get("fetch_latency_ms"),
            "long_signal": False,
            "short_signal": False,
            "valid": False,
            "reject_reason": None,
        }
        if error:
            item["reject_reason"] = error
            return item
        if age is not None and age > float(config.max_snapshot_age_seconds):
            item["reject_reason"] = "stale_snapshot"
            return item
        features, error = self.features(config, snapshot)
        if error:
            item["reject_reason"] = error
            return item
        raw_imbalance = features["imbalance"]
        anomaly_min = float(config.imbalance_anomaly_min)
        anomaly_max = float(config.imbalance_anomaly_max)
        is_anomaly = raw_imbalance < anomaly_min or raw_imbalance > anomaly_max
        item.update({
            "bid_volume_top_5": features["bid_volume_top_5"],
            "ask_volume_top_5": features["ask_volume_top_5"],
            "imbalance": raw_imbalance,
            "raw_imbalance": raw_imbalance,
            "capped_imbalance": min(max(raw_imbalance, anomaly_min), anomaly_max),
            "is_imbalance_anomaly": bool(is_anomaly),
            "spread_percent": features["spread_percent"],
            "momentum": features["short_momentum"],
            "bid": features["best_bid"],
            "ask": features["best_ask"],
            "mid_price": features["mid_price"],
            "long_signal": raw_imbalance > config.long_imbalance_threshold,
            "short_signal": raw_imbalance <= config.short_imbalance_threshold,
        })
        if age is not None and age > float(config.max_snapshot_age_seconds):
            item["reject_reason"] = "stale_snapshot"
            return item
        if not protective and features["spread_percent"] > config.max_spread_percent:
            item["reject_reason"] = "spread_too_high"
            return item
        if not protective and config.exclude_anomalous_imbalance and is_anomaly:
            item["reject_reason"] = "imbalance_anomaly"
            item["long_signal"] = False
            item["short_signal"] = False
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
        valid_imbalances = [row["imbalance"] for row in valid if row.get("imbalance") is not None]
        raw_imbalances = [row["raw_imbalance"] for row in rows if row.get("raw_imbalance") is not None]
        anomalous_rows = [row for row in rows if row.get("is_imbalance_anomaly")]
        excluded_anomalous = [
            f"{row.get('exchange')}:{row.get('symbol')}"
            for row in anomalous_rows
            if row.get("reject_reason") == "imbalance_anomaly"
        ]
        median_imbalance = self.median(valid_imbalances)
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
            "average_imbalance": (sum(valid_imbalances) / len(valid_imbalances)) if valid_imbalances else 0,
            "median_imbalance": median_imbalance,
            "raw_average_imbalance": (sum(raw_imbalances) / len(raw_imbalances)) if raw_imbalances else 0,
            "anomalous_exchanges_count": len(excluded_anomalous),
            "imbalance_anomaly_min": float(config.imbalance_anomaly_min),
            "imbalance_anomaly_max": float(config.imbalance_anomaly_max),
            "excluded_anomalous_imbalance_exchanges": excluded_anomalous,
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
        consensus["direction_rejections"] = direction_rejections(config, consensus)
        side, reason = consensus_side(config, consensus)
        consensus["consensus_direction"] = side or "none"
        consensus["reject_reason"] = reason
        return side, consensus
    def signal(self, config, state, features, current_time, pending_trade_id=None):
        risk_reason = self.risk_rejection(config, state, features, current_time, pending_trade_id)
        if risk_reason:
            self.observe_signal("risk_rejected", risk_reason)
            consensus = self.consensus_snapshot(config, current_time) if config.consensus_enabled else {"per_exchange_features": []}
            consensus["reject_reason"] = risk_reason
            self.store_last_evaluation(
                config,
                features,
                features["imbalance"] > config.long_imbalance_threshold and features["short_momentum"] > 0,
                features["imbalance"] <= config.short_imbalance_threshold and features["short_momentum"] < 0,
                "none",
                risk_reason,
                current_time,
                consensus,
            )
            self.reject(risk_reason, config, state)
            return None, consensus
        self.observe_signal("risk_passed")
        if config.consensus_enabled:
            side, consensus = self.consensus_signal(config, current_time)
            self.observe_signal("consensus_passed" if side else "consensus_rejected", consensus.get("reject_reason"), consensus)
            return side, consensus
        if features["imbalance"] > config.long_imbalance_threshold and features["short_momentum"] > 0:
            self.observe_signal("consensus_passed", details={"side": "long", "consensus_disabled": True})
            return "long", {}
        if features["imbalance"] <= config.short_imbalance_threshold and features["short_momentum"] < 0:
            self.observe_signal("consensus_passed", details={"side": "short", "consensus_disabled": True})
            return "short", {}
        self.observe_signal("consensus_rejected", "no_signal")
        return None, {"reject_reason": "no_signal"}

    def risk_rejection(self, config, state, features, current_time, pending_trade_id=None):
        if config.emergency_entry_block:
            return "emergency_entry_block"
        if not math.isfinite(float(config.leverage)) or config.leverage > config.max_leverage:
            return "max_leverage_exceeded"
        if state.is_stopped:
            return "strategy_stopped"
        if features["spread_percent"] > config.max_spread_percent:
            return "spread_too_high"
        if self.open_positions_count(config) - (1 if pending_trade_id else 0) >= config.max_open_positions:
            return "max_open_positions_reached"
        slot = db.session.get(ExecutionSlot, config.id)
        if slot and slot.trade_id != pending_trade_id:
            return "execution_reconciliation_required"
        if config.execution_mode != "live" and state.current_margin > self.available_equity(config):
            return "current_margin_exceeds_available_paper_equity"
        if config.execution_mode == "live":
            cached = self._live_equity.get(config.id)
            if not cached or (current_time - cached[0]).total_seconds() > 30:
                try:
                    balance = self.live_execution_service.client(config).fetch_balance({"type": "swap"})
                    self._live_equity[config.id] = (current_time, float((balance.get("free") or {}).get("USDT") or 0))
                except Exception:
                    return "live_balance_unavailable"
                state.current_margin = self.bounded_margin(config)
                if state.current_margin <= 0:
                    return "risk_budget_exhausted"
            live_reason = self.live_execution_service.validate_enabled(config, state.current_margin or config.base_margin_usdt)
            if live_reason:
                return live_reason
            if abs(self.live_daily_loss(config, current_time)) >= float(config.live_max_daily_loss_usdt):
                return "live_daily_loss_exceeded"
            if abs(self.live_total_loss(config)) >= float(config.live_max_total_loss_usdt):
                return "live_total_loss_exceeded"
            if self.open_live_trade(config):
                return "live_position_already_open"
            failed = self.last_live_open_failed(config)
            if failed:
                retry_at = failed.opened_at + timedelta(seconds=int(config.live_open_failed_cooldown_seconds))
                if current_time < retry_at:
                    return "live_open_failed_cooldown"
        if abs(self.daily_loss(config, current_time)) >= config.max_daily_loss_usdt:
            return "daily_loss_exceeded"
        if abs(self.total_loss(config)) >= config.max_total_loss_usdt:
            return "total_loss_exceeded"
        state.current_margin = self.bounded_margin(config, current_time)
        if state.current_margin <= 0:
            return "risk_budget_exhausted"
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

    def bounded_margin(self, config, current_time=None):
        current_time = current_time or datetime.utcnow()
        if config.execution_mode == "live":
            cached = self._live_equity.get(config.id)
            equity = cached[1] if cached and (datetime.utcnow() - cached[0]).total_seconds() <= 30 else 0
            fee_percent = float(config.live_fee_filter_taker_fee_percent)
            daily_remaining = max(0, float(config.live_max_daily_loss_usdt) - abs(self.live_daily_loss(config, current_time)))
            total_remaining = max(0, float(config.live_max_total_loss_usdt) - abs(self.live_total_loss(config)))
        else:
            equity = self.available_equity(config)
            fee_percent = float(config.paper_taker_fee_percent)
            daily_remaining = max(0, float(config.max_daily_loss_usdt) - abs(self.daily_loss(config, current_time)))
            total_remaining = max(0, float(config.max_total_loss_usdt) - abs(self.total_loss(config)))
        loss_fraction = float(config.stop_loss_percent_of_margin) / 100 + 2 * fee_percent / 100 * float(config.leverage)
        risk_budget = max(0, equity) * float(config.risk_per_trade_percent) / 100
        caps = [float(config.base_margin_usdt), float(config.max_position_margin_usdt), max(0, equity),
                min(risk_budget, daily_remaining, total_remaining) / loss_fraction]
        if config.execution_mode == "live":
            caps.append(float(config.live_max_margin_usdt))
        return max(0, min(caps))

    def trade_config(self, config, trade):
        if trade.execution_mode == "live" and not trade.execution_config_json:
            current_config = db.session.get(OrderBookPatternStrategyConfig, trade.strategy_config_id)
            if not PositionGuardian(self).prepare_legacy(current_config, trade):
                raise LiveExecutionError("legacy_trade_requires_execution_config_review")
        payload = self.parse_json(trade.execution_config_json)
        if not payload:
            payload = (self.parse_json(trade.decision_snapshot_json) or {}).get("config")
        if not isinstance(payload, dict):
            # Legacy positions may only be reconciled after explicit review.
            raise LiveExecutionError("legacy_trade_requires_execution_config_review")
        payload = dict(payload)
        payload.update(id=trade.strategy_config_id, exchange=trade.exchange, symbol=trade.symbol,
                       execution_mode=trade.execution_mode or "paper", leverage=trade.leverage)
        return SimpleNamespace(**payload)

    def paper_fill(self, snapshot, side, amount, config, current_time=None, not_before=None, protective=False):
        if not snapshot:
            return None
        current_time = current_time or datetime.utcnow()
        source = (snapshot.get("metadata") or {}).get("source_timestamp")
        if isinstance(source, (int, float)):
            source = datetime.utcfromtimestamp(source / 1000 if source > 1e11 else source)
        received = snapshot.get("updated_at")
        # Clock-skew tolerance for diagnostics must never allow a future paper fill.
        if (source and source > current_time) or (received and received > current_time):
            self.observe_signal("execution_waiting", "future_book_not_available")
            return None
        if not_before:
            source = (snapshot.get("metadata") or {}).get("source_timestamp")
            if isinstance(source, (int, float)):
                source = datetime.utcfromtimestamp(source / 1000 if source > 1e11 else source)
            if not source or source < not_before:
                self.observe_signal("execution_waiting", "no_new_book_after_latency")
                return None
        row = self.exchange_feature(config, snapshot, current_time or datetime.utcnow(), protective=protective)
        if not row.get("valid"):
            return None
        normalized, error = OrderBookNormalizer.normalize(snapshot.get("order_book"))
        if error:
            return None
        return consume_book(normalized, side, float(amount), float(config.paper_taker_fee_percent))

    def apply_live_open_result(self, trade, slot, result):
        tpsl = result.get("tpsl") or {}
        trade.entry_price = trade.live_entry_price = result["average_fill_price"]
        trade.amount = trade.live_filled_amount = result["filled_amount"]
        trade.notional = trade.entry_price * trade.amount
        trade.margin = trade.notional / trade.leverage
        trade.live_entry_fee = result["fee"]
        trade.total_fee = result["fee"]
        trade.live_exchange_order_id = result["order_id"]
        trade.live_status = result["status"]
        trade.live_error = result.get("warning")
        trade.live_raw_open_response_json = self.live_execution_service.raw_json(result.get("raw_response"))
        trade.exchange_tp_order_id = tpsl.get("tp_order_id") or trade.exchange_tp_order_id
        trade.exchange_sl_order_id = tpsl.get("sl_order_id") or trade.exchange_sl_order_id
        trade.exchange_tp_price = tpsl.get("tp_price") or trade.exchange_tp_price
        trade.exchange_sl_price = tpsl.get("sl_price") or trade.exchange_sl_price
        trade.tp_sl_created_at = tpsl.get("created_at") or trade.tp_sl_created_at
        trade.tp_sl_error = result.get("tpsl_error")
        trade.tp_sl_protected = bool(trade.exchange_tp_order_id and trade.exchange_sl_order_id and not trade.tp_sl_error)
        slot.status = "active"
        if not trade.tp_sl_protected and result.get("tpsl_error") != "protection_pending":
            db.session.get(OrderBookPatternStrategyConfig, trade.strategy_config_id).emergency_entry_block = True
        db.session.commit()

    def persist_protection(self, trade, result):
        trade.exchange_tp_order_id = result.get("tp_order_id")
        trade.exchange_sl_order_id = result.get("sl_order_id")
        trade.exchange_tp_price = result.get("tp_price")
        trade.exchange_sl_price = result.get("sl_price")
        trade.tp_sl_created_at = result.get("created_at")
        db.session.commit()

    def finalize_verified_close(self, trade, order, reason, state, config, current_time):
        price = order["average"]
        gross = order.get("realized_pnl")
        if gross is None:
            gross = self.calculate_pnl(trade.side, trade.entry_price, price, trade.notional)
        fee = self.live_execution_service.fee_cost(order)
        trade.live_exit_fee = fee
        trade.live_close_order_id = order["id"]
        trade.live_raw_close_response_json = json.dumps(order, default=str)
        trade.live_exit_price = trade.exit_price = price
        trade.gross_pnl = gross
        trade.total_fee = float(trade.live_entry_fee or 0) + fee
        trade.net_pnl = trade.pnl = gross - trade.total_fee
        trade.result = "win" if trade.pnl > 0 else "loss"
        trade.closed_at = current_time
        trade.reason_close = reason
        trade.live_status = "closed"
        trade.pnl_source = "exchange_order_details_excluding_funding"
        trade.live_error = "funding_not_reconciled"
        trade.holding_seconds = (current_time - trade.opened_at).total_seconds()
        # Funding may change the final outcome. Apply loss/win state only after settlement reconciliation.
        slot = db.session.get(ExecutionSlot, trade.strategy_config_id)
        if slot:
            db.session.delete(slot)
        db.session.commit()
        active_config = db.session.get(OrderBookPatternStrategyConfig, trade.strategy_config_id)
        PositionGuardian(self).funding(active_config, trade, current_time)
        try:
            cleanup = self.live_execution_service.cancel_exchange_tpsl_orders(config, trade)
            if cleanup and cleanup.get("errors"):
                trade.tp_sl_error = "protection_cleanup_unconfirmed"
                db.session.commit()
        except Exception:
            trade.tp_sl_error = "protection_cleanup_unconfirmed"
            db.session.commit()
        return self.trade_to_dict(trade)

    def reconcile_pending_trade(self, trade, config, state, current_time):
        slot = ExecutionSlot.query.filter_by(strategy_config_id=trade.strategy_config_id).with_for_update().first()
        if not slot:
            return None
        closing = trade.live_status in {"close_pending", "close_unknown"}
        try:
            frozen = self.live_execution_service.immutable_config(config, trade)
            client = self.live_execution_service.client(frozen)
            market = self.live_execution_service.market(client, trade.symbol)
            client_id = trade.live_close_client_order_id if closing else trade.live_client_order_id
            data = self.live_execution_service.mexc_order_read(client, market, client_id)
            order = self.live_execution_service.verified_mexc_order(market, data)
            if closing:
                if order["filled"] < float(trade.live_filled_amount or trade.amount) * (1 - 1e-8):
                    raise SubmissionUnknown("partial_close_requires_review")
                return self.finalize_verified_close(trade, order, "reconciled_close", state, frozen, current_time)
            else:
                # Persist fill before creating protection; a crash cannot cause another open.
                result = {"order_id": order["id"], "average_fill_price": order["average"],
                          "filled_amount": order["filled"], "fee": self.live_execution_service.fee_cost(order),
                          "status": "tp_sl_unprotected", "warning": "protection_requires_review", "raw_response": order,
                          "tpsl_error": "protection_requires_review"}
                self.apply_live_open_result(trade, slot, result)
            return self.trade_to_dict(trade)
        except OrderNotFilled:
            if closing:
                trade.live_status = "close_failed"
                slot.status = "active"
            else:
                trade.live_status = "open_failed"
                trade.closed_at = current_time
                trade.result = "rejected"
                db.session.delete(slot)
            trade.live_error = "exchange_confirmed_no_fill"
            db.session.commit()
            return self.trade_to_dict(trade)
        except Exception as error:
            trade.live_error = f"reconciliation_required:{type(error).__name__}"
            db.session.commit()
            return self.trade_to_dict(trade)

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
            "profit_protection": consensus.get("profit_protection") or {},
            "ml_prediction": consensus.get("ml_prediction") or {},
            "risk_decision": {
                "approved": True,
                "reason": None,
            },
            "entry_mode": consensus.get("entry_mode") or config.entry_mode,
            "first_signal_snapshot": consensus.get("first_signal_snapshot"),
            "confirmation_snapshot": consensus.get("confirmation_snapshot"),
            "confirmation_delay_actual_seconds": consensus.get("confirmation_delay_actual_seconds"),
            "confirmation_result": consensus.get("confirmation_result"),
            "consensus_decision": {
                "direction": consensus.get("consensus_direction"),
                "valid_exchanges_count": consensus.get("valid_exchanges_count"),
                "confirming_long_count": consensus.get("confirming_long_count"),
                "confirming_short_count": consensus.get("confirming_short_count"),
                "consensus_ratio_long": consensus.get("consensus_ratio_long"),
                "consensus_ratio_short": consensus.get("consensus_ratio_short"),
                "average_imbalance": consensus.get("average_imbalance"),
                "median_imbalance": consensus.get("median_imbalance"),
                "raw_average_imbalance": consensus.get("raw_average_imbalance"),
                "anomalous_exchanges_count": consensus.get("anomalous_exchanges_count"),
                "excluded_anomalous_imbalance_exchanges": consensus.get("excluded_anomalous_imbalance_exchanges") or [],
                "imbalance_anomaly_min": consensus.get("imbalance_anomaly_min"),
                "imbalance_anomaly_max": consensus.get("imbalance_anomaly_max"),
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

    def open_position(self, config, state, features, side, current_time, consensus=None, entry_context=None):
        if config.execution_mode == "paper":
            config = self.lock_config(config.id)
            db.session.refresh(state)
            if not config.enabled or state.is_stopped or config.emergency_entry_block:
                return self.reject("entries_paused", config, state)
            if not config.paper_session_id:
                self.create_paper_session(config)
        previous_slot = db.session.get(ExecutionSlot, config.id)
        if previous_slot:
            previous_trade = db.session.get(StrategyRunTrade, previous_slot.trade_id)
            if previous_trade and previous_trade.execution_mode == "paper" and previous_trade.closed_at:
                db.session.delete(previous_slot)
                db.session.commit()
            else:
                return self.reject("execution_slot_already_reserved", config, state)
        consensus = consensus or {}
        entry_context = entry_context or {}
        consensus.update({
            "entry_mode": entry_context.get("entry_mode") or config.entry_mode or "instant",
            "first_signal_snapshot": entry_context.get("first_signal_snapshot"),
            "confirmation_snapshot": entry_context.get("confirmation_snapshot"),
            "confirmation_delay_actual_seconds": entry_context.get("confirmation_delay_actual_seconds"),
            "confirmation_result": entry_context.get("confirmation_result") or ("confirmed" if entry_context else None),
        })
        margin = float(state.current_margin or config.base_margin_usdt)
        notional = margin * float(config.leverage)
        entry_price = features["mid_price"]
        amount = notional / entry_price if entry_price else 0
        if config.execution_mode != "live":
            execution_snapshot = self.snapshot_for(config.exchange, config.symbol) or {}
            self.features(config, execution_snapshot)
            book_side = "asks" if side == "long" else "bids"
            normalized, error = OrderBookNormalizer.normalize(execution_snapshot.get("order_book") or {})
            if error:
                self.observe_signal("execution_rejected", error)
                return self.reject(error, config, state)
            reference = float(normalized[book_side][0]["price"])
            amount, error = executable_amount(notional / reference, reference, execution_snapshot.get("metadata") or {},
                notional, strict=getattr(self, "strict_replay", False))
            if error:
                self.observe_signal("execution_rejected", error)
                return self.reject(error, config, state)
        live_result = None
        live_error = None
        tpsl = (live_result or {}).get("tpsl") or {}
        tpsl_error = (live_result or {}).get("tpsl_error")
        entry_reason = f"side={side}, consensus={consensus.get('consensus_direction')}, imbalance={features['imbalance']:.4f}, momentum={features['short_momentum']:.8f}, spread={features['spread_percent']:.4f}"
        if consensus.get("entry_mode") == "two_step_confirmation":
            entry_reason = (
                f"{entry_reason}, entry_mode=two_step_confirmation, "
                f"first_signal_time={entry_context.get('first_signal_time')}, confirmation_time={entry_context.get('confirmation_time')}, "
                f"first_momentum={(entry_context.get('first_signal_snapshot') or {}).get('average_momentum')}, "
                f"confirmed_momentum={(entry_context.get('confirmation_snapshot') or {}).get('average_momentum')}, "
                f"first_consensus={(entry_context.get('first_signal_snapshot') or {}).get('consensus_direction')}, "
                f"confirmed_consensus={(entry_context.get('confirmation_snapshot') or {}).get('consensus_direction')}"
            )
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
            reason_open=entry_reason[:255],
            opened_at=current_time,
            closed_at=current_time if live_error else None,
            result="rejected" if live_error else None,
            signal_consensus_direction=consensus.get("consensus_direction"),
            signal_valid_exchanges_count=consensus.get("valid_exchanges_count"),
            signal_confirming_long_count=consensus.get("confirming_long_count"),
            signal_confirming_short_count=consensus.get("confirming_short_count"),
            signal_consensus_ratio_long=consensus.get("consensus_ratio_long"),
            signal_consensus_ratio_short=consensus.get("consensus_ratio_short"),
            signal_average_imbalance=consensus.get("average_imbalance"),
            signal_median_imbalance=consensus.get("median_imbalance"),
            signal_raw_average_imbalance=consensus.get("raw_average_imbalance"),
            signal_anomalous_exchanges_count=consensus.get("anomalous_exchanges_count"),
            signal_excluded_anomalous_imbalance_exchanges_json=json.dumps(consensus.get("excluded_anomalous_imbalance_exchanges") or []),
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
            median_imbalance=consensus.get("median_imbalance"),
            raw_average_imbalance=consensus.get("raw_average_imbalance"),
            anomalous_exchanges_count=consensus.get("anomalous_exchanges_count"),
            excluded_anomalous_imbalance_exchanges_json=json.dumps(consensus.get("excluded_anomalous_imbalance_exchanges") or []),
            average_momentum=consensus.get("average_momentum"),
            configured_exchange_imbalance=consensus.get("configured_exchange_imbalance"),
            configured_exchange_spread=consensus.get("configured_exchange_spread"),
            configured_exchange_momentum=consensus.get("configured_exchange_momentum"),
            entry_reason=entry_reason[:255],
            entry_mode=consensus.get("entry_mode") or "instant",
            first_signal_snapshot_json=json.dumps(consensus.get("first_signal_snapshot"), default=str) if consensus.get("first_signal_snapshot") else None,
            confirmation_snapshot_json=json.dumps(consensus.get("confirmation_snapshot"), default=str) if consensus.get("confirmation_snapshot") else None,
            confirmation_delay_actual_seconds=consensus.get("confirmation_delay_actual_seconds"),
            confirmation_result=consensus.get("confirmation_result"),
            execution_mode=config.execution_mode or "paper",
            execution_config_json=json.dumps(self.config_to_dict(config), default=str),
            paper_session_id=config.paper_session_id if config.execution_mode == "paper" else None,
            pending_entry_expires_at=(current_time + timedelta(seconds=config.pending_entry_ttl_seconds)) if config.execution_mode == "paper" else None,
            live_client_order_id=f"arbi_{uuid4().hex}",
            live_exchange_order_id=live_result.get("order_id") if live_result else None,
            live_entry_price=live_result.get("average_fill_price") if live_result else None,
            live_filled_amount=live_result.get("filled_amount") if live_result else None,
            live_entry_fee=live_result.get("fee") if live_result else None,
            live_status=(live_result.get("status") if live_result else ("open_failed" if live_error else None)),
            live_error=live_error or tpsl_error or (live_result.get("warning") if live_result else None),
            live_raw_open_response_json=self.live_execution_service.raw_json(live_result.get("raw_response")) if live_result else None,
            gross_pnl=0,
            net_pnl=0,
            total_fee=live_result.get("fee") if live_result else 0,
            exchange_tp_order_id=tpsl.get("tp_order_id"),
            exchange_sl_order_id=tpsl.get("sl_order_id"),
            exchange_tp_price=tpsl.get("tp_price"),
            exchange_sl_price=tpsl.get("sl_price"),
            tp_sl_protected=bool(tpsl and not tpsl_error),
            tp_sl_error=tpsl_error,
            tp_sl_created_at=tpsl.get("created_at"),
        )
        state.last_opened_at = current_time
        if config.execution_mode == "paper":
            trade.live_status = "paper_pending"
        db.session.add(trade)
        db.session.flush()
        slot = ExecutionSlot(strategy_config_id=config.id, trade_id=trade.id, client_order_id=trade.live_client_order_id, status="paper_pending" if config.execution_mode == "paper" else "opening")
        db.session.add(slot)
        try:
            db.session.commit()
        except IntegrityError:
            db.session.rollback()
            return self.reject("execution_slot_already_reserved", config, state)
        self.observe_signal("order_reserved", details={"trade_id": trade.id, "side": side})
        if config.execution_mode == "live":
            trade.live_status = "open_pending"
            db.session.commit()
            try:
                live_result = self.live_execution_service.open_position(
                    config, side, margin, config.leverage, entry_price, trade.live_client_order_id,
                    on_fill=lambda result: self.apply_live_open_result(trade, slot, result),
                    on_protection=lambda result: self.persist_protection(trade, result))
                self.apply_live_open_result(trade, slot, live_result)
                PositionGuardian(self).protection(config, trade, datetime.utcnow())
            except LiveExecutionError as error:
                trade.live_error = str(error)
                if isinstance(error, SubmissionUnknown):
                    trade.live_status = "open_unknown"
                    slot.status = "open_unknown"
                else:
                    trade.live_status = "open_failed"
                    trade.closed_at = current_time
                    trade.result = "rejected"
                    db.session.delete(slot)
                db.session.commit()
                return self.trade_to_dict(trade)
            except Exception as error:
                trade.live_status = "open_unknown"
                trade.live_error = f"submit_outcome_unknown:{type(error).__name__}"
                slot.status = "open_unknown"
                db.session.commit()
                return self.trade_to_dict(trade)
        else:
            pass  # Paper reservation and pending status were committed atomically.
        self.create_ml_feature_snapshot(config, state, features, consensus, side, (None if live_error else side), current_time, trade)
        payload = self.trade_to_dict(trade)
        if live_error:
            logger.warning("OrderBookRecovery live open failed: trade_id=%s error=%s", trade.id, live_error)
            return payload
        self.record_opened_side(config, side)
        logger.info("OrderBookRecovery position opened: trade_id=%s side=%s margin=%s entry=%s", trade.id, side, margin, entry_price)
        self.publisher.publish("orderbook_recovery.position_opened", payload)
        return payload

    def evaluate_open_trade(self, trade, current_price: float, state, config, current_time):
        if trade.execution_mode == "paper":
            active_config = self.lock_config(trade.strategy_config_id)
            db.session.refresh(trade)
            db.session.refresh(state)
            if trade.closed_at:
                return self.trade_to_dict(trade)
        if trade.execution_mode == "live" and trade.live_status in {"open_pending", "open_unknown", "close_pending", "close_unknown"}:
            return self.reconcile_pending_trade(trade, config, state, current_time)
        if trade.live_status == "paper_pending":
            expires = trade.pending_entry_expires_at or (trade.opened_at + timedelta(seconds=getattr(config, "pending_entry_ttl_seconds", 5)))
            if current_time >= expires:
                return self.cancel_paper_entry(trade, "expired", current_time)
            if not active_config.enabled or state.is_stopped or active_config.emergency_entry_block or active_config.execution_mode != "paper":
                return self.cancel_paper_entry(trade, "paused", current_time)
            if current_time < trade.opened_at + timedelta(milliseconds=config.paper_latency_ms):
                return None
            snapshot = self.snapshot_for(trade.exchange, trade.symbol)
            source = (snapshot or {}).get("metadata", {}).get("source_timestamp")
            if isinstance(source, (int, float)):
                source = datetime.utcfromtimestamp(source / 1000 if source > 1e11 else source)
            receipt = (snapshot or {}).get("updated_at")
            if not source or (config.paper_latency_ms > 0 and source < trade.opened_at + timedelta(milliseconds=config.paper_latency_ms)) or source > current_time or (receipt and receipt > current_time):
                trade.live_error = "pending_entry_waiting_for_post_delay_book"
                db.session.commit()
                return None
            row = self.exchange_feature(config, snapshot, current_time) if snapshot else {}
            if not row.get("valid"):
                trade.live_error = "pending_entry_" + (row.get("reject_reason") or "no_fresh_book")
                db.session.commit()
                return None
            features, error = self.features(config, snapshot)
            signal, consensus = self.signal(config, state, features, current_time, pending_trade_id=trade.id)
            if signal != trade.side:
                return self.cancel_paper_entry(trade, consensus.get("reject_reason") or "signal_changed", current_time)
            reason = self.feedback_snapshot(config, signal, consensus, current_time).get("feedback_reject_reason")
            reason = reason or self.profit_protection_rejection(config, state, features, signal, consensus, current_time)
            reason = reason or self.risk_rejection(active_config, state, features, current_time, pending_trade_id=trade.id)
            if reason:
                return self.cancel_paper_entry(trade, reason, current_time)
            cap = min(self.bounded_margin(config, current_time), self.bounded_margin(active_config, current_time))
            reference = features["best_ask"] if trade.side == "long" else features["best_bid"]
            amount, error = executable_amount(min(trade.amount, cap * trade.leverage / reference), reference,
                snapshot.get("metadata") or {}, cap * trade.leverage, strict=getattr(self, "strict_replay", False))
            if error or not amount:
                return self.cancel_paper_entry(trade, error or "risk_budget_changed", current_time)
            fill = self.paper_fill(snapshot, trade.side, amount, config, current_time,
                trade.opened_at + timedelta(milliseconds=config.paper_latency_ms) if config.paper_latency_ms else None)
            if not fill:
                trade.live_error = "paper_insufficient_depth"
                self.observe_signal("execution_rejected", "paper_insufficient_depth")
                return None
            if fill["price"] * amount / trade.leverage > cap + 1e-9:
                return self.cancel_paper_entry(trade, "risk_budget_changed", current_time)
            trade.amount = amount
            trade.entry_price = fill["price"]
            trade.notional = trade.entry_price * trade.amount
            trade.margin = trade.notional / trade.leverage
            trade.live_entry_fee = fill["fee"]
            trade.total_fee = fill["fee"]
            trade.opened_at = current_time
            trade.live_status = None
            trade.live_error = None
            slot = db.session.get(ExecutionSlot, trade.strategy_config_id)
            if slot:
                slot.status = "active"
            self.observe_signal("paper_filled", details={"trade_id": trade.id, "price": fill["price"]})
            db.session.commit()
            return None
        if trade.execution_mode != "live" and trade.paper_close_requested_at:
            return self.close_trade(trade, current_price, trade.pnl, trade.paper_close_reason, state, config, current_time)
        pnl = self.calculate_pnl(trade.side, trade.entry_price, current_price, trade.notional)
        trade.pnl = pnl
        if trade.execution_mode == "live":
            try:
                if not self.live_execution_service.position_is_open(config, trade):
                    try:
                        order = self.live_execution_service.external_close_order(config, trade)
                        return self.finalize_verified_close(trade, order, "exchange_position_closed_external", state, config, current_time)
                    except Exception:
                        trade.live_status = "external_close_unreconciled"
                        trade.live_error = "exchange_position_missing_history_required"
                        db.session.commit()
                        return self.trade_to_dict(trade)
            except Exception as error:
                trade.live_error = str(error)
        if trade.execution_mode != "live":
            close_side = "short" if trade.side == "long" else "long"
            fill = self.paper_fill(self.snapshot_for(trade.exchange, trade.symbol), close_side, trade.amount, config, current_time, protective=True)
            if not fill:
                trade.paper_exit_status = "unresolved_no_fresh_book_or_depth"
                snapshot = self.snapshot_for(trade.exchange, trade.symbol)
                row = self.exchange_feature(config, snapshot, current_time, protective=True) if snapshot else {}
                if row.get("valid"):
                    quote = row["bid"] if trade.side == "long" else row["ask"]
                    indicated = self.calculate_pnl(trade.side, trade.entry_price, quote, trade.notional)
                    if indicated <= -trade.margin * float(config.stop_loss_percent_of_margin) / 100:
                        # Latch a stop intent, never book an indicative price as a fill.
                        return self.close_trade(trade, quote, indicated, "stop_loss", state, config, current_time)
                db.session.commit()
                return None
            trade.paper_exit_status = None
            current_price = fill["price"]
            pnl = self.calculate_pnl(trade.side, trade.entry_price, current_price, trade.notional)
        target_profit = trade.margin * (float(config.take_profit_percent_of_margin) / 100)
        max_loss = trade.margin * (float(config.stop_loss_percent_of_margin) / 100)
        if pnl >= target_profit:
            return self.close_trade(trade, current_price, pnl, "take_profit", state, config, current_time)
        if pnl <= -max_loss:
            return self.close_trade(trade, current_price, pnl, "stop_loss", state, config, current_time)
        db.session.commit()
        return None

    def reconcile_existing_positions(self):
        """Read exchange state even when entry scanning is stopped or books are unavailable."""
        self.__class__._paper_worker_heartbeat = datetime.utcnow()
        paper_trades = StrategyRunTrade.query.filter(
            StrategyRunTrade.execution_mode == "paper", StrategyRunTrade.closed_at.is_(None),
            or_(StrategyRunTrade.live_status.is_(None), StrategyRunTrade.live_status != "paper_pending")).all()
        for trade in paper_trades:
            try:
                config = self.lock_config(trade.strategy_config_id)
                db.session.refresh(trade)
                if trade.closed_at:
                    db.session.commit()
                    continue
                frozen = self.trade_config(config, trade)
                state = self.get_or_create_state(config)
                now = datetime.utcnow()
                snapshot = self.snapshot_for(trade.exchange, trade.symbol)
                row = self.exchange_feature(frozen, snapshot, now, protective=True) if snapshot else {}
                if row.get("valid"):
                    self.evaluate_open_trade(trade, row["mid_price"], state, frozen, now)
                else:
                    trade.paper_exit_status = "unresolved_no_fresh_valid_book"
                db.session.commit()
            except Exception:
                db.session.rollback()
                logger.exception("Paper position management failed trade_id=%s", trade.id)
        pending = StrategyRunTrade.query.filter_by(execution_mode="paper", live_status="paper_pending", closed_at=None).all()
        for trade in pending:
            config = self.lock_config(trade.strategy_config_id)
            db.session.refresh(trade)
            now = datetime.utcnow()
            expiry = trade.pending_entry_expires_at or trade.opened_at + timedelta(seconds=5)
            if now >= expiry or not config.enabled or config.emergency_entry_block:
                self.cancel_paper_entry(trade, "expired" if now >= expiry else "paused", now)
            else:
                db.session.commit()
        trades = StrategyRunTrade.query.filter(
            StrategyRunTrade.execution_mode == "live", StrategyRunTrade.closed_at.is_(None),
            StrategyRunTrade.live_status.isnot(None)).all()
        for trade in trades:
            config = db.session.get(OrderBookPatternStrategyConfig, trade.strategy_config_id)
            state = self.get_or_create_state(config)
            try:
                guardian = PositionGuardian(self)
                if not guardian.prepare_legacy(config, trade):
                    continue
                frozen = self.trade_config(config, trade)
                if trade.live_status in {"open_pending", "open_unknown", "close_pending", "close_unknown"}:
                    self.reconcile_pending_trade(trade, frozen, state, datetime.utcnow())
                elif not self.live_execution_service.position_is_open(frozen, trade):
                    order = self.live_execution_service.external_close_order(frozen, trade)
                    self.finalize_verified_close(trade, order, "exchange_position_closed_external", state, frozen, datetime.utcnow())
                else:
                    guardian.protection(config, trade, datetime.utcnow())
                    guardian.funding(config, trade, datetime.utcnow())
            except Exception as error:
                trade.live_error = f"periodic_reconciliation_required:{type(error).__name__}"
                db.session.commit()
        # Include closed trades until delayed settlement records are accounted for.
        closed = StrategyRunTrade.query.filter(StrategyRunTrade.execution_mode == "live",
            StrategyRunTrade.closed_at.isnot(None), StrategyRunTrade.result != "rejected",
            or_(StrategyRunTrade.funding_status.is_(None), StrategyRunTrade.funding_status != "reconciled")).order_by(StrategyRunTrade.funding_checked_at.asc().nullsfirst(), StrategyRunTrade.id.asc()).limit(100).all()
        for trade in closed:
            config = db.session.get(OrderBookPatternStrategyConfig, trade.strategy_config_id)
            guardian = PositionGuardian(self)
            if guardian.prepare_legacy(config, trade):
                guardian.funding(config, trade, datetime.utcnow())

    def exchange_tpsl_close_reason(self, trade, current_price):
        if trade.side == "long":
            if trade.exchange_tp_price and current_price >= trade.exchange_tp_price:
                return "exchange_take_profit"
            if trade.exchange_sl_price and current_price <= trade.exchange_sl_price:
                return "exchange_stop_loss"
        else:
            if trade.exchange_tp_price and current_price <= trade.exchange_tp_price:
                return "exchange_take_profit"
            if trade.exchange_sl_price and current_price >= trade.exchange_sl_price:
                return "exchange_stop_loss"
        return "exchange_position_closed"

    def close_exchange_closed_trade(
        self,
        trade,
        exit_price,
        pnl,
        reason,
        state,
        config,
        current_time,
        exit_price_fallback_used=False,
        exit_price_warning=None,
        pnl_source="fallback_market_price",
    ):
        if trade.exchange_tp_order_id or trade.exchange_sl_order_id:
            try:
                tpsl_cancel = self.live_execution_service.cancel_exchange_tpsl_orders(config, trade) or {}
                if tpsl_cancel.get("errors"):
                    trade.tp_sl_error = "; ".join(tpsl_cancel["errors"])
            except Exception as error:
                trade.tp_sl_error = str(error)
        trade.exit_price = exit_price
        entry_fee = float(trade.live_entry_fee or 0)
        trade.gross_pnl = pnl
        trade.total_fee = entry_fee
        trade.net_pnl = pnl - entry_fee
        trade.pnl = trade.net_pnl
        trade.result = "win" if trade.net_pnl > 0 else "loss"
        trade.reason_close = reason
        trade.closed_at = current_time
        trade.live_exit_price = exit_price
        trade.live_status = "closed"
        trade.exit_price_fallback_used = bool(exit_price_fallback_used)
        trade.exit_price_warning = exit_price_warning
        trade.pnl_source = pnl_source
        trade.tp_sl_protected = False
        trade.holding_seconds = (trade.closed_at - trade.opened_at).total_seconds() if trade.opened_at else None
        self.update_ml_snapshots_for_trade(trade)
        self.apply_recovery_after_close(state, config, trade.result, current_time)
        db.session.commit()
        payload = self.trade_to_dict(trade)
        logger.info("OrderBookRecovery exchange-side position closed: trade_id=%s reason=%s pnl=%s", trade.id, reason, trade.pnl)
        self.publisher.publish("orderbook_recovery.position_closed", payload)
        return payload

    def close_trade(self, trade, exit_price, pnl, reason, state, config, current_time):
        if trade.execution_mode == "paper":
            self.lock_config(trade.strategy_config_id)
            db.session.refresh(trade)
            if trade.live_status == "paper_pending":
                return self.cancel_paper_entry(trade, "manual_cancel", current_time)
        if trade.closed_at:
            return self.trade_to_dict(trade)
        config = self.trade_config(config, trade)
        if trade.execution_mode == "paper" and not trade.paper_close_requested_at:
            trade.paper_close_requested_at = current_time
            trade.paper_close_reason = reason
            trade.paper_exit_status = "pending_fixed_latency"
            db.session.commit()
        if trade.execution_mode != "live" and int(config.paper_latency_ms) > 0:
            if not trade.paper_close_requested_at:
                trade.paper_close_requested_at = current_time
                trade.paper_close_reason = reason
                db.session.commit()
            if current_time < trade.paper_close_requested_at + timedelta(milliseconds=int(config.paper_latency_ms)):
                return self.trade_to_dict(trade)
        if trade.execution_mode == "live":
            slot = ExecutionSlot.query.filter_by(strategy_config_id=trade.strategy_config_id).populate_existing().with_for_update().first()
            if not slot:
                trade.live_error = "missing_execution_slot_requires_review"
                db.session.commit()
                return None
            if slot.status in {"close_pending", "close_unknown", "open_unknown", "opening"}:
                return self.reconcile_pending_trade(trade, config, state, current_time)
            slot.status = "close_pending"
            trade.live_status = "close_pending"
            trade.live_close_client_order_id = f"arbi_close_{uuid4().hex}"
            db.session.commit()
            try:
                live_result = self.live_execution_service.close_position(config, trade, exit_price)
                verified = live_result.get("raw_response") or {}
                return self.finalize_verified_close(trade, {
                    "id": live_result["order_id"], "average": live_result["average_fill_price"],
                    "fee": {"cost": live_result["fee"]}, "realized_pnl": verified.get("realized_pnl"),
                    "raw": verified}, reason, state, config, current_time)
            except Exception as error:
                if self.live_execution_service.is_position_already_closed_error(error):
                    try:
                        order = self.live_execution_service.external_close_order(config, trade)
                        return self.finalize_verified_close(trade, order, "exchange_position_already_closed", state, config, current_time)
                    except Exception:
                        pass
                    trade.live_status = "external_close_unreconciled"
                    trade.live_error = "exchange_position_already_closed_history_required"
                    slot.status = "external_close_unreconciled"
                    db.session.commit()
                    return None
                trade.live_status = "close_unknown"
                slot.status = "close_unknown"
                trade.live_error = str(error)
                db.session.commit()
                logger.warning("OrderBookRecovery live close failed: trade_id=%s error=%s", trade.id, error)
                return None
        if trade.execution_mode != "live":
            self.lock_config(trade.strategy_config_id)
            db.session.refresh(trade)
            if trade.closed_at:
                return self.trade_to_dict(trade)
            fill = self.paper_fill(self.snapshot_for(trade.exchange, trade.symbol), "short" if trade.side == "long" else "long", trade.amount, config, current_time,
                trade.paper_close_requested_at + timedelta(milliseconds=config.paper_latency_ms) if trade.paper_close_requested_at and config.paper_latency_ms else None, protective=True)
            if not fill:
                trade.paper_exit_status = "unresolved_no_fresh_book_or_depth"
                db.session.commit()
                return self.trade_to_dict(trade)
            exit_price = fill["price"]
            pnl = self.calculate_pnl(trade.side, trade.entry_price, exit_price, trade.notional)
            trade.gross_pnl = pnl
            trade.total_fee = float(trade.live_entry_fee or 0) + fill["fee"]
            pnl -= trade.total_fee
            pnl += float(trade.funding_pnl or 0)
            trade.net_pnl = pnl
            trade.exit_price = exit_price
            trade.paper_exit_status = "filled"
        trade.pnl = pnl
        trade.result = "win" if pnl > 0 else "loss"
        trade.reason_close = reason
        trade.closed_at = current_time
        trade.holding_seconds = (trade.closed_at - trade.opened_at).total_seconds() if trade.opened_at else None
        self.update_ml_snapshots_for_trade(trade)
        self.apply_recovery_after_close(state, config, trade.result, current_time)
        slot = db.session.get(ExecutionSlot, trade.strategy_config_id)
        if slot:
            db.session.delete(slot)
        db.session.commit()
        payload = self.trade_to_dict(trade)
        logger.info("OrderBookRecovery position closed: trade_id=%s reason=%s pnl=%s", trade.id, reason, pnl)
        self.publisher.publish("orderbook_recovery.position_closed", payload)
        return payload

    def apply_recovery_after_close(self, state, config, result, current_time=None):
        state.last_trade_result = result
        state.last_closed_at = current_time or datetime.utcnow()
        # A loss never increases the next position size.
        state.current_step = 0
        state.current_margin = self.bounded_margin(config, current_time)
        if result == "win":
            state.current_step = 0
            state.consecutive_losses = 0
            state.current_margin = self.bounded_margin(config)
            state.is_stopped = False
            state.stop_reason = None
            state.paused_until = None
            return state

        state.consecutive_losses += 1
        if state.consecutive_losses >= config.max_consecutive_losses:
            state.is_stopped = True
            state.stop_reason = "max_recovery_pause"
            state.paused_until = (current_time or datetime.utcnow()) + timedelta(seconds=int(config.cooldown_after_max_recovery_seconds))
            state.current_step = 0
            state.current_margin = self.bounded_margin(config)
            state.consecutive_losses = 0
            config.enabled = False
            persisted_config = db.session.get(OrderBookPatternStrategyConfig, state.strategy_config_id)
            if persisted_config:
                persisted_config.enabled = False
            run = self.active_run(config)
            if run:
                run.status = "stopped"
                run.stopped_at = state.last_closed_at
                run.stop_reason = state.stop_reason
            return state
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
        return StrategyRunTrade.query.filter(
            StrategyRunTrade.strategy_config_id == config.id,
            StrategyRunTrade.closed_at.is_(None),
            or_(StrategyRunTrade.live_status.is_(None), StrategyRunTrade.live_status != "open_failed"),
        ).order_by(StrategyRunTrade.id.desc()).first()

    def open_live_trade(self, config):
        return StrategyRunTrade.query.filter(
            StrategyRunTrade.strategy_config_id == config.id,
            StrategyRunTrade.closed_at.is_(None),
            StrategyRunTrade.execution_mode == "live",
            or_(StrategyRunTrade.live_status.is_(None), StrategyRunTrade.live_status != "open_failed"),
        ).order_by(StrategyRunTrade.id.desc()).first()

    def last_live_open_failed(self, config):
        return StrategyRunTrade.query.filter_by(
            strategy_config_id=config.id,
            execution_mode="live",
            live_status="open_failed",
        ).order_by(StrategyRunTrade.opened_at.desc()).first()

    def open_positions_count(self, config):
        return StrategyRunTrade.query.filter(
            StrategyRunTrade.strategy_config_id == config.id,
            StrategyRunTrade.closed_at.is_(None),
            or_(StrategyRunTrade.live_status.is_(None), StrategyRunTrade.live_status != "open_failed"),
        ).count()

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
            trade.pnl for trade in self.metrics_trades_query(config)
            .filter(StrategyRunTrade.closed_at >= start, StrategyRunTrade.pnl < 0)
            .all()
        ]
        return sum(losses)

    def total_loss(self, config):
        losses = [trade.pnl for trade in self.metrics_trades_query(config).filter(StrategyRunTrade.pnl < 0).all()]
        return sum(losses)

    def live_daily_loss(self, config, current_time=None):
        current_time = current_time or datetime.utcnow()
        start = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
        losses = [
            trade.pnl for trade in self.metrics_trades_query(config, mode="live")
            .filter(StrategyRunTrade.execution_mode == "live", StrategyRunTrade.closed_at >= start, StrategyRunTrade.pnl < 0)
            .all()
        ]
        return sum(losses)

    def live_total_loss(self, config):
        losses = [
            trade.pnl for trade in self.metrics_trades_query(config, mode="live")
            .filter(StrategyRunTrade.execution_mode == "live", StrategyRunTrade.pnl < 0)
            .all()
        ]
        return sum(losses)

    def available_equity(self, config):
        realized = sum(trade.pnl for trade in self.metrics_trades_query(config, mode="paper").all())
        return self.paper_initial_equity(config) + realized

    def metrics(self):
        config = self.get_or_create_config()
        trades = self.metrics_trades_query(config).filter_by(is_archived=False).all()
        archived_trades = self.metrics_trades_query(config).filter_by(is_archived=True).all()
        return self.metrics_without_state_query(config, self.open_trade(config))

    def metrics_for_run(self, run):
        config = db.session.get(OrderBookPatternStrategyConfig, run.strategy_config_id)
        live_not_failed = or_(StrategyRunTrade.live_status.is_(None), StrategyRunTrade.live_status.notin_(["open_failed", "paper_cancelled"]))
        trades = StrategyRunTrade.query.filter(
            StrategyRunTrade.strategy_run_id == run.id,
            StrategyRunTrade.is_archived.is_(False),
            live_not_failed,
        ).all()
        archived_trades = StrategyRunTrade.query.filter(
            StrategyRunTrade.strategy_run_id == run.id,
            StrategyRunTrade.is_archived.is_(True),
            live_not_failed,
        ).all()
        open_trade = StrategyRunTrade.query.filter(
            StrategyRunTrade.strategy_run_id == run.id,
            StrategyRunTrade.closed_at.is_(None),
            live_not_failed,
        ).first()
        return self.calculate_metrics(trades, config.paper_equity_usdt if config else 10000, open_trade, archived_trades)

    def calculate_metrics(self, trades, initial_equity, open_trade=None, archived_trades=None):
        archived_trades = archived_trades or []
        trades = sorted(trades, key=lambda trade: (trade.closed_at or trade.opened_at, trade.id))
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
            "open_position": self.trade_to_dict(open_trade) if open_trade and open_trade.live_status != "paper_pending" else None,
            "pending_order": self.trade_to_dict(open_trade) if open_trade and open_trade.live_status == "paper_pending" else None,
            "paper_execution_model": "fixed_latency_full_depth_no_partial_fills_no_queue",
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
        open_trade = self.open_trade(config)
        paper_exit = None
        if open_trade and open_trade.execution_mode == "paper" and open_trade.live_status != "paper_pending":
            from src.OrderBookRecovery.PaperExitDiagnostics import exit_diagnostics
            paper_exit = exit_diagnostics(open_trade, self.trade_config(config, open_trade),
                self.snapshot_for(open_trade.exchange, open_trade.symbol), datetime.utcnow(),
                getattr(self.__class__, "_paper_worker_heartbeat", None))
        latest_snapshot = self.latest_snapshot_for(config)
        margin_limit = self.live_execution_service.margin_limit_debug(
            config,
            state.current_margin or config.base_margin_usdt,
            config.leverage,
        )
        return {
            "config": self.config_to_dict(config),
            "recovery_state": self.state_to_dict(state),
            "paper_exit_diagnostics": paper_exit,
            "status": self.status_for(config, state),
            "enabled": config.enabled,
            "exchange": config.exchange,
            "symbol": config.symbol,
            "exchange_id": config.exchange_id,
            "trading_pair_id": config.trading_pair_id,
            "open_position": self.trade_to_dict(open_trade) if open_trade and open_trade.live_status != "paper_pending" else None,
            "pending_order": self.trade_to_dict(open_trade) if open_trade and open_trade.live_status == "paper_pending" else None,
            "paper_execution_model": "fixed_latency_full_depth_no_partial_fills_no_queue",
            "last_evaluation": self.last_evaluation_for(config),
            "latest_snapshot": latest_snapshot,
            "last_order_book_snapshot_time": (latest_snapshot or {}).get("updated_at"),
            "reason_if_not_trading": self.reason_if_not_trading(config, state),
            "live_market": self.live_market_debug(config),
            **margin_limit,
            "metrics": self.metrics_without_state_query(config, open_trade=open_trade),
        }

    def metrics_without_state_query(self, config, open_trade=None):
        trades = self.metrics_trades_query(config).filter_by(is_archived=False).all()
        archived_trades = self.metrics_trades_query(config).filter_by(is_archived=True).all()
        if open_trade is None:
            open_trade = self.open_trade(config)
        if open_trade and (open_trade.execution_mode != config.execution_mode or open_trade.live_status == "paper_pending"):
            open_trade = None
        result = self.calculate_metrics(trades, self.paper_initial_equity(config), open_trade, archived_trades)
        result.update(execution_mode=config.execution_mode, paper_session_id=getattr(config, "paper_session_id", None),
                      accounting_scope="paper_session" if config.execution_mode == "paper" else "live_history")
        return result

    def metrics_trades_query(self, config=None, mode=None):
        mode = mode or (config.execution_mode if config else "paper")
        query = self.closed_trades_query(config).filter(StrategyRunTrade.execution_mode == mode,
            or_(StrategyRunTrade.live_status.is_(None), StrategyRunTrade.live_status.notin_(["open_failed", "paper_cancelled"])))
        if mode == "paper":
            query = query.filter(StrategyRunTrade.paper_session_id == getattr(config, "paper_session_id", None))
        return query

    def config_to_dict(self, config):
        return {
            "paper_session_id": config.paper_session_id,
            "pending_entry_ttl_seconds": config.pending_entry_ttl_seconds,
            "risk_per_trade_percent": config.risk_per_trade_percent,
            "max_position_margin_usdt": config.max_position_margin_usdt,
            "emergency_entry_block": config.emergency_entry_block,
            "paper_taker_fee_percent": config.paper_taker_fee_percent,
            "paper_latency_ms": config.paper_latency_ms,
            "max_consecutive_losses": config.max_consecutive_losses,
            "max_leverage": config.max_leverage,
            "id": config.id,
            "exchange": config.exchange,
            "symbol": config.symbol,
            "exchange_id": config.exchange_id,
            "trading_pair_id": config.trading_pair_id,
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
            "use_median_imbalance": config.use_median_imbalance,
            "imbalance_anomaly_min": config.imbalance_anomaly_min,
            "imbalance_anomaly_max": config.imbalance_anomaly_max,
            "exclude_anomalous_imbalance": config.exclude_anomalous_imbalance,
            "entry_mode": config.entry_mode,
            "confirmation_delay_seconds": config.confirmation_delay_seconds,
            "confirmation_max_wait_seconds": config.confirmation_max_wait_seconds,
            "confirmation_require_same_direction": config.confirmation_require_same_direction,
            "confirmation_require_momentum_improvement": config.confirmation_require_momentum_improvement,
            "confirmation_min_momentum_delta": config.confirmation_min_momentum_delta,
            "confirmation_require_consensus_still_valid": config.confirmation_require_consensus_still_valid,
            "execution_mode": config.execution_mode,
            "live_enabled_confirmation": config.live_enabled_confirmation,
            "live_kill_switch": config.live_kill_switch,
            "live_max_margin_usdt": config.live_max_margin_usdt,
            "live_max_daily_loss_usdt": config.live_max_daily_loss_usdt,
            "live_max_total_loss_usdt": config.live_max_total_loss_usdt,
            "live_order_type": config.live_order_type,
            "live_reduce_only_close": config.live_reduce_only_close,
            "live_open_failed_cooldown_seconds": config.live_open_failed_cooldown_seconds,
            "live_fee_filter_enabled": config.live_fee_filter_enabled,
            "live_fee_filter_taker_fee_percent": config.live_fee_filter_taker_fee_percent,
            "momentum_confirmation_enabled": config.momentum_confirmation_enabled,
            "side_quality_filter_enabled": config.side_quality_filter_enabled,
            "side_quality_lookback_trades": config.side_quality_lookback_trades,
            "side_quality_cooldown_seconds": config.side_quality_cooldown_seconds,
            "ml_mode": config.ml_mode,
            "ml_snapshot_capture_enabled": config.ml_snapshot_capture_enabled,
            "ml_snapshot_sample_rate": config.ml_snapshot_sample_rate,
            "ml_label_horizons_seconds": self.ml_label_horizons(config),
            "ml_max_snapshots_per_hour": config.ml_max_snapshots_per_hour,
            "cooldown_after_max_recovery_seconds": config.cooldown_after_max_recovery_seconds,
            "feedback_enabled": config.feedback_enabled,
            "feedback_lookback_trades": config.feedback_lookback_trades,
            "side_loss_streak_limit": config.side_loss_streak_limit,
            "side_cooldown_seconds": config.side_cooldown_seconds,
            "min_side_win_rate": config.min_side_win_rate,
            "adaptive_consensus_boost": config.adaptive_consensus_boost,
            "adaptive_min_valid_exchanges_boost": config.adaptive_min_valid_exchanges_boost,
            "signal_diagnostics_max_rows": self.signal_diagnostics_max_rows(config),
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
            "last_manual_recovery_reset_at": state.last_manual_recovery_reset_at,
            "last_manual_margin_override_at": state.last_manual_margin_override_at,
            "last_manual_margin_override_value": state.last_manual_margin_override_value,
            "updated_at": state.updated_at,
        }

    def trade_to_dict(self, trade):
        if not trade:
            return None
        return {
            "paper_session_id": trade.paper_session_id,
            "pending_entry_expires_at": trade.pending_entry_expires_at,
            "paper_exit_status": trade.paper_exit_status,
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
            "signal_median_imbalance": trade.signal_median_imbalance,
            "signal_raw_average_imbalance": trade.signal_raw_average_imbalance,
            "signal_anomalous_exchanges_count": trade.signal_anomalous_exchanges_count,
            "signal_excluded_anomalous_imbalance_exchanges_json": trade.signal_excluded_anomalous_imbalance_exchanges_json,
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
            "median_imbalance": trade.median_imbalance,
            "raw_average_imbalance": trade.raw_average_imbalance,
            "anomalous_exchanges_count": trade.anomalous_exchanges_count,
            "excluded_anomalous_imbalance_exchanges_json": trade.excluded_anomalous_imbalance_exchanges_json,
            "average_momentum": trade.average_momentum,
            "configured_exchange_imbalance": trade.configured_exchange_imbalance,
            "configured_exchange_spread": trade.configured_exchange_spread,
            "configured_exchange_momentum": trade.configured_exchange_momentum,
            "entry_reason": trade.entry_reason,
            "entry_mode": trade.entry_mode,
            "first_signal_snapshot_json": trade.first_signal_snapshot_json,
            "confirmation_snapshot_json": trade.confirmation_snapshot_json,
            "confirmation_delay_actual_seconds": trade.confirmation_delay_actual_seconds,
            "confirmation_result": trade.confirmation_result,
            "execution_mode": trade.execution_mode,
            "live_exchange_order_id": trade.live_exchange_order_id,
            "live_close_order_id": trade.live_close_order_id,
            "live_entry_price": trade.live_entry_price,
            "live_exit_price": trade.live_exit_price,
            "live_filled_amount": trade.live_filled_amount,
            "live_entry_fee": trade.live_entry_fee,
            "live_exit_fee": trade.live_exit_fee,
            "live_status": trade.live_status,
            "live_error": trade.live_error,
            "live_raw_open_response_json": trade.live_raw_open_response_json,
            "live_raw_close_response_json": trade.live_raw_close_response_json,
            "gross_pnl": trade.gross_pnl,
            "net_pnl": trade.net_pnl,
            "total_fee": trade.total_fee,
            "exchange_tp_order_id": trade.exchange_tp_order_id,
            "exchange_sl_order_id": trade.exchange_sl_order_id,
            "exchange_tp_price": trade.exchange_tp_price,
            "exchange_sl_price": trade.exchange_sl_price,
            "tp_sl_protected": trade.tp_sl_protected,
            "tp_sl_error": trade.tp_sl_error,
            "tp_sl_created_at": trade.tp_sl_created_at,
            "protection_status": trade.protection_status,
            "protection_checked_at": trade.protection_checked_at,
            "protection_expires_at": trade.protection_expires_at,
            "legacy_reconciliation_status": trade.legacy_reconciliation_status,
            "funding_pnl": trade.funding_pnl,
            "funding_status": trade.funding_status,
            "funding_checked_at": trade.funding_checked_at,
            "exit_price_fallback_used": trade.exit_price_fallback_used,
            "exit_price_warning": trade.exit_price_warning,
            "pnl_source": trade.pnl_source,
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
