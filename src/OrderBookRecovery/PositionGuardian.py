"""Read-only exchange monitoring plus durable local accounting. No submit/retry methods."""
import json
from datetime import datetime, timedelta
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from src import db
from src.OrderBookRecovery.OrderBookRecoveryModel import ExecutionSlot, TradeFundingEvent, StrategyRunTrade
from src.OrderBookRecovery.LiveExecutionService import SubmissionUnknown


class PositionGuardian:
    def __init__(self, service):
        self.service = service
        self.live = service.live_execution_service

    def prepare_legacy(self, config, trade):
        if trade.execution_config_json:
            return True
        config.emergency_entry_block = True
        try:
            snapshot = json.loads(trade.decision_snapshot_json or "{}").get("config")
            required = {"exchange", "symbol", "leverage", "take_profit_percent_of_margin", "stop_loss_percent_of_margin"}
            if not isinstance(snapshot, dict) or not required.issubset(snapshot) or not trade.live_exchange_order_id:
                raise SubmissionUnknown("legacy_immutable_evidence_missing")
            if self.service.normalize_exchange(snapshot["exchange"]) != self.service.normalize_exchange(trade.exchange) or self.service.normalize_symbol(snapshot["symbol"]) != self.service.normalize_symbol(trade.symbol) or float(snapshot["leverage"]) != trade.leverage:
                raise SubmissionUnknown("legacy_immutable_evidence_conflict")
            # Only newly introduced risk/accounting defaults may come from the present config.
            frozen = dict(snapshot)
            for field in ("risk_per_trade_percent", "max_position_margin_usdt", "max_consecutive_losses", "max_leverage", "paper_taker_fee_percent", "paper_latency_ms"):
                frozen.setdefault(field, getattr(config, field))
            frozen.update(exchange=trade.exchange, symbol=trade.symbol, leverage=trade.leverage, execution_mode="live", id=config.id)
            candidate = type("LegacyEvidence", (), {"execution_config_json": json.dumps(frozen), "exchange": trade.exchange, "symbol": trade.symbol, "leverage": trade.leverage})()
            client = self.live.client(self.live.immutable_config(config, candidate))
            market = self.live.market(client, trade.symbol)
            raw = self.live.mexc_order_read(client, market, order_id=trade.live_exchange_order_id)
            if raw.get("symbol") != market["id"] or int(raw.get("side", 0)) != (1 if trade.side == "long" else 3):
                raise SubmissionUnknown("legacy_order_identity_mismatch")
            order = self.live.verified_mexc_order(market, raw)
            if abs(order["filled"] - trade.amount) > trade.amount * 1e-7 or abs(order["average"] - trade.entry_price) > trade.entry_price * 1e-7:
                raise SubmissionUnknown("legacy_fill_evidence_conflict")
            trade.execution_config_json = candidate.execution_config_json
            trade.live_entry_fee = order["fee"]["cost"]
            trade.live_filled_amount = order["filled"]
            trade.legacy_reconciliation_status = "evidence_verified"
            if not db.session.get(ExecutionSlot, config.id):
                db.session.add(ExecutionSlot(strategy_config_id=config.id, trade_id=trade.id,
                    client_order_id=f"legacy_audit_{trade.id}", status="active"))
            db.session.commit()
            return True
        except Exception as error:
            trade.legacy_reconciliation_status = str(error) if isinstance(error, SubmissionUnknown) else f"legacy_review_required:{type(error).__name__}"
            trade.live_error = trade.legacy_reconciliation_status
            db.session.commit()
            return False

    def protection(self, config, trade, now):
        if trade.protection_checked_at and now - trade.protection_checked_at < timedelta(seconds=5):
            return
        try:
            result = self.live.protection_state(config, trade, now)
            trade.protection_status = result["status"]
            trade.protection_expires_at = result.get("expires_at")
            trade.tp_sl_protected = bool(result["protected"])
            trade.tp_sl_error = None if trade.tp_sl_protected else trade.tp_sl_error or f"protection_{result['status']}"
        except Exception as error:
            trade.tp_sl_protected = False
            trade.protection_status = "unconfirmed"
            trade.tp_sl_error = f"protection_monitor_failed:{type(error).__name__}"
        trade.protection_checked_at = now
        if not trade.tp_sl_protected:
            config.emergency_entry_block = True
        db.session.commit()

    def funding(self, config, trade, now):
        if trade.execution_mode != "live":
            return
        # Serialize ledger/outcome changes across worker processes on PostgreSQL.
        trade = StrategyRunTrade.query.filter_by(id=trade.id).with_for_update().populate_existing().one()
        if trade.funding_checked_at and now - trade.funding_checked_at < timedelta(seconds=30):
            return
        prior_status = trade.funding_status
        pending_outcome = trade.pnl_source in {"exchange_order_details_excluding_funding", "verified_fills_funding_pending"}
        try:
            rows = self.live.funding_records(config, trade, now)
            for row in rows:
                exists = TradeFundingEvent.query.filter_by(trade_id=trade.id, exchange_event_id=row["id"]).first()
                if exists and abs(exists.amount_usdt - row["amount"]) > 1e-10:
                    raise SubmissionUnknown("funding_record_changed_requires_review")
                if not exists:
                    try:
                        with db.session.begin_nested():
                            db.session.add(TradeFundingEvent(trade_id=trade.id, exchange_event_id=row["id"],
                                settled_at=datetime.utcfromtimestamp(row["timestamp"] / 1000), amount_usdt=row["amount"]))
                    except IntegrityError:
                        pass
            db.session.flush()
            trade.funding_pnl = float(db.session.query(func.coalesce(func.sum(TradeFundingEvent.amount_usdt), 0)).filter(TradeFundingEvent.trade_id == trade.id).scalar())
            # Exchange settlement can be delayed; zero records at close are provisional.
            trade.funding_status = "reconciled" if trade.closed_at and now >= trade.closed_at + timedelta(seconds=30) else "pending"
            if trade.funding_status == "reconciled" and trade.live_error == "funding_not_reconciled":
                trade.live_error = None
            if trade.closed_at:
                trade.net_pnl = trade.pnl = float(trade.gross_pnl) - float(trade.total_fee) + trade.funding_pnl
                trade.result = "win" if trade.pnl > 0 else "loss" if trade.pnl < 0 else "closed"
                trade.pnl_source = "verified_fills_with_funding" if trade.funding_status == "reconciled" else "verified_fills_funding_pending"
                if trade.funding_status == "reconciled" and prior_status != "reconciled" and pending_outcome:
                    state = self.service.get_or_create_state(config)
                    self.service.apply_recovery_after_close(state, self.service.trade_config(config, trade), trade.result, trade.closed_at)
                if trade.funding_status != "reconciled":
                    config.emergency_entry_block = True
        except Exception as error:
            trade.funding_status = "unavailable"
            trade.live_error = f"funding_reconciliation_required:{type(error).__name__}"
            config.emergency_entry_block = True
        trade.funding_checked_at = now
        db.session.commit()
