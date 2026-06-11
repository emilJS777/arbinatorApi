from datetime import datetime

from src import db
from src.__Parents.Model import Model


class ArbitrageConfig(Model, db.Model):
    enabled = db.Column(db.Boolean, default=False, nullable=False)
    symbols_allowlist = db.Column(db.JSON, nullable=True)
    exchanges_allowlist = db.Column(db.JSON, nullable=True)
    min_spread_percent = db.Column(db.Float, default=0.2, nullable=False)
    min_net_profit_percent = db.Column(db.Float, default=0.1, nullable=False)
    min_profit_usdt = db.Column(db.Float, default=1, nullable=False)
    max_order_margin_usdt = db.Column(db.Float, default=100, nullable=False)
    max_leverage = db.Column(db.Float, default=1, nullable=False)
    taker_fee_buffer_percent = db.Column(db.Float, default=0.2, nullable=False)
    slippage_buffer_percent = db.Column(db.Float, default=0.1, nullable=False)
    cooldown_seconds_per_symbol = db.Column(db.Integer, default=60, nullable=False)
    paper_execute_enabled = db.Column(db.Boolean, default=False, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
