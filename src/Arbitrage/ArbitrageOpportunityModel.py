from datetime import datetime

from src import db
from src.__Parents.Model import Model


class ArbitrageOpportunity(Model, db.Model):
    signal_id = db.Column(db.Integer, db.ForeignKey("trade_signal.id"), nullable=True)
    signal = db.relationship("TradeSignal")
    symbol = db.Column(db.String(40), nullable=False)
    buy_exchange = db.Column(db.String(80), nullable=False)
    sell_exchange = db.Column(db.String(80), nullable=False)
    buy_price = db.Column(db.Float, nullable=False)
    sell_price = db.Column(db.Float, nullable=False)
    amount = db.Column(db.Float, nullable=False)
    gross_spread_percent = db.Column(db.Float, nullable=False)
    net_profit_percent = db.Column(db.Float, nullable=False)
    expected_profit_usdt = db.Column(db.Float, nullable=False)
    total_cost_usdt = db.Column(db.Float, nullable=False)
    status = db.Column(db.String(30), default="created", nullable=False)
    dedupe_key = db.Column(db.String(180), nullable=False, index=True)
    config_snapshot = db.Column(db.JSON, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
