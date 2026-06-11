from datetime import datetime

from src import db
from src.__Parents.Model import Model


class TradeSignal(Model, db.Model):
    strategy_config_id = db.Column(db.Integer, db.ForeignKey("strategy_config.id"), nullable=True)
    strategy_config = db.relationship("StrategyConfig")
    symbol = db.Column(db.String(40), nullable=False)
    exchange = db.Column(db.String(80), nullable=False)
    side = db.Column(db.String(20), nullable=False)
    entry_price = db.Column(db.Float, nullable=False)
    take_profit_price = db.Column(db.Float, nullable=True)
    stop_loss_price = db.Column(db.Float, nullable=True)
    confidence = db.Column(db.Float, nullable=True)
    reason = db.Column(db.Text, nullable=True)
    status = db.Column(db.String(20), default="created", nullable=False)
    strategy_type = db.Column(db.String(80), nullable=True)
    buy_exchange = db.Column(db.String(80), nullable=True)
    sell_exchange = db.Column(db.String(80), nullable=True)
    buy_price = db.Column(db.Float, nullable=True)
    sell_price = db.Column(db.Float, nullable=True)
    gross_spread_percent = db.Column(db.Float, nullable=True)
    net_profit_percent = db.Column(db.Float, nullable=True)
    expected_profit_usdt = db.Column(db.Float, nullable=True)
    config_snapshot = db.Column(db.JSON, nullable=True)
    dedupe_key = db.Column(db.String(180), nullable=True, index=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
