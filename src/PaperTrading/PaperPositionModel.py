from datetime import datetime

from src import db
from src.__Parents.Model import Model


class PaperPosition(Model, db.Model):
    strategy_config_id = db.Column(db.Integer, db.ForeignKey("strategy_config.id"), nullable=True)
    strategy_config = db.relationship("StrategyConfig")
    exchange = db.Column(db.String(80), nullable=False)
    symbol = db.Column(db.String(40), nullable=False)
    side = db.Column(db.String(20), nullable=False)
    entry_price = db.Column(db.Float, nullable=False)
    amount = db.Column(db.Float, nullable=False)
    leverage = db.Column(db.Float, default=1, nullable=False)
    margin = db.Column(db.Float, default=0, nullable=False)
    margin_mode = db.Column(db.String(20), default="isolated", nullable=True)
    liquidation_price = db.Column(db.Float, nullable=True)
    take_profit_price = db.Column(db.Float, nullable=True)
    stop_loss_price = db.Column(db.Float, nullable=True)
    exit_price = db.Column(db.Float, nullable=True)
    exit_reason = db.Column(db.String(40), nullable=True)
    strategy_type = db.Column(db.String(80), nullable=True)
    unrealized_pnl = db.Column(db.Float, default=0, nullable=False)
    realized_pnl = db.Column(db.Float, default=0, nullable=False)
    status = db.Column(db.String(20), default="open", nullable=False)
    opened_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    closed_at = db.Column(db.DateTime, nullable=True)
