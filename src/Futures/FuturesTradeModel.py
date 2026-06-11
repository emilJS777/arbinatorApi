from datetime import datetime

from src import db
from src.__Parents.Model import Model


class FuturesTrade(Model, db.Model):
    position_id = db.Column(db.Integer, db.ForeignKey("paper_position.id"), nullable=True)
    signal_id = db.Column(db.Integer, db.ForeignKey("trade_signal.id"), nullable=True)
    symbol = db.Column(db.String(40), nullable=False)
    side = db.Column(db.String(20), nullable=False)
    entry_price = db.Column(db.Float, nullable=False)
    exit_price = db.Column(db.Float, nullable=False)
    amount = db.Column(db.Float, nullable=False)
    leverage = db.Column(db.Float, default=2, nullable=False)
    margin = db.Column(db.Float, default=0, nullable=False)
    realized_pnl = db.Column(db.Float, default=0, nullable=False)
    fee = db.Column(db.Float, default=0, nullable=False)
    exit_reason = db.Column(db.String(40), nullable=True)
    opened_at = db.Column(db.DateTime, nullable=True)
    closed_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
