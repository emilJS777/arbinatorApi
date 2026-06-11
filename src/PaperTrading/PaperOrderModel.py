from datetime import datetime

from src import db
from src.__Parents.Model import Model


class PaperOrder(Model, db.Model):
    signal_id = db.Column(db.Integer, db.ForeignKey("trade_signal.id"), nullable=True)
    signal = db.relationship("TradeSignal")
    exchange = db.Column(db.String(80), nullable=False)
    symbol = db.Column(db.String(40), nullable=False)
    side = db.Column(db.String(20), nullable=False)
    order_type = db.Column(db.String(20), default="market", nullable=False)
    price = db.Column(db.Float, nullable=False)
    amount = db.Column(db.Float, nullable=False)
    status = db.Column(db.String(20), default="created", nullable=False)
    filled_price = db.Column(db.Float, nullable=True)
    filled_amount = db.Column(db.Float, nullable=True)
    fee = db.Column(db.Float, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    filled_at = db.Column(db.DateTime, nullable=True)
