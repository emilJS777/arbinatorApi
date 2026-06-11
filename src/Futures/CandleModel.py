from datetime import datetime

from src import db
from src.__Parents.Model import Model


class Candle(Model, db.Model):
    exchange = db.Column(db.String(80), default="binance", nullable=False)
    symbol = db.Column(db.String(40), nullable=False)
    timeframe = db.Column(db.String(10), default="5m", nullable=False)
    timestamp = db.Column(db.DateTime, nullable=False)
    open = db.Column(db.Float, nullable=False)
    high = db.Column(db.Float, nullable=False)
    low = db.Column(db.Float, nullable=False)
    close = db.Column(db.Float, nullable=False)
    volume = db.Column(db.Float, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
