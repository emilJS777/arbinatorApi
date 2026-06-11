from datetime import datetime

from src import db
from src.__Parents.Model import Model


class EquityCurvePoint(Model, db.Model):
    equity = db.Column(db.Float, nullable=False)
    realized_pnl = db.Column(db.Float, default=0, nullable=False)
    unrealized_pnl = db.Column(db.Float, default=0, nullable=False)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
