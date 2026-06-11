from datetime import datetime

from src import db
from src.__Parents.Model import Model


class StrategyConfig(Model, db.Model):
    name = db.Column(db.String(120), nullable=False)
    strategy_type = db.Column(db.String(80), nullable=False)
    is_enabled = db.Column(db.Boolean, default=False, nullable=False)
    mode = db.Column(db.String(20), default="paper", nullable=False)
    config_json = db.Column(db.JSON, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
