from src import db
from src.__Parents.Model import Model

class Exchange(Model, db.Model):
    title = db.Column(db.String(80), nullable=False)
    icon_path = db.Column(db.Text)
    enabled = db.Column(db.Boolean, default=False)
    index = db.Column(db.Integer, default=1)
    trading_pairs = db.relationship('TradingPair', back_populates='exchange', cascade='all, delete-orphan')

    api_key = db.Column(db.Text, default="")
    api_secret = db.Column(db.Text, default="")
    password = db.Column(db.Text, default="")