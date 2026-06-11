from src import db
from src.__Parents.Model import Model

class TradingPair(Model, db.Model):
    pair = db.Column(db.String(30), nullable=False)
    icon_path = db.Column(db.Text)
    order_limit = db.Column(db.Integer)
    index = db.Column(db.Integer, default=1)
    max_purchase_price = db.Column(db.Integer)
    enabled = db.Column(db.Boolean, default=False)
    exchange_id = db.Column(db.Integer, db.ForeignKey('exchange.id'))
    exchange = db.relationship('Exchange')
