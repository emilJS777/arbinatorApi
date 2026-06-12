from sqlalchemy import text

from src import db
from src.__Parents.Controller import Controller


class HealthController(Controller):
    def get(self):
        return {"status": "ok"}


class ReadinessController(Controller):
    def get(self):
        db.session.execute(text("SELECT 1"))
        return {"status": "ok"}
