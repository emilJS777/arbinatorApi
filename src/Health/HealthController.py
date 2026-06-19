from sqlalchemy import text

from src import db
from src.__Parents.Controller import Controller


class HealthController(Controller):
    def get(self):
        return {"status": "ok"}


class ReadinessController(Controller):
    def get(self):
        try:
            db.session.execute(text("SELECT 1"))
        finally:
            db.session.remove()
        return {"status": "ok"}


class ApiHealthController(Controller):
    def get(self):
        try:
            db.session.execute(text("SELECT 1"))
        finally:
            db.session.remove()
        return {"success": True, "obj": {"status": "ok"}}
