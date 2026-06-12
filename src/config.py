import logging
import os
from datetime import datetime

from flask import Flask
from flask_cors import CORS
from flask_migrate import Migrate
from flask_restful import Api
from flask_sock import Sock
from flask_sqlalchemy import SQLAlchemy

DEFAULT_DB_CONNECTION = "postgresql+psycopg2://postgres:<password>@localhost:5432/ArbiNatorDb"
DEFAULT_ALLOWED_ORIGINS = "*"
LIVE_TRADING_ENABLED = os.getenv("LIVE_TRADING_ENABLED", "false").lower() == "true"


def build_sqlalchemy_engine_options(database_uri: str):
    if not str(database_uri or "").startswith(("postgresql://", "postgresql+")):
        return {}
    return {
        "pool_size": int(os.getenv("SQLALCHEMY_POOL_SIZE", "20")),
        "max_overflow": int(os.getenv("SQLALCHEMY_MAX_OVERFLOW", "40")),
        "pool_timeout": int(os.getenv("SQLALCHEMY_POOL_TIMEOUT", "5")),
        "pool_pre_ping": os.getenv("SQLALCHEMY_POOL_PRE_PING", "true").lower() == "true",
        "pool_recycle": int(os.getenv("SQLALCHEMY_POOL_RECYCLE", "1800")),
    }

app = Flask(__name__)
api = Api(app)

# CONNECT TO DATABASE CONFIG
database_uri = os.getenv("DB_CONNECTION_STRING") or DEFAULT_DB_CONNECTION
app.config["SQLALCHEMY_DATABASE_URI"] = database_uri
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
engine_options = build_sqlalchemy_engine_options(database_uri)
if engine_options:
    app.config["SQLALCHEMY_ENGINE_OPTIONS"] = engine_options
db = SQLAlchemy(app, session_options={"expire_on_commit": False})
migrate = Migrate(app, db)


@app.teardown_request
def cleanup_db_session(_exception=None):
    db.session.remove()

# LOGGING
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, log_level, logging.INFO))
logger = logging.getLogger(f"{datetime.utcnow()}")

# Set CORS options on app configuration
allowed_origins = os.getenv("CORS_ALLOWED_ORIGINS", DEFAULT_ALLOWED_ORIGINS)
app.config['CORS_RESOURCES'] = {r"/*": {"origins": allowed_origins}}
app.config['CORS_HEADERS'] = 'Content-Type'
CORS(app, supports_credentials=True)

# Socket
sock = Sock(app)
