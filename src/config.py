import logging
import os
import time
from datetime import datetime

from flask import Flask, g, request
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
    if _exception is not None:
        logger.error(
            "request failed path=%s method=%s",
            request.path,
            request.method,
            exc_info=(_exception.__class__, _exception, _exception.__traceback__),
        )
    db.session.remove()

# LOGGING
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, log_level, logging.INFO))
logger = logging.getLogger(f"{datetime.utcnow()}")
request_timing_logger = logging.getLogger("arbinator.request_timing")
TIMED_REQUEST_PATHS = {
    "/api/orderbook-recovery/state",
    "/api/orderbook-recovery/metrics",
    "/api/orderbook-recovery/debug",
    "/api/orderbook-recovery/trades",
    "/api/orderbook-recovery/trades/export",
    "/api/orderbook-recovery/ml/stats",
    "/api/orderbook-recovery/ml/dataset/clear",
    "/api/orderbook-recovery/ml/dataset/export",
    "/api/orderbook-recovery/ml/feature-snapshots",
    "/api/orderbook-recovery/ml/feature-snapshots/export",
    "/api/orderbook-recovery/ml/market-snapshots",
    "/api/orderbook-recovery/ml/market-snapshots/export",
    "/api/orderbook-recovery/ml/price-history",
    "/api/orderbook-recovery/ml/price-history/export",
    "/api/orderbook-recovery/ml/exchange-labels",
    "/api/orderbook-recovery/ml/exchange-labels/export",
    "/api/scanner/diagnostics",
}
TIMED_REQUEST_PREFIXES = (
    "/api/orderbook-recovery/ml/feature-snapshots/",
    "/api/orderbook-recovery/ml/market-snapshots/",
    "/api/orderbook-recovery/ml/price-history/",
    "/api/orderbook-recovery/ml/exchange-labels/",
)
SLOW_REQUEST_WARNING_MS = int(os.getenv("SLOW_REQUEST_WARNING_MS", "1000"))


@app.before_request
def mark_timed_request_start():
    if request.path in TIMED_REQUEST_PATHS or request.path.startswith(TIMED_REQUEST_PREFIXES):
        g.request_started_at = time.perf_counter()


@app.after_request
def log_timed_request(response):
    started_at = getattr(g, "request_started_at", None)
    if started_at is not None:
        duration_ms = (time.perf_counter() - started_at) * 1000
        log = request_timing_logger.warning if duration_ms >= SLOW_REQUEST_WARNING_MS else request_timing_logger.info
        log(
            "request_timing path=%s method=%s status=%s duration_ms=%.2f",
            request.path,
            request.method,
            response.status_code,
            duration_ms,
        )
    return response

# Set CORS options on app configuration
allowed_origins = os.getenv("CORS_ALLOWED_ORIGINS", DEFAULT_ALLOWED_ORIGINS)
app.config['CORS_RESOURCES'] = {r"/*": {"origins": allowed_origins}}
app.config['CORS_HEADERS'] = 'Content-Type'
CORS(app, supports_credentials=True)

# Socket
sock = Sock(app)
