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

app = Flask(__name__)
api = Api(app)

# CONNECT TO DATABASE CONFIG
app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DB_CONNECTION_STRING") or DEFAULT_DB_CONNECTION
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)
migrate = Migrate(app, db)

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
