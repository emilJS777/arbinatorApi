# ArbiNator Backend

Backend scanner and API for the ArbiNator arbitrage dashboard.

## Stack

- Flask
- Flask-RESTful
- SQLAlchemy
- Alembic
- CCXT
- Flask-Sock

## Environment

Create a local `.env` file from `.env.example`.

```env
DB_CONNECTION_STRING=postgresql+psycopg2://postgres:<password>@localhost:5432/ArbiNatorDb
APP_HOST=0.0.0.0
APP_PORT=5555
ASYNC_DEBUG=false
LOG_LEVEL=INFO
CORS_ALLOWED_ORIGINS=*
ORDER_SCAN_INTERVAL=0.5
BALANCE_SCAN_INTERVAL=1.5
ACCOUNT_ORDERS_SCAN_INTERVAL=1.5
SCANNER_MAX_PARALLEL_TASKS=10
EXCHANGE_ERROR_COOLDOWN_SECONDS=120
```

## Run

```sh
pip install -r requirements.txt
python app.py
```

## Runtime Contract

- REST responses are returned in the shape `{ success, obj }`.
- WebSocket messages are emitted as valid JSON in the shape `{ "topic": "...", "data": {...} }`.
- Scanner loops now recover from per-cycle failures instead of crashing the whole async process.
- Scanner cadence and concurrency are now configurable through environment variables.
- Exchanges that repeatedly fail are cooled down briefly to reduce log spam and API hammering.

## Stabilization Notes

- Exchange adapters use CCXT with `enableRateLimit=True`.
- Scanner intervals are intentionally conservative for now to reduce overload risk while we refactor further.
- Next backend step should be extracting scanner orchestration, adding backoff, and moving per-exchange limits into config.
# arbinatorApi
