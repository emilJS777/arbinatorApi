"""Read-only compatibility check; does not run migrations or create data."""
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.update(LIVE_TRADING_ENABLED='false', LIVE_TRADING_HARD_DISABLED='true')

from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import inspect
from src import app, db
from src.OrderBookRecovery.OrderBookRecoveryModel import (
    OrderBookPatternStrategyConfig, RecoveryState, StrategyRun, StrategyRunTrade,
    ExecutionSlot, PaperSession,
)


def report():
    inspector = inspect(db.engine)
    tables = set(inspector.get_table_names())
    missing = {}
    for model in (OrderBookPatternStrategyConfig, RecoveryState, StrategyRun,
                  StrategyRunTrade, ExecutionSlot, PaperSession):
        name = model.__tablename__
        present = {item['name'] for item in inspector.get_columns(name)} if name in tables else set()
        absent = sorted(set(model.__table__.columns.keys()) - present)
        if absent:
            missing[name] = absent
    with db.engine.connect() as connection:
        current = list(MigrationContext.configure(connection).get_current_heads())
    expected = ScriptDirectory(str(ROOT / 'migrations')).get_heads()
    return {'compatible': not missing and current == expected, 'current_revisions': current,
            'expected_heads': expected, 'missing_columns': missing}


if __name__ == '__main__':
    with app.app_context():
        try:
            result = report()
            print(json.dumps(result, sort_keys=True))
            sys.exit(0 if result['compatible'] else 1)
        except Exception as error:
            # Never print a connection URL, SQL parameters or exception message.
            print(json.dumps({'compatible': False, 'error_class': type(error).__name__}))
            sys.exit(2)
