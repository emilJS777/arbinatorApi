import os
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ["DB_CONNECTION_STRING"] = "sqlite:///:memory:"
os.environ["LIVE_TRADING_ENABLED"] = "false"

from src import app, db
from src.Arbitrage.ArbitrageConfigModel import ArbitrageConfig
from src.Arbitrage.ArbitrageOpportunityModel import ArbitrageOpportunity
from src.Arbitrage.ArbitrageStrategyService import ArbitrageStrategyService
from src.Arbitrage.OrderBookSnapshotStore import OrderBookSnapshotStore
from src.Exchange.ExchangeModel import Exchange
from src.Futures.BacktestModel import BacktestRun, BacktestTrade, StrategyCandidate
from src.Futures.CandleModel import Candle
from src.Futures.EquityCurveModel import EquityCurvePoint
from src.Futures.FuturesTradeModel import FuturesTrade
from src.OrderBookRecovery.OrderBookRecoveryModel import OrderBookPatternStrategyConfig, RecoveryState, StrategyRun, StrategyRunTrade
from src.OrderBookRecovery.OrderBookRecoveryService import OrderBookRecoveryService
from src.PaperTrading.PaperOrderModel import PaperOrder
from src.PaperTrading.PaperPositionModel import PaperPosition
from src.Signal.TradeSignalModel import TradeSignal
from src.Scanner.ScannerService import ScannerService
from src.Strategy.StrategyConfigModel import StrategyConfig
from src.TradingPair.TradingPairModel import TradingPair


@pytest.fixture()
def client():
    app.config["TESTING"] = True
    with app.app_context():
        db.drop_all()
        db.create_all()
        OrderBookSnapshotStore.clear()
        ArbitrageStrategyService._last_signal_at = {}
        OrderBookRecoveryService._last_evaluations = {}
        OrderBookRecoveryService._last_hook_seen_at = None
        OrderBookRecoveryService._last_hook_snapshot = None
        OrderBookRecoveryService._last_matching_hooks = {}
        OrderBookRecoveryService._last_mismatch_hooks = {}
        OrderBookRecoveryService._live_market_infos = {}
        OrderBookRecoveryService._signal_diagnostics.clear()
        OrderBookRecoveryService._signal_counters.clear()
        OrderBookRecoveryService._mid_price_history.clear()
        ScannerService._diagnostics = {}
        yield app.test_client()
        db.session.remove()
        OrderBookSnapshotStore.clear()
        ArbitrageStrategyService._last_signal_at = {}
        OrderBookRecoveryService._last_evaluations = {}
        OrderBookRecoveryService._last_hook_seen_at = None
        OrderBookRecoveryService._last_hook_snapshot = None
        OrderBookRecoveryService._last_matching_hooks = {}
        OrderBookRecoveryService._last_mismatch_hooks = {}
        OrderBookRecoveryService._live_market_infos = {}
        OrderBookRecoveryService._signal_diagnostics.clear()
        OrderBookRecoveryService._signal_counters.clear()
        OrderBookRecoveryService._mid_price_history.clear()
        ScannerService._diagnostics = {}
        db.drop_all()
