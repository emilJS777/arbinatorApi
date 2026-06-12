from src import api

from src.Exchange.ExchangeController import ExchangeController
api.add_resource(ExchangeController, "/api/exchange")

from src.TradingPair.TradingPairController import TradingPairController
api.add_resource(TradingPairController, "/api/tradingPair")

from src.Order.OrderController import OrderController
api.add_resource(OrderController, "/api/order")

from src.Trade.TradeController import TradeController
api.add_resource(TradeController, "/api/trade")

from src.Strategy.StrategyConfigController import StrategyConfigController, StrategyConfigItemController
api.add_resource(StrategyConfigController, "/api/strategy-configs")
api.add_resource(StrategyConfigItemController, "/api/strategy-configs/<int:strategy_config_id>")

from src.Signal.SignalController import SignalController, PaperSignalController
api.add_resource(SignalController, "/api/signals")
api.add_resource(PaperSignalController, "/api/signals/paper")

from src.PaperTrading.PaperTradingController import PaperOrderController, PaperPositionController, PaperPositionCloseController
api.add_resource(PaperOrderController, "/api/paper-orders")
api.add_resource(PaperPositionController, "/api/paper-positions")
api.add_resource(PaperPositionCloseController, "/api/paper-positions/<int:position_id>/close")

from src.Risk.RiskController import RiskStatusController
api.add_resource(RiskStatusController, "/api/risk/status")

from src.Arbitrage.ArbitrageController import ArbitrageConfigController, ArbitrageOpportunityController, ArbitrageSignalController, ArbitrageRunOnceController
api.add_resource(ArbitrageConfigController, "/api/arbitrage/config")
api.add_resource(ArbitrageOpportunityController, "/api/arbitrage/opportunities")
api.add_resource(ArbitrageSignalController, "/api/arbitrage/signals")
api.add_resource(ArbitrageRunOnceController, "/api/arbitrage/run-once")

from src.Futures.FuturesController import FuturesMetricsController, FuturesEquityController, FuturesSignalController, FuturesPositionController, FuturesTradeController
api.add_resource(FuturesMetricsController, "/api/futures/metrics")
api.add_resource(FuturesEquityController, "/api/futures/equity")
api.add_resource(FuturesSignalController, "/api/futures/signals")
api.add_resource(FuturesPositionController, "/api/futures/positions")
api.add_resource(FuturesTradeController, "/api/futures/trades")

from src.Futures.ResearchController import ResearchBacktestController, ResearchBacktestItemController, ResearchMonteCarloController, ResearchWalkForwardController, ResearchOptimizationController, ResearchExperimentController, ResearchCandidateController, ResearchHeatmapController
api.add_resource(ResearchBacktestController, "/api/research/backtests")
api.add_resource(ResearchBacktestItemController, "/api/research/backtests/<int:backtest_id>")
api.add_resource(ResearchMonteCarloController, "/api/research/monte-carlo/<int:backtest_id>")
api.add_resource(ResearchWalkForwardController, "/api/research/walk-forward/<int:backtest_id>")
api.add_resource(ResearchOptimizationController, "/api/research/optimization/<int:backtest_id>")
api.add_resource(ResearchExperimentController, "/api/research/experiments")
api.add_resource(ResearchCandidateController, "/api/research/candidates")
api.add_resource(ResearchHeatmapController, "/api/research/heatmaps")

from src.Scanner.ScannerController import ScannerDiagnosticsController
api.add_resource(ScannerDiagnosticsController, "/api/scanner/diagnostics")

from src.OrderBookRecovery.OrderBookRecoveryController import OrderBookRecoveryConfigController, OrderBookRecoveryStartController, OrderBookRecoveryStopController, OrderBookRecoveryStateController, OrderBookRecoveryTradeController, OrderBookRecoveryMetricsController, OrderBookRecoveryDebugController, OrderBookRecoveryForwardTestController, OrderBookRecoveryForwardTestItemController, OrderBookRecoveryForwardTestMetricsController, OrderBookRecoveryManualCloseController, OrderBookRecoveryTradeArchiveController, OrderBookRecoveryArchiveAllClosedController, OrderBookRecoveryUnarchiveAllController, OrderBookRecoveryTradeDecisionDetailsController, OrderBookRecoveryTradeExportController
api.add_resource(OrderBookRecoveryConfigController, "/api/orderbook-recovery/config")
api.add_resource(OrderBookRecoveryStartController, "/api/orderbook-recovery/start-paper")
api.add_resource(OrderBookRecoveryStopController, "/api/orderbook-recovery/stop")
api.add_resource(OrderBookRecoveryStateController, "/api/orderbook-recovery/state")
api.add_resource(OrderBookRecoveryTradeController, "/api/orderbook-recovery/trades")
api.add_resource(OrderBookRecoveryMetricsController, "/api/orderbook-recovery/metrics")
api.add_resource(OrderBookRecoveryDebugController, "/api/orderbook-recovery/debug")
api.add_resource(OrderBookRecoveryForwardTestController, "/api/orderbook-recovery/run-forward-test")
api.add_resource(OrderBookRecoveryForwardTestItemController, "/api/orderbook-recovery/forward-tests/<int:run_id>")
api.add_resource(OrderBookRecoveryForwardTestMetricsController, "/api/orderbook-recovery/forward-tests/<int:run_id>/metrics")
api.add_resource(OrderBookRecoveryManualCloseController, "/api/orderbook-recovery/positions/<int:position_id>/close-manual")
api.add_resource(OrderBookRecoveryTradeArchiveController, "/api/orderbook-recovery/trades/<int:trade_id>/archive")
api.add_resource(OrderBookRecoveryArchiveAllClosedController, "/api/orderbook-recovery/trades/archive-all-closed")
api.add_resource(OrderBookRecoveryUnarchiveAllController, "/api/orderbook-recovery/trades/unarchive-all")
api.add_resource(OrderBookRecoveryTradeDecisionDetailsController, "/api/orderbook-recovery/trades/<int:trade_id>/decision-details")
api.add_resource(OrderBookRecoveryTradeExportController, "/api/orderbook-recovery/trades/export")
