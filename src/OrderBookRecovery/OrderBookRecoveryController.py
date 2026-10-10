from src.OrderBookRecovery.OrderBookRecoveryService import OrderBookRecoveryService
from src.__Parents.Controller import Controller
import logging
import traceback
from uuid import uuid4
from src import db
from flask import jsonify, make_response


class OrderBookRecoveryConfigController(Controller):
    service = OrderBookRecoveryService()

    def get(self):
        return self.service.config_response()

    def patch(self):
        return self.service.update_config(self.request.get_json() or {})


class OrderBookRecoveryConfigRawController(Controller):
    service = OrderBookRecoveryService()

    def get(self):
        return self.service.config_raw_response()


class OrderBookRecoveryOptionsController(Controller):
    service = OrderBookRecoveryService()

    def get(self):
        return self.service.options_response()


class OrderBookRecoveryStartController(Controller):
    service = OrderBookRecoveryService()

    def post(self):
        return self.service.start_paper()


class OrderBookRecoveryStopController(Controller):
    service = OrderBookRecoveryService()

    def post(self):
        body = self.request.get_json() or {}
        return self.service.stop(body.get("reason") or "manual_stop")


class OrderBookRecoveryPaperSessionController(Controller):
    service = OrderBookRecoveryService()

    def post(self):
        return self.service.new_paper_session()


class OrderBookRecoveryStateController(Controller):
    service = OrderBookRecoveryService()

    def get(self):
        return self.service.state_response()


class OrderBookRecoveryTradeController(Controller):
    service = OrderBookRecoveryService()

    def get(self):
        include_archived = str(self.request.args.get("include_archived", "false")).lower() == "true"
        return self.service.trades_response(include_archived)


class OrderBookRecoveryMetricsController(Controller):
    service = OrderBookRecoveryService()

    def get(self):
        return self.service.metrics_response()


class OrderBookRecoveryDebugController(Controller):
    service = OrderBookRecoveryService()

    def get(self):
        return self.service.debug_response()


class OrderBookRecoveryDiagnosticsClearController(Controller):
    service = OrderBookRecoveryService()

    def post(self):
        return self.service.clear_signal_diagnostics()


class OrderBookRecoveryRecoveryResetController(Controller):
    service = OrderBookRecoveryService()

    def post(self):
        return self.service.reset_recovery()


class OrderBookRecoverySetCurrentMarginController(Controller):
    service = OrderBookRecoveryService()

    def post(self):
        return self.service.set_current_margin(self.request.get_json() or {})


class OrderBookRecoveryForwardTestController(Controller):
    service = OrderBookRecoveryService()

    def post(self):
        return self.service.run_forward_test(self.request.get_json() or {})


class OrderBookRecoveryForwardTestItemController(Controller):
    service = OrderBookRecoveryService()

    def get(self, run_id: int):
        return self.service.forward_test_status(run_id)


class OrderBookRecoveryForwardTestMetricsController(Controller):
    service = OrderBookRecoveryService()

    def get(self, run_id: int):
        return self.service.forward_test_metrics(run_id)


class OrderBookRecoveryManualCloseController(Controller):
    service = OrderBookRecoveryService()

    def post(self, position_id: int):
        body = self.request.get_json() or {}
        try:
            return self.service.close_manual(position_id, body)
        except Exception as error:
            db.session.rollback()
            incident_id = uuid4().hex
            # No SQL parameters, payloads, credentials or exception message in this diagnostic.
            frames = [{"file": frame.filename, "line": frame.lineno, "function": frame.name}
                      for frame in traceback.extract_tb(error.__traceback__)]
            logging.getLogger(__name__).error(
                "paper/live close failed incident_id=%s position_id=%s error_class=%s traceback=%s",
                incident_id, position_id, type(error).__name__, frames)
            return make_response(jsonify(success=False, obj={"msg": "close_failed_internal",
                "incident_id": incident_id, "position_id": position_id}), 500)


class OrderBookRecoveryTradeArchiveController(Controller):
    service = OrderBookRecoveryService()

    def post(self, trade_id: int):
        return self.service.archive_trade(trade_id, self.request.get_json() or {})


class OrderBookRecoveryTradeDeleteArchivedController(Controller):
    service = OrderBookRecoveryService()

    def post(self, trade_id: int):
        return self.service.delete_archived_trade(trade_id)


class OrderBookRecoveryDeleteAllArchivedController(Controller):
    service = OrderBookRecoveryService()

    def post(self):
        return self.service.delete_all_archived_trades()


class OrderBookRecoveryArchiveAllClosedController(Controller):
    service = OrderBookRecoveryService()

    def post(self):
        return self.service.archive_all_closed_trades(self.request.get_json() or {})


class OrderBookRecoveryUnarchiveAllController(Controller):
    service = OrderBookRecoveryService()

    def post(self):
        return self.service.unarchive_all_trades()


class OrderBookRecoveryTradeDecisionDetailsController(Controller):
    service = OrderBookRecoveryService()

    def get(self, trade_id: int):
        return self.service.decision_details(trade_id)


class OrderBookRecoveryTradeExportController(Controller):
    service = OrderBookRecoveryService()

    def get(self):
        include_archived = str(self.request.args.get("include_archived", "false")).lower() == "true"
        export_format = str(self.request.args.get("format", "csv")).lower()
        return self.service.export_trades(include_archived, export_format)


class OrderBookRecoveryMLDatasetExportController(Controller):
    service = OrderBookRecoveryService()

    def get(self):
        return self.service.export_ml_dataset_filtered("feature", self.request.args)


class OrderBookRecoveryMLStatsController(Controller):
    service = OrderBookRecoveryService()

    def get(self):
        return self.service.ml_stats_response()


class OrderBookRecoveryMLDatasetClearController(Controller):
    service = OrderBookRecoveryService()

    def post(self):
        return self.service.clear_ml_dataset()


class OrderBookRecoveryMLFeatureSnapshotExportController(Controller):
    service = OrderBookRecoveryService()

    def get(self):
        return self.service.export_ml_dataset_filtered("feature", self.request.args)


class OrderBookRecoveryMLMarketSnapshotExportController(Controller):
    service = OrderBookRecoveryService()

    def get(self):
        return self.service.export_ml_dataset_filtered("market", self.request.args)


class OrderBookRecoveryMLExchangeLabelsExportController(Controller):
    service = OrderBookRecoveryService()

    def get(self):
        return self.service.export_ml_dataset_filtered("exchange_label", self.request.args)


class OrderBookRecoveryMLPriceHistoryExportController(Controller):
    service = OrderBookRecoveryService()

    def get(self):
        return self.service.export_ml_dataset_filtered("price_history", self.request.args)


class OrderBookRecoveryMLFeatureSnapshotListController(Controller):
    service = OrderBookRecoveryService()

    def get(self):
        return self.service.get_ml_feature_snapshots(self.request.args)


class OrderBookRecoveryMLFeatureSnapshotDetailController(Controller):
    service = OrderBookRecoveryService()

    def get(self, item_id: int):
        return self.service.get_ml_feature_snapshot_detail(item_id)


class OrderBookRecoveryMLMarketSnapshotListController(Controller):
    service = OrderBookRecoveryService()

    def get(self):
        return self.service.get_ml_market_snapshots(self.request.args)


class OrderBookRecoveryMLMarketSnapshotDetailController(Controller):
    service = OrderBookRecoveryService()

    def get(self, item_id: int):
        return self.service.get_ml_market_snapshot_detail(item_id)


class OrderBookRecoveryMLPriceHistoryListController(Controller):
    service = OrderBookRecoveryService()

    def get(self):
        return self.service.get_price_history(self.request.args)


class OrderBookRecoveryMLPriceHistoryDetailController(Controller):
    service = OrderBookRecoveryService()

    def get(self, item_id: int):
        return self.service.get_price_history_detail(item_id)


class OrderBookRecoveryMLExchangeLabelListController(Controller):
    service = OrderBookRecoveryService()

    def get(self):
        return self.service.get_ml_exchange_labels(self.request.args)


class OrderBookRecoveryMLExchangeLabelDetailController(Controller):
    service = OrderBookRecoveryService()

    def get(self, item_id: int):
        return self.service.get_ml_exchange_label_detail(item_id)
