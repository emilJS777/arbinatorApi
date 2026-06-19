from src.OrderBookRecovery.OrderBookRecoveryService import OrderBookRecoveryService
from src.__Parents.Controller import Controller


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
        return self.service.close_manual(position_id, self.request.get_json() or {})


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
        return self.service.ml_dataset_query("feature", self.request.args)


class OrderBookRecoveryMLFeatureSnapshotDetailController(Controller):
    service = OrderBookRecoveryService()

    def get(self, item_id: int):
        return self.service.ml_dataset_detail("feature", item_id)


class OrderBookRecoveryMLMarketSnapshotListController(Controller):
    service = OrderBookRecoveryService()

    def get(self):
        return self.service.ml_dataset_query("market", self.request.args)


class OrderBookRecoveryMLMarketSnapshotDetailController(Controller):
    service = OrderBookRecoveryService()

    def get(self, item_id: int):
        return self.service.ml_dataset_detail("market", item_id)


class OrderBookRecoveryMLPriceHistoryListController(Controller):
    service = OrderBookRecoveryService()

    def get(self):
        return self.service.ml_dataset_query("price_history", self.request.args)


class OrderBookRecoveryMLPriceHistoryDetailController(Controller):
    service = OrderBookRecoveryService()

    def get(self, item_id: int):
        return self.service.ml_dataset_detail("price_history", item_id)


class OrderBookRecoveryMLExchangeLabelListController(Controller):
    service = OrderBookRecoveryService()

    def get(self):
        return self.service.ml_dataset_query("exchange_label", self.request.args)


class OrderBookRecoveryMLExchangeLabelDetailController(Controller):
    service = OrderBookRecoveryService()

    def get(self, item_id: int):
        return self.service.ml_dataset_detail("exchange_label", item_id)
