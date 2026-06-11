from src.Futures.BacktestModel import BacktestRun
from src.Futures.BacktestService import BacktestService
from src.Futures.ExperimentRunnerService import ExperimentRunnerService
from src.__Parents.Controller import Controller
from src.__Parents.Response import Response


class ResearchBacktestController(Controller, Response):
    service = BacktestService()

    def get(self):
        runs = BacktestRun.query.order_by(BacktestRun.id.desc()).all()
        return self.response_ok([self.service.run_to_dict(run) for run in runs])

    def post(self):
        return self.service.run_from_payload(self.request.get_json() or {})


class ResearchBacktestItemController(Controller, Response):
    service = BacktestService()

    def get(self, backtest_id: int):
        run = BacktestRun.query.get(backtest_id)
        if not run:
            return self.response_not_found("Backtest not found")
        return self.response_ok(self.service.run_to_dict(run, include_details=True))


class ResearchMonteCarloController(Controller, Response):
    def get(self, backtest_id: int):
        run = BacktestRun.query.get(backtest_id)
        if not run:
            return self.response_not_found("Backtest not found")
        return self.response_ok(run.monte_carlo_json or {})


class ResearchWalkForwardController(Controller, Response):
    def get(self, backtest_id: int):
        run = BacktestRun.query.get(backtest_id)
        if not run:
            return self.response_not_found("Backtest not found")
        return self.response_ok(run.walk_forward_json or {})


class ResearchOptimizationController(Controller, Response):
    def get(self, backtest_id: int):
        run = BacktestRun.query.get(backtest_id)
        if not run:
            return self.response_not_found("Backtest not found")
        return self.response_ok(run.optimization_json or {})


class ResearchExperimentController(Controller, Response):
    service = ExperimentRunnerService()

    def post(self):
        return self.service.run_from_payload(self.request.get_json() or {})


class ResearchCandidateController(Controller, Response):
    service = ExperimentRunnerService()

    def get(self):
        limit = int(self.arguments.get("limit") or 50)
        return self.service.candidates(limit=limit)


class ResearchHeatmapController(Controller, Response):
    service = ExperimentRunnerService()

    def get(self):
        return self.service.heatmap_response()
