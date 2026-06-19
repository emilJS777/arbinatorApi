import os


class MLPredictionService:
    def __init__(self, model_path=None):
        self.model_path = model_path or os.environ.get(
            "ORDERBOOK_RECOVERY_ML_MODEL_PATH",
            "models/orderbook_recovery_quality.json",
        )

    def predict(self, features: dict):
        if not self.model_path or not os.path.exists(self.model_path):
            return {
                "ml_score": None,
                "ml_decision": None,
                "ml_reason": "model_file_not_found",
                "ml_model_version": None,
            }
        return {
            "ml_score": None,
            "ml_decision": None,
            "ml_reason": "model_not_loaded_shadow_only",
            "ml_model_version": None,
        }
