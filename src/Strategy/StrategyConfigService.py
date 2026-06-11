from src import db
from src.Strategy.StrategyConfigModel import StrategyConfig
from src.__Parents.Response import Response


def strategy_config_to_dict(config: StrategyConfig) -> dict:
    return {
        "id": config.id,
        "name": config.name,
        "strategy_type": config.strategy_type,
        "is_enabled": config.is_enabled,
        "mode": config.mode,
        "config_json": config.config_json or {},
        "created_at": config.created_at,
        "updated_at": config.updated_at,
    }


class StrategyConfigService(Response):
    def get_all(self):
        configs = StrategyConfig.query.order_by(StrategyConfig.id.desc()).all()
        return self.response_ok([strategy_config_to_dict(config) for config in configs])

    def create(self, body: dict):
        config = StrategyConfig(
            name=body.get("name") or "Paper strategy",
            strategy_type=body.get("strategy_type") or "manual",
            is_enabled=bool(body.get("is_enabled", False)),
            mode=body.get("mode") or "paper",
            config_json=body.get("config_json") or {},
        )
        db.session.add(config)
        db.session.commit()
        return self.response_ok(strategy_config_to_dict(config))

    def patch(self, strategy_config_id: int, body: dict):
        config = StrategyConfig.query.get(strategy_config_id)
        if not config:
            return self.response_not_found("Strategy config not found")

        for field in ["name", "strategy_type", "is_enabled", "mode", "config_json"]:
            if field in body:
                setattr(config, field, body[field])

        db.session.commit()
        return self.response_ok(strategy_config_to_dict(config))
