from src.Arbitrage.ArbitrageConfigModel import ArbitrageConfig
from src.Arbitrage.ArbitrageOpportunityModel import ArbitrageOpportunity


def arbitrage_config_to_dict(config: ArbitrageConfig) -> dict:
    return {
        "id": config.id,
        "enabled": config.enabled,
        "symbols_allowlist": config.symbols_allowlist or [],
        "exchanges_allowlist": config.exchanges_allowlist or [],
        "min_spread_percent": config.min_spread_percent,
        "min_net_profit_percent": config.min_net_profit_percent,
        "min_profit_usdt": config.min_profit_usdt,
        "max_order_margin_usdt": config.max_order_margin_usdt,
        "max_leverage": config.max_leverage,
        "taker_fee_buffer_percent": config.taker_fee_buffer_percent,
        "slippage_buffer_percent": config.slippage_buffer_percent,
        "cooldown_seconds_per_symbol": config.cooldown_seconds_per_symbol,
        "paper_execute_enabled": config.paper_execute_enabled,
        "created_at": config.created_at,
        "updated_at": config.updated_at,
    }


def arbitrage_opportunity_to_dict(opportunity: ArbitrageOpportunity) -> dict:
    return {
        "id": opportunity.id,
        "signal_id": opportunity.signal_id,
        "symbol": opportunity.symbol,
        "buy_exchange": opportunity.buy_exchange,
        "sell_exchange": opportunity.sell_exchange,
        "buy_price": opportunity.buy_price,
        "sell_price": opportunity.sell_price,
        "amount": opportunity.amount,
        "gross_spread_percent": opportunity.gross_spread_percent,
        "net_profit_percent": opportunity.net_profit_percent,
        "expected_profit_usdt": opportunity.expected_profit_usdt,
        "total_cost_usdt": opportunity.total_cost_usdt,
        "status": opportunity.status,
        "dedupe_key": opportunity.dedupe_key,
        "config_snapshot": opportunity.config_snapshot or {},
        "created_at": opportunity.created_at,
    }
