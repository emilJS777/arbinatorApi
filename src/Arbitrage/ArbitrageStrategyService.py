from datetime import datetime, timedelta
import json

from src import db
from src.Arbitrage.ArbitrageConfigModel import ArbitrageConfig
from src.Arbitrage.ArbitrageOpportunityModel import ArbitrageOpportunity
from src.Arbitrage.ArbitrageSerializers import arbitrage_config_to_dict, arbitrage_opportunity_to_dict
from src.Arbitrage.OrderBookSnapshotStore import OrderBookSnapshotStore
from src.PaperTrading.PaperTradingService import PaperTradingService, signal_to_dict
from src.Signal.TradeSignalModel import TradeSignal
from src.Socket.EventPublisher import EventPublisher
from src.__Parents.Response import Response


class ArbitrageStrategyService(Response):
    _last_signal_at = {}

    def __init__(self, publisher=None, paper_trading_service=None):
        self.publisher = publisher or EventPublisher()
        self.paper_trading_service = paper_trading_service or PaperTradingService(publisher=self.publisher)

    def get_or_create_config(self) -> ArbitrageConfig:
        config = ArbitrageConfig.query.order_by(ArbitrageConfig.id.asc()).first()
        if config:
            return config

        config = ArbitrageConfig(
            enabled=False,
            symbols_allowlist=[],
            exchanges_allowlist=[],
            min_spread_percent=0.2,
            min_net_profit_percent=0.1,
            min_profit_usdt=1,
            max_order_margin_usdt=100,
            max_leverage=1,
            taker_fee_buffer_percent=0.2,
            slippage_buffer_percent=0.1,
            cooldown_seconds_per_symbol=60,
            paper_execute_enabled=False,
        )
        db.session.add(config)
        db.session.commit()
        return config

    def get_config(self):
        return self.response_ok(arbitrage_config_to_dict(self.get_or_create_config()))

    def patch_config(self, body: dict):
        config = self.get_or_create_config()
        fields = [
            "enabled",
            "symbols_allowlist",
            "exchanges_allowlist",
            "min_spread_percent",
            "min_net_profit_percent",
            "min_profit_usdt",
            "max_order_margin_usdt",
            "max_leverage",
            "taker_fee_buffer_percent",
            "slippage_buffer_percent",
            "cooldown_seconds_per_symbol",
            "paper_execute_enabled",
        ]
        for field in fields:
            if field in body:
                setattr(config, field, body[field])
        db.session.commit()
        return self.response_ok(arbitrage_config_to_dict(config))

    def get_opportunities(self):
        opportunities = ArbitrageOpportunity.query.order_by(ArbitrageOpportunity.id.desc()).limit(200).all()
        return self.response_ok([arbitrage_opportunity_to_dict(opportunity) for opportunity in opportunities])

    def get_signals(self):
        signals = TradeSignal.query.filter_by(strategy_type="arbitrage").order_by(TradeSignal.id.desc()).limit(200).all()
        return self.response_ok([signal_to_dict(signal) for signal in signals])

    def run_once_response(self):
        result = self.run_once(ignore_enabled=True)
        return self.response_ok(result)

    def run_once(self, snapshots: dict | None = None, ignore_enabled: bool = False) -> dict:
        config = self.get_or_create_config()
        if not config.enabled and not ignore_enabled:
            return {"opportunities": [], "signals": [], "executed": [], "rejected": [], "skipped": ["disabled"]}

        snapshots = snapshots or OrderBookSnapshotStore.all()
        calculated = self.calculate_opportunities(snapshots, config)
        created = []
        signals = []
        executed = []
        rejected = []

        for data in calculated:
            if self._is_on_cooldown(data["dedupe_key"], config):
                continue

            opportunity, signal = self._create_opportunity_and_signal(data, config)
            created.append(arbitrage_opportunity_to_dict(opportunity))
            signals.append(signal_to_dict(signal))
            self._mark_signal(data["dedupe_key"])
            self.publisher.publish("arbitrage.opportunity.created", arbitrage_opportunity_to_dict(opportunity))
            self.publisher.publish("arbitrage.signal.created", signal_to_dict(signal))

            if not config.paper_execute_enabled:
                continue

            ok, payload = self.paper_trading_service.execute_existing_signal(
                signal=signal,
                amount=data["amount"],
                leverage=config.max_leverage,
                body={
                    "price": data["buy_price"],
                    "last_price": data["buy_price"],
                    "risk_config": {
                        "max_order_margin_usdt": config.max_order_margin_usdt,
                        "max_leverage": config.max_leverage,
                        "min_expected_roi_percent": config.min_net_profit_percent,
                        "allowed_exchanges": [data["buy_exchange"], data["sell_exchange"]],
                        "allowed_symbols": [data["symbol"]],
                    },
                },
            )
            if ok:
                opportunity.status = "paper_executed"
                db.session.commit()
                executed.append(payload)
                self.publisher.publish("arbitrage.paper.executed", payload)
            else:
                opportunity.status = "rejected"
                db.session.commit()
                rejected.append(payload)
                self.publisher.publish("arbitrage.signal.rejected", payload)

        return {"opportunities": created, "signals": signals, "executed": executed, "rejected": rejected, "skipped": []}

    def calculate_opportunities(self, snapshots: dict, config: ArbitrageConfig) -> list[dict]:
        config_dict = self._config_snapshot(config)
        exchange_allowlist = set(config.exchanges_allowlist or [])
        symbol_allowlist = set(config.symbols_allowlist or [])
        grouped = {}

        for exchange, symbols in snapshots.items():
            if exchange_allowlist and exchange not in exchange_allowlist:
                continue
            for symbol, snapshot in symbols.items():
                if symbol_allowlist and symbol not in symbol_allowlist:
                    continue
                best = self._best_prices(snapshot.get("order_book") or {})
                if not best:
                    continue
                grouped.setdefault(symbol, []).append({"exchange": exchange, **best})

        opportunities = []
        for symbol, markets in grouped.items():
            for buy_market in markets:
                for sell_market in markets:
                    if buy_market["exchange"] == sell_market["exchange"]:
                        continue
                    data = self._build_opportunity(symbol, buy_market, sell_market, config_dict)
                    if data:
                        opportunities.append(data)

        return sorted(opportunities, key=lambda item: item["expected_profit_usdt"], reverse=True)

    def _config_snapshot(self, config: ArbitrageConfig) -> dict:
        return {
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
        }

    def _best_prices(self, order_book: dict) -> dict | None:
        sales = order_book.get("sales") or []
        purchases = order_book.get("purchases") or []
        if not sales or not purchases:
            return None

        best_ask = min(sales, key=lambda item: float(item.get("price", 0) or 0))
        best_bid = max(purchases, key=lambda item: float(item.get("price", 0) or 0))
        ask_price = float(best_ask.get("price") or 0)
        bid_price = float(best_bid.get("price") or 0)
        ask_amount = float(best_ask.get("amount") or 0)
        bid_amount = float(best_bid.get("amount") or 0)
        if ask_price <= 0 or bid_price <= 0 or ask_amount <= 0 or bid_amount <= 0:
            return None

        return {
            "ask": best_ask,
            "bid": best_bid,
            "ask_price": ask_price,
            "bid_price": bid_price,
            "ask_amount": ask_amount,
            "bid_amount": bid_amount,
        }

    def _build_opportunity(self, symbol: str, buy_market: dict, sell_market: dict, config: dict) -> dict | None:
        buy_ask = buy_market["ask_price"]
        sell_bid = sell_market["bid_price"]
        if sell_bid <= buy_ask:
            return None

        gross_spread_percent = ((sell_bid - buy_ask) / buy_ask) * 100
        amount = min(
            buy_market["ask_amount"],
            sell_market["bid_amount"],
            float(config["max_order_margin_usdt"]) / buy_ask,
        )
        amount = round(amount, 8)
        if amount <= 0:
            return None

        buy_total = buy_ask * amount
        sell_total = sell_bid * amount
        actual_fee_percent = self._commission_percent(buy_market["ask"], buy_total) + self._commission_percent(sell_market["bid"], sell_total)
        buffer_percent = float(config["taker_fee_buffer_percent"]) + float(config["slippage_buffer_percent"])
        net_profit_percent = gross_spread_percent - actual_fee_percent - buffer_percent
        expected_profit_usdt = buy_total * (net_profit_percent / 100)

        if gross_spread_percent < float(config["min_spread_percent"]):
            return None
        if net_profit_percent < float(config["min_net_profit_percent"]):
            return None
        if expected_profit_usdt < float(config["min_profit_usdt"]):
            return None

        return {
            "symbol": symbol,
            "buy_exchange": buy_market["exchange"],
            "sell_exchange": sell_market["exchange"],
            "buy_price": buy_ask,
            "sell_price": sell_bid,
            "amount": amount,
            "gross_spread_percent": gross_spread_percent,
            "net_profit_percent": net_profit_percent,
            "expected_profit_usdt": expected_profit_usdt,
            "total_cost_usdt": buy_total,
            "dedupe_key": self._dedupe_key(symbol, buy_market["exchange"], sell_market["exchange"], buy_ask, sell_bid),
            "config_snapshot": config,
        }

    def _commission_percent(self, level: dict, total: float) -> float:
        commission = float(level.get("commission") or 0)
        if total <= 0:
            return 0
        return (commission / total) * 100

    def _dedupe_key(self, symbol: str, buy_exchange: str, sell_exchange: str, buy_price: float, sell_price: float) -> str:
        return f"{symbol}:{buy_exchange}:{sell_exchange}:{round(buy_price, 8)}:{round(sell_price, 8)}"

    def _is_on_cooldown(self, dedupe_key: str, config: ArbitrageConfig) -> bool:
        cooldown_seconds = int(config.cooldown_seconds_per_symbol or 0)
        if cooldown_seconds <= 0:
            return False
        last_signal_at = self._last_signal_at.get(dedupe_key)
        if last_signal_at and datetime.utcnow() - last_signal_at < timedelta(seconds=cooldown_seconds):
            return True

        since = datetime.utcnow() - timedelta(seconds=cooldown_seconds)
        return TradeSignal.query.filter(
            TradeSignal.dedupe_key == dedupe_key,
            TradeSignal.created_at >= since,
        ).first() is not None

    def _mark_signal(self, dedupe_key: str):
        self._last_signal_at[dedupe_key] = datetime.utcnow()

    def _create_opportunity_and_signal(self, data: dict, config: ArbitrageConfig):
        reason = {
            "message": "Backend arbitrage opportunity detected",
            "buy_exchange": data["buy_exchange"],
            "sell_exchange": data["sell_exchange"],
            "gross_spread_percent": data["gross_spread_percent"],
            "net_profit_percent": data["net_profit_percent"],
            "expected_profit_usdt": data["expected_profit_usdt"],
        }
        signal = TradeSignal(
            strategy_config_id=None,
            strategy_type="arbitrage",
            symbol=data["symbol"],
            exchange=data["buy_exchange"],
            side="buy",
            entry_price=data["buy_price"],
            take_profit_price=data["sell_price"],
            confidence=1,
            reason=json.dumps(reason),
            status="created",
            buy_exchange=data["buy_exchange"],
            sell_exchange=data["sell_exchange"],
            buy_price=data["buy_price"],
            sell_price=data["sell_price"],
            gross_spread_percent=data["gross_spread_percent"],
            net_profit_percent=data["net_profit_percent"],
            expected_profit_usdt=data["expected_profit_usdt"],
            config_snapshot=data["config_snapshot"],
            dedupe_key=data["dedupe_key"],
        )
        db.session.add(signal)
        db.session.flush()
        opportunity = ArbitrageOpportunity(
            signal_id=signal.id,
            symbol=data["symbol"],
            buy_exchange=data["buy_exchange"],
            sell_exchange=data["sell_exchange"],
            buy_price=data["buy_price"],
            sell_price=data["sell_price"],
            amount=data["amount"],
            gross_spread_percent=data["gross_spread_percent"],
            net_profit_percent=data["net_profit_percent"],
            expected_profit_usdt=data["expected_profit_usdt"],
            total_cost_usdt=data["total_cost_usdt"],
            status="created",
            dedupe_key=data["dedupe_key"],
            config_snapshot=data["config_snapshot"],
        )
        db.session.add(opportunity)
        db.session.commit()
        return opportunity, signal
