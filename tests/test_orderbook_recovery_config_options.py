from src import db
from src.Exchange.ExchangeModel import Exchange
from src.TradingPair.TradingPairModel import TradingPair


def seed_exchange(title="Mexc", enabled=True, api_secret="secret"):
    exchange = Exchange(
        title=title,
        enabled=enabled,
        api_key="api-key",
        api_secret=api_secret,
        password="password",
        index=1,
    )
    db.session.add(exchange)
    db.session.flush()
    return exchange


def seed_pair(exchange, pair="BTC/USDT", enabled=True):
    trading_pair = TradingPair(
        pair=pair,
        exchange_id=exchange.id,
        enabled=enabled,
        order_limit=10,
        index=1,
        icon_path="",
        max_purchase_price=0,
    )
    db.session.add(trading_pair)
    db.session.flush()
    return trading_pair


def test_options_endpoint_returns_active_exchanges_with_pairs(client):
    active_exchange = seed_exchange("Mexc", enabled=True)
    inactive_exchange = seed_exchange("Binance", enabled=False)
    active_pair = seed_pair(active_exchange, "BTC/USDT", enabled=True)
    seed_pair(active_exchange, "ETH/USDT", enabled=False)
    seed_pair(inactive_exchange, "BTC/USDT", enabled=True)
    db.session.commit()

    response = client.get("/api/orderbook-recovery/options")

    assert response.status_code == 200
    payload = response.get_json()["obj"]
    assert [row["title"] for row in payload["exchanges"]] == ["Mexc"]
    assert payload["exchanges"][0]["pairs"] == [
        {
            "id": active_pair.id,
            "pair": "BTC/USDT",
            "normalized_symbol": "BTCUSDT",
            "is_active": True,
        }
    ]


def test_options_endpoint_does_not_expose_secrets(client):
    exchange = seed_exchange("Mexc", enabled=True)
    seed_pair(exchange, "BTC/USDT", enabled=True)
    db.session.commit()

    response = client.get("/api/orderbook-recovery/options")

    text = response.get_data(as_text=True)
    assert "api_key" not in text
    assert "api_secret" not in text
    assert "password" not in text
    assert "secret" not in text


def test_config_update_rejects_invalid_exchange(client):
    exchange = seed_exchange("Mexc", enabled=True)
    pair = seed_pair(exchange, "BTC/USDT", enabled=True)
    db.session.commit()

    response = client.patch("/api/orderbook-recovery/config", json={
        "exchange_id": 99999,
        "trading_pair_id": pair.id,
    })

    assert response.status_code == 400
    assert response.get_json()["obj"]["msg"] == "invalid_exchange"


def test_config_update_rejects_pair_not_belonging_to_exchange(client):
    mexc = seed_exchange("Mexc", enabled=True)
    binance = seed_exchange("Binance", enabled=True)
    binance_pair = seed_pair(binance, "BTC/USDT", enabled=True)
    db.session.commit()

    response = client.patch("/api/orderbook-recovery/config", json={
        "exchange_id": mexc.id,
        "trading_pair_id": binance_pair.id,
    })

    assert response.status_code == 400
    assert response.get_json()["obj"]["msg"] == "invalid_pair_for_exchange"


def test_config_update_accepts_valid_exchange_pair_and_fills_strings(client):
    exchange = seed_exchange("Mexc", enabled=True)
    trading_pair = seed_pair(exchange, "TON/USDT", enabled=True)
    db.session.commit()

    response = client.patch("/api/orderbook-recovery/config", json={
        "exchange_id": exchange.id,
        "trading_pair_id": trading_pair.id,
        "base_margin_usdt": 12,
    })

    assert response.status_code == 200
    payload = response.get_json()["obj"]
    assert payload["exchange_id"] == exchange.id
    assert payload["trading_pair_id"] == trading_pair.id
    assert payload["exchange"] == "Mexc"
    assert payload["symbol"] == "TON/USDT"
    assert payload["base_margin_usdt"] == 12
