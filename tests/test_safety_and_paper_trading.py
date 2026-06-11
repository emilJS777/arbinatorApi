from src import db
from src.Exchange.ExchangeModel import Exchange
from src.PaperTrading.PaperOrderModel import PaperOrder
from src.PaperTrading.PaperPositionModel import PaperPosition


def test_live_order_blocked_when_live_trading_disabled(client):
    response = client.post("/api/order", json={
        "exchange_id": 1,
        "pair": "BTC/USDT",
        "amount": 1,
        "price": 100,
        "type": "buy",
    })

    assert response.status_code == 403
    assert response.get_json()["obj"]["msg"] == "Live trading is disabled. Use paper trading mode."


def test_paper_signal_creates_paper_order(client):
    response = client.post("/api/signals/paper", json={
        "exchange": "binance",
        "symbol": "BTC/USDT",
        "side": "buy",
        "entry_price": 100,
        "take_profit_price": 102,
        "amount": 0.5,
        "leverage": 1,
    })

    assert response.status_code == 200
    assert response.get_json()["success"] is True
    assert PaperOrder.query.count() == 1
    assert PaperOrder.query.first().status == "filled"


def test_paper_order_creates_paper_position(client):
    client.post("/api/signals/paper", json={
        "exchange": "binance",
        "symbol": "ETH/USDT",
        "side": "long",
        "entry_price": 50,
        "take_profit_price": 51,
        "amount": 1,
        "leverage": 1,
    })

    assert PaperPosition.query.count() == 1
    position = PaperPosition.query.first()
    assert position.status == "open"
    assert position.symbol == "ETH/USDT"


def test_risk_manager_rejects_oversized_order(client):
    response = client.post("/api/signals/paper", json={
        "exchange": "binance",
        "symbol": "BTC/USDT",
        "side": "buy",
        "entry_price": 1000,
        "take_profit_price": 1010,
        "amount": 1,
        "leverage": 1,
        "risk_config": {
            "max_order_margin_usdt": 10,
        },
    })

    assert response.status_code == 403
    assert response.get_json()["success"] is False
    assert PaperOrder.query.count() == 0


def test_exchange_secrets_are_not_returned(client):
    exchange = Exchange(
        title="binance",
        icon_path="",
        enabled=True,
        index=1,
        api_key="public-key",
        api_secret="super-secret",
        password="private-password",
    )
    db.session.add(exchange)
    db.session.commit()

    response = client.get("/api/exchange")
    payload = response.get_json()["obj"][0]

    assert response.status_code == 200
    assert "api_secret" not in payload
    assert "password" not in payload
    assert payload["has_secret"] is True
    assert payload["has_password"] is True
    assert payload["api_key"] != "public-key"
