from src import db
from src.Exchange.ExchangeModel import Exchange
from src.Live.MexcDiagnosticsService import MexcDiagnosticsService


class FakeResponse:
    def __init__(self, status_code=200, text="ok"):
        self.status_code = status_code
        self.text = text
        self.ok = 200 <= status_code < 300


class FakeRequests:
    def __init__(self, private_status=200):
        self.private_status = private_status
        self.calls = []

    def get(self, url, timeout=None):
        self.calls.append(("GET", url, None, timeout))
        if "api.ipify.org" in url:
            return FakeResponse(200, "203.0.113.10")
        if "contract/ping" in url:
            return FakeResponse(200, '{"success":true}')
        return FakeResponse(404, "not found")

    def request(self, method, url, headers=None, data=None, timeout=None):
        self.calls.append((method, url, headers or {}, timeout))
        if self.private_status == 403:
            return FakeResponse(403, "Access Denied")
        return FakeResponse(200, '{"success":true,"data":[{"currency":"USDT"}]}')


class FakeMexcClient:
    has = {"fetchPositions": True}

    def __init__(self, options):
        self.options = options
        self.apiKey = options["apiKey"]
        self.secret = options["secret"]

    def fetch_balance(self, params=None):
        return {"total": {"USDT": 10}, "params": params}

    def fetch_positions(self):
        return []

    def sign(self, path, api=None, method="GET", params=None):
        return {
            "url": f"https://contract.mexc.com/api/v1/private/{path}",
            "method": method,
            "body": None,
            "headers": {
                "ApiKey": self.apiKey,
                "Request-Time": "123",
                "Signature": "secret-signature-value",
                "Content-Type": "application/json",
            },
        }


class FakeCcxt:
    def __init__(self):
        self.created = []

    def mexc(self, options):
        self.created.append(options)
        return FakeMexcClient(options)


def seed_mexc():
    exchange = Exchange(title="Mexc", enabled=True, api_key="api-key-value", api_secret="secret-value", password="", index=1)
    db.session.add(exchange)
    db.session.commit()
    return exchange


def test_mexc_diagnostics_success_is_read_only_and_redacted(client):
    seed_mexc()
    fake_requests = FakeRequests()
    fake_ccxt = FakeCcxt()
    service = MexcDiagnosticsService(requests_client=fake_requests, ccxt_module=fake_ccxt)

    payload = service.run().get_json()["obj"]

    assert payload["server_ip"] == "203.0.113.10"
    assert payload["ping_ok"] is True
    assert payload["ccxt_balance_ok"] is True
    assert payload["ccxt_positions_ok"] is True
    assert payload["raw_private_account_ok"] is True
    assert payload["private_endpoint_path"] == "account/assets"
    assert "order" not in payload["private_endpoint_path"]
    assert payload["headers_names_used"] == ["ApiKey", "Content-Type", "Request-Time", "Signature"]
    serialized = str(payload)
    assert "api-key-value" not in serialized
    assert "secret-value" not in serialized
    assert "secret-signature-value" not in serialized
    assert fake_ccxt.created[0]["options"]["defaultType"] == "swap"
    assert fake_ccxt.created[0]["options"]["defaultSettle"] == "USDT"


def test_mexc_diagnostics_private_403_points_to_key_ip_waf_access(client):
    seed_mexc()
    service = MexcDiagnosticsService(requests_client=FakeRequests(private_status=403), ccxt_module=FakeCcxt())

    payload = service.run().get_json()["obj"]

    assert payload["raw_private_account_ok"] is False
    assert payload["raw_private_account_status_code"] == 403
    assert payload["raw_private_account_error"] == "Access Denied"
    assert payload["conclusion"] == "read_only_private_failed_possible_ip_waf_api_key_private_access"


def test_mexc_diagnostics_missing_credentials_returns_error(client):
    response = MexcDiagnosticsService(requests_client=FakeRequests(), ccxt_module=FakeCcxt()).run()

    assert response.status_code == 404
    assert response.get_json()["obj"]["msg"] == "mexc_credentials_not_configured"


def test_mexc_diagnostics_route_uses_safe_endpoint(client, monkeypatch):
    seed_mexc()
    fake_requests = FakeRequests()
    fake_ccxt = FakeCcxt()
    monkeypatch.setattr("src.Live.MexcDiagnosticsService.requests", fake_requests)
    monkeypatch.setattr("src.Live.MexcDiagnosticsService.ccxt", fake_ccxt)

    response = client.get("/api/live/mexc-diagnostics")
    payload = response.get_json()["obj"]

    assert response.status_code == 200
    assert payload["raw_private_account_ok"] is True
    assert any("account/assets" in url for url in payload["actual_endpoint_urls_used"])
    assert not any("order/submit" in url for url in payload["actual_endpoint_urls_used"])
