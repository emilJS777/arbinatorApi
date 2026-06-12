from src import db
import json

from src.Exchange.ExchangeModel import Exchange
from src.Live.MexcDiagnosticsService import MexcDiagnosticsService
from src.Live.MexcOrderSubmitDryCheckService import MexcOrderSubmitDryCheckService
from src.OrderBookRecovery.LiveExecutionService import LiveExecutionService


class FakeResponse:
    def __init__(self, status_code=200, text="ok", headers=None):
        self.status_code = status_code
        self.text = text
        self.ok = 200 <= status_code < 300
        self.headers = headers or {}

    def json(self):
        return json.loads(self.text)


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


class FakeOrderSubmitRequests:
    def __init__(self, submit_status=200, submit_text='{"code":200,"data":"dry-real-order-id"}'):
        self.calls = []
        self.submit_status = submit_status
        self.submit_text = submit_text

    def request(self, method, url, headers=None, data=None, timeout=None):
        self.calls.append({
            "method": method,
            "url": url,
            "headers": headers or {},
            "data": data,
            "timeout": timeout,
        })
        return FakeResponse(self.submit_status, self.submit_text, headers={
            "Content-Type": "application/json",
            "x-cache": "AkamaiGHost",
            "akamai-request-id": "abc",
        })

    def get(self, url, params=None, timeout=None):
        self.calls.append({
            "method": "GET",
            "url": url,
            "params": params or {},
            "timeout": timeout,
        })
        if "contract/detail" in url:
            return FakeResponse(200, '{"success":true,"code":0,"data":{"symbol":"BTC_USDT","contractSize":0.001,"minVol":1,"maxVol":1000000,"volScale":0,"volUnit":1,"apiAllowed":true}}')
        return FakeResponse(404, "not found")


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

    def load_markets(self):
        return {
            "BTC/USDT:USDT": {
                "id": "BTC_USDT",
                "symbol": "BTC/USDT:USDT",
                "swap": True,
                "contract": True,
                "linear": True,
                "base": "BTC",
                "quote": "USDT",
                "settle": "USDT",
                "contractSize": 0.001,
                "type": "swap",
            }
        }

    def amount_to_precision(self, _symbol, amount):
        return amount

    def price_to_precision(self, _symbol, price):
        return price

    def sign(self, path, api=None, method="GET", params=None):
        body = None
        if method == "POST":
            import json
            body = json.dumps(params or {}, separators=(",", ":"))
        return {
            "url": f"https://contract.mexc.com/api/v1/private/{path}",
            "method": method,
            "body": body,
            "headers": {
                "ApiKey": self.apiKey,
                "Request-Time": "123",
                "Signature": f"signature-for:{body}",
                "Content-Type": "application/json",
                "source": "CCXT",
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
    assert payload["headers_names_used"] == ["ApiKey", "Content-Type", "Request-Time", "Signature", "source"]
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


def test_mexc_order_submit_drycheck_builds_expected_body_and_does_not_send(client):
    seed_mexc()
    requests_client = FakeOrderSubmitRequests()
    service = MexcOrderSubmitDryCheckService(
        live_execution_service=LiveExecutionService(client_factory=lambda _exchange: FakeMexcClient({"apiKey": "api-key-value", "secret": "secret-value"}), requests_client=requests_client)
    )

    payload = service.run({
        "symbol": "BTC/USDT",
        "margin_usdt": 1,
        "leverage": 1,
        "side": "long",
        "price": 100,
        "last_price": 65000,
    }).get_json()["obj"]

    assert payload["dry_run"] is True
    assert len(requests_client.calls) == 1
    assert requests_client.calls[0]["method"] == "GET"
    assert "contract/detail" in requests_client.calls[0]["url"]
    assert payload["endpoint"] == "https://contract.mexc.com/api/v1/private/order/submit"
    assert payload["method"] == "POST"
    assert payload["body"]["symbol"] == "BTC_USDT"
    assert payload["body"]["price"] == 100
    assert payload["body"]["vol"] == 10
    assert payload["body"]["side"] == 1
    assert payload["body"]["type"] == 5
    assert payload["body"]["openType"] == 1
    assert payload["body"]["leverage"] == 1
    assert set(payload["available_submit_formats"]) == {
        "ccxt_json",
        "json_price_omitted",
        "json_price_zero",
        "json_price_current",
        "json_integer_vol",
        "json_type6_price_omitted",
        "json_type6_price_zero",
        "json_type6_price_current",
        "json_type6_integer_vol",
        "form_urlencoded_type6",
        "json_type6_user_agent",
        "json_no_source",
        "json_user_agent",
        "form_urlencoded",
        "form_urlencoded_no_source",
        "ccxt_raw_request",
    }
    assert payload["submit_format_requests"]["ccxt_json"]["content_type"] == "application/json"
    assert "price" not in payload["submit_format_requests"]["json_price_omitted"]["body"]
    assert payload["submit_format_requests"]["json_price_zero"]["body"]["price"] == 0
    assert payload["submit_format_requests"]["json_price_current"]["body"]["price"] == 65000
    assert isinstance(payload["submit_format_requests"]["json_integer_vol"]["body"]["vol"], int)
    assert payload["submit_format_requests"]["json_integer_vol"]["serialized_body"] == '{"symbol":"BTC_USDT","vol":10,"type":5,"openType":1,"side":1,"leverage":1,"price":100}'
    assert "price" not in payload["submit_format_requests"]["json_type6_price_omitted"]["body"]
    assert payload["submit_format_requests"]["json_type6_price_omitted"]["body"]["type"] == 6
    assert payload["submit_format_requests"]["json_type6_price_zero"]["body"]["type"] == 6
    assert payload["submit_format_requests"]["json_type6_price_zero"]["body"]["price"] == 0
    assert payload["submit_format_requests"]["json_type6_price_current"]["body"]["type"] == 6
    assert payload["submit_format_requests"]["json_type6_price_current"]["body"]["price"] == 65000
    assert payload["submit_format_requests"]["json_type6_integer_vol"]["body"] == {
        "symbol": "BTC_USDT",
        "vol": 10,
        "type": 6,
        "openType": 1,
        "side": 1,
        "leverage": 1,
        "price": 100,
    }
    assert payload["submit_format_requests"]["json_type6_integer_vol"]["serialized_body"] == '{"symbol":"BTC_USDT","vol":10,"type":6,"openType":1,"side":1,"leverage":1,"price":100}'
    assert payload["submit_format_requests"]["form_urlencoded_type6"]["content_type"] == "application/x-www-form-urlencoded"
    assert "type=6" in payload["submit_format_requests"]["form_urlencoded_type6"]["serialized_body"]
    assert payload["submit_format_requests"]["json_type6_user_agent"]["body"]["type"] == 6
    assert payload["submit_format_requests"]["json_type6_user_agent"]["user_agent_used"].startswith("Mozilla/5.0")
    assert payload["submit_format_requests"]["json_no_source"]["headers_names_used"] == ["ApiKey", "Content-Type", "Request-Time", "Signature"]
    assert "User-Agent" in payload["submit_format_requests"]["json_user_agent"]["headers_names_used"]
    assert payload["submit_format_requests"]["json_user_agent"]["user_agent_used"].startswith("Mozilla/5.0")
    assert payload["submit_format_requests"]["form_urlencoded"]["content_type"] == "application/x-www-form-urlencoded"
    assert "symbol=BTC_USDT" in payload["submit_format_requests"]["form_urlencoded"]["serialized_body"]
    assert "vol=10" in payload["submit_format_requests"]["form_urlencoded"]["serialized_body"]
    assert payload["contract_detail"]["contractSize"] == 0.001
    assert payload["volume_details"]["raw_vol"] == 10
    assert payload["volume_details"]["rounded_vol"] == 10
    assert payload["volume_details"]["min_vol"] == 1
    assert payload["volume_details"]["api_allowed"] is True
    assert payload["order_type_mapping"]["mexc_type"] == 5
    assert payload["api_format_notes"]["mexc_docs_market_order_type"] == 5
    assert payload["api_format_notes"]["ccxt_mexc_swap_market_order_type"] == 6
    assert payload["side_mapping"]["mexc_side"] == 1
    assert "api-key-value" not in str(payload)
    assert "secret-value" not in str(payload)


def test_mexc_order_submit_short_side_mapping(client):
    seed_mexc()
    service = MexcOrderSubmitDryCheckService(
        live_execution_service=LiveExecutionService(client_factory=lambda _exchange: FakeMexcClient({"apiKey": "api-key-value", "secret": "secret-value"}), requests_client=FakeOrderSubmitRequests())
    )

    payload = service.run({
        "symbol": "BTC/USDT",
        "margin_usdt": 1,
        "leverage": 1,
        "side": "short",
        "price": 100,
    }).get_json()["obj"]

    assert payload["body"]["side"] == 3
    assert payload["side_mapping"]["mexc_side"] == 3


def test_mexc_order_submit_signature_payload_matches_sent_body(client):
    seed_mexc()
    requests_client = FakeOrderSubmitRequests()
    service = MexcOrderSubmitDryCheckService(
        live_execution_service=LiveExecutionService(client_factory=lambda _exchange: FakeMexcClient({"apiKey": "api-key-value", "secret": "secret-value"}), requests_client=requests_client)
    )

    payload = service.run({
        "confirm_real_order_test": True,
        "submit_format": "ccxt_json",
        "symbol": "BTC/USDT",
        "margin_usdt": 1,
        "leverage": 1,
        "side": "long",
        "price": 100,
    }).get_json()["obj"]

    sent = requests_client.calls[1]
    assert payload["confirm_real_order_test"] is True
    assert payload["real_order_sent"] is True
    assert sent["url"] == payload["endpoint"]
    assert sent["method"] == "POST"
    assert sent["data"] == payload["serialized_body"]
    assert payload["signature_payload_preview"].endswith(payload["serialized_body"])
    assert set(payload["headers_names_used"]) == {"ApiKey", "Content-Type", "Request-Time", "Signature", "source"}
    assert payload["real_order_status_code"] == 200
    assert payload["real_order_response_headers"]["akamai_headers"]["x-cache"] == "AkamaiGHost"
    assert payload["real_order_request"]["serialized_body"] == payload["serialized_body"]


def test_mexc_order_submit_real_test_reports_403_headers(client):
    seed_mexc()
    requests_client = FakeOrderSubmitRequests(submit_status=403, submit_text="Access Denied")
    service = MexcOrderSubmitDryCheckService(
        live_execution_service=LiveExecutionService(client_factory=lambda _exchange: FakeMexcClient({"apiKey": "api-key-value", "secret": "secret-value"}), requests_client=requests_client)
    )

    response = service.run({
        "confirm_real_order_test": True,
        "submit_format": "json_user_agent",
        "symbol": "BTC/USDT",
        "margin_usdt": 1,
        "leverage": 1,
        "side": "long",
        "price": 100,
    })
    payload = response.get_json()["obj"]

    assert response.status_code == 400
    assert payload["real_order_status_code"] == 403
    assert payload["real_order_response_preview"] == "Access Denied"
    assert payload["real_order_submit_format"] == "json_user_agent"
    assert "User-Agent" in payload["real_order_request"]["headers_names_used"]
    assert payload["real_order_response_headers"]["akamai_headers"]["akamai-request-id"] == "abc"


def test_mexc_order_submit_price_omitted_sends_exact_variant(client):
    seed_mexc()
    requests_client = FakeOrderSubmitRequests()
    service = MexcOrderSubmitDryCheckService(
        live_execution_service=LiveExecutionService(client_factory=lambda _exchange: FakeMexcClient({"apiKey": "api-key-value", "secret": "secret-value"}), requests_client=requests_client)
    )

    payload = service.run({
        "confirm_real_order_test": True,
        "submit_format": "json_price_omitted",
        "symbol": "BTC/USDT",
        "margin_usdt": 1,
        "leverage": 1,
        "side": "long",
        "price": 100,
    }).get_json()["obj"]

    sent = requests_client.calls[1]
    assert payload["real_order_submit_format"] == "json_price_omitted"
    assert "price" not in payload["real_order_request"]["body"]
    assert sent["data"] == payload["real_order_request"]["serialized_body"]
    assert '"price"' not in sent["data"]


def test_mexc_order_submit_form_urlencoded_signs_exact_sent_body(client):
    seed_mexc()
    requests_client = FakeOrderSubmitRequests()
    service = MexcOrderSubmitDryCheckService(
        live_execution_service=LiveExecutionService(client_factory=lambda _exchange: FakeMexcClient({"apiKey": "api-key-value", "secret": "secret-value"}), requests_client=requests_client)
    )

    payload = service.run({
        "confirm_real_order_test": True,
        "submit_format": "form_urlencoded_no_source",
        "symbol": "BTC/USDT",
        "margin_usdt": 1,
        "leverage": 1,
        "side": "long",
        "price": 100,
    }).get_json()["obj"]

    sent = requests_client.calls[1]
    assert payload["real_order_submit_format"] == "form_urlencoded_no_source"
    assert sent["headers"]["Content-Type"] == "application/x-www-form-urlencoded"
    assert "source" not in sent["headers"]
    assert sent["data"] == payload["real_order_request"]["serialized_body"]
    assert payload["real_order_request"]["signature_payload_preview"].endswith(sent["data"])
    assert "symbol=BTC_USDT" in sent["data"]
    assert "price=100" in sent["data"]


def test_mexc_order_submit_type6_variant_sends_exact_variant(client):
    seed_mexc()
    requests_client = FakeOrderSubmitRequests()
    service = MexcOrderSubmitDryCheckService(
        live_execution_service=LiveExecutionService(client_factory=lambda _exchange: FakeMexcClient({"apiKey": "api-key-value", "secret": "secret-value"}), requests_client=requests_client)
    )

    payload = service.run({
        "confirm_real_order_test": True,
        "submit_format": "json_type6_price_omitted",
        "symbol": "BTC/USDT",
        "margin_usdt": 1,
        "leverage": 1,
        "side": "long",
        "price": 100,
    }).get_json()["obj"]

    sent = requests_client.calls[1]
    assert payload["real_order_submit_format"] == "json_type6_price_omitted"
    assert payload["real_order_request"]["body"]["type"] == 6
    assert "price" not in payload["real_order_request"]["body"]
    assert sent["data"] == payload["real_order_request"]["serialized_body"]
    assert '"type":6' in sent["data"]
    assert '"price"' not in sent["data"]


def test_mexc_order_submit_type6_form_urlencoded_sends_exact_variant(client):
    seed_mexc()
    requests_client = FakeOrderSubmitRequests()
    service = MexcOrderSubmitDryCheckService(
        live_execution_service=LiveExecutionService(client_factory=lambda _exchange: FakeMexcClient({"apiKey": "api-key-value", "secret": "secret-value"}), requests_client=requests_client)
    )

    payload = service.run({
        "confirm_real_order_test": True,
        "submit_format": "form_urlencoded_type6",
        "symbol": "BTC/USDT",
        "margin_usdt": 1,
        "leverage": 1,
        "side": "long",
        "price": 100,
    }).get_json()["obj"]

    sent = requests_client.calls[1]
    assert payload["real_order_submit_format"] == "form_urlencoded_type6"
    assert sent["headers"]["Content-Type"] == "application/x-www-form-urlencoded"
    assert sent["data"] == payload["real_order_request"]["serialized_body"]
    assert "type=6" in sent["data"]
    assert payload["real_order_request"]["signature_payload_preview"].endswith(sent["data"])


def test_mexc_order_submit_drycheck_route(client, monkeypatch):
    seed_mexc()
    fake_ccxt = FakeCcxt()
    fake_requests = FakeOrderSubmitRequests()
    monkeypatch.setattr("src.OrderBookRecovery.LiveExecutionService.ccxt", fake_ccxt)
    monkeypatch.setattr("src.OrderBookRecovery.LiveExecutionService.requests", fake_requests)

    response = client.post("/api/live/mexc-order-submit-drycheck", json={
        "symbol": "BTC/USDT",
        "margin_usdt": 1,
        "leverage": 1,
        "side": "long",
        "price": 100,
    })
    payload = response.get_json()["obj"]

    assert response.status_code == 200
    assert payload["dry_run"] is True
    assert payload["body"]["type"] == 5


def test_mexc_order_submit_rejects_below_min_contract_volume(client):
    seed_mexc()
    service = MexcOrderSubmitDryCheckService(
        live_execution_service=LiveExecutionService(client_factory=lambda _exchange: FakeMexcClient({"apiKey": "api-key-value", "secret": "secret-value"}), requests_client=FakeOrderSubmitRequests())
    )

    response = service.run({
        "symbol": "BTC/USDT",
        "margin_usdt": 1,
        "leverage": 1,
        "side": "long",
        "price": 100000,
    })

    assert response.status_code == 400
    assert "live_mexc_order_below_min_vol" in response.get_json()["obj"]["msg"]
