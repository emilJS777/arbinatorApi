import logging
import time

import ccxt
import requests

from src.Exchange.ExchangeModel import Exchange
from src.__Parents.Response import Response


logger = logging.getLogger(__name__)


class MexcDiagnosticsService(Response):
    PUBLIC_IP_URLS = [
        "https://api.ipify.org",
        "https://ifconfig.me",
    ]
    CONTRACT_PING_URL = "https://contract.mexc.com/api/v1/contract/ping"
    RAW_PRIVATE_PATH = "account/assets"

    def __init__(self, requests_client=None, ccxt_module=None):
        self.requests = requests_client or requests
        self.ccxt = ccxt_module or ccxt

    def exchange_record(self):
        return Exchange.query.filter(Exchange.title.ilike("mexc")).order_by(Exchange.id.asc()).first()

    def client(self, exchange):
        return self.ccxt.mexc({
            "apiKey": exchange.api_key,
            "secret": exchange.api_secret,
            "password": exchange.password or None,
            "enableRateLimit": True,
            "options": {
                "defaultType": "swap",
                "defaultSettle": "USDT",
                "defaultSubType": "linear",
            },
        })

    def log_request(self, method, endpoint, status_code=None, error=None, duration_ms=None):
        logger.info(
            "MEXC futures diagnostic request endpoint=%s method=%s status_code=%s error_class=%s duration_ms=%s",
            endpoint,
            method,
            status_code,
            error.__class__.__name__ if error else None,
            duration_ms,
        )

    def preview(self, body):
        value = body if isinstance(body, str) else str(body)
        return value[:500]

    def safe_call(self, method, endpoint, func):
        start = time.time()
        try:
            result = func()
            status_code = getattr(result, "status_code", None)
            duration_ms = round((time.time() - start) * 1000, 2)
            self.log_request(method, endpoint, status_code=status_code, duration_ms=duration_ms)
            return result, None, status_code, duration_ms
        except Exception as error:
            duration_ms = round((time.time() - start) * 1000, 2)
            self.log_request(method, endpoint, error=error, duration_ms=duration_ms)
            return None, error, None, duration_ms

    def server_ip(self, urls_used):
        errors = []
        for url in self.PUBLIC_IP_URLS:
            urls_used.append(url)
            response, error, _status, _duration = self.safe_call("GET", url, lambda url=url: self.requests.get(url, timeout=5))
            if error:
                errors.append(f"{error.__class__.__name__}: {error}")
                continue
            if response and response.ok:
                return response.text.strip(), None
            errors.append(f"status={getattr(response, 'status_code', None)} body={self.preview(getattr(response, 'text', ''))}")
        return None, "; ".join(errors)

    def public_ping(self, urls_used):
        urls_used.append(self.CONTRACT_PING_URL)
        response, error, status_code, _duration = self.safe_call("GET", self.CONTRACT_PING_URL, lambda: self.requests.get(self.CONTRACT_PING_URL, timeout=5))
        if error:
            return False, status_code, f"{error.__class__.__name__}: {error}", None
        return bool(response and response.ok), status_code, None if response and response.ok else self.preview(response.text), self.preview(response.text)

    def ccxt_balance(self, client):
        response, error, _status, _duration = self.safe_call("CCXT", "mexc.fetch_balance(swap)", lambda: client.fetch_balance({"type": "swap"}))
        if error:
            return False, f"{error.__class__.__name__}: {error}", None
        return True, None, self.preview(response)

    def ccxt_positions(self, client):
        if not getattr(client, "has", {}).get("fetchPositions") and not hasattr(client, "fetch_positions"):
            return False, "fetch_positions_not_supported", None
        response, error, _status, _duration = self.safe_call("CCXT", "mexc.fetch_positions()", lambda: client.fetch_positions())
        if error:
            return False, f"{error.__class__.__name__}: {error}", None
        return True, None, self.preview(response)

    def raw_private_account(self, client, urls_used):
        signed = client.sign(self.RAW_PRIVATE_PATH, api=["contract", "private"], method="GET", params={})
        url = signed["url"]
        headers = signed.get("headers") or {}
        urls_used.append(url)
        response, error, status_code, _duration = self.safe_call(
            "GET",
            url,
            lambda: self.requests.request(
                signed.get("method") or "GET",
                url,
                headers=headers,
                data=signed.get("body"),
                timeout=10,
            ),
        )
        if error:
            return False, status_code, f"{error.__class__.__name__}: {error}", None, sorted(headers.keys())
        return bool(response and response.ok), status_code, None if response and response.ok else self.preview(response.text), self.preview(response.text), sorted(headers.keys())

    def conclusion(self, raw_ok, raw_error, balance_ok, positions_ok):
        if not raw_ok:
            return "read_only_private_failed_possible_ip_waf_api_key_private_access"
        if raw_ok and (balance_ok or positions_ok):
            return "read_only_private_ok_order_submit_issue_likely_params_signature_or_endpoint_specific"
        if raw_ok:
            return "raw_private_ok_ccxt_read_issue_needs_client_method_review"
        return raw_error or "unknown"

    def run(self):
        exchange = self.exchange_record()
        if not exchange or not exchange.api_key or not exchange.api_secret:
            return self.response(False, {"msg": "mexc_credentials_not_configured"}, 404)

        urls_used = []
        server_ip, server_ip_error = self.server_ip(urls_used)
        ping_ok, ping_status, ping_error, ping_preview = self.public_ping(urls_used)
        client = self.client(exchange)
        balance_ok, balance_error, balance_preview = self.ccxt_balance(client)
        positions_ok, positions_error, positions_preview = self.ccxt_positions(client)
        raw_ok, raw_status, raw_error, raw_preview, header_names = self.raw_private_account(client, urls_used)

        return self.response_ok({
            "server_ip": server_ip,
            "server_ip_error": server_ip_error,
            "ping_ok": ping_ok,
            "ping_status_code": ping_status,
            "ping_error": ping_error,
            "ping_response_preview": ping_preview,
            "ccxt_balance_ok": balance_ok,
            "ccxt_balance_error": balance_error,
            "ccxt_balance_preview": balance_preview,
            "ccxt_positions_ok": positions_ok,
            "ccxt_positions_error": positions_error,
            "ccxt_positions_preview": positions_preview,
            "raw_private_account_ok": raw_ok,
            "raw_private_account_status_code": raw_status,
            "raw_private_account_error": raw_error,
            "raw_private_account_response_preview": raw_preview,
            "actual_endpoint_urls_used": urls_used,
            "headers_names_used": header_names,
            "private_endpoint_path": self.RAW_PRIVATE_PATH,
            "conclusion": self.conclusion(raw_ok, raw_error, balance_ok, positions_ok),
        })
