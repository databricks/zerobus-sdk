"""get_token: a standalone Zerobus OAuth token, against a local mock token endpoint."""

import asyncio
import json
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs

from zerobus import NonRetriableException, OAuthToken, ZerobusException, get_token
from zerobus.sdk import aio

TABLE = "main.default.events"
WORKSPACE_ID = "1234567890"
SECRET_TOKEN = "secret-token-value"


class _TokenEndpoint(BaseHTTPRequestHandler):
    status = 200
    body = {"access_token": SECRET_TOKEN, "expires_in": 3600}
    requests = []

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        form = parse_qs(self.rfile.read(length).decode())
        type(self).requests.append((self.path, form))
        payload = json.dumps(type(self).body).encode()
        self.send_response(type(self).status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):
        pass


class TestGetToken(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), _TokenEndpoint)
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()
        cls.url = f"http://127.0.0.1:{cls.server.server_address[1]}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self):
        _TokenEndpoint.status = 200
        _TokenEndpoint.body = {"access_token": SECRET_TOKEN, "expires_in": 3600}
        _TokenEndpoint.requests = []

    def _get(self, url=None, table=TABLE):
        return get_token(url or self.url, table, "client-id", "client-secret", WORKSPACE_ID)

    def test_returns_token_and_expiry(self):
        before = datetime.now(timezone.utc)
        token = self._get()
        after = datetime.now(timezone.utc)

        self.assertIsInstance(token, OAuthToken)
        self.assertEqual(token.access_token, SECRET_TOKEN)
        self.assertEqual(token.expires_at.tzinfo, timezone.utc)
        slack = timedelta(milliseconds=1)
        self.assertGreaterEqual(token.expires_at, before + timedelta(hours=1) - slack)
        self.assertLessEqual(token.expires_at, after + timedelta(hours=1))

        path, form = _TokenEndpoint.requests[0]
        self.assertEqual(path, "/oidc/v1/token")
        self.assertEqual(form["grant_type"], ["client_credentials"])
        self.assertIn(WORKSPACE_ID, form["resource"][0])
        self.assertIn(TABLE, form["authorization_details"][0])

    def test_missing_expires_in_yields_no_expiry(self):
        _TokenEndpoint.body = {"access_token": SECRET_TOKEN}
        self.assertIsNone(self._get().expires_at)

    def test_every_call_mints(self):
        self._get()
        self._get()
        self.assertEqual(len(_TokenEndpoint.requests), 2)

    def test_repr_redacts_access_token(self):
        self.assertNotIn(SECRET_TOKEN, repr(self._get()))

    def test_rejected_request_is_non_retriable(self):
        _TokenEndpoint.status = 401
        _TokenEndpoint.body = {"error": "invalid_client"}
        with self.assertRaises(NonRetriableException):
            self._get()

    def test_server_error_is_retryable(self):
        _TokenEndpoint.status = 503
        _TokenEndpoint.body = {"error": "unavailable"}
        with self.assertRaises(ZerobusException) as ctx:
            self._get()
        self.assertNotIsInstance(ctx.exception, NonRetriableException)

    def test_invalid_table_name_fails_without_a_request(self):
        with self.assertRaises(NonRetriableException):
            self._get(table="not-a-table")
        self.assertEqual(_TokenEndpoint.requests, [])

    def test_sync_call_releases_the_gil(self):
        ticks = []
        stop = threading.Event()

        def tick():
            while not stop.is_set():
                ticks.append(1)
                time.sleep(0.001)

        original = _TokenEndpoint.do_POST

        def slow_post(handler):
            time.sleep(0.2)
            original(handler)

        _TokenEndpoint.do_POST = slow_post
        ticker = threading.Thread(target=tick)
        ticker.start()
        try:
            self._get()
        finally:
            stop.set()
            ticker.join()
            _TokenEndpoint.do_POST = original
        self.assertGreater(len(ticks), 10)

    def test_async_returns_token_and_expiry(self):
        token = asyncio.run(aio.get_token(self.url, TABLE, "client-id", "client-secret", WORKSPACE_ID))
        self.assertEqual(token.access_token, SECRET_TOKEN)
        self.assertIsNotNone(token.expires_at)

    def test_async_rejected_request_is_non_retriable(self):
        _TokenEndpoint.status = 403
        _TokenEndpoint.body = {"error": "forbidden"}
        with self.assertRaises(NonRetriableException):
            asyncio.run(aio.get_token(self.url, TABLE, "client-id", "client-secret", WORKSPACE_ID))


if __name__ == "__main__":
    unittest.main()
