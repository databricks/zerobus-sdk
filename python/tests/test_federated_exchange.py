"""End-to-end tests for FederatedToken through the native bridge.

Each test calls the real ``create_stream`` with ``auth=FederatedToken(...)``.
The SDK's Unity Catalog URL points at a loopback mock token endpoint that
records every exchange request and replies from a script. The gRPC endpoint is
a refused port with recovery off: the SDK exchanges the token before it
connects, so every create reaches the token endpoint and then fails to
connect. The tests check what reached the endpoint and count supplier calls,
because caching bugs show up only in counts, never as errors.
"""

import asyncio
import base64
import json
import socket
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from zerobus import (
    FederatedToken,
    NonRetriableException,
    RecordType,
    StreamConfigurationOptions,
    TableProperties,
    ZerobusException,
    ZerobusSdk,
)
from zerobus.sdk.aio import ZerobusSdk as AsyncZerobusSdk

TABLE = "catalog.schema.orders"
OTHER_TABLE = "catalog.schema.events"
SP_CLIENT_ID = "11111111-2222-3333-4444-555555555555"
OTHER_SP_CLIENT_ID = "66666666-7777-8888-9999-000000000000"
TOKEN_EXCHANGE_GRANT = "urn:ietf:params:oauth:grant-type:token-exchange"
JWT_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:jwt"


def idp_jwt(label, n=0):
    """A realistic ~1.5 KB JWT-shaped IdP token, distinct per ``(label, n)``.

    Its ``exp`` (2100-01-01) never caps the exchanged token's TTL.
    """

    def b64(data):
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

    header = {"alg": "RS256", "kid": "test-key-1", "typ": "JWT"}
    payload = {
        "aud": "api://databricks",
        "iss": "https://login.microsoftonline.com/tenant/v2.0",
        "sub": f"{label}-{n}",
        "exp": 4102444800,
        "pad": "x" * 700,
    }
    signature = bytes((i * 7 + n) % 256 for i in range(256))
    return ".".join([b64(json.dumps(header).encode()), b64(json.dumps(payload).encode()), b64(signature)])


class MockTokenEndpoint:
    """A loopback Unity Catalog token endpoint that records every request.

    ``replies`` is a script of ``(status, body)`` pairs consumed in order. Once
    it is drained, request ``n`` gets ``200`` with ``dbx-token-<n>`` and a
    one-hour ``expires_in``.
    """

    def __init__(self):
        self.requests = []
        self.replies = []
        self._lock = threading.Lock()
        endpoint = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", 0))).decode()
                form = {k: v[0] for k, v in urllib.parse.parse_qs(body, keep_blank_values=True).items()}
                with endpoint._lock:
                    n = len(endpoint.requests)
                    endpoint.requests.append(
                        {
                            "method": self.command,
                            "path": self.path,
                            "headers": {k.lower(): v for k, v in self.headers.items()},
                            "form": form,
                        }
                    )
                    if endpoint.replies:
                        status, reply = endpoint.replies.pop(0)
                    else:
                        status, reply = 200, {"access_token": f"dbx-token-{n}", "expires_in": 3600}
                data = (json.dumps(reply) if isinstance(reply, dict) else reply).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *args):
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self._server.server_address[1]}"
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def close(self):
        self._server.shutdown()
        self._server.server_close()


class CountingSupplier:
    """A sync IdP token supplier that returns ``idp_jwt(label, n)`` on call ``n``."""

    def __init__(self, label):
        self.label = label
        self.calls = 0

    def __call__(self):
        token = idp_jwt(self.label, self.calls)
        self.calls += 1
        return token


@pytest.fixture
def endpoint():
    mock = MockTokenEndpoint()
    yield mock
    mock.close()


def refused_url():
    """The URL of a loopback port that nothing listens on."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    return f"http://127.0.0.1:{port}"


@pytest.fixture
def sdk(endpoint):
    return ZerobusSdk(host=refused_url(), unity_catalog_url=endpoint.url)


def options():
    return StreamConfigurationOptions(record_type=RecordType.JSON, recovery=False)


def create(sdk, auth, table=TABLE):
    """Creates a stream; returns the error, which is expected once the exchange is done."""
    try:
        sdk.create_stream(table_properties=TableProperties(table), auth=auth, options=options())
    except ZerobusException as exc:
        return exc
    pytest.fail("the gRPC endpoint refuses connections, so create_stream must fail")


def assert_exchange(request, subject_token, client_id):
    assert request["method"] == "POST"
    assert request["path"] == "/oidc/v1/token"
    assert request["headers"].get("content-type") == "application/x-www-form-urlencoded"
    assert "authorization" not in request["headers"], "the exchange must not carry client-credentials auth"
    form = request["form"]
    assert form.get("grant_type") == TOKEN_EXCHANGE_GRANT
    assert form.get("subject_token_type") == JWT_TOKEN_TYPE
    # Compare without printing the token if it differs.
    assert form.get("subject_token") == subject_token, (
        f"subject_token must be byte-identical to the supplier's token "
        f"(sent {len(form.get('subject_token') or '')} chars, supplier returned {len(subject_token)})"
    )
    assert form.get("client_id") == client_id, "client_id is sent only for workload identity federation"


@pytest.mark.parametrize("client_id", [None, SP_CLIENT_ID], ids=["account_level", "workload_identity"])
def test_exchange_request(sdk, endpoint, client_id):
    supplier = CountingSupplier("wire")
    create(sdk, FederatedToken(idp_token_supplier=supplier, databricks_client_id=client_id))

    assert supplier.calls == 1
    assert len(endpoint.requests) == 1
    assert_exchange(endpoint.requests[0], idp_jwt("wire"), client_id)


def test_one_token_shares_one_exchange(sdk, endpoint):
    supplier = CountingSupplier("shared")
    auth = FederatedToken(idp_token_supplier=supplier)

    for _ in range(3):
        create(sdk, auth)

    assert supplier.calls == 1, "a cache hit must not call the supplier"
    assert len(endpoint.requests) == 1, "a cache hit must not exchange again"


def test_distinct_account_level_tokens_never_share(sdk, endpoint):
    alice = CountingSupplier("alice")
    bob = CountingSupplier("bob")
    alice_auth = FederatedToken(idp_token_supplier=alice)
    bob_auth = FederatedToken(idp_token_supplier=bob)

    create(sdk, alice_auth)
    create(sdk, bob_auth)
    create(sdk, alice_auth)

    assert (alice.calls, bob.calls) == (1, 1)
    assert len(endpoint.requests) == 2, "one exchange per FederatedToken"
    assert_exchange(endpoint.requests[0], idp_jwt("alice"), None)
    assert_exchange(endpoint.requests[1], idp_jwt("bob"), None)


def test_workload_identity_is_keyed_by_client_id(sdk, endpoint):
    first = CountingSupplier("wif-a")
    second = CountingSupplier("wif-b")
    other = CountingSupplier("wif-c")

    create(sdk, FederatedToken(idp_token_supplier=first, databricks_client_id=SP_CLIENT_ID))
    create(sdk, FederatedToken(idp_token_supplier=second, databricks_client_id=SP_CLIENT_ID))
    create(sdk, FederatedToken(idp_token_supplier=other, databricks_client_id=OTHER_SP_CLIENT_ID))

    assert (first.calls, second.calls, other.calls) == (1, 0, 1)
    assert len(endpoint.requests) == 2, "one exchange per service principal"
    assert_exchange(endpoint.requests[1], idp_jwt("wif-c"), OTHER_SP_CLIENT_ID)


def test_exchanged_tokens_are_per_table(sdk, endpoint):
    supplier = CountingSupplier("tables")
    auth = FederatedToken(idp_token_supplier=supplier)

    create(sdk, auth, TABLE)
    create(sdk, auth, OTHER_TABLE)

    assert supplier.calls == 2
    assert len(endpoint.requests) == 2, "a token is scoped to one table"


def test_supplier_error_is_retryable_and_not_cached(sdk, endpoint):
    secret = idp_jwt("flaky")
    calls = []

    def supplier():
        calls.append(None)
        if len(calls) == 1:
            raise RuntimeError("IdP unavailable")
        return secret

    auth = FederatedToken(idp_token_supplier=supplier)
    err = create(sdk, auth)
    assert not isinstance(err, NonRetriableException), f"a supplier error is retryable: {err!r}"
    assert "IdP unavailable" in str(err)
    assert endpoint.requests == [], "nothing is exchanged without a token"

    create(sdk, auth)
    assert len(calls) == 2, "a failed supplier call must not be cached"
    assert_exchange(endpoint.requests[0], secret, None)


def test_non_string_token_is_permanent_and_never_sent(sdk, endpoint):
    err = create(sdk, FederatedToken(idp_token_supplier=lambda: 42))

    assert isinstance(err, NonRetriableException), f"got {err!r}"
    assert "non-string" in str(err)
    assert endpoint.requests == []


def test_empty_token_is_permanent_and_never_sent(sdk, endpoint):
    err = create(sdk, FederatedToken(idp_token_supplier=lambda: ""))

    assert isinstance(err, NonRetriableException), f"got {err!r}"
    assert endpoint.requests == [], "an empty subject_token must never be sent"


def test_refresh_calls_the_supplier_again(sdk, endpoint):
    # dbx-token-0 lives 4s; the refresh lead is min(300s, 4s/2) = 2s, so after
    # 3s the next create refreshes it.
    endpoint.replies = [(200, {"access_token": "dbx-token-0", "expires_in": 4})]
    supplier = CountingSupplier("refresh")
    auth = FederatedToken(idp_token_supplier=supplier, databricks_client_id=SP_CLIENT_ID)

    create(sdk, auth)
    time.sleep(3)
    create(sdk, auth)

    assert supplier.calls == 2, "the refresh must call the supplier again"
    assert len(endpoint.requests) == 2
    assert_exchange(endpoint.requests[1], idp_jwt("refresh", 1), SP_CLIENT_ID)


@pytest.mark.parametrize("status", [400, 401])
def test_exchange_client_error_is_permanent(sdk, endpoint, status):
    endpoint.replies = [(status, '{"error":"invalid_request","error_description":"federation policy mismatch"}')]
    supplier = CountingSupplier("policy")

    err = create(sdk, FederatedToken(idp_token_supplier=supplier))

    assert isinstance(err, NonRetriableException), f"HTTP {status}: got {err!r}"
    assert "federation policy mismatch" in str(err), "the server's reason must surface"
    assert idp_jwt("policy") not in str(err), "the error must not contain the IdP token"


def test_exchange_server_error_is_retryable(sdk, endpoint):
    endpoint.replies = [(500, '{"error":"temporarily_unavailable"}')]
    supplier = CountingSupplier("retry")

    err = create(sdk, FederatedToken(idp_token_supplier=supplier))

    assert not isinstance(err, NonRetriableException), f"HTTP 500 is retryable: {err!r}"
    assert idp_jwt("retry") not in str(err), "the error must not contain the IdP token"
    assert len(endpoint.requests) == 1


@pytest.mark.parametrize("client_id", [None, SP_CLIENT_ID], ids=["account_level", "workload_identity"])
@pytest.mark.asyncio
async def test_async_sdk_with_async_supplier(endpoint, client_id):
    sdk = AsyncZerobusSdk(host=refused_url(), unity_catalog_url=endpoint.url)
    calls = []

    async def supplier():
        calls.append(None)
        await asyncio.sleep(0)
        return idp_jwt("async")

    auth = FederatedToken(idp_token_supplier=supplier, databricks_client_id=client_id)
    for _ in range(2):
        with pytest.raises(ZerobusException):
            await sdk.create_stream(table_properties=TableProperties(TABLE), auth=auth, options=options())

    assert len(calls) == 1, "the second create must hit the cache"
    assert len(endpoint.requests) == 1
    assert_exchange(endpoint.requests[0], idp_jwt("async"), client_id)
