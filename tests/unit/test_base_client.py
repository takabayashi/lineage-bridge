"""Unit tests for lineage_bridge.clients.base.ConfluentClient."""

from __future__ import annotations

import base64

import httpx
import pytest
import respx

from lineage_bridge.clients.base import ConfluentClient

API_KEY = "test-api-key"
API_SECRET = "test-api-secret"
BASE_URL = "https://api.confluent.cloud"


@pytest.fixture()
def client():
    """Create a ConfluentClient with test credentials and fast retries."""
    return ConfluentClient(
        base_url=BASE_URL,
        api_key=API_KEY,
        api_secret=API_SECRET,
        timeout=5.0,
        max_retries=2,
    )


# ── test_basic_auth_header ─────────────────────────────────────────────


async def test_basic_auth_header(client):
    """Verify the Authorization header is correctly set as Basic auth."""
    expected_credentials = base64.b64encode(f"{API_KEY}:{API_SECRET}".encode()).decode()
    expected_header = f"Basic {expected_credentials}"

    assert client._client.headers["Authorization"] == expected_header


# ── test_get_success ───────────────────────────────────────────────────


@respx.mock
async def test_get_success(client):
    """A 200 response returns parsed JSON."""
    payload = {"data": [{"topic_name": "orders"}]}
    respx.get(f"{BASE_URL}/kafka/v3/topics").mock(return_value=httpx.Response(200, json=payload))

    result = await client.get("/kafka/v3/topics")
    assert result == payload


# ── test_retry_on_429 ──────────────────────────────────────────────────


@respx.mock
async def test_retry_on_429(client, monkeypatch):
    """Client retries on 429 and succeeds on the second attempt."""
    # Patch asyncio.sleep to avoid actual delay
    monkeypatch.setattr("asyncio.sleep", _no_sleep)

    route = respx.get(f"{BASE_URL}/kafka/v3/topics")
    route.side_effect = [
        httpx.Response(429, json={"error": "rate limited"}),
        httpx.Response(200, json={"data": []}),
    ]

    result = await client.get("/kafka/v3/topics")
    assert result == {"data": []}
    assert route.call_count == 2


# ── test_retry_on_500 ──────────────────────────────────────────────────


@respx.mock
async def test_retry_on_500(client, monkeypatch):
    """Client retries on 500 and succeeds on the second attempt."""
    monkeypatch.setattr("asyncio.sleep", _no_sleep)

    route = respx.get(f"{BASE_URL}/kafka/v3/topics")
    route.side_effect = [
        httpx.Response(500, json={"error": "server error"}),
        httpx.Response(200, json={"data": ["ok"]}),
    ]

    result = await client.get("/kafka/v3/topics")
    assert result == {"data": ["ok"]}
    assert route.call_count == 2


# ── test_max_retries_exceeded ──────────────────────────────────────────


@respx.mock
async def test_max_retries_exceeded(client, monkeypatch):
    """After max_retries attempts, the last exception is raised."""
    monkeypatch.setattr("asyncio.sleep", _no_sleep)

    route = respx.get(f"{BASE_URL}/kafka/v3/topics")
    # client has max_retries=2, so total attempts = 3
    route.side_effect = [
        httpx.Response(500, json={"error": "fail"}),
        httpx.Response(500, json={"error": "fail"}),
        httpx.Response(500, json={"error": "fail"}),
    ]

    with pytest.raises(httpx.HTTPStatusError):
        await client.get("/kafka/v3/topics")

    assert route.call_count == 3


# ── test_pagination ────────────────────────────────────────────────────


@respx.mock
async def test_pagination(client):
    """paginate() follows metadata.next links and collects all items."""
    page1 = {
        "data": [{"name": "item1"}, {"name": "item2"}],
        "metadata": {"next": f"{BASE_URL}/kafka/v3/topics?page_token=tok2&page_size=100"},
    }
    page2 = {
        "data": [{"name": "item3"}],
        "metadata": {"next": None},
    }

    route = respx.get(f"{BASE_URL}/kafka/v3/topics")
    route.side_effect = [
        httpx.Response(200, json=page1),
        httpx.Response(200, json=page2),
    ]

    items = await client.paginate("/kafka/v3/topics")
    assert len(items) == 3
    assert items[0]["name"] == "item1"
    assert items[2]["name"] == "item3"


# ── helpers ────────────────────────────────────────────────────────────


async def _no_sleep(seconds: float) -> None:
    """Replacement for asyncio.sleep that does nothing."""
    pass
