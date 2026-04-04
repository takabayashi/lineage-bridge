"""Base HTTP client with retry logic, pagination, and authentication."""

from __future__ import annotations

import asyncio
import base64
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
_DEFAULT_TIMEOUT = 30.0
_MAX_RETRIES = 3
_BACKOFF_BASE = 1.0  # seconds


class ConfluentClient:
    """Async HTTP client for Confluent Cloud APIs.

    Handles Basic auth, retries with exponential backoff, and cursor-based pagination.
    """

    def __init__(
        self,
        base_url: str,
        api_key: str,
        api_secret: str,
        *,
        timeout: float = _DEFAULT_TIMEOUT,
        max_retries: int = _MAX_RETRIES,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries

        credentials = base64.b64encode(f"{api_key}:{api_secret}".encode()).decode()
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers={
                "Authorization": f"Basic {credentials}",
                "Content-Type": "application/json",
            },
            timeout=httpx.Timeout(timeout),
        )

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        await self._client.aclose()

    async def __aenter__(self) -> ConfluentClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> httpx.Response:
        """Send an HTTP request with retry logic.

        Retries on 429 (rate-limit) and 5xx errors using exponential backoff.
        Raises httpx.HTTPStatusError on non-retryable failures.
        """
        last_exc: Exception | None = None

        for attempt in range(self.max_retries + 1):
            try:
                logger.debug(
                    "Request %s %s params=%s attempt=%d",
                    method,
                    path,
                    params,
                    attempt + 1,
                )
                response = await self._client.request(method, path, params=params, json=json_body)

                if response.status_code not in _RETRYABLE_STATUS_CODES:
                    response.raise_for_status()
                    return response

                # Retryable status code
                logger.warning(
                    "Retryable status %d for %s %s (attempt %d/%d)",
                    response.status_code,
                    method,
                    path,
                    attempt + 1,
                    self.max_retries + 1,
                )
                last_exc = httpx.HTTPStatusError(
                    f"HTTP {response.status_code}",
                    request=response.request,
                    response=response,
                )

            except httpx.TransportError as exc:
                logger.warning(
                    "Transport error for %s %s: %s (attempt %d/%d)",
                    method,
                    path,
                    exc,
                    attempt + 1,
                    self.max_retries + 1,
                )
                last_exc = exc

            if attempt < self.max_retries:
                delay = _BACKOFF_BASE * (2**attempt)
                # Respect Retry-After header if present
                if isinstance(last_exc, httpx.HTTPStatusError) and last_exc.response is not None:
                    retry_after = last_exc.response.headers.get("Retry-After")
                    if retry_after and retry_after.isdigit():
                        delay = max(delay, float(retry_after))
                logger.debug("Sleeping %.1fs before retry", delay)
                await asyncio.sleep(delay)

        raise last_exc  # type: ignore[misc]

    async def get(self, path: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """GET request, returning the parsed JSON body."""
        response = await self._request("GET", path, params=params)
        return response.json()  # type: ignore[no-any-return]

    async def post(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """POST request, returning the parsed JSON body."""
        response = await self._request("POST", path, params=params, json_body=json_body)
        return response.json()  # type: ignore[no-any-return]

    async def delete(self, path: str, *, params: dict[str, Any] | None = None) -> None:
        """DELETE request."""
        await self._request("DELETE", path, params=params)

    async def paginate(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        data_key: str = "data",
        page_token_key: str = "page_token",
        page_size: int = 100,
    ) -> list[dict[str, Any]]:
        """Fetch all pages of a paginated endpoint.

        Confluent Cloud APIs typically return a structure like:
        {
            "data": [...],
            "metadata": {"next": "https://...?page_token=..."}
        }

        This helper follows the page_token until no more pages remain.

        Args:
            path: API endpoint path.
            params: Initial query parameters.
            data_key: JSON key containing the list of results.
            page_token_key: Query parameter name for the page token.
            page_size: Number of items per page.
        """
        all_items: list[dict[str, Any]] = []
        request_params = dict(params or {})
        request_params["page_size"] = page_size

        while True:
            response = await self.get(path, params=request_params)
            items = response.get(data_key) or []
            all_items.extend(items)

            # Check for next page
            metadata = response.get("metadata", {})
            next_url = metadata.get("next")
            if not next_url:
                break

            # Extract page token from next URL
            from urllib.parse import parse_qs, urlparse

            parsed = urlparse(next_url)
            qs = parse_qs(parsed.query)
            token_values = qs.get(page_token_key, [])
            if not token_values:
                break

            request_params[page_token_key] = token_values[0]
            logger.debug("Following pagination to page_token=%s", token_values[0])

        logger.debug("Paginated %s: fetched %d items total", path, len(all_items))
        return all_items
