from __future__ import annotations

from typing import Any, Dict, List

import httpx


class MockResponse:

    def __init__(self, json_data: Dict[str, Any], status_code: int = 200) -> None:
        self._json_data = json_data
        self.status_code = status_code

    def json(self) -> Dict[str, Any]:
        return self._json_data

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise httpx.HTTPError(f"HTTP error {self.status_code}")


class MockAsyncClient:

    def __init__(self, response_json: Dict[str, Any]) -> None:
        self._response_json = response_json
        self.requests: List[Dict[str, Any]] = []

    async def get(self, url: str, params: Dict[str, Any] | None = None) -> MockResponse:
        self.requests.append({"url": url, "params": params})
        return MockResponse(self._response_json)
