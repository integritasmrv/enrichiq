"""
camofox_client.py
Async HTTP client for the camofox-browser REST API.
Used as the stealth JS rendering tool in the EnrichIQ free parallel layer.
"""
import asyncio
import os
from typing import Optional

import httpx

CAMOFOX_BASE = os.getenv("CAMOFOX_URL", "http://enrichiq-camofox:9377")
CAMOFOX_API_KEY = os.getenv("CAMOFOX_API_KEY", "")
DEFAULT_TIMEOUT = 60


class CamofoxClient:
    def __init__(self, base_url: str = CAMOFOX_BASE):
        self.base = base_url
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            timeout=DEFAULT_TIMEOUT,
            headers={"Content-Type": "application/json"}
        )
        return self

    async def __aexit__(self, *_):
        if self._client:
            await self._client.aclose()

    async def inject_cookies(self, user_id: str, cookies: list[dict]) -> bool:
        resp = await self._client.post(
            f"{self.base}/sessions/{user_id}/cookies",
            headers={"Authorization": f"Bearer {CAMOFOX_API_KEY}"},
            json={"cookies": cookies}
        )
        return resp.status_code == 200

    async def fetch_snapshot(
        self,
        user_id: str,
        url: str,
        cookies: Optional[list[dict]] = None,
        wait_ms: int = 2000,
        session_key: str = "default"
    ) -> Optional[str]:
        if cookies:
            await self.inject_cookies(user_id, cookies)

        tab_resp = await self._client.post(
            f"{self.base}/tabs",
            json={"userId": user_id, "url": url, "sessionKey": session_key, "waitMs": wait_ms}
        )
        if tab_resp.status_code != 200:
            return None
        tab_id = tab_resp.json().get("tabId")

        try:
            snap_resp = await self._client.get(
                f"{self.base}/tabs/{tab_id}/snapshot",
                params={"userId": user_id}
            )
            return snap_resp.json().get("snapshot") if snap_resp.status_code == 200 else None
        finally:
            await self._client.delete(
                f"{self.base}/tabs/{tab_id}",
                params={"userId": user_id}
            )

    async def health_check(self) -> bool:
        try:
            resp = await self._client.get(f"{self.base}/health")
            return resp.status_code == 200
        except Exception:
            return False
