import asyncio
from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import Any, Dict, Optional

import httpx


class BaseAPI:

    def __init__(
            self,
            api_key: str,
            *,
            client: Optional[httpx.Client] = None,
            async_client: Optional[httpx.AsyncClient] = None,
            timeout: float = 10.0,
    ) -> None:
        self.api_key = api_key
        self._owns_client = client is None
        self.client = client or httpx.Client(timeout=timeout, verify=True)

        self._owns_async_client = async_client is None
        self.async_client = async_client or httpx.AsyncClient(timeout=timeout, verify=True)

    # public helpers
    def _get(self, url: str, params: Dict[str, Any]) -> Dict:
        """Blocking GET that returns parsed JSON or raises RuntimeError."""
        params = {**params, "crtfc_key": self.api_key}
        try:
            resp = self.client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            if data.get("status") != "000":
                raise RuntimeError(f"DART error {data.get('status')}: {data.get('message')}")
            return data
        except httpx.HTTPError as exc:
            raise RuntimeError(f"HTTP error calling DART: {exc}") from exc

    async def _aget(self, url: str, params: Dict[str, Any]) -> Dict:
        """Async GET counterpart (same semantics as _get)."""
        params = {**params, "crtfc_key": self.api_key}
        try:
            resp = await self.async_client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            if data.get("status") != "000":
                raise RuntimeError(f"DART error {data.get('status')}: {data.get('message')}")
            return data
        except httpx.HTTPError as exc:
            raise RuntimeError(f"HTTP error calling DART: {exc}") from exc

    # resource-management helpers
    def close(self) -> None:
        if self._owns_client:
            self.client.close()
        if self._owns_async_client:
            # make sure we always close async session from sync code
            try:
                asyncio.run(self.async_client.aclose())
            except RuntimeError:
                # already inside a running loop – schedule close
                loop = asyncio.get_running_loop()
                loop.create_task(self.async_client.aclose())

    async def aclose(self) -> None:
        if self._owns_async_client:
            await self.async_client.aclose()
        if self._owns_client:
            self.client.close()

    # allow with ... as
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):  # noqa: D401
        self.close()

    # allow async with ... as
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):  # noqa: D401
        await self.aclose()


class BaseModel:
    def to_dict(self) -> Dict[str, Any]:
        def serialize(value: Any) -> Any:
            if isinstance(value, datetime):
                return value.strftime("%Y-%m-%d")
            if is_dataclass(value):
                return asdict(value)
            return value

        return {k: serialize(v) for k, v in asdict(self).items()}
