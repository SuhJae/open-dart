import asyncio
from contextlib import suppress
from typing import Optional

import httpx

from .cache.corp_code import CorpCodeCache
from .endpoints.company import CompanyAPI
from .endpoints.financials import FinanciasAPI


class OpenDartClient:
    def __init__(
            self,
            api_key: str,
            *,
            client: Optional[httpx.Client] = None,
            async_client: Optional[httpx.AsyncClient] = None,
            timeout: float = 10.0,
    ) -> None:
        self.api_key = api_key

        # shared HTTP pools
        self._client = client or httpx.Client(timeout=timeout, verify=True)
        self._async_client = async_client or httpx.AsyncClient(timeout=timeout, verify=True)

        # endpoints share those pools
        self.financials = FinanciasAPI(
            api_key,
            client=self._client,
            async_client=self._async_client,
        )
        self.company = CompanyAPI(
            api_key,
            client=self._client,
            async_client=self._async_client,
        )
        self.corp_cache = CorpCodeCache(
            api_key,
            client=self._client,
            async_client=self._async_client,
        )

    def close(self) -> None:
        """Close *both* sync and async pools (safe to call multiple times)."""
        with suppress(Exception):
            self._client.close()
        with suppress(Exception):
            # Close async pool from sync world
            try:
                asyncio.run(self._async_client.aclose())
            except RuntimeError:
                loop = asyncio.get_running_loop()
                loop.create_task(self._async_client.aclose())

    async def aclose(self) -> None:
        with suppress(Exception):
            await self._async_client.aclose()
        with suppress(Exception):
            self._client.close()

    # sync context-manager
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):  # noqa: D401
        self.close()

    # async context-manager
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):  # noqa: D401
        await self.aclose()
