import asyncio
import os
import xml.etree.ElementTree as ET
import zipfile
from contextlib import suppress
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Optional

import httpx


class CorpCodeCache:
    """
    Local one-day cache for `corpCode.xml`

    * Works with either an injected `httpx.Client` / `AsyncClient`
      **or** creates its own.
    * Provides `close()` / `aclose()` so the owner (OpenDartClient)
      can shut pools down cleanly.
    """

    API_URL = "https://opendart.fss.or.kr/api/corpCode.xml"
    CACHE_DIR = Path.home() / ".openDart"
    CACHE_FILE = CACHE_DIR / "corp_codes.xml"
    LAST_UPDATED = CACHE_DIR / "last_updated.txt"

    # ------------------------------------------------------------------ #
    # Construction / resource management                                 #
    # ------------------------------------------------------------------ #
    def __init__(
            self,
            api_key: str,
            *,
            client: Optional[httpx.Client] = None,
            async_client: Optional[httpx.AsyncClient] = None,
            timeout: float = 10.0,
    ) -> None:
        self.api_key = api_key

        # shared pools (may be owned by someone else)
        self._client = client or httpx.Client(timeout=timeout, verify=True)
        self._async_client = async_client or httpx.AsyncClient(timeout=timeout, verify=True)
        self._owns_client = client is None
        self._owns_async_client = async_client is None

        self.corp_dict: Dict[str, Dict[str, str]] = {}
        self._ensure_cache()

    def close(self) -> None:
        if self._owns_client:
            with suppress(Exception):
                self._client.close()
        if self._owns_async_client:
            # close async pool from sync context
            try:
                asyncio.run(self._async_client.aclose())
            except RuntimeError:
                loop = asyncio.get_running_loop()
                loop.create_task(self._async_client.aclose())

    async def aclose(self) -> None:
        if self._owns_async_client:
            with suppress(Exception):
                await self._async_client.aclose()
        if self._owns_client:
            with suppress(Exception):
                self._client.close()

    # ------------------------------------------------------------------ #
    # Sync workflow (unchanged externally)                               #
    # ------------------------------------------------------------------ #
    def _ensure_cache(self) -> None:
        self.CACHE_DIR.mkdir(parents=True, exist_ok=True)

        needs_update = True
        if self.LAST_UPDATED.exists():
            last_updated = datetime.strptime(self.LAST_UPDATED.read_text().strip(), "%Y-%m-%d")
            if last_updated.date() == date.today():
                needs_update = False

        if needs_update or not self.CACHE_FILE.exists():
            print("Downloading latest corpCode.xml from DART...")
            self._download_and_extract()
            self.LAST_UPDATED.write_text(date.today().strftime("%Y-%m-%d"))
        else:
            print("Using cached corpCode.xml")

        self._parse_xml()

    def _download_and_extract(self) -> None:
        params = {"crtfc_key": self.api_key}
        resp = self._client.get(self.API_URL, params=params)
        resp.raise_for_status()

        zip_path = self.CACHE_DIR / "corp_codes.zip"
        zip_path.write_bytes(resp.content)

        with zipfile.ZipFile(zip_path, "r") as zf:
            xml_name = next(name for name in zf.namelist() if name.endswith(".xml"))
            zf.extract(xml_name, self.CACHE_DIR)
            os.replace(self.CACHE_DIR / xml_name, self.CACHE_FILE)
        zip_path.unlink()

    # ------------------------------------------------------------------ #
    # OPTIONAL – async refresh (not used by OpenDartClient yet)          #
    # ------------------------------------------------------------------ #
    async def aensure_cache(self) -> None:
        """Async variant of `_ensure_cache()`."""
        self.CACHE_DIR.mkdir(parents=True, exist_ok=True)

        needs_update = (
                not self.LAST_UPDATED.exists()
                or datetime.strptime(self.LAST_UPDATED.read_text().strip(), "%Y-%m-%d").date() != date.today()
        )

        if needs_update or not self.CACHE_FILE.exists():
            print("Downloading latest corpCode.xml from DART (async)…")
            await self._adownload_and_extract()
            self.LAST_UPDATED.write_text(date.today().strftime("%Y-%m-%d"))
        else:
            print("Using cached corpCode.xml")

        self._parse_xml()

    async def _adownload_and_extract(self) -> None:
        params = {"crtfc_key": self.api_key}
        resp = await self._async_client.get(self.API_URL, params=params)
        resp.raise_for_status()

        zip_path = self.CACHE_DIR / "corp_codes.zip"
        zip_path.write_bytes(resp.content)

        with zipfile.ZipFile(zip_path, "r") as zf:
            xml_name = next(name for name in zf.namelist() if name.endswith(".xml"))
            zf.extract(xml_name, self.CACHE_DIR)
            os.replace(self.CACHE_DIR / xml_name, self.CACHE_FILE)
        zip_path.unlink()

    # ------------------------------------------------------------------ #
    # XML parsing & look-ups                                             #
    # ------------------------------------------------------------------ #
    def _parse_xml(self) -> None:
        root = ET.parse(self.CACHE_FILE).getroot()
        self.corp_dict.clear()

        for item in root.findall("list"):
            corp_name = item.findtext("corp_name", "").strip()
            self.corp_dict[corp_name] = {
                "corp_code": item.findtext("corp_code", "").strip(),
                "corp_name": corp_name,
                "stock_code": item.findtext("stock_code", "").strip(),
                "corp_eng_name": item.findtext("corp_eng_name", "").strip(),
                "modify_date": item.findtext("modify_date", "").strip(),
            }

    # same public helpers as before ------------------------------------ #
    def get_id_by_name(self, name: str) -> str:
        try:
            return self.corp_dict[name]["corp_code"]
        except KeyError:
            raise ValueError(f"Corp name '{name}' not found in cache.")

    def get_id_by_stock_code(self, stock_code: str) -> str:
        for corp in self.corp_dict.values():
            if corp["stock_code"] == stock_code:
                return corp["corp_code"]
        raise ValueError(f"Stock code '{stock_code}' not found in cache.")

    def all(self) -> Dict[str, Dict[str, str]]:
        return self.corp_dict
