import os
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Dict

import httpx


class CorpCodeCache:
    API_URL = "https://opendart.fss.or.kr/api/corpCode.xml"
    CACHE_DIR = Path.home() / ".openDart"
    CACHE_FILE = CACHE_DIR / "corp_codes.xml"
    LAST_UPDATED = CACHE_DIR / "last_updated.txt"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.corp_dict: Dict[str, Dict[str, str]] = {}
        self._ensure_cache()

    def _ensure_cache(self):
        self.CACHE_DIR.mkdir(parents=True, exist_ok=True)

        needs_update = True
        if self.LAST_UPDATED.exists():
            last_updated = datetime.strptime(self.LAST_UPDATED.read_text().strip(), "%Y-%m-%d")
            if last_updated.date() == datetime.today().date():
                needs_update = False

        if needs_update or not self.CACHE_FILE.exists():
            print("Downloading latest corpCode.xml from DART...")
            self._download_and_extract()
            self.LAST_UPDATED.write_text(datetime.today().strftime("%Y-%m-%d"))
        else:
            print("Using cached corpCode.xml")

        self._parse_xml()

    def _download_and_extract(self):
        params = {"crtfc_key": self.api_key}
        with httpx.Client() as client:
            response = client.get(self.API_URL, params=params)
            response.raise_for_status()
            zip_path = self.CACHE_DIR / "corp_codes.zip"
            zip_path.write_bytes(response.content)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for name in zip_ref.namelist():
                if name.endswith('.xml'):
                    zip_ref.extract(name, self.CACHE_DIR)
                    os.replace(self.CACHE_DIR / name, self.CACHE_FILE)
        zip_path.unlink()

    def _parse_xml(self):
        tree = ET.parse(self.CACHE_FILE)
        root = tree.getroot()
        self.corp_dict.clear()

        for item in root.findall("list"):
            corp_code = item.findtext("corp_code", default="").strip()
            corp_name = item.findtext("corp_name", default="").strip()
            stock_code = item.findtext("stock_code", default="").strip()
            corp_eng_name = item.findtext("corp_eng_name", default="").strip()
            modify_date = item.findtext("modify_date", default="").strip()

            self.corp_dict[corp_name] = {
                "corp_code": corp_code,
                "corp_name": corp_name,
                "stock_code": stock_code,
                "corp_eng_name": corp_eng_name,
                "modify_date": modify_date,
            }

    def get_id_by_name(self, name: str) -> str:
        code = self.corp_dict.get(name, {}).get("corp_code")
        if not code:
            raise ValueError(f"Corp name '{name}' not found in cache.")
        return code

    def get_id_by_stock_code(self, stock_code: str) -> str:
        for corp in self.corp_dict.values():
            if corp.get("stock_code") == stock_code:
                return corp["corp_code"]
        raise ValueError(f"Stock code '{stock_code}' not found in cache.")

    def all(self) -> Dict[str, Dict[str, str]]:
        return self.corp_dict
