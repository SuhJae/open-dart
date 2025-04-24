from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import Any, Dict

import httpx


class BaseAPI:
    def __init__(self, api_key: str, timeout: float = 10.0):
        self.api_key = api_key
        self.client = httpx.Client(timeout=timeout, verify=True)

    def _get(self, url: str, params: Dict) -> dict:
        params.update({
            "crtfc_key": self.api_key,
        })
        try:
            response = self.client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            if data.get('status') != '000':
                raise RuntimeError(f"API error: {data.get('message')} ({data.get('status')})")

            return data
        except httpx.HTTPError as e:
            raise RuntimeError(f"API request failed: {e}")


class BaseModel:
    def to_dict(self) -> Dict[str, Any]:
        def serialize(value: Any) -> Any:
            if isinstance(value, datetime):
                return value.strftime("%Y-%m-%d")
            if is_dataclass(value):
                return asdict(value)
            return value

        return {k: serialize(v) for k, v in asdict(self).items()}
