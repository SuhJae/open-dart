from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Dict

from endpoint import StructuredFinancialsService
from openDart.client import OpenDartClient

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

API_KEY_PATH = Path("key.json")
if not API_KEY_PATH.exists():
    sys.exit("key.json not found; please supply your DART_KEY")
API_KEY: str = json.loads(API_KEY_PATH.read_text())["DART_KEY"]

TICKERS = ["089590", "005930", "067280", "000660", "005380"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def almost_equal(a: float, b: float, rel: float = 1e-6) -> bool:
    """Allow for tiny rounding differences."""

    return abs(a - b) <= rel * max(1, abs(int(b)))


async def check_one(service: StructuredFinancialsService, stock_code: str) -> bool:
    """Return True if quarterly sums match the YTD value everywhere."""

    data: Dict = await service.get(stock_code)
    issues = []

    for fs in ("CFS", "OFS"):
        if fs not in data or "IS" not in data[fs]:
            continue
        for acct, years in data[fs]["IS"].items():
            for yr, qs in years.items():
                if not ("all" in qs and all(str(q) in qs for q in (1, 2, 3, 4))):
                    continue  # skip incomplete years
                q_sum = sum(qs[str(q)] for q in (1, 2, 3, 4))
                if not almost_equal(q_sum, qs["all"]):
                    issues.append(
                        {
                            "fs": fs,
                            "account": acct,
                            "year": yr,
                            "q_sum": q_sum,
                            "all": qs["all"],
                        }
                    )

    if issues:
        print(f"❌ {stock_code} – mismatch detected")
        print(json.dumps({"issues": issues}, indent=2, ensure_ascii=False))
        return False

    print(f"✅ {stock_code} – all good")
    return True


# ---------------------------------------------------------------------------
# Main entry-point (async)
# ---------------------------------------------------------------------------

async def main() -> None:
    async with OpenDartClient(API_KEY) as client:
        service = StructuredFinancialsService(client)

        # Run ticker checks concurrently, but limit to 5 parallel tasks
        sem = asyncio.Semaphore(5)

        async def sem_check(code: str):
            async with sem:
                return await check_one(service, code)

        results = await asyncio.gather(*(sem_check(code) for code in TICKERS))

        if all(results):
            print("🎉  All tickers passed the quarterly-sum test")
        else:
            sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
