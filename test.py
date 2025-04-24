# test.py
import json
import sys

from endpoint import get_structured_financials

with open("key.json") as f:
    API_KEY = json.load(f)["DART_KEY"]

TICKERS = ["089590", "005930", "067280", "000660", "005380"]


def almost_equal(a, b, rel=1e-6):
    """Allow for tiny rounding differences."""
    return abs(a - b) <= rel * max(1, abs(b))


def check_one(stock_code: str) -> bool:
    """Return True if the quarterly sums match the YTD ‘all’ value everywhere."""
    data = get_structured_financials(stock_code, API_KEY)
    issues = []

    for fs in ("CFS", "OFS"):  # 연결 / 별도
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
        print(json.dumps({"issues": issues, "raw": data}, indent=2, ensure_ascii=False))
        return False

    print(f"✅ {stock_code} – all good")
    return True


if __name__ == "__main__":
    for code in TICKERS:
        if not check_one(code):
            sys.exit(1)
    print("🎉  All tickers passed the quarterly-sum test")
