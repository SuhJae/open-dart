from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set, Tuple

import httpx

from openDart.client import OpenDartClient
from openDart.endpoints.financials import CompanyFinancialReport

# ---------------------------------------------------------------------------
# Configuration & logging
# ---------------------------------------------------------------------------

FIRST_YEAR_SUPPORTED: int = 2015
CPU_COUNT: int = os.cpu_count() or 1
MAX_WORKERS_DEFAULT: int = max(CPU_COUNT, 4) * 4  # ≈ 4×CPU – good for I/O

logger = logging.getLogger("open_dart.structured_financials")
if not logger.handlers:
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s | %(name)s: %(message)s")


# ---------------------------------------------------------------------------
# Domain constants & errors
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class _QuarterLimit:
    quarters: int
    max_days: int


KIFRS_QUARTER_LIMITS: tuple[_QuarterLimit, ...] = (
    _QuarterLimit(1, 111),
    _QuarterLimit(2, 201),
    _QuarterLimit(3, 291),
)

try:
    from openDart.errors import OpenDartException as _OpenDartError  # type: ignore
except (ImportError, AttributeError):
    _OpenDartError = Exception  # type: ignore[assignment]

API_ERRORS: tuple[type[BaseException], ...] = (httpx.RequestError, _OpenDartError)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

def _parse_decimal(raw: str | int | float) -> Decimal:
    s = str(raw).replace(",", "").strip()
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    try:
        return Decimal(s)
    except InvalidOperation:
        return Decimal(0)


def _q_index(dt: datetime) -> int:
    return (dt.month - 1) // 3 + 1


def _ordered_qmap(qvals: Dict[int | str, Decimal]) -> Dict[str, float]:
    return {
        str(k): float(qvals[k])
        for k in sorted(qvals, key=lambda x: (x == "all", int(str(x)) if str(x).isdigit() else 0))
    }


def _quarters_from_span(start: datetime | None, end: datetime) -> Tuple[Set[int], bool]:
    q_end = _q_index(end)
    if start and start.month == 1 and start.day == 1 and start.year == end.year:
        return set(range(1, q_end + 1)), True

    delta_days = (end - (start or end)).days + 1
    for limit in KIFRS_QUARTER_LIMITS:
        if delta_days <= limit.max_days:
            k = limit.quarters
            break
    else:
        k = q_end
    return set(range(q_end - k + 1, q_end + 1)), False


def _solve_year(equations: Iterable[Tuple[Set[int], Decimal, bool]], *, annual_ytd: Decimal | None = None) -> Dict[
    str, float]:
    pending_by_len: Dict[int, List[Tuple[Set[int], Decimal, bool]]] = defaultdict(list)
    for qs, amt, is_ytd in ((qs, Decimal(amt), is_ytd) for qs, amt, is_ytd in equations):
        pending_by_len[len(qs)].append((qs, amt, is_ytd))

    solved: Dict[int | str, Decimal] = {}
    changed = True
    while changed:
        changed = False
        for span_len in (4, 3, 2, 1):
            for qs, amt, is_ytd in pending_by_len.get(span_len, []):
                unknown = [q for q in qs if q not in solved]
                if len(unknown) != 1:
                    continue
                q_to_solve = unknown[0]
                if is_ytd and q_to_solve != max(qs):
                    continue
                solved[q_to_solve] = amt - sum(solved[q] for q in qs if q in solved)
                changed = True

    if annual_ytd is not None:
        solved["all"] = annual_ytd
    elif all(q in solved for q in (1, 2, 3, 4)):
        solved["all"] = sum((solved[q] for q in (1, 2, 3, 4)), Decimal(0))

    return _ordered_qmap(solved)


# ---------------------------------------------------------------------------
# Caching (thread-safe, shared across async tasks via executor threads)
# ---------------------------------------------------------------------------

_API_CACHE: dict[tuple[str, int, int], Dict[str, CompanyFinancialReport] | None] = {}
_CACHE_DATE: date = date.today()
_CACHE_LOCK = threading.Lock()


def _get_financials_cached(client: OpenDartClient, corp_id: str, year: int, quarter: int):
    global _CACHE_DATE
    today = date.today()
    if today != _CACHE_DATE:
        with _CACHE_LOCK:
            if today != _CACHE_DATE:
                _API_CACHE.clear()
                _CACHE_DATE = today

    key = (corp_id, year, quarter)
    if key in _API_CACHE:
        return _API_CACHE[key]

    try:
        data = client.financials.get_financials(corp_id, fiscal_year=year, quarter=quarter)
    except API_ERRORS as exc:
        if "013" in str(exc):
            logger.debug("No DART data for %s Y%d Q%d", corp_id, year, quarter)
            data = None
        else:
            logger.error("DART API error for %s Y%d Q%d: %s", corp_id, year, quarter, exc)
            raise

    with _CACHE_LOCK:
        _API_CACHE[key] = data
    return data


async def _get_financials_cached_async(loop: asyncio.AbstractEventLoop, client: OpenDartClient, corp_id: str, year: int,
                                       quarter: int):
    return await loop.run_in_executor(None, _get_financials_cached, client, corp_id, year, quarter)


# ---------------------------------------------------------------------------
# Fetch helpers
# ---------------------------------------------------------------------------

def _iter_year_quarters(start_year: int, *, today: date) -> Iterable[Tuple[int, int]]:
    curr_year, curr_q = today.year, _q_index(datetime(today.year, today.month, 1))
    for year in range(start_year, curr_year + 1):
        max_q = curr_q if year == curr_year else 4
        for q in range(1, max_q + 1):
            yield year, q


async def _fetch_raw_structures_async(loop: asyncio.AbstractEventLoop, client: OpenDartClient, corp_id: str, *,
                                      max_workers: int):
    bs_snap: Dict[str, Dict[str, Dict[int, Dict[int, Decimal]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(dict)))
    is_eqs: Dict[str, Dict[str, Dict[int, List[Tuple[Set[int], Decimal, bool]]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list)))
    is_ytd_latest: Dict[str, Dict[str, Dict[int, Tuple[int, Decimal]]]] = defaultdict(lambda: defaultdict(dict))

    tasks = [
        _get_financials_cached_async(loop, client, corp_id, y, q)
        for y, q in _iter_year_quarters(FIRST_YEAR_SUPPORTED, today=date.today())
    ]
    sem = asyncio.Semaphore(max_workers)

    async def sem_coro(c):
        async with sem:
            return await c

    for coro in asyncio.as_completed([sem_coro(t) for t in tasks]):
        reports = await coro
        _aggregate_one_report(reports, bs_snap, is_eqs, is_ytd_latest)

    return bs_snap, is_eqs, is_ytd_latest


# ---------------------------------------------------------------------------
# Aggregation util
# ---------------------------------------------------------------------------

def _aggregate_one_report(reports, bs_snap, is_eqs, is_ytd_latest):
    if not reports:
        return
    for key, rpt in reports.items():
        cons_label, stmt_code = key.split("-", 1)
        stmt_obj = rpt.statements.get(stmt_code)
        if stmt_obj is None:
            continue
        for acct, series in stmt_obj.time_series_by_account.items():
            for entry in series:
                amt = _parse_decimal(entry.amount)
                if amt == 0:
                    continue
                if stmt_code == "BS":
                    d = entry.report_date
                    bs_snap[cons_label][acct][d.year][_q_index(d)] = amt
                    continue
                start = getattr(entry, "start_date", None)
                end = entry.end_date or entry.report_date
                qset, is_ytd = _quarters_from_span(start, end)
                is_eqs[cons_label][acct][end.year].append((qset, amt, is_ytd))
                if is_ytd:
                    qmax = max(qset)
                    prev = is_ytd_latest[cons_label][acct].get(end.year)
                    if prev is None or qmax > prev[0]:
                        is_ytd_latest[cons_label][acct][end.year] = (qmax, amt)


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------

def _normalise_financials(bs_snap, is_eqs, is_ytd_latest):
    cons_fin: Dict[str, Dict[str, Dict[str, Dict[str, Any]]]] = {"CFS": {"BS": {}, "IS": {}},
                                                                 "OFS": {"BS": {}, "IS": {}}}
    for cons in ("CFS", "OFS"):
        for acct, years in bs_snap[cons].items():
            for year, qmap in years.items():
                cons_fin[cons]["BS"].setdefault(acct, {})[str(year)] = _ordered_qmap(qmap)
        for acct, years in is_eqs[cons].items():
            for year, eq_list in years.items():
                annual_ytd_amt = is_ytd_latest[cons][acct].get(year, (None, None))[1]
                solved = _solve_year(eq_list, annual_ytd=annual_ytd_amt)
                if solved:
                    cons_fin[cons]["IS"].setdefault(acct, {})[str(year)] = solved
    return cons_fin


# ---------------------------------------------------------------------------
# Service class (async-only)
# ---------------------------------------------------------------------------

class StructuredFinancialsService:
    """Reusable async service that shares an `OpenDartClient` instance."""

    def __init__(self, client: OpenDartClient, *, max_workers: int | None = None):
        self.client = client
        self.max_workers = max_workers or MAX_WORKERS_DEFAULT

    async def get(self, stock_code: str) -> Dict[str, Any]:
        loop = asyncio.get_running_loop()
        corp_id = self.client.corp_cache.get_id_by_stock_code(stock_code)
        summary = self.client.company.get_company_summary(corp_id).to_dict()
        bs_snap, is_eqs, is_ytd_latest = await _fetch_raw_structures_async(
            loop, self.client, corp_id, max_workers=self.max_workers
        )
        structured = _normalise_financials(bs_snap, is_eqs, is_ytd_latest)
        return {"company_summary": summary, "CFS": structured["CFS"], "OFS": structured["OFS"]}


# ---------------------------------------------------------------------------
# Utility helpers/CLI
# ---------------------------------------------------------------------------

def _load_api_key() -> str:
    key_path = Path("key.json")
    if not key_path.exists():
        raise SystemExit("key.json not found – please supply your DART_KEY")
    return json.loads(key_path.read_text())["DART_KEY"]


async def main():
    """Example async CLI entry-point."""

    api_key = _load_api_key()
    async with OpenDartClient(api_key) as client:
        service = StructuredFinancialsService(client)
        data = await service.get("089590")  # A test stock code
        print(json.dumps(data, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
