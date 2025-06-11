from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from fastapi import FastAPI, HTTPException, Path as P
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from endpoint import StructuredFinancialsService
from openDart.client import OpenDartClient

# ─────────────────────────────────────────────────────────────────────────────
# Load API key
# ─────────────────────────────────────────────────────────────────────────────

API_KEY_PATH = Path(__file__).with_name("key.json")
if not API_KEY_PATH.exists():
    raise RuntimeError(
        "key.json not found – please place your DART_KEY alongside this file"
    )

API_KEY: str = json.loads(API_KEY_PATH.read_text())["DART_KEY"]

# ─────────────────────────────────────────────────────────────────────────────
# Application & CORS
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="DART Structured Financials API",
    version="1.0.0",
)

# allow only your Nuxt dev server in development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ─────────────────────────────────────────────────────────────────────────────
# Lifespan: startup & shutdown
# ─────────────────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def _startup() -> None:
    # create & enter the OpenDartClient async context
    client = OpenDartClient(API_KEY)
    await client.__aenter__()  # type: ignore[attr-defined]
    app.state.client = client
    app.state.svc = StructuredFinancialsService(client)


@app.on_event("shutdown")
async def _shutdown() -> None:
    # cleanly close the OpenDartClient
    client = app.state.client
    await client.__aexit__(None, None, None)  # type: ignore[attr-defined]


# ─────────────────────────────────────────────────────────────────────────────
# Route: GET /financials/{stock_code}
# ─────────────────────────────────────────────────────────────────────────────

@app.get(
    "/financials/{stock_code}",
    response_class=JSONResponse,
    summary="Get structured financials",
)
async def get_financials(
        stock_code: str = P(
            ...,
            regex=r"^\d{6}$",
            description="6-digit KRX stock code",
        )
) -> Dict[str, Any]:
    """
    Return the structured financial statements for *stock_code*.

    - 404 = corp/stock code not found (in corpCode.xml).
    - 502 = Upstream DART error / transient failure.
    """
    svc: StructuredFinancialsService = app.state.svc

    try:
        return await svc.get(stock_code)
    except ValueError as exc:
        # corp_code lookup failure
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        # upstream or parsing error
        raise HTTPException(status_code=502, detail=str(exc)) from exc
