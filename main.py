import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, HTTPException, Query

app = FastAPI(title="MT5 US News Blackout Bridge")

# --- ENV ---
BRIDGE_TOKEN = os.getenv("BRIDGE_TOKEN", "")
TE_API_KEY = os.getenv("TE_API_KEY", "")  # Trading Economics API key

# Default blackout window (minutes)
DEFAULT_PRE_MIN = int(os.getenv("PRE_MINUTES", "10"))
DEFAULT_POST_MIN = int(os.getenv("POST_MINUTES", "30"))

# Cache to reduce API calls
_CACHE: Dict[str, Any] = {"ts": 0.0, "data": None}
CACHE_TTL_SEC = int(os.getenv("CACHE_TTL_SEC", "60"))

# Trading Economics endpoint (US calendar)
# Docs show /calendar/country/{country}?c=API_KEY and that "Importance" exists in response. :contentReference[oaicite:1]{index=1}
TE_US_CAL_URL = "https://api.tradingeconomics.com/calendar/country/united%20states"

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _parse_te_date(s: str) -> Optional[datetime]:
    # TradingEconomics returns ISO-like timestamps (e.g., "2023-04-03T13:45:00")
    # Assume UTC if no tzinfo in string.
    try:
        dt = datetime.fromisoformat(s.replace("Z", ""))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

async def _fetch_us_high_impact_events() -> List[Dict[str, Any]]:
    if not TE_API_KEY:
        raise HTTPException(status_code=500, detail="Missing TE_API_KEY in Railway Variables")

    # Cache
    now_ts = time.time()
    if _CACHE["data"] is not None and (now_ts - _CACHE["ts"]) < CACHE_TTL_SEC:
        return _CACHE["data"]

    params = {"c": TE_API_KEY, "f": "json"}  # f=json supported per docs examples :contentReference[oaicite:2]{index=2}

    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(TE_US_CAL_URL, params=params)
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Calendar upstream error: {r.status_code}")

        data = r.json()
        if not isinstance(data, list):
            raise HTTPException(status_code=502, detail="Unexpected calendar response format")

    # Filter for high impact:
    # TradingEconomics returns "Importance" as an integer field in the calendar response. :contentReference[oaicite:3]{index=3}
    high = []
    for ev in data:
        try:
            imp = int(ev.get("Importance", 0))
        except Exception:
            imp = 0
        if imp >= 3:
            high.append(ev)

    _CACHE["ts"] = now_ts
    _CACHE["data"] = high
    return high

def _is_blackout(now: datetime, event_time: datetime, pre_min: int, post_min: int) -> bool:
    start = event_time.timestamp() - (pre_min * 60)
    end = event_time.timestamp() + (post_min * 60)
    return start <= now.timestamp() <= end

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/us_news_status")
async def us_news_status(
    token: str = Query(..., description="Shared secret token"),
    pre_minutes: int = Query(DEFAULT_PRE_MIN, ge=0, le=240),
    post_minutes: int = Query(DEFAULT_POST_MIN, ge=0, le=240),
):
    if not BRIDGE_TOKEN or token != BRIDGE_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

    now = _now_utc()
    events = await _fetch_us_high_impact_events()

    nearest = None
    minutes_to_clear = None

    for ev in events:
        dt = _parse_te_date(str(ev.get("Date", "")))
        if not dt:
            continue

        if _is_blackout(now, dt, pre_minutes, post_minutes):
            # If multiple overlap, report the first one found.
            # You can refine later to pick the closest.
            nearest = ev
            end_ts = dt.timestamp() + (post_minutes * 60)
            minutes_to_clear = max(0, int((end_ts - now.timestamp()) / 60))
            break

    if nearest:
        return {
            "us_news_blackout": True,
            "event": nearest.get("Event", ""),
            "category": nearest.get("Category", ""),
            "event_time_utc": _parse_te_date(str(nearest.get("Date", ""))).isoformat() if nearest.get("Date") else None,
            "minutes_to_clear": minutes_to_clear,
            "pre_minutes": pre_minutes,
            "post_minutes": post_minutes,
        }

    return {"us_news_blackout": False, "pre_minutes": pre_minutes, "post_minutes": post_minutes}
