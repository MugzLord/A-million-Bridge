import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, HTTPException, Query
from xml.etree import ElementTree as ET

app = FastAPI(title="MT5 US News Blackout Bridge")

# =========================
# ENV
# =========================
BRIDGE_TOKEN = os.getenv("BRIDGE_TOKEN", "")

# Blackout window (minutes)
DEFAULT_PRE_MIN = int(os.getenv("PRE_MINUTES", "10"))
DEFAULT_POST_MIN = int(os.getenv("POST_MINUTES", "30"))

# Cache to reduce upstream hits
_CACHE: Dict[str, Any] = {"ts": 0.0, "data": None}
CACHE_TTL_SEC = int(os.getenv("CACHE_TTL_SEC", "300"))  # 5 min default

# ForexFactory calendar XML (via faireconomy)
# We will parse events and filter USD high impact.
FF_THISWEEK_URL = os.getenv(
    "FF_CAL_URL",
    "https://nfs.faireconomy.media/ff_calendar_thisweek.xml"
)

# =========================
# HELPERS
# =========================
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _safe_text(el: Optional[ET.Element]) -> str:
    return (el.text or "").strip() if el is not None else ""

def _parse_epoch_utc(s: str) -> Optional[datetime]:
    """
    Some FF XML feeds include <timestamp> as epoch seconds.
    If present, use it (best).
    """
    try:
        if not s:
            return None
        ts = int(float(s))
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:
        return None

def _is_blackout(now: datetime, event_time: datetime, pre_min: int, post_min: int) -> bool:
    start = event_time.timestamp() - (pre_min * 60)
    end = event_time.timestamp() + (post_min * 60)
    return start <= now.timestamp() <= end

# =========================
# CALENDAR FETCH
# =========================
async def _fetch_us_high_impact_events() -> List[Dict[str, Any]]:
    # Cache
    now_ts = time.time()
    if _CACHE["data"] is not None and (now_ts - _CACHE["ts"]) < CACHE_TTL_SEC:
        return _CACHE["data"]

    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        r = await client.get(FF_THISWEEK_URL)

        if r.status_code != 200:
            upstream_body = (r.text or "")[:300]
            raise HTTPException(
                status_code=502,
                detail=f"Calendar upstream error: {r.status_code} | {upstream_body}"
            )

    xml_text = r.text
    try:
        root = ET.fromstring(xml_text)
    except Exception:
        upstream_body = (xml_text or "")[:300]
        raise HTTPException(status_code=502, detail=f"Upstream returned invalid XML | {upstream_body}")

    events: List[Dict[str, Any]] = []

    # Common structure: <weeklyevents><event>...</event></weeklyevents>
    for ev in root.findall(".//event"):
        # ForexFactory feeds typically tag currency as USD for US events
        currency = _safe_text(ev.find("currency")) or _safe_text(ev.find("country"))
        impact = _safe_text(ev.find("impact"))

        # Keep it strict: US = USD and High impact
        if currency.upper() != "USD":
            continue
        if impact.lower() != "high":
            continue

        title = _safe_text(ev.find("title")) or _safe_text(ev.find("event"))
        timestamp = _safe_text(ev.find("timestamp"))  # best-case
        dt = _parse_epoch_utc(timestamp)

        # If no timestamp exists, try date+time (fallback).
        # Some feeds have <date> and <time> but timezone can vary; timestamp is preferred.
        if dt is None:
            date_s = _safe_text(ev.find("date"))
            time_s = _safe_text(ev.find("time"))
            try:
                # Fallback assumes UTC if no tz info (less ideal but usable)
                # Expected formats vary; keep conservative.
                # If parsing fails, skip.
                if date_s and time_s and time_s.lower() not in ("all day", "tentative"):
                    # Try: YYYY-MM-DD + HH:MM
                    # If date is like "2025-12-28"
                    dt_guess = datetime.fromisoformat(f"{date_s}T{time_s}:00")
                    dt = dt_guess.replace(tzinfo=timezone.utc)
            except Exception:
                dt = None

        if dt is None:
            continue

        events.append(
            {
                "event": title,
                "currency": "USD",
                "impact": "High",
                "event_time_utc": dt,
            }
        )

    _CACHE["ts"] = now_ts
    _CACHE["data"] = events
    return events

# =========================
# ROUTES
# =========================
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

    # Find any event currently in blackout
    for ev in events:
        dt: datetime = ev["event_time_utc"]
        if _is_blackout(now, dt, pre_minutes, post_minutes):
            end_ts = dt.timestamp() + (post_minutes * 60)
            minutes_to_clear = max(0, int((end_ts - now.timestamp()) / 60))
            return {
                "us_news_blackout": True,
                "event": ev.get("event", ""),
                "currency": "USD",
                "impact": "High",
                "event_time_utc": dt.isoformat(),
                "minutes_to_clear": minutes_to_clear,
                "pre_minutes": pre_minutes,
                "post_minutes": post_minutes,
                "source": "ff_thisweek_xml",
            }

    return {
        "us_news_blackout": False,
        "pre_minutes": pre_minutes,
        "post_minutes": post_minutes,
        "source": "ff_thisweek_xml",
    }
