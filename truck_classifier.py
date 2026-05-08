"""
truck_classifier.py — Classify trucks as active/idle/at_yard/in_shop/unassigned/unknown.

Runs every 30 minutes as a background job (main.py). Writes to trucks.activity_status.
The main polling loop skips trucks whose status is at_yard, in_shop, or unassigned.

Classification rules (first match wins):
  1. hours_at_shop > 24                                  → in_shop
  2. hours_at_yard > 48 AND miles_driven_48h < 20        → at_yard
  3. NOT has_assigned_driver                              → unassigned
  4. engine_hours_24h > 2                                → active
  5. miles_driven_48h > 0 AND engine_hours_24h < 0.5     → idle
  6. otherwise                                           → unknown

Grep for STATUS_CHANGE to track classification shifts for threshold tuning.
"""

import json
import logging
import math
import time
from datetime import datetime, timezone, timedelta

import requests

import samsara_client
from samsara_client import HEADERS

log = logging.getLogger(__name__)

HISTORY_HOURS = 50  # long enough to detect yard > 48h and shop > 24h

# Batch-level cache — reset at the start of each classify_all_trucks() run.
# Avoids duplicate Samsara calls within a single batch.
_cache: dict = {}


# ---------------------------------------------------------------------------
# Geo helpers
# ---------------------------------------------------------------------------

def _haversine_miles(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    R = 3958.8
    φ1, φ2 = math.radians(lat1), math.radians(lat2)
    dφ = math.radians(lat2 - lat1)
    dλ = math.radians(lng2 - lng1)
    a  = math.sin(dφ / 2) ** 2 + math.cos(φ1) * math.cos(φ2) * math.sin(dλ / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def _haversine_meters(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    return _haversine_miles(lat1, lng1, lat2, lng2) * 1609.344


def _compute_miles_driven(history: list[dict]) -> float:
    """Sum haversine distances between consecutive GPS points."""
    total, prev = 0.0, None
    for pt in history:
        if prev:
            lat1, lng1 = prev.get("lat"), prev.get("lng")
            lat2, lng2 = pt.get("lat"),  pt.get("lng")
            if None not in (lat1, lng1, lat2, lng2):
                total += _haversine_miles(lat1, lng1, lat2, lng2)
        prev = pt
    return round(total, 1)


# ---------------------------------------------------------------------------
# Samsara API helpers
# ---------------------------------------------------------------------------

def _samsara_get(url: str, params: dict, retries: int = 3) -> dict:
    """GET with exponential backoff on 429 or connection errors."""
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=15)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 10)) * (attempt + 1)
                log.warning("Samsara 429 rate limit — waiting %ds (attempt %d)", wait, attempt + 1)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            log.warning("Samsara GET attempt %d/%d failed: %s", attempt + 1, retries, exc)
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return {}


def _get_location_history(vehicle_id: str) -> list[dict]:
    """50h GPS history (sorted oldest first), with batch cache."""
    key = f"hist_{vehicle_id}"
    if key not in _cache:
        _cache[key] = samsara_client.get_vehicle_location_history(vehicle_id, hours_back=HISTORY_HOURS)
    return _cache[key]


def _get_engine_hours_24h(vehicle_id: str) -> float:
    """
    Total engine-on time in the last 24h, in hours.
    Uses Samsara /fleet/vehicles/stats/history with engineStates type.
    """
    key = f"eng_{vehicle_id}"
    if key in _cache:
        return _cache[key]

    now        = datetime.now(timezone.utc)
    start_time = now - timedelta(hours=24)

    data = _samsara_get(
        "https://api.samsara.com/fleet/vehicles/stats/history",
        {
            "vehicleIds": vehicle_id,
            "types":      "engineStates",
            "startTime":  start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "endTime":    now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
    )

    vehicles_data = data.get("data", [])
    if not vehicles_data:
        _cache[key] = 0.0
        return 0.0

    events = vehicles_data[0].get("engineStates", [])
    total_on_s, prev_val, prev_t = 0.0, None, None

    for ev in sorted(events, key=lambda e: e.get("time", "")):
        try:
            t = datetime.fromisoformat(ev["time"].replace("Z", "+00:00"))
        except Exception:
            continue
        if prev_val == "On" and prev_t is not None:
            total_on_s += (t - prev_t).total_seconds()
        prev_val, prev_t = ev.get("value"), t

    # If engine is still on at window end, count to now
    if prev_val == "On" and prev_t is not None:
        total_on_s += (now - prev_t).total_seconds()

    hours = round(total_on_s / 3600, 2)
    _cache[key] = hours
    return hours


def _get_has_driver(vehicle_id: str) -> bool:
    """Check driver assignment; uses batch cache when pre-populated by classify_all_trucks."""
    key = f"has_driver_{vehicle_id}"
    if key not in _cache:
        driver = samsara_client.get_driver_for_vehicle(vehicle_id)
        _cache[key] = driver is not None
    return _cache[key]


# ---------------------------------------------------------------------------
# Geofence helpers
# ---------------------------------------------------------------------------

def _get_geofences() -> list[dict]:
    if "geofences" not in _cache:
        from database import get_all_geofences
        _cache["geofences"] = get_all_geofences()
    return _cache["geofences"]


def _point_in_fence(lat: float, lng: float, fence: dict) -> bool:
    return _haversine_meters(lat, lng, float(fence["latitude"]), float(fence["longitude"])) <= fence["radius_meters"]


def _continuous_hours_in_type(history: list[dict], fence_type: str) -> float:
    """
    Hours the truck has been continuously inside any fence of the given type,
    measured backward from the most recent GPS point.
    Returns 0 if the truck is not currently inside such a fence.

    If the streak reaches the oldest available history point without a break,
    the returned value approximates the window size (up to HISTORY_HOURS).
    """
    fences = [f for f in _get_geofences() if f.get("type") == fence_type]
    if not fences or not history:
        return 0.0

    now          = datetime.now(timezone.utc)
    streak_start = None

    for pt in reversed(history):
        lat, lng, ts = pt.get("lat"), pt.get("lng"), pt.get("time", "")
        if lat is None or lng is None:
            continue
        try:
            t = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            continue
        if any(_point_in_fence(lat, lng, f) for f in fences):
            streak_start = t
        else:
            break  # left the fence — streak ended

    if streak_start is None:
        return 0.0
    return max(0.0, (now - streak_start).total_seconds() / 3600)


# ---------------------------------------------------------------------------
# Core classifier
# ---------------------------------------------------------------------------

def classify_truck(vehicle_id: str, vehicle_name: str = "") -> dict:
    """
    Classify a single truck. Returns {"status": str, "signals": dict}.
    Writes the result to the trucks table and logs STATUS_CHANGE lines on transitions.
    """
    from database import get_truck_activity_status, save_truck_activity_status
    try:
        history         = _get_location_history(vehicle_id)
        miles_48h       = _compute_miles_driven(history)
        engine_hours_24 = _get_engine_hours_24h(vehicle_id)
        hours_at_yard   = _continuous_hours_in_type(history, "yard")
        hours_at_shop   = _continuous_hours_in_type(history, "shop")
        has_driver      = _get_has_driver(vehicle_id)

        signals = {
            "miles_driven_48h":    miles_48h,
            "engine_hours_24h":    engine_hours_24,
            "hours_at_yard":       round(hours_at_yard, 1),
            "hours_at_shop":       round(hours_at_shop, 1),
            "has_assigned_driver": has_driver,
        }

        # First match wins
        if hours_at_shop > 24:
            status = "in_shop"
        elif hours_at_yard > 48 and miles_48h < 20:
            status = "at_yard"
        elif not has_driver:
            status = "unassigned"
        elif engine_hours_24 > 2:
            status = "active"
        elif miles_48h > 0 and engine_hours_24 < 0.5:
            status = "idle"
        else:
            status = "unknown"

        old_status = get_truck_activity_status(vehicle_name)
        if old_status != status:
            log.info(
                "STATUS_CHANGE truck_id=%s name=%r old=%r new=%r signals=%s",
                vehicle_id, vehicle_name, old_status, status, json.dumps(signals),
            )

        save_truck_activity_status(vehicle_name, status, signals)
        return {"status": status, "signals": signals}

    except Exception as exc:
        log.error(
            "classify_truck failed for %s (%s): %s", vehicle_id, vehicle_name, exc, exc_info=True
        )
        return {"status": "unknown", "signals": {}}


def classify_all_trucks() -> dict[str, str]:
    """
    Classify every truck currently visible in Samsara.
    Writes results to the trucks table. Returns {vehicle_id: status}.

    Batch cache avoids duplicate API calls within the run.
    Samsara API timeout for a single truck → logged, skipped, batch continues.
    """
    global _cache
    _cache = {}

    log.info("Classifier: starting batch run...")
    try:
        vehicles = samsara_client.get_combined_vehicle_data()
    except Exception as exc:
        log.error("Classifier: failed to fetch vehicles from Samsara: %s", exc)
        return {}

    # Pre-populate driver cache from combined-data results (driver_name already fetched there)
    for v in vehicles:
        _cache[f"has_driver_{v['vehicle_id']}"] = v.get("driver_name") is not None

    _cache["geofences"] = _get_geofences()

    results: dict[str, str] = {}
    counts:  dict[str, int] = {}

    for v in vehicles:
        vid, name = v["vehicle_id"], v["vehicle_name"]
        try:
            r = classify_truck(vid, name)
            s = r["status"]
            results[vid]  = s
            counts[s]     = counts.get(s, 0) + 1
            time.sleep(0.1)  # gentle pacing between per-truck API calls
        except Exception as exc:
            log.warning("Classifier: skipped %s (%s): %s", vid, name, exc)
            results[vid] = "unknown"

    log.info("Classifier: done — %d trucks: %s", len(results), counts)
    _cache = {}
    return results
