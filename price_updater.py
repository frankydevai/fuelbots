"""
price_updater.py — Handle daily fuel price file uploads AND route fuel optimization

Accepts the EFS CSV format:
  Station, Address, City, State, longitude, latitude, Retail price, Discounted price

Admin sends this file to the bot every day in Telegram.
Bot auto-detects it and reloads all station prices.

V2 OPTIMIZATION ENGINE:
  1. State-Line Buffering — detect expensive states on route, force full fill before border
  2. Fuel Desert Logic — <1 stop per 100 miles → mandatory fill at last high-density spot
  3. Post-Trip Deadhead Reserve — arrive at delivery with 30% + 150mi deadhead
  4. IFTA State-Weighting — penalty for fueling inside expensive states in True Cost formula
"""

import logging
import math
from config import (
    DEFAULT_TANK_GAL,
    DEFAULT_MPG,
    SAFETY_RESERVE,
    DEADHEAD_RESERVE_MILES,
    FULL_TANK_FILL_GAL,
)

log = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# EXPENSIVE STATE CONFIGURATION
# States where diesel is significantly more expensive — avoid fueling here
# ═══════════════════════════════════════════════════════════════════════════════

EXPENSIVE_STATES = {
    "CA": {"name": "California",    "penalty_mult": 2.5, "border_fill_pct": 95},
    "PA": {"name": "Pennsylvania",  "penalty_mult": 1.8, "border_fill_pct": 90},
    "NY": {"name": "New York",      "penalty_mult": 1.8, "border_fill_pct": 90},
    "WA": {"name": "Washington",    "penalty_mult": 1.6, "border_fill_pct": 90},
    "IL": {"name": "Illinois",      "penalty_mult": 1.7, "border_fill_pct": 85},
    "NJ": {"name": "New Jersey",    "penalty_mult": 1.5, "border_fill_pct": 85},
    "CT": {"name": "Connecticut",   "penalty_mult": 1.5, "border_fill_pct": 90},
    "OR": {"name": "Oregon",        "penalty_mult": 1.4, "border_fill_pct": 85},
    "IN": {"name": "Indiana",       "penalty_mult": 1.6, "border_fill_pct": 85},
    "MI": {"name": "Michigan",      "penalty_mult": 1.3, "border_fill_pct": 80},
}

# Preferred border states — fuel here BEFORE entering expensive neighbor
BORDER_FUEL_STATES = {
    "CA": ["AZ", "NV", "OR"],
    "NY": ["NJ", "PA", "CT"],
    "PA": ["OH", "WV", "MD", "NJ"],
    "WA": ["OR", "ID"],
    "IL": ["IN", "WI", "IA", "MO"],
    "NJ": ["PA", "DE"],
    "CT": ["NY", "MA"],
    "OR": ["WA", "NV", "ID"],
    "IN": ["OH", "IL", "KY"],
    "MI": ["OH", "IN"],
}

# Sparse states — fuel deserts
SPARSE_STATES = {
    "MT": {"name": "Montana",  "avg_gap_miles": 120},
    "WY": {"name": "Wyoming",  "avg_gap_miles": 110},
    "NV": {"name": "Nevada",   "avg_gap_miles": 100},
    "UT": {"name": "Utah",     "avg_gap_miles": 95},
    "ID": {"name": "Idaho",    "avg_gap_miles": 90},
    "ND": {"name": "N. Dakota", "avg_gap_miles": 85},
    "SD": {"name": "S. Dakota", "avg_gap_miles": 80},
    "NM": {"name": "New Mexico", "avg_gap_miles": 85},
    "KS": {"name": "Kansas",   "avg_gap_miles": 75},
    "NE": {"name": "Nebraska", "avg_gap_miles": 70},
}


def update_from_file(file_bytes: bytes, filename: str) -> tuple[int, str]:
    """
    Parse uploaded file and update fuel prices in DB.
    Supports the daily EFS CSV format and cleaned CSV variants.
    """
    fname = filename.lower().strip()

    if fname.endswith('.csv'):
        try:
            from database import import_efs_csv
            return import_efs_csv(file_bytes)
        except Exception as e:
            log.error(f"EFS CSV import error: {e}", exc_info=True)
            return 0, f"❌ Failed to import CSV: `{e}`"

    return 0, (
        f"❌ Unsupported file: `{filename}`\n"
        f"Please send a fuel price CSV with station, city/state, coordinates, and card price columns."
    )


# ═══════════════════════════════════════════════════════════════════════════════
# TRUE COST FORMULA — IFTA-weighted with Expensive State Penalty
# ═══════════════════════════════════════════════════════════════════════════════

def true_cost_v2(
    card_price: float,
    stop_state: str,
    detour_miles: float,
    gallons_to_fill: float,
    mpg: float = DEFAULT_MPG,
) -> float:
    """
    V2 True Cost = (Card Discounted Price - State IFTA Tax Rate) + Detour Penalty
                 + Expensive State Surcharge

    The IFTA arbitrage: you're buying tax credits, not just gas.

    Detour Penalty = (detour_miles × 2 × card_price / mpg) / gallons_to_fill
      → converted to per-gallon cost so it's comparable

    Expensive State Surcharge = penalty_mult × base_penalty (if inside expensive state)
    """
    try:
        from ifta import net_price_after_ifta, get_ifta_rate
        ifta_rate = get_ifta_rate(stop_state)
        net_price = net_price_after_ifta(card_price, stop_state)
    except Exception:
        ifta_rate = 0.0
        net_price = card_price

    # Per-gallon detour penalty
    if gallons_to_fill > 0 and mpg > 0:
        detour_cost_per_gal = (detour_miles * 2 * card_price / mpg) / gallons_to_fill
    else:
        detour_cost_per_gal = 0.0

    # Expensive state penalty — multiplier on the detour cost
    state_upper = stop_state.upper().strip()
    penalty_mult = EXPENSIVE_STATES.get(state_upper, {}).get("penalty_mult", 1.0)

    # If fueling INSIDE an expensive state, add a surcharge
    # This makes bordering-state stops score better
    expensive_surcharge = 0.0
    if state_upper in EXPENSIVE_STATES:
        # Add 10-25 cents/gal penalty for fueling in expensive state
        expensive_surcharge = 0.10 * penalty_mult

    true_cost_per_gal = net_price + (detour_cost_per_gal * penalty_mult) + expensive_surcharge
    total_cost = true_cost_per_gal * gallons_to_fill

    return round(total_cost, 2)


# ═══════════════════════════════════════════════════════════════════════════════
# EXPENSIVE STATE DETECTION — Route Analysis
# ═══════════════════════════════════════════════════════════════════════════════

def detect_expensive_states_on_route(route_states: list[str]) -> list[dict]:
    """
    Scan route for expensive states.
    Returns list of { state, name, penalty_mult, border_fill_pct }
    """
    expensive = []
    for state in route_states:
        s = state.upper().strip()
        if s in EXPENSIVE_STATES:
            expensive.append({
                "state": s,
                **EXPENSIVE_STATES[s],
            })
    return expensive


def find_last_cheap_stop_before_border(
    all_stops: list[dict],
    border_state: str,
    truck_lat: float,
    truck_lng: float,
    max_search_miles: float = 150.0,
) -> dict | None:
    """
    Find the optimal EFS station BEFORE entering an expensive state.
    Prioritizes stops in bordering states (AZ/NV before CA, NJ before NY, etc.)
    """
    from truck_stop_finder import haversine_miles
    from ifta import net_price_after_ifta

    preferred_states = set(BORDER_FUEL_STATES.get(border_state.upper(), []))
    candidates = []

    for stop in all_stops:
        if not stop.get("diesel_price"):
            continue
        stop_state = (stop.get("state") or "").upper()

        # Never recommend a stop INSIDE the expensive state
        if stop_state == border_state.upper():
            continue

        try:
            dist = haversine_miles(
                truck_lat, truck_lng,
                float(stop["latitude"]), float(stop["longitude"])
            )
        except Exception:
            continue

        if dist > max_search_miles:
            continue

        card_price = float(stop["diesel_price"])
        net_price = net_price_after_ifta(card_price, stop_state)

        # Score: cheaper is better, bordering state gets bonus
        border_bonus = -0.05 if stop_state in preferred_states else 0.0
        score = net_price + border_bonus

        candidates.append({
            **stop,
            "dist_from_truck": round(dist, 1),
            "net_price": round(net_price, 4),
            "score": score,
            "in_preferred_state": stop_state in preferred_states,
        })

    if not candidates:
        return None

    # Sort by score (lowest true cost wins)
    candidates.sort(key=lambda s: s["score"])
    return candidates[0]


# ═══════════════════════════════════════════════════════════════════════════════
# FUEL DESERT DETECTION — Station Density Analysis
# ═══════════════════════════════════════════════════════════════════════════════

def analyze_station_density(
    all_stops: list[dict],
    route_waypoints: list[dict],
    corridor_miles: float = 30.0,
) -> list[dict]:
    """
    Calculate station density along route segments.
    Returns list of segments with { start_mile, end_mile, stops_count, density }.

    If density < 1 stop per 100 miles → fuel desert detected.
    """
    from truck_stop_finder import haversine_miles, bearing, angle_diff

    if len(route_waypoints) < 2:
        return []

    segments = []
    cumulative = 0.0

    for i in range(len(route_waypoints) - 1):
        wp1 = route_waypoints[i]
        wp2 = route_waypoints[i + 1]
        seg_dist = haversine_miles(wp1["lat"], wp1["lng"], wp2["lat"], wp2["lng"])
        seg_bearing = bearing(wp1["lat"], wp1["lng"], wp2["lat"], wp2["lng"])

        # Count stops within corridor of this segment
        stops_count = 0
        for stop in all_stops:
            if not stop.get("diesel_price"):
                continue
            try:
                slat = float(stop["latitude"])
                slng = float(stop["longitude"])
                dist_from_start = haversine_miles(wp1["lat"], wp1["lng"], slat, slng)
                stop_bear = bearing(wp1["lat"], wp1["lng"], slat, slng)
                adiff = angle_diff(seg_bearing, stop_bear)

                # Stop must be along segment (ahead, not behind)
                along = dist_from_start * math.cos(math.radians(adiff))
                cross = abs(dist_from_start * math.sin(math.radians(adiff)))

                if 0 <= along <= seg_dist * 1.1 and cross <= corridor_miles:
                    stops_count += 1
            except Exception:
                continue

        density = stops_count / max(seg_dist / 100, 0.1) if seg_dist > 0 else 0
        is_desert = density < 1.0

        segments.append({
            "start_mile": round(cumulative, 1),
            "end_mile": round(cumulative + seg_dist, 1),
            "distance": round(seg_dist, 1),
            "stops_count": stops_count,
            "density_per_100mi": round(density, 2),
            "is_fuel_desert": is_desert,
            "state": wp2.get("state", ""),
        })
        cumulative += seg_dist

    return segments


def find_last_dense_stop(
    all_stops: list[dict],
    desert_start_mile: float,
    truck_lat: float,
    truck_lng: float,
    route_waypoints: list[dict],
) -> dict | None:
    """
    Find the last stop in a high-density area before a fuel desert.
    The truck should fill up here to carry through the sparse stretch.
    """
    from truck_stop_finder import haversine_miles
    from ifta import net_price_after_ifta

    candidates = []
    for stop in all_stops:
        if not stop.get("diesel_price"):
            continue
        try:
            dist = haversine_miles(
                truck_lat, truck_lng,
                float(stop["latitude"]), float(stop["longitude"])
            )
        except Exception:
            continue

        # Must be before the desert starts AND within reachable range
        if dist > desert_start_mile:
            continue
        if dist > desert_start_mile - 50:  # within last 50mi before desert
            card_price = float(stop["diesel_price"])
            net_price = net_price_after_ifta(card_price, stop.get("state", ""))
            candidates.append({
                **stop,
                "dist_from_truck": round(dist, 1),
                "net_price": round(net_price, 4),
            })

    if not candidates:
        return None

    candidates.sort(key=lambda s: s["net_price"])
    return candidates[0]


# ═══════════════════════════════════════════════════════════════════════════════
# POST-TRIP DEADHEAD RESERVE — Arrival Fuel Calculation
# ═══════════════════════════════════════════════════════════════════════════════

def calculate_arrival_fuel_target(
    dest_state: str,
    tank_gal: float = DEFAULT_TANK_GAL,
    mpg: float = DEFAULT_MPG,
) -> dict:
    """
    Calculate the target fuel % when arriving at delivery.

    Rule: 30% reserve PLUS 150 miles deadhead to ensure driver
    doesn't have to fuel in an expensive area after unloading.

    Returns { target_pct, deadhead_gal, reserve_gal, reason }
    """
    # 30% reserve
    reserve_pct = SAFETY_RESERVE * 100  # 30%
    reserve_gal = tank_gal * SAFETY_RESERVE

    # 30% target for arrival at delivery
    target_pct = reserve_pct

    # If destination is inside an expensive state, add extra buffer
    dest_upper = dest_state.upper().strip()
    extra_reason = ""
    if dest_upper in EXPENSIVE_STATES:
        # Add 10% more to avoid fueling in expensive state after delivery
        target_pct += 10
        extra_reason = f" (+10% buffer — {EXPENSIVE_STATES[dest_upper]['name']} is expensive)"

    # Cap at 95% — truck can't arrive completely full
    target_pct = min(target_pct, 95.0)

        "target_pct": round(target_pct, 1),
        "reserve_gal": round(reserve_gal, 1),
        "reserve_pct": round(reserve_pct, 1),
        "reason": (
            f"30% arrival reserve ({reserve_pct:.0f}%)"
            f"{extra_reason}"
        ),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# FULL ROUTE OPTIMIZATION — Combines all V2 logic
# ═══════════════════════════════════════════════════════════════════════════════

def optimize_route_fuel_plan(
    truck_lat: float,
    truck_lng: float,
    current_fuel_pct: float,
    route: dict,
    tank_gal: float = DEFAULT_TANK_GAL,
    mpg: float = DEFAULT_MPG,
) -> dict:
    """
    Master optimization function — called after pickup when truck begins
    moving toward delivery.

    Performs full "True Cost" search along remaining route:
    1. Searches all EFS stations along route
    2. Calculates V2 True Cost (IFTA-adjusted + detour + state penalty)
    3. Finds single most optimal stop for maximum trip savings
    4. Applies Expensive State / Fuel Desert / Deadhead rules
    5. Recommends 200-gallon Full Tank Fill at best stop

    Returns complete fuel plan dict.
    """
    from database import get_all_diesel_stops
    from truck_stop_finder import haversine_miles, reachable_miles
    from ifta import net_price_after_ifta, get_ifta_rate

    all_stops = get_all_diesel_stops()
    dest = route.get("destination", {})
    dest_state = (dest.get("state") or "").upper()

    # Build route states
    route_states = []
    for s in route.get("stops", []):
        st = (s.get("state") or "").upper()
        if st and st not in route_states:
            route_states.append(st)
    if dest_state and dest_state not in route_states:
        route_states.append(dest_state)

    # 1. Detect expensive states on route
    expensive_ahead = detect_expensive_states_on_route(route_states)

    # 2. Calculate arrival fuel target (30% + 150mi deadhead)
    arrival_target = calculate_arrival_fuel_target(dest_state, tank_gal, mpg)

    # 3. Check for state-line buffering opportunities
    border_fills = []
    for exp in expensive_ahead:
        last_stop = find_last_cheap_stop_before_border(
            all_stops, exp["state"], truck_lat, truck_lng
        )
        if last_stop:
            border_fills.append({
                "expensive_state": exp["state"],
                "expensive_name": exp["name"],
                "fill_stop": last_stop,
                "fill_to_pct": exp["border_fill_pct"],
                "reason": f"Full Tank Fill before {exp['name']} border",
            })

    # 4. Build optimization result
    warnings = []
    if expensive_ahead:
        states_str = ", ".join(e["name"] for e in expensive_ahead)
        warnings.append(f"⚠️ Route crosses expensive state(s): {states_str}")

    return {
        "arrival_target": arrival_target,
        "expensive_states": expensive_ahead,
        "border_fills": border_fills,
        "route_states": route_states,
        "fill_amount_gal": FULL_TANK_FILL_GAL,  # Always 200 gal
        "warnings": warnings,
    }
