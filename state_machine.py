"""
state_machine.py  -  Core truck state logic.

V2 BUSINESS LOGIC:
  1. True Cost Search — triggered when truck finishes pickup, moving to delivery
  2. 30% Safety + 200-Gallon Rule — arrive at delivery with ≥30% fuel
  3. 40% Low Fuel Trigger — re-verify plan, fire alert with Google Maps link
  4. Emergency Check — can't reach rec stop? Find next-best immediately
  5. Wrong Stop / Missed Stop detection with financial loss calculation

STATES:
  HEALTHY          — fuel > 50%, poll every 30 min
  WATCH            — 40–50%, poll every 15 min
  CRITICAL_MOVING  — ≤40% and moving, poll every 10 min, alert fired
  CRITICAL_PARKED  — ≤40% and parked, poll every 60 min, alert fired once
  IN_YARD          — ignored entirely, poll every 30 min

ALERT RULES (moving):
  - First alert fires immediately when fuel drops below threshold
  - Re-alert every 30 min (ADVISORY/WARNING) or 10 min (CRITICAL/EMERGENCY)
  - Re-alert immediately on tier escalation or 5%+ fuel drop
  - Previous alert message deleted before new one sent

ALERT RULES (parked):
  - One alert fires immediately
  - Re-alert only if fuel drops 5%+ OR truck moves 1+ mile
  - Same spot + same fuel = silent

REFUEL DETECTION:
  - Fuel jumps 5%+ → refueled → close alert, send confirmation
  - Works for both sleeping and moving trucks
"""

import logging
from datetime import datetime, timedelta, timezone

from config import (
    FUEL_ALERT_THRESHOLD_PCT,
    POLL_INTERVAL_HEALTHY,
    POLL_INTERVAL_WATCH,
    POLL_INTERVAL_CRITICAL_MOVING,
    POLL_INTERVAL_CRITICAL_PARKED,
    DEFAULT_TANK_GAL,
    DEFAULT_MPG,
    DISPATCHER_GROUP_ID,
)
from yard_geofence import is_in_yard, get_yard_name
from truck_stop_finder import (
    find_best_stops, find_best_stops_on_route, calc_savings,
    get_urgency, find_current_stop, haversine_miles,
    find_emergency_stop, find_critical_radial_stop, gallons_to_fill,
)
from california import (
    should_send_ca_reminder,
    should_reset_ca_reminder,
    get_ca_avg_diesel_price,
    _dist_to_ca_border,
)
from telegram_bot import (
    send_low_fuel_alert,
    send_at_stop_alert,
    delete_message,
    send_ca_border_reminder,
    send_refueled_alert,
    send_left_yard_low_fuel,
    send_emergency_alert,
)
from database import (
    create_fuel_alert,
    resolve_alert,
    get_truck_config,
    get_all_diesel_stops,
)

log = logging.getLogger(__name__)

_MOVING_MPH      = 5     # below this = parked
_REFUEL_PCT      = 5.0   # fuel rise that triggers refuel detection
_PARKED_MOVE_MI  = 1.0   # miles moved to reset parked state
_ALERT_FUEL_DROP = 5.0   # fuel drop to force re-alert


def _utcnow():
    return datetime.now(timezone.utc)

def _next_poll(minutes):
    return _utcnow() + timedelta(minutes=minutes)

def _tz(dt):
    if dt is None:
        return None
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt)
        except Exception:
            return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


# -- State skeleton -----------------------------------------------------------

def _new_state(vid, data):
    return {
        "vehicle_id":             vid,
        "vehicle_name":           data["vehicle_name"],
        "state":                  "UNKNOWN",
        "fuel_pct":               data["fuel_pct"],
        "lat":                    data["lat"],
        "lng":                    data["lng"],
        "speed_mph":              data["speed_mph"],
        "heading":                data["heading"],
        "next_poll":              _utcnow(),
        "parked_since":           None,
        "alert_sent":             False,
        "overnight_alert_sent":   False,
        "open_alert_id":          None,
        "assigned_stop_id":       None,
        "assigned_stop_name":     None,
        "assigned_stop_lat":      None,
        "assigned_stop_lng":      None,
        "assigned_stop_net_price": None,
        "assigned_stop_card_price": None,
        "assignment_time":        None,
        "in_yard":                False,
        "yard_name":              None,
        "sleeping":               False,
        "fuel_when_parked":       None,
        "ca_reminder_sent":       False,
        "last_alert_time":        None,
        "last_alert_urgency":     None,
        "last_alert_fuel":        None,
        "last_alert_lat":         None,
        "last_alert_lng":         None,
        "last_alerted_fuel":      None,
        "prev_truck_group":                   None,
        "prev_truck_msg_id":                  None,
        "prev_dispatcher_msg_id":             None,
        "prev_emergency_truck_msg_id":        None,
        "prev_emergency_dispatcher_msg_id":   None,
        "prev_briefing_truck_msg_id":         None,
        "prev_briefing_dispatcher_msg_id":    None,
        "prev_ca_truck_msg_id":               None,
        "prev_ca_dispatcher_msg_id":          None,
        "missed_stop_name":               None,
        "missed_stop_card_price":         None,
        "missed_stop_net_price":          None,
        "low_fuel_flagged":               False,
        "assigned_stop_min_dist":         None,
        "assigned_stop_fill_instruction": None,
        "pending_wrong_stop":             None,
    }


def _clear_alert(state):
    state["open_alert_id"]        = None
    state["assigned_stop_id"]     = None
    state["assigned_stop_name"]   = None
    state["assigned_stop_lat"]    = None
    state["assigned_stop_lng"]    = None
    state["assigned_stop_net_price"] = None
    state["assigned_stop_card_price"] = None
    state["assignment_time"]      = None
    state["alert_sent"]           = False
    state["overnight_alert_sent"] = False
    state["fuel_when_parked"]     = None
    state["sleeping"]             = False
    state["last_alert_time"]      = None
    state["last_alert_urgency"]   = None
    state["last_alert_fuel"]      = None
    state["last_alert_lat"]       = None
    state["last_alert_lng"]       = None
    state["last_alerted_fuel"]    = None
    state["missed_stop_name"]               = None
    state["missed_stop_card_price"]         = None
    state["missed_stop_net_price"]          = None
    state["low_fuel_flagged"]               = False
    state["assigned_stop_min_dist"]         = None
    state["assigned_stop_fill_instruction"] = None
    state["pending_wrong_stop"]             = None


def _get_truck_params(vehicle_name: str) -> tuple[float, float]:
    """Return (tank_gal, mpg) — uses real Samsara MPG if available, else default."""
    from database import get_truck_params, get_truck_mpg
    try:
        params = get_truck_params(vehicle_name)
        tank = float(params.get("tank_gal") or DEFAULT_TANK_GAL) if params else DEFAULT_TANK_GAL
    except Exception:
        tank = DEFAULT_TANK_GAL
    try:
        # Get real MPG from Samsara data (updated every hour in background)
        from database import db_cursor
        with db_cursor() as cur:
            cur.execute(
                "SELECT mpg FROM truck_efficiency WHERE vehicle_name = %s AND mpg > 3",
                (vehicle_name,)
            )
            row = cur.fetchone()
            mpg = float(row["mpg"]) if row else DEFAULT_MPG
    except Exception:
        mpg = DEFAULT_MPG
    return tank, mpg


def _get_state_code(lat: float, lng: float) -> str | None:
    if 35.0 <= lat <= 42.0 and -120.1 <= lng <= -113.9:
        return "NV"
    if 31.3 <= lat <= 37.0 and -115.0 <= lng <= -109.0:
        return "AZ"
    if 41.9 <= lat <= 46.3 and -124.6 <= lng <= -116.3:
        return "OR"
    if 32.5 <= lat <= 42.0 and -124.5 <= lng <= -114.1:
        return "CA"
    return None


# -- Main entry point ---------------------------------------------------------

def process_truck(vid, prev_state, current_data, truck_states):
    fuel    = current_data["fuel_pct"]
    speed   = current_data["speed_mph"]
    lat     = current_data["lat"]
    lng     = current_data["lng"]
    heading = current_data["heading"]
    vname   = current_data["vehicle_name"]

    if vid not in truck_states:
        truck_states[vid] = _new_state(vid, current_data)

    state = truck_states[vid]

    # Update live fields
    state["vehicle_name"] = vname
    state["fuel_pct"]     = fuel
    state["lat"]          = lat
    state["lng"]          = lng
    state["speed_mph"]    = speed
    state["heading"]      = heading

    moving = speed > _MOVING_MPH
    tank_gal, mpg = _get_truck_params(vname)

    log.info(f"  {vname}: fuel={fuel:.1f}%  speed={speed:.0f}mph  "
             f"state={state.get('state','NEW')}  sleeping={state.get('sleeping',False)}")

    # ══════════════════════════════════════════════════════════════════════════
    # 0a. ROUTE BRIEFING — send once when trip goes dispatched → in_transit
    # ══════════════════════════════════════════════════════════════════════════
    route     = state.get("qm_route")
    # Use trip_num OR ref_number OR origin+dest as unique trip identifier
    route_id  = (
        (route or {}).get("trip_num") or
        (route or {}).get("ref_number") or
        (f"{(route or {}).get('origin',{}).get('city','')}_"
         f"{(route or {}).get('destination',{}).get('city','')}")
        if route else None
    )

    # Normalize status — QM may return variations
    raw_status    = (route or {}).get("status", "").lower().replace(" ", "_").replace("-", "_")
    curr_status   = raw_status
    is_in_transit = any(s in curr_status for s in ["in_transit", "intransit", "transit", "picked"])
    is_dispatched = any(s in curr_status for s in ["dispatched", "dispatch"])

    prev_status   = state.get("last_trip_status", "")
    briefing_sent = state.get("briefing_sent_trip")

    # Fire briefing when:
    # 1. Status just changed to in_transit (dispatched → in_transit)
    # 2. OR already in_transit but briefing was never sent for this trip
    #    (handles bot restarts mid-trip)
    should_brief = (
        route and route_id and
        is_in_transit and
        briefing_sent != route_id
    )

    # Always log briefing status for debugging
    log.info(
        f"  {vname}: briefing check — "
        f"route={'yes' if route else 'NO'} "
        f"route_id={route_id!r} "
        f"status={curr_status!r} "
        f"is_in_transit={is_in_transit} "
        f"briefing_sent={briefing_sent!r} "
        f"should_brief={should_brief}"
    )

    if should_brief:
        # Trip is in_transit and briefing not yet sent — send now
        try:
            from route_briefing import plan_route_briefing, format_route_briefing
            from telegram_bot import _send_to, _send_to_dispatcher
            from database import get_truck_group

            plan = plan_route_briefing(
                truck_lat=lat, truck_lng=lng,
                current_fuel_pct=fuel,
                tank_gal=tank_gal, mpg=mpg,
                route=route,
            )
            msg = format_route_briefing(plan, vname, route, fuel, mpg,
                                        driver_name=current_data.get("driver_name", ""))

            # Send to driver group + dispatcher only if a message was generated
            if msg:
                # Delete previous route briefing first (only briefings, not flags/emergencies)
                truck_group = get_truck_group(vname)
                prev_briefing_truck      = state.get("prev_briefing_truck_msg_id")
                prev_briefing_dispatcher = state.get("prev_briefing_dispatcher_msg_id")

                if truck_group and prev_briefing_truck:
                    delete_message(truck_group, prev_briefing_truck)
                if prev_briefing_dispatcher:
                    delete_message(str(DISPATCHER_GROUP_ID), prev_briefing_dispatcher)

                truck_msg_id      = _send_to(truck_group, msg) if truck_group else None
                dispatcher_msg_id = _send_to_dispatcher(msg)

                # Store message IDs so next briefing can delete these
                state["prev_briefing_truck_msg_id"]      = truck_msg_id
                state["prev_briefing_dispatcher_msg_id"] = dispatcher_msg_id

                # Send border warnings as separate alert if any
                border_warnings = plan.get("border_warnings", [])
                if border_warnings:
                    NL       = chr(10)
                    bw_title = f"⚠️ *State Border Alert — Truck {vname}*"
                    bw_msg   = bw_title + NL + NL.join(border_warnings)
                    if truck_group:
                        _send_to(truck_group, bw_msg)
                    _send_to_dispatcher(bw_msg)
            else:
                log.info(f"  {vname}: route briefing not needed (no message generated)")

            # Store planned stops so missed-stop detection works
            if plan.get("planned_stops"):
                next_stop = plan["planned_stops"][0]
                state["assigned_stop_name"]             = next_stop["store_name"]
                state["assigned_stop_lat"]              = next_stop.get("latitude") or next_stop.get("lat")
                state["assigned_stop_lng"]              = next_stop.get("longitude") or next_stop.get("lng")
                state["assigned_stop_dist"]             = next_stop.get("dist_from_truck", 0)
                state["assigned_stop_card_price"]       = next_stop.get("card_price") or next_stop.get("diesel_price")
                state["assigned_stop_net_price"]        = next_stop.get("net_price")
                state["assigned_stop_fill_instruction"] = next_stop.get("low_stop_warning") or "Full Tank Fill (200 Gallons)"
                state["all_planned_stops"]              = plan["planned_stops"]
                state["planned_stop_index"]             = 0
                state["assignment_time"]                = _utcnow()
                state["assigned_stop_min_dist"]         = float(next_stop.get("dist_from_truck") or 9999)
            else:
                state["all_planned_stops"]              = []
                state["planned_stop_index"]             = 0
                state["assigned_stop_name"]             = None
                state["assigned_stop_lat"]              = None
                state["assigned_stop_lng"]              = None
                state["assigned_stop_card_price"]       = None
                state["assigned_stop_net_price"]        = None
                state["assigned_stop_fill_instruction"] = None

            state["briefing_sent_trip"] = route_id
            try:
                from database import save_trip_state
                save_trip_state(vname, state)
                log.info(f"  {vname}: trip state persisted — trip {route_id}")
            except Exception as dbe:
                log.warning(f"  {vname}: trip state save failed: {dbe}")

            if plan.get("planned_stops"):
                log.info(f"  {vname}: route briefing sent — trip {route_id}, "
                         f"{plan['stops_needed']} stops planned, "
                         f"first stop: {next_stop['store_name']}")
            else:
                log.info(f"  {vname}: route briefing evaluated — trip {route_id}, "
                         f"no stops needed")
        except Exception as e:
            log.error(f"  {vname}: route briefing failed: {e}", exc_info=True)

    state["last_trip_status"] = curr_status  # normalized status

    # ══════════════════════════════════════════════════════════════════════════
    # 0b. RE-BRIEF after each delivery stop completion
    # When truck arrives at a delivery waypoint, send updated fuel plan
    # for remaining legs of the trip
    # ══════════════════════════════════════════════════════════════════════════
    if route:
        route_stops      = route.get("stops", [])
        completed_wps    = state.get("completed_waypoints", set())
        if not isinstance(completed_wps, set):
            completed_wps = set(completed_wps)

        for stop in route_stops:
            stop_id  = stop.get("id") or stop.get("address", "")
            stop_lat = stop.get("lat") or stop.get("latitude")
            stop_lng = stop.get("lng") or stop.get("longitude")
            if not stop_lat or not stop_lng or stop_id in completed_wps:
                continue
            from truck_stop_finder import haversine_miles as _hav
            dist_to_stop = _hav(lat, lng, float(stop_lat), float(stop_lng))
            if dist_to_stop < 0.5 and speed < 5:
                completed_wps.add(stop_id)
                state["completed_waypoints"] = completed_wps

                # Enforce 30% fuel minimum at delivery
                if fuel < FUEL_ALERT_THRESHOLD_PCT:
                    try:
                        from flag_system import flag_low_fuel
                        from database import get_truck_group
                        flag_low_fuel(
                            vehicle_name=vname,
                            truck_group_id=get_truck_group(vname),
                            fuel_pct=fuel,
                            truck_lat=lat,
                            truck_lng=lng,
                            planned_stop_name=f"Arrived at delivery with {fuel:.0f}% — below 30% minimum",
                        )
                        log.warning(f"  {vname}: arrived at delivery with {fuel:.0f}% fuel — flagged")
                    except Exception as dfe:
                        log.warning(f"  {vname}: delivery fuel flag failed: {dfe}")

                remaining = [s for s in route_stops
                             if (s.get("id") or s.get("address","")) not in completed_wps]
                if remaining or route.get("destination"):
                    try:
                        from route_briefing import plan_route_briefing, format_route_briefing
                        from telegram_bot import _send_to, _send_to_dispatcher
                        from database import get_truck_group, save_trip_state
                        updated_route = {**route, "stops": remaining}
                        plan = plan_route_briefing(lat, lng, fuel, tank_gal, mpg, updated_route)
                        msg  = format_route_briefing(plan, vname, updated_route, fuel, mpg,
                                                     driver_name=current_data.get("driver_name", ""))
                        hdr  = f"📍 *Delivery Complete — Truck {vname}*\nUpdated fuel plan for remaining route:\n\n"
                        # Delete previous briefing, send new one
                        truck_group = get_truck_group(vname)
                        prev_b_truck = state.get("prev_briefing_truck_msg_id")
                        prev_b_disp  = state.get("prev_briefing_dispatcher_msg_id")
                        if truck_group and prev_b_truck:
                            delete_message(truck_group, prev_b_truck)
                        if prev_b_disp:
                            delete_message(str(DISPATCHER_GROUP_ID), prev_b_disp)
                        tmid = _send_to(truck_group, hdr + msg) if truck_group else None
                        dmid = _send_to_dispatcher(hdr + msg)
                        state["prev_briefing_truck_msg_id"]      = tmid
                        state["prev_briefing_dispatcher_msg_id"] = dmid
                        state["briefing_sent_trip"] = route_id  # keep same trip ID
                        save_trip_state(vname, state)
                        log.info(f"  {vname}: re-briefed after delivery at {stop_id}")
                    except Exception as rbe:
                        log.error(f"  {vname}: re-brief failed: {rbe}", exc_info=True)
                break

    # ══════════════════════════════════════════════════════════════════════════
    # 0c. FUEL STOP GEOFENCE — track if truck enters/exits any fuel stop
    # ══════════════════════════════════════════════════════════════════════════
    try:
        current_stop_gf = find_current_stop(lat, lng)
        prev_stop_id    = state.get("at_stop_id")

        if current_stop_gf:
            stop_id   = current_stop_gf.get("id") or current_stop_gf.get("store_name")
            stop_name = current_stop_gf.get("store_name", "Unknown")

            if prev_stop_id != stop_id:
                # Truck just entered this stop
                log.info(f"  {vname}: 📍 entered fuel stop: {stop_name}")
                state["at_stop_id"]    = stop_id
                state["at_stop_name"]  = stop_name
                state["at_stop_since"] = _utcnow()
                state["at_stop_fuel"]  = fuel

                # Check if this is the recommended stop
                rec_name = state.get("assigned_stop_name")
                rec_lat  = state.get("assigned_stop_lat")
                rec_lng  = state.get("assigned_stop_lng")
                is_recommended = False
                if rec_lat and rec_lng:
                    dist_to_rec = haversine_miles(lat, lng, rec_lat, rec_lng)
                    visited     = dist_to_rec <= 0.5
                    is_recommended = visited
                    try:
                        from database import log_stop_visit
                        log_stop_visit(
                            vehicle_name=vname,
                            alert_id=state.get("open_alert_id"),
                            recommended_stop_name=rec_name,
                            recommended_lat=rec_lat, recommended_lng=rec_lng,
                            actual_stop_name=stop_name,
                            actual_lat=float(current_stop_gf.get("latitude", lat)),
                            actual_lng=float(current_stop_gf.get("longitude", lng)),
                            actual_stop_state=current_stop_gf.get("state"),
                            visited=visited,
                            fuel_before=fuel, fuel_after=fuel,
                        )
                        if visited:
                            log.info(f"  {vname}: ✅ entered RECOMMENDED stop {stop_name}")
                        else:
                            log.info(f"  {vname}: ⚠️ entered DIFFERENT stop {stop_name} (rec was {rec_name})")
                    except Exception as e:
                        log.warning(f"  {vname}: geofence visit log failed: {e}")

                # ── Entry alert — green if advised stop, red flag if wrong stop ──
                truck_group_cached = None
                try:
                    from telegram_bot import _send_to, _send_to_dispatcher
                    from database import get_truck_group
                    stop_city  = current_stop_gf.get("city", "")
                    stop_state = current_stop_gf.get("state", "")
                    raw_addr   = current_stop_gf.get("address", "").strip()
                    city_in_addr = stop_city and stop_city.upper() in raw_addr.upper()
                    addr_line  = raw_addr if city_in_addr else ", ".join(filter(None, [raw_addr, stop_city, stop_state]))
                    slat = current_stop_gf.get("latitude", lat)
                    slng = current_stop_gf.get("longitude", lng)
                    maps_url   = f"https://maps.google.com/?q={slat},{slng}"
                    card_price = current_stop_gf.get("diesel_price")
                    truck_group_cached = get_truck_group(vname)

                    if rec_name and is_recommended:
                        # ✅ Driver is at the advised stop
                        fill_instruction = (
                            state.get("assigned_stop_fill_instruction")
                            or "Full Tank Fill (200 Gallons)"
                        )
                        green_msg = "\n".join(filter(None, [
                            f"✅ CORRECT STOP — TRUCK {vname}",
                            "",
                            stop_name,
                            f"📌 {addr_line}" if addr_line else None,
                            f"🗺️ [Directions]({maps_url})",
                            f"⛽ Fuel: {fuel:.0f}%",
                            f"💳 Card: ${card_price:.3f}/gal" if card_price else None,
                            "",
                            "💧 INSTRUCTIONS:",
                            fill_instruction,
                        ]))
                        if truck_group_cached:
                            _send_to(truck_group_cached, green_msg)
                        _send_to_dispatcher(green_msg)

                    elif not rec_name:
                        # No planned stop — neutral entry notification to dispatcher only
                        entry_msg = "\n".join(filter(None, [
                            f"📍 *Truck {vname} — Entered Fuel Stop*",
                            stop_name,
                            f"📌 {addr_line}" if addr_line else None,
                            f"🗺️ [Directions]({maps_url})",
                        ]))
                        _send_to_dispatcher(entry_msg)
                        if truck_group_cached:
                            _send_to(truck_group_cached, entry_msg)
                    # Wrong stop: flag_wrong_stop below sends the red flag message
                except Exception as ne:
                    log.warning(f"  {vname}: stop entry notification failed: {ne}")

                # ── Wrong-stop / unplanned-stop flag ─────────────────────────
                try:
                    driver_name_gf = current_data.get("driver_name") or ""
                    from database import get_truck_group as _gtg, log_driver_flag

                    truck_group_gf = truck_group_cached or _gtg(vname)
                    rec_price_gf   = state.get("assigned_stop_card_price")
                    act_price_gf   = current_stop_gf.get("diesel_price")

                    if rec_name and not is_recommended:
                        # Driver is at wrong stop — wait for fuel to change before flagging.
                        # Driver may be sleeping or just passing through without refueling.
                        state["pending_wrong_stop"] = {
                            "stop_name":     stop_name,
                            "rec_name":      rec_name,
                            "rec_price":     float(rec_price_gf) if rec_price_gf else None,
                            "act_price":     float(act_price_gf) if act_price_gf else None,
                            "fuel_at_entry": fuel,
                            "driver_name":   driver_name_gf,
                            "truck_group":   truck_group_gf,
                        }
                        log.info(
                            f"  {vname}: ⏳ wrong stop pending — "
                            f"entered {stop_name}, plan was {rec_name}; "
                            f"will flag if fuel changes"
                        )

                    elif not rec_name and state.get("qm_route"):
                        # Truck is on an active route but stopped with no assignment
                        from flag_system import send_flag
                        flag_type_us = "UNPLANNED_STOP"
                        unplanned_msg = "\n".join(filter(None, [
                            f"🚩 *Unplanned Stop — Truck {vname}*",
                            f"👤 Driver: *{driver_name_gf}*" if driver_name_gf else None,
                            f"⛽ Entered: *{stop_name}*",
                            f"⛽ Fuel: {fuel:.0f}%",
                            "🛣 No fuel stop was assigned for current route.",
                            "Incident logged.",
                        ]))
                        log_driver_flag(
                            truck_id=vname,
                            driver_name=driver_name_gf,
                            flag_type=flag_type_us,
                            planned_stop=None,
                            actual_stop=stop_name,
                            fuel_pct=fuel,
                            details=f"Entered {stop_name} with no assigned stop on active route",
                        )
                        send_flag(vname, flag_type_us, unplanned_msg, truck_group_gf)
                        log.warning(
                            f"  {vname}: 🚩 UNPLANNED STOP flagged — "
                            f"entered {stop_name} with no assignment on active route"
                        )
                except Exception as fe:
                    log.warning(f"  {vname}: wrong-stop flag failed: {fe}")
        else:
            if prev_stop_id:
                stop_name_left = state.get("at_stop_name", "Unknown")
                fuel_at_entry  = state.get("at_stop_fuel", fuel)
                log.info(f"  {vname}: 🚗 left fuel stop: {stop_name_left} fuel {fuel_at_entry:.0f}%→{fuel:.0f}%")
                # Left without fueling — discard any pending wrong-stop flag
                if state.get("pending_wrong_stop") and fuel <= fuel_at_entry + _REFUEL_PCT:
                    log.info(f"  {vname}: pending wrong-stop cleared — left {stop_name_left} without refueling")
                    state["pending_wrong_stop"] = None
            state["at_stop_id"]    = None
            state["at_stop_name"]  = None
            state["at_stop_since"] = None
            state["at_stop_fuel"]  = None
    except Exception as e:
        log.warning(f"  {vname}: geofence check failed: {e}")

    # ══════════════════════════════════════════════════════════════════════════
    # 1. YARD CHECK — always first, silences everything
    # ══════════════════════════════════════════════════════════════════════════
    in_yard_now = is_in_yard(lat, lng)
    was_in_yard = state.get("in_yard", False)

    if in_yard_now:
        yard_name = get_yard_name(lat, lng)
        if not was_in_yard:
            log.info(f"  {vname}: entered yard: {yard_name}")
        state.update({
            "in_yard": True, "yard_name": yard_name,
            "state": "IN_YARD", "next_poll": _next_poll(30),
        })
        return

    if was_in_yard and not in_yard_now:
        yard_name = state.get("yard_name", "yard")
        log.info(f"  {vname}: left {yard_name} at {fuel:.1f}% fuel")
        state.update({"in_yard": False, "yard_name": None})
        if fuel <= FUEL_ALERT_THRESHOLD_PCT:
            send_left_yard_low_fuel(vname, fuel, yard_name)
            _fire_alert(vid, state, current_data, tank_gal, mpg, state_code="")
            return

    # ══════════════════════════════════════════════════════════════════════════
    # 2. CALIFORNIA BORDER REMINDER (checked every poll, independent of fuel)
    # ══════════════════════════════════════════════════════════════════════════
    state_code = _get_state_code(lat, lng)

    if should_reset_ca_reminder(state_code or "", fuel, heading,
                                 state.get("ca_reminder_sent", False)):
        log.info(f"  {vname}: CA reminder reset (state={state_code} fuel={fuel:.0f}%)")
        state["ca_reminder_sent"] = False

    # ── APPROACHING BORDER CHECK — warn when within 150 miles ────────────────
    route = state.get("qm_route")
    if route:
        try:
            from border_strategy import (
                analyze_route_borders, build_border_strategy,
                format_border_warnings, AVOID_FUEL_STATES, LOW_STOP_STATES
            )
            from truck_stop_finder import reachable_miles
            from database import get_all_diesel_stops

            waypoints = []
            for wp in route.get("stops", []):
                if wp.get("lat") and wp.get("lng"):
                    d = haversine_miles(lat, lng, float(wp["lat"]), float(wp["lng"]))
                    waypoints.append({**wp, "dist_from_truck": d})
            dest = route.get("destination", {})
            if dest.get("lat") and dest.get("lng"):
                d = haversine_miles(lat, lng, float(dest["lat"]), float(dest["lng"]))
                waypoints.append({**dest, "dist_from_truck": d})

            border_events = analyze_route_borders(waypoints, state_code or "")

            # Only warn about the NEXT upcoming border — not all borders on route
            upcoming = [e for e in border_events if e.dist_to_entry > 0]
            next_event = min(upcoming, key=lambda e: e.dist_to_entry) if upcoming else None

            for event in ([next_event] if next_event else []):
                key = f"border_warned_{event.state}"
                dist = event.dist_to_entry

                # Only warn if truck doesn't have enough fuel to cross the state
                range_miles  = reachable_miles(fuel, tank_gal, mpg)
                needs_fuel   = range_miles < (dist + event.exit_dist)
                # Fire only when the truck is actually close to the border
                # and still under the desired entry fuel threshold.
                if 0 < dist <= 100 and fuel < 70 and needs_fuel and not state.get(key):
                    all_stops = get_all_diesel_stops()
                    for s in all_stops:
                        s["dist_from_truck"] = haversine_miles(
                            lat, lng, float(s["latitude"]), float(s["longitude"])
                        )
                        from ifta import net_price_after_ifta
                        s["net_price"] = net_price_after_ifta(
                            float(s.get("diesel_price", 0)), s.get("state", "")
                        )

                    decisions = build_border_strategy(
                        fuel, tank_gal, mpg, [event], all_stops,
                        route_waypoints=waypoints,
                        truck_lat=lat,
                        truck_lng=lng,
                        truck_heading=heading,
                    )
                    lines = format_border_warnings(decisions, approaching_miles=100)

                    if lines:
                        from telegram_bot import _send_to, _send_to_dispatcher
                        from database import get_truck_group
                        truck_group = get_truck_group(vname)
                        line1  = f"⚠️ *Border Ahead — Truck {vname}*"
                        line2  = f"⛽ Fuel: {fuel:.0f}%  |  {dist:.0f} miles to {event.state_name}"
                        header = line1 + "\n" + line2 + "\n\n"
                        nl     = "\n"
                        msg    = header + nl.join(lines)
                        if truck_group:
                            _send_to(truck_group, msg)
                        _send_to_dispatcher(msg)
                        state[key] = True
                        # Persist immediately so restarts don't re-fire
                        try:
                            from database import save_trip_state
                            save_trip_state(vname, state)
                        except Exception:
                            pass
                        log.info(f"  {vname}: border warning sent — "
                                 f"{event.state_name} in {dist:.0f}mi")
        except Exception as be:
            log.warning(f"  {vname}: border check failed: {be}")

    # Check if truck just entered a low-stop state under-fueled
    if state_code and state_code != state.get("last_state_code"):
        from ifta import LOW_STOP_STATES, LOW_STOP_MIN_FUEL
        if state_code.upper() in LOW_STOP_STATES:
            min_fuel = LOW_STOP_MIN_FUEL.get(state_code.upper(), 50)
            if fuel < min_fuel:
                try:
                    from flag_system import flag_low_stop_state
                    from database import get_truck_group
                    info = LOW_STOP_STATES[state_code.upper()]
                    truck_group = get_truck_group(vname)
                    flag_low_stop_state(
                        vehicle_name=vname,
                        truck_group_id=truck_group,
                        state=state_code.upper(),
                        state_name=info["name"],
                        fuel_pct=fuel,
                        min_fuel=min_fuel,
                    )
                    log.warning(f"  {vname}: flagged low-stop state {state_code} at {fuel:.0f}%")
                except Exception as fe:
                    log.warning(f"  {vname}: flag_low_stop_state failed: {fe}")
    state["last_state_code"] = state_code

    # Get route destination state for CA crossing estimate
    _ca_route       = state.get("qm_route")
    _ca_dest_state  = (_ca_route.get("destination") or {}).get("state", "") if _ca_route else ""

    if should_send_ca_reminder(state_code or "", lat, lng, heading,
                                fuel, state.get("ca_reminder_sent", False),
                                tank_gal=tank_gal, mpg=mpg,
                                route_dest_state=_ca_dest_state):
        # Check QM route — only send if truck is actually going to CA
        route      = state.get("qm_route")
        going_to_ca = True  # default: send if no route info
        if route:
            dest_state = (route.get("destination") or {}).get("state", "").upper()
            if dest_state and dest_state != "CA":
                going_to_ca = False
                log.info(f"  {vname}: CA reminder suppressed — route dest is {dest_state}, not CA")
        if going_to_ca:
            _fire_ca_reminder(state, current_data, tank_gal, mpg, state_code=state_code or "")

    # ══════════════════════════════════════════════════════════════════════════
    # 3. FUEL IS FINE
    # ══════════════════════════════════════════════════════════════════════════
    if fuel > FUEL_ALERT_THRESHOLD_PCT:
        if state.get("open_alert_id"):
            log.info(f"  {vname}: fuel recovered to {fuel:.1f}% — closing alert")
            resolve_alert(state["open_alert_id"])
            _clear_alert(state)

        state["low_fuel_flagged"] = False

        if fuel > 50:
            state["state"]     = "HEALTHY"
            state["next_poll"] = _next_poll(POLL_INTERVAL_HEALTHY)
        else:
            state["state"]     = "WATCH"
            state["next_poll"] = _next_poll(
                POLL_INTERVAL_WATCH if moving else POLL_INTERVAL_HEALTHY
            )
        state["parked_since"] = None
        state["sleeping"]     = False
        return

    # ══════════════════════════════════════════════════════════════════════════
    # 3b. 40% LOW FUEL TRIGGER — re-verify plan and fire alert
    #     Per V2 spec: at 40% fuel, re-verify the plan and send Low Fuel Alert
    #     to Dispatcher and Driver groups with Google Maps link.
    # ══════════════════════════════════════════════════════════════════════════
    if fuel <= FUEL_ALERT_THRESHOLD_PCT and not state.get("low_fuel_flagged"):
        log.info(f"  {vname}: 40% fuel trigger — re-verifying plan")
        try:
            from flag_system import flag_low_fuel
            from database import get_truck_group
            truck_group = get_truck_group(vname)
            planned_stop = state.get("assigned_stop_name")
            flag_low_fuel(
                vehicle_name=vname,
                truck_group_id=truck_group,
                fuel_pct=fuel,
                truck_lat=lat,
                truck_lng=lng,
                planned_stop_name=planned_stop,
            )
            state["low_fuel_flagged"] = True

            # Emergency reachability check — can truck reach planned stop?
            planned_lat = state.get("assigned_stop_lat")
            planned_lng = state.get("assigned_stop_lng")
            if planned_lat and planned_lng:
                range_miles = (fuel / 100) * tank_gal * mpg * 0.85
                dist_to_planned = haversine_miles(
                    lat, lng, float(planned_lat), float(planned_lng)
                )
                if range_miles < dist_to_planned:
                    log.warning(
                        f"  {vname}: EMERGENCY — can't reach planned stop "
                        f"{state.get('assigned_stop_name')} "
                        f"({dist_to_planned:.0f}mi away, range={range_miles:.0f}mi)"
                    )
                    # Find next-best reachable stop — cheapest within actual range
                    best = find_emergency_stop(
                        truck_lat=lat, truck_lng=lng,
                        truck_heading=heading,
                        fuel_pct=fuel,
                        tank_gal=tank_gal,
                        mpg=mpg,
                        range_budget_miles=range_miles,
                        truck_state=state_code or "",
                    )
                    if best:
                        old_name = state.get("assigned_stop_name")
                        state["assigned_stop_name"] = best["store_name"]
                        state["assigned_stop_lat"]  = float(best["latitude"])
                        state["assigned_stop_lng"]  = float(best["longitude"])
                        state["assigned_stop_card_price"] = best.get("diesel_price")
                        log.info(
                            f"  {vname}: reassigned to reachable stop "
                            f"{best['store_name']} (was {old_name})"
                        )
                        # Fire emergency alert with new stop
                        result = send_emergency_alert(
                            vehicle_name=vname,
                            fuel_pct=fuel,
                            truck_lat=lat,
                            truck_lng=lng,
                            heading=heading,
                            speed_mph=speed,
                            best_stop=best,
                            planned_stop_name=old_name,
                            range_miles=range_miles,
                            gps_stale=current_data.get("gps_stale", False),
                        )
                        if isinstance(result, dict):
                            state["prev_truck_group"]  = result.get("truck_group")
                            state["prev_truck_msg_id"] = result.get("truck_msg_id")
                            state["prev_dispatcher_msg_id"] = result.get("dispatcher_msg_id")
                        state["alert_sent"] = True
        except Exception as lfe:
            log.warning(f"  {vname}: low fuel flag failed: {lfe}")

    # ══════════════════════════════════════════════════════════════════════════
    # 4. FUEL IS LOW
    # ══════════════════════════════════════════════════════════════════════════
    was_sleeping = state.get("sleeping", False)

    # ── 4a. REFUEL CHECK (both moving and waking) ─────────────────────────────
    prev_fuel = state.get("fuel_pct", fuel)
    if fuel >= prev_fuel + _REFUEL_PCT:
        gallons_added = round(tank_gal * (fuel - prev_fuel) / 100, 1)
        log.info(f"  {vname}: refueled — {prev_fuel:.0f}%→{fuel:.0f}% "
                 f"(~{gallons_added:.0f} gal)")

        # ── Find which stop truck fueled at ─────────────────────────────────
        rec_lat   = state.get("assigned_stop_lat")
        rec_lng   = state.get("assigned_stop_lng")
        rec_name  = state.get("assigned_stop_name")

        actual_stop = None
        try:
            actual_stop = find_current_stop(lat, lng)

            # If not found at current GPS — check location history
            # (truck may have already left the stop)
            if not actual_stop:
                try:
                    from samsara_client import get_vehicle_location_history
                    history = get_vehicle_location_history(vid, hours_back=1)
                    for point in reversed(history):
                        stop_check = find_current_stop(point["lat"], point["lng"])
                        if stop_check:
                            actual_stop = stop_check
                            log.info(f"  {vname}: found refuel stop via history: "
                                     f"{stop_check['store_name']}")
                            break
                except Exception as he:
                    log.warning(f"  {vname}: history lookup failed: {he}")

            actual_name = actual_stop["store_name"] if actual_stop else "Unknown stop"
            actual_lat  = float(actual_stop["latitude"])  if actual_stop else lat
            actual_lng  = float(actual_stop["longitude"]) if actual_stop else lng
            card_price  = float(actual_stop.get("diesel_price", 0)) if actual_stop else 0
            retail      = float(actual_stop.get("retail_price", 0)) if actual_stop else 0

        except Exception:
            actual_name = "Unknown stop"
            actual_lat  = lat
            actual_lng  = lng
            card_price  = 0
            retail      = 0

        # ── Was this planned or unplanned? ───────────────────────────────────
        is_planned = False
        if rec_lat and rec_lng:
            dist_to_rec = haversine_miles(lat, lng,
                                          float(rec_lat), float(rec_lng))
            is_planned  = dist_to_rec <= 2.0

        # ── UNPLANNED REFUEL — driver fueled without bot knowing ─────────────
        if not rec_name:
            # No planned stop — driver fueled on their own (rest stop, etc.)
            total_paid = round(card_price * gallons_added, 2) if card_price else 0
            msg = (
                f"⛽ *Unplanned Refuel Detected — Truck {vname}*\n"
                f"Fuel: {prev_fuel:.0f}% → *{fuel:.0f}%*\n"
                f"📍 *{actual_name}*\n"
            )
            if actual_stop:
                city  = actual_stop.get("city", "")
                state_code = actual_stop.get("state", "")
                msg += f"📌 {city}, {state_code}\n"
            if card_price:
                msg += f"💳 Card: ${card_price:.3f}/gal\n"
            if retail and retail != card_price:
                msg += f"💰 Retail: ${retail:.3f}/gal\n"
            msg += f"⛽ Est. ~{gallons_added:.0f} gal added"
            if total_paid:
                msg += f" = *${total_paid:.0f}*"
            msg += "\n\n📋 Logged. Route plan updated."

            # Send to dispatcher only — informational, not a flag
            from telegram_bot import _send_to_dispatcher, _send_to
            from database import get_truck_group
            truck_group = get_truck_group(vname)
            if truck_group:
                _send_to(truck_group, msg)
            _send_to_dispatcher(msg)
            log.info(f"  {vname}: unplanned refuel at {actual_name} "
                     f"({prev_fuel:.0f}%→{fuel:.0f}%, ~{gallons_added:.0f}gal)")

        else:
            # ── PLANNED REFUEL — check if right or wrong stop ────────────────
            try:
                from database import log_stop_visit
                log_stop_visit(
                    vehicle_name=vname,
                    alert_id=state.get("open_alert_id"),
                    recommended_stop_name=rec_name,
                    recommended_lat=rec_lat, recommended_lng=rec_lng,
                    actual_stop_name=actual_name,
                    actual_lat=actual_lat, actual_lng=actual_lng,
                    actual_stop_state=actual_stop.get("state") if actual_stop else None,
                    visited=is_planned,
                    fuel_before=prev_fuel, fuel_after=fuel,
                )
                if is_planned:
                    log.info(f"  {vname}: ✅ visited recommended stop {rec_name}")
                    state["pending_wrong_stop"] = None
                else:
                    log.info(f"  {vname}: ⚠️ wrong stop — fueled at {actual_name}")
                    try:
                        from flag_system import flag_wrong_stop
                        from database import get_truck_group
                        pws = state.get("pending_wrong_stop") or {}
                        truck_group = pws.get("truck_group") or get_truck_group(vname)
                        rec_card_price_val = state.get("assigned_stop_card_price")
                        flag_wrong_stop(
                            vehicle_name=vname,
                            truck_group_id=truck_group,
                            recommended=rec_name or "Unknown",
                            actual=actual_name,
                            fuel_before=pws.get("fuel_at_entry") or prev_fuel,
                            fuel_after=fuel,
                            rec_card_price=float(rec_card_price_val) if rec_card_price_val else None,
                            actual_card_price=card_price if card_price else None,
                            driver_name=pws.get("driver_name") or current_data.get("driver_name", ""),
                            gallons_to_fill=gallons_added,
                        )
                        state["pending_wrong_stop"] = None
                    except Exception as fe:
                        log.warning(f"  {vname}: flag_wrong_stop failed: {fe}")
            except Exception as e:
                log.warning(f"  {vname}: stop visit logging failed: {e}")

        # ── Calculate REAL savings lost if driver missed a recommended stop ────
        missed_stop_name      = state.get("missed_stop_name")
        missed_stop_price     = state.get("missed_stop_card_price")
        missed_stop_net_price = state.get("missed_stop_net_price")
        actual_net_price      = card_price
        if actual_stop and card_price:
            try:
                from ifta import net_price_after_ifta
                actual_net_price = net_price_after_ifta(card_price, actual_stop.get("state", ""))
            except Exception:
                actual_net_price = card_price

        if missed_stop_name and missed_stop_price and card_price and gallons_added > 0:
            compare_rec_price = missed_stop_net_price if missed_stop_net_price is not None else missed_stop_price
            compare_act_price = actual_net_price if missed_stop_net_price is not None else card_price
            real_loss = round((compare_act_price - compare_rec_price) * gallons_added, 2)
            if real_loss > 0:
                from telegram_bot import _send_to, _send_to_dispatcher
                from database import get_truck_group, db_cursor
                truck_group = get_truck_group(vname)
                price_label = "Net after IFTA" if missed_stop_net_price is not None else "Card price"
                loss_msg = "\n".join([
                    f"🚩 *Updated Flag — Truck {vname}*",
                    "🧾 Type: *Missed Stop Loss Update*",
                    f"❌ Missed stop: *{missed_stop_name}* → ${missed_stop_price:.3f}/gal",
                    f"✅ Fueled at: *{actual_name}* → ${card_price:.3f}/gal",
                    f"⛽ Filled: {gallons_added:.0f} gal",
                    f"💸 *Real savings lost: ${real_loss:.2f}*",
                    f"📊 {price_label}: (${compare_act_price:.3f} - ${compare_rec_price:.3f}) × {gallons_added:.0f} gal",
                ])
                if truck_group:
                    _send_to(truck_group, loss_msg)
                _send_to_dispatcher(loss_msg)
                try:
                    with db_cursor() as cur:
                        cur.execute("""
                            UPDATE driver_flags
                            SET savings_lost = %s, actual_stop = %s
                            WHERE vehicle_name = %s
                              AND flag_type = 'MISSED_STOP'
                              AND savings_lost IS NULL
                              AND flagged_at >= NOW() - INTERVAL '24 hours'
                        """, (real_loss, actual_name, vname))
                except Exception as de:
                    log.warning(f"  {vname}: flag update failed: {de}")
                log.info(f"  {vname}: real loss ${real_loss:.2f} "
                         f"({missed_stop_name} → {actual_name})")
            state["missed_stop_name"]       = None
            state["missed_stop_card_price"] = None
            state["missed_stop_net_price"]  = None

        # ── Advance to next planned stop ─────────────────────────────────────
        all_planned = state.get("all_planned_stops", [])
        cur_idx     = state.get("planned_stop_index", 0)
        next_idx    = cur_idx + 1
        next_planned_stop = None
        if next_idx < len(all_planned):
            next_stop = all_planned[next_idx]
            next_planned_stop = next_stop
            state["assigned_stop_name"]             = next_stop["store_name"]
            state["assigned_stop_lat"]              = next_stop.get("latitude")
            state["assigned_stop_lng"]              = next_stop.get("longitude")
            state["assigned_stop_card_price"]       = next_stop.get("card_price") or next_stop.get("diesel_price")
            state["assigned_stop_net_price"]        = next_stop.get("net_price")
            state["assigned_stop_fill_instruction"] = next_stop.get("low_stop_warning") or "Full Tank Fill (200 Gallons)"
            state["planned_stop_index"]             = next_idx
            state["assignment_time"]                = _utcnow()
            ns_lat2 = float(next_stop.get("latitude") or lat)
            ns_lng2 = float(next_stop.get("longitude") or lng)
            state["assigned_stop_min_dist"] = haversine_miles(lat, lng, ns_lat2, ns_lng2)
            log.info(f"  {vname}: next planned stop → {next_stop['store_name']}")
        else:
            state["assigned_stop_name"]             = None
            state["assigned_stop_lat"]              = None
            state["assigned_stop_lng"]              = None
            state["assigned_stop_card_price"]       = None
            state["assigned_stop_net_price"]        = None
            state["assigned_stop_fill_instruction"] = None

        if state.get("open_alert_id"):
            resolve_alert(state["open_alert_id"])

        stop_name = actual_name
        send_refueled_alert(vname, stop_name, fuel,
                            truck_lat=lat, truck_lng=lng,
                            actual_stop=actual_stop)
        _clear_alert(state)
        if next_planned_stop:
            state["assigned_stop_name"] = next_planned_stop["store_name"]
            state["assigned_stop_lat"]  = next_planned_stop.get("latitude")
            state["assigned_stop_lng"]  = next_planned_stop.get("longitude")
            state["assigned_stop_card_price"] = next_planned_stop.get("card_price") or next_planned_stop.get("diesel_price")
        state["state"]     = "HEALTHY" if fuel > FUEL_ALERT_THRESHOLD_PCT else "WATCH"
        state["next_poll"] = _next_poll(POLL_INTERVAL_HEALTHY)
        try:
            from database import save_trip_state
            save_trip_state(vname, state)
        except Exception as dbe:
            log.warning(f"  {vname}: trip state save after refuel failed: {dbe}")
        return

    # ── 4b. WOKE UP (was parked, now moving) ──────────────────────────────────
    if was_sleeping and moving:
        fuel_when_parked = state.get("fuel_when_parked") or fuel
        log.info(f"  {vname}: woke up — {fuel_when_parked:.1f}%→{fuel:.1f}%")
        state.update({
            "sleeping": False, "fuel_when_parked": None,
            "parked_since": None, "last_alerted_fuel": None,
        })
        # Fresh alert with current heading
        if state.get("open_alert_id"):
            resolve_alert(state["open_alert_id"])
        _clear_alert(state)
        state["state"]     = "CRITICAL_MOVING"
        state["next_poll"] = _next_poll(POLL_INTERVAL_CRITICAL_MOVING)
        _fire_alert(vid, state, current_data, tank_gal, mpg, state_code=state_code or "")
        return

    # ── 4c. MOVING + LOW FUEL ─────────────────────────────────────────────────
    if moving:
        state["state"]        = "CRITICAL_MOVING"
        state["next_poll"]    = _next_poll(POLL_INTERVAL_CRITICAL_MOVING)
        state["parked_since"] = None
        state["sleeping"]     = False

        current_urgency = get_urgency(fuel)
        last_urgency    = state.get("last_alert_urgency")
        urgency_order   = {"ADVISORY": 0, "WARNING": 1, "CRITICAL": 2, "EMERGENCY": 3}

        tier_escalated = (
            last_urgency is not None and
            urgency_order.get(current_urgency, 0) > urgency_order.get(last_urgency, 0)
        )

        # Time since last alert
        last_alert_time = _tz(state.get("last_alert_time"))
        minutes_since   = (
            (_utcnow() - last_alert_time).total_seconds() / 60
            if last_alert_time else 9999
        )
        # 30% - 20%: every 30 min | 20% - 10%: every 10 min
        # 35-20%: alert every 20 min | below 20%: alert every 10 min
        time_threshold  = 10 if fuel <= 20 else 20
        time_elapsed    = minutes_since >= time_threshold

        # Fuel drop since last alert
        last_alert_fuel = state.get("last_alert_fuel")
        fuel_dropped    = (
            last_alert_fuel is not None and
            fuel <= last_alert_fuel - _ALERT_FUEL_DROP
        )

        # Check if truck passed its assigned stop without stopping
        passed_assigned_stop = False
        assigned_lat = state.get("assigned_stop_lat")
        assigned_lng = state.get("assigned_stop_lng")
        if assigned_lat and assigned_lng:
            dist_to_stop = haversine_miles(lat, lng, assigned_lat, assigned_lng)
            # Track minimum approach distance to detect when truck passes without stopping
            min_dist_so_far = state.get("assigned_stop_min_dist")
            if min_dist_so_far is None or dist_to_stop < min_dist_so_far:
                state["assigned_stop_min_dist"] = dist_to_stop
                min_dist_so_far = dist_to_stop

            assignment_time = _tz(state.get("assignment_time"))
            minutes_since_assign = (
                (_utcnow() - assignment_time).total_seconds() / 60
                if assignment_time else 0
            )
            # Truck passed the stop if it was within 5 miles then moved 10+ miles away
            was_near_stop = min_dist_so_far is not None and min_dist_so_far < 5.0
            # Emergency fallback for alert_sent stops without approach tracking
            emergency_check = state.get("alert_sent") and dist_to_stop > 5 and minutes_since_assign > 10
            if (was_near_stop and dist_to_stop > 10) or emergency_check:
                passed_assigned_stop = True
                log.info(f"  {vname}: passed assigned stop ({dist_to_stop:.1f} mi away, min was {min_dist_so_far:.1f} mi) — finding next stop")
                # Flag missed stop
                try:
                    from flag_system import flag_missed_stop
                    from database import get_truck_group
                    truck_group = get_truck_group(vname)
                    # Get card price from planned stop if available
                    planned_stops = state.get("all_planned_stops", [])
                    planned_idx   = state.get("planned_stop_index", 0)
                    card_price    = None
                    if planned_stops and planned_idx < len(planned_stops):
                        ps         = planned_stops[planned_idx]
                        card_price = ps.get("card_price") or ps.get("diesel_price")
                    # Fallback — use last known stop price from state
                    if not card_price:
                        card_price = state.get("assigned_stop_card_price")

                    # Get net_price (IFTA-adjusted) from planned stop
                    net_price = None
                    if planned_stops and planned_idx < len(planned_stops):
                        ps        = planned_stops[planned_idx]
                        net_price = ps.get("net_price")
                    if net_price is None:
                        net_price = state.get("assigned_stop_net_price")

                    flag_missed_stop(
                        vehicle_name=vname,
                        truck_group_id=truck_group,
                        stop_name=state.get("assigned_stop_name", "Unknown"),
                        dist_past=dist_to_stop,
                        fuel_pct=fuel,
                        tank_gal=tank_gal,
                        card_price=card_price,
                        net_price=net_price,
                    )
                    # Save for real loss calculation when driver fuels elsewhere
                    state["missed_stop_name"]       = state.get("assigned_stop_name")
                    state["missed_stop_card_price"] = card_price
                    state["missed_stop_net_price"]  = net_price
                    try:
                        from database import save_trip_state
                        save_trip_state(vname, state)
                    except Exception: pass
                except Exception as fe:
                    log.warning(f"  {vname}: flag_missed_stop failed: {fe}")
                # Advance to next planned stop
                all_planned  = state.get("all_planned_stops", [])
                current_idx  = state.get("planned_stop_index", 0)
                next_idx     = current_idx + 1
                if next_idx < len(all_planned):
                    next_stop = all_planned[next_idx]
                    state["assigned_stop_name"]             = next_stop["store_name"]
                    state["assigned_stop_lat"]              = next_stop.get("latitude") or next_stop.get("lat")
                    state["assigned_stop_lng"]              = next_stop.get("longitude") or next_stop.get("lng")
                    state["assigned_stop_card_price"]       = next_stop.get("card_price") or next_stop.get("diesel_price")
                    state["assigned_stop_net_price"]        = next_stop.get("net_price")
                    state["assigned_stop_fill_instruction"] = next_stop.get("low_stop_warning") or "Full Tank Fill (200 Gallons)"
                    state["planned_stop_index"]             = next_idx
                    state["assignment_time"]                = _utcnow()
                    ns_lat = float(next_stop.get("latitude") or next_stop.get("lat") or lat)
                    ns_lng = float(next_stop.get("longitude") or next_stop.get("lng") or lng)
                    state["assigned_stop_min_dist"]         = haversine_miles(lat, lng, ns_lat, ns_lng)
                    log.info(f"  {vname}: advanced to next planned stop: {next_stop['store_name']}")

                    # Send next stop alert to driver + dispatcher
                    try:
                        from route_briefing import format_next_stop
                        from telegram_bot import _send_to, _send_to_dispatcher
                        from database import get_truck_group
                        total_stops = len(state.get("all_planned_stops", []))
                        msg = format_next_stop(
                            stop=next_stop,
                            stop_num=next_idx + 1,
                            total_stops=total_stops,
                            truck_name=vname,
                            current_fuel_pct=fuel,
                            tank_gal=tank_gal,
                            driver_name=current_data.get("driver_name", ""),
                        )
                        truck_group = get_truck_group(vname)
                        if truck_group:
                            _send_to(truck_group, msg)
                        _send_to_dispatcher(msg)
                        log.info(f"  {vname}: next stop sent — {next_stop['store_name']}")
                    except Exception as nse:
                        log.warning(f"  {vname}: next stop alert failed: {nse}")
                else:
                    state["assigned_stop_name"]       = None
                    state["assigned_stop_lat"]        = None
                    state["assigned_stop_lng"]        = None
                    state["assigned_stop_card_price"] = None
                    state["assigned_stop_net_price"]  = None
                    state["planned_stop_index"]       = 0
                # Persist updated stop index to DB
                try:
                    from database import save_trip_state
                    save_trip_state(vname, state)
                except Exception: pass

        should_alert = (
            not state.get("alert_sent")
            or tier_escalated
            or time_elapsed
            or fuel_dropped
            or passed_assigned_stop
        )

        if should_alert:
            if state.get("alert_sent"):
                if passed_assigned_stop:
                    reason = "passed assigned stop"
                elif tier_escalated:
                    reason = f"tier {last_urgency}→{current_urgency}"
                elif fuel_dropped:
                    reason = f"fuel dropped {last_alert_fuel:.0f}%→{fuel:.0f}%"
                else:
                    reason = f"{minutes_since:.0f}min since last alert"
                log.info(f"  {vname}: re-alert — {reason}")
            _fire_alert(vid, state, current_data, tank_gal, mpg, state_code=state_code or "")
            state["last_alert_urgency"] = current_urgency
            state["last_alert_time"]    = _utcnow()
            state["last_alert_lat"]     = lat
            state["last_alert_lng"]     = lng
            state["last_alert_fuel"]    = fuel
        else:
            log.info(f"  {vname}: moving, skipping alert — "
                     f"{minutes_since:.0f}min ago, urgency={current_urgency}")
        return

    # ── 4d. PARKED + LOW FUEL ─────────────────────────────────────────────────
    was_parked   = state.get("parked_since") is not None
    last_park_lat = state.get("last_alert_lat")
    last_park_lng = state.get("last_alert_lng")

    # Check if truck moved to a new spot since last alert
    if was_parked and last_park_lat and last_park_lng:
        moved = haversine_miles(last_park_lat, last_park_lng, lat, lng)
        if moved > _PARKED_MOVE_MI:
            log.info(f"  {vname}: re-parked at new spot ({moved:.1f}mi) — reset sleep")
            state["parked_since"]         = None
            state["overnight_alert_sent"] = False
            state["last_alerted_fuel"]    = None
            state["sleeping"]             = False

    if not state.get("parked_since"):
        state["parked_since"]     = _utcnow()
        state["fuel_when_parked"] = fuel
        log.info(f"  {vname}: parked at {fuel:.1f}% — sleep mode")

    state["state"]    = "CRITICAL_PARKED"
    state["sleeping"] = True

    # Poll fast initially to confirm parked, then slow down
    parked_since   = _tz(state.get("parked_since"))
    parked_minutes = (
        (_utcnow() - parked_since).total_seconds() / 60
        if parked_since else 0
    )
    state["next_poll"] = _next_poll(
        POLL_INTERVAL_CRITICAL_MOVING if parked_minutes < 30
        else POLL_INTERVAL_CRITICAL_PARKED
    )

    already_alerted = state.get("overnight_alert_sent", False)

    moved_since_alert = 0.0
    if last_park_lat and last_park_lng:
        moved_since_alert = haversine_miles(last_park_lat, last_park_lng, lat, lng)
    location_changed = moved_since_alert >= _PARKED_MOVE_MI

    # Re-alert only on first park or when truck moves to a new spot.
    # Do NOT re-alert on fuel drop alone — driver may be sleeping.
    if not already_alerted or location_changed:
        if already_alerted:
            log.info(f"  {vname}: parked re-alert — moved {moved_since_alert:.1f}mi")
        _fire_alert(vid, state, current_data, tank_gal, mpg, state_code=state_code or "")
        state["overnight_alert_sent"] = True
        state["last_alerted_fuel"]    = fuel
        state["last_alert_lat"]       = lat
        state["last_alert_lng"]       = lng
    else:
        log.info(f"  {vname}: parked, skipping — same spot, waiting for truck to roll")


# -- Alert firing -------------------------------------------------------------

def _fire_alert(vid, state, data, tank_gal, mpg, state_code=""):
    """
    EMERGENCY ONLY alert system.

    Normal low fuel alerts removed — driver already has the route briefing
    with all planned stops. Only fires when:
      1. Truck cannot reach the planned stop on current fuel
      2. Fuel drops below 15% with no plan
      3. Truck is parked at a fuel stop (at-stop reminder)
    """
    vname   = data["vehicle_name"]
    fuel    = data["fuel_pct"]
    lat     = data["lat"]
    lng     = data["lng"]
    speed   = data["speed_mph"]
    heading = data["heading"]

    # Correct heading from movement
    prev_lat = state.get("lat")
    prev_lng = state.get("lng")
    if (prev_lat and prev_lng and speed > 10 and
            (abs(lat - prev_lat) > 0.001 or abs(lng - prev_lng) > 0.001)):
        from truck_stop_finder import bearing as calc_bearing
        heading = calc_bearing(prev_lat, prev_lng, lat, lng)

    # ── Case 1: Truck is AT a fuel stop — send at-stop reminder ──────────────
    current_stop = find_current_stop(lat, lng) if speed < 3 else None
    if current_stop:
        log.info(f"  {vname}: at stop {current_stop['store_name']} — at-stop alert")
        result = send_at_stop_alert(vname, fuel, lat, lng, current_stop)
        if isinstance(result, dict):
            state["prev_truck_group"]       = result.get("truck_group")
            state["prev_truck_msg_id"]      = result.get("truck_msg_id")
            state["prev_dispatcher_msg_id"] = result.get("dispatcher_msg_id")
        state["alert_sent"] = True
        return

    # ── Case 2: Check if truck can reach planned stop ─────────────────────────
    planned_stop_name = state.get("assigned_stop_name")
    planned_stop_lat  = state.get("assigned_stop_lat")
    planned_stop_lng  = state.get("assigned_stop_lng")
    range_miles       = (fuel / 100) * tank_gal * mpg * 0.85

    if planned_stop_lat and planned_stop_lng:
        dist_to_planned = haversine_miles(lat, lng,
                                          float(planned_stop_lat),
                                          float(planned_stop_lng))
        if range_miles >= dist_to_planned:
            # ✅ Still on track — no emergency
            log.info(f"  {vname}: on track to {planned_stop_name} "
                     f"({dist_to_planned:.0f}mi), range={range_miles:.0f}mi — no alert")
            return

    # ── Case 3: EMERGENCY — can't reach planned stop or fuel <= 20% ──────────
    log.warning(f"  {vname}: EMERGENCY — fuel={fuel:.0f}% range={range_miles:.0f}mi")

    # NOTE: We do NOT delete previous messages from driver chat
    # All alerts stay visible in chat history for accountability

    # At ≤20% fuel: pure radial search (50mi, no directional/CA filters, nearest wins)
    # Above 20%: price-optimised search within actual range budget
    if fuel <= 20:
        log.warning(f"  {vname}: CRITICAL override ≤20% — radial 50mi search, no filters")
        best = find_critical_radial_stop(truck_lat=lat, truck_lng=lng, radius_miles=50.0)
        if best is None:
            # Widen to 100mi if nothing in 50
            best = find_critical_radial_stop(truck_lat=lat, truck_lng=lng, radius_miles=100.0)
    else:
        best = find_emergency_stop(
            truck_lat=lat, truck_lng=lng,
            truck_heading=heading,
            fuel_pct=fuel,
            tank_gal=tank_gal,
            mpg=mpg,
            range_budget_miles=range_miles,
            truck_state=state_code or "",
        )

    # Log to DB
    alert_id = create_fuel_alert(
        vid, vname, fuel, lat, lng, heading, speed,
        alert_type="emergency",
        best_stop=best, alt_stop=None, savings_usd=None,
    )
    state["open_alert_id"] = alert_id

    if best:
        state["assigned_stop_name"] = best["store_name"]
        state["assigned_stop_lat"]  = float(best["latitude"])
        state["assigned_stop_lng"]  = float(best["longitude"])
        state["assignment_time"]    = _utcnow()

    # Delete previous emergency alert before sending new one
    prev_emg_truck      = state.get("prev_emergency_truck_msg_id")
    prev_emg_dispatcher = state.get("prev_emergency_dispatcher_msg_id")
    prev_truck_group    = state.get("prev_truck_group")

    if prev_truck_group and prev_emg_truck:
        delete_message(prev_truck_group, prev_emg_truck)
    if prev_emg_dispatcher:
        delete_message(str(DISPATCHER_GROUP_ID), prev_emg_dispatcher)

    # Send emergency alert
    result = send_emergency_alert(
        vehicle_name=vname,
        fuel_pct=fuel,
        truck_lat=lat,
        truck_lng=lng,
        heading=heading,
        speed_mph=speed,
        best_stop=best,
        planned_stop_name=planned_stop_name,
        range_miles=range_miles,
        gps_stale=data.get("gps_stale", False),
    )

    if isinstance(result, dict):
        state["prev_truck_group"]                = result.get("truck_group")
        state["prev_emergency_truck_msg_id"]     = result.get("truck_msg_id")
        state["prev_emergency_dispatcher_msg_id"] = result.get("dispatcher_msg_id")
        # Keep old keys for compatibility
        state["prev_truck_msg_id"]      = result.get("truck_msg_id")
        state["prev_dispatcher_msg_id"] = result.get("dispatcher_msg_id")

    state["alert_sent"] = True


def _fire_ca_reminder(state, data, tank_gal, mpg, state_code=""):
    """Send California border reminder."""
    vid     = state.get("vehicle_id")
    vname   = data["vehicle_name"]
    fuel    = data["fuel_pct"]
    lat     = data["lat"]
    lng     = data["lng"]
    heading = data["heading"]
    speed   = data["speed_mph"]

    log.info(f"  {vname}: sending CA border reminder")

    best, _     = find_best_stops(lat, lng, heading, speed, fuel, tank_gal, mpg, truck_state=state_code or "")
    all_stops   = get_all_diesel_stops()
    ca_avg      = get_ca_avg_diesel_price(all_stops)
    dist_border = _dist_to_ca_border(lat, lng)

    # Keep all CA alerts in chat — not deleting
    prev_ca_truck      = state.get("prev_ca_truck_msg_id")
    prev_ca_dispatcher = state.get("prev_ca_dispatcher_msg_id")
    truck_group        = state.get("truck_group")
    # NOTE: Not deleting previous alerts — all messages kept for accountability

    result = send_ca_border_reminder(
        vehicle_name=vname,
        fuel_pct=fuel,
        truck_lat=lat,
        truck_lng=lng,
        best_stop=best,
        ca_avg_price=ca_avg,
        dist_to_border=dist_border,
    )

    state["prev_ca_truck_msg_id"]      = result.get("truck_msg_id")
    state["prev_ca_dispatcher_msg_id"] = result.get("dispatcher_msg_id")
    state["truck_group"]               = result.get("truck_group")
    state["ca_reminder_sent"] = True

    # Save immediately so restart doesn't re-send
    from database import save_truck_state
    save_truck_state(state)

    create_fuel_alert(
        vid, vname, fuel, lat, lng, heading, speed,
        alert_type="ca_border", best_stop=best,
    )
