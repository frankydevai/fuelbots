"""
telegram_bot.py  -  Telegram message sending for FleetFuel bot.
"""

import time
import logging
import requests
from config import TELEGRAM_BOT_TOKEN, DISPATCHER_GROUP_ID, ADMIN_CHAT_ID, MIN_SAVINGS_DISPLAY

import metrics

log = logging.getLogger(__name__)

force_check_now: bool = False
BASE_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"


def _post(method: str, payload: dict, retries: int = 4) -> dict | None:
    for attempt in range(retries + 1):
        try:
            with metrics.Timer(f"telegram_{method}_seconds"):
                resp = requests.post(f"{BASE_URL}/{method}", json=payload, timeout=10)
            if resp.status_code == 429:
                wait = max(resp.json().get("parameters", {}).get("retry_after", 5), 5)
                wait *= (attempt + 1)
                metrics.incr("telegram_429_total")
                log.warning(f"Telegram 429 — waiting {wait}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            metrics.incr(f"telegram_{method}_ok_total")
            return resp.json()
        except requests.RequestException as exc:
            metrics.incr(f"telegram_{method}_err_total")
            log.error(f"Telegram {method} failed (attempt {attempt+1}): {exc}")
            if attempt < retries:
                time.sleep(3 * (attempt + 1))
    return None


def _send_to(chat_id: str, text: str) -> int | None:
    if not chat_id:
        return None
    result = _post("sendMessage", {
        "chat_id": chat_id, "text": text,
        "parse_mode": "Markdown", "disable_web_page_preview": True,
    })
    if result and result.get("ok"):
        return result["result"]["message_id"]
    import re
    plain = re.sub(r"[*_`\[\]]", "", text)
    result2 = _post("sendMessage", {"chat_id": chat_id, "text": plain, "disable_web_page_preview": True})
    if result2 and result2.get("ok"):
        return result2["result"]["message_id"]
    return None


def _send_to_truck(vehicle_name: str, text: str) -> dict:
    from database import get_truck_group, get_truck_alert_status
    # Layer 3 alert guard — do not send if alerts are paused for this truck
    try:
        status = get_truck_alert_status(vehicle_name)
        if status and status.get("alert_paused"):
            log.warning(
                f"Alert skipped — {vehicle_name} paused: {status.get('pause_reason', 'unknown')}"
            )
            return {"truck_group": None, "truck_msg_id": None, "dispatcher_msg_id": None,
                    "paused": True, "pause_reason": status.get("pause_reason")}
    except Exception as _e:
        log.warning(f"Alert guard check failed for {vehicle_name}: {_e}")
    truck_group = get_truck_group(vehicle_name)
    truck_msg_id = None
    dispatcher_msg_id = None
    if truck_group:
        truck_msg_id = _send_to(truck_group, text)
    else:
        log.info(f"No group set for {vehicle_name} — dispatcher only")
    if DISPATCHER_GROUP_ID and truck_group != str(DISPATCHER_GROUP_ID):
        dispatcher_msg_id = _send_to_dispatcher(text)
    return {"truck_group": truck_group, "truck_msg_id": truck_msg_id, "dispatcher_msg_id": dispatcher_msg_id}


def delete_message(chat_id: str, message_id: int) -> bool:
    result = _post("deleteMessage", {"chat_id": chat_id, "message_id": message_id}, retries=0)
    return bool(result and result.get("ok"))


def _send_to_dispatcher(text: str) -> int | None:
    if not DISPATCHER_GROUP_ID:
        return None
    return _send_to(DISPATCHER_GROUP_ID, text)


def _compass(heading: float) -> str:
    dirs = ["N","NNE","NE","ENE","E","ESE","SE","SSE","S","SSW","SW","WSW","W","WNW","NW","NNW"]
    return dirs[round(heading / 22.5) % 16]


def _urgency_emoji(fuel_pct: float) -> str:
    if fuel_pct <= 10: return "🚨"
    if fuel_pct <= 15: return "🔴"
    if fuel_pct <= 25: return "🟠"
    return "🟡"


def send_low_fuel_alert(vehicle_name, fuel_pct, truck_lat, truck_lng,
                        heading, speed_mph, best_stop, alt_stop=None, savings_usd=None) -> dict:
    """Send fuel plan — next stop name, address, Google Maps link, gallons to fill."""
    emoji = _urgency_emoji(fuel_pct)
    lines = [
        f"{emoji} *Fuel Stop — Truck {vehicle_name}*",
        f"⛽ Fuel: *{fuel_pct:.0f}%*",
    ]

    if best_stop:
        from config import DEFAULT_TANK_GAL
        name     = best_stop.get("store_name", "Unknown")
        street   = best_stop.get("address", "")
        city     = best_stop.get("city", "")
        state    = best_stop.get("state", "")
        zip_code = best_stop.get("zip", "")
        lat      = best_stop.get("latitude")
        lng      = best_stop.get("longitude")
        addr     = ", ".join(filter(None, [street, city, state, zip_code]))
        maps_url = f"https://maps.google.com/?q={lat},{lng}" if lat and lng else None
        gallons  = round(DEFAULT_TANK_GAL * (1 - fuel_pct / 100), 1)

        lines += ["", f"⛽ *Next Stop: {name}*", f"📌 {addr}"]
        if maps_url:
            lines.append(f"🗺 [Open in Google Maps]({maps_url})")
        lines.append(f"💧 Fill *{gallons:.0f} gallons*")
    else:
        lines += ["", "❌ No fuel stops found on route.", "Dispatcher has been notified."]
        _send_to_dispatcher(f"{emoji} *{vehicle_name}* — {fuel_pct:.0f}% — NO STOP FOUND")

    result = _send_to_truck(vehicle_name, "\n".join(lines))
    return result if isinstance(result, dict) else {"truck_group": None, "truck_msg_id": result, "dispatcher_msg_id": None}


def send_emergency_alert(vehicle_name, fuel_pct, truck_lat, truck_lng,
                          heading, speed_mph, best_stop,
                          planned_stop_name=None, range_miles=0,
                          gps_stale=False) -> dict:
    """
    Emergency alert — only fires when truck cannot reach planned stop.
    Sent to driver group + dispatcher immediately.
    """
    compass   = _compass(heading)
    truck_url = f"https://maps.google.com/?q={truck_lat:.6f},{truck_lng:.6f}"

    lines = [
        f"🔴 *Emergency — Truck {vehicle_name}*",
        f"⛽ Fuel: *{fuel_pct:.0f}%*  🧭 {speed_mph:.0f} mph {compass}",
        f"📍 [Truck Location]({truck_url})",
        f"🌐 `{truck_lat:.5f}, {truck_lng:.5f}`",
    ]
    if gps_stale:
        lines.append("⚠️ _GPS location may be outdated — verify with driver_")
    lines.append("")

    if planned_stop_name:
        lines.append(f"⚠️ Cannot reach planned stop: *{planned_stop_name}*")
        lines.append(f"Range on current fuel: ~{range_miles:.0f} miles")
        lines.append("")

    if best_stop:
        name     = best_stop.get("store_name", "Unknown")
        street   = best_stop.get("address", "")
        city     = best_stop.get("city", "")
        state    = best_stop.get("state", "")
        dist     = best_stop.get("distance_miles", 0)
        pump     = best_stop.get("diesel_price")
        net      = best_stop.get("net_price")
        retail   = best_stop.get("retail_price")
        discount = best_stop.get("discount_per_gallon")
        lat      = best_stop.get("latitude")
        lng      = best_stop.get("longitude")
        addr     = ", ".join(filter(None, [street, city, state]))
        maps_url = f"https://maps.google.com/?q={lat},{lng}" if lat and lng else None

        from config import DEFAULT_TANK_GAL
        gallons = round(DEFAULT_TANK_GAL * (1 - fuel_pct / 100), 1)

        lines.append(f"Nearest reachable stop:")
        lines.append(f"⛽ *{name}*")
        lines.append(f"📌 {addr}")
        lines.append(f"🛣 *{dist:.1f} mi ahead*")

        if retail and pump and retail != pump:
            lines.append(f"💰 Retail: ${retail:.3f}/gal")
        if pump:
            lines.append(f"💳 Card: *${pump:.3f}/gal*" +
                         (f"  (save ${discount:.2f}/gal)" if discount else ""))

        true_price = pump if pump else None
        if true_price and gallons:
            total = round(true_price * gallons, 2)
            lines.append(f"💵 Fill *{gallons:.0f} gal = ${total:.0f}*")

        if maps_url:
            lines.append(f"🗺 [Open in Google Maps]({maps_url})")
    else:
        lines += [
            "❌ *NO FUEL STOPS found within range.*",
            f"Range remaining: ~{range_miles:.0f} miles",
            "Dispatcher has been notified — immediate assistance needed.",
        ]

    # Always notify dispatcher on emergency
    _send_to_dispatcher("\n".join(lines))
    result = _send_to_truck(vehicle_name, "\n".join(lines))
    return result if isinstance(result, dict) else {
        "truck_group": None,
        "truck_msg_id": result,
        "dispatcher_msg_id": None
    }


def send_ca_border_reminder(vehicle_name, fuel_pct, truck_lat, truck_lng,
                             best_stop, ca_avg_price, dist_to_border):
    truck_url = f"https://maps.google.com/?q={truck_lat:.6f},{truck_lng:.6f}"
    lines = [
        f"🌵 *California Border Ahead — Truck {vehicle_name}*",
        f"🛣 {dist_to_border:.0f} miles to CA border",
        f"⛽ Fuel: *{fuel_pct:.0f}%*",
        f"📍 [Truck Location]({truck_url})", "",
        f"💡 *Fill up before crossing — diesel is ~$1/gal more expensive in CA!*",
    ]
    if best_stop:
        addr = ", ".join(filter(None, [best_stop.get("address",""), best_stop.get("city",""),
                                        best_stop.get("state",""), best_stop.get("zip","")]))
        price = best_stop.get("diesel_price")
        lat = best_stop.get("latitude"); lng = best_stop.get("longitude")
        maps_url = f"https://maps.google.com/?q={lat},{lng}" if lat and lng else None
        lines += ["", f"⛽ *{best_stop.get('store_name','')}*", f"📌 {addr}",
                  f"🛣 {best_stop.get('distance_miles',0):.1f} mi away",
                  f"💰 Diesel: *${price:.3f}/gal*" if price else "💰 Diesel: Price N/A"]
        if maps_url:
            lines.append(f"🗺 [Open in Google Maps]({maps_url})")
    return _send_to_truck(vehicle_name, "\n".join(lines))


def send_at_stop_alert(vehicle_name, fuel_pct, truck_lat, truck_lng, current_stop) -> dict:
    emoji     = _urgency_emoji(fuel_pct)
    truck_url = f"https://maps.google.com/?q={truck_lat:.6f},{truck_lng:.6f}"
    name      = current_stop.get("store_name", "Fuel Stop")
    address   = ", ".join(filter(None, [current_stop.get("address",""), current_stop.get("city",""),
                                         current_stop.get("state",""), current_stop.get("zip","")]))
    price     = current_stop.get("diesel_price")
    slat      = current_stop.get("latitude"); slng = current_stop.get("longitude")
    maps_url  = f"https://maps.google.com/?q={slat},{slng}" if slat and slng else None
    lines = [
        f"{emoji} *Low Fuel Alert — Truck {vehicle_name}*",
        f"⛽ Fuel: *{fuel_pct:.0f}%*",
        f"📍 [View on Map]({truck_url})", "",
        f"🅿️ *Already stopped at:*",
        f"⛽ *{name}*", f"📌 {address}",
        f"💰 Diesel: *${price:.3f}/gal*" if price else "💰 Diesel: Price N/A",
    ]
    if maps_url:
        lines.append(f"🗺 [Open in Google Maps]({maps_url})")
    return _send_to_truck(vehicle_name, "\n".join(lines))


def send_refueled_alert(vehicle_name, stop_name, fuel_pct,
                         truck_lat=None, truck_lng=None, actual_stop=None):
    """Send refuel confirmation showing where truck actually fueled."""
    lines = [
        f"✅ *REFUELED — Truck {vehicle_name}*",
        f"⛽ Fuel now: *{fuel_pct:.0f}%*",
    ]

    # Show actual stop if we detected it
    if actual_stop and actual_stop.get("store_name"):
        name    = actual_stop["store_name"]
        address = ", ".join(filter(None, [
            actual_stop.get("address",""), actual_stop.get("city",""),
            actual_stop.get("state",""), actual_stop.get("zip",""),
        ]))
        price   = actual_stop.get("diesel_price")
        slat    = actual_stop.get("latitude")
        slng    = actual_stop.get("longitude")
        maps_url = f"https://maps.google.com/?q={slat},{slng}" if slat and slng else None
        lines += [
            f"🏪 *Fueled at:* {name}",
            f"📌 {address}",
        ]
        if price:
            lines.append(f"💰 Diesel: ${price:.3f}/gal")
        if maps_url:
            lines.append(f"🗺 [Open in Google Maps]({maps_url})")
    else:
        # Fallback — show GPS location
        lines.append(f"🏪 *Fueled at:* {stop_name}")
        if truck_lat and truck_lng:
            maps_url = f"https://maps.google.com/?q={truck_lat:.6f},{truck_lng:.6f}"
            lines.append(f"🗺 [View location]({maps_url})")

    _send_to_truck(vehicle_name, "\n".join(lines))
    # Also notify dispatcher
    _send_to_dispatcher(f"✅ *{vehicle_name}* refueled at {stop_name} — {fuel_pct:.0f}% fuel")


def send_left_yard_low_fuel(vehicle_name, fuel_pct, yard_name):
    text = f"🏠 *LEFT YARD — LOW FUEL*\n🚛 *Truck:* {vehicle_name}\n⛽ *Fuel:* {fuel_pct:.0f}%\n📍 *Departed:* {yard_name}"
    _send_to_truck(vehicle_name, text)
    _send_to_dispatcher(f"🏠 *{vehicle_name}* left {yard_name} with {fuel_pct:.0f}% fuel.")


def register_commands():
    commands = [
        {"command": "checknow",    "description": "Force immediate fuel check"},
        {"command": "findstop",    "description": "Find cheapest stops — /findstop 0792"},
        {"command": "route",       "description": "Show active load — /route 0792"},
        {"command": "findload",    "description": "Search QM trip — /findload 8656"},
        {"command": "qmload",      "description": "Read QM load by truck - /qmload 0792"},
        {"command": "resetpilot",  "description": "Wipe Pilot DB rows"},
        {"command": "dbstats",     "description": "Show DB stats"},
        {"command": "addtruck",    "description": "Add truck — /addtruck 4821 -100123456"},
        {"command": "setgroup",    "description": "Set group — /setgroup 4821 -100123456"},
        {"command": "listtruck",   "description": "List all trucks"},
        {"command": "removetruck",   "description": "Deactivate truck"},
        {"command": "updategroup",   "description": "Assign this group to a truck — /updategroup 0792"},
        {"command": "alertcount",    "description": "How many drivers receiving alerts (by system)"},
    ]
    _post("setMyCommands", {"commands": commands})


def send_startup_message():
    register_commands()
    _send_to(ADMIN_CHAT_ID, "🚛 *FleetFuel Bot online.* Monitoring fuel levels.")


def send_price_update_notification(pilot_count, loves_count):
    log.info(f"Prices updated: Pilot={pilot_count} Love's={loves_count}")


_last_update_id: int = 0


def _get_file_url(file_id):
    result = _post("getFile", {"file_id": file_id})
    if result and result.get("ok"):
        path = result["result"]["file_path"]
        return f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{path}"
    return None


def _download_file(file_url):
    try:
        resp = requests.get(file_url, timeout=30)
        resp.raise_for_status()
        return resp.content
    except Exception as e:
        log.error(f"Download failed: {e}")
        return None


def poll_for_uploads():
    global _last_update_id
    if not ADMIN_CHAT_ID:
        return
    try:
        result = _post("getUpdates", {
            "offset": _last_update_id + 1, "timeout": 0, "limit": 20,
            "allowed_updates": ["message", "my_chat_member", "chat_member"],
        })
        if not result or not result.get("ok"):
            return
        for update in result.get("result", []):
            _last_update_id = update["update_id"]

            # Bot added to group
            chat_member = update.get("my_chat_member", {})
            if chat_member:
                new_status = chat_member.get("new_chat_member", {}).get("status", "")
                if new_status in ("member", "administrator"):
                    chat = chat_member.get("chat", {})
                    g_id = str(chat.get("id", ""))
                    g_title = chat.get("title", "") or ""
                    # Extract truck number from group name
                    # Supports formats:
                    #   "1769 (32%) Kendy Louis"  → truck 1769
                    #   "Truck 0792 John Smith"   → truck 0792
                    #   "0792 Driver Name"        → truck 0792
                    import re as _re
                    first_word  = g_title.strip().split()[0] if g_title.strip() else ""
                    # Also try matching any numeric sequence in the title
                    num_matches = _re.findall(r'\b(\d{3,6})\b', g_title)
                    candidates  = [first_word] + num_matches

                    matched = None
                    if candidates:
                        from database import get_all_registered_trucks, upsert_truck_group
                        trucks = get_all_registered_trucks()
                        truck_names = {t["vehicle_name"]: t for t in trucks}
                        for candidate in candidates:
                            if candidate in truck_names:
                                matched = candidate
                                break
                            # Try partial match e.g. "0792" matches "Truck 0792"
                            for name in truck_names:
                                if candidate in name or name in candidate:
                                    matched = name
                                    break
                            if matched:
                                break
                    if matched:
                        upsert_truck_group(matched, g_id)
                        _send_to(ADMIN_CHAT_ID, f"✅ *Auto-assigned*\nTruck: *{matched}*\nGroup: *{g_title}*\nID: `{g_id}`")
                    else:
                        _send_to(ADMIN_CHAT_ID, f"➕ *Bot added to group*\n*{g_title}*\nID: `{g_id}`\n`/setgroup TRUCKNAME {g_id}`")
                continue

            # -- Layer 1: real-time chat_member events (requires bot to be admin) --
            member_evt = update.get("chat_member", {})
            if member_evt:
                _handle_chat_member_event(member_evt)
                continue

            message  = update.get("message", {})
            chat_id  = str(message.get("chat", {}).get("id", ""))
            document = message.get("document")
            text     = message.get("text", "").strip()

            # -- Layer 1 fallback: new_chat_members / left_chat_member in messages --
            # (fires even when bot is not admin — less reliable but works everywhere)
            new_members = message.get("new_chat_members", [])
            if new_members:
                _handle_new_members_message(chat_id, new_members)

            left_member = message.get("left_chat_member")
            if left_member and not left_member.get("is_bot"):
                _handle_left_member_message(chat_id, left_member)

            # QM Notifier — detect by content
            if "NEW TRIP" in text and "HAS BEEN ASSIGNED" in text:
                try:
                    from route_reader import parse_qm_notifier_message
                    from database import save_truck_route, get_truck_by_group
                    route = parse_qm_notifier_message(text, chat_id)
                    if route:
                        truck = get_truck_by_group(chat_id)
                        if truck:
                            save_truck_route(truck["vehicle_name"], chat_id, route)
                            log.info(f"Route saved for truck {truck['vehicle_name']}: trip {route['trip_num']} {route['origin']['city']} → {route['destination']['city']}")
                        else:
                            log.warning(f"QM message in group {chat_id} — no truck matched")
                except Exception as e:
                    log.error(f"QM Notifier parse error: {e}", exc_info=True)

            # Natural language trigger — "plan route" in any driver group
            if text.lower().strip() == "plan route":
                _handle_plan_route_from_group(chat_id)
                continue

            if text.startswith("/"):
                text = text.split("@")[0]

            # Commands for any group
            if text.startswith("/loadroute"):
                _handle_loadroute(text, chat_id)
                continue
            elif text.startswith("/route"):
                _handle_route(text, chat_id)
                continue
            elif text.startswith("/qmload"):
                _handle_qmload(text, chat_id)
                continue
            elif text.startswith("/newalert"):
                _handle_newalert(text)
                continue
            elif text.startswith("/flags"):
                _handle_flags(text, chat_id)
                continue
            elif text.startswith("/stopvisits"):
                _handle_stopvisits(text, chat_id)
                continue
            elif text.startswith("/compliance"):
                _handle_compliance(text, chat_id)
                continue
            elif text.startswith("/fuelhistory"):
                _handle_fuelhistory(text, chat_id)
                continue
            elif text.startswith("/findstop"):
                try:
                    _handle_findstop(text, chat_id)
                except Exception as e:
                    _send_to(chat_id, f"❌ Error: `{e}`")
                continue
            elif text.startswith("/updategroup"):
                _handle_updategroup(text, chat_id)
                continue
            elif text.startswith("/active"):
                _handle_active(chat_id)
                continue
            elif text.startswith("/relayapp"):
                _handle_relayapp(chat_id)
                continue
            elif text.startswith("/cityfuel"):
                _handle_cityfuel(chat_id)
                continue
            elif text.startswith("/planroute"):
                try:
                    _handle_planroute(text, chat_id)
                except Exception as e:
                    _send_to(chat_id, f"❌ Error: `{e}`")
                continue
            elif text.startswith("/confirm"):
                _handle_driver_confirm(chat_id, message)
                continue
            elif text.startswith("/wrong"):
                _handle_driver_wrong(chat_id, message)
                continue

            if chat_id != ADMIN_CHAT_ID:
                continue

            if text.startswith("/"):
                try:
                    if text.startswith("/addtruck"):       _handle_addtruck(text)
                    elif text.startswith("/setgroup"):     _handle_setgroup(text)
                    elif text.startswith("/listtruck"):    _handle_listtruck()
                    elif text.startswith("/removetruck"):  _handle_removetruck(text)
                    elif text.startswith("/resetstops"):   _handle_resetstops()
                    elif text.startswith("/checkall"):     _handle_checkall()
                    elif text.startswith("/checknow"):     _handle_checknow()
                    elif text.startswith("/dbstats"):      _handle_dbstats()
                    elif text.startswith("/resetpilot"):   _handle_resetpilot()
                    elif text.startswith("/findload"):     _handle_findload(text, chat_id)
                    elif text.startswith("/testroute"):    _handle_testroute(text)
                    elif text.startswith("/truckstats"):      _handle_truckstats(text, chat_id)
                    elif text.startswith("/routelist"):       _handle_routelist(chat_id)
                    elif text.startswith("/classify_truck"):  _handle_classify_truck(text, chat_id)
                    elif text.startswith("/setnewsystem"):    _handle_setnewsystem(text)
                    elif text.startswith("/setoldsystem"):    _handle_setoldsystem(text)
                    elif text.startswith("/weeklyreport"):    _handle_weeklyreport()
                    elif text.startswith("/dedupetrucks"):    _handle_dedupetrucks()
                    elif text.startswith("/syncsamsara"):     _handle_syncsamsara(text)
                    elif text.startswith("/assigndriver"):    _handle_assigndriver(text)
                    elif text.startswith("/removedriver"):    _handle_removedriver(text)
                    elif text.startswith("/whodrives"):       _handle_whodrives(text)
                    elif text.startswith("/driverlog"):       _handle_driverlog(text)
                    elif text.startswith("/driverlist"):      _handle_driverlist()
                    elif text.startswith("/alertcount"):      _handle_alertcount()
                    elif text.startswith("/resumeall"):       _handle_resumeall()
                    else:
                        _send_to(ADMIN_CHAT_ID,
                            "Available commands:\n"
                            "/addtruck Unit4821 -100123456\n"
                            "/setgroup Unit4821 -100123456\n"
                            "/listtruck\n/removetruck Unit4821\n"
                            "/setnewsystem 0801 0802  — move trucks to Relay card\n"
                            "/setoldsystem 0801 0802  — move trucks back to old EFS card\n"
                            "/alertcount  — how many drivers receiving alerts (by system)\n"
                            "/weeklyreport  — resend weekly Excel reports now\n"
                            "/findstop 0792  — any group\n"
                            "/route 0792  — any group\n"
                            "/qmload 0792  — read QM load by truck\n"
                            "/findload 8656  — search QM trip\n"
                            "/classify_truck 0792  — show classifier status+signals\n"
                            "\nDriver group management:\n"
                            "/assigndriver 0792 John Smith -100GROUP_ID\n"
                            "/removedriver 0792  — pause alerts, clear driver\n"
                            "/whodrives 0792  — show driver + alert status\n"
                            "/driverlog 0792  — last 10 group events\n"
                            "/driverlist  — all trucks with driver + status\n"
                            "/resumeall  — restore alerts paused by heartbeat false-positive\n"
                            "\nIn driver groups:\n"
                            "/relayapp  — move group trucks to new Relay card\n"
                            "/cityfuel  — move group trucks back to old EFS card\n"
                            "/planroute 0792  — full fuel plan (works in any group)\n"
                            "/confirm  — driver confirms assignment\n"
                            "/wrong  — wrong driver in group"
                        )
                except Exception as e:
                    log.error(f"Command error: {e}", exc_info=True)
                    _send_to(ADMIN_CHAT_ID, f"❌ Command failed: `{e}`")
                continue

            if not document:
                _send_to(ADMIN_CHAT_ID, "📂 Send CSV/XLSX to update prices, or use a command.")
                continue

            filename   = document.get("file_name", "upload").strip()
            file_id    = document.get("file_id")
            ext        = filename.lower().split(".")[-1].strip()
            if ext not in ("csv", "xlsx", "xls", "zip"):
                _send_to(ADMIN_CHAT_ID, f"❌ Unsupported file: `{filename}`\nSend .csv for old system or .xlsx/.xls for new Relay system.")
                continue
            system_label = "new Relay card (Pilot/FJ)" if ext in ("xls", "xlsx") else "old card (all stops)"
            _send_to(ADMIN_CHAT_ID, f"📥 Received `{filename}` — processing {system_label}...")
            file_url = _get_file_url(file_id)
            if not file_url:
                _send_to(ADMIN_CHAT_ID, "❌ Could not retrieve file.")
                continue
            file_bytes = _download_file(file_url)
            if not file_bytes:
                _send_to(ADMIN_CHAT_ID, "❌ Failed to download file.")
                continue
            from price_updater import update_from_file
            count, msg = update_from_file(file_bytes, filename)
            _send_to(ADMIN_CHAT_ID, msg)
            if count > 0:
                log.info(f"Admin uploaded {filename} — {count} stops updated.")
    except Exception as e:
        log.error(f"poll_for_uploads error: {e}", exc_info=True)


# -- Admin handlers -----------------------------------------------------------

def _handle_checkall() -> None:
    """/checkall — immediately check all trucks and report low fuel ones"""
    from samsara_client import get_combined_vehicle_data
    from truck_stop_finder import get_urgency
    from config import FUEL_ALERT_THRESHOLD_PCT

    _send_to(ADMIN_CHAT_ID, "🔄 Checking all trucks now...")

    try:
        vehicles = get_combined_vehicle_data()
    except Exception as e:
        _send_to(ADMIN_CHAT_ID, f"❌ Samsara error: `{e}`")
        return

    low_fuel   = []
    critical   = []
    healthy    = []

    for v in vehicles:
        fuel  = v.get("fuel_pct", 100)
        name  = v.get("vehicle_name", "?")
        speed = v.get("speed_mph", 0)
        if fuel <= 10:
            critical.append((name, fuel, speed))
        elif fuel <= FUEL_ALERT_THRESHOLD_PCT:
            low_fuel.append((name, fuel, speed))
        else:
            healthy.append(name)

    # Sort by fuel level (lowest first)
    critical.sort(key=lambda x: x[1])
    low_fuel.sort(key=lambda x: x[1])

    lines = [
        f"📊 *Fleet Fuel Check — {len(vehicles)} trucks*",
        f"✅ Healthy: {len(healthy)}  |  🟡 Low: {len(low_fuel)}  |  🚨 Critical: {len(critical)}",
        "",
    ]

    if critical:
        lines.append("🚨 *CRITICAL (≤10%):*")
        for name, fuel, speed in critical:
            lines.append(f"   🚨 Truck *{name}* — {fuel:.0f}% | {speed:.0f} mph")
        lines.append("")

    if low_fuel:
        lines.append("🟡 *Low Fuel (≤35%):*")
        for name, fuel, speed in low_fuel:
            urgency = get_urgency(fuel)
            emoji   = {"WARNING": "🟠", "CRITICAL": "🔴"}.get(urgency, "🟡")
            lines.append(f"   {emoji} Truck *{name}* — {fuel:.0f}% | {speed:.0f} mph")
        lines.append("")

    if not critical and not low_fuel:
        lines.append("✅ All trucks have sufficient fuel.")

    _send_to(ADMIN_CHAT_ID, "\n".join(lines))

    # Also trigger force check so alerts fire for low fuel trucks
    global force_check_now
    force_check_now = True
    if low_fuel or critical:
        _send_to(ADMIN_CHAT_ID, f"⚡ Alerts will fire for {len(low_fuel)+len(critical)} trucks in next poll cycle.")


def _handle_newalert(text: str) -> None:
    """/newalert <truck_number> — force immediate new alert for a truck"""
    from database import load_all_truck_states, save_truck_state
    parts = text.strip().split()
    if len(parts) < 2:
        _send_to(ADMIN_CHAT_ID, "Usage: `/newalert 3663`")
        return

    truck_num = parts[1].strip()
    states = load_all_truck_states()

    # Find truck by name
    found = None
    for vid, state in states.items():
        if str(state.get("vehicle_name","")) == truck_num:
            found = (vid, state)
            break

    if not found:
        _send_to(ADMIN_CHAT_ID, f"❌ Truck *{truck_num}* not found in active states.")
        return

    vid, state = found
    # Reset alert timer and clear assignment so fresh stop is found
    state["last_alert_time"]    = None
    state["alert_sent"]         = False
    state["assigned_stop_id"]   = None
    state["assigned_stop_name"] = None
    state["assigned_stop_lat"]  = None
    state["assigned_stop_lng"]  = None
    state["assignment_time"]    = None
    save_truck_state(state)

    # Also trigger force check
    global force_check_now
    force_check_now = True

    _send_to(ADMIN_CHAT_ID,
        f"✅ *Truck {truck_num}* — new alert triggered.\n"
        f"Fresh stop recommendation will send in next poll cycle (~30 sec)."
    )


def _handle_resetstops():
    """/resetstops — show fuel stop count (no deletion — data is permanent)"""
    from database import db_cursor
    with db_cursor() as cur:
        cur.execute("SELECT COUNT(*) as cnt FROM fuel_stops")
        cnt = cur.fetchone()["cnt"]
        cur.execute("SELECT MAX(price_updated) as latest FROM fuel_stops")
        latest = cur.fetchone()["latest"]
    _send_to(ADMIN_CHAT_ID,
        f"⛽ *Fuel Stops DB*\n"
        f"📍 {cnt} stations loaded\n"
        f"🕐 Last updated: {latest.strftime('%b %d %H:%M') if latest else 'never'}\n\n"
        f"To update prices, send the new CSV file here.\n"
        f"Prices are updated in place — nothing is deleted."
    )

def _handle_checknow():
    global force_check_now
    force_check_now = True
    _send_to(ADMIN_CHAT_ID, "🔄 *Force check triggered.*")


def _handle_addtruck(text):
    from database import auto_register_truck, upsert_truck_group
    parts = text.split()
    if len(parts) < 2:
        _send_to(ADMIN_CHAT_ID, "Usage: /addtruck <name> [group_id]")
        return
    name = parts[1]
    gid  = parts[2] if len(parts) >= 3 else None
    try:
        auto_register_truck(name, name)
        if gid:
            upsert_truck_group(name, gid)
        _send_to(ADMIN_CHAT_ID, f"✅ Truck *{name}* added" + (f" → group `{gid}`" if gid else ""))
    except Exception as e:
        _send_to(ADMIN_CHAT_ID, f"❌ Failed: `{e}`")


def _handle_setgroup(text):
    from database import upsert_truck_group
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        _send_to(ADMIN_CHAT_ID, "Usage: `/setgroup Unit4821 -1009876543210`")
        return
    tokens = parts[1].rsplit(maxsplit=1)
    if len(tokens) != 2 or not tokens[1].lstrip("-").isdigit():
        _send_to(ADMIN_CHAT_ID, "Usage: `/setgroup Unit4821 -1009876543210`")
        return
    name = tokens[0].strip(); gid = tokens[1].strip()
    if upsert_truck_group(name, gid):
        _send_to(ADMIN_CHAT_ID, f"✅ *{name}* → group `{gid}`")
    else:
        _send_to(ADMIN_CHAT_ID, f"❌ Truck not found: *{name}*")


def _handle_listtruck():
    from database import get_all_registered_trucks
    trucks = get_all_registered_trucks()
    if not trucks:
        _send_to(ADMIN_CHAT_ID, "No trucks registered.")
        return

    old_trucks = [t for t in trucks if (t.get('fuel_card_system') or 'old') == 'old']
    new_trucks = [t for t in trucks if (t.get('fuel_card_system') or 'old') == 'new']

    def _fmt(t):
        grp = t.get('telegram_group_id') or '— no group'
        return f"• *{t['vehicle_name']}*  `{grp}`"

    all_lines = []
    if old_trucks:
        all_lines.append(f"*🟡 Old System (EFS/WEX) — {len(old_trucks)} trucks:*")
        all_lines += [_fmt(t) for t in old_trucks]
        all_lines.append("")
    if new_trucks:
        all_lines.append(f"*🟢 New System (Relay/Pilot FJ) — {len(new_trucks)} trucks:*")
        all_lines += [_fmt(t) for t in new_trucks]

    chunks = [all_lines[i:i+50] for i in range(0, len(all_lines), 50)]
    for i, chunk in enumerate(chunks):
        header = f"🚛 *Trucks ({len(trucks)} total)*" + (f" — page {i+1}/{len(chunks)}" if len(chunks) > 1 else "") + "\n"
        _send_to(ADMIN_CHAT_ID, header + "\n".join(chunk))


def _handle_removetruck(text):
    from database import deactivate_truck
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        _send_to(ADMIN_CHAT_ID, "Usage: `/removetruck Unit4821`")
        return
    name = parts[1].strip()
    if deactivate_truck(name):
        _send_to(ADMIN_CHAT_ID, f"✅ Deactivated: *{name}*")
    else:
        _send_to(ADMIN_CHAT_ID, f"❌ Not found: *{name}*")


def _handle_resetpilot():
    from database import db_cursor
    with db_cursor() as cur:
        cur.execute("SELECT COUNT(*) as cnt FROM fuel_stops WHERE station_name ILIKE '%pilot%' OR station_name ILIKE '%FJ%'")
        cnt = cur.fetchone()["cnt"]
    _send_to(ADMIN_CHAT_ID, f"⛽ *{cnt}* Pilot/Flying J stops in DB.\nTo update prices, send the new CSV file.")


def _handle_dbstats():
    from database import db_cursor
    with db_cursor() as cur:
        # Old EFS/WEX system — fuel_stops table
        cur.execute("""
            SELECT
                network,
                COUNT(*)                                    AS total,
                COUNT(discounted_price)                     AS priced,
                ROUND(AVG(discounted_price)::numeric, 3)    AS avg_price,
                MIN(discounted_price)                       AS min_price,
                MAX(discounted_price)                       AS max_price,
                MAX(price_updated)                          AS last_updated
            FROM fuel_stops
            GROUP BY network
            ORDER BY network
        """)
        old_rows = cur.fetchall()
        cur.execute("SELECT COUNT(*) AS total FROM fuel_stops")
        old_total = cur.fetchone()["total"]

        # New Relay system — pilot_contracted_prices table
        cur.execute("""
            SELECT
                COUNT(*)                                    AS total,
                COUNT(discounted_price)                     AS priced,
                ROUND(AVG(discounted_price)::numeric, 3)    AS avg_price,
                MIN(discounted_price)                       AS min_price,
                MAX(discounted_price)                       AS max_price,
                MAX(price_updated)                          AS last_updated
            FROM pilot_contracted_prices
        """)
        relay_row = cur.fetchone()

        # Truck counts per system
        cur.execute("""
            SELECT fuel_card_system, COUNT(*) AS cnt
            FROM trucks WHERE is_active = TRUE
            GROUP BY fuel_card_system
        """)
        truck_rows = {r["fuel_card_system"]: r["cnt"] for r in cur.fetchall()}

    lines = ["📊 *Fuel Price DB Stats*\n"]

    # Old system section
    lines.append(f"*🟡 EFS/WEX System (old card)* — {old_total} stops total")
    lines.append(f"  Trucks on this system: {truck_rows.get('old', 0)}")
    if old_rows:
        for r in old_rows:
            net = (r["network"] or "other").replace("_", " ").title()
            upd = r["last_updated"].strftime("%b %d %H:%M UTC") if r["last_updated"] else "never"
            if r["priced"]:
                lines.append(f"  {net}: {r['priced']}/{r['total']} priced  "
                             f"${r['min_price'] or 0:.3f}–${r['max_price'] or 0:.3f} "
                             f"(avg ${r['avg_price'] or 0:.3f})  upd {upd}")
            else:
                lines.append(f"  {net}: {r['total']} stops — no prices")
    else:
        lines.append("  ⚠️ No stops loaded")

    lines.append("")

    # New Relay system section
    r = relay_row
    lines.append(f"*🟢 Relay System (new card)* — {r['total']} Pilot/Flying J stops")
    lines.append(f"  Trucks on this system: {truck_rows.get('new', 0)}")
    if r["total"]:
        upd = r["last_updated"].strftime("%b %d %H:%M UTC") if r["last_updated"] else "never"
        if r["priced"]:
            lines.append(f"  Priced: {r['priced']}/{r['total']}  "
                         f"${r['min_price'] or 0:.3f}–${r['max_price'] or 0:.3f} "
                         f"(avg ${r['avg_price'] or 0:.3f})  upd {upd}")
        else:
            lines.append("  ⚠️ Stops loaded but no prices — upload XLS to add prices")
    else:
        lines.append("  ⚠️ No stops loaded — upload Relay XLS file")

    _send_to(ADMIN_CHAT_ID, "\n".join(lines))


def _handle_findload(text: str, chat_id: str) -> None:
    parts = text.strip().split()
    if len(parts) < 2:
        _send_to(chat_id, "Usage: `/findload 8656`")
        return
    trip_num = parts[1].strip()
    try:
        from config import QM_CLIENT_ID, QM_CLIENT_SECRET
        if not QM_CLIENT_ID or not QM_CLIENT_SECRET:
            _send_to(chat_id, "❌ QuickManage credentials not configured.")
            return
        from quickmanage_client import _get_token
        token = _get_token()
        if not token:
            _send_to(chat_id, "❌ Could not get QuickManage token.")
            return
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        payload = {"query": trip_num, "filters": [], "page": 0, "page_size": 10}
        resp = requests.post("https://api.quickmanage.com/x/trips/search", json=payload, headers=headers, timeout=10)
        log.info(f"/findload {trip_num} → {resp.status_code}: {resp.text[:800]}")
        if not resp.ok:
            _send_to(chat_id, f"❌ QM API error {resp.status_code}:\n`{resp.text[:200]}`")
            return
        data  = resp.json()
        items = data.get("data", {}).get("items", [])
        if not items:
            _send_to(chat_id, f"❌ Trip *{trip_num}* not found.\nRaw: `{str(data)[:300]}`")
            return
        trip  = items[0]
        stops = trip.get("stops") or []
        lines = [
            f"✅ *Trip #{trip_num} found*",
            f"📋 Ref: `{trip.get('ref_number','')}` | Status: `{trip.get('status','')}`",
            f"👤 {trip.get('customer_name','')}", "",
        ]
        for i, s in enumerate(stops, 1):
            addr  = s.get("address") or {}
            icon  = "📦" if s.get("pickup") else "🏁"
            stype = "Pickup" if s.get("pickup") else "Delivery"
            truck = s.get("assigned_truck") or {}
            tnum  = truck.get("number", "")
            lines += [f"{icon} *Stop {i} — {stype}*",
                      f"   {s.get('company_name','')}",
                      f"   📍 {addr.get('city','')}, {addr.get('state','')} {addr.get('zip_code','')}"]
            if tnum and tnum != "0":
                lines.append(f"   🚛 Truck: *{tnum}*")
            lines.append("")
        _send_to(chat_id, "\n".join(lines))
    except Exception as e:
        _send_to(chat_id, f"❌ Error: `{e}`")
        log.error(f"/findload error: {e}", exc_info=True)


def _handle_route(text: str, chat_id: str) -> None:
    parts = text.strip().split()
    if len(parts) < 2:
        _send_to(chat_id, "Usage: `/route 0792`")
        return
    truck_num = parts[1].strip()
    try:
        from config import QM_CLIENT_ID, QM_CLIENT_SECRET
        route = None
        if QM_CLIENT_ID and QM_CLIENT_SECRET:
            from quickmanage_client import get_route_for_truck
            route = get_route_for_truck(truck_num)
        if not route:
            from database import get_truck_route
            route = get_truck_route(truck_num)
    except Exception as e:
        _send_to(chat_id, f"❌ Error: `{e}`")
        return
    if not route:
        # Try searching QM by truck number as query string
        try:
            from config import QM_CLIENT_ID, QM_CLIENT_SECRET
            if QM_CLIENT_ID and QM_CLIENT_SECRET:
                from quickmanage_client import _get_token, _build_route, _ACTIVE_STATUSES
                token = _get_token()
                if token:
                    hdrs = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
                    resp = requests.post(
                        "https://api.quickmanage.com/x/trips/search",
                        json={"query": truck_num, "filters": [], "page": 0, "page_size": 20},
                        headers=hdrs, timeout=10
                    )
                    if resp.ok:
                        items = resp.json().get("data", {}).get("items", [])
                        for trip in items:
                            if trip.get("status","").lower() in _ACTIVE_STATUSES:
                                route = _build_route(trip, truck_num)
                                if route:
                                    break
        except Exception as e:
            log.warning(f"/route QM query fallback failed: {e}")

    if not route:
        _send_to(chat_id, f"🚛 Truck *{truck_num}*\n❌ No route found.\nRoute is saved when QM Notifier posts a trip in the driver group.")
        return
    status = route.get("status", "").lower()
    status_label = {"dispatched": "🟡 Dispatched → heading to pickup", "in_transit": "🟢 In Transit → heading to delivery"}.get(status, f"📌 {status}")
    dest   = route.get("destination", {})
    lines  = [
        f"🗺 *Truck {truck_num} — Active Load*",
        f"📋 Trip #: `{route.get('trip_num','')}` | Ref: `{route.get('ref_number','')}`",
        f"{status_label}", "",
    ]
    for i, s in enumerate(route.get("stops", []), 1):
        icon    = "📦" if s.get("pickup") else "🏁"
        stype   = "Pickup" if s.get("pickup") else "Delivery"
        city    = s.get("city") or s.get("address", {}).get("city", "") if isinstance(s.get("address"), dict) else s.get("city","")
        state   = s.get("state") or s.get("address", {}).get("state", "") if isinstance(s.get("address"), dict) else s.get("state","")
        zip_    = s.get("zip","") or s.get("address", {}).get("zip_code","") if isinstance(s.get("address"), dict) else s.get("zip","")
        company = s.get("company") or s.get("company_name","")
        loc     = f"{city}, {state} {zip_}".strip()
        stop_n  = s.get("stop_num", i)
        is_next = (city == dest.get("city") and state == dest.get("state"))
        arrow   = "  ← *NEXT*" if is_next else ""
        appt    = s.get("appointment") or s.get("appointment_time") or s.get("appt")
        lines  += [f"{icon} *Stop {stop_n} — {stype}*{arrow}", f"   {company}", f"   📍 {loc}"]
        if appt:
            lines.append(f"   🕐 {str(appt)[:16].replace('T',' ')}")
        lines.append("")
    lines.append(f"🏁 *Destination: {dest.get('city')}, {dest.get('state')}*")
    _send_to(chat_id, "\n".join(lines))


def _handle_qmload(text: str, chat_id: str) -> None:
    """/qmload <truck> - alias for active QuickManage load details by truck number."""
    _handle_route(text.replace("/qmload", "/route", 1), chat_id)



def _handle_loadroute(text: str, chat_id: str) -> None:
    parts = text.strip().split(None, 1)
    rest  = parts[1] if len(parts) > 1 else ""
    rest_lines = rest.strip().split("\n", 1)
    if rest_lines[0].strip().replace(" ","").isdigit() or (rest_lines[0].strip() and "NEW TRIP" not in rest_lines[0]):
        truck_num = rest_lines[0].strip()
        msg_text  = rest_lines[1].strip() if len(rest_lines) > 1 else ""
    else:
        truck_num = ""; msg_text = rest.strip()
    if not truck_num:
        _send_to(chat_id, "Usage: `/loadroute 630862\n<paste QM message>`")
        return
    if "NEW TRIP" not in msg_text or "HAS BEEN ASSIGNED" not in msg_text:
        _send_to(chat_id, "❌ Message must contain 'NEW TRIP X HAS BEEN ASSIGNED'")
        return
    try:
        from route_reader import parse_qm_notifier_message
        from database import save_truck_route
        route = parse_qm_notifier_message(msg_text, chat_id)
    except Exception as e:
        _send_to(chat_id, f"❌ Parse error: `{e}`")
        return
    if not route:
        _send_to(chat_id, "❌ Could not parse route.")
        return
    save_truck_route(truck_num, chat_id, route)
    o = route["origin"]; d = route["destination"]
    _send_to(chat_id, f"✅ *Route saved for Truck {truck_num}*\n📋 Trip #{route['trip_num']} | Ref: {route['ref_number']}\n🚀 From: {o['city']}, {o['state']}\n🏁 To: {d['city']}, {d['state']}\n📍 {len(route['stops'])} stops\n\nType `/route {truck_num}` to verify.")


def _handle_testroute(text: str) -> None:
    parts = text.split("\n", 1)
    if len(parts) < 2:
        _send_to(ADMIN_CHAT_ID, "Usage: `/testroute`\n`<paste QM message>`")
        return
    msg_text = parts[1].strip()
    try:
        from route_reader import parse_qm_notifier_message
        route = parse_qm_notifier_message(msg_text, "test")
    except Exception as e:
        _send_to(ADMIN_CHAT_ID, f"❌ Parser error: `{e}`")
        return
    if not route:
        _send_to(ADMIN_CHAT_ID, "❌ Could not parse. Make sure it contains 'NEW TRIP X HAS BEEN ASSIGNED'")
        return
    lines = [f"✅ *Parser Test*\n\n📋 Trip #: `{route['trip_num']}`\n📋 Ref: `{route['ref_number']}`\n"]
    for s in route["stops"]:
        icon   = "📦" if s["pickup"] else "🏁"
        coords = f"{s['lat']:.4f}, {s['lng']:.4f}" if s["lat"] else "❌ no coords"
        lines += [f"{icon} *Stop {s['stop_num']}* {'Pickup' if s['pickup'] else 'Delivery'}", f"   {s['company']}", f"   📍 {s['address']}", f"   🌐 {coords}", ""]
    o = route["origin"]; d = route["destination"]
    lines += [f"🚀 *Origin:* {o['city']}, {o['state']} ({o['lat']:.4f}, {o['lng']:.4f})",
              f"🏁 *Destination:* {d['city']}, {d['state']} ({d['lat']:.4f}, {d['lng']:.4f})"]
    _send_to(ADMIN_CHAT_ID, "\n".join(lines))


def _handle_findstop(text: str, chat_id: str):
    from database import get_all_diesel_stops_for_system, get_truck_card_system
    from samsara_client import get_combined_vehicle_data
    from truck_stop_finder import haversine_miles

    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        _send_to(chat_id, "Usage: `/findstop 0792`")
        return
    truck_number = parts[1].strip()
    try:
        vehicles = get_combined_vehicle_data()
    except Exception as e:
        _send_to(chat_id, f"❌ Could not reach Samsara: `{e}`")
        return
    truck = next((v for v in vehicles if truck_number.lower() in v.get("vehicle_name","").lower()), None)
    if not truck:
        _send_to(chat_id, f"❌ Truck *{truck_number}* not found in Samsara.")
        return
    lat = truck.get("lat"); lng = truck.get("lng")
    if not lat or not lng:
        _send_to(chat_id, f"❌ No GPS for truck *{truck.get('vehicle_name',truck_number)}*.")
        return
    fuel  = truck.get("fuel_pct", 0)
    speed = truck.get("speed_mph", 0)
    vname = truck.get("vehicle_name", truck_number)

    # Use the correct price table for this truck's card system
    card_system   = get_truck_card_system(vname)
    system_label  = "Relay (Pilot/FJ)" if card_system == 'new' else "EFS/WEX (all stops)"
    all_stops     = get_all_diesel_stops_for_system(card_system)

    nearby = sorted(
        [{ **s, "distance_miles": round(haversine_miles(lat, lng, float(s["latitude"]), float(s["longitude"])), 1)}
         for s in all_stops if haversine_miles(lat, lng, float(s["latitude"]), float(s["longitude"])) <= 50 and s.get("diesel_price")],
        key=lambda s: s["diesel_price"]
    )[:3]
    if not nearby:
        _send_to(chat_id, f"⚠️ No fuel stops within 50 miles of *{vname}* ({system_label}).\n📍 GPS: `{lat:.5f}, {lng:.5f}`")
        return
    lines = [
        f"⛽ *Fuel Stops — Truck {vname}*",
        f"💳 System: *{system_label}*",
        f"📍 ⛽ {fuel:.0f}% fuel | 🧭 {speed:.0f} mph",
        f"🌐 GPS: `{lat:.5f}, {lng:.5f}`",
        f"🔍 Top 3 cheapest within 50 miles\n",
    ]
    for i, s in enumerate(nearby, 1):
        addr = ", ".join(filter(None, [s.get("address",""), s.get("city",""), s.get("state","")]))
        lines += [f"*#{i} — {s['store_name']}*", f"📌 {addr}", f"🛣 {s['distance_miles']} mi away",
                  f"💰 Diesel: ${s['diesel_price']:.3f}/gal",
                  f"🗺 [Open in Google Maps](https://maps.google.com/?q={s['latitude']},{s['longitude']})"]
        if i < len(nearby):
            lines.append("")
    _send_to(chat_id, "\n".join(lines))


def _handle_routelist(chat_id: str) -> None:
    """/routelist — show all trucks with active QM routes"""
    try:
        from config import QM_CLIENT_ID, QM_CLIENT_SECRET
        routes = {}
        if QM_CLIENT_ID and QM_CLIENT_SECRET:
            from quickmanage_client import get_all_truck_routes
            routes = get_all_truck_routes()
        if not routes:
            from database import get_all_truck_routes_from_db
            routes = get_all_truck_routes_from_db()
    except Exception as e:
        _send_to(chat_id, f"❌ Error: `{e}`")
        return

    if not routes:
        _send_to(chat_id, "❌ No active routes found.")
        return

    status_emoji = {"dispatched": "🟡", "in_transit": "🟢", "upcoming": "🔵"}

    lines = [f"🗺 *Active Routes — {len(routes)} trucks*\n"]
    for truck_num, route in sorted(routes.items()):
        status = route.get("status", "").lower()
        emoji  = status_emoji.get(status, "⚪")
        origin = route.get("origin", {})
        dest   = route.get("destination", {})
        trip   = route.get("trip_num", "")
        o_city = f"{origin.get('city','?')}, {origin.get('state','')}"
        d_city = f"{dest.get('city','?')}, {dest.get('state','')}"
        lines.append(f"{emoji} *Truck {truck_num}* — Trip #{trip}")
        lines.append(f"   {o_city} → {d_city}")
        lines.append("")

    # Split into chunks if too long
    msg = "\n".join(lines)
    if len(msg) > 4000:
        chunks = []
        chunk  = [f"🗺 *Active Routes — {len(routes)} trucks*\n"]
        for truck_num, route in sorted(routes.items()):
            status = route.get("status", "").lower()
            emoji  = status_emoji.get(status, "⚪")
            origin = route.get("origin", {})
            dest   = route.get("destination", {})
            trip   = route.get("trip_num", "")
            line   = f"{emoji} *{truck_num}* #{trip} | {origin.get('city','?')},{origin.get('state','')} → {dest.get('city','?')},{dest.get('state','')}"
            chunk.append(line)
            if len("\n".join(chunk)) > 3800:
                chunks.append("\n".join(chunk))
                chunk = []
        if chunk:
            chunks.append("\n".join(chunk))
        for c in chunks:
            _send_to(chat_id, c)
    else:
        _send_to(chat_id, msg)


def _handle_fuelhistory(text: str, chat_id: str) -> None:
    """/fuelhistory <truck_number> — show recent fuel stop visits"""
    from database import db_cursor
    parts = text.strip().split()
    if len(parts) < 2:
        _send_to(chat_id, "Usage: `/fuelhistory 0792`")
        return

    truck_num = parts[1].strip()

    with db_cursor() as cur:
        cur.execute("""
            SELECT alerted_at, best_stop_name, best_stop_price,
                   savings_usd, alert_type, fuel_pct
            FROM fuel_alerts
            WHERE vehicle_name = %s
            ORDER BY alerted_at DESC
            LIMIT 10
        """, (truck_num,))
        rows = cur.fetchall()

        # Also check if truck actually refueled (fuel went up after alert)
        cur.execute("""
            SELECT alerted_at, fuel_pct, best_stop_name
            FROM fuel_alerts
            WHERE vehicle_name = %s AND alert_type = 'refueled'
            ORDER BY alerted_at DESC
            LIMIT 5
        """, (truck_num,))
        refueled = cur.fetchall()

    if not rows:
        _send_to(chat_id, f"❌ No fuel alert history for truck *{truck_num}*.")
        return

    header = f"⛽ *Fuel History — Truck {truck_num}*"
    lines = [header + "\n"]

    if refueled:
        lines.append("✅ *Confirmed Refuels:*")
        for r in refueled:
            dt   = r["alerted_at"].strftime("%b %d %H:%M")
            stop = r["best_stop_name"] or "Unknown stop"
            lines.append(f"   ✅ {dt} — {stop}")
        lines.append("")

    lines.append("📋 *Recent Alerts:*")
    for r in rows:
        dt    = r["alerted_at"].strftime("%b %d %H:%M")
        stop  = r["best_stop_name"] or "No stop found"
        price = f"${r['best_stop_price']:.3f}" if r["best_stop_price"] else "N/A"
        saved = f"saved ${r['savings_usd']:.0f}" if r["savings_usd"] else ""
        fuel  = f"{r['fuel_pct']:.0f}%" if r["fuel_pct"] else ""
        lines.append(f"   🟡 {dt} | ⛽{fuel} | {stop} {price} {saved}")

    _send_to(chat_id, "\n".join(lines))


def _handle_compliance(text: str, chat_id: str) -> None:
    """/compliance [truck_number] — show fuel stop compliance report"""
    from database import db_cursor
    from datetime import datetime, timezone, timedelta

    parts = text.strip().split()
    truck_num = parts[1].strip() if len(parts) > 1 else None
    now       = datetime.now(timezone.utc)
    since     = now - timedelta(days=30)

    with db_cursor() as cur:
        if truck_num:
            # Per-truck detail
            cur.execute("""
                SELECT recommended_stop_name, actual_stop_name, visited,
                       fuel_before, fuel_after, visited_at
                FROM stop_visits
                WHERE vehicle_name = %s AND created_at >= %s
                ORDER BY visited_at DESC LIMIT 15
            """, (truck_num, since))
            rows = cur.fetchall()

            if not rows:
                _send_to(chat_id, f"❌ No compliance data for truck *{truck_num}* in last 30 days.")
                return

            visited = sum(1 for r in rows if r["visited"] is True)
            skipped = sum(1 for r in rows if r["visited"] is False)
            total   = len(rows)
            pct     = round(visited / total * 100) if total else 0

            lines = [
                f"📊 *Compliance — Truck {truck_num}*",
                f"📅 Last 30 days",
                f"",
                f"✅ Visited recommended: *{visited}/{total}* ({pct}%)",
                f"⚠️ Skipped recommended: *{skipped}/{total}*",
                f"",
            ]
            for r in rows:
                dt   = r["visited_at"].strftime("%b %d %H:%M") if r["visited_at"] else "?"
                icon = "✅" if r["visited"] else "⚠️"
                rec  = r["recommended_stop_name"] or "?"
                act  = r["actual_stop_name"] or "?"
                fb   = f"{r['fuel_before']:.0f}%" if r["fuel_before"] else "?"
                fa   = f"{r['fuel_after']:.0f}%" if r["fuel_after"] else "?"
                if r["visited"]:
                    lines.append(f"{icon} {dt} | {rec} | {fb}→{fa}")
                else:
                    lines.append(f"{icon} {dt} | Rec: {rec} | Went to: {act} | {fb}→{fa}")

        else:
            # Fleet-wide summary
            cur.execute("""
                SELECT
                    COUNT(*)                                       AS total,
                    COUNT(*) FILTER (WHERE visited = TRUE)        AS visited,
                    COUNT(*) FILTER (WHERE visited = FALSE)       AS skipped,
                    COUNT(*) FILTER (WHERE visited IS NULL)       AS unknown
                FROM stop_visits WHERE created_at >= %s
            """, (since,))
            stats = dict(cur.fetchone())

            cur.execute("""
                SELECT vehicle_name,
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE visited = TRUE) AS visited,
                    COUNT(*) FILTER (WHERE visited = FALSE) AS skipped
                FROM stop_visits WHERE created_at >= %s
                GROUP BY vehicle_name
                ORDER BY (COUNT(*) FILTER (WHERE visited = FALSE)) DESC
                LIMIT 10
            """, (since,))
            trucks = cur.fetchall()

            total   = stats["total"] or 0
            visited = stats["visited"] or 0
            skipped = stats["skipped"] or 0
            pct     = round(visited / total * 100) if total else 0

            lines = [
                f"📊 *Fleet Compliance Report*",
                f"📅 Last 30 days",
                f"",
                f"✅ Visited recommended stop: *{visited}/{total}* ({pct}%)",
                f"⚠️ Skipped recommended stop: *{skipped}/{total}*",
                f"",
            ]

            if trucks:
                lines.append("🚛 *Trucks with most skips:*")
                for t in trucks:
                    if t["skipped"] > 0:
                        lines.append(f"   • Truck *{t['vehicle_name']}* — {t['skipped']} skips / {t['total']} alerts")

            lines += [
                "",
                "Type `/compliance <truck#>` for per-truck detail.",
            ]

    _send_to(chat_id, "\n".join(lines))


def _handle_stopvisits(text: str, chat_id: str) -> None:
    """/stopvisits <truck> — show all fuel stops truck entered recently"""
    from database import db_cursor
    from datetime import datetime, timezone, timedelta

    parts = text.strip().split()
    if len(parts) < 2:
        _send_to(chat_id, "Usage: `/stopvisits 2837`")
        return

    truck_num = parts[1].strip()
    since     = datetime.now(timezone.utc) - timedelta(days=7)

    with db_cursor() as cur:
        cur.execute("""
            SELECT recommended_stop_name, actual_stop_name,
                   visited, fuel_before, fuel_after, visited_at
            FROM stop_visits
            WHERE vehicle_name = %s AND created_at >= %s
            ORDER BY visited_at DESC LIMIT 20
        """, (truck_num, since))
        rows = cur.fetchall()

    if not rows:
        _send_to(chat_id,
            f"❌ No stop visits recorded for truck *{truck_num}* in last 7 days.\n"
            f"Geofence tracking requires trucks to pass within 0.25 miles of a known stop."
        )
        return

    lines = [f"📍 *Stop Visits — Truck {truck_num}* (last 7 days)\n"]
    for r in rows:
        dt      = r["visited_at"].strftime("%b %d %H:%M") if r["visited_at"] else "?"
        actual  = r["actual_stop_name"] or "Unknown"
        rec     = r["recommended_stop_name"] or "none"
        fb      = f"{r['fuel_before']:.0f}%" if r["fuel_before"] else "?"
        fa      = f"{r['fuel_after']:.0f}%" if r["fuel_after"] else "?"
        if r["visited"] is True:
            icon = "✅"
            lines.append(f"{icon} {dt} | *{actual}* | ⛽ {fb}→{fa} | followed recommendation")
        elif r["visited"] is False:
            icon = "⚠️"
            lines.append(f"{icon} {dt} | *{actual}* | ⛽ {fb}→{fa} | rec was: {rec}")
        else:
            icon = "📍"
            lines.append(f"{icon} {dt} | *{actual}* | ⛽ {fb}")

    _send_to(chat_id, "\n".join(lines))


def _handle_plan_route_from_group(chat_id: str) -> None:
    """Triggered by 'plan route' text in a driver group.

    Identifies the truck from the group, fetches GPS from Samsara, then
    returns the single BEST next fuel stop ahead — filtered by the truck's
    fuel-card system (old EFS/WEX = all stops; new Relay = Pilot/FJ only).
    """
    from database import get_truck_by_group, get_truck_card_system
    from samsara_client import get_combined_vehicle_data
    from truck_stop_finder import find_best_stops
    from config import DEFAULT_TANK_GAL, DEFAULT_MPG

    truck_row = get_truck_by_group(chat_id)
    if not truck_row:
        _send_to(chat_id, "❌ No truck is assigned to this group. Ask admin to run `/setgroup`.")
        return

    vname = truck_row["vehicle_name"]
    tank_gal = truck_row.get("tank_capacity_gal") or DEFAULT_TANK_GAL
    mpg      = truck_row.get("avg_mpg") or DEFAULT_MPG

    # Live Samsara data — GPS, fuel %, heading, speed
    try:
        vehicles = get_combined_vehicle_data()
    except Exception as e:
        _send_to(chat_id, f"❌ Could not reach Samsara: `{e}`")
        return
    truck = next((v for v in vehicles
                  if vname.lower() in v.get("vehicle_name", "").lower()), None)
    if not truck:
        _send_to(chat_id, f"❌ Truck *{vname}* not found in Samsara right now.")
        return

    lat     = truck.get("lat")
    lng     = truck.get("lng")
    fuel    = truck.get("fuel_pct") or 0
    heading = truck.get("heading") or 0
    speed   = truck.get("speed_mph") or 0
    if not lat or not lng:
        _send_to(chat_id, f"❌ No GPS for truck *{vname}* — cannot plan.")
        return

    # Find the SINGLE best next stop, filtered by this truck's card system
    card_system = get_truck_card_system(vname)
    best, _alt = find_best_stops(
        lat, lng, heading, speed, fuel, tank_gal, mpg,
        card_system=card_system,
    )

    if not best:
        scope = "Pilot/Flying J only" if card_system == 'new' else "all networks"
        _send_to(
            chat_id,
            f"⚠️ No fuel stops found ahead for truck *{vname}* "
            f"({scope}).\n📍 GPS: `{lat:.5f}, {lng:.5f}`",
        )
        return

    # Compose the message — one stop, clean
    name      = best.get("store_name", "Unknown")
    addr_part = ", ".join(filter(None, [
        best.get("address", ""), best.get("city", ""), best.get("state", ""),
    ]))
    dist      = best.get("distance_miles", 0)
    price     = best.get("diesel_price")
    net       = best.get("net_price")
    slat      = best.get("latitude")
    slng      = best.get("longitude")
    maps_url  = f"https://maps.google.com/?q={slat},{slng}" if slat and slng else None
    fill_gal  = round(tank_gal * (1 - fuel / 100), 0)

    card_label = "Relay (Pilot/Flying J only)" if card_system == 'new' else "EFS / WEX (all stops)"

    lines = [
        f"⛽ FUEL PLAN — TRUCK {vname} ⛽",
        f"⛽ Current Fuel: {fuel:.0f}%   💳 Card: {card_label}",
        f"🎯 NEXT STOP (In {dist:.0f} miles):",
        "",
        name,
    ]
    if addr_part:
        lines.append(f"📌 {addr_part}")
    if maps_url:
        lines.append(f"🗺️ [Directions]({maps_url})")
    if price:
        price_line = f"💰 Card: ${price:.3f}/gal"
        if net is not None and net != price:
            price_line += f"  (net after IFTA: ${net:.3f})"
        lines.append(price_line)
    if fill_gal > 0:
        lines += ["💧 INSTRUCTIONS:", f"Full Tank Fill (~{fill_gal:.0f} gallons)"]

    _send_to(chat_id, "\n".join(lines))


def _handle_planroute(text: str, chat_id: str) -> None:
    """/planroute <truck> — full IFTA-aware fuel plan for entire route"""
    parts = text.strip().split()
    if len(parts) < 2:
        _send_to(chat_id, "Usage: `/planroute 0792`")
        return
    truck_num = parts[1].strip()
    _send_to(chat_id, f"🗺 Planning route for truck *{truck_num}*...")
    try:
        from samsara_client import get_combined_vehicle_data
        from database import get_truck_route
        from config import QM_CLIENT_ID, QM_CLIENT_SECRET
        from route_planner import plan_route_fuel, format_route_plan

        # Get truck GPS
        vehicles = get_combined_vehicle_data()
        truck = next((v for v in vehicles if truck_num.lower() in v.get("vehicle_name","").lower()), None)
        if not truck:
            _send_to(chat_id, f"❌ Truck *{truck_num}* not found in Samsara.")
            return

        lat  = truck.get("lat")
        lng  = truck.get("lng")
        fuel = truck.get("fuel_pct", 50)
        vid  = truck.get("vehicle_id", "")

        # Get route
        route = None
        if QM_CLIENT_ID and QM_CLIENT_SECRET:
            from quickmanage_client import get_route_for_truck
            route = get_route_for_truck(truck_num)
        if not route:
            route = get_truck_route(truck_num)
        if not route:
            _send_to(chat_id, f"❌ No active route for truck *{truck_num}*. Needs active QM load.")
            return

        plan = plan_route_fuel(lat, lng, fuel, vid, route)
        msg  = format_route_plan(plan, truck_num)

        # Split if too long
        if len(msg) > 4000:
            parts_msg = [msg[i:i+3900] for i in range(0, len(msg), 3900)]
            for p in parts_msg:
                _send_to(chat_id, p)
        else:
            _send_to(chat_id, msg)

    except Exception as e:
        _send_to(chat_id, f"❌ Route plan error: `{e}`")
        log.error(f"/planroute error: {e}", exc_info=True)


def _handle_truckstats(text: str, chat_id: str) -> None:
    """/truckstats [truck] — show MPG and idle stats from Samsara"""
    from database import get_all_truck_efficiency, db_cursor
    parts = text.strip().split()

    if len(parts) >= 2:
        truck_num = parts[1].strip()
        with db_cursor() as cur:
            cur.execute(
                "SELECT * FROM truck_efficiency WHERE vehicle_name = %s",
                (truck_num,)
            )
            row = cur.fetchone()
        if not row:
            _send_to(chat_id, f"❌ No stats for truck *{truck_num}* yet. Stats update every hour.")
            return
        upd = row["updated_at"].strftime("%b %d %H:%M") if row["updated_at"] else "?"
        msg = (
            f"📊 *Truck {truck_num} — Efficiency Stats*\n"
            f"⚡ MPG (30d avg): *{row['mpg']:.1f}*\n"
            f"😴 Idle: *{row['idle_hours_30d']:.1f} hrs* ({row['idle_pct_30d']:.1f}%)\n"
            f"⛽ Fuel used (30d): *{row['fuel_used_30d']:.0f} gal*\n"
            f"🕐 Updated: {upd}"
        )
        _send_to(chat_id, msg)
    else:
        # Fleet summary
        trucks = get_all_truck_efficiency()
        if not trucks:
            _send_to(chat_id, "❌ No efficiency data yet. Updating hourly from Samsara.")
            return
        valid = [t for t in trucks if t["mpg"] and t["mpg"] > 3]
        avg_mpg = sum(t["mpg"] for t in valid) / len(valid) if valid else 0
        total_idle = sum(t["idle_hours_30d"] or 0 for t in valid)
        worst  = valid[:3] if valid else []
        best   = valid[-3:] if len(valid) >= 3 else valid

        lines = [
            f"📊 *Fleet Efficiency — Last 30 Days*",
            f"",
            f"⚡ Fleet avg MPG: *{avg_mpg:.1f}*",
            f"😴 Total idle hours: *{total_idle:.0f} hrs*",
            f"",
            f"🐢 *Worst MPG:*",
        ]
        for t in worst:
            lines.append(f"   • Truck *{t['vehicle_name']}* — {t['mpg']:.1f} MPG | {t['idle_hours_30d']:.0f}h idle")
        lines.append(f"")
        lines.append(f"🚀 *Best MPG:*")
        for t in reversed(best):
            lines.append(f"   • Truck *{t['vehicle_name']}* — {t['mpg']:.1f} MPG | {t['idle_hours_30d']:.0f}h idle")
        _send_to(chat_id, "\n".join(lines))


def _handle_updategroup(text: str, chat_id: str) -> None:
    """/updategroup <truck_name> — link this group to a truck. Saves group ID and group name automatically."""
    from database import upsert_truck_group
    parts = text.strip().split()
    if len(parts) < 2:
        _send_to(chat_id, "Usage: `/updategroup 0792`\nBot will automatically assign this group to truck 0792.")
        return
    name = parts[1].strip()
    # Auto-fetch the Telegram group title
    group_name = None
    chat_info = _post("getChat", {"chat_id": chat_id})
    if chat_info and chat_info.get("ok"):
        group_name = chat_info.get("result", {}).get("title")
    if upsert_truck_group(name, chat_id, group_name):
        name_line = f"\nGroup name: *{group_name}*" if group_name else ""
        _send_to(chat_id, f"✅ *Group assigned to Truck {name}*{name_line}\nThis group will now receive fuel alerts for truck *{name}*.")
        _send_to(ADMIN_CHAT_ID, f"🔄 *Group updated via /updategroup*\nTruck: *{name}*{name_line}\nNew group ID: `{chat_id}`")
    else:
        _send_to(chat_id, f"❌ Truck *{name}* not found.\nContact your dispatcher to register this truck.")


def _handle_flags(text: str, chat_id: str) -> None:
    """/flags [truck] — show recent driver flags with financial impact"""
    from flag_system import (
        get_flags_summary, get_total_savings_lost,
        FLAG_WRONG_STOP, FLAG_MISSED_STOP, FLAG_LOW_STOP_STATE, FLAG_LOW_FUEL,
    )
    from database import db_cursor
    from datetime import datetime, timezone, timedelta

    parts = text.strip().split()
    since = datetime.now(timezone.utc) - timedelta(days=7)

    if len(parts) >= 2:
        truck_num = parts[1].strip()
        with db_cursor() as cur:
            cur.execute("""
                SELECT flag_type, details, flagged_at, savings_lost
                FROM driver_flags
                WHERE vehicle_name = %s AND flagged_at >= %s
                ORDER BY flagged_at DESC LIMIT 10
            """, (truck_num, since))
            rows = cur.fetchall()
        if not rows:
            _send_to(chat_id, f"✅ No flags for truck *{truck_num}* in last 7 days.")
            return
        total_lost = sum(float(r.get("savings_lost") or 0) for r in rows)
        lines = [f"🚩 *Flags — Truck {truck_num}* (last 7 days)\n"]
        for r in rows:
            dt = r["flagged_at"].strftime("%b %d %H:%M")
            ft = r["flag_type"].replace("_", " ").title()
            loss = float(r.get("savings_lost") or 0)
            loss_str = f" — *${loss:.2f} lost*" if loss > 0 else ""
            lines.append(f"🚩 {dt} — *{ft}*{loss_str}")
        if total_lost > 0:
            lines.append(f"\n💸 *Total savings lost: ${total_lost:.2f}*")
        _send_to(chat_id, "\n".join(lines))
    else:
        summary = get_flags_summary(days=7)
        if not summary:
            _send_to(chat_id, "✅ No flags in the last 7 days.")
            return
        lines = ["🚩 *Driver Flags — Last 7 Days*\n"]
        icons = {
            FLAG_WRONG_STOP:    "⛽ Wrong Stop",
            FLAG_MISSED_STOP:   "🛣 Missed Stop",
            FLAG_LOW_STOP_STATE: "⚠️ Low-Stop State",
            FLAG_LOW_FUEL:       "🔋 Low Fuel Event",
        }
        for flag_type, data in summary.items():
            label  = icons.get(flag_type, flag_type)
            trucks = ", ".join(data["trucks"][:5])
            loss_str = f" — *${data.get('total_lost', 0):.2f} lost*" if data.get('total_lost', 0) > 0 else ""
            lines.append(f"*{label}:* {data['count']} times{loss_str}")
            lines.append(f"   Trucks: {trucks}")

        total_lost = get_total_savings_lost(days=7)
        if total_lost > 0:
            lines.append(f"\n💸 *Total fleet savings lost: ${total_lost:.2f}*")

        lines.append("\nType `/flags <truck#>` for per-truck detail.")
        _send_to(chat_id, "\n".join(lines))


def _send_excel_to_admin(data: bytes, filename: str, caption: str) -> None:
    """Send an Excel file to ADMIN_CHAT_ID via sendDocument. Raises on failure."""
    url  = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    resp = requests.post(
        url,
        data={"chat_id": ADMIN_CHAT_ID, "caption": caption},
        files={"document": (filename, data,
                            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        timeout=60,
    )
    if not resp.ok or not resp.json().get("ok"):
        raise RuntimeError(f"Telegram sendDocument failed: {resp.text[:200]}")


def send_weekly_truck_report() -> None:
    """Send per-truck Excel report every Monday alongside fleet summary."""
    import tempfile, os
    from truck_report import build_truck_report
    from datetime import datetime, timezone
    week = datetime.now(timezone.utc).strftime("%b %d %Y")
    try:
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
            path = f.name
        build_truck_report(path)
        with open(path, "rb") as f:
            data = f.read()
        os.unlink(path)
        _send_excel_to_admin(
            data,
            f"DieselUp_Trucks_{week.replace(' ','_')}.xlsx",
            f"📊 Per-Truck Weekly Report  |  {week}",
        )
        log.info("Per-truck Excel report sent to admin")
    except Exception as e:
        log.error(f"Truck report send failed: {e}", exc_info=True)
        _send_to(ADMIN_CHAT_ID, f"❌ *Weekly per-truck Excel failed:*\n`{e}`")


def send_weekly_fleet_excel() -> None:
    """Send 4-sheet fleet summary Excel (Summary, Compliance, Flags, IFTA)."""
    import tempfile, os
    from weekly_report import get_real_data, build_report
    from datetime import datetime, timezone
    week = datetime.now(timezone.utc).strftime("%b %d %Y")
    try:
        summary, compliance, flags, ifta = get_real_data(days=7)
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
            path = f.name
        build_report(summary, compliance, flags, ifta, path)
        with open(path, "rb") as f:
            data = f.read()
        os.unlink(path)
        _send_excel_to_admin(
            data,
            f"DieselUp_Fleet_{week.replace(' ','_')}.xlsx",
            f"📋 Fleet Weekly Report  |  {week}\n4 sheets: Summary · Compliance · Flags · IFTA",
        )
        log.info("Fleet summary Excel report sent to admin")
    except Exception as e:
        log.error(f"Fleet Excel report failed: {e}", exc_info=True)
        _send_to(ADMIN_CHAT_ID, f"❌ *Weekly fleet Excel failed:*\n`{e}`")


def send_weekly_savings_report() -> None:
    """Weekly owner report — savings, IFTA analysis, compliance. Owner only, not drivers."""
    from database import db_cursor
    from datetime import datetime, timezone, timedelta
    now      = datetime.now(timezone.utc)
    week_ago = now - timedelta(days=7)

    # ── Core stats ──────────────────────────────────────────────────────────
    with db_cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) AS total_alerts,
                   COUNT(DISTINCT vehicle_name) AS trucks_active,
                   COALESCE(SUM(savings_usd),0) AS total_savings,
                   COUNT(*) FILTER (WHERE savings_usd > 0) AS alerts_with_savings
            FROM fuel_alerts WHERE alerted_at >= %s
        """, (week_ago,))
        stats = dict(cur.fetchone())

        cur.execute("""
            SELECT vehicle_name, COALESCE(SUM(savings_usd),0) AS saved, COUNT(*) AS alerts
            FROM fuel_alerts WHERE alerted_at >= %s
            GROUP BY vehicle_name ORDER BY saved DESC LIMIT 5
        """, (week_ago,))
        top_trucks = cur.fetchall()

        # IFTA data — fuel purchased by state this week
        try:
            cur.execute("""
                SELECT actual_stop_state,
                       COUNT(*) AS stops,
                       COALESCE(SUM(gallons_purchased),0) AS total_gal
                FROM stop_visits
                WHERE visited_at >= %s
                  AND actual_stop_state IS NOT NULL
                  AND gallons_purchased > 0
                GROUP BY actual_stop_state
                ORDER BY total_gal DESC
                LIMIT 8
            """, (week_ago,))
            ifta_by_state = cur.fetchall()
        except Exception:
            ifta_by_state = []

        # Compliance
        cur.execute("""
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE visited=TRUE)  AS visited,
                   COUNT(*) FILTER (WHERE visited=FALSE) AS skipped
            FROM stop_visits WHERE visited_at >= %s
        """, (week_ago,))
        compliance = dict(cur.fetchone())

        # Truck efficiency summary
        cur.execute("""
            SELECT AVG(mpg) AS fleet_mpg,
                   SUM(idle_hours_30d) AS total_idle,
                   SUM(fuel_used_30d) AS total_fuel
            FROM truck_efficiency
        """)
        eff = cur.fetchone()

    total_savings = float(stats["total_savings"] or 0)
    week_start    = week_ago.strftime("%b %d")
    week_end      = now.strftime("%b %d, %Y")

    lines = [
        f"📊 *FleetFuel AI — Weekly Owner Report*",
        f"📅 {week_start} – {week_end}",
        f"─────────────────────────────",
        f"",
        f"🚛 Trucks monitored:    *{stats['trucks_active']}*",
        f"⚡ Alerts fired:         *{stats['total_alerts']}*",
        f"💡 Alerts with savings: *{stats['alerts_with_savings']}*",
        f"",
        f"💰 *Total Diesel Savings: ${total_savings:,.2f}*",
    ]

    # ── V2 Flag System Summary ──────────────────────────────────────────────
    try:
        from flag_system import get_flags_summary, get_total_savings_lost
        flag_summary = get_flags_summary(days=7)
        total_lost = get_total_savings_lost(days=7)

        if flag_summary or total_lost > 0:
            lines += [
                "",
                "─────────────────────────────",
                "🚩 *Driver Accountability Flags:*",
            ]
            flag_icons = {
                "WRONG_STOP":     "⛽ Wrong Stop",
                "MISSED_STOP":    "🛣 Missed Stop",
                "LOW_FUEL":       "🔋 Low Fuel Event",
                "LOW_STOP_STATE": "⚠️ Low-Stop State",
                "UNPLANNED_STOP": "🚦 Unplanned Stop",
            }
            for flag_type, data in flag_summary.items():
                label = flag_icons.get(flag_type, flag_type)
                loss_str = f" — *${data.get('total_lost', 0):.2f} lost*" if data.get('total_lost', 0) > 0 else ""
                lines.append(f"   {label}: *{data['count']}*{loss_str}")
            if total_lost > 0:
                lines.append(f"   💸 *Total savings lost to flags: ${total_lost:.2f}*")
    except Exception as fe:
        log.warning(f"Weekly report flag summary failed: {fe}")

    # Top trucks
    if top_trucks:
        lines += ["", "🏅 *Top Trucks — Most Saved:*"]
        medals = ["🥇","🥈","🥉","4️⃣","5️⃣"]
        for i, t in enumerate(top_trucks):
            lines.append(f"   {medals[i]} Truck *{t['vehicle_name']}* — ${float(t['saved']):.2f} ({t['alerts']} alerts)")

    # ── IFTA Section (owner only) ────────────────────────────────────────────
    lines += [
        "",
        "─────────────────────────────",
        "📋 *IFTA Analysis — Home State: Indiana*",
        "",
    ]

    if ifta_by_state:
        try:
            from ifta import get_ifta_rate, HOME_STATE_RATE
            total_gal_all = 0.0
            total_adj     = 0.0
            ifta_lines    = []

            for r in ifta_by_state:
                state    = r["actual_stop_state"] or "?"
                gal      = float(r["total_gal"] or 0)
                rate     = get_ifta_rate(state)
                net_rate = HOME_STATE_RATE - rate  # adjustment per gallon
                adj_cost = net_rate * gal           # + means owe IN, - means credit

                total_gal_all += gal
                total_adj     += adj_cost

                sign = "⚠️ owes IN" if net_rate > 0 else "✅ credit"
                ifta_lines.append(
                    f"   {state}: {gal:.0f} gal | "
                    f"IFTA adj: ${net_rate:+.3f}/gal = ${adj_cost:+.2f} | {sign}"
                )

            lines += ifta_lines
            lines += [
                "",
                f"⛽ Total fuel purchased: *{total_gal_all:.0f} gal*",
                f"📋 IFTA settlement (est.): *${total_adj:+,.2f}*",
            ]
        except Exception as e:
            lines.append(f"   _(IFTA calculation error: {e})_")
    else:
        lines.append("   _(No refuel data recorded this week)_")

    # ── Fleet Efficiency ─────────────────────────────────────────────────────
    if eff and eff["fleet_mpg"]:
        lines += [
            "",
            "─────────────────────────────",
            "⚡ *Fleet Efficiency (30 day):*",
            f"   MPG avg: *{float(eff['fleet_mpg']):.1f}*",
            f"   Total idle: *{float(eff['total_idle'] or 0):.0f} hrs*",
            f"   Total fuel used: *{float(eff['total_fuel'] or 0):.0f} gal*",
        ]

    # ── Compliance ───────────────────────────────────────────────────────────
    if compliance["total"]:
        cpct = round((compliance["visited"] or 0) / compliance["total"] * 100)
        lines += [
            "",
            "─────────────────────────────",
            f"🎯 *Stop Compliance:* {compliance['visited']}/{compliance['total']} followed recommendation ({cpct}%)",
            f"⚠️ Skipped: {compliance['skipped']} | `/compliance` for details",
        ]

    lines += ["", "─────────────────────────────", "⚙️ _FleetFuel AI — Owner Report (confidential)_"]

    msg = "\n".join(lines)

    # Send ONLY to admin (owner) — never to dispatcher group or driver groups
    _send_to(ADMIN_CHAT_ID, msg)


def _handle_active(chat_id: str) -> None:
    """/active — fleet-wide activity status summary from the truck classifier."""
    from database import db_cursor
    from datetime import datetime, timezone

    try:
        with db_cursor() as cur:
            cur.execute("""
                SELECT activity_status,
                       COUNT(*)               AS cnt,
                       MAX(status_updated_at) AS last_update
                  FROM trucks
                 WHERE is_active = TRUE
                 GROUP BY activity_status
                 ORDER BY cnt DESC
            """)
            rows = cur.fetchall()
    except Exception as e:
        _send_to(chat_id, f"❌ Error querying truck statuses: `{e}`")
        return

    counts: dict = {}
    last_update   = None

    for row in rows:
        key = row["activity_status"] or "unclassified"
        counts[key] = int(row["cnt"])
        ts = row["last_update"]
        if ts and (last_update is None or ts > last_update):
            last_update = ts

    if not counts:
        _send_to(chat_id, "No truck status data yet — classifier runs every 30 min after startup.")
        return

    total = sum(counts.values())
    STATUS_DISPLAY = [
        ("active",       "🟢 Active"),
        ("idle",         "🟡 Idle"),
        ("at_yard",      "🏠 At Yard"),
        ("in_shop",      "🔧 In Shop"),
        ("unassigned",   "⚪ Unassigned"),
        ("unknown",      "❓ Unknown"),
        ("unclassified", "➖ Unclassified"),
    ]

    lines = [f"🚛 *Fleet Status ({total} trucks)*"]
    for key, label in STATUS_DISPLAY:
        n = counts.get(key, 0)
        if n:
            lines.append(f"{label}: {n}")

    if last_update:
        now = datetime.now(timezone.utc)
        if last_update.tzinfo is None:
            last_update = last_update.replace(tzinfo=timezone.utc)
        mins = max(0, int((now - last_update).total_seconds() / 60))
        lines.append(f"\n_Last updated: {mins} min ago_")

    _send_to(chat_id, "\n".join(lines))


def _handle_classify_truck(text: str, chat_id: str) -> None:
    """/classify_truck <unit> — show current classifier status + raw signals for one truck."""
    import json as _json
    from database import db_cursor
    from datetime import datetime, timezone
    from config import CLASSIFIER_ENFORCEMENT_MODE

    parts = text.strip().split()
    if len(parts) < 2:
        _send_to(chat_id, "Usage: `/classify_truck 0792`")
        return

    unit = parts[1].strip()

    with db_cursor() as cur:
        cur.execute(
            """
            SELECT vehicle_name, activity_status, status_updated_at, status_signals
              FROM trucks
             WHERE vehicle_name ILIKE %s AND is_active = TRUE
             LIMIT 1
            """,
            (f"%{unit}%",),
        )
        row = cur.fetchone()

    if not row:
        _send_to(chat_id, f"❌ No active truck matching `{unit}` found in DB.")
        return

    name    = row["vehicle_name"]
    status  = row["activity_status"] or "unclassified"
    updated = row["status_updated_at"]
    signals = row["status_signals"]

    STATUS_EMOJI = {
        "active":       "🟢",
        "idle":         "🟡",
        "at_yard":      "🏠",
        "in_shop":      "🔧",
        "unassigned":   "⚪",
        "unknown":      "❓",
        "unclassified": "➖",
    }
    emoji = STATUS_EMOJI.get(status, "❓")

    lines = [f"🚛 *Truck {name} — Classifier*", f"{emoji} Status: *{status}*"]

    if signals:
        try:
            sig = signals if isinstance(signals, dict) else _json.loads(signals)
            lines.append("\n*Signals:*")
            lines.append(f"  miles\\_driven\\_48h:    {sig.get('miles_driven_48h', 'n/a')}")
            lines.append(f"  engine\\_hours\\_24h:    {sig.get('engine_hours_24h', 'n/a')}")
            lines.append(f"  hours\\_at\\_yard:       {sig.get('hours_at_yard', 'n/a')}")
            lines.append(f"  hours\\_at\\_shop:       {sig.get('hours_at_shop', 'n/a')}")
            lines.append(f"  has\\_assigned\\_driver: {sig.get('has_assigned_driver', 'n/a')}")
        except Exception:
            lines.append("_(signals unavailable)_")
    else:
        lines.append("_(not yet classified — classifier runs every 30 min)_")

    if updated:
        now = datetime.now(timezone.utc)
        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=timezone.utc)
        mins = max(0, int((now - updated).total_seconds() / 60))
        lines.append(f"\n_Updated {mins} min ago_")

    lines.append(f"_Enforcement mode: `{CLASSIFIER_ENFORCEMENT_MODE}`_")

    _send_to(chat_id, "\n".join(lines))
    log.info(f"Weekly owner report sent — ${total_savings:,.2f} savings")


def _handle_weeklyreport() -> None:
    """/weeklyreport — manually resend all weekly Excel reports right now."""
    _send_to(ADMIN_CHAT_ID, "📊 Generating weekly reports now...")
    send_weekly_savings_report()
    send_weekly_fleet_excel()
    send_weekly_truck_report()


def _handle_syncsamsara(text: str) -> None:
    """/syncsamsara [new] — register/re-activate all Samsara trucks missing from DB.
    Add 'new' to also set them all to Relay card system.
    """
    from database import auto_register_truck
    from samsara_client import get_combined_vehicle_data

    set_new_card = "new" in text.lower()

    _send_to(ADMIN_CHAT_ID, "🔄 Pulling truck list from Samsara...")
    try:
        vehicles = get_combined_vehicle_data()
    except Exception as e:
        _send_to(ADMIN_CHAT_ID, f"❌ Samsara error: `{e}`")
        return

    changed    = []   # newly registered or re-activated
    already_ok = 0

    for v in vehicles:
        vname = v.get("vehicle_name", "").strip()
        vid   = v.get("vehicle_id",   "").strip()
        if not vname:
            continue
        if auto_register_truck(vid, vname):
            changed.append(vname)
        else:
            already_ok += 1

    if set_new_card:
        from database import set_truck_card_system
        for v in vehicles:
            vname = v.get("vehicle_name", "").strip()
            if vname:
                set_truck_card_system(vname, 'new')

    lines = [f"📡 *Samsara sync complete* — {len(vehicles)} trucks in Samsara"]
    if changed:
        truck_lines = "\n".join(f"  • {n}" for n in changed)
        lines.append(f"✅ *Added/re-activated ({len(changed)}):*\n{truck_lines}")
    else:
        lines.append(f"✅ All {already_ok} trucks already registered and active.")
    if set_new_card:
        lines.append(f"🔄 All trucks set to *new Relay card* (Pilot/FJ only).")
    _send_to(ADMIN_CHAT_ID, "\n\n".join(lines))


def _handle_dedupetrucks() -> None:
    """/dedupetrucks — remove duplicate truck DB rows, keep the most complete name."""
    from database import cleanup_duplicate_trucks
    try:
        removed = cleanup_duplicate_trucks()
        if not removed:
            _send_to(ADMIN_CHAT_ID, "✅ No duplicate trucks found.")
            return
        lines = ["🧹 *Duplicate trucks cleaned up:*"]
        for keep, dupe in removed:
            lines.append(f"  • Kept `{keep}` — removed `{dupe}`")
        _send_to(ADMIN_CHAT_ID, "\n".join(lines))
    except Exception as e:
        _send_to(ADMIN_CHAT_ID, f"❌ Dedupe failed: `{e}`")


def _handle_setnewsystem(text: str) -> None:
    """/setnewsystem <truck1> [truck2 ...] — move trucks to new Relay card (Pilot/FJ only).
    Auto-registers trucks from Samsara if they aren't in the DB yet.
    """
    from database import set_truck_card_system, auto_register_truck, get_all_registered_trucks
    raw = text.strip()
    after_cmd = raw.split(None, 1)[1] if len(raw.split()) > 1 else ""
    if not after_cmd:
        _send_to(ADMIN_CHAT_ID, "Usage: `/setnewsystem all` or `/setnewsystem 1079 1190 1655`")
        return

    # /setnewsystem all — move every active truck at once
    if after_cmd.strip().lower() == "all":
        trucks = get_all_registered_trucks()
        count = 0
        for t in trucks:
            if set_truck_card_system(t["vehicle_name"], 'new'):
                count += 1
        _send_to(ADMIN_CHAT_ID,
            f"✅ *All trucks moved to new Relay card (Pilot/Flying J only)*\n"
            f"{len(trucks)} trucks updated.")
        return

    tokens = [t.strip().strip(",") for t in after_cmd.replace(",", " ").split() if t.strip().strip(",")]
    tokens = list(dict.fromkeys(tokens))  # deduplicate, preserve order

    moved_full  = []  # actual DB names successfully updated
    registered  = []  # trucks auto-registered from Samsara then moved
    not_found   = []  # not in DB and not in Samsara

    # Build Samsara lookup once for auto-registration of missing trucks
    samsara_map = {}  # partial_number -> vehicle_name as stored in Samsara
    try:
        from samsara_client import get_combined_vehicle_data
        vehicles = get_combined_vehicle_data()
        for v in vehicles:
            vname = v.get("vehicle_name", "")
            vid   = v.get("vehicle_id", "")
            samsara_map[vname] = (vname, vid)
    except Exception:
        pass  # Samsara unavailable — only DB matching will work

    for token in tokens:
        matched = set_truck_card_system(token, 'new')
        if matched:
            moved_full.extend(matched)
            continue

        # Not in DB — try to find in Samsara by number prefix and auto-register
        samsara_match = next(
            ((vname, vid) for vname, vid in samsara_map.values()
             if vname == token
             or vname.startswith(token + ' ')
             or vname.startswith(token + '-')),
            None
        )
        if samsara_match:
            s_name, s_vid = samsara_match
            try:
                auto_register_truck(s_vid, s_name)
                # Now set card system on the newly registered truck
                matched2 = set_truck_card_system(token, 'new')
                if matched2:
                    registered.extend(matched2)
                    continue
            except Exception as e:
                log.warning(f"Auto-register failed for {token}: {e}")

        not_found.append(token)

    lines = []
    all_moved = moved_full + registered
    if all_moved:
        truck_list = "\n".join(f"  • {n}" for n in all_moved)
        lines.append(f"✅ *Moved to new Relay card (Pilot/Flying J only):*\n{truck_list}")
    if registered:
        lines.append(f"_({len(registered)} auto-registered from Samsara)_")
    if not_found:
        lines.append(
            f"❌ *Not found in DB or Samsara:* " + ", ".join(f"`{n}`" for n in not_found) +
            f"\nCheck truck numbers or run `/listtruck` to see registered names."
        )
    _send_to(ADMIN_CHAT_ID, "\n\n".join(lines) or "No trucks updated.")


def _handle_setoldsystem(text: str) -> None:
    """/setoldsystem <truck1> [truck2 ...] — move trucks back to old EFS card (all stops).
    Auto-registers trucks from Samsara if they aren't in the DB yet.
    """
    from database import set_truck_card_system, auto_register_truck, get_all_registered_trucks
    raw = text.strip()
    after_cmd = raw.split(None, 1)[1] if len(raw.split()) > 1 else ""
    if not after_cmd:
        _send_to(ADMIN_CHAT_ID, "Usage: `/setoldsystem all` or `/setoldsystem 1079 1190 1655`")
        return

    # /setoldsystem all — move every active truck at once
    if after_cmd.strip().lower() == "all":
        trucks = get_all_registered_trucks()
        count = 0
        for t in trucks:
            if set_truck_card_system(t["vehicle_name"], 'old'):
                count += 1
        _send_to(ADMIN_CHAT_ID,
            f"✅ *All trucks moved back to old EFS card (all stops)*\n"
            f"{len(trucks)} trucks updated.")
        return

    tokens = [t.strip().strip(",") for t in after_cmd.replace(",", " ").split() if t.strip().strip(",")]
    tokens = list(dict.fromkeys(tokens))  # deduplicate, preserve order

    moved_full  = []  # actual DB names successfully updated
    registered  = []  # trucks auto-registered from Samsara then moved
    not_found   = []  # not in DB and not in Samsara

    # Build Samsara lookup once for auto-registration of missing trucks
    samsara_map = {}  # partial_number -> vehicle_name as stored in Samsara
    try:
        from samsara_client import get_combined_vehicle_data
        vehicles = get_combined_vehicle_data()
        for v in vehicles:
            vname = v.get("vehicle_name", "")
            vid   = v.get("vehicle_id", "")
            samsara_map[vname] = (vname, vid)
    except Exception:
        pass  # Samsara unavailable — only DB matching will work

    for token in tokens:
        matched = set_truck_card_system(token, 'old')
        if matched:
            moved_full.extend(matched)
            continue

        # Not in DB — try to find in Samsara by number prefix and auto-register
        samsara_match = next(
            ((vname, vid) for vname, vid in samsara_map.values()
             if vname == token
             or vname.startswith(token + ' ')
             or vname.startswith(token + '-')),
            None
        )
        if samsara_match:
            s_name, s_vid = samsara_match
            try:
                auto_register_truck(s_vid, s_name)
                # Now set card system on the newly registered truck
                matched2 = set_truck_card_system(token, 'old')
                if matched2:
                    registered.extend(matched2)
                    continue
            except Exception as e:
                log.warning(f"Auto-register failed for {token}: {e}")

        not_found.append(token)

    lines = []
    all_moved = moved_full + registered
    if all_moved:
        truck_list = "\n".join(f"  • {n}" for n in all_moved)
        lines.append(f"✅ *Moved back to old EFS card (all stops):*\n{truck_list}")
    if registered:
        lines.append(f"_({len(registered)} auto-registered from Samsara)_")
    if not_found:
        lines.append(
            f"❌ *Not found in DB or Samsara:* " + ", ".join(f"`{n}`" for n in not_found) +
            f"\nCheck truck numbers or run `/listtruck` to see registered names."
        )
    _send_to(ADMIN_CHAT_ID, "\n\n".join(lines) or "No trucks updated.")


# -- Driver group change detection handlers -----------------------------------

def _handle_chat_member_event(evt: dict) -> None:
    """Handle chat_member update (requires bot to be admin in group)."""
    from database import get_truck_by_group, log_driver_group_event, set_truck_alert_paused
    try:
        chat    = evt.get("chat", {})
        gid     = str(chat.get("id", ""))
        old_s   = evt.get("old_chat_member", {}).get("status", "")
        new_s   = evt.get("new_chat_member", {}).get("status", "")
        user    = evt.get("new_chat_member", {}).get("user", {})
        is_bot  = user.get("is_bot", False)
        uname   = user.get("first_name", "") + (" " + user.get("last_name", "")).rstrip()

        if is_bot:
            return  # bot join/leave handled by my_chat_member

        truck = get_truck_by_group(gid)
        if not truck:
            return  # not a truck group

        vname = truck["vehicle_name"]

        # Member joined
        if old_s in ("left", "kicked") and new_s in ("member", "administrator", "restricted"):
            log_driver_group_event(vname, "joined", gid, uname, "realtime")
            set_truck_alert_paused(vname, False, None, group_verified=False)
            _send_to(gid,
                f"👋 *DieselUp Bot here.* You're assigned to Truck *{vname}*.\n"
                f"Reply /confirm to activate fuel alerts.\n"
                f"If you're not the driver, reply /wrong"
            )
            _send_to(ADMIN_CHAT_ID,
                f"👤 New member in *Truck {vname}* group — awaiting driver confirmation\n"
                f"Group: `{gid}`  Member: {uname}"
            )

        # Member left or kicked
        elif old_s in ("member", "administrator", "restricted") and new_s in ("left", "kicked"):
            log_driver_group_event(vname, "left", gid, uname, "realtime")
            set_truck_alert_paused(vname, True, "driver_left", group_verified=False)
            _send_to(ADMIN_CHAT_ID,
                f"🚪 *{vname} — Driver Left Group*\n"
                f"Group: `{gid}`\n"
                f"Driver: {uname}\n"
                f"Alerts: PAUSED ⏸\n"
                f"To reassign: `/assigndriver {vname} NAME {gid}`"
            )
    except Exception as e:
        log.warning(f"_handle_chat_member_event error: {e}")


def _handle_new_members_message(chat_id: str, new_members: list) -> None:
    """Fallback: new_chat_members in message update (works without bot-admin)."""
    from database import get_truck_by_group, log_driver_group_event, set_truck_alert_paused
    try:
        truck = get_truck_by_group(chat_id)
        if not truck:
            return
        vname = truck["vehicle_name"]
        for user in new_members:
            if user.get("is_bot"):
                continue
            uname = user.get("first_name", "") + (" " + user.get("last_name", "")).rstrip()
            log_driver_group_event(vname, "joined", chat_id, uname, "realtime")
            set_truck_alert_paused(vname, False, None, group_verified=False)
            _send_to(chat_id,
                f"👋 *DieselUp Bot here.* You're assigned to Truck *{vname}*.\n"
                f"Reply /confirm to activate fuel alerts.\n"
                f"If you're not the driver, reply /wrong"
            )
            _send_to(ADMIN_CHAT_ID,
                f"👤 New member in *Truck {vname}* group — awaiting driver confirmation\n"
                f"Group: `{chat_id}`  Member: {uname}"
            )
    except Exception as e:
        log.warning(f"_handle_new_members_message error: {e}")


def _handle_left_member_message(chat_id: str, user: dict) -> None:
    """Fallback: left_chat_member in message update."""
    from database import get_truck_by_group, log_driver_group_event, set_truck_alert_paused
    try:
        truck = get_truck_by_group(chat_id)
        if not truck:
            return
        vname = truck["vehicle_name"]
        uname = user.get("first_name", "") + (" " + user.get("last_name", "")).rstrip()
        log_driver_group_event(vname, "left", chat_id, uname, "realtime")
        set_truck_alert_paused(vname, True, "driver_left", group_verified=False)
        _send_to(ADMIN_CHAT_ID,
            f"🚪 *{vname} — Driver Left Group*\n"
            f"Driver: {uname}\nGroup: `{chat_id}`\n"
            f"Alerts: PAUSED ⏸\n"
            f"To reassign: `/assigndriver {vname} NAME {chat_id}`"
        )
    except Exception as e:
        log.warning(f"_handle_left_member_message error: {e}")


def _handle_driver_confirm(chat_id: str, message: dict) -> None:
    """/confirm — driver confirms they are the correct person in this group."""
    from database import get_truck_by_group, set_truck_group_verified, log_driver_group_event
    truck = get_truck_by_group(chat_id)
    if not truck:
        return
    vname  = truck["vehicle_name"]
    sender = message.get("from", {})
    uname  = sender.get("first_name", "Driver") + (" " + sender.get("last_name", "")).rstrip()
    set_truck_group_verified(vname, True)
    log_driver_group_event(vname, "verified", chat_id, uname, "realtime")
    _send_to(chat_id, f"✅ Confirmed. Fuel alerts are active for Truck *{vname}*.")
    _send_to(ADMIN_CHAT_ID,
        f"✅ *Truck {vname}* — {uname} driver confirmed — alerts resumed"
    )


def _handle_driver_wrong(chat_id: str, message: dict) -> None:
    """/wrong — person in group is not the assigned driver."""
    from database import get_truck_by_group, set_truck_alert_paused, log_driver_group_event
    truck = get_truck_by_group(chat_id)
    if not truck:
        return
    vname = truck["vehicle_name"]
    set_truck_alert_paused(vname, True, "wrong_driver", group_verified=False)
    log_driver_group_event(vname, "deactivated", chat_id, detected_by="realtime")
    _send_to(chat_id, "⛔ Understood. Alerts paused. Admin has been notified.")
    _send_to(ADMIN_CHAT_ID,
        f"⚠️ *Truck {vname} — Wrong driver in group*\n"
        f"Group: `{chat_id}`\nAlerts: PAUSED ⏸\n"
        f"Action needed: `/assigndriver {vname} NAME {chat_id}`"
    )


# -- Layer 2: heartbeat group verification ------------------------------------

def verify_driver_groups() -> None:
    """Check every active truck's Telegram group — pause alerts if group is empty or bot was kicked.
    Called from a background thread in main.py every 6 hours.

    FIX (2026-05-08): Previously treated ANY failed API call (network error, rate limit,
    Telegram 5xx) as a bot-kick, causing mass false-positive alerts on restart.
    Now uses raw HTTP so we can inspect the actual error_code:
      - 403 Forbidden  → bot was genuinely kicked — pause + alert
      - anything else  → transient error — skip silently, do NOT pause alerts
    Also adds 0.3s sleep between calls to avoid triggering rate limits on large fleets.
    """
    from database import get_all_registered_trucks, set_truck_alert_paused, log_driver_group_event
    trucks = get_all_registered_trucks()
    log.info(f"verify_driver_groups: checking {len(trucks)} trucks")
    real_kicks = []   # (vname, gid) — confirmed 403 only

    for truck in trucks:
        vname = truck["vehicle_name"]
        gid   = truck.get("telegram_group_id")
        if not gid:
            continue
        try:
            # Raw request — we need the JSON body even on 4xx to read error_code.
            # _post() would swallow 4xx into None, making 403 indistinguishable from
            # a network timeout or a 429 rate-limit error.
            resp = requests.post(
                f"{BASE_URL}/getChatMemberCount",
                json={"chat_id": gid},
                timeout=10,
            )
            data = resp.json()

            if not data.get("ok"):
                error_code = data.get("error_code", 0)
                description = data.get("description", "unknown error")

                if error_code == 403:
                    # Confirmed: bot was actually kicked or banned from this group
                    set_truck_alert_paused(vname, True, "bot_kicked", group_verified=False)
                    log_driver_group_event(vname, "deactivated", gid, detected_by="heartbeat")
                    real_kicks.append((vname, gid))
                    log.warning(f"verify_driver_groups: {vname} — bot kicked (403): {description}")
                else:
                    # Transient error (400 bad request, 429 rate limit, 500 server error, etc.)
                    # Do NOT pause alerts — this is not a real kick.
                    log.warning(
                        f"verify_driver_groups: transient error for {vname} "
                        f"(code={error_code}, desc={description}) — skipping, alerts unchanged"
                    )
                time.sleep(0.3)   # rate-limit safety
                continue

            count = data.get("result", 0)
            if count < 2:
                set_truck_alert_paused(vname, True, "group_empty", group_verified=False)
                log_driver_group_event(vname, "deactivated", gid, detected_by="heartbeat")
                _send_to(ADMIN_CHAT_ID,
                    f"⚠️ *Truck {vname} group is empty — alerts paused*\n"
                    f"Group: `{gid}`  Members: {count}"
                )

        except Exception as e:
            log.warning(f"verify_driver_groups: skipping {vname}: {e}")

        time.sleep(0.3)   # rate-limit safety between every group check

    # Send one alert per genuinely kicked truck (only fires for real 403s)
    for vname, gid in real_kicks:
        _send_to(ADMIN_CHAT_ID,
            f"🚨 *Bot removed from Truck {vname} group — alerts stopped*\n"
            f"Group: `{gid}`\n"
            f"To reassign: `/assigndriver {vname} NAME {gid}`"
        )
    log.info(f"verify_driver_groups: done — {len(real_kicks)} real kicks, "
             f"{len([t for t in trucks if t.get('telegram_group_id')])} groups checked")


# -- Layer 3: admin driver management commands --------------------------------

def _handle_assigndriver(text: str) -> None:
    """/assigndriver <vehicle_name_or_number> <driver_name...> <group_id>"""
    from database import assign_driver, log_driver_group_event, resolve_truck_by_number
    parts = text.strip().split()
    # Minimum: /assigndriver TRUCK FIRSTNAME GROUP_ID  → 4 tokens
    if len(parts) < 4:
        _send_to(ADMIN_CHAT_ID,
            "Usage: `/assigndriver 0792 John Smith -1001234567890`\n"
            "Last argument must be the Telegram group ID."
        )
        return
    token    = parts[1]
    group_id = parts[-1]
    dname    = " ".join(parts[2:-1])

    vname = resolve_truck_by_number(token)
    if not vname:
        _send_to(ADMIN_CHAT_ID, f"❌ Truck `{token}` not found in DB.")
        return

    assign_driver(vname, dname, group_id, assigned_by="admin")
    log_driver_group_event(vname, "reassigned", group_id, dname, "admin")

    # Send verification message to the new group
    _send_to(group_id,
        f"👋 *DieselUp Bot here.* {dname} has been assigned to Truck *{vname}*.\n"
        f"Reply /confirm to activate fuel alerts.\n"
        f"If you're not the driver, reply /wrong"
    )
    _send_to(ADMIN_CHAT_ID,
        f"✅ *Truck {vname}* reassigned to *{dname}* — verification sent to group `{group_id}`"
    )


def _handle_removedriver(text: str) -> None:
    """/removedriver <vehicle_name_or_number>"""
    from database import remove_driver, log_driver_group_event, resolve_truck_by_number
    parts = text.strip().split()
    if len(parts) < 2:
        _send_to(ADMIN_CHAT_ID, "Usage: `/removedriver 0792`")
        return
    vname = resolve_truck_by_number(parts[1])
    if not vname:
        _send_to(ADMIN_CHAT_ID, f"❌ Truck `{parts[1]}` not found in DB.")
        return
    remove_driver(vname)
    log_driver_group_event(vname, "deactivated", detected_by="admin")
    _send_to(ADMIN_CHAT_ID, f"✅ *Truck {vname}* alerts paused — no active driver")


def _handle_whodrives(text: str) -> None:
    """/whodrives <vehicle_name_or_number>"""
    from database import get_active_driver, get_truck_config, resolve_truck_by_number
    from datetime import datetime, timezone
    parts = text.strip().split()
    if len(parts) < 2:
        _send_to(ADMIN_CHAT_ID, "Usage: `/whodrives 0792`")
        return
    vname = resolve_truck_by_number(parts[1])
    if not vname:
        _send_to(ADMIN_CHAT_ID, f"❌ Truck `{parts[1]}` not found in DB.")
        return

    cfg    = get_truck_config(vname) or {}
    driver = get_active_driver(vname)

    dname   = driver["driver_name"] if driver else "None assigned"
    gid     = cfg.get("telegram_group_id") or "—"
    paused  = cfg.get("alert_paused", False)
    reason  = cfg.get("pause_reason") or ""
    verified = cfg.get("group_verified", False)
    alert_s  = "Active ✅" if not paused else f"Paused ⏸ ({reason})"
    ver_s    = "✅ Verified" if verified else "❌ Unverified"

    lines = [
        f"🚛 *Truck {vname}*",
        f"Driver: {dname}",
        f"Group: `{gid}`",
        f"Verified: {ver_s}",
        f"Alerts: {alert_s}",
    ]
    if driver and driver.get("assigned_at"):
        ts = driver["assigned_at"]
        if hasattr(ts, "strftime"):
            lines.append(f"Assigned: {ts.strftime('%b %d %H:%M UTC')}")
    _send_to(ADMIN_CHAT_ID, "\n".join(lines))


def _handle_driverlog(text: str) -> None:
    """/driverlog <vehicle_name_or_number> — show last 10 driver group events."""
    from database import get_driver_event_log, resolve_truck_by_number
    parts = text.strip().split()
    if len(parts) < 2:
        _send_to(ADMIN_CHAT_ID, "Usage: `/driverlog 0792`")
        return
    vname = resolve_truck_by_number(parts[1])
    if not vname:
        _send_to(ADMIN_CHAT_ID, f"❌ Truck `{parts[1]}` not found.")
        return

    events = get_driver_event_log(vname, limit=10)
    if not events:
        _send_to(ADMIN_CHAT_ID, f"No events logged for *{vname}* yet.")
        return

    lines = [f"📋 *Driver log — Truck {vname}* (last {len(events)})"]
    for e in events:
        ts  = e["created_at"]
        tss = ts.strftime("%b %d %H:%M") if hasattr(ts, "strftime") else str(ts)
        dby = e.get("detected_by") or "?"
        drv = e.get("driver_name") or ""
        lines.append(f"`{tss}` *{e['event_type']}* via {dby}{' — ' + drv if drv else ''}")
    _send_to(ADMIN_CHAT_ID, "\n".join(lines))


def _handle_driverlist() -> None:
    """/driverlist — all active trucks with driver and alert status."""
    from database import get_all_driver_statuses
    rows = get_all_driver_statuses()
    if not rows:
        _send_to(ADMIN_CHAT_ID, "No active trucks found.")
        return
    lines = [f"🚛 *Driver List ({len(rows)} trucks)*\n"]
    for r in rows:
        vname   = r["vehicle_name"]
        dname   = r.get("driver_name") or "—"
        paused  = r.get("alert_paused", False)
        ver     = r.get("group_verified", False)
        a_icon  = "🟢" if not paused else "🔴"
        v_icon  = "✅" if ver else "❌"
        lines.append(f"{a_icon} *{vname}* | {dname} | {v_icon}")
    # Send in chunks to avoid Telegram message length limit
    chunks = [lines[0:1] + lines[i:i+50] for i in range(1, len(lines), 50)]
    for chunk in chunks:
        _send_to(ADMIN_CHAT_ID, "\n".join(chunk))


def _handle_alertcount() -> None:
    """/alertcount — show how many drivers are actively receiving fuel alerts, broken down by system."""
    from database import get_alert_count_by_system
    try:
        data = get_alert_count_by_system()
        old  = data.get('old', {})
        new  = data.get('new', {})

        total_active = old.get('active', 0) + new.get('active', 0)
        total_trucks = old.get('total', 0)  + new.get('total', 0)

        lines = [f"📊 *Alert Recipients — {total_active}/{total_trucks} drivers active*\n"]

        lines.append("*🟡 Old System (EFS/WEX card):*")
        lines.append(f"  Total trucks:       {old.get('total', 0)}")
        lines.append(f"  ✅ Receiving alerts: {old.get('active', 0)}")
        lines.append(f"  📱 Has group set:    {old.get('with_group', 0)}")
        lines.append(f"  🔕 Paused:           {old.get('paused', 0)}")
        lines.append("")

        lines.append("*🟢 New System (Relay — Pilot/Flying J):*")
        lines.append(f"  Total trucks:       {new.get('total', 0)}")
        lines.append(f"  ✅ Receiving alerts: {new.get('active', 0)}")
        lines.append(f"  📱 Has group set:    {new.get('with_group', 0)}")
        lines.append(f"  🔕 Paused:           {new.get('paused', 0)}")
        lines.append("")

        lines.append("_Use /driverlist for per-truck detail._")

        _send_to(ADMIN_CHAT_ID, "\n".join(lines))
    except Exception as e:
        log.error(f"/alertcount error: {e}", exc_info=True)
        _send_to(ADMIN_CHAT_ID, f"❌ Error: `{e}`")


def _handle_resumeall() -> None:
    """/resumeall — un-pause all trucks that were paused by the heartbeat false-positive (bot_kicked reason only)."""
    from database import bulk_resume_bot_kicked
    try:
        count = bulk_resume_bot_kicked()
        if count == 0:
            _send_to(ADMIN_CHAT_ID,
                "✅ No trucks were paused with reason `bot_kicked` — nothing to resume.\n"
                "_(If trucks are paused for other reasons use /driverlist to check.)_"
            )
        else:
            _send_to(ADMIN_CHAT_ID,
                f"✅ *{count} truck(s) resumed* — fuel alerts reactivated fleet-wide.\n"
                f"_(Only trucks paused by the heartbeat false-positive were affected.)_\n"
                f"Use /alertcount to confirm active count."
            )
        log.info(f"/resumeall: resumed {count} trucks from bot_kicked pause")
    except Exception as e:
        log.error(f"/resumeall error: {e}", exc_info=True)
        _send_to(ADMIN_CHAT_ID, f"❌ /resumeall failed: `{e}`")


def _handle_relayapp(chat_id: str) -> None:
    """/relayapp — move all trucks in this group to new Relay fuel card (Pilot/FJ only)."""
    from database import set_group_card_system, db_cursor
    try:
        names = set_group_card_system(chat_id, 'new')
        if not names:
            _send_to(chat_id,
                "❌ No trucks found for this group.\n"
                "Ask admin to assign trucks first with /setgroup.")
            return
        truck_list = ", ".join(names)
        _send_to(chat_id,
            f"✅ All trucks in this group have been moved to the new Relay fuel card.\n"
            f"From now on, fuel stops will be *Pilot and Flying J only*.\n\n"
            f"Trucks updated: {truck_list}")
        # Get group name for admin notification
        with db_cursor() as cur:
            cur.execute(
                "SELECT telegram_group_name FROM trucks WHERE telegram_group_id = %s LIMIT 1",
                (str(chat_id),)
            )
            row = cur.fetchone()
            group_name = (row["telegram_group_name"] or chat_id) if row else chat_id
        _send_to(ADMIN_CHAT_ID,
            f"🔄 *Group moved to Relay system*\n"
            f"Group: *{group_name}*\n"
            f"Trucks ({len(names)}): {truck_list}")
        log.info(f"/relayapp: group {chat_id} moved {len(names)} trucks to new system: {truck_list}")
    except Exception as e:
        log.error(f"/relayapp error: {e}", exc_info=True)
        _send_to(chat_id, f"❌ Error switching to new system: `{e}`")


def _handle_cityfuel(chat_id: str) -> None:
    """/cityfuel — move all trucks in this group back to old EFS fuel card (all stops)."""
    from database import set_group_card_system, db_cursor
    try:
        names = set_group_card_system(chat_id, 'old')
        if not names:
            _send_to(chat_id,
                "❌ No trucks found for this group.\n"
                "Ask admin to assign trucks first with /setgroup.")
            return
        truck_list = ", ".join(names)
        _send_to(chat_id,
            f"✅ All trucks in this group have been moved back to the old EFS fuel card.\n"
            f"From now on, fuel stops will be calculated across *all available networks*.\n\n"
            f"Trucks updated: {truck_list}")
        # Get group name for admin notification
        with db_cursor() as cur:
            cur.execute(
                "SELECT telegram_group_name FROM trucks WHERE telegram_group_id = %s LIMIT 1",
                (str(chat_id),)
            )
            row = cur.fetchone()
            group_name = (row["telegram_group_name"] or chat_id) if row else chat_id
        _send_to(ADMIN_CHAT_ID,
            f"🔄 *Group moved back to old EFS system*\n"
            f"Group: *{group_name}*\n"
            f"Trucks ({len(names)}): {truck_list}")
        log.info(f"/cityfuel: group {chat_id} moved {len(names)} trucks to old system: {truck_list}")
    except Exception as e:
        log.error(f"/cityfuel error: {e}", exc_info=True)
        _send_to(chat_id, f"❌ Error switching to old system: `{e}`")
