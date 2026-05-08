"""
main.py  -  FleetFuel Bot entry point.

Thread layout:
  main thread        — Samsara poll loop (truck monitoring, every 30 s tick)
  telegram-thread    — Telegram getUpdates loop (driver commands, file uploads)
  background threads — QM routes, MPG sync, IFTA rates, truck classifier
  health-thread      — HTTP /health endpoint on PORT (Railway health checks)
"""

import http.server
import logging
import threading
import time
import signal
import sys
import os
import re
from datetime import datetime, timedelta, timezone

from config import STATE_SAVE_INTERVAL_SECONDS, CLASSIFIER_ENFORCEMENT_MODE
from database import init_db, load_all_truck_states, save_all_truck_states, reset_truck_states, auto_register_truck
from samsara_client import get_combined_vehicle_data
from state_machine import process_truck
import telegram_bot
from telegram_bot import send_startup_message, send_price_update_notification, poll_for_uploads
from truck_classifier import classify_all_trucks

import metrics
from circuit_breaker import samsara_breaker, CircuitOpenError
from worker_pool import TruckWorkerPool

# -- Logging ------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# -- Shared state -------------------------------------------------------------
truck_states     = {}
_running         = True

# Timestamp of the last completed poll cycle — used by the health check.
_last_poll_at:    datetime | None = None
_last_poll_lock = threading.Lock()

# -- Graceful shutdown --------------------------------------------------------
def _shutdown(signum, frame):
    global _running
    log.info("Shutdown signal — saving state...")
    save_all_truck_states(truck_states)
    _running = False

signal.signal(signal.SIGTERM, _shutdown)
signal.signal(signal.SIGINT,  _shutdown)


# -- Health check HTTP server -------------------------------------------------

class _HealthHandler(http.server.BaseHTTPRequestHandler):
    """Serves three endpoints:
        GET /          — liveness (Railway healthcheck)
        GET /metrics   — counters/gauges/timers
        GET /circuits  — circuit breaker states
    """
    def _send(self, code: int, body: str, ctype: str = "text/plain") -> None:
        body_bytes = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body_bytes)))
        self.end_headers()
        self.wfile.write(body_bytes)

    def do_GET(self):
        path = self.path.split("?", 1)[0]

        if path == "/metrics":
            self._send(200, metrics.render_text())
            return

        if path == "/circuits":
            from circuit_breaker import samsara_breaker, telegram_breaker, quickmng_breaker
            lines = [
                f"samsara  {samsara_breaker.state()}",
                f"telegram {telegram_breaker.state()}",
                f"quickmng {quickmng_breaker.state()}",
            ]
            self._send(200, "\n".join(lines) + "\n")
            return

        # Default: liveness check
        with _last_poll_lock:
            last = _last_poll_at
        now = datetime.now(timezone.utc)
        age = (now - last).total_seconds() if last else 9999
        ok  = age < 120   # stale if no poll completed in 2 minutes
        body = f"{'OK' if ok else 'STALE'} last_poll={age:.0f}s_ago trucks={len(truck_states)}\n"
        self._send(200 if ok else 503, body)

    def log_message(self, *_):
        pass   # silence default request logging


def _start_health_server():
    port = int(os.getenv("PORT", "8080"))
    server = http.server.HTTPServer(("0.0.0.0", port), _HealthHandler)
    log.info(f"Health check server on :{port}")
    server.serve_forever()


# -- Telegram polling thread --------------------------------------------------

def _telegram_loop():
    """Runs in a dedicated thread — polls Telegram independently of truck monitoring.
    Separating this prevents slow truck processing from delaying driver commands,
    and prevents slow Telegram responses from delaying fuel alerts."""
    log.info("Telegram polling thread started")
    while _running:
        try:
            poll_for_uploads()
        except Exception as e:
            log.error(f"Telegram poll error: {e}", exc_info=True)
        time.sleep(1)   # 1 s between polls keeps commands responsive


# -- Helpers ------------------------------------------------------------------
def _utcnow():
    return datetime.now(timezone.utc)


def _truck_route_keys(vehicle_name: str) -> list[str]:
    """Possible QuickManage route keys for a Samsara vehicle name."""
    text = str(vehicle_name or "").strip()
    if not text:
        return []

    keys: list[str] = []

    def _add(value: str):
        value = str(value or "").strip()
        if value and value not in keys:
            keys.append(value)

    _add(text)

    for grp in re.findall(r"\d+", text):
        _add(grp)
        _add(grp.lstrip("0") or "0")

    first = text.split()[0] if text.split() else ""
    _add(first)
    return keys



# -- Main loop ----------------------------------------------------------------
def main():
    global truck_states, _last_poll_at

    log.info("FleetFuel Bot starting up...")
    log.info(f"Classifier enforcement mode: {CLASSIFIER_ENFORCEMENT_MODE!r}  "
             f"(shadow=observe only, log=observe+warn, enforce=active filtering)")
    log.info("Initializing database...")
    init_db()
    if os.getenv("RESET_DB", "0") == "1":
        log.info("RESET_DB=1 — clearing truck states...")
        reset_truck_states()

    # Load persisted truck states
    truck_states = load_all_truck_states()
    log.info(f"Loaded {len(truck_states)} truck states from DB.")

    # Restore trip planning state from DB into truck_states
    # This ensures planned stops, briefing status survive restarts
    try:
        from database import load_all_trip_states
        trip_states = load_all_trip_states()
        for vname, tstate in trip_states.items():
            # Find truck_states entry by vehicle_name
            for vid, ts in truck_states.items():
                if ts.get("vehicle_name") == vname or ts.get("vname") == vname:
                    # Merge trip state into truck state — don't overwrite existing
                    for key, val in tstate.items():
                        if val is not None and key not in ts:
                            ts[key] = val
                    log.info(f"  Restored trip state for {vname}: "
                             f"briefing={tstate.get('briefing_sent_trip')!r} "
                             f"stop={tstate.get('assigned_stop_name')!r}")
                    break
        log.info(f"Trip states restored for {len(trip_states)} trucks.")
    except Exception as e:
        log.warning(f"Could not restore trip states: {e}")

    try:
        send_startup_message()
    except Exception as e:
        log.warning(f"Could not send startup message: {e}")

    # -- Start health check server (Railway pings this to detect hangs) -------
    threading.Thread(target=_start_health_server, daemon=True, name="health").start()

    # -- Start Telegram polling thread ----------------------------------------
    threading.Thread(target=_telegram_loop, daemon=True, name="telegram").start()

    # -- Truck worker pool — parallel processing with per-truck timeout -------
    pool_size = int(os.getenv("TRUCK_WORKERS", "8"))
    truck_pool = TruckWorkerPool(max_workers=pool_size, per_truck_timeout=30.0)
    log.info(f"Truck worker pool: {pool_size} workers")

    log.info("Polling loop started.")

    last_db_save           = _utcnow()
    last_weekly_report_date = None          # date object; None = not sent yet this boot
    last_route_fetch    = _utcnow() - timedelta(minutes=10)  # fetch immediately on start
    last_mpg_sync       = _utcnow() - timedelta(hours=2)     # sync MPG immediately on start
    last_ifta_check     = _utcnow() - timedelta(hours=25)    # check IFTA rates immediately on start
    last_ifta_update    = _utcnow() - timedelta(days=91)     # update IFTA rates immediately
    last_classifier_run = _utcnow() - timedelta(minutes=31)  # classify immediately on start
    last_group_verify   = _utcnow()                           # start timer at boot — first check runs after 6h
    poll_cycle          = 0

    # Start background thread for QM route geocoding (slow — don't block alerts)
    _route_lock = threading.Lock()

    def _fetch_routes_background():
        """Fetch + geocode QM routes in background. Saves to DB when done."""
        try:
            from config import QM_CLIENT_ID, QM_CLIENT_SECRET
            if not QM_CLIENT_ID or not QM_CLIENT_SECRET:
                return
            from quickmanage_client import get_all_truck_routes
            routes = get_all_truck_routes()
            if routes:
                from database import save_truck_route
                for tn, route in routes.items():
                    save_truck_route(tn, "", route)
                log.info(f"Background: routes saved for {len(routes)} trucks")
        except Exception as e:
            log.warning(f"Background route fetch failed: {e}")

    def _update_ifta_background():
        """Auto-update IFTA rates when new quarter starts."""
        try:
            from ifta import should_update_rates, scrape_and_update_ifta_rates, get_rates_info
            if should_update_rates():
                log.info("IFTA: new quarter detected — updating rates...")
                result = scrape_and_update_ifta_rates()
                if result:
                    log.info(f"IFTA rates updated: {len(result)} states")
                    from telegram_bot import _send_to
                    from config import ADMIN_CHAT_ID
                    _send_to(ADMIN_CHAT_ID, f"📋 *IFTA rates auto-updated*\n{get_rates_info()}")
                else:
                    log.warning("IFTA auto-update failed — keeping current rates")
            else:
                log.info(f"IFTA rates current: {get_rates_info()}")
        except Exception as e:
            log.warning(f"IFTA update check failed: {e}")

    def _sync_mpg_background():
        """Sync real MPG and idle data from Samsara every hour."""
        try:
            from samsara_client import get_vehicle_fuel_efficiency, get_combined_vehicle_data
            from database import save_truck_efficiency
            efficiency = get_vehicle_fuel_efficiency()
            if not efficiency:
                log.warning("Background MPG sync: no data from Samsara fuel report")
                return
            # Get vehicle names
            vehicles = get_combined_vehicle_data()
            name_map = {v["vehicle_id"]: v["vehicle_name"] for v in vehicles}
            updated  = 0
            for vid, stats in efficiency.items():
                if stats.get("mpg") and stats["mpg"] > 3:
                    # Prefer name from locations feed; fall back to name from fuel report;
                    # last resort: use the vehicle_id (will still be queryable by vid)
                    name = name_map.get(vid) or stats.get("name") or vid
                    save_truck_efficiency(
                        vehicle_id=vid, vehicle_name=name,
                        mpg=stats["mpg"], idle_hours=stats["idle_hours"],
                        idle_pct=stats["idle_pct"], fuel_gal=stats["fuel_gal"],
                    )
                    updated += 1
            log.info(f"Background MPG sync: updated {updated} trucks")
        except Exception as e:
            log.warning(f"Background MPG sync failed: {e}")

    def _classify_trucks_background():
        """Classify all trucks as active/idle/at_yard/in_shop/unassigned every 30 min."""
        try:
            classify_all_trucks()
        except Exception as e:
            log.warning(f"Background classifier failed: {e}")

    while _running:
        try:
            poll_cycle += 1
            now = _utcnow()

            # -- Background route fetch (every 5 min) ------------------------
            if (now - last_route_fetch).total_seconds() >= 300:
                t = threading.Thread(target=_fetch_routes_background, daemon=True)
                t.start()
                last_route_fetch = now

            # -- IFTA rates update (every 90 days = quarterly) ---------------
            if (now - last_ifta_update).total_seconds() >= 7776000:
                def _update_ifta():
                    try:
                        from ifta import update_ifta_rates_from_web
                        ok = update_ifta_rates_from_web()
                        if ok:
                            from telegram_bot import _send_to
                            from config import ADMIN_CHAT_ID
                            _send_to(ADMIN_CHAT_ID, "✅ IFTA rates updated from official source.")
                    except Exception as e:
                        log.warning(f"IFTA update failed: {e}")
                threading.Thread(target=_update_ifta, daemon=True).start()
                last_ifta_update = now

            # -- IFTA rates check (every 24h — updates when new quarter starts) --
            if (now - last_ifta_check).total_seconds() >= 86400:
                t = threading.Thread(target=_update_ifta_background, daemon=True)
                t.start()
                last_ifta_check = now

            # -- Background MPG sync (every hour) -----------------------------
            if (now - last_mpg_sync).total_seconds() >= 3600:
                t = threading.Thread(target=_sync_mpg_background, daemon=True)
                t.start()
                last_mpg_sync = now

            # -- Active truck classifier (every 30 min) -----------------------
            if (now - last_classifier_run).total_seconds() >= 1800:
                t = threading.Thread(target=_classify_trucks_background, daemon=True)
                t.start()
                last_classifier_run = now

            # -- Driver group heartbeat (every 6 hours) ----------------------
            if (now - last_group_verify).total_seconds() >= 21600:
                def _verify_groups_background():
                    try:
                        from telegram_bot import verify_driver_groups
                        verify_driver_groups()
                    except Exception as e:
                        log.warning(f"verify_driver_groups failed: {e}")
                threading.Thread(target=_verify_groups_background, daemon=True,
                                 name="group-verify").start()
                last_group_verify = now

            # -- Weekly reports (every Monday 08:00+ UTC) --------------------
            # Date-based — not affected by bot restart time or slow cycles.
            # Fires once per Monday regardless of when the bot started.
            today = now.date()
            if (now.weekday() == 0 and now.hour >= 8
                    and last_weekly_report_date != today):
                last_weekly_report_date = today   # mark immediately to prevent double-send
                try:
                    from telegram_bot import (send_weekly_savings_report,
                                              send_weekly_fleet_excel,
                                              send_weekly_truck_report)
                    send_weekly_savings_report()
                    send_weekly_fleet_excel()
                    send_weekly_truck_report()
                    log.info("Weekly reports sent successfully")
                except Exception as e:
                    log.error(f"Weekly report error: {e}", exc_info=True)

            # Telegram polling is now in _telegram_loop() thread — removed from here.

            # -- Fetch from Samsara (circuit-breaker protected) ---------------
            try:
                with metrics.Timer("samsara_fetch_seconds"):
                    all_trucks = samsara_breaker.call(get_combined_vehicle_data)
                metrics.gauge("samsara_trucks_total", len(all_trucks))
            except CircuitOpenError:
                log.warning("Samsara circuit OPEN — skipping cycle (will retry after cooldown)")
                metrics.incr("poll_cycles_skipped_total")
                time.sleep(15)
                continue
            except Exception as e:
                log.error(f"Samsara fetch failed: {e}")
                metrics.incr("samsara_fetch_errors_total")
                time.sleep(60)
                continue

            # -- Fetch routes from DB (pre-cached by background thread) ------
            # Routes are geocoded in background every 5 min — never blocks alerts
            try:
                from database import get_all_truck_routes_from_db
                qm_routes = get_all_truck_routes_from_db()
            except Exception as e:
                log.warning(f"DB route load failed: {e}")
                qm_routes = {}

            # -- Load activity statuses to skip inactive trucks ---------------
            try:
                from database import get_all_truck_activity_statuses
                activity_statuses = get_all_truck_activity_statuses()
            except Exception as e:
                log.warning(f"Could not load activity statuses: {e}")
                activity_statuses = {}

            INACTIVE_STATUSES = {"at_yard", "in_shop", "unassigned"}

            # -- Find trucks due for polling -----------------------------------
            due_trucks = []
            for truck in all_trucks:
                vid   = truck["vehicle_id"]
                vname = truck.get("vehicle_name", "")
                status = activity_statuses.get(vname)

                if status in INACTIVE_STATUSES:
                    if CLASSIFIER_ENFORCEMENT_MODE == "enforce":
                        continue  # actively skip inactive trucks
                    elif CLASSIFIER_ENFORCEMENT_MODE == "log":
                        log.warning(
                            "SHADOW_WOULD_FILTER truck=%r status=%r — not skipped (mode=log)",
                            vname, status,
                        )
                    # shadow: no action, no log noise

                if vid not in truck_states:
                    # Brand new truck — register and process immediately
                    auto_register_truck(vid, truck["vehicle_name"])
                    log.info(f"New truck: {truck['vehicle_name']} — registered, processing now.")
                    due_trucks.append(truck)
                else:
                    # Force check bypasses next_poll entirely
                    if telegram_bot.force_check_now:
                        due_trucks.append(truck)
                        continue
                    next_poll = truck_states[vid].get("next_poll")
                    if next_poll is None:
                        due_trucks.append(truck)
                    else:
                        if next_poll.tzinfo is None:
                            next_poll = next_poll.replace(tzinfo=timezone.utc)
                        if next_poll <= now:
                            due_trucks.append(truck)

            if telegram_bot.force_check_now:
                telegram_bot.force_check_now = False
                log.info(f"/checknow: forcing check on all {len(due_trucks)} trucks")

            log.info(f"Poll #{poll_cycle}: {len(all_trucks)} trucks  "
                     f"{len(due_trucks)} due for check")
            metrics.gauge("trucks_total",     len(all_trucks))
            metrics.gauge("trucks_due_now",   len(due_trucks))

            # -- Process due trucks (parallel via worker pool) ----------------
            # Load card systems once per cycle (one DB round-trip for all trucks)
            from database import get_all_truck_card_systems, save_truck_state
            card_systems = get_all_truck_card_systems()

            # Attach QM routes to states up front (cheap, sequential — no DB)
            for truck in due_trucks:
                vid          = truck["vehicle_id"]
                vehicle_name = truck.get("vehicle_name", "")
                route        = None
                matched_key  = None
                for key in _truck_route_keys(vehicle_name):
                    if key in qm_routes:
                        route       = qm_routes[key]
                        matched_key = key
                        break
                if route:
                    prev_trip = (truck_states.get(vid, {}) or {}).get("qm_route", {}).get("trip_num")
                    truck_states.setdefault(vid, {})["qm_route"] = route
                    if prev_trip != route.get("trip_num"):
                        log.info(
                            f"Attached QM route to {vehicle_name}: key={matched_key} "
                            f"trip={route.get('trip_num')} status={route.get('status')}"
                        )

            def _process_one(truck):
                vid           = truck["vehicle_id"]
                vname         = truck.get("vehicle_name", "")
                truck_card_sys = card_systems.get(vname, 'old')
                process_truck(vid, truck_states.get(vid, {}), truck, truck_states,
                              card_system=truck_card_sys)
                # Save immediately if an alert was fired — preserves msg_ids for deletion
                if truck_states.get(vid, {}).get("alert_sent"):
                    save_truck_state(truck_states[vid])

            with metrics.Timer("poll_cycle_seconds"):
                cycle_start = _utcnow()
                stats = truck_pool.process_batch(due_trucks, _process_one)
                cycle_elapsed = (_utcnow() - cycle_start).total_seconds()

            metrics.incr("poll_cycles_total")
            log.info(
                f"Poll #{poll_cycle} done in {cycle_elapsed:.2f}s  "
                f"ok={stats['ok']} err={stats['errored']} "
                f"timeout={stats['timed_out']} skipped={stats['skipped']}"
            )
            if cycle_elapsed > 60:
                log.warning(f"Slow cycle: {cycle_elapsed:.1f}s for {len(due_trucks)} trucks")

            # Update health-check heartbeat — lets /health report cycle age
            with _last_poll_lock:
                _last_poll_at = _utcnow()

            # -- Periodic DB save ---------------------------------------------
            if (now - last_db_save).total_seconds() >= STATE_SAVE_INTERVAL_SECONDS:
                save_all_truck_states(truck_states)
                last_db_save = now

        except Exception as e:
            log.error(f"Unhandled error in poll cycle: {e}", exc_info=True)
            metrics.incr("poll_cycles_errored_total")

        time.sleep(30)

    log.info("Shutting down worker pool...")
    truck_pool.shutdown()
    log.info("FleetFuel Bot stopped cleanly.")


if __name__ == "__main__":
    main()
