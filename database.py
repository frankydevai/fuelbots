"""
database.py  -  PostgreSQL connection + schema + all queries.

Tables:
  trucks          - maps Samsara vehicle_name → telegram_group_id
  fuel_stops      - all Pilot + Love's locations with diesel prices
  truck_states    - current state of each truck (persisted across restarts)
  fuel_alerts     - one row per low-fuel alert event
  ca_reminders    - tracks California border reminders sent (cooldown)
  bot_config      - key/value store for config (pilot locations cache etc.)
"""

import json
import logging
import threading
import time
import psycopg2
import psycopg2.extras
import psycopg2.pool
from contextlib import contextmanager
from datetime import datetime
from config import DATABASE_URL

log = logging.getLogger(__name__)


# -- Connection pool ----------------------------------------------------------
# One shared pool for the entire process. ThreadedConnectionPool is safe to
# call from multiple threads simultaneously (background MPG sync, Telegram
# thread, and main truck-processing thread all share it).

_pool: psycopg2.pool.ThreadedConnectionPool | None = None
_pool_lock = threading.Lock()

_POOL_MIN  = 2
_POOL_MAX  = 15   # raised to handle 50-truck fleet + background threads
_CONN_ARGS = dict(
    connect_timeout=10,
    keepalives=1,
    keepalives_idle=30,
    keepalives_interval=10,
    keepalives_count=3,
)


def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    """Return (and lazily initialise) the shared connection pool."""
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:   # double-checked locking
                _pool = psycopg2.pool.ThreadedConnectionPool(
                    _POOL_MIN, _POOL_MAX, DATABASE_URL, **_CONN_ARGS
                )
                log.info(f"DB pool ready (min={_POOL_MIN} max={_POOL_MAX})")
    return _pool


def _connect_direct():
    """Open a single throw-away connection — used only by init_db()."""
    last_err = None
    for attempt in range(1, 4):
        try:
            return psycopg2.connect(DATABASE_URL, **_CONN_ARGS)
        except psycopg2.OperationalError as e:
            last_err = e
            log.warning(f"DB connect attempt {attempt}/3 failed: {e}")
            if attempt < 3:
                time.sleep(2 * attempt)
    raise last_err


# Keep the old name so any external callers don't break.
def get_connection(retries: int = 3, delay: float = 2.0):
    return _connect_direct()


@contextmanager
def db_cursor():
    """Yield a RealDictCursor from the pool; commit on success, rollback on error."""
    pool = _get_pool()
    conn = None
    try:
        conn = pool.getconn()
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        yield cur
        conn.commit()
    except Exception:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    finally:
        if conn:
            try:
                pool.putconn(conn)
            except Exception as _pe:
                log.warning(f"pool.putconn failed: {_pe}")


# -- Schema -------------------------------------------------------------------

SCHEMA_SQL = """
-- trucks: manually inserted to map Samsara name → Telegram group
CREATE TABLE IF NOT EXISTS trucks (
    id                  SERIAL PRIMARY KEY,
    vehicle_name        TEXT    NOT NULL UNIQUE,
    telegram_group_id   TEXT,
    tank_capacity_gal   REAL    NOT NULL DEFAULT 150,
    avg_mpg             REAL    NOT NULL DEFAULT 6.5,
    tank_size_known     BOOLEAN NOT NULL DEFAULT FALSE,
    mpg_known           BOOLEAN NOT NULL DEFAULT FALSE,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    fuel_card_system    VARCHAR(20) NOT NULL DEFAULT 'old',
    notes               TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- fuel_stops: loaded daily from EFS price CSV
-- Columns: Station, Address, City, State, longitude, latitude, Retail price, Discounted price
-- Drop old fuel_stops table if it has wrong schema, recreate clean
DO $$
BEGIN
    -- Check if old schema (has 'source' column) — drop and recreate
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'fuel_stops' AND column_name = 'source'
    ) THEN
        DROP TABLE IF EXISTS fuel_stops CASCADE;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS fuel_stops (
    id               SERIAL PRIMARY KEY,
    station_name     TEXT    NOT NULL,
    address          TEXT,
    city             TEXT,
    state            TEXT    NOT NULL,
    longitude        FLOAT   NOT NULL,
    latitude         FLOAT   NOT NULL,
    retail_price     REAL,
    discounted_price REAL,
    network          VARCHAR(50) DEFAULT 'other',
    new_card_price   REAL,
    price_updated    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (station_name, city, state)
);

CREATE INDEX IF NOT EXISTS idx_fuel_stops_location ON fuel_stops (latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_fuel_stops_state    ON fuel_stops (state);

-- pilot_contracted_prices: Relay card prices — Pilot/Flying J only, loaded from XLS
-- Identical column layout to fuel_stops so query code just switches the table name.
CREATE TABLE IF NOT EXISTS pilot_contracted_prices (
    id               SERIAL PRIMARY KEY,
    station_name     TEXT    NOT NULL,
    address          TEXT,
    city             TEXT,
    state            TEXT    NOT NULL,
    longitude        FLOAT   NOT NULL,
    latitude         FLOAT   NOT NULL,
    retail_price     REAL,
    discounted_price REAL,
    network          VARCHAR(50) DEFAULT 'pilot_flying_j',
    price_updated    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (station_name, city, state)
);

CREATE INDEX IF NOT EXISTS idx_pilot_prices_location ON pilot_contracted_prices (latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_pilot_prices_state    ON pilot_contracted_prices (state);

-- truck_states: full state persisted to survive Railway redeploys
CREATE TABLE IF NOT EXISTS truck_states (
    vehicle_id              TEXT PRIMARY KEY,
    vehicle_name            TEXT,
    state                   TEXT        NOT NULL DEFAULT 'UNKNOWN',
    fuel_pct                REAL,
    latitude                REAL,
    longitude               REAL,
    speed_mph               REAL,
    heading                 REAL,
    next_poll               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    parked_since            TIMESTAMPTZ,
    alert_sent              BOOLEAN     NOT NULL DEFAULT FALSE,
    overnight_alert_sent    BOOLEAN     NOT NULL DEFAULT FALSE,
    open_alert_id           INTEGER,
    assigned_stop_id        INTEGER,
    assigned_stop_name      TEXT,
    assigned_stop_lat       REAL,
    assigned_stop_lng       REAL,
    assignment_time         TIMESTAMPTZ,
    in_yard                 BOOLEAN     NOT NULL DEFAULT FALSE,
    yard_name               TEXT,
    sleeping                BOOLEAN     NOT NULL DEFAULT FALSE,
    fuel_when_parked        REAL,
    ca_reminder_sent        BOOLEAN     NOT NULL DEFAULT FALSE,
    prev_truck_group        TEXT,
    prev_truck_msg_id       BIGINT,
    prev_dispatcher_msg_id  BIGINT,
    prev_ca_truck_msg_id       BIGINT,
    prev_ca_dispatcher_msg_id  BIGINT,
    last_updated            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- fuel_alerts: history of every alert sent
CREATE TABLE IF NOT EXISTS fuel_alerts (
    id                  SERIAL PRIMARY KEY,
    vehicle_id          TEXT    NOT NULL,
    vehicle_name        TEXT,
    fuel_pct            REAL    NOT NULL,
    latitude            REAL    NOT NULL,
    longitude           REAL    NOT NULL,
    heading             REAL,
    speed_mph           REAL,
    best_stop_id        INTEGER,
    best_stop_name      TEXT,
    best_stop_price     REAL,
    alt_stop_id         INTEGER,
    alt_stop_name       TEXT,
    alt_stop_price      REAL,
    best_stop_state     TEXT,
    gallons_purchased   REAL,
    savings_usd         REAL,
    alert_type          TEXT    NOT NULL DEFAULT 'low_fuel',
    status              TEXT    NOT NULL DEFAULT 'open',
    alerted_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at         TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_fuel_alerts_vehicle ON fuel_alerts (vehicle_id);
CREATE INDEX IF NOT EXISTS idx_fuel_alerts_status  ON fuel_alerts (status);

-- ca_reminders: prevent duplicate CA border reminders
CREATE TABLE IF NOT EXISTS ca_reminders (
    id          SERIAL PRIMARY KEY,
    vehicle_id  TEXT        NOT NULL,
    sent_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bot_config (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS truck_efficiency (
    vehicle_id      TEXT PRIMARY KEY,
    vehicle_name    TEXT,
    mpg             FLOAT DEFAULT 6.5,
    idle_hours_30d  FLOAT DEFAULT 0,
    idle_pct_30d    FLOAT DEFAULT 0,
    fuel_used_30d   FLOAT DEFAULT 0,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS truck_routes (
    truck_number    TEXT PRIMARY KEY,
    group_chat_id   TEXT,
    trip_num        TEXT,
    ref_number      TEXT,
    route_json      TEXT,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- trip_state: persists all trip planning data across bot restarts
-- One row per active truck — updated every time state changes
CREATE TABLE IF NOT EXISTS trip_state (
    vehicle_name            TEXT PRIMARY KEY,
    trip_num                TEXT,
    trip_status             TEXT,
    briefing_sent_trip      TEXT,       -- trip_id of last briefing sent
    all_planned_stops       TEXT,       -- JSON array of planned stops
    planned_stop_index      INTEGER DEFAULT 0,
    assigned_stop_name      TEXT,
    assigned_stop_lat       REAL,
    assigned_stop_lng       REAL,
    assigned_stop_card_price REAL,
    assigned_stop_net_price  REAL,
    missed_stop_name        TEXT,
    missed_stop_card_price  REAL,
    completed_waypoints     TEXT,       -- JSON array of completed stop IDs
    border_warned           TEXT,       -- JSON dict of {state: True} already warned
    updated_at              TIMESTAMPTZ DEFAULT NOW()
);
-- Add border_warned column if it doesn't exist (migration)
DO $$ BEGIN
    ALTER TABLE trip_state ADD COLUMN IF NOT EXISTS border_warned TEXT DEFAULT '{}';
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

-- driver_flags: wrong stop / missed stop / low-stop state flags
CREATE TABLE IF NOT EXISTS driver_flags (
    id               SERIAL PRIMARY KEY,
    vehicle_name     TEXT NOT NULL,
    flag_type        TEXT NOT NULL,
    details          TEXT,
    recommended_stop TEXT,
    actual_stop      TEXT,
    fuel_pct         REAL,
    state            TEXT,
    card_price       REAL,
    savings_lost     REAL,
    flagged_at       TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_flags_truck ON driver_flags (vehicle_name);
CREATE INDEX IF NOT EXISTS idx_flags_time  ON driver_flags (flagged_at);

CREATE TABLE IF NOT EXISTS stop_visits (
    id              SERIAL PRIMARY KEY,
    vehicle_name    TEXT NOT NULL,
    alert_id        INTEGER,
    recommended_stop_name  TEXT,
    recommended_stop_lat   FLOAT,
    recommended_stop_lng   FLOAT,
    actual_stop_name       TEXT,
    actual_stop_lat        FLOAT,
    actual_stop_lng        FLOAT,
    actual_stop_state      TEXT,
    visited         BOOLEAN,   -- TRUE=went to recommended, FALSE=went elsewhere, NULL=unknown
    fuel_before     FLOAT,
    fuel_after      FLOAT,
    gallons_purchased REAL,
    savings_usd     REAL,
    visited_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- geofences: yard and shop locations used by the active truck classifier
CREATE TABLE IF NOT EXISTS geofences (
    id             SERIAL PRIMARY KEY,
    name           VARCHAR(100),
    type           VARCHAR(20),        -- 'yard' or 'shop'
    latitude       DECIMAL(10,7)  NOT NULL,
    longitude      DECIMAL(10,7)  NOT NULL,
    radius_meters  INTEGER        NOT NULL DEFAULT 200,
    company_id     INTEGER,
    created_at     TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

-- driver_group_events: audit log for all Telegram group membership changes
-- event_type: 'joined','left','kicked','verified','deactivated','reassigned'
-- detected_by: 'realtime','heartbeat','admin'
CREATE TABLE IF NOT EXISTS driver_group_events (
    id            SERIAL PRIMARY KEY,
    truck_number  TEXT NOT NULL,
    event_type    TEXT NOT NULL,
    group_id      TEXT,
    driver_name   TEXT,
    detected_by   TEXT DEFAULT 'realtime',
    created_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_dge_truck ON driver_group_events(truck_number);
CREATE INDEX IF NOT EXISTS idx_dge_time  ON driver_group_events(created_at);

-- drivers: one row per assignment (is_active=FALSE when reassigned or removed)
CREATE TABLE IF NOT EXISTS drivers (
    id             SERIAL PRIMARY KEY,
    truck_number   TEXT NOT NULL,
    driver_name    TEXT NOT NULL,
    telegram_group TEXT,
    assigned_at    TIMESTAMPTZ DEFAULT NOW(),
    assigned_by    TEXT,
    is_active      BOOLEAN DEFAULT TRUE
);
CREATE INDEX IF NOT EXISTS idx_drivers_truck ON drivers(truck_number);
"""


def init_db():
    """Create all tables if they don't exist. Runs migrations for existing DBs."""
    log.info("Initializing PostgreSQL schema...")
    conn = _connect_direct()    # direct conn — pool not warmed yet at startup
    cur = conn.cursor()
    cur.execute(SCHEMA_SQL)
    # Migrations for existing DBs
    cur.execute("ALTER TABLE trucks ALTER COLUMN telegram_group_id DROP NOT NULL")
    # Add missing fuel_alerts columns if not exists
    for col_def in [
        "alt_stop_id INTEGER",
        "alt_stop_name TEXT",
        "alt_stop_price REAL",
        "best_stop_state TEXT",
        "gallons_purchased REAL",
    ]:
        col_name = col_def.split()[0]
        try:
            cur.execute(f"ALTER TABLE fuel_alerts ADD COLUMN IF NOT EXISTS {col_def}")
        except Exception as _e:
            log.warning(f"init_db: could not add column {col_def!r}: {_e}")
    for col, coltype in [
        ("prev_truck_group",       "TEXT"),
        ("prev_truck_msg_id",      "BIGINT"),
        ("prev_dispatcher_msg_id", "BIGINT"),
        ("prev_ca_truck_msg_id",      "BIGINT"),
        ("prev_ca_dispatcher_msg_id", "BIGINT"),
    ]:
        cur.execute(f"ALTER TABLE truck_states ADD COLUMN IF NOT EXISTS {col} {coltype}")
    for col, coltype in [
        ("assigned_stop_card_price", "REAL"),
        ("assigned_stop_net_price",  "REAL"),
        ("missed_stop_name",         "TEXT"),
        ("missed_stop_card_price",   "REAL"),
        ("completed_waypoints",      "TEXT"),
        ("border_warned",            "TEXT"),
    ]:
        cur.execute(f"ALTER TABLE trip_state ADD COLUMN IF NOT EXISTS {col} {coltype}")
    for col, coltype in [
        ("card_price",   "REAL"),
        ("savings_lost", "REAL"),
    ]:
        cur.execute(f"ALTER TABLE driver_flags ADD COLUMN IF NOT EXISTS {col} {coltype}")
    for col, coltype in [
        ("actual_stop_state",  "TEXT"),
        ("gallons_purchased",  "REAL"),
        ("savings_usd",        "REAL"),
    ]:
        cur.execute(f"ALTER TABLE stop_visits ADD COLUMN IF NOT EXISTS {col} {coltype}")
    cur.execute("ALTER TABLE trucks ADD COLUMN IF NOT EXISTS telegram_group_name TEXT")
    # Active truck classifier columns
    for col, coltype in [
        ("activity_status",   "VARCHAR(20)"),
        ("status_updated_at", "TIMESTAMPTZ"),
        ("status_signals",    "JSONB"),
    ]:
        cur.execute(f"ALTER TABLE trucks ADD COLUMN IF NOT EXISTS {col} {coltype}")
    # Dual fuel card system
    cur.execute("ALTER TABLE trucks ADD COLUMN IF NOT EXISTS fuel_card_system VARCHAR(20) NOT NULL DEFAULT 'old'")
    cur.execute("ALTER TABLE fuel_stops ADD COLUMN IF NOT EXISTS network VARCHAR(50) DEFAULT 'other'")
    cur.execute("ALTER TABLE fuel_stops ADD COLUMN IF NOT EXISTS new_card_price REAL")
    # Driver group change detection
    for col, coltype in [
        ("group_verified",   "BOOLEAN DEFAULT FALSE"),
        ("last_verified_at", "TIMESTAMPTZ"),
        ("alert_paused",     "BOOLEAN DEFAULT FALSE"),
        ("pause_reason",     "TEXT"),
    ]:
        cur.execute(f"ALTER TABLE trucks ADD COLUMN IF NOT EXISTS {col} {coltype}")
    conn.commit()
    conn.close()
    log.info("✅ Database schema ready.")


# -- Config -------------------------------------------------------------------

def set_config_value(key: str, value: str):
    sql = """
        INSERT INTO bot_config (key, value, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()
    """
    with db_cursor() as cur:
        cur.execute(sql, (key, value))


def get_config_value(key: str) -> str | None:
    with db_cursor() as cur:
        cur.execute("SELECT value FROM bot_config WHERE key=%s", (key,))
        row = cur.fetchone()
        return row["value"] if row else None


# -- Helpers ------------------------------------------------------------------

def _row(r):
    return dict(r) if r else None

def _rows(rs):
    return [dict(r) for r in rs]

def _dt(val):
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    try:
        return datetime.fromisoformat(str(val))
    except Exception:
        return None


# -- trucks -------------------------------------------------------------------

def get_truck_group(vehicle_name: str) -> str | None:
    with db_cursor() as cur:
        cur.execute(
            "SELECT telegram_group_id FROM trucks WHERE vehicle_name = %s AND is_active = TRUE",
            (vehicle_name,)
        )
        row = cur.fetchone()
        return row["telegram_group_id"] if row else None


def get_truck_config(vehicle_name: str) -> dict | None:
    with db_cursor() as cur:
        cur.execute(
            "SELECT * FROM trucks WHERE vehicle_name = %s AND is_active = TRUE",
            (vehicle_name,)
        )
        return _row(cur.fetchone())


def get_all_registered_trucks() -> list:
    with db_cursor() as cur:
        cur.execute("SELECT * FROM trucks WHERE is_active = TRUE ORDER BY vehicle_name")
        return _rows(cur.fetchall())


# -- Active truck classifier --------------------------------------------------

def get_truck_activity_status(vehicle_name: str) -> str | None:
    with db_cursor() as cur:
        cur.execute(
            "SELECT activity_status FROM trucks WHERE vehicle_name = %s",
            (vehicle_name,)
        )
        row = cur.fetchone()
        return row["activity_status"] if row else None


def save_truck_activity_status(vehicle_name: str, status: str, signals: dict) -> None:
    import json as _json
    with db_cursor() as cur:
        cur.execute(
            """
            UPDATE trucks
               SET activity_status   = %s,
                   status_updated_at = NOW(),
                   status_signals    = %s
             WHERE vehicle_name = %s
            """,
            (status, _json.dumps(signals), vehicle_name),
        )


def get_all_truck_activity_statuses() -> dict[str, str]:
    """Return {vehicle_name: activity_status} for all active trucks."""
    with db_cursor() as cur:
        cur.execute(
            "SELECT vehicle_name, activity_status FROM trucks WHERE is_active = TRUE"
        )
        return {
            row["vehicle_name"]: row["activity_status"]
            for row in cur.fetchall()
            if row["activity_status"] is not None
        }


# -- Geofences ----------------------------------------------------------------

def get_all_geofences() -> list[dict]:
    with db_cursor() as cur:
        cur.execute(
            "SELECT id, name, type, latitude, longitude, radius_meters FROM geofences ORDER BY type, name"
        )
        return _rows(cur.fetchall())


def add_geofence(name: str, fence_type: str, lat: float, lng: float,
                 radius_m: int = 200, company_id: int = None) -> int:
    with db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO geofences (name, type, latitude, longitude, radius_meters, company_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (name, fence_type, lat, lng, radius_m, company_id),
        )
        return cur.fetchone()["id"]


# -- trucks (continued) -------------------------------------------------------

def _truck_number(name: str) -> str:
    """Extract the numeric truck ID from any name format.
    '4966 - AQUISHA HARRIS' → '4966'
    'UNIT 1627'             → '1627'
    'Unit - 1637'           → '1637'
    'Unit 3821'             → '3821'
    '777'                   → '777'
    Returns the first run of 3+ digits found, or the raw name if none.
    """
    import re
    m = re.search(r'\b(\d{3,})\b', name or '')
    return m.group(1) if m else (name or '').strip()


def auto_register_truck(vehicle_id: str, vehicle_name: str) -> bool:
    """Register a truck from Samsara, keyed by numeric truck number.

    - If a truck with the same number prefix already exists: update its name to
      the current Samsara name (driver reassignment) and re-activate if inactive.
    - If no match: insert a new row.
    Returns True if anything changed (insert, rename, or re-activation).
    """
    number = _truck_number(vehicle_name)
    with db_cursor() as cur:
        # Find any existing row whose name starts with this truck number
        cur.execute(
            """SELECT vehicle_name, is_active FROM trucks
               WHERE vehicle_name = %s
                  OR vehicle_name ~* %s
               ORDER BY is_active DESC, LENGTH(vehicle_name) DESC
               LIMIT 1""",
            (vehicle_name, r'(^|[^0-9])' + number + r'([^0-9]|$)'),
        )
        row = cur.fetchone()

        if row:
            existing_name   = row["vehicle_name"]
            existing_active = row["is_active"]
            name_changed    = existing_name != vehicle_name
            if name_changed or not existing_active:
                cur.execute(
                    "UPDATE trucks SET vehicle_name = %s, is_active = TRUE WHERE vehicle_name = %s",
                    (vehicle_name, existing_name),
                )
                if name_changed:
                    log.info(f"Truck renamed: '{existing_name}' → '{vehicle_name}'")
                else:
                    log.info(f"Re-activated truck: {vehicle_name}")
                return True
            return False  # already active with correct name

        # Genuinely new truck
        cur.execute(
            "INSERT INTO trucks (vehicle_name, is_active) VALUES (%s, TRUE)",
            (vehicle_name,)
        )
        log.info(f"Auto-registered new truck: {vehicle_name}")
        return True


def get_truck_by_group(group_id: str) -> dict | None:
    """Get truck record by its Telegram group ID."""
    with db_cursor() as cur:
        cur.execute(
            "SELECT * FROM trucks WHERE telegram_group_id = %s AND is_active = TRUE",
            (str(group_id),)
        )
        return cur.fetchone()


def upsert_truck_group(vehicle_name: str, group_id: str, group_name: str = None) -> bool:
    with db_cursor() as cur:
        if group_name:
            cur.execute(
                "UPDATE trucks SET telegram_group_id = %s, telegram_group_name = %s WHERE vehicle_name = %s",
                (group_id, group_name, vehicle_name)
            )
        else:
            cur.execute(
                "UPDATE trucks SET telegram_group_id = %s WHERE vehicle_name = %s",
                (group_id, vehicle_name)
            )
        return cur.rowcount > 0


def deactivate_truck(vehicle_name: str) -> bool:
    with db_cursor() as cur:
        cur.execute(
            "UPDATE trucks SET is_active = FALSE WHERE vehicle_name = %s",
            (vehicle_name,)
        )
        return cur.rowcount > 0


# -- fuel_stops ---------------------------------------------------------------

def upsert_fuel_stop(row: dict):
    bulk_upsert_fuel_stops([row])


def bulk_upsert_fuel_stops(records: list[dict]) -> int:
    """Bulk insert/update fuel stops. Returns count inserted/updated."""
    if not records:
        return 0
    # Legacy function — use import_efs_csv instead
    return 0


def import_efs_csv(file_bytes: bytes) -> tuple[int, str]:
    """
    Import daily EFS price CSV into fuel_stops.
    Expected columns: Station, Address, City, State, longitude, latitude,
                      Retail price, Discounted price
    Clears existing data and reloads fresh every time (daily upload).
    """
    import csv, io, re

    def _norm(value: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", (value or "").strip().lower())

    def _pick(row: dict, aliases: tuple[str, ...], default: str = "") -> str:
        for alias in aliases:
            actual = header_map.get(_norm(alias))
            if actual is not None:
                return str(row.get(actual, default) or "").strip()
        return default

    def _to_float(value: str):
        text = str(value or "").strip()
        if not text:
            return None
        text = text.replace("$", "").replace(",", "")
        return float(text)

    def _norm_address(value: str) -> str:
        text = str(value or "").strip().lower()
        replacements = {
            " street": " st",
            " road": " rd",
            " avenue": " ave",
            " boulevard": " blvd",
            " highway": " hwy",
            " drive": " dr",
            " lane": " ln",
            " place": " pl",
            " court": " ct",
        }
        for src, dst in replacements.items():
            text = text.replace(src, dst)
        return re.sub(r"[^a-z0-9]+", "", text)
    try:
        text   = file_bytes.decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(text))
        if not reader.fieldnames:
            return 0, "CSV file is missing a header row."
        rows   = list(reader)
    except Exception as e:
        return 0, f"❌ Could not parse file: {e}"

    header_map = {_norm(name): name for name in (reader.fieldnames or [])}
    required_groups = {
        "station": ("Station", "station_name", "store_name", "name"),
        "city": ("City", "city"),
        "state": ("State", "state"),
        "latitude": ("latitude", "lat"),
        "longitude": ("longitude", "lng", "lon"),
        "discounted_price": (
            "Discounted price", "discounted_price", "discount_price",
            "card price", "card_price", "diesel_price",
        ),
    }
    missing = [
        group for group, aliases in required_groups.items()
        if not any(_norm(alias) in header_map for alias in aliases)
    ]
    if missing:
        return 0, "CSV missing required columns: " + ", ".join(missing)

    records = []
    skipped = 0
    duplicates = 0
    conflicts = 0
    for r in rows:
        try:
            lat = _to_float(_pick(r, required_groups["latitude"]))
            lng = _to_float(_pick(r, required_groups["longitude"]))
            name = _pick(r, required_groups["station"])
            city = _pick(r, required_groups["city"])
            state = _pick(r, required_groups["state"]).upper()
            retail = _to_float(_pick(r, ("Retail price", "retail_price", "retail", "pump_price")))
            discount = _to_float(_pick(r, required_groups["discounted_price"]))
            if not name or not state or lat is None or lng is None or discount is None:
                skipped += 1
                continue
            from price_updater import _detect_network
            records.append({
                "station_name":     name,
                "address":          _pick(r, ("Address", "address", "street_address")),
                "city":             city,
                "state":            state,
                "longitude":        lng,
                "latitude":         lat,
                "retail_price":     retail,
                "discounted_price": discount,
                "network":          _detect_network(name),
            })
        except Exception:
            skipped += 1

    if not records:
        return 0, "No valid records found in file."

    # Reject conflicting rows that point to the same physical location/address
    # but claim different city/state values. This protects routing from bad
    # cleaned CSV merges like "Columbia, SC" with NJ coordinates.
    conflict_indexes = set()
    coord_groups = {}
    addr_groups = {}
    for idx, record in enumerate(records):
        coord_key = (
            round(float(record["latitude"]), 3),
            round(float(record["longitude"]), 3),
        )
        coord_groups.setdefault(coord_key, []).append((idx, record))

        addr_key = _norm_address(record.get("address", ""))
        if addr_key:
            addr_groups.setdefault(addr_key, []).append((idx, record))

    def _mark_conflicts(groups: dict):
        nonlocal conflicts
        for items in groups.values():
            locations = {
                (
                    str(rec.get("city", "")).strip().upper(),
                    str(rec.get("state", "")).strip().upper(),
                )
                for _, rec in items
                if rec.get("state")
            }
            if len(locations) > 1:
                for idx, _ in items:
                    if idx not in conflict_indexes:
                        conflict_indexes.add(idx)
                        conflicts += 1

    _mark_conflicts(coord_groups)
    _mark_conflicts(addr_groups)
    if conflict_indexes:
        records = [
            record for idx, record in enumerate(records)
            if idx not in conflict_indexes
        ]

    deduped = {}
    for record in records:
        key = (
            str(record.get("station_name", "")).strip().upper(),
            str(record.get("city", "")).strip().upper(),
            str(record.get("state", "")).strip().upper(),
        )
        if key in deduped:
            duplicates += 1
        deduped[key] = record
    records = list(deduped.values())

    with db_cursor() as cur:
        cur.execute("TRUNCATE TABLE fuel_stops RESTART IDENTITY")
        cur.executemany("""
            INSERT INTO fuel_stops
                (station_name, address, city, state, longitude, latitude,
                 retail_price, discounted_price, network, price_updated)
            VALUES
                (%(station_name)s, %(address)s, %(city)s, %(state)s,
                 %(longitude)s, %(latitude)s,
                 %(retail_price)s, %(discounted_price)s, %(network)s, NOW())
        """, records)

    msg = (
        f"Fuel prices updated\n"
        f"{len(records)} stations loaded\n"
        f"{skipped} skipped (missing data)\n"
        f"{conflicts} conflicting rows rejected\n"
        f"{duplicates} duplicate rows merged\n"
        f"Using discounted (card) price for routing"
    )
    return len(records), msg


def get_all_diesel_stops() -> list:
    """Return all stops for old card system (CSV-loaded, any network)."""
    with db_cursor() as cur:
        cur.execute("""
            SELECT
                id,
                station_name  AS store_name,
                address,
                city,
                state,
                longitude,
                latitude,
                retail_price,
                discounted_price AS diesel_price,
                network,
                price_updated
            FROM fuel_stops
            WHERE discounted_price IS NOT NULL
            ORDER BY state, city
        """)
        return _rows(cur.fetchall())


def get_all_diesel_stops_for_system(card_system: str = 'old') -> list:
    """Return stops from the correct price table for this card system.

    old → fuel_stops            (EFS/WEX card, all networks, CSV-loaded)
    new → pilot_contracted_prices (Relay card, Pilot/FJ only, XLS-loaded)

    Both tables have identical column layouts — only the table name changes.
    """
    _VALID = {'old': 'fuel_stops', 'new': 'pilot_contracted_prices'}
    table  = _VALID.get(card_system, 'fuel_stops')
    with db_cursor() as cur:
        cur.execute(f"""
            SELECT
                id,
                station_name  AS store_name,
                address,
                city,
                state,
                longitude,
                latitude,
                retail_price,
                discounted_price AS diesel_price,
                network,
                price_updated
            FROM {table}
            WHERE discounted_price IS NOT NULL
            ORDER BY state, city
        """)
        return _rows(cur.fetchall())


def _read_excel_rows(file_bytes: bytes) -> tuple[list[str], list[list], str | None]:
    """Detect .xls vs .xlsx by file magic bytes and return (headers, data_rows, error).

    .xlsx files start with PK\x03\x04 (ZIP container) → openpyxl
    .xls  files start with \xD0\xCF\x11\xE0 (OLE2 compound) → xlrd
    """
    import io

    if not file_bytes or len(file_bytes) < 8:
        return [], [], "file is empty or too small"

    head = file_bytes[:8]

    # -- .xlsx (ZIP) -----------------------------------------------------------
    if head.startswith(b"PK\x03\x04"):
        try:
            import openpyxl
        except ImportError:
            return [], [], "openpyxl not installed (pip install openpyxl)"
        try:
            wb = openpyxl.load_workbook(
                io.BytesIO(file_bytes), read_only=True, data_only=True
            )
            ws = wb.active
            rows_iter = iter(ws.iter_rows(values_only=True))
            header_row = None
            for row in rows_iter:
                valid_cells = [str(c).strip() for c in row if c is not None and str(c).strip()]
                if len(valid_cells) >= 3:
                    header_row = row
                    break
            if not header_row:
                return [], [], "xlsx file has no valid header row"
            headers = [str(h or "").strip().lower() for h in header_row]
            data_rows = [list(r) for r in rows_iter]
            return headers, data_rows, None
        except Exception as e:
            return [], [], f"xlsx parse error: {e}"

    # -- .xls (OLE2) -----------------------------------------------------------
    if head.startswith(b"\xD0\xCF\x11\xE0"):
        try:
            import xlrd
        except ImportError:
            return [], [], (
                "xlrd not installed (pip install xlrd==2.0.1) — "
                "needed to read legacy .xls files"
            )
        try:
            book = xlrd.open_workbook(file_contents=file_bytes)
            sheet = book.sheet_by_index(0)
            if sheet.nrows < 1:
                return [], [], "xls sheet is empty"
            
            header_row_idx = -1
            headers = []
            for r in range(sheet.nrows):
                row = [sheet.cell_value(r, c) for c in range(sheet.ncols)]
                valid_cells = [str(c).strip() for c in row if c is not None and str(c).strip()]
                if len(valid_cells) >= 3:
                    header_row_idx = r
                    headers = [str(c or "").strip().lower() for c in row]
                    break
                    
            if header_row_idx == -1:
                return [], [], "xls sheet has no valid header row"
                
            data_rows = []
            for r in range(header_row_idx + 1, sheet.nrows):
                row = [sheet.cell_value(r, c) for c in range(sheet.ncols)]
                data_rows.append(row)
            return headers, data_rows, None
        except Exception as e:
            return [], [], f"xls parse error: {e}"

    # -- Some Relay exports are actually HTML tables saved with .xls extension
    if head.lstrip().lower().startswith((b"<html", b"<!doc", b"<?xml", b"<table")):
        try:
            from html.parser import HTMLParser

            class _T(HTMLParser):
                def __init__(self):
                    super().__init__()
                    self.rows = []
                    self.row = []
                    self.cell = []
                    self.in_cell = False
                def handle_starttag(self, tag, attrs):
                    if tag in ("td", "th"):
                        self.in_cell = True
                        self.cell = []
                    elif tag == "tr":
                        self.row = []
                def handle_endtag(self, tag):
                    if tag in ("td", "th"):
                        self.row.append("".join(self.cell).strip())
                        self.in_cell = False
                    elif tag == "tr":
                        if self.row:
                            self.rows.append(self.row)
                def handle_data(self, data):
                    if self.in_cell:
                        self.cell.append(data)

            text = file_bytes.decode("utf-8", errors="ignore")
            p = _T()
            p.feed(text)
            if not p.rows:
                return [], [], "html-style xls had no <table> rows"
                
            header_row_idx = -1
            headers = []
            for i, row in enumerate(p.rows):
                valid_cells = [str(c).strip() for c in row if c is not None and str(c).strip()]
                if len(valid_cells) >= 3:
                    header_row_idx = i
                    headers = [str(c or "").strip().lower() for c in row]
                    break
                    
            if header_row_idx == -1:
                return [], [], "html-style xls has no valid header row"
                
            return headers, [list(r) for r in p.rows[header_row_idx + 1:]], None
        except Exception as e:
            return [], [], f"html-style xls parse error: {e}"

    return [], [], (
        f"unrecognized file format (first 8 bytes: {head!r}). "
        f"Expected .xls (OLE2), .xlsx (ZIP), or HTML table."
    )


def import_relay_xls(file_bytes: bytes) -> tuple[int, str]:
    """
    Import Relay card price file into pilot_contracted_prices (new system).
    Only Pilot/Flying J rows are accepted. Upserts by (station_name, city, state).
    Supports .xls (legacy OLE2), .xlsx (ZIP), and HTML-table-as-xls.

    TWO MODES:
      Full upload  (lat/lng columns present) — stores coordinates permanently.
      Price-only update (no lat/lng columns) — updates card price only using
        stored coordinates from a previous full upload. City is optional:
        matches by (station_name, city, state) first, then (station_name, state).

    Price column: looks for "card price" first, then "card", "discount", "discounted".
    """
    from price_updater import _detect_network

    def _to_float(val):
        if val is None:
            return None
        text = str(val).strip().replace("$", "").replace(",", "")
        try:
            return float(text)
        except Exception:
            return None

    headers, data_rows, err = _read_excel_rows(file_bytes)
    if err:
        return 0, f"❌ Could not read XLS file: {err}"
    if not headers:
        return 0, "❌ XLS file has no header row."

    def _col(names):
        # Exact match first (full column name), then substring fallback
        for n in names:
            for i, h in enumerate(headers):
                if h.strip().lower() == n.lower():
                    return i
        for n in names:
            for i, h in enumerate(headers):
                if n.lower() in h:
                    return i
        return None

    idx_name   = _col(["station", "store", "name"])
    idx_addr   = _col(["address", "street"])
    idx_city   = _col(["city"])
    idx_state  = _col(["state"])
    idx_lat    = _col(["lat"])
    idx_lng    = _col(["lon", "lng", "long"])
    idx_retail = _col(["retail"])
    # "card price" first, then fallbacks — per user requirement
    idx_card   = _col(["card price", "card", "discount", "discounted"])

    if any(i is None for i in [idx_name, idx_state, idx_card]):
        col_list = ", ".join(f"`{h}`" for h in headers[:20])
        return 0, (
            "❌ XLS missing required columns: station name, state, card price.\n"
            f"Columns found: {col_list}"
        )

    has_coords = idx_lat is not None and idx_lng is not None

    loaded    = 0
    skipped   = 0
    no_match  = 0   # price-only mode: row found no stored coords to update

    with db_cursor() as cur:
        for row in data_rows:
            try:
                def _cell(i):
                    return row[i] if i is not None and 0 <= i < len(row) else None

                name  = str(_cell(idx_name) or "").strip()
                state = str(_cell(idx_state) or "").strip().upper()
                price = _to_float(_cell(idx_card))

                if not name or not state or price is None:
                    skipped += 1
                    continue
                if _detect_network(name) != 'pilot_flying_j':
                    skipped += 1
                    continue

                city    = str(_cell(idx_city) or "").strip()
                address = str(_cell(idx_addr) or "").strip()
                retail  = _to_float(_cell(idx_retail))

                if has_coords:
                    # ── Full upload: store coordinates permanently ──────────
                    lat = _to_float(_cell(idx_lat))
                    lng = _to_float(_cell(idx_lng))
                    if lat is None or lng is None:
                        skipped += 1
                        continue
                    cur.execute("""
                        INSERT INTO pilot_contracted_prices
                            (station_name, address, city, state, longitude, latitude,
                             retail_price, discounted_price, network, price_updated)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'pilot_flying_j', NOW())
                        ON CONFLICT (station_name, city, state) DO UPDATE SET
                            address          = COALESCE(EXCLUDED.address,
                                                        pilot_contracted_prices.address),
                            longitude        = EXCLUDED.longitude,
                            latitude         = EXCLUDED.latitude,
                            discounted_price = EXCLUDED.discounted_price,
                            retail_price     = COALESCE(EXCLUDED.retail_price,
                                                        pilot_contracted_prices.retail_price),
                            price_updated    = NOW()
                    """, (name, address, city, state, lng, lat, retail, price))
                    loaded += 1
                else:
                    # ── Price-only update: use stored lat/lng ───────────────
                    # Try exact (name, city, state) first
                    rows_updated = 0
                    if city:
                        cur.execute("""
                            UPDATE pilot_contracted_prices
                               SET discounted_price = %s,
                                   retail_price     = COALESCE(%s, retail_price),
                                   price_updated    = NOW()
                             WHERE station_name = %s AND city = %s AND state = %s
                        """, (price, retail, name, city, state))
                        rows_updated = cur.rowcount

                    # Fallback: match by (name, state) if city didn't match or is blank
                    if rows_updated == 0:
                        cur.execute("""
                            UPDATE pilot_contracted_prices
                               SET discounted_price = %s,
                                   retail_price     = COALESCE(%s, retail_price),
                                   price_updated    = NOW()
                             WHERE station_name = %s AND state = %s
                        """, (price, retail, name, state))
                        rows_updated = cur.rowcount

                    if rows_updated > 0:
                        loaded += rows_updated
                    else:
                        no_match += 1

            except Exception:
                skipped += 1

    if has_coords:
        msg = (
            "✅ *Relay prices updated — full upload*\n"
            f"{loaded} Pilot/Flying J stops loaded with coordinates\n"
            f"{skipped} rows skipped (not Pilot/FJ or missing data)\n"
            "📌 Coordinates stored permanently — future uploads can be price-only\n"
            "Old EFS/WEX prices in fuel\\_stops — unchanged"
        )
    else:
        msg = (
            "✅ *Relay prices updated — price-only update*\n"
            f"{loaded} stops updated (card price refreshed)\n"
            f"{no_match} rows skipped (station not found in DB — upload full file first)\n"
            f"{skipped} rows skipped (not Pilot/FJ or missing data)\n"
            "Old EFS/WEX prices in fuel\\_stops — unchanged"
        )
    return loaded, msg


# -- Fuel card system management ---------------------------------------------

def get_truck_card_system(vehicle_name: str) -> str:
    """Return 'old' or 'new' for a truck's fuel card system."""
    with db_cursor() as cur:
        cur.execute(
            "SELECT fuel_card_system FROM trucks WHERE vehicle_name = %s AND is_active = TRUE",
            (vehicle_name,)
        )
        row = cur.fetchone()
        return (row["fuel_card_system"] or 'old') if row else 'old'


def get_all_truck_card_systems() -> dict:
    """Return {vehicle_name: 'old'|'new'} for all active trucks in one query."""
    with db_cursor() as cur:
        cur.execute("SELECT vehicle_name, fuel_card_system FROM trucks WHERE is_active = TRUE")
        rows = cur.fetchall()
    return {r["vehicle_name"]: (r["fuel_card_system"] or 'old') for r in rows}


def set_truck_card_system(vehicle_name: str, system: str) -> list:
    """Set a truck's fuel card system ('old' or 'new').
    Matches by exact name OR by numeric truck ID anywhere in the name.
    e.g. '1637' matches 'Unit - 1637', '1079' matches '1079 - JEAN VOLMAR'.
    Returns list of actual vehicle_names updated (empty list = not found).
    """
    number = _truck_number(vehicle_name)
    with db_cursor() as cur:
        # Use word-boundary regex so '777' doesn't match '7770'
        cur.execute(
            """SELECT vehicle_name FROM trucks
               WHERE is_active = TRUE
                 AND (vehicle_name = %s
                      OR vehicle_name ~* %s)""",
            (vehicle_name, r'(^|[^0-9])' + number + r'([^0-9]|$)'),
        )
        matched = [r["vehicle_name"] for r in cur.fetchall()]
        if matched:
            cur.execute(
                "UPDATE trucks SET fuel_card_system = %s WHERE vehicle_name = ANY(%s) AND is_active = TRUE",
                (system, matched)
            )
    return matched


def cleanup_duplicate_trucks() -> list:
    """Remove duplicate truck rows that share the same numeric prefix.
    e.g. '8238', '8238 -', '8238 - JOHN DOE' are the same truck.
    Keeps the longest (most complete) name, deactivates the shorter duplicates.
    Returns list of (kept_name, removed_name) pairs.
    """
    removed = []
    with db_cursor() as cur:
        cur.execute("SELECT vehicle_name FROM trucks WHERE is_active = TRUE ORDER BY vehicle_name")
        all_names = [r["vehicle_name"] for r in cur.fetchall()]

    # Group names by their numeric truck ID (works for all name formats)
    from collections import defaultdict
    groups = defaultdict(list)
    for name in all_names:
        groups[_truck_number(name)].append(name)

    with db_cursor() as cur:
        for prefix, names in groups.items():
            if len(names) < 2:
                continue
            # Keep the longest name (most complete with driver info)
            names_sorted = sorted(names, key=len, reverse=True)
            keep  = names_sorted[0]
            dupes = names_sorted[1:]
            for dupe in dupes:
                cur.execute(
                    "UPDATE trucks SET is_active = FALSE WHERE vehicle_name = %s",
                    (dupe,)
                )
                removed.append((keep, dupe))

    return removed


def set_group_card_system(group_chat_id: str, system: str) -> list:
    """Move all trucks in a group to a fuel card system. Returns list of truck names updated."""
    with db_cursor() as cur:
        cur.execute(
            "SELECT vehicle_name FROM trucks WHERE telegram_group_id = %s AND is_active = TRUE",
            (str(group_chat_id),)
        )
        names = [row["vehicle_name"] for row in cur.fetchall()]
        if names:
            cur.execute(
                "UPDATE trucks SET fuel_card_system = %s WHERE telegram_group_id = %s AND is_active = TRUE",
                (system, str(group_chat_id))
            )
    return names


def get_stops_count() -> dict:
    """Return stop counts for both price tables."""
    with db_cursor() as cur:
        cur.execute("SELECT COUNT(*) as cnt FROM fuel_stops WHERE discounted_price IS NOT NULL")
        old_cnt = cur.fetchone()["cnt"]
        cur.execute("SELECT COUNT(*) as cnt FROM pilot_contracted_prices WHERE discounted_price IS NOT NULL")
        new_cnt = cur.fetchone()["cnt"]
    return {"old": old_cnt, "new": new_cnt}


def get_price_last_updated():
    with db_cursor() as cur:
        cur.execute("SELECT MAX(price_updated) as latest FROM fuel_stops")
        row = cur.fetchone()
        return row["latest"] if row else None


def get_alert_count_by_system() -> dict:
    """Return how many trucks are actively receiving fuel alerts, per card system.

    Returns:
        {
          'old': {'total': N, 'with_group': N, 'active': N, 'paused': N},
          'new': {'total': N, 'with_group': N, 'active': N, 'paused': N},
        }
    where 'active' = has Telegram group AND alerts not paused.
    """
    with db_cursor() as cur:
        cur.execute("""
            SELECT
                fuel_card_system,
                COUNT(*)                                                               AS total,
                COUNT(telegram_group_id)                                               AS with_group,
                COUNT(*) FILTER (
                    WHERE telegram_group_id IS NOT NULL
                      AND (alert_paused = FALSE OR alert_paused IS NULL)
                )                                                                      AS active,
                COUNT(*) FILTER (WHERE alert_paused = TRUE)                            AS paused
            FROM trucks
            WHERE is_active = TRUE
            GROUP BY fuel_card_system
            ORDER BY fuel_card_system
        """)
        result = {
            'old': {'total': 0, 'with_group': 0, 'active': 0, 'paused': 0},
            'new': {'total': 0, 'with_group': 0, 'active': 0, 'paused': 0},
        }
        for row in cur.fetchall():
            system = row['fuel_card_system'] or 'old'
            result[system] = {
                'total':      int(row['total']),
                'with_group': int(row['with_group']),
                'active':     int(row['active']),
                'paused':     int(row['paused']),
            }
    return result


# -- truck_states -------------------------------------------------------------

def load_all_truck_states() -> dict:
    """Load all truck states from DB. Returns {vehicle_id: state_dict}."""
    with db_cursor() as cur:
        cur.execute("SELECT * FROM truck_states")
        rows = cur.fetchall()

    states = {}
    for row in rows:
        r = dict(row)
        vid = r["vehicle_id"]
        states[vid] = {
            "vehicle_id":             vid,
            "vehicle_name":           r["vehicle_name"],
            "state":                  r["state"],
            "fuel_pct":               r["fuel_pct"],
            "lat":                    r["latitude"],
            "lng":                    r["longitude"],
            "speed_mph":              r["speed_mph"],
            "heading":                r["heading"],
            "next_poll":              _dt(r["next_poll"]),
            "parked_since":           _dt(r["parked_since"]),
            "alert_sent":             bool(r["alert_sent"]),
            "overnight_alert_sent":   bool(r["overnight_alert_sent"]),
            "open_alert_id":          r["open_alert_id"],
            "assigned_stop_id":       r["assigned_stop_id"],
            "assigned_stop_name":     r["assigned_stop_name"],
            "assigned_stop_lat":      r["assigned_stop_lat"],
            "assigned_stop_lng":      r["assigned_stop_lng"],
            "assignment_time":        _dt(r["assignment_time"]),
            "in_yard":                bool(r["in_yard"]),
            "yard_name":              r["yard_name"],
            "sleeping":               bool(r["sleeping"]),
            "fuel_when_parked":       r["fuel_when_parked"],
            "ca_reminder_sent":       bool(r["ca_reminder_sent"]),
            "prev_truck_group":       r.get("prev_truck_group"),
            "prev_truck_msg_id":      r.get("prev_truck_msg_id"),
            "prev_dispatcher_msg_id": r.get("prev_dispatcher_msg_id"),
            "prev_ca_truck_msg_id":      r.get("prev_ca_truck_msg_id"),
            "prev_ca_dispatcher_msg_id": r.get("prev_ca_dispatcher_msg_id"),
        }
    return states


def save_truck_state(state: dict):
    """Upsert a single truck state to DB."""
    sql = """
        INSERT INTO truck_states (
            vehicle_id, vehicle_name, state, fuel_pct,
            latitude, longitude, speed_mph, heading,
            next_poll, parked_since, alert_sent, overnight_alert_sent,
            open_alert_id, assigned_stop_id, assigned_stop_name,
            assigned_stop_lat, assigned_stop_lng, assignment_time,
            in_yard, yard_name, sleeping, fuel_when_parked,
            ca_reminder_sent, prev_truck_group, prev_truck_msg_id,
            prev_dispatcher_msg_id, prev_ca_truck_msg_id, prev_ca_dispatcher_msg_id, last_updated
        ) VALUES (
            %(vehicle_id)s, %(vehicle_name)s, %(state)s, %(fuel_pct)s,
            %(lat)s, %(lng)s, %(speed_mph)s, %(heading)s,
            %(next_poll)s, %(parked_since)s, %(alert_sent)s, %(overnight_alert_sent)s,
            %(open_alert_id)s, %(assigned_stop_id)s, %(assigned_stop_name)s,
            %(assigned_stop_lat)s, %(assigned_stop_lng)s, %(assignment_time)s,
            %(in_yard)s, %(yard_name)s, %(sleeping)s, %(fuel_when_parked)s,
            %(ca_reminder_sent)s, %(prev_truck_group)s, %(prev_truck_msg_id)s,
            %(prev_dispatcher_msg_id)s, %(prev_ca_truck_msg_id)s, %(prev_ca_dispatcher_msg_id)s, NOW()
        )
        ON CONFLICT (vehicle_id) DO UPDATE SET
            vehicle_name           = EXCLUDED.vehicle_name,
            state                  = EXCLUDED.state,
            fuel_pct               = EXCLUDED.fuel_pct,
            latitude               = EXCLUDED.latitude,
            longitude              = EXCLUDED.longitude,
            speed_mph              = EXCLUDED.speed_mph,
            heading                = EXCLUDED.heading,
            next_poll              = EXCLUDED.next_poll,
            parked_since           = EXCLUDED.parked_since,
            alert_sent             = EXCLUDED.alert_sent,
            overnight_alert_sent   = EXCLUDED.overnight_alert_sent,
            open_alert_id          = EXCLUDED.open_alert_id,
            assigned_stop_id       = EXCLUDED.assigned_stop_id,
            assigned_stop_name     = EXCLUDED.assigned_stop_name,
            assigned_stop_lat      = EXCLUDED.assigned_stop_lat,
            assigned_stop_lng      = EXCLUDED.assigned_stop_lng,
            assignment_time        = EXCLUDED.assignment_time,
            in_yard                = EXCLUDED.in_yard,
            yard_name              = EXCLUDED.yard_name,
            sleeping               = EXCLUDED.sleeping,
            fuel_when_parked       = EXCLUDED.fuel_when_parked,
            ca_reminder_sent       = EXCLUDED.ca_reminder_sent,
            prev_truck_group       = EXCLUDED.prev_truck_group,
            prev_truck_msg_id      = EXCLUDED.prev_truck_msg_id,
            prev_dispatcher_msg_id = EXCLUDED.prev_dispatcher_msg_id,
            prev_ca_truck_msg_id      = EXCLUDED.prev_ca_truck_msg_id,
            prev_ca_dispatcher_msg_id = EXCLUDED.prev_ca_dispatcher_msg_id,
            last_updated           = NOW()
    """
    with db_cursor() as cur:
        cur.execute(sql, {
            "vehicle_id":             state["vehicle_id"],
            "vehicle_name":           state.get("vehicle_name"),
            "state":                  state.get("state", "UNKNOWN"),
            "fuel_pct":               state.get("fuel_pct"),
            "lat":                    state.get("lat"),
            "lng":                    state.get("lng"),
            "speed_mph":              state.get("speed_mph"),
            "heading":                state.get("heading"),
            "next_poll":              state.get("next_poll"),
            "parked_since":           state.get("parked_since"),
            "alert_sent":             bool(state.get("alert_sent", False)),
            "overnight_alert_sent":   bool(state.get("overnight_alert_sent", False)),
            "open_alert_id":          state.get("open_alert_id"),
            "assigned_stop_id":       state.get("assigned_stop_id"),
            "assigned_stop_name":     state.get("assigned_stop_name"),
            "assigned_stop_lat":      state.get("assigned_stop_lat"),
            "assigned_stop_lng":      state.get("assigned_stop_lng"),
            "assignment_time":        state.get("assignment_time"),
            "in_yard":                bool(state.get("in_yard", False)),
            "yard_name":              state.get("yard_name"),
            "sleeping":               bool(state.get("sleeping", False)),
            "fuel_when_parked":       state.get("fuel_when_parked"),
            "ca_reminder_sent":       bool(state.get("ca_reminder_sent", False)),
            "prev_truck_group":       state.get("prev_truck_group"),
            "prev_truck_msg_id":      state.get("prev_truck_msg_id"),
            "prev_dispatcher_msg_id": state.get("prev_dispatcher_msg_id"),
            "prev_ca_truck_msg_id":      state.get("prev_ca_truck_msg_id"),
            "prev_ca_dispatcher_msg_id": state.get("prev_ca_dispatcher_msg_id"),
        })


def save_all_truck_states(states: dict):
    for state in states.values():
        save_truck_state(state)


def reset_truck_states():
    with db_cursor() as cur:
        cur.execute("DELETE FROM truck_states")
    log.info("✅ Truck states reset.")


# -- fuel_alerts --------------------------------------------------------------

def create_fuel_alert(vehicle_id, vehicle_name, fuel_pct, lat, lng,
                      heading, speed_mph, alert_type="low_fuel",
                      best_stop=None, alt_stop=None, savings_usd=None) -> int:
    sql = """
        INSERT INTO fuel_alerts (
            vehicle_id, vehicle_name, fuel_pct, latitude, longitude,
            heading, speed_mph, alert_type,
            best_stop_id, best_stop_name, best_stop_price, best_stop_state,
            alt_stop_id,  alt_stop_name,  alt_stop_price, savings_usd
        ) VALUES (
            %(vehicle_id)s, %(vehicle_name)s, %(fuel_pct)s, %(lat)s, %(lng)s,
            %(heading)s, %(speed_mph)s, %(alert_type)s,
            %(best_stop_id)s, %(best_stop_name)s, %(best_stop_price)s, %(best_stop_state)s,
            %(alt_stop_id)s,  %(alt_stop_name)s,  %(alt_stop_price)s, %(savings_usd)s
        ) RETURNING id
    """
    with db_cursor() as cur:
        cur.execute(sql, {
            "vehicle_id":      vehicle_id,
            "vehicle_name":    vehicle_name,
            "fuel_pct":        fuel_pct,
            "lat":             lat,
            "lng":             lng,
            "heading":         heading,
            "speed_mph":       speed_mph,
            "alert_type":      alert_type,
            "best_stop_id":    best_stop["id"]          if best_stop else None,
            "best_stop_name":  best_stop["store_name"]  if best_stop else None,
            "best_stop_price": best_stop["diesel_price"] if best_stop else None,
            "best_stop_state": best_stop["state"]       if best_stop else None,
            "alt_stop_id":     alt_stop["id"]           if alt_stop else None,
            "alt_stop_name":   alt_stop["store_name"]   if alt_stop else None,
            "alt_stop_price":  alt_stop["diesel_price"]  if alt_stop else None,
            "savings_usd":     savings_usd,
        })
        return cur.fetchone()["id"]


def resolve_alert(alert_id: int):
    with db_cursor() as cur:
        cur.execute(
            "UPDATE fuel_alerts SET status='resolved', resolved_at=NOW() WHERE id=%s",
            (alert_id,)
        )


# -- Aliases ------------------------------------------------------------------

def get_bot_config(key: str) -> str | None:
    return get_config_value(key)

def set_bot_config(key: str, value: str):
    set_config_value(key, value)

def save_truck_route(truck_number: str, group_chat_id: str, route: dict) -> None:
    """Save parsed QM Notifier route for a truck."""
    with db_cursor() as cur:
        cur.execute("""
            INSERT INTO truck_routes (truck_number, group_chat_id, trip_num, ref_number, route_json, updated_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (truck_number) DO UPDATE SET
                group_chat_id = EXCLUDED.group_chat_id,
                trip_num      = EXCLUDED.trip_num,
                ref_number    = EXCLUDED.ref_number,
                route_json    = EXCLUDED.route_json,
                updated_at    = NOW()
        """, (
            truck_number,
            group_chat_id,
            str(route.get("trip_num", "")),
            str(route.get("ref_number", "")),
            json.dumps(route),
        ))
    log.info(f"Route saved for truck {truck_number}: trip {route.get('trip_num')}")


def get_truck_route(truck_number: str) -> dict | None:
    """Get the last saved route for a truck."""
    with db_cursor() as cur:
        cur.execute(
            "SELECT route_json FROM truck_routes WHERE truck_number = %s",
            (truck_number,)
        )
        row = cur.fetchone()
        if row and row["route_json"]:
            return json.loads(row["route_json"])
    return None


def get_all_truck_routes_from_db() -> dict[str, dict]:
    """Get all saved routes keyed by truck_number."""
    routes = {}
    with db_cursor() as cur:
        cur.execute("SELECT truck_number, route_json FROM truck_routes")
        for row in cur.fetchall():
            if row["route_json"]:
                try:
                    routes[row["truck_number"]] = json.loads(row["route_json"])
                except Exception:
                    pass
    return routes



def log_stop_visit(vehicle_name: str, alert_id: int,
                   recommended_stop_name: str,
                   recommended_lat: float, recommended_lng: float,
                   actual_stop_name: str,
                   actual_lat: float, actual_lng: float,
                   visited: bool,
                   fuel_before: float, fuel_after: float,
                   actual_stop_state: str = None,
                   gallons_purchased: float = None,
                   savings_usd: float = None) -> int:
    """Insert a stop visit row. Returns the new row ID."""
    from datetime import datetime, timezone
    if gallons_purchased is None and fuel_before is not None and fuel_after is not None:
        fuel_delta = max(float(fuel_after) - float(fuel_before), 0.0)
        gallons_purchased = round(150.0 * fuel_delta / 100.0, 1) if fuel_delta > 0 else 0.0
    with db_cursor() as cur:
        cur.execute("""
            INSERT INTO stop_visits (
                vehicle_name, alert_id,
                recommended_stop_name, recommended_stop_lat, recommended_stop_lng,
                actual_stop_name, actual_stop_lat, actual_stop_lng, actual_stop_state,
                visited, fuel_before, fuel_after, gallons_purchased, savings_usd, visited_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
        """, (
            vehicle_name, alert_id,
            recommended_stop_name, recommended_lat, recommended_lng,
            actual_stop_name, actual_lat, actual_lng, actual_stop_state,
            visited, fuel_before, fuel_after, gallons_purchased, savings_usd,
            datetime.now(timezone.utc)
        ))
        return cur.fetchone()["id"]


def update_stop_visit_refuel(
    visit_id: int,
    fuel_after: float,
    gallons_purchased: float,
    actual_stop_state: str = None,
    savings_usd: float = None,
) -> None:
    """Update an existing stop_visit row with actual refuel data (gallons, fuel_after)."""
    with db_cursor() as cur:
        cur.execute("""
            UPDATE stop_visits SET
                fuel_after        = %s,
                gallons_purchased = %s,
                actual_stop_state = COALESCE(%s, actual_stop_state),
                savings_usd       = COALESCE(%s, savings_usd)
            WHERE id = %s
        """, (fuel_after, gallons_purchased, actual_stop_state, savings_usd, visit_id))


def log_driver_flag(
    truck_id: str,
    driver_name: str,
    flag_type: str,
    planned_stop: str,
    actual_stop: str,
    fuel_pct: float,
    details: str = None,
    savings_lost: float = None,
) -> int:
    """
    Log a driver compliance flag to driver_flags.

    Creates the table and adds driver_name/planned_stop columns if they don't
    exist yet (safe to call alongside flag_system.save_flag).

    Returns the new row id.
    """
    with db_cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS driver_flags (
                id           SERIAL PRIMARY KEY,
                flagged_at   TIMESTAMPTZ DEFAULT NOW(),
                vehicle_name TEXT NOT NULL,
                flag_type    TEXT NOT NULL,
                details      TEXT,
                recommended_stop TEXT,
                actual_stop  TEXT,
                fuel_pct     REAL,
                state        TEXT,
                savings_lost REAL
            )
        """)
        for col_sql in [
            "ALTER TABLE driver_flags ADD COLUMN IF NOT EXISTS driver_name TEXT",
            "ALTER TABLE driver_flags ADD COLUMN IF NOT EXISTS planned_stop TEXT",
        ]:
            cur.execute(col_sql)

        cur.execute(
            """
            INSERT INTO driver_flags
                (vehicle_name, driver_name, flag_type, planned_stop,
                 actual_stop, fuel_pct, details, savings_lost)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (truck_id, driver_name or "", flag_type,
             planned_stop, actual_stop, fuel_pct, details, savings_lost),
        )
        return cur.fetchone()["id"]


def get_stop_compliance(vehicle_name: str = None, days: int = 7) -> list:
    """Get stop visit compliance stats."""
    from datetime import datetime, timezone, timedelta
    since = datetime.now(timezone.utc) - timedelta(days=days)
    with db_cursor() as cur:
        if vehicle_name:
            cur.execute("""
                SELECT vehicle_name, recommended_stop_name, actual_stop_name,
                       visited, fuel_before, fuel_after, visited_at
                FROM stop_visits
                WHERE vehicle_name = %s AND visited_at >= %s
                ORDER BY visited_at DESC LIMIT 20
            """, (vehicle_name, since))
        else:
            cur.execute("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE visited = TRUE)  AS visited,
                    COUNT(*) FILTER (WHERE visited = FALSE) AS skipped,
                    COUNT(*) FILTER (WHERE visited IS NULL) AS unknown
                FROM stop_visits WHERE visited_at >= %s
            """, (since,))
        return cur.fetchall()

def save_truck_efficiency(vehicle_id: str, vehicle_name: str, 
                           mpg: float, idle_hours: float, 
                           idle_pct: float, fuel_gal: float) -> None:
    """Save real MPG and idle data from Samsara."""
    with db_cursor() as cur:
        cur.execute("""
            INSERT INTO truck_efficiency 
                (vehicle_id, vehicle_name, mpg, idle_hours_30d, idle_pct_30d, fuel_used_30d, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (vehicle_id) DO UPDATE SET
                vehicle_name   = EXCLUDED.vehicle_name,
                mpg            = EXCLUDED.mpg,
                idle_hours_30d = EXCLUDED.idle_hours_30d,
                idle_pct_30d   = EXCLUDED.idle_pct_30d,
                fuel_used_30d  = EXCLUDED.fuel_used_30d,
                updated_at     = NOW()
        """, (vehicle_id, vehicle_name, mpg, idle_hours, idle_pct, fuel_gal))


def get_all_truck_efficiency() -> list:
    """Get all truck efficiency stats."""
    with db_cursor() as cur:
        cur.execute("""
            SELECT vehicle_name, mpg, idle_hours_30d, idle_pct_30d, fuel_used_30d, updated_at
            FROM truck_efficiency
            ORDER BY mpg ASC
        """)
        return cur.fetchall()

def get_truck_params(vehicle_name: str) -> dict | None:
    """Get tank size and MPG for a truck. Returns None if not found."""
    with db_cursor() as cur:
        cur.execute(
            """SELECT tank_capacity_gal AS tank_gal, avg_mpg AS mpg
               FROM trucks WHERE vehicle_name = %s AND is_active = TRUE""",
            (vehicle_name,)
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_truck_mpg(vehicle_id: str) -> float:
    """Get real MPG for a truck from Samsara efficiency data. Returns 6.5 default."""
    with db_cursor() as cur:
        try:
            cur.execute(
                "SELECT mpg FROM truck_efficiency WHERE vehicle_id = %s AND mpg > 3",
                (vehicle_id,)
            )
            row = cur.fetchone()
            if row and row["mpg"]:
                return float(row["mpg"])
        except Exception:
            pass
    return 6.5


# ═══════════════════════════════════════════════════════════════════
# DATA RETENTION POLICY — NOTHING IS EVER DELETED
# fuel_alerts, stop_visits, driver_flags, trip_state all grow forever
# Weekly report queries by date range — data stays in DB permanently
# ═══════════════════════════════════════════════════════════════════

def get_flags_for_report(days: int = 7) -> list:
    """Get all flags for weekly Excel report."""
    from datetime import datetime, timezone, timedelta
    since = datetime.now(timezone.utc) - timedelta(days=days)
    with db_cursor() as cur:
        try:
            cur.execute("""
                SELECT vehicle_name, flag_type, details,
                       recommended_stop, actual_stop,
                       fuel_pct, state, card_price, savings_lost,
                       flagged_at
                FROM driver_flags
                WHERE flagged_at >= %s
                ORDER BY flagged_at DESC
            """, (since,))
            return _rows(cur.fetchall())
        except Exception:
            return []


def get_compliance_for_report(days: int = 7) -> list:
    """Get per-truck compliance stats for weekly Excel report."""
    from datetime import datetime, timezone, timedelta
    since = datetime.now(timezone.utc) - timedelta(days=days)
    with db_cursor() as cur:
        try:
            cur.execute("""
                SELECT
                    vehicle_name,
                    COUNT(*) FILTER (WHERE visited = TRUE)  AS visited,
                    COUNT(*) FILTER (WHERE visited = FALSE) AS skipped,
                    COUNT(*) AS total,
                    COALESCE(SUM(CASE WHEN visited THEN savings_usd ELSE 0 END), 0) AS savings,
                    COALESCE(SUM(CASE WHEN NOT visited THEN ABS(savings_usd) ELSE 0 END), 0) AS losses
                FROM stop_visits
                WHERE visited_at >= %s
                GROUP BY vehicle_name
                ORDER BY vehicle_name
            """, (since,))
            return _rows(cur.fetchall())
        except Exception:
            return []


def save_trip_state(vehicle_name: str, state: dict) -> None:
    """Save trip planning state to DB — survives bot restarts."""
    # Collect all border_warned_{STATE} keys into a single dict
    border_warned = {
        k.replace("border_warned_", ""): v
        for k, v in state.items()
        if k.startswith("border_warned_") and v
    }

    with db_cursor() as cur:
        cur.execute("""
            INSERT INTO trip_state (
                vehicle_name, trip_num, trip_status,
                briefing_sent_trip,
                all_planned_stops, planned_stop_index,
                assigned_stop_name, assigned_stop_lat, assigned_stop_lng,
                assigned_stop_card_price, assigned_stop_net_price,
                missed_stop_name, missed_stop_card_price,
                completed_waypoints, border_warned, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
            )
            ON CONFLICT (vehicle_name) DO UPDATE SET
                trip_num                 = EXCLUDED.trip_num,
                trip_status              = EXCLUDED.trip_status,
                briefing_sent_trip       = EXCLUDED.briefing_sent_trip,
                all_planned_stops        = EXCLUDED.all_planned_stops,
                planned_stop_index       = EXCLUDED.planned_stop_index,
                assigned_stop_name       = EXCLUDED.assigned_stop_name,
                assigned_stop_lat        = EXCLUDED.assigned_stop_lat,
                assigned_stop_lng        = EXCLUDED.assigned_stop_lng,
                assigned_stop_card_price = EXCLUDED.assigned_stop_card_price,
                assigned_stop_net_price  = EXCLUDED.assigned_stop_net_price,
                missed_stop_name         = EXCLUDED.missed_stop_name,
                missed_stop_card_price   = EXCLUDED.missed_stop_card_price,
                completed_waypoints      = EXCLUDED.completed_waypoints,
                border_warned            = EXCLUDED.border_warned,
                updated_at               = NOW()
        """, (
            vehicle_name,
            state.get("qm_route", {}).get("trip_num") if state.get("qm_route") else None,
            state.get("last_trip_status"),
            state.get("briefing_sent_trip"),
            json.dumps(state.get("all_planned_stops") or []),
            state.get("planned_stop_index", 0),
            state.get("assigned_stop_name"),
            state.get("assigned_stop_lat"),
            state.get("assigned_stop_lng"),
            state.get("assigned_stop_card_price"),
            state.get("assigned_stop_net_price"),
            state.get("missed_stop_name"),
            state.get("missed_stop_card_price"),
            json.dumps(list(state.get("completed_waypoints") or [])),
            json.dumps(border_warned),
        ))


def _parse_trip_state_row(row: dict) -> dict:
    result = {
        "briefing_sent_trip":       row.get("briefing_sent_trip"),
        "planned_stop_index":       row.get("planned_stop_index") or 0,
        "assigned_stop_name":       row.get("assigned_stop_name"),
        "assigned_stop_lat":        row.get("assigned_stop_lat"),
        "assigned_stop_lng":        row.get("assigned_stop_lng"),
        "assigned_stop_card_price": row.get("assigned_stop_card_price"),
        "assigned_stop_net_price":  row.get("assigned_stop_net_price"),
        "missed_stop_name":         row.get("missed_stop_name"),
        "missed_stop_card_price":   row.get("missed_stop_card_price"),
    }
    try:
        result["all_planned_stops"] = json.loads(row.get("all_planned_stops") or "[]")
    except (json.JSONDecodeError, TypeError):
        result["all_planned_stops"] = []
    try:
        result["completed_waypoints"] = set(json.loads(row.get("completed_waypoints") or "[]"))
    except (json.JSONDecodeError, TypeError):
        result["completed_waypoints"] = set()
    try:
        bw = json.loads(row.get("border_warned") or "{}")
        for state_code, warned in bw.items():
            if warned:
                result[f"border_warned_{state_code}"] = True
    except (json.JSONDecodeError, TypeError):
        pass
    return result


def load_trip_state(vehicle_name: str) -> dict:
    """Load persisted trip state for a truck. Returns {} if not found."""
    with db_cursor() as cur:
        cur.execute("SELECT * FROM trip_state WHERE vehicle_name = %s", (vehicle_name,))
        row = cur.fetchone()
    return _parse_trip_state_row(dict(row)) if row else {}


def load_all_trip_states() -> dict:
    """Load all persisted trip states on bot startup. Returns {vehicle_name: state_dict}."""
    with db_cursor() as cur:
        cur.execute("SELECT * FROM trip_state")
        rows = cur.fetchall()
    return {row["vehicle_name"]: _parse_trip_state_row(dict(row)) for row in rows}


# -- Driver group change detection --------------------------------------------

def log_driver_group_event(truck_number: str, event_type: str, group_id: str = None,
                           driver_name: str = None, detected_by: str = 'realtime') -> None:
    with db_cursor() as cur:
        cur.execute("""
            INSERT INTO driver_group_events (truck_number, event_type, group_id, driver_name, detected_by)
            VALUES (%s, %s, %s, %s, %s)
        """, (truck_number, event_type, group_id, driver_name, detected_by))


def set_truck_alert_paused(vehicle_name: str, paused: bool, reason: str = None,
                           group_verified: bool = None) -> None:
    with db_cursor() as cur:
        if group_verified is not None:
            cur.execute("""
                UPDATE trucks
                   SET alert_paused = %s, pause_reason = %s, group_verified = %s
                 WHERE vehicle_name = %s
            """, (paused, reason, group_verified, vehicle_name))
        else:
            cur.execute("""
                UPDATE trucks
                   SET alert_paused = %s, pause_reason = %s
                 WHERE vehicle_name = %s
            """, (paused, reason, vehicle_name))


def bulk_resume_bot_kicked() -> int:
    """Un-pause all trucks paused with reason='bot_kicked' (heartbeat false-positives).
    Returns the number of trucks resumed."""
    with db_cursor() as cur:
        cur.execute("""
            UPDATE trucks
               SET alert_paused = FALSE, pause_reason = NULL
             WHERE alert_paused = TRUE
               AND pause_reason = 'bot_kicked'
               AND is_active = TRUE
        """)
        return cur.rowcount


def set_truck_group_verified(vehicle_name: str, verified: bool) -> None:
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc) if verified else None
    with db_cursor() as cur:
        cur.execute("""
            UPDATE trucks
               SET group_verified = %s, last_verified_at = %s,
                   alert_paused = FALSE, pause_reason = NULL
             WHERE vehicle_name = %s
        """, (verified, now, vehicle_name))


def get_truck_alert_status(vehicle_name: str) -> dict | None:
    with db_cursor() as cur:
        cur.execute("""
            SELECT alert_paused, pause_reason, group_verified, last_verified_at,
                   telegram_group_id, vehicle_name
              FROM trucks
             WHERE vehicle_name = %s AND is_active = TRUE
        """, (vehicle_name,))
        return _row(cur.fetchone())


def assign_driver(vehicle_name: str, driver_name: str, group_id: str,
                  assigned_by: str = 'admin') -> None:
    with db_cursor() as cur:
        cur.execute("""
            UPDATE drivers SET is_active = FALSE
             WHERE truck_number = %s AND is_active = TRUE
        """, (vehicle_name,))
        cur.execute("""
            INSERT INTO drivers (truck_number, driver_name, telegram_group, assigned_at, assigned_by, is_active)
            VALUES (%s, %s, %s, NOW(), %s, TRUE)
        """, (vehicle_name, driver_name, group_id, assigned_by))
        cur.execute("""
            UPDATE trucks
               SET telegram_group_id = %s, alert_paused = FALSE, pause_reason = NULL,
                   group_verified = FALSE
             WHERE vehicle_name = %s
        """, (group_id, vehicle_name))


def remove_driver(vehicle_name: str) -> bool:
    with db_cursor() as cur:
        cur.execute("""
            UPDATE drivers SET is_active = FALSE
             WHERE truck_number = %s AND is_active = TRUE
        """, (vehicle_name,))
        rows = cur.rowcount
        cur.execute("""
            UPDATE trucks
               SET alert_paused = TRUE, pause_reason = 'manually_removed'
             WHERE vehicle_name = %s
        """, (vehicle_name,))
    return rows > 0


def get_active_driver(vehicle_name: str) -> dict | None:
    with db_cursor() as cur:
        cur.execute("""
            SELECT driver_name, telegram_group, assigned_at, assigned_by
              FROM drivers
             WHERE truck_number = %s AND is_active = TRUE
             ORDER BY assigned_at DESC LIMIT 1
        """, (vehicle_name,))
        return _row(cur.fetchone())


def get_driver_event_log(vehicle_name: str, limit: int = 10) -> list:
    with db_cursor() as cur:
        cur.execute("""
            SELECT event_type, detected_by, group_id, driver_name, created_at
              FROM driver_group_events
             WHERE truck_number = %s
             ORDER BY created_at DESC
             LIMIT %s
        """, (vehicle_name, limit))
        return _rows(cur.fetchall())


def get_all_driver_statuses() -> list:
    with db_cursor() as cur:
        cur.execute("""
            SELECT t.vehicle_name, t.telegram_group_id, t.alert_paused, t.pause_reason,
                   t.group_verified, d.driver_name
              FROM trucks t
              LEFT JOIN drivers d ON d.truck_number = t.vehicle_name AND d.is_active = TRUE
             WHERE t.is_active = TRUE
             ORDER BY t.vehicle_name
        """)
        return _rows(cur.fetchall())


def resolve_truck_by_number(token: str) -> str | None:
    """Return vehicle_name for a truck identified by partial number (e.g. '1637').
    Matches the trucks table using word-boundary regex — same logic as set_truck_card_system.
    Returns None if not found."""
    import re
    m = re.search(r'\b(\d{3,})\b', token or '')
    number = m.group(1) if m else token.strip()
    with db_cursor() as cur:
        cur.execute("""
            SELECT vehicle_name FROM trucks
             WHERE is_active = TRUE
               AND (vehicle_name = %s OR vehicle_name ~* %s)
             ORDER BY LENGTH(vehicle_name) DESC
             LIMIT 1
        """, (token, r'(^|[^0-9])' + number + r'([^0-9]|$)'))
        row = cur.fetchone()
        return row["vehicle_name"] if row else None
