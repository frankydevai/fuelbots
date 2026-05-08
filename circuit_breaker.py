"""
circuit_breaker.py — Protect against cascading failures from external APIs.

When Samsara / Telegram / QuickManage have an outage, retrying every poll
cycle wastes resources and floods logs. The circuit breaker fails fast
during outages, then probes the API after a cooldown.

STATES
──────
  CLOSED     normal — requests pass through
  OPEN       failing — reject calls instantly until cooldown elapses
  HALF_OPEN  cooldown elapsed — allow one probe; success → CLOSED, failure → OPEN

USAGE
─────
    samsara_breaker = CircuitBreaker("samsara", fail_threshold=5, cooldown=60)

    # Option 1 — explicit:
    try:
        data = samsara_breaker.call(fetch_locations)
    except CircuitOpenError:
        log.warning("samsara is open — skipping")

    # Option 2 — decorator:
    @samsara_breaker.guard
    def fetch_locations(): ...
"""

import logging
import threading
import time
from enum import Enum
from functools import wraps

import metrics

log = logging.getLogger(__name__)


class State(Enum):
    CLOSED    = "closed"
    OPEN      = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(Exception):
    """Raised when a call is rejected because the breaker is open."""


class CircuitBreaker:
    def __init__(
        self,
        name: str,
        fail_threshold: int = 5,
        cooldown: float = 60,
    ):
        self.name           = name
        self.fail_threshold = fail_threshold
        self.cooldown       = cooldown

        self._state      = State.CLOSED
        self._failures   = 0
        self._opened_at  = 0.0
        self._lock       = threading.Lock()

    # -- internal --------------------------------------------------------------

    def _can_attempt(self) -> bool:
        with self._lock:
            if self._state is State.CLOSED:
                return True
            if self._state is State.OPEN:
                if time.monotonic() - self._opened_at >= self.cooldown:
                    self._state = State.HALF_OPEN
                    log.info(f"CircuitBreaker[{self.name}]: → HALF_OPEN (probing)")
                    return True
                return False
            # HALF_OPEN: allow probe
            return True

    def _record_success(self) -> None:
        with self._lock:
            if self._state is not State.CLOSED:
                log.info(f"CircuitBreaker[{self.name}]: → CLOSED (recovered)")
            self._state    = State.CLOSED
            self._failures = 0
        metrics.incr(f"circuit_{self.name}_success_total")

    def _record_failure(self) -> None:
        opened_now = False
        with self._lock:
            self._failures += 1
            if self._state is State.HALF_OPEN:
                self._state    = State.OPEN
                self._opened_at = time.monotonic()
                opened_now     = True
            elif self._failures >= self.fail_threshold and self._state is not State.OPEN:
                self._state    = State.OPEN
                self._opened_at = time.monotonic()
                opened_now     = True
        if opened_now:
            log.warning(f"CircuitBreaker[{self.name}]: → OPEN ({self._failures} failures)")
            metrics.incr(f"circuit_{self.name}_opened_total")
        metrics.incr(f"circuit_{self.name}_failure_total")

    # -- public ----------------------------------------------------------------

    def call(self, fn, *args, **kwargs):
        if not self._can_attempt():
            metrics.incr(f"circuit_{self.name}_rejected_total")
            raise CircuitOpenError(f"circuit {self.name!r} is OPEN")
        try:
            result = fn(*args, **kwargs)
        except Exception:
            self._record_failure()
            raise
        else:
            self._record_success()
            return result

    def guard(self, fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            return self.call(fn, *args, **kwargs)
        return wrapper

    def state(self) -> str:
        with self._lock:
            return self._state.value


# -- Shared breakers ----------------------------------------------------------
# Tunable per service. Use these everywhere instead of constructing new ones,
# so multiple call-sites for the same API share state.

samsara_breaker  = CircuitBreaker("samsara",  fail_threshold=5, cooldown=60)
telegram_breaker = CircuitBreaker("telegram", fail_threshold=8, cooldown=30)
quickmng_breaker = CircuitBreaker("quickmng", fail_threshold=4, cooldown=120)
