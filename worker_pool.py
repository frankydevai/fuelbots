"""
worker_pool.py — Parallel truck processing with safety guarantees.

Why this exists
───────────────
With 50 trucks, sequential processing means one slow truck (e.g. a hung
DB query, a slow Telegram send) holds up every other truck. On cold start
or /checknow, all 50 trucks are due at once and the cycle takes 30+ seconds.

Guarantees
──────────
1. Per-truck mutex     — same vehicle_id can never be processed twice in
                         parallel (avoids state races)
2. Per-truck timeout   — one stuck truck can't block the cycle
3. Bounded parallelism — at most max_workers DB connections used at once
4. Stats collection    — successes/errors/timeouts visible per cycle
"""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeout

import metrics

log = logging.getLogger(__name__)


class TruckWorkerPool:
    def __init__(self, max_workers: int = 8, per_truck_timeout: float = 30.0):
        self.max_workers      = max_workers
        self.per_truck_timeout = per_truck_timeout
        self._executor        = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="truck"
        )
        self._truck_locks: dict[str, threading.Lock] = {}
        self._locks_lock = threading.Lock()

    def _lock_for(self, vid: str) -> threading.Lock:
        """Return (creating if needed) the per-truck lock."""
        with self._locks_lock:
            lk = self._truck_locks.get(vid)
            if lk is None:
                lk = threading.Lock()
                self._truck_locks[vid] = lk
            return lk

    def process_batch(self, trucks: list, process_fn) -> dict:
        """
        Submit each truck to the pool and collect results.
        process_fn(truck) is invoked per truck.

        Returns: {"ok": int, "errored": int, "timed_out": int, "skipped": int}
        """
        stats = {"ok": 0, "errored": 0, "timed_out": 0, "skipped": 0}
        if not trucks:
            return stats

        def _run(truck):
            vid   = truck["vehicle_id"]
            vname = truck.get("vehicle_name") or vid
            lock  = self._lock_for(vid)
            if not lock.acquire(blocking=False):
                log.warning(f"Truck {vname}: skipped — previous cycle still in progress")
                return "skipped"
            try:
                with metrics.Timer("truck_process_seconds"):
                    process_fn(truck)
                return "ok"
            finally:
                lock.release()

        futures = {self._executor.submit(_run, t): t for t in trucks}
        deadline_per_call = self.per_truck_timeout

        try:
            for fut in as_completed(futures, timeout=deadline_per_call * len(trucks) + 5):
                truck = futures[fut]
                vname = truck.get("vehicle_name", "?")
                try:
                    outcome = fut.result(timeout=deadline_per_call)
                    if outcome == "skipped":
                        stats["skipped"] += 1
                    else:
                        stats["ok"] += 1
                except FutureTimeout:
                    stats["timed_out"] += 1
                    metrics.incr("truck_process_timeouts_total")
                    log.error(f"Truck {vname}: TIMED OUT after {deadline_per_call}s")
                except Exception as e:
                    stats["errored"] += 1
                    metrics.incr("truck_process_errors_total")
                    log.error(f"Truck {vname}: {e}", exc_info=True)
        except FutureTimeout:
            log.error("Worker pool batch timed out as a whole — moving on")

        metrics.gauge("trucks_processed_last_cycle",  stats["ok"])
        metrics.gauge("trucks_errored_last_cycle",    stats["errored"])
        metrics.gauge("trucks_timed_out_last_cycle",  stats["timed_out"])
        return stats

    def shutdown(self) -> None:
        self._executor.shutdown(wait=True)
