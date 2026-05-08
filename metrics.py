"""
metrics.py — Lightweight in-process metrics. Thread-safe, no external deps.

Exposed at /metrics by main.py. Counters never reset; gauges and timers
reflect current/recent state.

USAGE
─────
    from metrics import incr, gauge, Timer

    incr("alerts_fired_total")
    gauge("trucks_due_now", len(due_trucks))

    with Timer("samsara_fetch"):
        data = samsara.fetch()
"""

import threading
import time
from collections import defaultdict, deque

_TIMER_WINDOW = 200   # last N samples kept per timer

_lock     = threading.Lock()
_counters: dict[str, int]   = defaultdict(int)
_gauges:   dict[str, float] = {}
_timers:   dict[str, deque] = defaultdict(lambda: deque(maxlen=_TIMER_WINDOW))


def incr(name: str, by: int = 1) -> None:
    with _lock:
        _counters[name] += by


def gauge(name: str, value: float) -> None:
    with _lock:
        _gauges[name] = float(value)


class Timer:
    """Context manager — records elapsed seconds into a rolling window."""
    __slots__ = ("name", "_start")

    def __init__(self, name: str):
        self.name = name

    def __enter__(self):
        self._start = time.perf_counter()
        return self

    def __exit__(self, *_):
        elapsed = time.perf_counter() - self._start
        with _lock:
            _timers[self.name].append(elapsed)


def _percentile(sorted_samples: list[float], pct: int) -> float:
    if not sorted_samples:
        return 0.0
    k = int(len(sorted_samples) * pct / 100)
    return sorted_samples[min(k, len(sorted_samples) - 1)]


def snapshot() -> dict:
    """Return a JSON-serializable snapshot of all metrics."""
    with _lock:
        timer_summary = {}
        for name, samples in _timers.items():
            if not samples:
                continue
            s = sorted(samples)
            timer_summary[name] = {
                "count":  len(s),
                "avg_ms": round(sum(s) / len(s) * 1000, 2),
                "p50_ms": round(_percentile(s, 50) * 1000, 2),
                "p95_ms": round(_percentile(s, 95) * 1000, 2),
                "max_ms": round(max(s) * 1000, 2),
            }
        return {
            "counters": dict(_counters),
            "gauges":   dict(_gauges),
            "timers":   timer_summary,
        }


def render_text() -> str:
    """Plain-text format suitable for the /metrics endpoint."""
    snap = snapshot()
    lines = []

    if snap["counters"]:
        lines.append("# COUNTERS")
        for k in sorted(snap["counters"]):
            lines.append(f"{k} {snap['counters'][k]}")
        lines.append("")

    if snap["gauges"]:
        lines.append("# GAUGES")
        for k in sorted(snap["gauges"]):
            lines.append(f"{k} {snap['gauges'][k]}")
        lines.append("")

    if snap["timers"]:
        lines.append("# TIMERS (ms)")
        for k in sorted(snap["timers"]):
            t = snap["timers"][k]
            lines.append(
                f"{k}  count={t['count']} "
                f"avg={t['avg_ms']} p50={t['p50_ms']} "
                f"p95={t['p95_ms']} max={t['max_ms']}"
            )
    return "\n".join(lines) + "\n"
