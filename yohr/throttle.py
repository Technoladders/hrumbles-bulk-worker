"""
yohr/throttle.py
Run a periodic maintenance step at most once per interval, from inside a job
that ticks much more often.

The stuck-row resets in resume_downloader / ai_processor only act on rows
untouched for 10 / 5 minutes, but they ran on every 15 / 20 s tick. Their
filter (org_id + an in-progress status + updated_at) matches no index on
org_csv_import_rows, so each tick was a full scan of the table even when
nothing was stuck — the single largest DB cost in production
(~6,300 min of execution time in 12.6 days).

State is per process: after a container restart the first tick runs the
reset immediately, which is exactly when rows are most likely to be stuck.
"""
import threading
import time

_lock = threading.Lock()
_last_run: dict[str, float] = {}


def due(key: str, interval_seconds: float) -> bool:
    """True (and records the run) if `key` hasn't run in the last interval."""
    now = time.monotonic()
    with _lock:
        last = _last_run.get(key)
        if last is not None and now - last < interval_seconds:
            return False
        _last_run[key] = now
        return True


def reset_all() -> None:
    """Forget all recorded runs (tests)."""
    with _lock:
        _last_run.clear()
