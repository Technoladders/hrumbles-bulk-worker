"""
yohr/scheduler.py
APScheduler job registrations for the YOHR CSV import pipeline.

Usage in app.py:
    from yohr.scheduler import register_yohr_jobs
    register_yohr_jobs(scheduler)   # pass your existing APScheduler instance
"""
import logging

logger = logging.getLogger(__name__)


def register_yohr_jobs(scheduler) -> None:
    """Register all four YOHR pipeline stages with the provided APScheduler instance."""
    from .csv_parser        import run_csv_parser    # noqa: F401 — lazy import avoids circular
    from .resume_downloader import run_downloader
    from .ai_processor      import run_ai_processor
    from .ingestor          import run_ingestor

    scheduler.add_job(
        func=_safe(run_csv_parser),
        trigger="interval", seconds=30,
        id="yohr_s1_csv_parser",
        max_instances=1,
        replace_existing=True,
    )
    scheduler.add_job(
        func=_safe(run_downloader),
        trigger="interval", seconds=15,
        id="yohr_s2_downloader",
        max_instances=1,
        replace_existing=True,
    )
    scheduler.add_job(
        func=_safe(run_ai_processor),
        trigger="interval", seconds=20,
        id="yohr_s3_ai_processor",
        max_instances=1,
        replace_existing=True,
    )
    scheduler.add_job(
        func=_safe(run_ingestor),
        trigger="interval", seconds=15,
        id="yohr_s4_ingestor",
        max_instances=1,
        replace_existing=True,
    )
    logger.info("YOHR: all 4 pipeline jobs registered")


def _safe(fn):
    """Wrap job function so uncaught exceptions are logged but don't crash scheduler."""
    def wrapper():
        try:
            fn()
        except Exception as exc:
            logger.error("YOHR job %s raised: %s", fn.__name__, exc, exc_info=True)
    wrapper.__name__ = fn.__name__
    return wrapper