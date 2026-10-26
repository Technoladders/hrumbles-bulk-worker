"""
mail_intake — Gmail Resume Intake.

Connect an office Gmail inbox, find resume attachments in past and new mail,
hold them for review, and add approved ones to hr_talent_pool. Self-contained:
own blueprint, own scheduler job, own SQLite store; nothing in the bulk or
YOHR pipelines is touched.

Usage in app.py (same pattern as register_yohr_jobs):
    from mail_intake import register_mail_intake
    register_mail_intake(app, scheduler)
"""

import logging

logger = logging.getLogger(__name__)


def register_mail_intake(app, scheduler) -> None:
    from . import settings

    if not settings.ENABLED:
        logger.info('[mail_intake] disabled (MAIL_INTAKE_ENABLED=false)')
        return

    from .routes import bp
    app.register_blueprint(bp)

    scheduler.add_job(
        func=_tick,
        trigger='interval', seconds=settings.TICK_SECONDS,
        id='mail_intake_tick',
        max_instances=1,
        coalesce=True,
        misfire_grace_time=settings.TICK_SECONDS,
        replace_existing=True,
    )
    logger.info(f'[mail_intake] registered: /api/mail-intake, tick every {settings.TICK_SECONDS}s, data {settings.DATA_DIR}')


_deps = None


def _tick() -> None:
    global _deps
    try:
        if _deps is None:
            from config import supabase
            from .parse import extract_text, parse_resume
            from .pipeline import FileStore
            _deps = {'supabase': supabase, 'extract_text': extract_text, 'parse_resume': parse_resume, 'files': FileStore()}
        from .worker import tick
        tick(_deps)
    except Exception as e:  # never let one bad tick kill the scheduler job
        logger.error(f'[mail_intake] tick failed: {e}', exc_info=True)
