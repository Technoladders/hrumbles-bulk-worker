"""
mail_intake/worker.py — the APScheduler tick (every INTAKE_TICK_SECONDS,
max_instances=1, same pattern as the YOHR jobs).

Each tick, in order:
  1. approvals   ('approving' items → Supabase), few per tick, paced
  2. per mailbox, one at a time:
     a. live sync   history.list from our own cursor every ~5 min; the first
                    run only records the cursor (the past is the backfill's
                    job); 404 → search since the last successful sync
     b. backfill    list a couple of search pages into the queue
     c. queue       up to MESSAGES_PER_TICK messages, live first; messages of
                    a paused/cancelled run wait
     d. reprocess   "process anyway" items
"""

import logging
import time

from . import settings
from .crypto_utils import decrypt
from .gmail import GmailAuthError, GmailClient, HistoryGoneError, build_backfill_query
from .pipeline import approve_item, process_message, reprocess_item
from .store import all_rows, db, now, one

logger = logging.getLogger(__name__)


def queue_ids(mailbox_id: str, ids: list[str], run_id: str | None) -> int:
    t = now()
    c = db()
    added = 0
    c.execute('begin')
    try:
        for gid in ids:
            added += c.execute("""
                insert into messages (mailbox_id, gmail_id, run_id, state, created_at, updated_at)
                values (?, ?, ?, 'queued', ?, ?) on conflict do nothing""", (mailbox_id, gid, run_id, t, t)).rowcount
        c.execute('commit')
    except Exception:
        c.execute('rollback')
        raise
    return added


def _due(last_sync_at: str | None) -> bool:
    if not last_sync_at:
        return True
    from datetime import datetime
    try:
        last = datetime.fromisoformat(last_sync_at.replace('Z', '+00:00')).timestamp()
    except ValueError:
        return True
    return time.time() - last >= settings.LIVE_SYNC_EVERY_SECONDS


def live_sync(mbx: dict, gmail) -> None:
    if not _due(mbx.get('last_sync_at')):
        return
    if not mbx.get('history_id'):
        hid = gmail.get_profile()['historyId']
        db().execute('update mailboxes set history_id = ?, last_sync_at = ?, sync_error = null, updated_at = ? where id = ?',
                     (str(hid), now(), now(), mbx['id']))
        return
    try:
        token, latest = None, str(mbx['history_id'])
        while True:
            ids, hid, token = gmail.list_history(mbx['history_id'], token)
            queue_ids(mbx['id'], ids, None)
            if hid and int(hid) > int(latest):
                latest = str(hid)
            if not token:
                break
        db().execute("update mailboxes set history_id = ?, last_sync_at = ?, status = 'active', sync_error = null, updated_at = ? where id = ?",
                     (latest, now(), now(), mbx['id']))
    except HistoryGoneError:
        # Cursor too old (idle a week+): search everything since the last
        # successful sync, then restart the cursor from now.
        hid = gmail.get_profile()['historyId']
        q = build_backfill_query((mbx.get('last_sync_at') or now())[:10], None, mbx['email'])
        token = None
        while True:
            ids, token = gmail.list_messages(q, token)
            queue_ids(mbx['id'], ids, None)
            if not token:
                break
        db().execute('update mailboxes set history_id = ?, last_sync_at = ?, sync_error = null, updated_at = ? where id = ?',
                     (str(hid), now(), now(), mbx['id']))


def backfill_listing(mbx: dict, gmail) -> None:
    run = one("select * from backfill_runs where mailbox_id = ? and state = 'listing' order by created_at limit 1", (mbx['id'],))
    if not run:
        return
    q = build_backfill_query(run['after_date'], run['before_date'], mbx['email'])
    token = run.get('page_token')
    for _ in range(settings.LIST_PAGES_PER_TICK):
        ids, token = gmail.list_messages(q, token)
        added = queue_ids(mbx['id'], ids, run['id'])
        db().execute('update backfill_runs set listed = listed + ?, page_token = ?, state = ?, updated_at = ? where id = ?',
                     (added, token, 'listing' if token else 'processing', now(), run['id']))
        if not token:
            break


def process_queue(deps: dict, mbx: dict, gmail) -> None:
    rows = all_rows("""
        select m.gmail_id, m.attempts from messages m
          left join backfill_runs r on r.id = m.run_id
         where m.mailbox_id = ? and m.state = 'queued'
           and (m.run_id is null or r.state in ('listing', 'processing'))
         order by (m.run_id is null) desc, m.created_at
         limit ?""", (mbx['id'], settings.MESSAGES_PER_TICK))

    finish = 'update messages set state = ?, reason = ?, attempts = ?, updated_at = ? where mailbox_id = ? and gmail_id = ?'
    for row in rows:
        attempts = row['attempts'] + 1
        try:
            r = process_message({**deps, 'gmail': gmail}, mbx, row['gmail_id'])
            db().execute(finish, (r['state'], r['reason'], attempts, now(), mbx['id'], row['gmail_id']))
        except GmailAuthError:
            raise
        except Exception as e:
            state = 'failed' if attempts >= settings.MAX_ATTEMPTS else 'queued'
            db().execute(finish, (state, str(e)[:300], attempts, now(), mbx['id'], row['gmail_id']))
        time.sleep(settings.GMAIL_GAP_SECONDS)

    # A run whose listing finished and whose queue is empty is complete.
    db().execute("""
        update backfill_runs set state = 'completed', updated_at = ?
         where mailbox_id = ? and state = 'processing'
           and not exists (select 1 from messages m where m.run_id = backfill_runs.id and m.state = 'queued')""",
                 (now(), mbx['id']))


def reprocess_queue(deps: dict, mbx: dict, gmail) -> None:
    for item in all_rows("select * from items where mailbox_id = ? and status = 'reprocess' order by updated_at limit 10", (mbx['id'],)):
        try:
            reprocess_item({**deps, 'gmail': gmail}, item)
        except GmailAuthError:
            raise
        except Exception as e:
            db().execute("update items set status = 'pending', reason = ?, updated_at = ? where id = ?",
                         (f'Could not process: {str(e)[:200]}', now(), item['id']))


def process_approvals(deps: dict) -> None:
    clients: dict[str, object] = {}
    for item in all_rows("select * from items where status = 'approving' order by updated_at limit ?", (settings.APPROVALS_PER_TICK,)):
        try:
            if item['mailbox_id'] not in clients:
                mbx = one('select * from mailboxes where id = ?', (item['mailbox_id'],))
                clients[item['mailbox_id']] = deps['gmail_for'](mbx) if mbx else None
            approve_item({**deps, 'gmail': clients[item['mailbox_id']]}, item)
        except Exception as e:
            db().execute("update items set status = 'failed', result_message = ?, updated_at = ? where id = ?",
                         (str(e)[:300], now(), item['id']))
        time.sleep(settings.APPROVAL_GAP_SECONDS)


def default_gmail_for(mbx: dict) -> GmailClient:
    return GmailClient(decrypt(mbx['refresh_token_enc'], settings.ENCRYPTION_KEY_HEX))


def tick(deps: dict) -> None:
    """deps: supabase, extract_text, parse_resume, files, gmail_for (optional)."""
    deps = {'gmail_for': default_gmail_for, **deps}
    process_approvals(deps)

    for mbx in all_rows("select * from mailboxes where status = 'active' order by created_at"):
        try:
            gmail = deps['gmail_for'](mbx)
            live_sync(mbx, gmail)
            backfill_listing(mbx, gmail)
            process_queue(deps, mbx, gmail)
            reprocess_queue(deps, mbx, gmail)
        except GmailAuthError:
            db().execute("update mailboxes set status = 'error', sync_error = ?, updated_at = ? where id = ?",
                         ('Google access was revoked or expired — please reconnect the mailbox.', now(), mbx['id']))
        except Exception as e:
            logger.error(f"[mail_intake] mailbox {mbx['id']}: {e}")
            db().execute('update mailboxes set sync_error = ?, updated_at = ? where id = ?', (str(e)[:500], now(), mbx['id']))
