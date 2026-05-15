"""
app.py — Flask API + embedded APScheduler

Starts APScheduler in a background thread alongside Flask.
RQ worker runs as a separate process (started by entrypoint.sh).

Endpoints:
  GET  /health
  GET  /api/bulk/status?org_id=...
  GET  /api/bulk/failed-files?org_id=...&stage=parse|ai|ingest
  POST /api/bulk/retry  { org_id, stage, file_ids? }
  GET  /api/bulk/ai-result/<file_id>
  GET  /api/bulk/ingest-log?org_id=...&limit=...
"""

import logging
import os

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED
from flask import Flask, jsonify, request
from flask_cors import CORS

from config import supabase, bulk_queue, PORT

app    = Flask(__name__)
CORS(app, resources={r'/api/*': {'origins': '*'}})
logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════════════════════
# APScheduler — enqueues RQ jobs on fixed intervals
# ════════════════════════════════════════════════════════════════════════════

def _enqueue(task_name: str, timeout: int = 300):
    try:
        job = bulk_queue.enqueue(f'bulk_tasks.{task_name}', job_timeout=timeout)
        logger.debug(f'[Scheduler] Enqueued {task_name}: {job.id}')
    except Exception as e:
        logger.error(f'[Scheduler] Failed to enqueue {task_name}: {e}')


def _on_error(event):
    logger.error(f'[Scheduler] Job error: {event.job_id} — {event.exception}')

def _on_missed(event):
    logger.warning(f'[Scheduler] Job missed: {event.job_id}')


scheduler = BackgroundScheduler(timezone='UTC')
scheduler.add_listener(_on_error,  EVENT_JOB_ERROR)
scheduler.add_listener(_on_missed, EVENT_JOB_MISSED)

scheduler.add_job(lambda: _enqueue('parse_resume_batch',      300), 'interval', seconds=15,  id='parse',   max_instances=1, coalesce=True, misfire_grace_time=10)
scheduler.add_job(lambda: _enqueue('submit_ai_batch',         600), 'interval', minutes=5,   id='submit',  max_instances=1, coalesce=True, misfire_grace_time=60)
scheduler.add_job(lambda: _enqueue('poll_ai_batches',         600), 'interval', minutes=10,  id='poll',    max_instances=1, coalesce=True, misfire_grace_time=60)
scheduler.add_job(lambda: _enqueue('ingest_candidates_batch', 600), 'interval', seconds=30,  id='ingest',  max_instances=1, coalesce=True, misfire_grace_time=15)

scheduler.start()
logger.info('[Scheduler] Started: parse/15s, submit/5m, poll/10m, ingest/30s')


# ════════════════════════════════════════════════════════════════════════════
# Flask routes
# ════════════════════════════════════════════════════════════════════════════

@app.route('/health')
def health():
    try:
        # Quick Redis ping
        from config import redis_conn
        redis_conn.ping()
        redis_ok = True
    except Exception:
        redis_ok = False

    return jsonify({
        'status':    'ok',
        'service':   'hrumbles-bulk-worker',
        'redis':     redis_ok,
        'scheduler': scheduler.running,
    })


@app.route('/api/bulk/status')
def pipeline_status():
    org_id = request.args.get('org_id')
    if not org_id:
        return jsonify({'error': 'org_id required'}), 400

    result = supabase.rpc('get_bulk_pipeline_stats', {'p_org_id': org_id}).execute()
    return jsonify(result.data or {})


@app.route('/api/bulk/failed-files')
def failed_files():
    org_id = request.args.get('org_id')
    stage  = request.args.get('stage', 'parse')   # parse | ai | ingest
    limit  = min(int(request.args.get('limit', 100)), 500)

    if not org_id:
        return jsonify({'error': 'org_id required'}), 400

    q = supabase.table('hr_resume_files').eq('organization_id', org_id)

    if stage == 'parse':
        result = q.select('id, file_name, parse_status, parse_error, parse_attempts, mime_type, uploaded_at') \
                  .eq('parse_status', 'failed') \
                  .order('uploaded_at', desc=True).limit(limit).execute()

    elif stage == 'ai':
        result = q.select('id, file_name, ai_status, parse_status, uploaded_at') \
                  .eq('ai_status', 'failed') \
                  .order('uploaded_at', desc=True).limit(limit).execute()

    else:  # ingest
        result = q.select('id, file_name, ingest_status, ingest_result, ingest_error, uploaded_at') \
                  .eq('ingest_status', 'failed') \
                  .order('uploaded_at', desc=True).limit(limit).execute()

    data = result.data or []
    return jsonify({'files': data, 'count': len(data), 'stage': stage})


@app.route('/api/bulk/retry', methods=['POST'])
def retry_failed():
    body    = request.get_json(force=True) or {}
    org_id  = body.get('org_id')
    stage   = body.get('stage')
    file_ids = body.get('file_ids')   # optional list; if absent, retry all failed for that stage

    if not org_id or not stage:
        return jsonify({'error': 'org_id and stage required'}), 400

    q = supabase.table('hr_resume_files').eq('organization_id', org_id)
    if file_ids:
        q = q.in_('id', file_ids)

    if stage == 'parse':
        q.eq('parse_status', 'failed').update({
            'parse_status':   'pending',
            'parse_attempts': 0,
            'parse_error':    None,
        }).execute()

    elif stage == 'ai':
        q.eq('ai_status', 'failed').update({
            'ai_status':      'pending',
            'ai_batch_job_id': None,
        }).execute()

    elif stage == 'ingest':
        q.eq('ingest_status', 'failed').update({
            'ingest_status':   'pending',
            'ingest_attempts': 0,
            'ingest_error':    None,
        }).execute()

    else:
        return jsonify({'error': f'Unknown stage: {stage}'}), 400

    return jsonify({'message': f'Retrying {stage} failures', 'org_id': org_id})


@app.route('/api/bulk/ai-result/<file_id>')
def get_ai_result(file_id: str):
    """Return raw AI output for a file (for debugging)."""
    result = supabase.table('hr_resume_ai_results') \
        .select('*') \
        .eq('resume_file_id', file_id) \
        .maybe_single() \
        .execute()

    if not result.data:
        return jsonify({'error': 'Not found'}), 404

    return jsonify(result.data)


@app.route('/api/bulk/ingest-log')
def ingest_log():
    org_id = request.args.get('org_id')
    limit  = min(int(request.args.get('limit', 100)), 500)

    if not org_id:
        return jsonify({'error': 'org_id required'}), 400

    result = supabase.table('hr_resume_ingest_log') \
        .select('*') \
        .eq('organization_id', org_id) \
        .order('created_at', desc=True) \
        .limit(limit) \
        .execute()

    return jsonify({'logs': result.data or [], 'count': len(result.data or [])})


# ════════════════════════════════════════════════════════════════════════════
# Start
# ════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    logger.info(f'Starting bulk worker Flask API on port {PORT}')
    app.run(host='0.0.0.0', port=PORT, debug=False, use_reloader=False)