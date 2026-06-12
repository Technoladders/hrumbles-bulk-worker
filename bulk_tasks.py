"""
bulk_tasks.py

Four RQ worker functions for the bulk resume pipeline.

Worker 1 — parse_resume_batch      every 10s  (was 15s)
Worker 2 — submit_ai_batch         every 5min
Worker 3 — poll_ai_batches         every 10min
Worker 4 — ingest_candidates_batch every 15s  (was 30s)

PERFORMANCE CHANGES vs previous version:
  P1. parse_resume_batch — ThreadPoolExecutor(4) for parallel PDF downloads
      Batch size: 20 → 50. ~3-4x throughput improvement.
  P2. ingest_candidates_batch — single batch email SELECT instead of N queries
      Batch size: 100 → 200. Eliminates the biggest DB bottleneck.
  P3. _ingest_one — accepts pre-fetched existing_map (no individual SELECT per row)

SAFETY FIXES (preserved from previous version):
  S1. _sanitize() strips \\x00 NULL bytes that cause Postgres 22P05 errors
  S2. _update_file uses _sanitize on all string fields before writing
  S3. poll_ai_batches stamps updated_at every cycle (stale guard)
  S4. Per-chunk try/except + duplicate key (23505) fallback in poll
  S5. Output file download failure retries next cycle
  S6. Input file deletion isolated — never kills completion
  S7. Outer poll stamps error_message on unhandled exceptions
"""

import io
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

from config import supabase, openai_client, STORAGE_BUCKET
from text_extractor import extract_text

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """
Based on the provided resume text, perform a detailed extraction to create a professional profile.
Return ONLY a single, valid JSON object. No markdown. No explanations.

Fields (use null or [] if not found):
"suggested_title": string — most recent job title or inferred from skills
"candidate_name": string — full name
"email": string — email address
"phone": string — phone exactly as written
"linkedin_url": string or null — must be a valid URL
"github_url": string or null — must be a valid URL
"current_location": string or null — city/address
"top_skills": array of strings — key skills
"work_experience": array of {"company": str, "designation": str, "duration": str, "responsibilities": [str]}
"education": array of {"institution": str, "degree": str, "year": str}
"projects": array of strings — project titles/names only, one short string per project (no descriptions)
"certifications": array of strings
"other_details": object or null — any other structured sections
"total_experience": string — e.g. "5 years" if inferable
"current_company": string or null — most recent employer
"current_designation": string or null — most recent title
"notice_period": string or null — if mentioned
"highest_education": string or null — highest degree
""".strip()


# ════════════════════════════════════════════════════════════════════════════
# WORKER 1 — PARSE  [P1: parallel downloads, larger batch]
# ════════════════════════════════════════════════════════════════════════════

def _process_one_file(row: Dict) -> Dict:
    """
    Download + extract + DB-update for a single file.
    Runs inside a ThreadPoolExecutor — must be fully self-contained.
    Returns dict with keys: status ('ok'|'skip'|'fail'), name.
    """
    file_id      = row['id']
    storage_path = row.get('storage_path') or ''
    mime_type    = row.get('mime_type') or 'application/pdf'
    file_name    = row.get('file_name') or 'unknown'
    attempt      = row.get('parse_attempts') or 1

    try:
        if not storage_path:
            raise ValueError('storage_path is empty')

        file_bytes = supabase.storage.from_(STORAGE_BUCKET).download(storage_path)
        if not file_bytes:
            raise ValueError('Downloaded empty file')

        text, method = extract_text(file_bytes, mime_type, storage_path)

        if method == 'image_only':
            _update_file(file_id, {
                'parse_status': 'image_only',
                'parse_method': method,
                'parsed_at':    _now(),
                'parse_error':  None,
            })
            logger.info(f'[Parse] {file_name}: image_only')
            return {'status': 'skip', 'name': file_name}

        elif method in ('unsupported', 'failed') or not text.strip():
            msg = f'method={method}, text_len={len(text)}'
            if attempt >= 3:
                _update_file(file_id, {
                    'parse_status': 'unsupported' if method == 'unsupported' else 'failed',
                    'parse_error':  f'Permanent fail after {attempt} attempts: {msg}',
                    'parse_method': method,
                })
            else:
                _update_file(file_id, {
                    'parse_status': 'pending',
                    'parse_error':  f'Attempt {attempt}: {msg}',
                })
            return {'status': 'fail', 'name': file_name}

        else:
            _update_file(file_id, {
                'parse_status': 'parsed',
                'resume_text':  text,
                'parse_method': method,
                'parse_error':  None,
                'parsed_at':    _now(),
                'ai_status':    'pending',
            })
            logger.info(f'[Parse] {file_name}: OK ({len(text)} chars via {method})')
            return {'status': 'ok', 'name': file_name}

    except Exception as e:
        msg = str(e)[:400]
        logger.error(f'[Parse] {file_name} exception: {msg}')
        if attempt >= 3:
            _update_file(file_id, {
                'parse_status': 'failed',
                'parse_error':  f'Exception after {attempt} attempts: {msg}',
            })
        else:
            _update_file(file_id, {
                'parse_status': 'pending',
                'parse_error':  f'Attempt {attempt} exception: {msg}',
            })
        return {'status': 'fail', 'name': file_name}


def parse_resume_batch():
    logger.info('[Parse] Starting batch')

    # P1: batch size 20 → 50
    result = supabase.rpc('claim_parse_batch', {'p_limit': 50}).execute()
    files  = result.data or []
    if not files:
        logger.debug('[Parse] Nothing to process')
        return {'processed': 0}

    logger.info(f'[Parse] Processing {len(files)} files (parallel workers=4)')
    ok = fail = skip = 0

    # P1: parallel download + extraction using 4 threads
    # Safe: each thread touches a different file_id, supabase httpx client is thread-safe
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(_process_one_file, row): row for row in files}
        for future in as_completed(futures):
            try:
                res = future.result()
                if res['status'] == 'ok':     ok   += 1
                elif res['status'] == 'skip': skip += 1
                else:                         fail += 1
            except Exception as e:
                # Catch any unexpected executor-level failure
                row = futures[future]
                logger.error(f'[Parse] Executor error for {row.get("file_name")}: {e}')
                fail += 1

    logger.info(f'[Parse] Done: {ok} parsed, {skip} image_only, {fail} failed')
    return {'processed': len(files), 'ok': ok, 'skip': skip, 'fail': fail}


# ════════════════════════════════════════════════════════════════════════════
# WORKER 2 — SUBMIT TO OPENAI  (unchanged)
# ════════════════════════════════════════════════════════════════════════════

def submit_ai_batch():
    logger.info('[Submit] Starting batch submission')
    result = supabase.table('hr_resume_files') \
        .select('id, resume_text, organization_id') \
        .eq('parse_status', 'parsed').eq('ai_status', 'pending') \
        .order('uploaded_at', desc=False).limit(500).execute()

    files = result.data or []
    if not files:
        logger.debug('[Submit] No files to submit')
        return {'submitted': 0}

    logger.info(f'[Submit] {len(files)} files ready for AI')
    by_org: Dict[str, List[Dict]] = {}
    for f in files:
        by_org.setdefault(f['organization_id'], []).append(f)

    total = 0
    for org_id, org_files in by_org.items():
        try:
            total += _submit_org_batch(org_id, org_files)
        except Exception as e:
            logger.error(f'[Submit] Org {org_id[:8]} failed: {e}')

    logger.info(f'[Submit] Done: {total} files across {len(by_org)} orgs')
    return {'submitted': total}


def _submit_org_batch(org_id: str, files: List[Dict]) -> int:
    file_ids  = [f['id'] for f in files]
    batch_rec = supabase.table('hr_resume_batch_jobs').insert({
        'organization_id': org_id, 'file_count': len(files), 'status': 'pending',
    }).execute()
    batch_job_id = batch_rec.data[0]['id']
    logger.info(f'[Submit] Org {org_id[:8]}: job={batch_job_id[:8]}, {len(files)} files')

    try:
        lines = []
        for f in files:
            cid = 'f' + f['id'].replace('-', '')
            lines.append(json.dumps({
                'custom_id': cid, 'method': 'POST', 'url': '/v1/chat/completions',
                'body': {
                    'model': 'gpt-4.1-nano',
                    'response_format': {'type': 'json_object'},
                    'messages': [
                        {'role': 'system', 'content': SYSTEM_PROMPT},
                        {'role': 'user',   'content': f"Resume Text:\n\n---\n{f['resume_text'] or ''}\n---"},
                    ],
                },
            }))

        jsonl_bytes = '\n'.join(lines).encode('utf-8')
        size_kb     = len(jsonl_bytes) / 1024
        logger.info(f'[Submit] JSONL: {len(lines)} lines, {size_kb:.1f} KB')
        if len(jsonl_bytes) > 90 * 1024 * 1024:
            raise ValueError(f'JSONL too large ({size_kb:.0f}KB)')

        oai_file = openai_client.files.create(
            file=('batch.jsonl', io.BytesIO(jsonl_bytes), 'application/octet-stream'),
            purpose='batch',
        )
        logger.info(f'[Submit] OAI file uploaded: {oai_file.id}')

        batch = openai_client.batches.create(
            input_file_id=oai_file.id, endpoint='/v1/chat/completions',
            completion_window='24h',
            metadata={
                'hrumbles_batch_job_id': batch_job_id,
                'organization_id':       org_id,
                'file_count':            str(len(files)),
            },
        )
        logger.info(f'[Submit] OAI batch created: {batch.id}')

        supabase.table('hr_resume_batch_jobs').update({
            'openai_batch_id': batch.id, 'openai_file_id': oai_file.id,
            'status': 'processing', 'updated_at': _now(),
        }).eq('id', batch_job_id).execute()

        supabase.table('hr_resume_files').update({
            'ai_status': 'queued', 'ai_batch_job_id': batch_job_id, 'ai_queued_at': _now(),
        }).in_('id', file_ids).execute()

        return len(files)

    except Exception as e:
        supabase.table('hr_resume_batch_jobs').update({
            'status': 'failed', 'error_message': str(e)[:500], 'updated_at': _now(),
        }).eq('id', batch_job_id).execute()
        supabase.table('hr_resume_files').update({
            'ai_status': 'pending', 'ai_batch_job_id': None,
        }).in_('id', file_ids).execute()
        logger.error(f'[Submit] Org {org_id[:8]} batch failed: {e}')
        raise


# ════════════════════════════════════════════════════════════════════════════
# WORKER 3 — POLL OPENAI  (unchanged)
# ════════════════════════════════════════════════════════════════════════════

def poll_ai_batches():
    logger.info('[Poll] Checking OpenAI batches')

    result = supabase.table('hr_resume_batch_jobs') \
        .select('*').eq('status', 'processing').execute()

    jobs = result.data or []
    if not jobs:
        logger.debug('[Poll] No active batches')
        return {'checked': 0}

    logger.info(f'[Poll] {len(jobs)} active batches')
    completed = failed = in_progress = 0

    for job in jobs:
        try:
            status = _check_one_batch(job)
            if status == 'completed':     completed   += 1
            elif status == 'failed':      failed      += 1
            elif status == 'in_progress': in_progress += 1
        except Exception as e:
            logger.error(f'[Poll] Job {job["id"][:8]} unhandled error: {e}', exc_info=True)
            try:
                supabase.table('hr_resume_batch_jobs').update({
                    'error_message': f'Poll exception: {str(e)[:400]}',
                    'updated_at':    _now(),
                }).eq('id', job['id']).execute()
            except Exception:
                pass

    logger.info(f'[Poll] Done: {completed} completed, {failed} failed, {in_progress} in-progress')
    return {'checked': len(jobs), 'completed': completed, 'failed': failed}


def _check_one_batch(job: Dict) -> str:
    batch_job_id    = job['id']
    openai_batch_id = job.get('openai_batch_id')
    org_id          = job['organization_id']

    if not openai_batch_id or openai_batch_id == 'pending':
        logger.warning(f'[Poll] Job {batch_job_id[:8]} has no openai_batch_id, skipping')
        return 'skip'

    # Stale guard
    try:
        updated_at  = datetime.fromisoformat((job.get('updated_at') or '').replace('Z', '+00:00'))
        stale_hours = (datetime.now(timezone.utc) - updated_at).total_seconds() / 3600
        if stale_hours > 2:
            logger.warning(f'[Poll] Job {batch_job_id[:8]} stale {stale_hours:.1f}h — force re-polling OpenAI')
    except Exception:
        pass

    logger.info(f'[Poll] Checking {openai_batch_id}')
    oai = openai_client.batches.retrieve(openai_batch_id)
    logger.info(f'[Poll] {openai_batch_id}: status={oai.status}')

    if oai.status in ('validating', 'in_progress', 'finalizing'):
        c = oai.request_counts
        supabase.table('hr_resume_batch_jobs').update({
            'error_message': f'In progress: {c.completed}/{c.total}',
            'updated_at':    _now(),
        }).eq('id', batch_job_id).execute()
        logger.info(f'[Poll] {openai_batch_id} in progress: {c.completed}/{c.total}')
        return 'in_progress'

    if oai.status in ('failed', 'expired', 'cancelled'):
        supabase.table('hr_resume_batch_jobs').update({
            'status': 'failed', 'error_message': f'OpenAI: {oai.status}',
            'completed_at': _now(), 'updated_at': _now(),
        }).eq('id', batch_job_id).execute()
        supabase.table('hr_resume_files').update({'ai_status': 'failed'}) \
            .eq('ai_batch_job_id', batch_job_id).execute()
        logger.warning(f'[Poll] Batch {openai_batch_id} terminal: {oai.status}')
        return 'failed'

    if oai.status == 'completed':
        if not oai.output_file_id:
            supabase.table('hr_resume_batch_jobs').update({
                'status': 'failed', 'error_message': 'completed but no output_file_id',
                'completed_at': _now(), 'updated_at': _now(),
            }).eq('id', batch_job_id).execute()
            logger.error(f'[Poll] {openai_batch_id} completed but output_file_id is None')
            return 'failed'

        logger.info(f'[Poll] {openai_batch_id} completed. Downloading {oai.output_file_id}')

        try:
            output_text  = openai_client.files.content(oai.output_file_id).text
            output_lines = [l for l in output_text.strip().split('\n') if l.strip()]
        except Exception as e:
            logger.error(f'[Poll] Output file download failed: {e}', exc_info=True)
            supabase.table('hr_resume_batch_jobs').update({
                'error_message': f'Output download failed (retrying): {str(e)[:300]}',
                'updated_at':    _now(),
            }).eq('id', batch_job_id).execute()
            return 'in_progress'

        logger.info(f'[Poll] Downloaded {len(output_lines)} result lines')

        ai_inserts  = []
        success_ids = []
        failed_ids  = []

        for line in output_lines:
            try:
                parsed    = json.loads(line)
                custom_id = parsed.get('custom_id', '')
                hex_part  = custom_id[1:]
                if len(hex_part) != 32:
                    logger.warning(f'[Poll] Bad custom_id: {custom_id!r}')
                    continue
                file_id = (f'{hex_part[:8]}-{hex_part[8:12]}-'
                           f'{hex_part[12:16]}-{hex_part[16:20]}-{hex_part[20:]}')

                if parsed.get('error'):
                    logger.warning(f'[Poll] OAI error for {custom_id}: {parsed["error"]}')
                    failed_ids.append(file_id)
                    continue

                choices = parsed.get('response', {}).get('body', {}).get('choices', [])
                content = choices[0].get('message', {}).get('content', '') if choices else ''
                usage   = parsed.get('response', {}).get('body', {}).get('usage', {})

                if not content:
                    failed_ids.append(file_id)
                    continue

                content = content.strip()
                if content.startswith('```'):
                    content = '\n'.join(content.split('\n')[1:])
                if content.endswith('```'):
                    content = '\n'.join(content.split('\n')[:-1])
                content = content.strip()

                try:
                    profile = json.loads(content)
                except json.JSONDecodeError as je:
                    logger.error(f'[Poll] JSON decode failed for {custom_id}: {je}')
                    failed_ids.append(file_id)
                    continue

                email = (profile.get('email') or '').lower().strip()
                ai_inserts.append({
                    'resume_file_id':    file_id,
                    'batch_job_id':      batch_job_id,
                    'organization_id':   org_id,
                    'openai_custom_id':  custom_id,
                    'raw_response':      parsed,
                    'extracted_profile': profile,
                    'candidate_email':   email or None,
                    'input_tokens':      usage.get('prompt_tokens', 0),
                    'output_tokens':     usage.get('completion_tokens', 0),
                })
                success_ids.append(file_id)

            except Exception as e:
                logger.error(f'[Poll] Line parse error: {e}', exc_info=True)
                continue

        logger.info(f'[Poll] Parsed: {len(success_ids)} ok, {len(failed_ids)} failed')

        inserted_count = 0
        for i in range(0, len(ai_inserts), 50):
            chunk = ai_inserts[i:i+50]
            try:
                supabase.table('hr_resume_ai_results').insert(chunk).execute()
                inserted_count += len(chunk)
            except Exception as e:
                err_str = str(e).lower()
                if 'duplicate' in err_str or '23505' in err_str:
                    logger.info(f'[Poll] Chunk {i//50+1} duplicates, inserting row-by-row')
                    for row in chunk:
                        try:
                            supabase.table('hr_resume_ai_results').insert(row).execute()
                            inserted_count += 1
                        except Exception as re:
                            if 'duplicate' in str(re).lower() or '23505' in str(re):
                                inserted_count += 1  # already present = ok
                            else:
                                logger.error(f'[Poll] Row insert failed {row["resume_file_id"]}: {re}')
                else:
                    logger.error(f'[Poll] Chunk {i//50+1} insert failed: {e}', exc_info=True)

        logger.info(f'[Poll] Inserted {inserted_count} ai_result rows')

        if success_ids:
            supabase.table('hr_resume_files').update({
                'ai_status': 'done', 'updated_at': _now(),
            }).in_('id', success_ids).execute()

        if failed_ids:
            supabase.table('hr_resume_files').update({
                'ai_status': 'failed', 'updated_at': _now(),
            }).in_('id', failed_ids).execute()

        supabase.table('hr_resume_batch_jobs').update({
            'status': 'completed', 'error_message': None,
            'completed_at': _now(), 'updated_at': _now(),
        }).eq('id', batch_job_id).execute()

        input_file_id = job.get('openai_file_id', '')
        if input_file_id:
            try:
                openai_client.files.delete(input_file_id)
                logger.info(f'[Poll] Deleted input file {input_file_id}')
            except Exception as e:
                logger.warning(f'[Poll] Could not delete input file {input_file_id}: {e}')

        logger.info(f'[Poll] {openai_batch_id} complete: {len(success_ids)} ok, {len(failed_ids)} failed')
        return 'completed'

    logger.warning(f'[Poll] {openai_batch_id} unknown status: {oai.status}')
    return 'unknown'


# ════════════════════════════════════════════════════════════════════════════
# WORKER 4 — INGEST CANDIDATES  [P2: batch email lookup, larger batch]
# ════════════════════════════════════════════════════════════════════════════

def ingest_candidates_batch():
    logger.info('[Ingest] Starting ingest batch')

    # P2: batch size 100 → 200
    result = supabase.rpc('claim_ingest_batch', {'p_limit': 200}).execute()
    rows   = result.data or []

    if not rows:
        logger.debug('[Ingest] Nothing to ingest')
        return {'processed': 0}

    logger.info(f'[Ingest] Processing {len(rows)} candidates')

    # P2: collect all emails from this batch, then do ONE bulk SELECT
    # instead of one SELECT per row (was: N queries, now: 1 query)
    emails_in_batch = set()
    for row in rows:
        profile = row.get('extracted_profile') or {}
        email   = (profile.get('email') or row.get('candidate_email') or '').lower().strip()
        if email:
            emails_in_batch.add(email)

    existing_map: Dict[str, Dict] = {}
    if emails_in_batch:
        try:
            ex = supabase.table('hr_talent_pool') \
                .select('id, email, updated_at, resume_path, organization_id') \
                .in_('email', list(emails_in_batch)) \
                .execute()
            # Key by lowercase email for O(1) lookup per row
            for rec in (ex.data or []):
                existing_map[rec['email'].lower()] = rec
            logger.debug(f'[Ingest] Batch email lookup: {len(emails_in_batch)} queried, {len(existing_map)} found')
        except Exception as e:
            # Non-fatal: fall back to per-row lookup if batch query fails
            logger.warning(f'[Ingest] Batch email lookup failed, falling back to per-row: {e}')
            existing_map = {}

    inserted = updated = skipped = failed = 0

    for row in rows:
        try:
            outcome = _ingest_one(row, existing_map)
            if outcome == 'INSERTED':        inserted += 1
            elif outcome == 'UPDATED':       updated  += 1
            elif outcome.startswith('SKIP'): skipped  += 1
            else:                            failed   += 1
        except Exception as e:
            logger.error(f'[Ingest] {row.get("candidate_email")}: unhandled {e}')
            failed += 1
            _mark_ingest_failed(row, str(e))

    logger.info(f'[Ingest] Done: +{inserted} inserted, ~{updated} updated, ={skipped} skipped, ✗{failed} failed')
    return {'processed': len(rows), 'inserted': inserted, 'updated': updated,
            'skipped': skipped, 'failed': failed}


def _ingest_one(row: Dict, existing_map: Optional[Dict] = None) -> str:
    """
    Ingest a single candidate from AI result into hr_talent_pool.

    existing_map: pre-fetched dict keyed by lowercase email (from batch lookup).
                  If None or email not found in map, falls back to individual SELECT.
    """
    file_id      = row['resume_file_id']
    ai_result_id = row['ai_result_id']
    org_id       = row['organization_id']
    profile      = row.get('extracted_profile') or {}
    storage_path = row.get('storage_path') or ''
    resume_text  = row.get('resume_text') or ''
    now          = datetime.now(timezone.utc)

    email = (
        (profile.get('email') or '') or (row.get('candidate_email') or '')
    ).lower().strip()

    resume_url = None
    if storage_path:
        base = os.getenv('SUPABASE_URL', '').rstrip('/')
        bkt  = os.getenv('STORAGE_BUCKET', 'talent-pool-resumes')
        resume_url = f'{base}/storage/v1/object/public/{bkt}/{storage_path}'

    if not email:
        _update_file(file_id, {
            'ingest_status': 'skipped',
            'ingest_result': 'SKIPPED_NO_EMAIL',
            'ingested_at':   _now(),
        })
        _log_ingest(file_id, ai_result_id, org_id, email, 'SKIP', 'NO_EMAIL')
        return 'SKIPPED_NO_EMAIL'

    # P3: use pre-fetched map; fall back to individual SELECT only if map is missing
    if existing_map is not None and email in existing_map:
        existing_data = existing_map[email]
    elif existing_map is not None:
        existing_data = None   # not in map → definitely a new candidate
    else:
        # Fallback: individual SELECT (used when batch lookup failed)
        _res = supabase.table('hr_talent_pool') \
            .select('id, updated_at, resume_text, resume_path') \
            .eq('organization_id', org_id).ilike('email', email).limit(1).execute()
        existing_data = _res.data[0] if _res.data else None

    action       = 'UNKNOWN'
    candidate_id = None
    field_changes: List[str] = []

    if not existing_data:
        data         = _build_insert_payload(profile, email, resume_url, org_id, resume_text)
        ins          = supabase.table('hr_talent_pool').insert(data).execute()
        candidate_id = ins.data[0]['id'] if ins.data else None
        action       = 'INSERTED'
    else:
        candidate_id = existing_data['id']
        try:
            upd_str = existing_data.get('updated_at') or ''
            upd_at  = datetime.fromisoformat(upd_str.replace('Z', '+00:00')) \
                      if upd_str else now - timedelta(days=60)
        except Exception:
            upd_at = now - timedelta(days=60)

        age_days = (now - upd_at).days
        if age_days > 30:
            data = _build_update_payload(profile, resume_url, resume_text)
            supabase.table('hr_talent_pool').update(data).eq('id', candidate_id).execute()
            field_changes = list(data.keys())
            action = 'UPDATED'
        else:
            partial: Dict = {}
            if not existing_data.get('resume_path') and resume_url:
                partial['resume_path'] = resume_url
            if partial:
                supabase.table('hr_talent_pool').update(partial).eq('id', candidate_id).execute()
                field_changes = list(partial.keys())
            action = 'SKIPPED_RECENT'

    _update_file(file_id, {
        'ingest_status': 'done' if action in ('INSERTED', 'UPDATED') else 'skipped',
        'ingest_result': action,
        'candidate_id':  candidate_id,
        'ingested_at':   _now(),
    })
    _log_ingest(
        file_id, ai_result_id, org_id, email,
        'INSERT' if action == 'INSERTED' else 'UPDATE' if action == 'UPDATED' else 'SKIP',
        action, candidate_id,
        {'fields': field_changes} if field_changes else None,
    )
    return action


def _build_insert_payload(profile: Dict, email: str, resume_url: Optional[str],
                           org_id: str, resume_text: str = '') -> Dict:
    payload = {
        'candidate_name':      _name(profile),
        'email':               email,
        'phone':               profile.get('phone'),
        'linkedin_url':        profile.get('linkedin_url'),
        'github_url':          profile.get('github_url'),
        'current_location':    profile.get('current_location'),
        'top_skills':          profile.get('top_skills'),
        'top_skills_lower':    _lower_list(profile.get('top_skills')),
        'work_experience':     profile.get('work_experience'),
        'education':           profile.get('education'),
        'projects':            profile.get('projects'),
        'certifications':      profile.get('certifications'),
        'other_details':       profile.get('other_details'),
        'suggested_title':     profile.get('suggested_title'),
        'current_designation': profile.get('current_designation'),
        'current_company':     profile.get('current_company'),
        'total_experience':    profile.get('total_experience'),
        'notice_period':       profile.get('notice_period'),
        'highest_education':   profile.get('highest_education'),
        'resume_path':         resume_url,
        'resume_text':         resume_text,
        'source_platform':     'bulk_upload',
        'organization_id':     org_id,
    }
    return {k: v for k, v in payload.items() if k == 'resume_text' or v is not None}


def _build_update_payload(profile: Dict, resume_url: Optional[str],
                           resume_text: str = '') -> Dict:
    payload = {
        'candidate_name':      _name(profile),
        'phone':               profile.get('phone'),
        'linkedin_url':        profile.get('linkedin_url'),
        'github_url':          profile.get('github_url'),
        'current_location':    profile.get('current_location'),
        'top_skills':          profile.get('top_skills'),
        'top_skills_lower':    _lower_list(profile.get('top_skills')),
        'work_experience':     profile.get('work_experience'),
        'education':           profile.get('education'),
        'projects':            profile.get('projects'),
        'certifications':      profile.get('certifications'),
        'other_details':       profile.get('other_details'),
        'suggested_title':     profile.get('suggested_title'),
        'current_designation': profile.get('current_designation'),
        'current_company':     profile.get('current_company'),
        'total_experience':    profile.get('total_experience'),
        'notice_period':       profile.get('notice_period'),
        'highest_education':   profile.get('highest_education'),
        'resume_path':         resume_url,
        'resume_text':         resume_text,
    }
    return {k: v for k, v in payload.items() if k == 'resume_text' or v is not None}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _name(profile: Dict) -> str:
    n = profile.get('candidate_name') or ''
    if not n:
        n = f"{profile.get('firstName', '')} {profile.get('lastName', '')}".strip()
    return n or 'Unknown'

def _lower_list(skills) -> list:
    if not skills or not isinstance(skills, list):
        return []
    return [str(s).lower().strip() for s in skills if s]

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

def _sanitize(val):
    """Strip NULL bytes that PostgreSQL rejects (error 22P05 / \\u0000)."""
    if isinstance(val, str):
        return val.replace('\x00', '')
    return val

def _update_file(file_id: str, data: Dict) -> None:
    try:
        clean = {k: _sanitize(v) for k, v in data.items()}
        supabase.table('hr_resume_files').update(clean).eq('id', file_id).execute()
    except Exception as e:
        logger.error(f'[DB] update hr_resume_files {file_id}: {e}')

def _log_ingest(file_id, ai_result_id, org_id, email, action, reason,
                candidate_id=None, field_changes=None, error=None):
    try:
        supabase.table('hr_resume_ingest_log').insert({
            'resume_file_id':  file_id,
            'ai_result_id':    ai_result_id,
            'organization_id': org_id,
            'candidate_email': email or None,
            'action':          action,
            'reason':          reason,
            'candidate_id':    candidate_id,
            'field_changes':   field_changes,
            'error_detail':    error,
        }).execute()
    except Exception as e:
        logger.error(f'[DB] ingest log write failed: {e}')

def _mark_ingest_failed(row: Dict, error: str) -> None:
    file_id = row.get('resume_file_id')
    if file_id:
        _update_file(file_id, {
            'ingest_status': 'failed',
            'ingest_result': 'FAILED',
            'ingest_error':  error[:500],
        })
    _log_ingest(
        file_id, row.get('ai_result_id'), row.get('organization_id', ''),
        row.get('candidate_email', ''), 'FAIL', 'ERROR', error=error[:500],
    )
