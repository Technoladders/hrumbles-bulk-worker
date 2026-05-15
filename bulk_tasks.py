"""
bulk_tasks.py

Four RQ worker functions for the bulk resume pipeline.
Each is enqueued by APScheduler (via app.py) on its own interval.

Worker 1 — parse_resume_batch      every 15s  — extract text from uploaded files
Worker 2 — submit_ai_batch         every 5min — group parsed files → OpenAI Batch API
Worker 3 — poll_ai_batches         every 10min — check batch status → save AI results
Worker 4 — ingest_candidates_batch every 30s  — upsert candidates into hr_talent_pool
"""

import io
import json
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple

from config import supabase, openai_client, STORAGE_BUCKET
from text_extractor import extract_text

logger = logging.getLogger(__name__)

# ── OpenAI Batch API system prompt ────────────────────────────────────────────
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
"professional_summary": array of strings — bullet points
"top_skills": array of strings — key skills
"work_experience": array of {"company": str, "designation": str, "duration": str, "responsibilities": [str]}
"education": array of {"institution": str, "degree": str, "year": str}
"projects": array of strings — copy project blocks fully, do not summarize
"certifications": array of strings
"other_details": object or null — any other structured sections
"total_experience": string — e.g. "5 years" if inferable
"current_company": string or null — most recent employer
"current_designation": string or null — most recent title
"notice_period": string or null — if mentioned
"highest_education": string or null — highest degree
""".strip()


# ════════════════════════════════════════════════════════════════════════════
# WORKER 1 — PARSE
# ════════════════════════════════════════════════════════════════════════════

def parse_resume_batch():
    """
    Pick up 20 pending files, extract text, update parse_status.
    FOR UPDATE SKIP LOCKED is handled inside claim_parse_batch RPC.
    """
    logger.info('[Parse] Starting batch')

    result = supabase.rpc('claim_parse_batch', {'p_limit': 20}).execute()
    files = result.data or []

    if not files:
        logger.debug('[Parse] Nothing to process')
        return {'processed': 0}

    logger.info(f'[Parse] Processing {len(files)} files')
    ok = fail = skip = 0

    for row in files:
        file_id      = row['id']
        storage_path = row.get('storage_path') or ''
        mime_type    = row.get('mime_type') or 'application/pdf'
        file_name    = row.get('file_name') or 'unknown'
        attempt      = row.get('parse_attempts') or 1

        try:
            if not storage_path:
                raise ValueError('storage_path is empty')

            # Download raw bytes from Supabase Storage
            file_bytes = supabase.storage.from_(STORAGE_BUCKET).download(storage_path)
            if not file_bytes:
                raise ValueError('Downloaded empty file')

            # Extract text
            text, method = extract_text(file_bytes, mime_type, storage_path)

            # ── Handle result ──────────────────────────────────────────────
            if method == 'image_only':
                _update_file(file_id, {
                    'parse_status': 'image_only',
                    'parse_method': method,
                    'parsed_at':    _now(),
                    'parse_error':  None,
                })
                logger.info(f'[Parse] {file_name}: image_only')
                skip += 1

            elif method in ('unsupported', 'failed') or not text.strip():
                msg = f'method={method}, text_len={len(text)}'
                if attempt >= 3:
                    _update_file(file_id, {
                        'parse_status': 'unsupported' if method == 'unsupported' else 'failed',
                        'parse_error': f'Permanent fail after {attempt} attempts: {msg}',
                        'parse_method': method,
                    })
                    logger.warning(f'[Parse] {file_name}: permanent fail ({msg})')
                else:
                    _update_file(file_id, {
                        'parse_status': 'pending',
                        'parse_error': f'Attempt {attempt}: {msg}',
                    })
                    logger.info(f'[Parse] {file_name}: will retry ({msg})')
                fail += 1

            else:
                _update_file(file_id, {
                    'parse_status': 'parsed',
                    'resume_text':  text,
                    'parse_method': method,
                    'parse_error':  None,
                    'parsed_at':    _now(),
                    'ai_status':    'pending',   # ensure AI queue is reset
                })
                logger.info(f'[Parse] {file_name}: OK ({len(text)} chars via {method})')
                ok += 1

        except Exception as e:
            msg = str(e)[:400]
            logger.error(f'[Parse] {file_name} exception: {msg}')
            if attempt >= 3:
                _update_file(file_id, {
                    'parse_status': 'failed',
                    'parse_error': f'Exception after {attempt} attempts: {msg}',
                })
            else:
                _update_file(file_id, {
                    'parse_status': 'pending',
                    'parse_error': f'Attempt {attempt} exception: {msg}',
                })
            fail += 1

    logger.info(f'[Parse] Done: {ok} parsed, {skip} image_only, {fail} failed')
    return {'processed': len(files), 'ok': ok, 'skip': skip, 'fail': fail}


# ════════════════════════════════════════════════════════════════════════════
# WORKER 2 — SUBMIT TO OPENAI
# ════════════════════════════════════════════════════════════════════════════

def submit_ai_batch():
    """
    Group up to 500 parsed files → build JSONL → upload to OpenAI Batch API.
    Groups by organization — one OpenAI batch per org per run.
    """
    logger.info('[Submit] Starting batch submission')

    result = supabase.table('hr_resume_files') \
        .select('id, resume_text, organization_id') \
        .eq('parse_status', 'parsed') \
        .eq('ai_status', 'pending') \
        .order('uploaded_at', desc=False) \
        .limit(500) \
        .execute()

    files = result.data or []
    if not files:
        logger.debug('[Submit] No files to submit')
        return {'submitted': 0}

    logger.info(f'[Submit] {len(files)} files ready for AI')

    # Group by org
    by_org: Dict[str, List[Dict]] = {}
    for f in files:
        by_org.setdefault(f['organization_id'], []).append(f)

    total = 0
    for org_id, org_files in by_org.items():
        try:
            n = _submit_org_batch(org_id, org_files)
            total += n
        except Exception as e:
            logger.error(f'[Submit] Org {org_id[:8]} failed: {e}')

    logger.info(f'[Submit] Done: {total} files submitted across {len(by_org)} orgs')
    return {'submitted': total}


def _submit_org_batch(org_id: str, files: List[Dict]) -> int:
    file_ids = [f['id'] for f in files]

    # Create batch job record
    batch_rec = supabase.table('hr_resume_batch_jobs').insert({
        'organization_id': org_id,
        'file_count':      len(files),
        'status':          'pending',
    }).execute()
    batch_job_id = batch_rec.data[0]['id']
    logger.info(f'[Submit] Org {org_id[:8]}: job={batch_job_id[:8]}, {len(files)} files')

    try:
        # Build JSONL — custom_id encodes the file UUID (no dashes) for safe round-trip
        lines = []
        for f in files:
            cid = 'f' + f['id'].replace('-', '')   # 33 chars, unique, no special chars
            lines.append(json.dumps({
                'custom_id': cid,
                'method':    'POST',
                'url':       '/v1/chat/completions',
                'body': {
                    'model':           'gpt-4.1-nano',
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

        if len(jsonl_bytes) > 90 * 1024 * 1024:  # 90MB safety check
            raise ValueError(f'JSONL too large ({size_kb:.0f}KB) — reduce batch size')

        # Upload JSONL to OpenAI Files API
        oai_file = openai_client.files.create(
            file=('batch.jsonl', io.BytesIO(jsonl_bytes), 'application/octet-stream'),
            purpose='batch',
        )
        logger.info(f'[Submit] OAI file uploaded: {oai_file.id}')

        # Create OpenAI batch
        batch = openai_client.batches.create(
            input_file_id    =oai_file.id,
            endpoint         ='/v1/chat/completions',
            completion_window='24h',
            metadata={
                'hrumbles_batch_job_id': batch_job_id,
                'organization_id':       org_id,
                'file_count':            str(len(files)),
            },
        )
        logger.info(f'[Submit] OAI batch created: {batch.id}')

        # Persist to DB
        supabase.table('hr_resume_batch_jobs').update({
            'openai_batch_id': batch.id,
            'openai_file_id':  oai_file.id,
            'status':          'processing',
        }).eq('id', batch_job_id).execute()

        supabase.table('hr_resume_files').update({
            'ai_status':      'queued',
            'ai_batch_job_id': batch_job_id,
            'ai_queued_at':    _now(),
        }).in_('id', file_ids).execute()

        return len(files)

    except Exception as e:
        # Roll back: mark batch failed, reset files to pending
        supabase.table('hr_resume_batch_jobs').update({
            'status':         'failed',
            'error_message':  str(e)[:500],
        }).eq('id', batch_job_id).execute()

        supabase.table('hr_resume_files').update({
            'ai_status':       'pending',
            'ai_batch_job_id': None,
        }).in_('id', file_ids).execute()

        logger.error(f'[Submit] Org {org_id[:8]} batch failed: {e}')
        raise


# ════════════════════════════════════════════════════════════════════════════
# WORKER 3 — POLL OPENAI
# ════════════════════════════════════════════════════════════════════════════

def poll_ai_batches():
    """
    Check all 'processing' batch jobs against OpenAI.
    Download and store results when completed.
    """
    logger.info('[Poll] Checking OpenAI batches')

    result = supabase.table('hr_resume_batch_jobs') \
        .select('*') \
        .eq('status', 'processing') \
        .execute()

    jobs = result.data or []
    if not jobs:
        logger.debug('[Poll] No active batches')
        return {'checked': 0}

    logger.info(f'[Poll] {len(jobs)} active batches')
    completed = failed = in_progress = 0

    for job in jobs:
        try:
            status = _check_one_batch(job)
            if status == 'completed':  completed  += 1
            elif status == 'failed':   failed     += 1
            else:                      in_progress += 1
        except Exception as e:
            logger.error(f'[Poll] Job {job["id"][:8]} error: {e}')

    logger.info(f'[Poll] Done: {completed} completed, {failed} failed, {in_progress} in-progress')
    return {'checked': len(jobs), 'completed': completed, 'failed': failed}


def _check_one_batch(job: Dict) -> str:
    batch_job_id    = job['id']
    openai_batch_id = job.get('openai_batch_id')
    org_id          = job['organization_id']

    if not openai_batch_id or openai_batch_id == 'pending':
        return 'skip'

    oai = openai_client.batches.retrieve(openai_batch_id)
    logger.info(f'[Poll] {openai_batch_id}: status={oai.status}')

    # Still running
    if oai.status in ('validating', 'in_progress', 'finalizing'):
        c = oai.request_counts
        supabase.table('hr_resume_batch_jobs').update({
            'error_message': f'In progress: {c.completed}/{c.total}',
        }).eq('id', batch_job_id).execute()
        return 'in_progress'

    # Terminal failure
    if oai.status in ('failed', 'expired', 'cancelled'):
        supabase.table('hr_resume_batch_jobs').update({
            'status':        'failed',
            'error_message': f'OpenAI: {oai.status}',
            'completed_at':  _now(),
        }).eq('id', batch_job_id).execute()
        supabase.table('hr_resume_files').update({
            'ai_status': 'failed',
        }).eq('ai_batch_job_id', batch_job_id).execute()
        logger.warning(f'[Poll] Batch {openai_batch_id} terminal: {oai.status}')
        return 'failed'

    # Completed — process output
    if oai.status == 'completed':
        if not oai.output_file_id:
            supabase.table('hr_resume_batch_jobs').update({
                'status':       'failed',
                'error_message': 'No output_file_id',
                'completed_at':  _now(),
            }).eq('id', batch_job_id).execute()
            return 'failed'

        # Download output JSONL
        output_text  = openai_client.files.content(oai.output_file_id).text
        output_lines = [l for l in output_text.strip().split('\n') if l.strip()]
        logger.info(f'[Poll] Downloaded {len(output_lines)} result lines')

        ai_inserts      = []
        success_ids     = []
        failed_ids      = []

        for line in output_lines:
            try:
                parsed    = json.loads(line)
                custom_id = parsed.get('custom_id', '')

                # Reconstruct file UUID: custom_id = 'f' + uuid_no_dashes (32 hex chars)
                hex_part = custom_id[1:]  # strip leading 'f'
                if len(hex_part) != 32:
                    logger.warning(f'[Poll] Bad custom_id: {custom_id}')
                    continue
                file_id = (
                    f'{hex_part[:8]}-{hex_part[8:12]}-'
                    f'{hex_part[12:16]}-{hex_part[16:20]}-{hex_part[20:]}'
                )

                if parsed.get('error'):
                    logger.warning(f'[Poll] OAI error for {custom_id}: {parsed["error"]}')
                    failed_ids.append(file_id)
                    continue

                # Extract profile JSON from response
                choices = parsed.get('response', {}).get('body', {}).get('choices', [])
                content = choices[0].get('message', {}).get('content', '') if choices else ''
                usage   = parsed.get('response', {}).get('body', {}).get('usage', {})

                if not content:
                    failed_ids.append(file_id)
                    continue

                # Clean up any markdown fences
                content = content.strip().lstrip('```json').lstrip('```').rstrip('```').strip()

                try:
                    profile = json.loads(content)
                except json.JSONDecodeError:
                    logger.error(f'[Poll] Bad JSON for {custom_id}')
                    failed_ids.append(file_id)
                    continue

                email = (profile.get('email') or '').lower().strip()

                ai_inserts.append({
                    'resume_file_id':   file_id,
                    'batch_job_id':     batch_job_id,
                    'organization_id':  org_id,
                    'openai_custom_id': custom_id,
                    'raw_response':     parsed,
                    'extracted_profile': profile,
                    'candidate_email':  email or None,
                    'input_tokens':     usage.get('prompt_tokens', 0),
                    'output_tokens':    usage.get('completion_tokens', 0),
                })
                success_ids.append(file_id)

            except Exception as e:
                logger.error(f'[Poll] Line parse error: {e}')
                continue

        # Bulk insert AI results (chunks of 100 to stay under request limits)
        for i in range(0, len(ai_inserts), 100):
            chunk = ai_inserts[i:i+100]
            if chunk:
                supabase.table('hr_resume_ai_results').insert(chunk).execute()

        if success_ids:
            supabase.table('hr_resume_files').update({'ai_status': 'done'}) \
                .in_('id', success_ids).execute()
        if failed_ids:
            supabase.table('hr_resume_files').update({'ai_status': 'failed'}) \
                .in_('id', failed_ids).execute()

        # Mark batch job done
        supabase.table('hr_resume_batch_jobs').update({
            'status':       'completed',
            'completed_at': _now(),
        }).eq('id', batch_job_id).execute()

        # Clean up OpenAI input file (saves OpenAI storage cost)
        try:
            openai_client.files.delete(job.get('openai_file_id', ''))
        except Exception:
            pass

        logger.info(f'[Poll] {openai_batch_id} complete: {len(success_ids)} ok, {len(failed_ids)} failed')
        return 'completed'

    return 'unknown'


# ════════════════════════════════════════════════════════════════════════════
# WORKER 4 — INGEST CANDIDATES
# ════════════════════════════════════════════════════════════════════════════

def ingest_candidates_batch():
    """
    Read AI results, upsert into hr_talent_pool.
    Each candidate is independent — one failure never blocks others.
    No RPC, no transactions, no timeouts.
    """
    logger.info('[Ingest] Starting ingest batch')

    result = supabase.rpc('claim_ingest_batch', {'p_limit': 100}).execute()
    rows   = result.data or []

    if not rows:
        logger.debug('[Ingest] Nothing to ingest')
        return {'processed': 0}

    logger.info(f'[Ingest] Processing {len(rows)} candidates')
    inserted = updated = skipped = failed = 0

    for row in rows:
        try:
            outcome = _ingest_one(row)
            if outcome == 'INSERTED':      inserted += 1
            elif outcome == 'UPDATED':     updated  += 1
            elif outcome.startswith('SKIP'): skipped += 1
            else:                          failed   += 1
        except Exception as e:
            logger.error(f'[Ingest] {row.get("candidate_email")}: unhandled {e}')
            failed += 1
            _mark_ingest_failed(row, str(e))

    logger.info(f'[Ingest] Done: +{inserted} inserted, ~{updated} updated, ={skipped} skipped, ✗{failed} failed')
    return {'processed': len(rows), 'inserted': inserted, 'updated': updated, 'skipped': skipped, 'failed': failed}


def _ingest_one(row: Dict) -> str:
    """Upsert one candidate. Returns outcome string."""
    file_id      = row['resume_file_id']
    ai_result_id = row['ai_result_id']
    org_id       = row['organization_id']
    profile      = row.get('extracted_profile') or {}
    storage_path = row.get('storage_path') or ''
    now          = datetime.now(timezone.utc)

    email = (
        (profile.get('email') or '') or
        (row.get('candidate_email') or '')
    ).lower().strip()

    # Build public resume URL
    resume_url = None
    if storage_path:
        base = os.getenv('SUPABASE_URL', '').rstrip('/')
        bkt  = os.getenv('STORAGE_BUCKET', 'talent-pool-resumes')
        resume_url = f'{base}/storage/v1/object/public/{bkt}/{storage_path}'

    # ── No email → skip ───────────────────────────────────────────────────────
    if not email:
        _update_file(file_id, {
            'ingest_status': 'skipped',
            'ingest_result': 'SKIPPED_NO_EMAIL',
            'ingested_at':   _now(),
        })
        _log_ingest(file_id, ai_result_id, org_id, email, 'SKIP', 'NO_EMAIL')
        return 'SKIPPED_NO_EMAIL'

    # ── Lookup existing candidate ─────────────────────────────────────────────
    existing = supabase.table('hr_talent_pool') \
        .select('id, updated_at, resume_text, resume_path') \
        .eq('organization_id', org_id) \
        .ilike('email', email) \
        .maybe_single() \
        .execute()

    action       = 'UNKNOWN'
    candidate_id = None
    field_changes = []

    if not existing.data:
        # ── INSERT new candidate ──────────────────────────────────────────────
        data = _build_insert_payload(profile, email, resume_url, org_id)
        ins  = supabase.table('hr_talent_pool').insert(data).execute()
        candidate_id = ins.data[0]['id'] if ins.data else None
        action       = 'INSERTED'

    else:
        existing_data = existing.data
        candidate_id  = existing_data['id']

        # Parse updated_at
        try:
            upd_str = existing_data.get('updated_at') or ''
            upd_at  = datetime.fromisoformat(upd_str.replace('Z', '+00:00')) \
                      if upd_str else now - timedelta(days=60)
        except Exception:
            upd_at = now - timedelta(days=60)

        age_days = (now - upd_at).days

        if age_days > 30:
            # ── Full update ───────────────────────────────────────────────────
            data = _build_update_payload(profile, resume_url)
            supabase.table('hr_talent_pool').update(data).eq('id', candidate_id).execute()
            field_changes = list(data.keys())
            action        = 'UPDATED'
        else:
            # ── Partial update — only fill empty resume fields ────────────────
            partial: Dict = {}
            if not existing_data.get('resume_path') and resume_url:
                partial['resume_path'] = resume_url
            if partial:
                supabase.table('hr_talent_pool').update(partial).eq('id', candidate_id).execute()
                field_changes = list(partial.keys())
            action = 'SKIPPED_RECENT'

    # ── Update file record ────────────────────────────────────────────────────
    _update_file(file_id, {
        'ingest_status': 'done' if action in ('INSERTED', 'UPDATED') else 'skipped',
        'ingest_result': action,
        'candidate_id':  candidate_id,
        'ingested_at':   _now(),
    })

    # ── Audit log ────────────────────────────────────────────────────────────
    _log_ingest(
        file_id, ai_result_id, org_id, email,
        'INSERT'  if action == 'INSERTED' else
        'UPDATE'  if action == 'UPDATED'  else 'SKIP',
        action,
        candidate_id,
        {'fields': field_changes} if field_changes else None,
    )

    return action


def _build_insert_payload(profile: Dict, email: str, resume_url: str, org_id: str) -> Dict:
    name = _name(profile)
    payload = {
        'candidate_name':      name,
        'email':               email,
        'phone':               profile.get('phone'),
        'linkedin_url':        profile.get('linkedin_url'),
        'github_url':          profile.get('github_url'),
        'current_location':    profile.get('current_location'),
        'professional_summary': profile.get('professional_summary'),
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
        'source_platform':     'bulk_upload',
        'organization_id':     org_id,
    }
    # Strip None values (let DB defaults apply, avoid overwriting with NULL)
    return {k: v for k, v in payload.items() if v is not None}


def _build_update_payload(profile: Dict, resume_url: str) -> Dict:
    name = _name(profile)
    payload = {
        'candidate_name':      name,
        'phone':               profile.get('phone'),
        'linkedin_url':        profile.get('linkedin_url'),
        'github_url':          profile.get('github_url'),
        'current_location':    profile.get('current_location'),
        'professional_summary': profile.get('professional_summary'),
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
    }
    return {k: v for k, v in payload.items() if v is not None}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _name(profile: Dict) -> str:
    n = profile.get('candidate_name') or ''
    if not n:
        n = f"{profile.get('firstName','')} {profile.get('lastName','')}".strip()
    return n or 'Unknown'


def _lower_list(skills) -> list:
    if not skills or not isinstance(skills, list):
        return []
    return [str(s).lower().strip() for s in skills if s]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _update_file(file_id: str, data: Dict) -> None:
    """Update hr_resume_files row. Logs but never raises."""
    try:
        supabase.table('hr_resume_files').update(data).eq('id', file_id).execute()
    except Exception as e:
        logger.error(f'[DB] update hr_resume_files {file_id}: {e}')


def _log_ingest(
    file_id: str, ai_result_id: str, org_id: str, email: str,
    action: str, reason: str,
    candidate_id: str = None,
    field_changes=None,
    error: str = None,
) -> None:
    """Insert audit log row. Never raises."""
    try:
        supabase.table('hr_resume_ingest_log').insert({
            'resume_file_id': file_id,
            'ai_result_id':   ai_result_id,
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
    """Fallback: mark a file as failed when outer exception occurs."""
    file_id = row.get('resume_file_id')
    if file_id:
        _update_file(file_id, {
            'ingest_status': 'failed',
            'ingest_result': 'FAILED',
            'ingest_error':  error[:500],
        })
    _log_ingest(
        file_id, row.get('ai_result_id'),
        row.get('organization_id', ''), row.get('candidate_email', ''),
        'FAIL', 'ERROR', error=error[:500],
    )