"""
run_pipeline.py — manually drive the full pipeline for local testing.
Runs: parse → submit_ai → poll (with retry loop) → ingest

Usage:
    py run_pipeline.py              # full pipeline
    py run_pipeline.py submit       # only AI submit
    py run_pipeline.py poll         # only poll OpenAI
    py run_pipeline.py ingest       # only ingest
"""
import sys
import time
import logging
from collections import Counter
from config import supabase

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

def show_status():
    files = supabase.table('hr_resume_files').select('parse_status, ai_status, ingest_status').execute().data
    parse   = Counter(r['parse_status']  for r in files)
    ai      = Counter(r['ai_status']     for r in files)
    ingest  = Counter(r['ingest_status'] for r in files)
    print(f"\n  Parse:  {dict(parse)}")
    print(f"  AI:     {dict(ai)}")
    print(f"  Ingest: {dict(ingest)}\n")

# ────────────────────────────────────────────────────────────────────────────
mode = sys.argv[1] if len(sys.argv) > 1 else 'all'

# ── PARSE ────────────────────────────────────────────────────────────────────
if mode in ('all', 'parse'):
    print("\n" + "="*50)
    print(" STAGE 1: PARSE")
    print("="*50)
    from bulk_tasks import parse_resume_batch
    result = parse_resume_batch()
    print("Result:", result)
    show_status()

# ── SUBMIT TO OPENAI ──────────────────────────────────────────────────────────
if mode in ('all', 'submit'):
    print("\n" + "="*50)
    print(" STAGE 2: SUBMIT TO OPENAI BATCH API")
    print("="*50)
    from bulk_tasks import submit_ai_batch
    result = submit_ai_batch()
    print("Result:", result)

    # Show batch job created
    jobs = supabase.table('hr_resume_batch_jobs')\
        .select('id, status, openai_batch_id, file_count')\
        .order('created_at', desc=True)\
        .limit(3)\
        .execute().data
    print("\nBatch jobs:")
    for j in jobs:
        print(f"  {j['id'][:8]}  status={j['status']}  oai={j['openai_batch_id']}  files={j['file_count']}")
    show_status()

# ── POLL OPENAI ───────────────────────────────────────────────────────────────
if mode in ('all', 'poll'):
    print("\n" + "="*50)
    print(" STAGE 3: POLL OPENAI (checks every 2 min, max 30 min)")
    print("="*50)
    from bulk_tasks import poll_ai_batches

    for attempt in range(15):   # 15 × 2 min = 30 min max
        result = poll_ai_batches()
        print(f"Poll #{attempt+1}: {result}")

        files = supabase.table('hr_resume_files').select('ai_status').execute().data
        ai_counts = Counter(r['ai_status'] for r in files)
        print(f"  AI status: {dict(ai_counts)}")

        done  = ai_counts.get('done', 0)
        total = len(files)

        if done == total:
            print(f"\n✅ All {total} files AI-processed!")
            break
        elif ai_counts.get('failed', 0) > 0:
            print(f"⚠️  {ai_counts['failed']} files failed AI")
            if ai_counts.get('done', 0) + ai_counts.get('failed', 0) == total:
                break

        if attempt < 14:
            print(f"  Waiting 2 minutes before next poll... ({done}/{total} done)")
            time.sleep(120)
    else:
        print("⚠️  Timed out after 30 minutes. Run 'py run_pipeline.py poll' again later.")

    show_status()

# ── INGEST ────────────────────────────────────────────────────────────────────
if mode in ('all', 'ingest'):
    print("\n" + "="*50)
    print(" STAGE 4: INGEST CANDIDATES INTO hr_talent_pool")
    print("="*50)
    from bulk_tasks import ingest_candidates_batch
    result = ingest_candidates_batch()
    print("Result:", result)

    # Show ingest log
    logs = supabase.table('hr_resume_ingest_log')\
        .select('candidate_email, action, reason, error_detail')\
        .order('created_at', desc=True)\
        .limit(10)\
        .execute().data
    print(f"\nIngest log (last {len(logs)} rows):")
    for l in logs:
        status = "✅" if l['action'] in ('INSERT','UPDATE') else "⚠️ " if l['action'] == 'SKIP' else "❌"
        print(f"  {status} {l['candidate_email'] or '(no email)':40s} {l['reason']}")
        if l['error_detail']:
            print(f"     ERROR: {l['error_detail']}")

    show_status()

    # Final summary from hr_talent_pool
    inserted = result.get('inserted', 0)
    updated  = result.get('updated', 0)
    skipped  = result.get('skipped', 0)
    failed   = result.get('failed', 0)
    print(f"\n{'='*50}")
    print(f" PIPELINE COMPLETE")
    print(f"  +{inserted} candidates inserted into hr_talent_pool")
    print(f"  ~{updated}  candidates updated")
    print(f"  ={skipped}  candidates skipped (recent / no email)")
    print(f"  ✗{failed}  failed")
    print(f"{'='*50}\n")