"""
test_local.py
─────────────
Local end-to-end test for hrumbles-bulk-worker.
Run this AFTER:
  1. Redis is running (docker)
  2. Flask API is running (python app.py)
  3. RQ worker is running (rq worker bulk-pipeline)

Usage:
  python test_local.py

Tests:
  1. Redis ping
  2. Flask health endpoint
  3. Supabase connection + RPC check
  4. Upload a test PDF to Supabase Storage and insert a row into hr_resume_files
  5. Watch the row move through parse → ai_queued status
"""

import sys
import time
import uuid
import os
import json
import urllib.request

from dotenv import load_dotenv
load_dotenv()

# ── helpers ───────────────────────────────────────────────────────────────────
PASS = "✅"
FAIL = "❌"
WARN = "⚠️ "

def section(title):
    print(f"\n{'─'*50}")
    print(f" {title}")
    print('─'*50)

def ok(msg):   print(f"  {PASS} {msg}")
def fail(msg): print(f"  {FAIL} {msg}"); sys.exit(1)
def warn(msg): print(f"  {WARN} {msg}")


# ═══════════════════════════════════════════════════════════════════════════
# TEST 1 — Redis
# ═══════════════════════════════════════════════════════════════════════════
section("TEST 1: Redis connectivity")
try:
    import redis as _redis
    r = _redis.Redis(host=os.getenv('REDIS_HOST','localhost'), port=int(os.getenv('REDIS_PORT',6379)))
    r.ping()
    ok(f"Redis ping OK at {os.getenv('REDIS_HOST','localhost')}:{os.getenv('REDIS_PORT',6379)}")
    
    # Check queue
    from rq import Queue
    q = Queue('bulk-pipeline', connection=r)
    print(f"  ℹ️  Queue length: {len(q)}")
    
except Exception as e:
    fail(f"Redis failed: {e}\n  → Is Redis running? Run: docker run -d --name redis-bulk -p 6379:6379 redis")


# ═══════════════════════════════════════════════════════════════════════════
# TEST 2 — Flask health
# ═══════════════════════════════════════════════════════════════════════════
section("TEST 2: Flask API health endpoint")
PORT = os.getenv('PORT', '5010')
try:
    with urllib.request.urlopen(f"http://localhost:{PORT}/health", timeout=5) as resp:
        data = json.loads(resp.read())
    ok(f"Health: {data}")
    if not data.get('redis'):
        warn("Flask can't reach Redis — check REDIS_HOST in .env")
    if not data.get('scheduler'):
        warn("APScheduler not running in Flask")
except Exception as e:
    fail(f"Flask API not responding at ::{PORT}\n  → Run: python app.py")


# ═══════════════════════════════════════════════════════════════════════════
# TEST 3 — Supabase connection
# ═══════════════════════════════════════════════════════════════════════════
section("TEST 3: Supabase connection + RPC check")

SUPABASE_URL = os.getenv('SUPABASE_URL', '')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_KEY', '')

if not SUPABASE_URL or 'your_' in SUPABASE_KEY:
    fail("SUPABASE_URL or SUPABASE_SERVICE_KEY not set in .env")

try:
    from supabase import create_client
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    ok("Supabase client created")
except Exception as e:
    fail(f"Supabase client failed: {e}")

# Check if migration was run
try:
    result = sb.table('hr_resume_files').select('id').limit(1).execute()
    ok("Table hr_resume_files exists")
except Exception as e:
    fail(f"hr_resume_files table missing → Run the SQL migration first!\n  Error: {e}")

try:
    result = sb.rpc('get_bulk_pipeline_stats', {'p_org_id': '5db5a8c2-94e2-4327-8041-389cafdf452c'}).execute()
    ok(f"get_bulk_pipeline_stats RPC works: {result.data}")
except Exception as e:
    fail(f"get_bulk_pipeline_stats RPC failed → Check migration ran: {e}")

try:
    result = sb.rpc('check_resume_hashes_bulk', {
        'p_hashes': ['abc123'],
        'p_org_id': '5db5a8c2-94e2-4327-8041-389cafdf452c'
    }).execute()
    ok(f"check_resume_hashes_bulk RPC works")
except Exception as e:
    fail(f"check_resume_hashes_bulk RPC failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# TEST 4 — Upload a test PDF and watch it get parsed
# ═══════════════════════════════════════════════════════════════════════════
section("TEST 4: Upload test PDF → watch parse pipeline")

ORG_ID = '5db5a8c2-94e2-4327-8041-389cafdf452c'
BUCKET = os.getenv('STORAGE_BUCKET', 'talent-pool-resumes')

# Create a minimal but valid text-based PDF in memory
TEST_PDF_BYTES = b"""%PDF-1.4
1 0 obj
<< /Type /Catalog /Pages 2 0 R >>
endobj
2 0 obj
<< /Type /Pages /Kids [3 0 R] /Count 1 >>
endobj
3 0 obj
<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842]
/Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>
endobj
4 0 obj
<< /Length 300 >>
stream
BT
/F1 12 Tf
50 750 Td
(John Doe - Software Engineer) Tj
0 -20 Td
(Email: john.doe@example.com) Tj
0 -20 Td
(Phone: +91 9876543210) Tj
0 -20 Td
(Skills: Python, React, TypeScript, Node.js, PostgreSQL) Tj
0 -20 Td
(Experience: 5 years at TechCorp as Senior Engineer) Tj
0 -20 Td
(Education: B.Tech Computer Science, IIT Madras 2018) Tj
ET
endstream
endobj
5 0 obj
<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>
endobj
xref
0 6
0000000000 65535 f
0000000015 00000 n
0000000064 00000 n
0000000123 00000 n
0000000274 00000 n
0000000626 00000 n
trailer
<< /Size 6 /Root 1 0 R >>
startxref
718
%%EOF"""

test_file_id = str(uuid.uuid4())
storage_key  = f"bulk/{ORG_ID}/test-{test_file_id}.pdf"

print(f"  ℹ️  Uploading test PDF to storage: {storage_key}")
try:
    resp = sb.storage.from_(BUCKET).upload(
        storage_key,
        TEST_PDF_BYTES,
        {'content-type': 'application/pdf', 'upsert': 'true'}
    )
    ok(f"File uploaded to storage: {storage_key}")
except Exception as e:
    fail(f"Storage upload failed: {e}\n  → Check STORAGE_BUCKET and service key permissions")

# Compute a fake hash for the test
import hashlib
file_hash = hashlib.sha256(TEST_PDF_BYTES).hexdigest()

# Insert row into hr_resume_files
print(f"  ℹ️  Inserting row into hr_resume_files...")
try:
    row = {
        'id':              test_file_id,
        'organization_id': ORG_ID,
        'file_name':       'test-resume-john-doe.pdf',
        'file_size':       len(TEST_PDF_BYTES),
        'mime_type':       'application/pdf',
        'file_hash':       file_hash + '_test',   # suffix to avoid collision on re-runs
        'storage_path':    storage_key,
        'parse_status':    'pending',
        'ai_status':       'pending',
        'ingest_status':   'pending',
    }
    sb.table('hr_resume_files').insert(row).execute()
    ok(f"Row inserted: {test_file_id}")
except Exception as e:
    # Clean up storage
    sb.storage.from_(BUCKET).remove([storage_key])
    fail(f"DB insert failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# TEST 5 — Poll for parse completion (up to 60s)
# ═══════════════════════════════════════════════════════════════════════════
section("TEST 5: Watch parse worker process the file (up to 60s)")
print("  ℹ️  Waiting for RQ worker to parse the uploaded PDF...")
print("  ℹ️  If parse_status does not change, check RQ worker is running\n")

for attempt in range(12):   # 12 × 5s = 60s
    time.sleep(5)
    result = sb.table('hr_resume_files') \
        .select('id, parse_status, parse_method, ai_status, ingest_status, parse_error') \
        .eq('id', test_file_id) \
        .single() \
        .execute()
    row = result.data
    print(f"  [{attempt+1:2d}/12] parse={row['parse_status']:12s}  ai={row['ai_status']:12s}  ingest={row['ingest_status']}")

    if row['parse_status'] == 'parsed':
        ok(f"Parse SUCCESSFUL — method: {row['parse_method']}")
        print(f"         ai_status: {row['ai_status']} (waiting for 5-min AI submit cycle)")
        break
    elif row['parse_status'] == 'image_only':
        warn("PDF parsed as image_only (no text extracted) — this shouldn't happen with the test PDF")
        break
    elif row['parse_status'] == 'failed':
        fail(f"Parse FAILED: {row['parse_error']}\n  → Check RQ worker logs")
else:
    warn("Timed out waiting for parse. Check:\n  1. Is 'rq worker bulk-pipeline' running?\n  2. Are there errors in the RQ terminal?")


# ═══════════════════════════════════════════════════════════════════════════
# TEST 6 — Check pipeline stats RPC
# ═══════════════════════════════════════════════════════════════════════════
section("TEST 6: Pipeline stats via Flask API")
try:
    url = f"http://localhost:{PORT}/api/bulk/status?org_id={ORG_ID}"
    with urllib.request.urlopen(url, timeout=5) as resp:
        stats = json.loads(resp.read())
    ok("Stats from Flask API:")
    for k, v in stats.items():
        if v > 0:
            print(f"    {k:20s}: {v}")
except Exception as e:
    warn(f"Stats API failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# CLEANUP send
# ═══════════════════════════════════════════════════════════════════════════
section("Cleanup test data")
try:
    sb.table('hr_resume_files').delete().eq('id', test_file_id).execute()
    sb.storage.from_(BUCKET).remove([storage_key])
    ok("Test row and storage file deleted")
except Exception as e:
    warn(f"Cleanup failed (manual cleanup needed): {e}")

print(f"\n{'═'*50}")
print(" Local test complete!")
print(f"{'═'*50}\n")