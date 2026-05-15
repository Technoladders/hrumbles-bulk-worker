"""
fix_stuck.py — run once to fix the 5 stuck 'parsing' files
then parse them, submit to OpenAI, and watch the full pipeline.
"""
from config import supabase
from collections import Counter
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

# ── 1. Reset the 5 stuck 'parsing' files ─────────────────────────────────────
print("\n=== Resetting stuck files ===")
supabase.table('hr_resume_files')\
    .update({'parse_status': 'pending', 'parse_attempts': 0})\
    .eq('parse_status', 'parsing')\
    .execute()

result = supabase.table('hr_resume_files').select('parse_status').execute()
counts = Counter(row['parse_status'] for row in result.data)
print("Status after reset:", dict(counts))

# ── 2. Parse the remaining 5 files ───────────────────────────────────────────
print("\n=== Parsing remaining files ===")
from bulk_tasks import parse_resume_batch
r = parse_resume_batch()
print("Parse result:", r)

# ── 3. Final parse status check ───────────────────────────────────────────────
result = supabase.table('hr_resume_files').select('parse_status').execute()
counts = Counter(row['parse_status'] for row in result.data)
print("\nFinal parse status:", dict(counts))
print("✅ Ready for AI submission" if counts.get('parsed', 0) == 24 else "⚠️  Some files still not parsed")