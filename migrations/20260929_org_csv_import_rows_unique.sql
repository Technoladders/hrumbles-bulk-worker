-- Make org_csv_import_rows idempotent per file row.
-- The csv_parser upserts with on_conflict (session_id, row_number) and
-- ignore_duplicates, which needs this unique index.
--
-- Apply AFTER removing existing duplicates (creating the index fails if any
-- (session_id, row_number) pair still appears more than once), and BEFORE
-- deploying the csv_parser that uses upsert.
--
-- Check first (should return 0 rows):
--   SELECT session_id, row_number, count(*) FROM org_csv_import_rows
--   GROUP BY 1, 2 HAVING count(*) > 1 LIMIT 20;

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS org_csv_import_rows_session_row_uniq
  ON public.org_csv_import_rows (session_id, row_number);
