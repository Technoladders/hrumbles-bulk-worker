# Gmail Resume Intake (`mail_intake/`)

Reads a connected office Gmail inbox, picks out resume attachments, parses them
with AI and holds them for review. Approved resumes are added to
`hr_talent_pool` through `upsert_candidate_with_timeframe` (same rules as the
bulk upload). The frontend page is `/talent-pool/gmail-intake`; it is shown
only to users with `hr_employees.gmail_resume_intake_access = true`.

## How it plugs in

Self-contained package. Changes to existing files (nothing else in the bulk or
YOHR pipelines is touched):

| File | Change |
|---|---|
| `app.py` | `register_mail_intake(app, scheduler)` — same pattern as `register_yohr_jobs` |
| `.github/workflows/deploy-bulk-worker.yml`, `deploy.sh` | `-v "$APP_DIR/intake-data:/data"` so the SQLite queue and held files survive redeploys |

- Routes: `/api/mail-intake/*` (blueprint; existing `CORS(app, '/api/*')` covers it).
- Job: `mail_intake_tick` in the existing APScheduler, every 15 s, `max_instances=1`.
  It runs in the scheduler thread pool, **not** the RQ `bulk-pipeline` queue, so
  bulk jobs are never delayed.
- Storage: SQLite + held resume files under `/data` (the volume). Supabase gets one
  access-check query per user per minute, plus one upload + one RPC per approved
  resume.
- Kill switch: `MAIL_INTAKE_ENABLED=false` registers nothing.

## Setup checklist

1. **SQL** (owner, in the frontend repo): `supabase/pending-approval/gmail_intake/01_…sql`,
   then `02_grant_access.sql` for the client's users.
2. **Server**: `mkdir -p /opt/hrumbles-bulk-worker/intake-data` (the deploy mounts it).
3. **Google Cloud** (same project as the Gmail app):
   - OAuth client → redirect URI `https://bulk-api.xrilic.ai/api/mail-intake/oauth/callback`.
   - Consent screen status **In production** (in *Testing*, refresh tokens expire after 7 days).
4. **`/opt/hrumbles-bulk-worker/.env`** — add `GOOGLE_INTAKE_CLIENT_ID`,
   `GOOGLE_INTAKE_CLIENT_SECRET`, `GOOGLE_INTAKE_REDIRECT_URI`, `INTAKE_ENCRYPTION_KEY`
   (`python -c "import secrets; print(secrets.token_hex(32))"`; losing it means
   mailboxes must be reconnected). Optional ones: see `settings.py`.
5. **Frontend env**: `VITE_BULK_WORKER_URL=https://bulk-api.xrilic.ai`.
6. Users log out and in again after access is granted.

## Detection funnel (`detect.py`)

Gmail search (`has:attachment filename:(pdf OR doc OR docx)`, excludes sent /
spam / promotions) → header score (file name, subject, sender, attachment count;
invoices etc. rejected without download) → download + magic bytes + sha256
dedupe → text score via the existing `text_extractor` (sections, dates, contact
details vs invoice/letter signals) → AI parse with `is_resume` → review tabs
*Likely resumes* / *Needs check* / *Not resumes*.

## Tests

```
python -m unittest tests.test_mail_intake_detect tests.test_mail_intake_security tests.test_mail_intake_pipeline -v
```
