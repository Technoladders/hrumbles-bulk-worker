"""
tests/test_mail_intake_pipeline.py
End-to-end tick with fakes: backfill listing -> detection funnel -> review
items -> approval -> Supabase RPC; then a live (history) sync; and a revoked
Google grant.

Run:
    python -m unittest tests.test_mail_intake_pipeline -v
"""
import os
import unittest

os.environ['INTAKE_GMAIL_GAP_SECONDS'] = '0.001'
os.environ['INTAKE_APPROVAL_GAP_SECONDS'] = '0.001'

from mail_intake import store  # noqa: E402
from mail_intake.gmail import GmailAuthError  # noqa: E402
from mail_intake.worker import tick  # noqa: E402

RESUME_TEXT = """RAVI KUMAR ravi.kumar@gmail.com +91 98765 43210
Professional Summary  Java developer.
Work Experience  Acme 2021 - Present  Beta 2018 - 2021
Education B.Tech 2018   Technical Skills Java   Projects Payments   Certifications AWS
""" * 3
OFFER_TEXT = """Dear Ravi, we are pleased to offer you the position. Terms and conditions apply. GSTIN 33AAA.
Grand Total 59000 Invoice No 12""" * 5

FILES = {
    'A1': b'%PDF-1.4 RESUME-A',
    'A2': b'%PDF-1.4 INVOICE',
    'A3': b'%PDF-1.4 RESUME-A',   # same bytes as A1, another email
    'A4': b'%PDF-1.4 OFFER',
    'A5': b'%PDF-1.4 SCAN',
}
TEXT = {'RESUME-A': RESUME_TEXT, 'OFFER': OFFER_TEXT, 'SCAN': ''}


def structure(mid, frm, subject, filename, att_id):
    return {
        'id': mid, 'labelIds': ['INBOX'], 'internalDate': '1788256800000',
        'payload': {
            'headers': [{'name': 'From', 'value': frm}, {'name': 'Subject', 'value': subject}],
            'parts': [{'partId': '1', 'mimeType': 'application/pdf', 'filename': filename,
                       'body': {'attachmentId': att_id, 'size': 60_000}}],
        },
    }


MESSAGES = {
    'm1': structure('m1', 'Ravi Kumar <ravi.kumar@gmail.com>', 'Application for Java Developer', 'Ravi_Kumar_Resume.pdf', 'A1'),
    'm2': structure('m2', 'Billing <billing@vendor.com>', 'Invoice for October', 'INV-2026-1043.pdf', 'A2'),
    'm3': structure('m3', 'Ravi Kumar <ravi.kumar@gmail.com>', 'Resending my resume', 'Ravi_Kumar_Resume.pdf', 'A3'),
    'm4': structure('m4', 'HR <hr@othercorp.com>', 'Documents', 'document.pdf', 'A4'),
    'm5': structure('m5', 'Priya <priya.s@yahoo.com>', 'CV for opening', 'Priya_Sharma_CV.pdf', 'A5'),
}


class FakeGmail:
    def __init__(self, calls):
        self.calls = calls

    def get_profile(self):
        return {'historyId': '100'}

    def list_messages(self, q, token=None):
        return ['m1', 'm2', 'm3', 'm4'], None

    def list_history(self, start, token=None):
        return (['m5'] if start == '100' else []), '101', None

    def get_structure(self, mid):
        return MESSAGES.get(mid)

    def get_attachment(self, _mid, att_id):
        self.calls['attachments'].append(att_id)
        return FILES[att_id]


class FakeFiles:
    def __init__(self):
        self.store = {}

    def save(self, org, sha, kind, buf):
        p = f'{org}/{sha}.{kind}'
        self.store[p] = buf
        return p

    def read(self, p):
        return self.store[p]

    def exists(self, p):
        return bool(p) and p in self.store

    def remove(self, p):
        self.store.pop(p, None)


class _Exec:
    def __init__(self, data):
        self.data = data

    def execute(self):
        return self


class FakeSupabase:
    def __init__(self, calls):
        self.calls = calls
        outer = self

        class _Bucket:
            def upload(self, key, buf, opts):
                outer.calls['uploads'].append(key)

            def get_public_url(self, key):
                return f'https://cdn/{key}'

        class _Storage:
            def from_(self, _bucket):
                return _Bucket()

        self.storage = _Storage()

    def rpc(self, name, args):
        self.calls['rpc'].append((name, args))
        return _Exec('INSERTED')


def make_deps():
    calls = {'attachments': [], 'ai': 0, 'rpc': [], 'uploads': []}
    gmail = FakeGmail(calls)

    def parse_resume(_text):
        calls['ai'] += 1
        return {'profile': {'candidate_name': 'Ravi Kumar', 'email': 'ravi.kumar@gmail.com', 'top_skills': ['Java']},
                'is_resume': True, 'confidence': 0.95, 'input_tokens': 10, 'output_tokens': 5}

    files = FakeFiles()
    deps = {
        'supabase': FakeSupabase(calls),
        'files': files,
        'extract_text': lambda buf, kind: TEXT.get(buf.decode('latin1').replace('%PDF-1.4 ', ''), ''),
        'parse_resume': parse_resume,
        'gmail_for': lambda mbx: gmail,
    }
    return deps, calls, files


def add_mailbox(mid='mbx-1', org='org-1'):
    store.db().execute("""insert into mailboxes (id, organization_id, connected_by, email, refresh_token_enc, status, created_at, updated_at)
                          values (?, ?, 'emp-1', 'hr@client.com', 'x', 'active', ?, ?)""", (mid, org, store.now(), store.now()))
    return mid


class PipelineTests(unittest.TestCase):
    def setUp(self):
        store.use_memory_for_tests()

    def test_backfill_review_approve_live(self):
        deps, calls, files = make_deps()
        mbx = add_mailbox()
        store.db().execute("""insert into backfill_runs (id, mailbox_id, after_date, state, created_by, created_at, updated_at)
                              values ('run-1', ?, '2026-01-01', 'listing', 'emp-1', ?, ?)""", (mbx, store.now(), store.now()))

        tick(deps)

        self.assertEqual(store.one('select history_id from mailboxes')['history_id'], '100',
                         'first live sync only records the cursor')
        run = store.one("select * from backfill_runs where id = 'run-1'")
        self.assertEqual(run['listed'], 4)
        self.assertEqual(run['state'], 'completed')

        items = {i['gmail_id']: i for i in store.all_rows('select * from items')}
        self.assertEqual(items['m1']['category'], 'likely')
        self.assertEqual(items['m2']['category'], 'rejected')
        self.assertIsNone(items['m2']['sha256'], 'invoice rejected from headers, never downloaded')
        self.assertNotIn('m3', items, 'same file in a second email is not a new item')
        self.assertEqual(items['m4']['category'], 'rejected')
        self.assertIsNone(items['m4']['local_path'], 'rejected files are not kept')
        self.assertNotIn('A2', calls['attachments'])
        self.assertEqual(calls['ai'], 1, 'AI runs only for the file that passed the cheap checks')
        self.assertIn(items['m1']['local_path'], files.store)

        # Approve m1 -> Supabase.
        store.db().execute("update items set status = 'approving', decided_by = 'emp-1' where id = ?", (items['m1']['id'],))
        store.db().execute('update mailboxes set last_sync_at = ?', (store.now(),))
        tick(deps)
        approved = store.one('select * from items where id = ?', (items['m1']['id'],))
        self.assertEqual(approved['status'], 'added')
        self.assertEqual(len(calls['rpc']), 1)
        name, args = calls['rpc'][0]
        self.assertEqual(name, 'upsert_candidate_with_timeframe')
        self.assertEqual(args['profile_data']['email'], 'ravi.kumar@gmail.com')
        self.assertEqual(args['organization_id_input'], 'org-1')
        self.assertEqual(args['user_id_input'], 'emp-1')
        self.assertEqual(args['usage_type_input'], 'gmail_resume_intake')
        self.assertRegex(args['resume_path_input'], r'^https://cdn/org-1/gmail-intake/[0-9a-f]{64}\.pdf$')
        self.assertIsNone(approved['local_path'])
        self.assertEqual(files.store, {}, 'local copy removed once it is in storage')

        # Live sync (cursor due) picks up m5: scanned PDF -> "check", no AI.
        store.db().execute("update mailboxes set last_sync_at = '2000-01-01T00:00:00+00:00'")
        tick(deps)
        m5 = store.one("select * from items where gmail_id = 'm5'")
        self.assertEqual(m5['category'], 'check')
        self.assertIn('scanned', m5['reason'])
        self.assertEqual(calls['ai'], 1)
        self.assertEqual(store.one('select history_id from mailboxes')['history_id'], '101')

    def test_revoked_grant_marks_mailbox_for_reconnect(self):
        deps, _calls, _files = make_deps()

        class Revoked:
            def get_profile(self):
                raise GmailAuthError('invalid_grant')

        deps['gmail_for'] = lambda mbx: Revoked()
        add_mailbox()
        tick(deps)
        mbx = store.one('select * from mailboxes')
        self.assertEqual(mbx['status'], 'error')
        self.assertIn('reconnect', mbx['sync_error'])


if __name__ == '__main__':
    unittest.main()
