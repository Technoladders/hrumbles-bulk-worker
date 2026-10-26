"""
tests/test_mail_intake_detect.py
Resume detection funnel + Gmail helpers (pure functions, no network).

Run:
    python -m unittest tests.test_mail_intake_detect -v
"""
import unittest

from mail_intake.detect import (
    META_REJECT, attachment_gate, categorize, content_score, looks_like_person_name, meta_score, skip_message, sniff_type,
)
from mail_intake.gmail import build_backfill_query, parse_address, summarize_message


def msg(**over):
    m = {
        'label_ids': ['INBOX'], 'subject': '', 'from_email': 'someone@gmail.com', 'from_name': 'Someone',
        'is_reply': False, 'auto_submitted': False,
        'attachments': [{'filename': 'x.pdf', 'mime_type': 'application/pdf', 'size': 50_000}],
    }
    m.update(over)
    return m


RESUME_TEXT = """
RAVI KUMAR
ravi.kumar@gmail.com | +91 98765 43210 | linkedin.com/in/ravikumar
Professional Summary
Java developer with 5 years of experience.
Work Experience
Acme Corp — Senior Developer   2021 - Present
Beta Ltd — Developer   2018 - 2021
Education
B.Tech, Anna University, 2018
Technical Skills
Java, Spring Boot, AWS
Projects
Payments platform
Certifications
AWS Certified Developer
""" * 2

INVOICE_TEXT = """
TAX INVOICE
GSTIN: 33AAAAA0000A1Z5
Invoice No: INV-1043   Invoice Date: 01/10/2026
Bill To: Client Pvt Ltd     Ship To: Chennai
Consulting services  1  50000  50000
Sub Total 50000  Grand Total 59000  Amount Due 59000
Terms and Conditions apply. Contact accounts@vendor.com
""" * 3


class DetectTests(unittest.TestCase):
    def test_skip_message(self):
        self.assertEqual(skip_message(msg(label_ids=['SENT']), 'hr@co.com'), 'label')
        self.assertEqual(skip_message(msg(label_ids=['SPAM']), 'hr@co.com'), 'label')
        self.assertEqual(skip_message(msg(from_email='hr@co.com'), 'HR@co.com'), 'own_mail')
        self.assertEqual(skip_message(msg(attachments=[]), 'hr@co.com'), 'no_attachment')
        self.assertIsNone(skip_message(msg(), 'hr@co.com'))

    def test_attachment_gate(self):
        self.assertIsNone(attachment_gate({'filename': 'a.pdf', 'mime_type': 'application/pdf', 'size': 50_000}))
        self.assertIsNone(attachment_gate({'filename': 'a.docx', 'mime_type': 'application/octet-stream', 'size': 50_000}))
        self.assertEqual(attachment_gate({'filename': 'a.xlsx', 'mime_type': 'application/vnd.ms-excel', 'size': 50_000}), 'file_type')
        self.assertEqual(attachment_gate({'filename': 'a.jpg', 'mime_type': 'image/jpeg', 'size': 50_000}), 'file_type')
        self.assertEqual(attachment_gate({'filename': 'a.pdf', 'mime_type': 'image/png', 'size': 50_000}), 'file_type')
        self.assertEqual(attachment_gate({'filename': 'a.pdf', 'mime_type': 'application/pdf', 'size': 2_000}), 'too_small')
        self.assertEqual(attachment_gate({'filename': 'a.pdf', 'mime_type': 'application/pdf', 'size': 20 * 1024 * 1024}), 'too_large')

    def test_person_name(self):
        self.assertTrue(looks_like_person_name('Ravi_Kumar.pdf'))
        self.assertTrue(looks_like_person_name('Priya Sharma Java Developer.docx'))
        self.assertTrue(looks_like_person_name('RAVI KUMAR.pdf'))
        self.assertFalse(looks_like_person_name('scan0001.pdf'))
        self.assertFalse(looks_like_person_name('document.pdf'))

    def test_meta_score(self):
        s, why = meta_score(msg(subject='Application for Java Developer', from_email='ravi.k@gmail.com'),
                            {'filename': 'Ravi_Kumar_Resume.pdf'})
        self.assertGreaterEqual(s, 6, why)
        s, why = meta_score(msg(subject='Invoice for October', from_email='billing@vendor.com'),
                            {'filename': 'INV-2026-1043.pdf'})
        self.assertLess(s, META_REJECT, why)
        s, why = meta_score(msg(subject='Offer', from_email='hr@company.com'), {'filename': 'Offer_Letter_Ravi.pdf'})
        self.assertLess(s, 0, why)
        s, _ = meta_score(msg(from_email='a@company.com'), {'filename': 'doc.pdf'}, 8)
        self.assertLess(s, 0)
        s, why = meta_score(msg(from_email='info@naukri.com', subject='New application received'),
                            {'filename': 'Naukri_AmitShah[5y_0m].pdf'})
        self.assertGreaterEqual(s, 3, why)

    def test_content_score(self):
        r = content_score(RESUME_TEXT)
        self.assertGreaterEqual(r['score'], 8, r['reasons'])
        inv = content_score(INVOICE_TEXT)
        self.assertLess(inv['score'], 2, inv['reasons'])
        self.assertTrue(content_score('   \n ')['no_text'])

    def test_categorize(self):
        c = {'score': 8, 'no_text': False}
        self.assertEqual(categorize(5, c, {'is_resume': True, 'confidence': 0.9}, True)[0], 'likely')
        self.assertEqual(categorize(5, c, {'is_resume': True, 'confidence': 0.6}, True)[0], 'check')
        self.assertEqual(categorize(5, c, {'is_resume': True, 'confidence': 0.9}, False)[0], 'check')
        self.assertEqual(categorize(5, c, {'is_resume': False, 'confidence': 0.9}, False)[0], 'rejected')
        self.assertEqual(categorize(5, {'score': 0, 'no_text': True}, None, False)[0], 'check')
        self.assertEqual(categorize(-10, c, None, False)[0], 'rejected')
        self.assertEqual(categorize(0, {'score': 1, 'no_text': False}, None, False)[0], 'rejected')

    def test_sniff_type(self):
        self.assertEqual(sniff_type(b'%PDF-1.7 rest'), 'pdf')
        self.assertEqual(sniff_type(b'PK\x03\x04\x00\x00\x00\x00'), 'docx')
        self.assertEqual(sniff_type(bytes.fromhex('d0cf11e0a1b11ae1')), 'doc')
        self.assertIsNone(sniff_type(b'<html><body>'))


class GmailHelperTests(unittest.TestCase):
    def test_backfill_query(self):
        q = build_backfill_query('2026-01-15', '2026-06-01', 'hr@client.com')
        for part in ('has:attachment', 'filename:(pdf OR doc OR docx)', 'after:2026/01/15', 'before:2026/06/01',
                     '-from:hr@client.com', '-in:sent'):
            self.assertIn(part, q)

    def test_summarize_message(self):
        s = summarize_message({
            'id': 'm1', 'labelIds': ['INBOX'], 'internalDate': '1767225600000',
            'payload': {
                'headers': [{'name': 'From', 'value': '"Ravi Kumar" <Ravi.K@Gmail.com>'},
                            {'name': 'Subject', 'value': 'Re: Java Developer role'}],
                'parts': [
                    {'partId': '0', 'mimeType': 'multipart/alternative',
                     'parts': [{'partId': '0.0', 'mimeType': 'text/plain', 'body': {'size': 10}}]},
                    {'partId': '1', 'mimeType': 'application/pdf', 'filename': 'Ravi_Kumar.pdf',
                     'body': {'attachmentId': 'A1', 'size': 80_000}},
                    {'partId': '2', 'mimeType': 'image/png', 'filename': '', 'body': {'size': 500}},
                ],
            },
        })
        self.assertEqual(s['from_email'], 'ravi.k@gmail.com')
        self.assertEqual(s['from_name'], 'Ravi Kumar')
        self.assertTrue(s['is_reply'])
        self.assertEqual(s['attachments'], [{'part_id': '1', 'filename': 'Ravi_Kumar.pdf', 'mime_type': 'application/pdf',
                                             'size': 80_000, 'attachment_id': 'A1'}])
        self.assertTrue(s['received_at'].startswith('2026-01-01T00:00:00'))

    def test_parse_address(self):
        self.assertEqual(parse_address('plain@x.com'), (None, 'plain@x.com'))
        self.assertEqual(parse_address('Name <A@X.com>'), ('Name', 'a@x.com'))


if __name__ == '__main__':
    unittest.main()
