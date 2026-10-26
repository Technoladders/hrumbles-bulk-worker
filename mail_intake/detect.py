"""
mail_intake/detect.py — resume detection, pure functions
(tests/test_mail_intake_detect.py).

Funnel, cheapest first:
  1. skip_message()     labels / own mail — no attachment looked at
  2. attachment_gate()  file type + size — no download
  3. meta_score()       file name, subject, sender, reply — no download;
                        < META_REJECT -> "rejected" without download
  4. content_score()    text of the downloaded file;
                        < CONTENT_REJECT -> "rejected" without AI
  5. AI parse           only for files that passed 3 and 4; also answers
                        is_resume + confidence
  6. categorize()       likely | check | rejected for the review screen
"""

import re

META_REJECT = -4
CONTENT_REJECT = 2

ALLOWED_EXT = {'pdf', 'docx', 'doc'}
ALLOWED_MIME = {
    'application/pdf',
    'application/x-pdf',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    'application/msword',
    'application/octet-stream',  # many clients send documents as octet-stream
}

SKIP_LABELS = {'SENT', 'DRAFT', 'SPAM', 'TRASH', 'CHAT', 'CATEGORY_PROMOTIONS'}

PERSONAL_DOMAINS = {
    'gmail.com', 'googlemail.com', 'yahoo.com', 'yahoo.co.in', 'yahoo.in', 'outlook.com', 'hotmail.com',
    'live.com', 'live.in', 'msn.com', 'icloud.com', 'me.com', 'rediffmail.com', 'rediff.com', 'aol.com',
    'proton.me', 'protonmail.com', 'zoho.com', 'zohomail.in', 'ymail.com', 'mail.com', 'gmx.com',
}

JOB_PORTAL_DOMAINS = (
    'naukri.com', 'linkedin.com', 'indeed.com', 'foundit.in', 'monsterindia.com', 'shine.com',
    'timesjobs.com', 'instahyre.com', 'iimjobs.com', 'hirist.com', 'apna.co', 'workindia.in', 'cutshort.io',
)

RESUME_NAME = re.compile(r'\b(resume|résumé|cv|curriculum\s*vitae|bio\s*data|biodata|profile)\b', re.I)
NOT_RESUME_NAME = re.compile(
    r'\b(invoice|inv\s?\d+|bill|receipt|statement|quotation|quote|purchase\s*order|po\s?\d+|payslip|pay\s*slip|'
    r'salary\s*slip|offer\s*letter|appointment\s*letter|relieving|experience\s*letter|form\s*16|itr|gst|tds|ticket|'
    r'itinerary|boarding|agreement|contract|nda|mou|proposal|brochure|catalog(ue)?|report|minutes|policy|jd|'
    r'job\s*description|presentation|deck|manual|certificate|aadhaar|aadhar|pan\s*card|passport|marksheet|transcript)\b',
    re.I,
)
RESUME_SUBJECT = re.compile(
    r'\b(resume|résumé|cv|curriculum vitae|bio[\s-]*data|application|applying|apply|candidature|job|position|'
    r'opening|vacancy|role|opportunity|profile|fresher|experienced|interview|referral|candidate)\b', re.I,
)
NOT_RESUME_SUBJECT = re.compile(
    r'\b(invoice|payment|receipt|order|quotation|statement|newsletter|webinar|subscription|bill|purchase|delivery|'
    r'shipment|meeting notes|minutes|agreement|contract|proposal|offer letter)\b', re.I,
)
AUTOMATED_SENDER = re.compile(r'^(no-?reply|billing|invoice|accounts?|finance|payments?|support|notifications?|alerts?)@', re.I)

SECTION_HEADINGS = [re.compile(p, re.I) for p in (
    r'\b(work|professional)\s+experience\b', r'\bexperience\b', r'\beducation(al)?\b',
    r'\b(technical\s+)?skills\b', r'\bprojects?\b', r'\b(career\s+)?objective\b',
    r'\b(professional\s+)?summary\b', r'\bcertifications?\b', r'\bqualifications?\b',
    r'\bemployment\s+history\b', r'\bdeclaration\b', r'\bhobbies\b', r'\blanguages\s+known\b',
    r'\bdate\s+of\s+birth\b|\bdob\b', r'\binternships?\b', r'\bachievements\b', r'\bresponsibilities\b',
)]

NOT_RESUME_CONTENT = [(re.compile(p, re.I), why) for p, why in (
    (r'\bGSTIN\b|\bGST\s*No\b', 'GST number'),
    (r'\binvoice\s*(no|number|#|date)\b', 'invoice fields'),
    (r'\b(sub\s*total|grand\s*total|amount\s+due|total\s+amount|balance\s+due)\b', 'billing totals'),
    (r'\b(purchase\s+order|bill\s+to|ship\s+to)\b', 'order fields'),
    (r'\bwe\s+are\s+pleased\s+to\s+(offer|inform)\b', 'offer/appointment letter'),
    (r'\b(earnings|deductions)\b[\s\S]{0,200}\bnet\s+pay\b', 'payslip'),
    (r'\bterms\s+and\s+conditions\b|\bwhereas\b|\bhereinafter\b', 'contract language'),
)]

EMAIL_RE = re.compile(r'[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}', re.I)
PHONE_RE = re.compile(r'\+?\d[\d\s\-().]{8,}\d')
RANGE_RE = re.compile(r'\b(19|20)\d{2}\s*(-|–|to)\s*((19|20)\d{2}|present|current|till\s+date|now)\b', re.I)


def _domain(email: str | None) -> str:
    return (email or '').rpartition('@')[2].lower()


def ext_of(filename: str) -> str:
    m = re.search(r'\.([a-z0-9]+)$', (filename or '').lower())
    return m.group(1) if m else ''


def skip_message(msg: dict, own_email: str | None) -> str | None:
    """Reason to ignore the whole message, or None."""
    if any(l in SKIP_LABELS for l in msg.get('label_ids') or []):
        return 'label'
    if own_email and msg.get('from_email') and msg['from_email'] == own_email.lower():
        return 'own_mail'
    if not msg.get('attachments'):
        return 'no_attachment'
    return None


def attachment_gate(att: dict, min_bytes: int = 10 * 1024, max_bytes: int = 10 * 1024 * 1024) -> str | None:
    """None = may be a resume file; otherwise the reason to ignore it."""
    if ext_of(att.get('filename', '')) not in ALLOWED_EXT:
        return 'file_type'
    mime = (att.get('mime_type') or '').lower()
    if mime and mime not in ALLOWED_MIME:
        return 'file_type'
    size = att.get('size') or 0
    if size and size < min_bytes:
        return 'too_small'
    if size and size > max_bytes:
        return 'too_large'
    return None


def looks_like_person_name(filename: str) -> bool:
    """'Ravi_Kumar.pdf', 'Priya Sharma Java Developer.docx'."""
    base = re.sub(r'\.[a-z0-9]+$', '', filename or '', flags=re.I)
    base = re.sub(r'\d+', ' ', re.sub(r'[_\-.()+]+', ' ', base)).strip()
    words = base.split()
    if not 2 <= len(words) <= 6:
        return False
    return sum(1 for w in words if re.fullmatch(r'[A-Za-z][a-z]+|[A-Z]{2,}', w)) >= 2


def meta_score(msg: dict, att: dict, attachment_count: int = 1) -> tuple[int, list[str]]:
    score, reasons = 0, []

    def add(n, why):
        nonlocal score
        score += n
        reasons.append(f"{'+' if n > 0 else ''}{n} {why}")

    # "Ravi_Kumar_Resume.pdf" -> "Ravi Kumar Resume pdf" so \b works around "_".
    name = re.sub(r'[_.\-()\[\]+]+', ' ', att.get('filename') or '')
    is_resume_name = bool(RESUME_NAME.search(name))
    if is_resume_name:
        add(4, 'file name says resume/CV')
    if NOT_RESUME_NAME.search(name):
        add(-6, 'file name looks like a non-resume document')
    elif not is_resume_name and looks_like_person_name(att.get('filename') or ''):
        add(2, "file name looks like a person's name")

    subject = msg.get('subject') or ''
    if RESUME_SUBJECT.search(subject):
        add(2, 'subject mentions a job/application')
    if NOT_RESUME_SUBJECT.search(subject):
        add(-3, 'subject looks transactional')

    domain = _domain(msg.get('from_email'))
    if domain in PERSONAL_DOMAINS:
        add(2, 'sender uses a personal mailbox')
    if any(domain == d or domain.endswith('.' + d) for d in JOB_PORTAL_DOMAINS):
        add(2, 'sent via a job portal')
    if AUTOMATED_SENDER.search(msg.get('from_email') or ''):
        add(-3, 'automated or accounts sender')
    if msg.get('auto_submitted'):
        add(-1, 'bulk/automated mail')
    if msg.get('is_reply'):
        add(1, 'reply in a conversation')

    if attachment_count > 5:
        add(-3, 'many attachments')
    elif attachment_count > 2:
        add(-1, 'several attachments')
    return score, reasons


def content_score(text: str) -> dict:
    t = text or ''
    if len(t.strip()) < 200:
        return {'score': 0, 'reasons': ['too little text (scanned image?)'], 'no_text': True}
    score, reasons = 0, []

    def add(n, why):
        nonlocal score
        score += n
        reasons.append(f"{'+' if n > 0 else ''}{n} {why}")

    head = t[:3000]
    if EMAIL_RE.search(head):
        add(2, 'email address')
    if PHONE_RE.search(head):
        add(1, 'phone number')
    if re.search(r'linkedin\.com/in/', t, re.I):
        add(1, 'LinkedIn profile')

    headings = sum(1 for r in SECTION_HEADINGS if r.search(t))
    if headings >= 5:
        add(4, f'{headings} resume section headings')
    elif headings >= 3:
        add(2, f'{headings} resume section headings')
    elif headings >= 1:
        add(1, 'a resume section heading')

    if len(RANGE_RE.findall(t)) >= 2:
        add(2, 'employment/education date ranges')
    if re.search(r'\b(resume|curriculum vitae)\b', head, re.I):
        add(1, 'says resume/CV')

    for rx, why in NOT_RESUME_CONTENT:
        if rx.search(t):
            add(-3, why)
    if len(t) > 60_000:
        add(-2, 'very long document')
    return {'score': score, 'reasons': reasons, 'no_text': False}


def categorize(meta: int, content: dict | None, ai: dict | None, has_email: bool) -> tuple[str, str | None]:
    """ai = {'is_resume': bool, 'confidence': float} or None."""
    if meta < META_REJECT:
        return 'rejected', 'file name/sender do not look like a resume'
    if content and content.get('no_text'):
        return 'check', 'no readable text — may be a scanned resume'
    if content and content['score'] < CONTENT_REJECT:
        return 'rejected', 'content does not look like a resume'
    if not ai:
        return 'check', 'not analysed'
    conf = float(ai.get('confidence') or 0)
    if ai.get('is_resume') is False and conf >= 0.7:
        return 'rejected', 'AI: not a resume'
    if ai.get('is_resume') is True and conf >= 0.75 and has_email:
        return 'likely', None
    if ai.get('is_resume') is True and not has_email:
        return 'check', 'no email in resume — sender email will be used'
    return 'check', 'unclear — please check'


def sniff_type(buf: bytes) -> str | None:
    """Magic bytes: the declared type is not evidence."""
    if not buf or len(buf) < 8:
        return None
    if buf[:5] == b'%PDF-':
        return 'pdf'
    if buf[:4] == b'PK\x03\x04':
        return 'docx'
    if buf[:8] == bytes.fromhex('d0cf11e0a1b11ae1'):
        return 'doc'
    return None
