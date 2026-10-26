"""
mail_intake/parse.py — AI resume parsing + text extraction.

Output fields match the talent-pool bulk upload so an approved profile goes
straight into upsert_candidate_with_timeframe; is_resume / resume_confidence
let the same call settle unclear files. Uses its own OpenAI client (key
MAIL_INTAKE_OPENAI_API_KEY, falling back to OPENAI_API_KEY) — never the shared
config.openai_client, so nothing about the bulk/YOHR clients changes.
"""

import json
import threading

from openai import OpenAI

from . import settings

SYSTEM_PROMPT = """
  You receive the text of an email attachment. First decide whether it is a candidate's resume / CV / bio-data.
  Return ONLY a single, valid JSON object with the exact keys specified below. Do not include any markdown formatting or explanations.

  "is_resume": boolean. true only for a person's resume, CV or bio-data. Invoices, offer letters, job descriptions, forms, reports, certificates and letters are false.
  "resume_confidence": number between 0 and 1.

  If is_resume is false, return only those two keys. Otherwise also return:

  "suggested_title": string.
  Rule 1: Use the most recent or prominent job title from work experience.
  Rule 2: If no work experience, infer a suitable title from the professional summary and skills.

  "candidate_name": string (Full Name).
  "email": string (Email address).
  "phone": string (Phone number, exactly as found).
  "linkedin_url": string. Rule: Must be a valid URL (starts with http, https, or www). If the text is just a phrase like "LinkedIn Profile", return null.
  "github_url": string. Rule: Must be a valid URL. If not a URL, return null.
  "current_location": string. Rule: Extract the full address if available. If no address is found, return null. Do not merge email or phone into address.
  "professional_summary": array of strings. Rules: 1. Always return as an array of separate bullet points. 2. Each bullet point must be a single complete sentence or phrase. 3. Do not merge multiple points into one string. 4. If no professional summary, omit this field.
  "top_skills": array of strings (List of key technical and soft skills).
  "work_experience": array of objects, each with "company", "designation", "duration", and "responsibilities" (array of strings). Rule: If a company and a client are mentioned, extract only the primary company name.
  "education": array of objects, each with "institution", "degree", and "year". Rule: If institution name is not explicitly stated, return null.
  "projects": array of strings. Rules: - Always extract the **entire Project Summary section exactly as written**, including titles, clients, environments, roles, descriptions, and bullet-point responsibilities. - Each project (PROJECT #1, PROJECT #2, etc.) must be preserved as one full string. - Do not replace with placeholders. - Do not summarize or shorten. Preserve all formatting and wording. - If no projects, return an empty array.
  "certifications": array of strings.
  "other_details": object. Rules: - Include only additional resume information. - Use exact heading names as keys. - Do NOT include project-related information. - If no such sections, return null.
  Important: - If a field cannot be found, use null or an empty array/string as appropriate.
"""

MAX_INPUT_CHARS = 30_000
_client = None
_client_lock = threading.Lock()

MIME_BY_KIND = {
    'pdf': 'application/pdf',
    'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    'doc': 'application/msword',
}


def _openai():
    global _client
    with _client_lock:
        if _client is None:
            _client = OpenAI(api_key=settings.OPENAI_API_KEY)
    return _client


def parse_resume(text: str) -> dict:
    """{'profile', 'is_resume', 'confidence', 'input_tokens', 'output_tokens'}"""
    res = _openai().chat.completions.create(
        model=settings.OPENAI_MODEL,
        response_format={'type': 'json_object'},
        messages=[
            {'role': 'system', 'content': SYSTEM_PROMPT},
            {'role': 'user', 'content': f'Attachment text:\n\n---\n{text[:MAX_INPUT_CHARS]}\n---'},
        ],
    )
    raw = res.choices[0].message.content if res.choices else None
    if not raw:
        raise RuntimeError('AI response was empty.')
    data = json.loads(raw)
    is_resume = data.pop('is_resume', None) is True
    try:
        confidence = max(0.0, min(1.0, float(data.pop('resume_confidence', 0) or 0)))
    except (TypeError, ValueError):
        confidence = 0.0
    if isinstance(data.get('email'), str):
        data['email'] = data['email'].strip().lower() or None
    usage = getattr(res, 'usage', None)
    return {
        'profile': data,
        'is_resume': is_resume,
        'confidence': confidence,
        'input_tokens': getattr(usage, 'prompt_tokens', 0) or 0,
        'output_tokens': getattr(usage, 'completion_tokens', 0) or 0,
    }


def extract_text(buf: bytes, kind: str) -> str:
    """Reuses the service's existing text_extractor (pdfminer/pypdf, python-docx, antiword)."""
    from text_extractor import extract_text as _extract
    text, _method = _extract(buf, MIME_BY_KIND.get(kind, ''), None)
    return text or ''
