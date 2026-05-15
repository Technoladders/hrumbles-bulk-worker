"""
text_extractor.py

Fast text extraction strategy (ordered by speed):
  PDF  → pdfminer.six → pypdf → mark as image_only (no OCR in this container)
  DOCX → python-docx
  DOC  → call existing resume-parser-container /api/extract-doc-text endpoint
  Other → unsupported

The existing resume-parser-container already has LibreOffice + Tesseract for DOC/OCR.
We call it for .doc files only to avoid duplicating heavy dependencies here.
"""

import io
import os
import logging
import requests
from typing import Tuple

logger = logging.getLogger(__name__)

MIN_TEXT_LENGTH = 100  # chars — below this we consider extraction failed/image


# ── PDF: Strategy 1 — pdfminer.six ───────────────────────────────────────────
def _extract_pdfminer(file_bytes: bytes) -> str:
    from pdfminer.high_level import extract_text_to_fp
    from pdfminer.layout import LAParams
    output = io.StringIO()
    extract_text_to_fp(
        io.BytesIO(file_bytes),
        output,
        laparams=LAParams(detect_vertical=False, all_texts=True),
        page_numbers=None,
    )
    return output.getvalue()


# ── PDF: Strategy 2 — pypdf ──────────────────────────────────────────────────
def _extract_pypdf(file_bytes: bytes) -> str:
    from pypdf import PdfReader
    reader = PdfReader(io.BytesIO(file_bytes))
    parts = []
    for page in reader.pages:
        t = page.extract_text()
        if t:
            parts.append(t)
    return '\n'.join(parts)


# ── DOCX ─────────────────────────────────────────────────────────────────────
def _extract_docx(file_bytes: bytes) -> str:
    from docx import Document
    doc = Document(io.BytesIO(file_bytes))
    parts = []
    for para in doc.paragraphs:
        if para.text.strip():
            parts.append(para.text)
    for table in doc.tables:
        for row in table.rows:
            for cell in row.cells:
                if cell.text.strip():
                    parts.append(cell.text.strip())
    return '\n'.join(parts)


# ── DOC — calls existing parser container ─────────────────────────────────────
def _extract_doc_via_parser(storage_path: str) -> str:
    """
    Calls /api/extract-doc-text on the existing resume-parser-container.
    That container has LibreOffice installed and handles .doc conversion.
    This endpoint needs to be added to the existing container (see DEPLOYMENT.md).
    """
    parser_url = os.getenv('RESUME_PARSER_URL', 'http://resume-parser-container:5005')
    try:
        resp = requests.post(
            f'{parser_url}/api/extract-doc-text',
            json={'storage_path': storage_path},
            timeout=120,
        )
        if resp.status_code == 200:
            return resp.json().get('text', '')
        logger.error(f'DOC extraction API {resp.status_code}: {resp.text[:300]}')
        return ''
    except requests.exceptions.ConnectionError:
        logger.error('Cannot reach resume-parser-container for DOC extraction')
        return ''
    except Exception as e:
        logger.error(f'DOC extraction via parser container failed: {e}')
        return ''


# ── Public entry point ────────────────────────────────────────────────────────
def extract_text(
    file_bytes: bytes,
    mime_type: str,
    storage_path: str = None,
) -> Tuple[str, str]:
    """
    Returns (text, method_used).

    method_used values:
      pdfminer   — text-based PDF, extracted with pdfminer.six
      pypdf      — text-based PDF, extracted with pypdf (pdfminer fallback)
      docx       — DOCX file, extracted with python-docx
      doc_api    — DOC file, extracted via existing parser container
      image_only — PDF with no extractable text (scanned/image-only)
      unsupported — file type not handled
      failed     — extraction attempted but returned empty
    """
    mime = (mime_type or '').lower()
    name = (storage_path or '').lower()

    # ── PDF ──────────────────────────────────────────────────────────────────
    if 'pdf' in mime or name.endswith('.pdf'):

        # Strategy 1: pdfminer.six (~0.1-0.5s for text PDFs)
        try:
            text = _extract_pdfminer(file_bytes)
            if len(text.strip()) >= MIN_TEXT_LENGTH:
                logger.debug(f'pdfminer extracted {len(text)} chars')
                return text.strip(), 'pdfminer'
        except Exception as e:
            logger.warning(f'pdfminer failed: {e}')

        # Strategy 2: pypdf
        try:
            text = _extract_pypdf(file_bytes)
            if len(text.strip()) >= MIN_TEXT_LENGTH:
                logger.debug(f'pypdf extracted {len(text)} chars')
                return text.strip(), 'pypdf'
        except Exception as e:
            logger.warning(f'pypdf failed: {e}')

        # No extractable text — scanned/image PDF
        logger.info('PDF is image-only (no extractable text found)')
        return '', 'image_only'

    # ── DOCX ─────────────────────────────────────────────────────────────────
    elif (
        'wordprocessingml' in mime or
        'docx' in mime or
        name.endswith('.docx') or
        mime == 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
    ):
        try:
            text = _extract_docx(file_bytes)
            if len(text.strip()) >= 50:
                logger.debug(f'python-docx extracted {len(text)} chars')
                return text.strip(), 'docx'
            logger.warning('DOCX returned very short text')
            return '', 'failed'
        except Exception as e:
            logger.error(f'DOCX extraction failed: {e}')
            return '', 'failed'

    # ── DOC (old binary Word format) ──────────────────────────────────────────
    elif (
        mime == 'application/msword' or
        'msword' in mime or
        name.endswith('.doc')
    ):
        if storage_path:
            text = _extract_doc_via_parser(storage_path)
            if len(text.strip()) >= 50:
                logger.debug(f'doc_api extracted {len(text)} chars')
                return text.strip(), 'doc_api'
        logger.warning(f'DOC extraction failed, storage_path={storage_path}')
        return '', 'failed'

    # ── Unsupported ───────────────────────────────────────────────────────────
    else:
        logger.warning(f'Unsupported mime type: {mime_type} for {storage_path}')
        return '', 'unsupported'