"""
yohr/csv_parser.py
Stage 1 — parse CSV, normalise phone/LinkedIn/name, bulk insert rows.
Runs every 30 s via APScheduler. Processes all sessions with status='pending'.
"""
import csv
import io
import logging
import re
import unicodedata
from typing import Optional

import phonenumbers

from .constants import supabase, YOHR_ORG_ID, STORAGE_BUCKET   # ← .constants not .config

logger = logging.getLogger(__name__)

# ── Country name → ISO-2 map (location field heuristic) ───────────────────────
COUNTRY_MAP: dict[str, str] = {
    "japan": "JP", "singapore": "SG", "turkey": "TR", "türkiye": "TR",
    "poland": "PL", "netherlands": "NL", "india": "IN", "sweden": "SE",
    "belgium": "BE", "bangladesh": "BD", "canada": "CA", "germany": "DE",
    "france": "FR", "spain": "ES", "portugal": "PT", "italy": "IT",
    "dubai": "AE", "uae": "AE", "united arab emirates": "AE",
    "saudi arabia": "SA", "egypt": "EG", "south africa": "ZA",
    "malaysia": "MY", "indonesia": "ID", "philippines": "PH",
    "armenia": "AM", "estonia": "EE", "lithuania": "LT",
    "iran": "IR", "myanmar": "MM", "bulgaria": "BG",
    "zimbabwe": "ZW", "cameroon": "CM", "jordan": "JO",
    "pakistan": "PK", "brazil": "BR", "tunisia": "TN",
    "ireland": "IE", "uk": "GB", "united kingdom": "GB",
    "usa": "US", "united states": "US", "america": "US",
    "malta": "MT", "latvia": "LV", "czech republic": "CZ", "czechia": "CZ",
    "switzerland": "CH", "austria": "AT", "denmark": "DK", "norway": "NO",
    "finland": "FI", "ukraine": "UA", "russia": "RU", "nigeria": "NG",
    "kenya": "KE", "ghana": "GH", "morocco": "MA", "thailand": "TH",
    "vietnam": "VN", "china": "CN", "hong kong": "HK", "australia": "AU",
    "new zealand": "NZ", "argentina": "AR", "colombia": "CO", "mexico": "MX",
    "israel": "IL", "greece": "GR", "romania": "RO", "hungary": "HU",
    "serbia": "RS", "croatia": "HR", "slovakia": "SK", "luxembourg": "LU",
    "iceland": "IS", "north holland": "NL", "south holland": "NL",
    "flevoland": "NL", "north brabant": "NL", "overijssel": "NL",
    "karnataka": "IN", "maharashtra": "IN", "tamil nadu": "IN",
    "telangana": "IN", "uttar pradesh": "IN", "gujarat": "IN",
    "ontario": "CA", "british columbia": "CA",
    "catalonia": "ES", "andalusia": "ES",
}

NULL_MARKERS = {" - ", "-", "- ", " -", "n/a", "na", "nil", "none", ""}


def clean_null(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    stripped = value.strip()
    return None if stripped.lower() in NULL_MARKERS or stripped == "-" else stripped


def split_name(full_name: str) -> tuple[str, str]:
    parts = full_name.strip().split(None, 1)
    first = parts[0].title() if parts else ""
    last  = parts[1].title() if len(parts) > 1 else ""
    return first, last


def normalise_linkedin(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    url = raw.strip().lower()
    url = re.sub(r'^https?://(www\.)?', '', url)
    url = re.sub(r'^in\.linkedin\.com/', 'linkedin.com/', url)
    url = url.rstrip('/')
    if 'linkedin.com/in/' in url:
        return 'https://' + url
    return None


def country_from_location(location: str) -> Optional[str]:
    if not location:
        return None
    segments = [s.strip().lower() for s in location.split(",")]
    for seg in reversed(segments):
        code = COUNTRY_MAP.get(seg)
        if code:
            return code
    loc_lower = location.lower()
    for name, code in COUNTRY_MAP.items():
        if name in loc_lower:
            return code
    return None


def normalise_phone(raw_phone: str, location: str = "") -> tuple[Optional[str], Optional[str]]:
    if not raw_phone:
        return None, None
    raw = raw_phone.strip()
    try:
        parsed = phonenumbers.parse(raw, None)
        if phonenumbers.is_valid_number(parsed):
            e164    = phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
            country = phonenumbers.region_code_for_number(parsed)
            return e164, country
    except phonenumbers.NumberParseException:
        pass
    hint = country_from_location(location)
    if hint:
        try:
            parsed = phonenumbers.parse(raw, hint)
            if phonenumbers.is_valid_number(parsed):
                e164    = phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
                country = phonenumbers.region_code_for_number(parsed)
                return e164, country
        except phonenumbers.NumberParseException:
            pass
    return raw, "UNKNOWN"


# ── Main entry point ───────────────────────────────────────────────────────────

def run_csv_parser() -> None:
    try:
        sessions = (
            supabase.table("org_csv_import_sessions")
            .select("id, file_storage_path, filename")
            .eq("org_id", YOHR_ORG_ID)
            .eq("status", "pending")
            .execute()
            .data
        )
    except Exception as exc:
        logger.error("csv_parser: failed to fetch pending sessions: %s", exc)
        return

    for session in sessions:
        _process_session(session)


def _process_session(session: dict) -> None:
    session_id   = session["id"]
    storage_path = session["file_storage_path"]
    logger.info("csv_parser: starting session %s (%s)", session_id, session["filename"])

    supabase.table("org_csv_import_sessions").update(
        {"status": "processing"}
    ).eq("id", session_id).execute()

    try:
        csv_bytes: bytes = supabase.storage.from_(STORAGE_BUCKET).download(storage_path)
        csv_text = csv_bytes.decode("utf-8-sig")
    except Exception as exc:
        logger.error("csv_parser: storage download failed for session %s: %s", session_id, exc)
        supabase.table("org_csv_import_sessions").update(
            {"status": "failed", "error_summary": f"CSV download failed: {exc}"}
        ).eq("id", session_id).execute()
        return

    try:
        reader       = csv.DictReader(io.StringIO(csv_text))
        all_headers  = reader.fieldnames or []
        extra_headers = all_headers[9:]

        rows_to_insert = []
        for row_num, raw_row in enumerate(reader, start=1):
            record = _build_row_record(session_id, row_num, raw_row, extra_headers)
            rows_to_insert.append(record)

        if not rows_to_insert:
            logger.warning("csv_parser: no rows found in session %s", session_id)
            supabase.table("org_csv_import_sessions").update(
                {"status": "failed", "error_summary": "CSV contained no data rows"}
            ).eq("id", session_id).execute()
            return

        for i in range(0, len(rows_to_insert), 200):
            batch = rows_to_insert[i:i + 200]
            supabase.table("org_csv_import_rows").insert(batch).execute()

        supabase.rpc("refresh_csv_session_counts", {"p_session_id": session_id}).execute()
        logger.info("csv_parser: session %s — inserted %d rows", session_id, len(rows_to_insert))

    except Exception as exc:
        logger.error("csv_parser: parse error for session %s: %s", session_id, exc, exc_info=True)
        supabase.table("org_csv_import_sessions").update(
            {"status": "failed", "error_summary": str(exc)}
        ).eq("id", session_id).execute()


def _build_row_record(session_id: str, row_num: int, raw: dict, extra_headers: list) -> dict:
    raw_name       = clean_null(raw.get("name", "")) or ""
    raw_email      = clean_null(raw.get("email", ""))
    raw_phone_str  = clean_null(raw.get("phone", "")) or ""
    raw_location   = clean_null(raw.get("location", "")) or ""
    raw_linkedin   = clean_null(raw.get("linkedin", ""))
    raw_resume_url = clean_null(raw.get("resume", ""))

    if not raw_email:
        return {
            "session_id": session_id, "row_number": row_num,
            "org_id": YOHR_ORG_ID, "raw_name": raw_name, "raw_email": None,
            "s1_status": "failed", "s1_error": "Missing email — row skipped",
        }

    phone_e164, phone_country = normalise_phone(raw_phone_str, raw_location)
    first_name, last_name     = split_name(raw_name)
    linkedin_clean            = normalise_linkedin(raw_linkedin)

    extra_fields: dict = {}
    for hdr in extra_headers:
        val = clean_null(raw.get(hdr, ""))
        if val:
            extra_fields[hdr] = val

    return {
        "session_id":           session_id,
        "row_number":           row_num,
        "org_id":               YOHR_ORG_ID,
        "raw_name":             raw_name or None,
        "raw_designation":      clean_null(raw.get("designation", "")),
        "raw_company":          clean_null(raw.get("company", "")),
        "raw_notice":           clean_null(raw.get("notice", "")),
        "raw_location":         raw_location or None,
        "raw_phone":            raw_phone_str or None,
        "raw_email":            raw_email,
        "raw_resume_url":       raw_resume_url,
        "raw_linkedin":         raw_linkedin,
        "raw_extra_fields":     extra_fields,
        "s1_status":            "done",
        "parsed_first_name":    first_name or None,
        "parsed_last_name":     last_name or None,
        "parsed_phone":         phone_e164,
        "parsed_phone_country": phone_country,
        "parsed_linkedin":      linkedin_clean,
        "s2_status":            "pending" if raw_resume_url else "skipped",
    }