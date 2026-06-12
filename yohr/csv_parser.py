"""
yohr/csv_parser.py
Stage 1 — parse CSV, normalise phone/LinkedIn/name, bulk insert rows.
Runs every 30 s via APScheduler. Processes all sessions with status='pending'.

Phone normalisation:
  - Detects Excel scientific notation (9.19767E+11) and converts best-effort
  - Uses phonenumbers lib with country hint from location field
  - Country hint uses pycountry + country_converter (no giant hardcoded dict)
  - Raw original always stored in _raw_phone_csv for ingestor → other_details
"""
import csv
import io
import logging
import re
import unicodedata
from decimal import Decimal, InvalidOperation
from typing import Optional

import phonenumbers
import pycountry
import country_converter as coco

from .constants import supabase, ACTIVE_ORG_IDS

logger = logging.getLogger(__name__)

# ── Sub-national region overrides (pycountry/coco don't know these) ───────────
# Only states, provinces, cities that lookup libs won't resolve to a country.
REGION_OVERRIDES: dict[str, str] = {
    # India — states & major cities
    "karnataka": "IN", "maharashtra": "IN", "tamil nadu": "IN",
    "telangana": "IN", "uttar pradesh": "IN", "gujarat": "IN",
    "rajasthan": "IN", "west bengal": "IN", "kerala": "IN",
    "andhra pradesh": "IN", "madhya pradesh": "IN", "bihar": "IN",
    "haryana": "IN", "punjab": "IN", "odisha": "IN", "assam": "IN",
    "jharkhand": "IN", "uttarakhand": "IN", "himachal pradesh": "IN",
    "goa": "IN", "delhi": "IN", "noida": "IN", "gurugram": "IN",
    "hyderabad": "IN", "pune": "IN", "bengaluru": "IN", "bangalore": "IN",
    "chennai": "IN", "mumbai": "IN", "kolkata": "IN", "coimbatore": "IN",
    # Canada — provinces
    "ontario": "CA", "british columbia": "CA", "alberta": "CA",
    "quebec": "CA", "nova scotia": "CA", "manitoba": "CA",
    # Netherlands — provinces
    "north holland": "NL", "south holland": "NL", "flevoland": "NL",
    "north brabant": "NL", "overijssel": "NL", "gelderland": "NL",
    "utrecht": "NL", "friesland": "NL", "zeeland": "NL",
    # Spain — regions
    "catalonia": "ES", "andalusia": "ES", "madrid": "ES",
    "valencia": "ES", "galicia": "ES",
    # Poland — cities / voivodeships
    "małopolskie": "PL", "masovian": "PL", "silesia": "PL",
    "krakow": "PL", "kraków": "PL", "warsaw": "PL",
    # Sweden — counties / cities
    "skåne": "SE", "skane": "SE", "stockholm": "SE", "gothenburg": "SE",
    # UAE clarifications
    "dubai": "AE", "abu dhabi": "AE", "sharjah": "AE",
    # Common aliases coco/pycountry might miss
    "uk": "GB", "great britain": "GB", "england": "GB",
    "usa": "US", "america": "US",
    "uae": "AE",
}

NULL_MARKERS = {" - ", "-", "- ", " -", "n/a", "na", "nil", "none", "null", ""}

# Singleton country_converter (avoids reloading 3 MB CSV on every call)
_CC = coco.CountryConverter()


# ── Helpers ───────────────────────────────────────────────────────────────────

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
    return 'https://' + url if 'linkedin.com/in/' in url else None


def country_from_location(location: str) -> Optional[str]:
    """
    Layered country detection from a free-text location string.
      1. Sub-national region overrides (states, provinces, cities)
      2. country_converter  (handles aliases: UK, USA, Emirates, etc.)
      3. pycountry          (strict ISO standard name lookup)
    Checks each comma-separated segment right-to-left (country usually last).
    """
    if not location:
        return None

    segments = [s.strip().lower() for s in location.split(",")]
    for seg in reversed(segments):
        if not seg:
            continue

        # 1. Region overrides
        code = REGION_OVERRIDES.get(seg)
        if code:
            return code

        # 2. country_converter
        try:
            result = _CC.convert(names=seg, to="ISO2", not_found=None)
            if result and result not in ("not found", "nan"):
                return str(result)
        except Exception:
            pass

        # 3. pycountry
        try:
            return pycountry.countries.lookup(seg).alpha_2
        except LookupError:
            pass

    return None


def _sci_to_str(raw: str) -> Optional[str]:
    """
    Convert Excel scientific notation to integer string.
    e.g. "9.19767E+11" → "919767000000"

    WARNING: Excel only retains ~6 significant figures, so the last digits
    are ZERO-PADDED (e.g. actual 919766748078 becomes 919767000000).
    Always store the original in other_details.raw_phone.
    """
    raw = raw.strip()
    if not re.match(r'^-?[\d.]+[Ee][+\-]?\d+$', raw):
        return None
    try:
        return str(int(Decimal(raw)))
    except (InvalidOperation, OverflowError, ValueError):
        return None


def normalise_phone(
    raw_phone: str,
    location: str = "",
) -> tuple[Optional[str], Optional[str], str]:
    """
    Returns (e164 | None,  country_code | None,  raw_original_csv_value).

    The third return value MUST be stored in other_details["raw_phone"] by the
    ingestor so no data is lost when sci notation truncated digits.

    Strategy:
      1. Detect & expand Excel scientific notation (best-effort, may lose digits)
      2. Try phonenumbers without region hint (catches +XX international numbers)
      3. Try phonenumbers with country hint from location field
      4. Try prepending "+" in case it was stripped
      5. Fallback: return digit-only string + "UNKNOWN"
    """
    raw_original = (raw_phone or "").strip()
    if not raw_original or raw_original.lower() in NULL_MARKERS:
        return None, None, raw_original

    working = raw_original

    # Step 1: sci notation → integer string
    converted = _sci_to_str(working)
    if converted:
        logger.debug("phone: sci '%s' → '%s' (precision may differ)", working, converted)
        working = converted

    # Steps 2–4: phonenumbers attempts
    region_hint = country_from_location(location)
    attempts: list[tuple[str, Optional[str]]] = [
        (working, None),
        (working, region_hint),
    ]
    if not working.startswith("+"):
        attempts.append(("+" + working, None))

    for candidate, region in attempts:
        if not candidate:
            continue
        try:
            parsed = phonenumbers.parse(candidate, region)
            if phonenumbers.is_valid_number(parsed):
                e164    = phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
                country = phonenumbers.region_code_for_number(parsed)
                return e164, country, raw_original
        except phonenumbers.NumberParseException:
            continue

    # Step 5: fallback — digits only
    cleaned = re.sub(r"[^\d+]", "", working)
    return (cleaned or None), "UNKNOWN", raw_original


# ── Main entry point ───────────────────────────────────────────────────────────

def run_csv_parser() -> None:
    try:
        sessions = (
            supabase.table("org_csv_import_sessions")
            .select("id, file_storage_path, filename, org_id")
            .in_("org_id", ACTIVE_ORG_IDS)
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
    org_id       = session["org_id"]
    storage_path = session["file_storage_path"]
    logger.info("csv_parser: starting session %s (%s)", session_id, session["filename"])

    from .constants import STORAGE_BUCKET

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
        reader        = csv.DictReader(io.StringIO(csv_text))
        all_headers   = reader.fieldnames or []
        extra_headers = all_headers[10:]   # cols 11+ are screening questions

        rows_to_insert = []
        for row_num, raw_row in enumerate(reader, start=1):
            record = _build_row_record(session_id, org_id, row_num, raw_row, extra_headers)
            rows_to_insert.append(record)

        if not rows_to_insert:
            logger.warning("csv_parser: no rows found in session %s", session_id)
            supabase.table("org_csv_import_sessions").update(
                {"status": "failed", "error_summary": "CSV contained no data rows"}
            ).eq("id", session_id).execute()
            return

        for i in range(0, len(rows_to_insert), 200):
            supabase.table("org_csv_import_rows").insert(rows_to_insert[i:i + 200]).execute()

        supabase.rpc("refresh_csv_session_counts", {"p_session_id": session_id}).execute()
        logger.info("csv_parser: session %s — inserted %d rows", session_id, len(rows_to_insert))

    except Exception as exc:
        logger.error("csv_parser: parse error for session %s: %s", session_id, exc, exc_info=True)
        supabase.table("org_csv_import_sessions").update(
            {"status": "failed", "error_summary": str(exc)}
        ).eq("id", session_id).execute()


def _build_row_record(
    session_id: str,
    org_id: str,
    row_num: int,
    raw: dict,
    extra_headers: list,
) -> dict:
    raw_name       = clean_null(raw.get("name", "")) or ""
    raw_email      = clean_null(raw.get("email", ""))
    raw_phone_str  = clean_null(raw.get("phone", "")) or ""
    raw_location   = clean_null(raw.get("location", "")) or ""
    raw_linkedin   = clean_null(raw.get("linkedin", ""))
    raw_resume_url = clean_null(raw.get("resume", ""))

    if not raw_email:
        return {
            "session_id": session_id, "row_number": row_num,
            "org_id": org_id, "raw_name": raw_name, "raw_email": None,
            "s1_status": "failed", "s1_error": "Missing email — row skipped",
        }

    # Phone: normalise + preserve raw original for ingestor
    phone_e164, phone_country, phone_raw_original = normalise_phone(
        raw_phone_str, raw_location
    )

    first_name, last_name = split_name(raw_name)
    linkedin_clean        = normalise_linkedin(raw_linkedin)

    # Extra screening question columns (col 11+)
    extra_fields: dict = {}
    for hdr in extra_headers:
        val = clean_null(raw.get(hdr, ""))
        if val:
            extra_fields[hdr] = val
    # Tag the raw phone original for ingestor to put in other_details
    if phone_raw_original and phone_raw_original != phone_e164:
        extra_fields["_raw_phone_csv"] = phone_raw_original

    return {
        "session_id":           session_id,
        "row_number":           row_num,
        "org_id":               org_id,
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