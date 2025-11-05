from __future__ import annotations
from typing import Dict, Any, List, Optional
from urllib.parse import urlparse
import re
from timezonefinder import TimezoneFinder
import pytz
_tf = TimezoneFinder()
from typing import Tuple, Dict, Any, List, Optional


def _first_or_none(seq: Optional[List[str]]) -> Optional[str]:
    return (seq[0] if seq else None)


def guess_company_name(site_name: Optional[str], title: Optional[str]) -> Optional[str]:
    """
    Returns a neat brand name from site_name/title, stripping boilerplate like
    ': Home', '- Home', '| Official Site', etc.
    """
    CANDIDATES = [site_name, title]
    for cand in CANDIDATES:
        if not cand:
            continue
        s = cand.strip()

        # Strip common separators + trailing sections
        # Examples handled: "Zapcom: Home", "Zapcom - Home", "Zapcom | Home"
        s = re.sub(r"\s*[:\-\|–—]\s*home\s*$", "", s, flags=re.I)

        # Also strip generic tails: About us / Official Site / Welcome
        s = re.sub(r"\s*[:\-\|–—]\s*(about( us)?|official site|welcome)\s*$", "", s, flags=re.I)

        # Remove repeated spaces
        s = re.sub(r"\s{2,}", " ", s).strip()

        if s:
            return s

    return site_name or title


# --- Country code -> name (tiny map; extend as needed) ---
COUNTRY_CODE_MAP = {
    "IN": "India", "US": "United States", "GB": "United Kingdom", "AE": "United Arab Emirates",
    "SG": "Singapore", "DE": "Germany", "FR": "France", "ES": "Spain", "IT": "Italy",
    "CA": "Canada", "AU": "Australia"
}

def _expand_country(val: str) -> str:
    if not val:
        return ""
    v = val.strip()
    return COUNTRY_CODE_MAP.get(v.upper(), v)

def _strip_trailing_dash(u: str) -> str:
    return u[:-1] if u and u.endswith("-") else u


def pick_official_site(google_results: List[Dict[str, Any]]) -> Optional[str]:
    if not google_results:
        return None
    bad_hosts = ("linkedin.com", "facebook.com", "instagram.com", "tripadvisor.", "booking.", "google.com", "maps.google")
    filtered = [r for r in google_results if not any(b in (r.get("url") or "") for b in bad_hosts)]
    return (filtered[0].get("url") if filtered else google_results[0].get("url"))

def _root_token(host: Optional[str]) -> Optional[str]:
    if not host:
        return None
    parts = host.lower().split(".")
    # crude root token: second-level label (e.g., "zapcom" from zapcom.ai)
    return parts[-2] if len(parts) >= 2 else parts[0]

from urllib.parse import urlparse

def _filter_emails_by_domain(emails: List[str], official_url: Optional[str]) -> List[str]:
    if not emails:
        return []
    if not official_url:
        return sorted(set(emails))

    host = urlparse(official_url).netloc.lower().split(":")[0]
    parts = host.split(".")
    registrable = ".".join(parts[-2:]) if len(parts) >= 2 else host  # naive eTLD+1
    keep = [e for e in set(emails) if e.lower().endswith("@" + registrable)]
    # Fallback: if strict match finds none, fall back to brand-token logic you had
    if keep:
        return sorted(set(keep))

    # brand token fallback
    token = parts[-2] if len(parts) >= 2 else parts[0]
    keep2 = []
    for e in set(emails):
        try:
            edomain = e.split("@", 1)[1].lower()
        except Exception:
            continue
        if token in edomain:
            keep2.append(e)
    return sorted(set(keep2))


def _clean_phone(p: str) -> Optional[str]:
    # keep digits and +; collapse spaces/dashes
    digits = re.sub(r"[^\d+]", "", p)
    # reject obvious non-phones like employee ranges 201-1000, etc.
    # keep 8..15 digits (typical E.164 length range)
    dcount = len(re.sub(r"[^\d]", "", digits))
    if dcount < 8 or dcount > 15:
        return None
    return digits

def _merge_and_clean_phones(raws: List[str]) -> List[str]:
    cleaned = []
    for p in raws or []:
        cp = _clean_phone(p)
        if cp:
            cleaned.append(cp)
    return sorted(set(cleaned))

def _pick_linkedin(urls: List[str]) -> Optional[str]:
    if not urls:
        return None
    # prefer company page without /posts
    companies = [u for u in urls if "/company/" in u and "/posts" not in u]
    if companies:
        return companies[0]
    # else first linkedin link
    return urls[0]

from typing import Dict, Any, List, Optional
from urllib.parse import urlparse
import re

def _first_or_none(seq: Optional[List[str]]) -> Optional[str]:
    return (seq[0] if seq else None)

def guess_company_name(site_name: Optional[str], title: Optional[str]) -> Optional[str]:
    for cand in [site_name, title]:
        if cand:
            c = re.sub(r"\s*\|\s*.*$", "", cand).strip()
            c = re.sub(r"\s*-\s*Home\s*$", "", c, flags=re.I)
            if c:
                return c
    return site_name or title

def pick_official_site(google_results: List[Dict[str, Any]]) -> Optional[str]:
    if not google_results:
        return None
    bad_hosts = ("linkedin.com", "facebook.com", "instagram.com", "tripadvisor.", "booking.", "google.com", "maps.google")
    filtered = [r for r in google_results if not any(b in (r.get("url") or "") for b in bad_hosts)]
    return (filtered[0].get("url") if filtered else google_results[0].get("url"))

def _root_token(host: Optional[str]) -> Optional[str]:
    if not host:
        return None
    parts = host.lower().split(".")
    return parts[-2] if len(parts) >= 2 else parts[0]

def _filter_emails_by_domain(emails: List[str], official_url: Optional[str]) -> List[str]:
    if not emails:
        return []
    if not official_url:
        return sorted(set(emails))
    host = urlparse(official_url).netloc
    token = _root_token(host)
    if not token:
        return sorted(set(emails))
    keep = []
    for e in set(emails):
        parts = e.split("@", 1)
        if len(parts) == 2:
            edomain = parts[1].lower()
            if token in edomain:
                keep.append(e)
    return sorted(set(keep))

def _clean_phone(p: str) -> Optional[str]:
    digits = re.sub(r"[^\d+]", "", p)
    dcount = len(re.sub(r"[^\d]", "", digits))
    if dcount < 8 or dcount > 15:
        return None
    return digits

def _merge_and_clean_phones(raws: List[str]) -> List[str]:
    cleaned = []
    for p in raws or []:
        cp = _clean_phone(p)
        if cp:
            cleaned.append(cp)
    return sorted(set(cleaned))

def _pick_linkedin(urls: List[str]) -> Optional[str]:
    if not urls:
        return None
    companies = [u for u in urls if "/company/" in u and "/posts" not in u]
    if companies:
        return companies[0]
    return urls[0]

from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urlparse

def assemble_lead_record(
    query: str,
    google_results: List[Dict[str, Any]],
    scraped_rows: List[Dict[str, Any]],
    maps_place: Optional[Dict[str, Any]] = None,
    linkedin_url: Optional[str] = None
) -> Dict[str, Any]:
    official = pick_official_site(google_results)
    domain = urlparse(official).netloc if official else None

    all_emails, all_phones, all_linkedins = set(), [], []
    rating_value, review_count = None, None
    company, city, country = None, None, None
    industry_type = None
    timezone = ""
    locations = set()  # NEW: to populate "Location(s) of Operation)"

    # -------- helpers --------
    def _clean_linkedin(u: Optional[str]) -> Optional[str]:
        if not u:
            return None
        u = u.split("?", 1)[0].rstrip("/")
        if u.endswith("/posts"):
            u = u[:-6].rstrip("/")
        u = _strip_trailing_dash(u)  # NEW: remove trailing "-"
        return u

    def _classify_industry(category: str, schema: str) -> Tuple[str, str]:
        c = (category or "").lower()
        s = (schema or "").lower()
        text = f"{c} {s}"
        if any(x in text for x in ["hotel", "resort", "lodging", "accommodation", "apartment", "stay", "hostel"]):
            if "hotel" in text:  return "Hospitality", "Hotel"
            if "resort" in text: return "Hospitality", "Resort"
            if any(x in text for x in ["apartment","accommodation","stay","hostel"]):
                return "Hospitality", "Accommodation"
            return "Hospitality", ""
        if any(x in text for x in ["software", "it", "technology", "saas", "ai", "data"]):
            return "Software/IT", ""
        if any(x in text for x in ["restaurant", "cafe", "bar"]):
            return "Food & Beverage", "Restaurant" if "restaurant" in text else ""
        return "", ""

    # -------- scraped website signals --------
    for raw in (scraped_rows or []):
        row = raw.get("pageFunctionResult") if isinstance(raw, dict) else None
        if not row:
            row = raw if isinstance(raw, dict) else {}

        for e in row.get("emails") or []:
            all_emails.add(e)
        all_phones.extend(row.get("phones") or [])
        all_phones.extend(row.get("structuredTelephones") or [])
        for l in row.get("linkedins") or []:
            cl = _clean_linkedin(l)
            if cl:
                all_linkedins.append(cl)

        if row.get("ratingValue") is not None and rating_value is None:
            rating_value = row.get("ratingValue")
        if row.get("reviewCount") is not None and review_count is None:
            review_count = row.get("reviewCount")

        if not company:
            company = guess_company_name(row.get("siteName"), row.get("title"))

        addr = row.get("address") or {}
        if not city and addr.get("city"):
            city = addr.get("city")
        if not country and addr.get("country"):
            country = addr.get("country")

        # NEW: collect location strings like "City, Region/Country"
        loc_parts = [addr.get("city"), addr.get("region"), addr.get("country")]
        loc_str = ", ".join([p for p in loc_parts if p])
        if loc_str:
            locations.add(loc_str)

        stype = row.get("schemaType") or ""
        if isinstance(stype, str) and not industry_type:
            s = stype.lower()
            if "hotel" in s:       industry_type = "Hotel"
            elif "resort" in s:    industry_type = "Resort"
            elif "organization" in s: industry_type = "Organization"

    # -------- Google Maps enrichment --------
    if maps_place:
        if not company:
            company = maps_place.get("name") or company

        maps_phone = maps_place.get("phone") or maps_place.get("internationalPhoneNumber")
        if maps_phone:
            all_phones.append(maps_phone)

        if maps_place.get("website"):
            official = maps_place["website"]

        if maps_place.get("rating") is not None:
            rating_value = maps_place["rating"]
        if maps_place.get("userRatingsTotal") is not None:
            review_count = maps_place["userRatingsTotal"]

        if maps_place.get("city") and not city:
            city = maps_place["city"]
        if maps_place.get("country") and not country:
            country = maps_place["country"]

        addr = maps_place.get("address") or {}
        if isinstance(addr, dict):
            if not city and addr.get("city"):
                city = addr.get("city")
            if not country and (addr.get("country") or addr.get("countryCode")):
                country = addr.get("country") or addr.get("countryCode")

            # NEW: add a location string from Maps address
            loc_parts = [addr.get("city"), addr.get("region"), addr.get("country") or addr.get("countryCode")]
            loc_str = ", ".join([p for p in loc_parts if p])
            if loc_str:
                locations.add(loc_str)

        # timezone from lat/lng (silent fallback if libs missing)
        try:
            loc = (maps_place.get("_raw") or {}).get("location") or {}
            lat, lng = loc.get("lat"), loc.get("lng")
            if isinstance(lat, (int, float)) and isinstance(lng, (int, float)):
                try:
                    from timezonefinder import TimezoneFinder  # optional dependency
                    _tf = TimezoneFinder()
                    tzname = _tf.timezone_at(lng=lng, lat=lat)
                    if tzname:
                        timezone = tzname
                except Exception:
                    pass
        except Exception:
            pass

    # -------- apply filters / pick best --------
    emails = _filter_emails_by_domain(sorted(all_emails), official)
    phones = _merge_and_clean_phones(all_phones)

    linkedin_candidates = []
    if linkedin_url:
        cleaned = _clean_linkedin(linkedin_url)
        if cleaned:
            linkedin_candidates.append(cleaned)
    linkedin_candidates.extend([u for u in list(dict.fromkeys(all_linkedins)) if u])
    linkedin = _pick_linkedin(linkedin_candidates)

    # Industry segment/type
    maps_category = ((maps_place or {}).get("_raw") or {}).get("categoryName") or ""
    seg_from_cat, type_from_cat = _classify_industry(maps_category, industry_type or "")
    industry_segment = seg_from_cat or ""
    if type_from_cat and not industry_type:
        industry_type = type_from_cat

    # Expand country code (e.g., "IN" -> "India")
    country_expanded = _expand_country(country or "")

    # Compose a readable locations string (dedup, keep order)
    locations_str = " | ".join(list(dict.fromkeys(locations))) if locations else ""

    # -------- build output --------
    out = {
        "Lead Name": query,
        "Designation / Role": "",
        "Company Name": (company or "").strip(),
        "Country / City": ", ".join([v for v in [country_expanded, city] if v]) if (country_expanded or city) else "",
        "Industry Segment": industry_segment,
        "Property Type (Chain / Independent / Partner)": "",
        "Star Rating (1–5)": str(rating_value) if rating_value is not None else "",
        "Email ID": emails[0] if emails else "",
        "Phone (if verified)": phones[0] if phones else "",
        "LinkedIn Profile URL": linkedin or "",
        "Department / Function": "",
        "Time Zone": timezone,
        "Company Name (linked)": (company or "").strip(),
        "Website URL": official or "",
        "Number of Properties": "",
        "Number of Rooms": "",
        "Average Daily Rate (ADR)": "",
        "Location(s) of Operation": locations_str,  # NEW
        "Industry Type (Hotel / Resort / Service Apartment, etc.)": industry_type or "",
        "Google Rating": str(rating_value) if rating_value is not None else "",
        "Total Google Reviews": str(review_count) if review_count is not None else "",
    }

    # Optional: mark hotel-only fields as N/A for non-hospitality
    # if industry_segment != "Hospitality":
    #     for k in ["Property Type (Chain / Independent / Partner)",
    #               "Star Rating (1–5)", "Number of Properties",
    #               "Number of Rooms", "Average Daily Rate (ADR)"]:
    #         out[k] = "N/A"

    return out
