from __future__ import annotations
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urlparse
import re

# Optional: timezone from lat/lng if present in Maps output
try:
    from timezonefinder import TimezoneFinder
    _TF = TimezoneFinder()
except Exception:
    _TF = None

# -----------------------------
# Small utilities
# -----------------------------
def _first_or_none(seq: Optional[List[str]]) -> Optional[str]:
    return seq[0] if seq else None


def guess_company_name(site_name: Optional[str], title: Optional[str]) -> Optional[str]:
    """
    Returns a neat brand name from site_name/title, stripping boilerplate like
    ': Home', '- Home', '| Official Site', etc.
    """
    for cand in (site_name, title):
        if not cand:
            continue
        s = cand.strip()
        s = re.sub(r"\s*[:\-\|–—]\s*home\s*$", "", s, flags=re.I)
        s = re.sub(r"\s*[:\-\|–—]\s*(about( us)?|official site|welcome)\s*$", "", s, flags=re.I)
        s = re.sub(r"\s*\|\s*.*$", "", s).strip()
        s = re.sub(r"\s{2,}", " ", s).strip()
        if s:
            return s
    return site_name or title


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
    bad_hosts = ("linkedin.com", "facebook.com", "instagram.com",
                 "tripadvisor.", "booking.", "google.com", "maps.google")
    filtered = [r for r in google_results if not any(b in (r.get("url") or "") for b in bad_hosts)]
    return (filtered[0].get("url") if filtered else google_results[0].get("url"))


def _root_token(host: Optional[str]) -> Optional[str]:
    if not host:
        return None
    parts = host.lower().split(".")
    return parts[-2] if len(parts) >= 2 else parts[0]


def _registrable_domain(host: str) -> str:
    """
    Naive eTLD+1 (good enough for most cases, avoids extra deps).
    """
    host = (host or "").lower().split(":")[0]
    parts = host.split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else host


def _filter_emails_by_domain(emails: List[str], official_url: Optional[str]) -> List[str]:
    """
    Prefer emails that match the site's registrable domain (e.g., foo@zapcom.ai for zapcom.ai).
    Falls back to brand-token containment if strict match yields none.
    """
    if not emails:
        return []
    if not official_url:
        return sorted(set(emails))

    host = urlparse(official_url).netloc
    registrable = _registrable_domain(host)
    strict = [e for e in set(emails) if e.lower().endswith("@" + registrable)]
    if strict:
        return sorted(set(strict))

    token = _root_token(host) or ""
    keep2 = []
    for e in set(emails):
        try:
            edomain = e.split("@", 1)[1].lower()
        except Exception:
            continue
        if token and token in edomain:
            keep2.append(e)
    return sorted(set(keep2))


def _clean_phone(p: str) -> Optional[str]:
    # keep digits and +; collapse spaces/dashes
    digits = re.sub(r"[^\d+]", "", p or "")
    # typical E.164 range (8..15 digits)
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


# ============================
# NEW: Normalizer for leads API
# ============================
def normalize_items(items: List[Dict[str, Any]], filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Convert raw Apify items (from Sales Navigator-style actor) to a stable schema
    and enforce filters defensively.

    Output keys (stable):
      company_name, company_size, country, city, website, linkedin_url,
      role, person_name, person_email, person_linkedin, source
    """
    out: List[Dict[str, Any]] = []
    size_min = int(filters.get("company_size_min", 1))
    size_max = int(filters.get("company_size_max", 10_000_000))
    countries = set(filters.get("countries", []))

    for it in items or []:
        row = {
            "company_name": it.get("companyName") or it.get("name") or it.get("Company Name") or "",
            "company_size": it.get("companySize") or it.get("Employees") or None,
            "country": it.get("country") or it.get("Country") or None,
            "city": it.get("city") or it.get("City") or None,
            "website": it.get("website") or it.get("Website") or None,
            "linkedin_url": it.get("companyLinkedinUrl") or it.get("linkedin") or it.get("LinkedIn URL") or None,
            "role": it.get("role") or it.get("title") or None,
            "person_name": it.get("personName") or it.get("contactName") or None,
            "person_email": it.get("email") or it.get("personEmail") or None,
            "person_linkedin": it.get("personLinkedin") or it.get("contactLinkedinUrl") or None,
            "source": "apify",
        }

        # Enforce filters (best-effort parsing of size)
        try:
            size_val = int(row["company_size"]) if row["company_size"] is not None else None
        except Exception:
            size_val = None

        if size_val is not None and not (size_min <= size_val <= size_max):
            continue
        if countries and row["country"] and row["country"] not in countries:
            continue

        out.append(row)
    return out


# ==============================================
# Legacy: Build a single enriched record (old UX)
# ==============================================
def assemble_lead_record(
    query: str,
    google_results: List[Dict[str, Any]],
    scraped_rows: List[Dict[str, Any]],
    maps_place: Optional[Dict[str, Any]] = None,
    linkedin_url: Optional[str] = None
) -> Dict[str, Any]:
    """
    Merges Google+Scraper+Maps into one human-friendly record
    (your old /api/run flow).
    """
    official = pick_official_site(google_results)
    domain = urlparse(official).netloc if official else None

    all_emails, all_phones, all_linkedins = set(), [], []
    rating_value, review_count = None, None
    company, city, country = None, None, None
    industry_type = None
    timezone = ""
    locations = set()  # to populate "Location(s) of Operation)"

    def _clean_linkedin(u: Optional[str]) -> Optional[str]:
        if not u:
            return None
        u = u.split("?", 1)[0].rstrip("/")
        if u.endswith("/posts"):
            u = u[:-6].rstrip("/")
        return _strip_trailing_dash(u)

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

        # Collect location strings like "City, Region/Country"
        loc_parts = [addr.get("city"), addr.get("region"), addr.get("country")]
        loc_str = ", ".join([p for p in loc_parts if p])
        if loc_str:
            locations.add(loc_str)

        stype = row.get("schemaType") or ""
        if isinstance(stype, str) and not industry_type:
            s = stype.lower()
            if   "hotel"  in s: industry_type = "Hotel"
            elif "resort" in s: industry_type = "Resort"
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

            loc_parts = [addr.get("city"), addr.get("region"), addr.get("country") or addr.get("countryCode")]
            loc_str = ", ".join([p for p in loc_parts if p])
            if loc_str:
                locations.add(loc_str)

        # timezone from lat/lng (best-effort)
        try:
            loc = (maps_place.get("_raw") or {}).get("location") or {}
            lat, lng = loc.get("lat"), loc.get("lng")
            if _TF and isinstance(lat, (int, float)) and isinstance(lng, (int, float)):
                tzname = _TF.timezone_at(lng=lng, lat=lat)
                if tzname:
                    timezone = tzname
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

    country_expanded = _expand_country(country or "")
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
        "Location(s) of Operation": locations_str,
        "Industry Type (Hotel / Resort / Service Apartment, etc.)": industry_type or "",
        "Google Rating": str(rating_value) if rating_value is not None else "",
        "Total Google Reviews": str(review_count) if review_count is not None else "",
    }
    return out
