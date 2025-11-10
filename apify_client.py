import os
import time
import json
import requests
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

load_dotenv()

# --- Env / constants ---
APIFY_TOKEN = os.getenv("APIFY_TOKEN")

# Existing actors
GOOGLE_ACTOR_ID = os.getenv("GOOGLE_ACTOR_ID", "apify~google-search-scraper")
WEB_SCRAPER_ACTOR_ID = os.getenv("WEB_SCRAPER_ACTOR_ID", "apify~web-scraper")
GOOGLE_MAPS_ACTOR_ID = os.getenv("GOOGLE_MAPS_ACTOR_ID", "compass~crawler-google-places")

# Sales Navigator-style actor (both spellings supported)
SALES_NAV_ACTOR_ID = os.getenv("SALES_NAV_ACTOR_ID", "muhammad_usama~Apify-Sales-Navifgator")
FALLBACK_SALES_NAV_ACTOR_ID = "muhammad_usama~Apify-Sales-Navigator"

# Sales Navigator auth + behavior (read *only* for convenience; we also read dynamically inside the call)
SALES_NAV_COOKIE_STRING = os.getenv("SALES_NAV_COOKIE_STRING")  # e.g. 'li_at=...; JSESSIONID="ajax:..."; ...'
SALES_NAV_COOKIES_JSON = os.getenv("SALES_NAV_COOKIES_JSON")    # JSON array of cookie objects

API_BASE = "https://api.apify.com/v2"


class ApifyError(Exception):
    pass


def _ensure_token(token: Optional[str] = None) -> str:
    tok = token or APIFY_TOKEN
    if not tok:
        raise ApifyError("APIFY_TOKEN missing. Set it in .env")
    return tok


# ---------------------------
# Generic run helpers
# ---------------------------
def start_actor(actor_id: str, input_body: Dict[str, Any], token: Optional[str] = None) -> Dict[str, Any]:
    tok = _ensure_token(token)
    url = f"{API_BASE}/acts/{actor_id}/runs?token={tok}"
    resp = requests.post(url, json=input_body, timeout=90)
    if resp.status_code >= 400:
        raise ApifyError(f"Failed to start actor {actor_id}: {resp.text}")
    return resp.json().get("data", {})


def get_run(run_id: str, token: Optional[str] = None) -> Dict[str, Any]:
    tok = _ensure_token(token)
    url = f"{API_BASE}/actor-runs/{run_id}?token={tok}"
    resp = requests.get(url, timeout=60)
    if resp.status_code >= 400:
        raise ApifyError(f"Failed to get run {run_id}: {resp.text}")
    return resp.json().get("data", {})


def wait_for_run_finished(
    run_id: str,
    timeout_sec: int = 300,
    poll_interval: float = 2.5,
    token: Optional[str] = None,
) -> Dict[str, Any]:
    start = time.time()
    terminal = {"SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT", "TIMED_OUT"}
    while True:
        run = get_run(run_id, token=token)
        status = (run or {}).get("status")
        if status in terminal:
            return run
        if (time.time() - start) > timeout_sec:
            raise ApifyError(f"Run {run_id} timed out. Last status: {status}")
        time.sleep(poll_interval)


def dataset_items(dataset_id: str, clean: bool = True, limit: Optional[int] = None, token: Optional[str] = None) -> List[Dict[str, Any]]:
    tok = _ensure_token(token)
    params = {"clean": "true" if clean else "false", "format": "json", "token": tok}
    if limit:
        params["limit"] = str(limit)
    url = f"{API_BASE}/datasets/{dataset_id}/items"
    resp = requests.get(url, params=params, timeout=120)
    if resp.status_code >= 400:
        raise ApifyError(f"Failed to fetch dataset {dataset_id}: {resp.text}")
    return resp.json()


# ---------------------------
# Google Search
# ---------------------------
def google_search(query: str, max_results: int = 5) -> List[Dict[str, Any]]:
    results_per_page = max(1, min(max_results, 10))
    input_body = {
        "queries": query,
        "maxPagesPerQuery": 1,
        "resultsPerPage": results_per_page,
        "includeUnfilteredResults": False,
    }
    try:
        run = start_actor(GOOGLE_ACTOR_ID, input_body)
    except ApifyError:
        fallback_body = {
            "query": query,
            "maxPagesPerQuery": 1,
            "resultsPerPage": results_per_page,
            "includeUnfilteredResults": False,
        }
        run = start_actor(GOOGLE_ACTOR_ID, fallback_body)

    run = wait_for_run_finished(run["id"], timeout_sec=120)
    if run.get("status") != "SUCCEEDED":
        raise ApifyError(f"Google search run failed: status={run.get('status')}")

    ds_id = run.get("defaultDatasetId")
    items = dataset_items(ds_id, clean=True)

    results: List[Dict[str, Any]] = []
    for it in items:
        organic = it.get("organicResults")
        if isinstance(organic, list) and organic:
            for r in organic:
                u = r.get("url")
                if u:
                    results.append({
                        "url": u,
                        "title": r.get("title"),
                        "snippet": r.get("snippet"),
                        "siteLinks": r.get("sitelinks"),
                    })
            continue

        u = it.get("url")
        if u:
            results.append({
                "url": u,
                "title": it.get("title"),
                "snippet": it.get("snippet") or it.get("description"),
                "siteLinks": it.get("sitelinks") or it.get("siteLinks"),
            })

    return results[:max_results]


# ---------------------------
# Web Scraper
# ---------------------------
def _default_page_function() -> str:
    return r"""
    async function pageFunction(context) {
      const uniq = (arr) => Array.from(new Set((arr || []).filter(Boolean)));
      const metaContent = (sel) => {
        const el = document.querySelector(sel);
        return el ? (el.content || el.getAttribute('content') || '').trim() : '';
      };
      const safeText = (el) => (el ? (el.textContent || '').trim() : '');

      let bodyText = '';
      try { bodyText = document.body ? (document.body.innerText || '') : ''; } catch (e) {}

      const plainEmails = (bodyText.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi) || []).map(s => s.trim());

      const obfus = [];
      const lowered = (bodyText || '').toLowerCase();
      const obfusRe = /([a-z0-9._%+-]+)\s*(\(|\[)?\s*at\s*(\)|\])?\s*([a-z0-9.-]+)\s*(\(|\[)?\s*dot\s*(\)|\])?\s*([a-z]{2,})/gi;
      let m;
      while ((m = obfusRe.exec(lowered)) !== null) {
        const email = `${m[1]}@${m[4]}.${m[7]}`;
        obfus.push(email);
      }

      const emailsFromHref = [];
      const phones = [];
      try {
        for (const a of Array.from(document.querySelectorAll('a[href]'))) {
          const href = (a.getAttribute('href') || '').trim();
          if (/^mailto:/i.test(href)) {
            const m = href.replace(/^mailto:/i, '').split('?')[0];
            if (m) emailsFromHref.push(m);
          }
          if (/^tel:/i.test(href)) {
            const t = href.replace(/^tel:/i, '');
            if (t) phones.push(t);
          }
        }
      } catch (_) {}

      function cfDecode(cfhex) {
        try {
          const r = parseInt(cfhex.substr(0, 2), 16);
          let email = '';
          for (let n = 2; n < cfhex.length; n += 2) {
            const charCode = parseInt(cfhex.substr(n, 2), 16) ^ r;
            email += String.fromCharCode(charCode);
          }
          return email;
        } catch (e) { return null; }
      }
      const cfEmails = [];
      try {
        for (const el of Array.from(document.querySelectorAll('[data-cfemail]'))) {
          const hex = el.getAttribute('data-cfemail');
          const dec = hex ? cfDecode(hex) : null;
          if (dec) cfEmails.push(dec);
        }
        const html = document.documentElement ? (document.documentElement.innerHTML || '') : '';
        const cfRe = /data-cfemail="([0-9a-fA-F]+)"/g;
        let mm;
        while ((mm = cfRe.exec(html)) !== null) {
          const dec = cfDecode(mm[1]);
          if (dec) cfEmails.push(dec);
        }
      } catch (_) {}

      let linkedins = [];
      try {
        linkedins = Array.from(document.querySelectorAll('a[href*="linkedin.com"]')).map(a => a.href);
      } catch (_) {}

      const title = safeText(document.querySelector('title'));
      const siteName = metaContent('meta[property="og:site_name"]') || metaContent('meta[property="og:title"]') || title;

      let ratingValue = null, reviewCount = null, address = null, schemaType = null, structuredTelephones = [];
      try {
        const scripts = Array.from(document.querySelectorAll('script[type="application/ld+json"]'));
        for (const s of scripts) {
          const txt = s.textContent || s.innerText || '';
          if (!txt) continue;
          try {
            const json = JSON.parse(txt);
            const arr = Array.isArray(json) ? json : [json];
            for (const obj of arr) {
              const t = obj['@type'];
              if (!schemaType && t) schemaType = Array.isArray(t) ? t.join(',') : t;
              if (obj.aggregateRating) {
                if (obj.aggregateRating.ratingValue && !ratingValue) ratingValue = obj.aggregateRating.ratingValue;
                if (obj.aggregateRating.reviewCount && !reviewCount) reviewCount = obj.aggregateRating.reviewCount;
              }
              if (obj.address && !address) {
                const a = obj.address;
                address = {
                  city: a.addressLocality || null,
                  region: a.addressRegion || null,
                  country: a.addressCountry || null,
                };
              }
              if (obj.telephone) {
                const tel = Array.isArray(obj.telephone) ? obj.telephone : [obj.telephone];
                structuredTelephones.push(...tel.map(String));
              }
              if (obj.sameAs) {
                const arrSame = Array.isArray(obj.sameAs) ? obj.sameAs : [obj.sameAs];
                linkedins.push(...arrSame.filter(u => typeof u === 'string' && u.includes('linkedin.com')));
              }
            }
          } catch (_) {}
        }
      } catch (_) {}

      const emails = uniq([].concat(plainEmails, obfus, cfEmails, emailsFromHref));

      return {
        pageUrl: location.href,
        siteName,
        title,
        emails,
        phones: uniq(phones),
        linkedins: uniq(linkedins),
        ratingValue,
        reviewCount,
        address,
        schemaType: schemaType || null,
        structuredTelephones: uniq(structuredTelephones),
      };
    }
    """


def web_scrape(urls: List[str], max_pages: int = 10) -> List[Dict[str, Any]]:
    if not urls:
        return []
    start_urls = [{"url": u} for u in urls]
    input_body = {
        "startUrls": start_urls,
        "maxRequestsPerCrawl": max_pages,
        "maxConcurrency": 1,
        "pageFunction": _default_page_function(),
        "useChrome": True,
        "ignoreSslErrors": True,
        "downloadMedia": False,
        "downloadCss": False,
        "downloadJavascript": False,
        "maxRequestRetries": 1,
        "requestHandlerTimeoutSecs": 60,
    }
    run = start_actor(WEB_SCRAPER_ACTOR_ID, input_body)
    run = wait_for_run_finished(run["id"], timeout_sec=240)
    if run.get("status") != "SUCCEEDED":
        raise ApifyError(f"Web-scraper run failed: status={run.get('status')}")
    ds_id = run.get("defaultDatasetId")
    return dataset_items(ds_id, clean=True)


# ---------------------------
# Google Maps enrichment
# ---------------------------
def google_maps_enrich(query: str) -> Dict[str, Any]:
    input_body = {
        "searchStringsArray": [query],
        "maxCrawledPlacesPerSearch": 1,
        "language": "en",
        "maxReviews": 0,
        "maxImages": 0,
    }
    try:
        run = start_actor(GOOGLE_MAPS_ACTOR_ID, input_body)
    except ApifyError:
        fallback = {
            "searchString": query,
            "maxCrawledPlacesPerSearch": 1,
            "language": "en",
            "maxReviews": 0,
            "maxImages": 0,
        }
        run = start_actor(GOOGLE_MAPS_ACTOR_ID, fallback)

    run = wait_for_run_finished(run["id"], timeout_sec=180)
    if run.get("status") != "SUCCEEDED":
        return {}

    ds_id = run.get("defaultDatasetId")
    items = dataset_items(ds_id, clean=True) or []
    if not items:
        return {}

    raw = items[0]
    return {
        "name": raw.get("title") or raw.get("name"),
        "website": raw.get("website"),
        "phone": raw.get("phone"),
        "internationalPhoneNumber": raw.get("phoneUnformatted") or raw.get("internationalPhoneNumber"),
        "rating": raw.get("rating") if raw.get("rating") is not None else raw.get("totalScore"),
        "userRatingsTotal": raw.get("userRatingsTotal") if raw.get("userRatingsTotal") is not None else raw.get("reviewsCount"),
        "city": raw.get("city"),
        "country": raw.get("country") or raw.get("countryCode"),
        "address": {
            "street": raw.get("street"),
            "city": raw.get("city"),
            "region": raw.get("state"),
            "postal": raw.get("postalCode"),
            "country": raw.get("country") or raw.get("countryCode"),
        },
        "_raw": raw,
    }

from urllib.parse import quote

def build_sales_nav_company_url(industry: str, size_min: int, size_max: int, geo_ids: list[str]) -> str:
    """
    Builds a Sales Navigator company search URL:
    - keywords: industry intent (e.g., hotel/resort/serviced apartment)
    - companyHeadcountRanges: 50..5000
    - geoIncluded: list of LinkedIn geo URNs (numbers as strings)
    """
    # Keywords: tune as you like
    keywords_expr = '"hotel" OR "resort" OR "serviced apartment" OR "hospitality"'
    if industry and industry.lower() not in keywords_expr.lower():
        keywords_expr = f'{keywords_expr} OR "{industry}"'

    # Encode pieces
    kw = quote(keywords_expr, safe="")
    headcount = f"List((start:{size_min},end:{size_max}))"
    geos = "List(" + ",".join(geo_ids) + ")"

    q = f"(keywords:{kw},companyHeadcountRanges:{headcount},geoIncluded:{geos})"
    return "https://www.linkedin.com/sales/search/company?query=" + quote(q, safe="(),:")


# =====================================================================
# Sales Navigator actor wrapper (filters -> companies/contacts)
# =====================================================================
def _load_sales_nav_cookies_from_env() -> Any:
    """
    Load cookies for LinkedIn Sales Navigator from env.
    Prefer SALES_NAV_COOKIES_JSON (JSON array of cookie objects).
    Fallback to SALES_NAV_COOKIE_STRING (raw Cookie header string).
    """
    json_env = os.getenv("SALES_NAV_COOKIES_JSON", SALES_NAV_COOKIES_JSON) or ""
    str_env = os.getenv("SALES_NAV_COOKIE_STRING", SALES_NAV_COOKIE_STRING) or ""

    if json_env:
        try:
            parsed = json.loads(json_env)
        except Exception:
            raise ApifyError("SALES_NAV_COOKIES_JSON is not valid JSON.")
        return parsed
    if str_env:
        return str_env.strip()
    return None


def _build_payload_variants(filters: Dict[str, Any], mode: str, cookies_payload: Any, search_url: str) -> List[Dict[str, Any]]:
    """
    Build payloads for multiple schemas the actor family uses:
    - With "body": {...}
    - Plain top-level keys (no wrapper)
    - LinkedIn-ish synonyms
    - URL modes (only if search_url provided)
    """
    size_min = int(filters.get("company_size_min") or 1)
    size_max = int(filters.get("company_size_max") or 10_000_000)
    industry = (filters.get("industry_focus") or "").strip()
    countries = list(filters.get("countries") or [])
    roles = list(filters.get("roles") or [])

    variants: List[Dict[str, Any]] = []

    # ---------- A) With "body" wrapper (your current shape) ----------
    variants.append({
        "body": {
            "mode": mode,
            "filters": {
                "industry": industry,
                "companySize": {"min": size_min, "max": size_max},
                "countries": countries,
                "roles": roles,
            },
            "includeContacts": True,
            "deduplicate": True,
            "cookies": cookies_payload,
        }
    })

    variants.append({
        "body": {
            "mode": mode,
            "industry": industry,
            "companySize": {"min": size_min, "max": size_max},
            "countries": countries,
            "roles": roles,
            "includeContacts": True,
            "deduplicate": True,
            "cookies": cookies_payload,
        }
    })

    variants.append({
        "body": {
            "mode": mode,
            "industries": [industry] if industry else [],
            "companySizeRange": {"minEmployees": size_min, "maxEmployees": size_max},
            "geos": countries,
            "titles": roles,
            "includeContacts": True,
            "deduplicate": True,
            "cookies": cookies_payload,
        }
    })

    if "-via-url" in mode.lower() and search_url:
        variants.append({
            "body": {
                "mode": mode,
                "search_url": search_url,
                "page": int(os.getenv("SALES_NAV_PAGE", "1")),
                "cookies": cookies_payload,
            }
        })

    # ---------- B) PLAIN top-level (NO "body" wrapper) ----------
    variants.append({
        "mode": mode,
        "filters": {
            "industry": industry,
            "companySize": {"min": size_min, "max": size_max},
            "countries": countries,
            "roles": roles,
        },
        "includeContacts": True,
        "deduplicate": True,
        "cookies": cookies_payload,
    })

    variants.append({
        "mode": mode,
        "industry": industry,
        "companySize": {"min": size_min, "max": size_max},
        "countries": countries,
        "roles": roles,
        "includeContacts": True,
        "deduplicate": True,
        "cookies": cookies_payload,
    })

    variants.append({
        "mode": mode,
        "industries": [industry] if industry else [],
        "companySizeRange": {"minEmployees": size_min, "maxEmployees": size_max},
        "geos": countries,
        "titles": roles,
        "includeContacts": True,
        "deduplicate": True,
        "cookies": cookies_payload,
    })

    if "-via-url" in mode.lower() and search_url:
        variants.append({
            "mode": mode,
            "search_url": search_url,
            "page": int(os.getenv("SALES_NAV_PAGE", "1")),
            "cookies": cookies_payload,
        })

    return variants

def call_apify_actor(filters: Dict[str, Any], apify_token: Optional[str] = None) -> List[Dict[str, Any]]:
    tok = _ensure_token(apify_token)

    # Force URL mode for now
    mode = "search-leads-via-url"  # or "search-companies-via-url" if you want company results
    page = int(os.getenv("SALES_NAV_PAGE", "1"))

    # Try to read a supplied URL
    search_url = (os.getenv("SALES_NAV_SEARCH_URL", "") or "").strip()

    # Basic validation: must be Sales Navigator and contain 'query='
    def _looks_valid(u: str) -> bool:
        return u.startswith("https://www.linkedin.com/sales/search/") and "query=" in u

    # If URL missing/invalid, auto-build a Companies URL from filters (then still call URL mode)
    if not _looks_valid(search_url):
        size_min = int(filters.get("company_size_min") or 50)
        size_max = int(filters.get("company_size_max") or 5000)
        industry = (filters.get("industry_focus") or "Hospitality").strip()
        countries = list(filters.get("countries") or [])
        # You need to map countries → geo IDs; put your IDs here:
        GEO_MAP = {
            "United Kingdom": "101165590",
            "Italy": "103350119",
            "Spain": "105646813",
            "Germany": "101282230",
            "Switzerland": "106693272",
        }
        geo_ids = [GEO_MAP[c] for c in countries if c in GEO_MAP]
        if not geo_ids:
            raise ApifyError("No geo IDs derived from countries. Please select at least one supported country.")
        search_url = build_sales_nav_company_url(industry, size_min, size_max, geo_ids)
        # And use companies URL mode if we built a company URL:
        mode = "search-companies-via-url"

    cookies_payload = _load_sales_nav_cookies_from_env()
    if not cookies_payload:
        raise ApifyError("Sales Navigator cookies missing. Set SALES_NAV_COOKIES_JSON or SALES_NAV_COOKIE_STRING in .env.")

    # The actor build you’re hitting expects top-level `"url"` and `"page"`
    payload_plain = {
        "mode": mode,
        "url": search_url,
        "page": page,
        "cookies": cookies_payload,
        "includeContacts": True,
        "deduplicate": True,
    }
    payload_wrapped = {"body": dict(payload_plain)}  # fallback variant

    last_err = None
    for pv in (payload_plain, payload_wrapped):
        try:
            try:
                run = start_actor(SALES_NAV_ACTOR_ID, pv, token=tok)
            except ApifyError as e:
                if "not found" in str(e).lower() or "invalid act id" in str(e).lower():
                    run = start_actor(FALLBACK_SALES_NAV_ACTOR_ID, pv, token=tok)
                else:
                    raise
            run = wait_for_run_finished(run["id"], timeout_sec=300, token=tok)
            if run.get("status") != "SUCCEEDED":
                last_err = ApifyError(f"Run status {run.get('status')}")
                continue
            dataset_id = run.get("defaultDatasetId")
            if not dataset_id:
                last_err = ApifyError("No defaultDatasetId in actor run response.")
                continue
            return dataset_items(dataset_id, clean=True, token=tok)
        except ApifyError as e:
            last_err = e
            continue

    raise ApifyError(f"Lead actor failed in URL mode. Last error: {last_err}")


# ---------------------------
# Optional: Local mock data
# ---------------------------
def mock_results(filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    companies = [
        {"companyName": "Premier Inns UK", "companySize": 3500, "country": "United Kingdom", "city": "London", "website": "https://www.premierinn.com", "companyLinkedinUrl": "https://www.linkedin.com/company/premier-inn/"},
        {"companyName": "Meliá Hotels International", "companySize": 4500, "country": "Spain", "city": "Palma", "website": "https://www.melia.com", "companyLinkedinUrl": "https://www.linkedin.com/company/meliahotelsinternational/"},
        {"companyName": "Deutsche Hospitality", "companySize": 1200, "country": "Germany", "city": "Frankfurt", "website": "https://www.deutschehospitality.com", "companyLinkedinUrl": "https://www.linkedin.com/company/deutsche-hospitality/"},
        {"companyName": "Tivoli Hotels & Resorts", "companySize": 900, "country": "Portugal", "city": "Lisbon", "website": "https://www.tivolihotels.com", "companyLinkedinUrl": "https://www.linkedin.com/company/tivoli-hotels-resorts/"},
        {"companyName": "B&B HOTELS", "companySize": 3000, "country": "Italy", "city": "Milan", "website": "https://www.hotel-bb.com", "companyLinkedinUrl": "https://www.linkedin.com/company/b-b-hotels/"},
    ]
    out: List[Dict[str, Any]] = []
    size_min = int(filters.get("company_size_min", 1))
    size_max = int(filters.get("company_size_max", 10_000_000))
    countries = set(filters.get("countries", []))
    roles = filters.get("roles", ["CEO"])
    for c in companies:
        if countries and c.get("country") not in countries:
            continue
        size = int(c.get("companySize") or 0)
        if not (size_min <= size <= size_max):
            continue
        c2 = {**c}
        c2["role"] = roles[0] if roles else None
        out.append(c2)
    return out
