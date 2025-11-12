# import os
# import time
# import requests
# from typing import Dict, Any, List, Optional
# from dotenv import load_dotenv

# load_dotenv()

# APIFY_TOKEN = os.getenv("APIFY_TOKEN")
# GOOGLE_ACTOR_ID = os.getenv("GOOGLE_ACTOR_ID", "apify~google-search-scraper")
# WEB_SCRAPER_ACTOR_ID = os.getenv("WEB_SCRAPER_ACTOR_ID", "apify~web-scraper")



# API_BASE = "https://api.apify.com/v2"


# class ApifyError(Exception):
#     pass


# def _ensure_token():
#     if not APIFY_TOKEN:
#         raise ApifyError("APIFY_TOKEN missing. Set it in .env")


# def start_actor(actor_id: str, input_body: Dict[str, Any]) -> Dict[str, Any]:
#     """Start an Apify actor run and return the run object."""
#     _ensure_token()
#     url = f"{API_BASE}/acts/{actor_id}/runs?token={APIFY_TOKEN}"
#     resp = requests.post(url, json=input_body, timeout=60)
#     if resp.status_code >= 400:
#         raise ApifyError(f"Failed to start actor {actor_id}: {resp.text}")
#     return resp.json().get("data", {})  # run object


# def get_run(run_id: str) -> Dict[str, Any]:
#     _ensure_token()
#     url = f"{API_BASE}/actor-runs/{run_id}"
#     resp = requests.get(url, timeout=60)
#     if resp.status_code >= 400:
#         raise ApifyError(f"Failed to get run {run_id}: {resp.text}")
#     return resp.json().get("data", {})


# def wait_for_run_finished(run_id: str, timeout_sec: int = 180, poll_interval: float = 2.5) -> Dict[str, Any]:
#     """Polls until run is FINISHED / FAILED / ABORTED or timeout."""
#     start = time.time()
#     while True:
#         run = get_run(run_id)
#         status = run.get("status")
#         if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
#             return run
#         if (time.time() - start) > timeout_sec:
#             raise ApifyError(f"Run {run_id} timed out. Last status: {status}")
#         time.sleep(poll_interval)


# def dataset_items(dataset_id: str, clean: bool = True, limit: Optional[int] = None) -> List[Dict[str, Any]]:
#     _ensure_token()
#     params = {"clean": "true" if clean else "false", "format": "json"}
#     if limit:
#         params["limit"] = str(limit)
#     url = f"{API_BASE}/datasets/{dataset_id}/items"
#     resp = requests.get(url, params=params, timeout=60)
#     if resp.status_code >= 400:
#         raise ApifyError(f"Failed to fetch dataset {dataset_id}: {resp.text}")
#     return resp.json()


# def google_search(query: str, max_results: int = 5) -> List[Dict[str, Any]]:
#     """
#     Uses apify/google-search-scraper to get organic results.
#     The actor variant in use expects `queries` as a *string* (newline-separated for many).
#     Returns list of {url, title, snippet, ...}
#     """
#     # Normalize limits
#     results_per_page = max(1, min(max_results, 10))

#     # Primary schema: queries as a string
#     input_body = {
#         "queries": query,                 # <-- IMPORTANT: string, not list
#         "maxPagesPerQuery": 1,
#         "resultsPerPage": results_per_page,
#         "includeUnfilteredResults": False,
#     }

#     try:
#         run = start_actor(GOOGLE_ACTOR_ID, input_body)
#     except ApifyError as e:
#         err = str(e)
#         # Fallback 1: some builds expect "query" instead of "queries"
#         if "queries must be string" in err or "invalid-input" in err:
#             fallback_body = {
#                 "query": query,
#                 "maxPagesPerQuery": 1,
#                 "resultsPerPage": results_per_page,
#                 "includeUnfilteredResults": False,
#             }
#             run = start_actor(GOOGLE_ACTOR_ID, fallback_body)
#         else:
#             raise

#     run = wait_for_run_finished(run["id"], timeout_sec=120)
#     if run.get("status") != "SUCCEEDED":
#         raise ApifyError(f"Google search run failed: status={run.get('status')}")

#     ds_id = run.get("defaultDatasetId")
#     items = dataset_items(ds_id, clean=True)

#     # Some versions return top-level items with url/title/snippet
#     # Others wrap results in item['organicResults']
#     results = []

#     for it in items:
#         # Preferred: organicResults array
#         organic = it.get("organicResults")
#         if isinstance(organic, list) and organic:
#             for r in organic:
#                 u = r.get("url")
#                 if u:
#                     results.append({
#                         "url": u,
#                         "title": r.get("title"),
#                         "snippet": r.get("snippet"),
#                         "siteLinks": r.get("sitelinks"),
#                     })
#             continue

#         # Fallback: flat item
#         u = it.get("url")
#         if u:
#             results.append({
#                 "url": u,
#                 "title": it.get("title"),
#                 "snippet": it.get("snippet") or it.get("description"),
#                 "siteLinks": it.get("sitelinks") or it.get("siteLinks"),
#             })

#     return results[:max_results]



# def _default_page_function() -> str:
#     return r"""
#     async function pageFunction(context) {
#       // Runs inside page context
#       const uniq = (arr) => Array.from(new Set((arr || []).filter(Boolean)));
#       const metaContent = (sel) => {
#         const el = document.querySelector(sel);
#         return el ? (el.content || el.getAttribute('content') || '').trim() : '';
#       };
#       const safeText = (el) => (el ? (el.textContent || '').trim() : '');

#       // --- 1) Body text (for plain emails + obfuscated "at/dot") ---
#       let bodyText = '';
#       try { bodyText = document.body ? (document.body.innerText || '') : ''; } catch (e) {}

#       const plainEmails = (bodyText.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi) || []).map(s => s.trim());

#       // Obfuscated forms: "name [at] domain [dot] com", "name (at) domain dot com"
#       const obfus = [];
#       const lowered = (bodyText || '').toLowerCase();
#       // capture sequences like: xxx at yyy dot tld
#       const obfusRe = /([a-z0-9._%+-]+)\s*(\(|\[)?\s*at\s*(\)|\])?\s*([a-z0-9.-]+)\s*(\(|\[)?\s*dot\s*(\)|\])?\s*([a-z]{2,})/gi;
#       let m;
#       while ((m = obfusRe.exec(lowered)) !== null) {
#         const email = `${m[1]}@${m[4]}.${m[7]}`;
#         obfus.push(email);
#       }

#       // --- 2) mailto: and tel: anchors ---
#       const emailsFromHref = [];
#       const phones = [];
#       try {
#         for (const a of Array.from(document.querySelectorAll('a[href]'))) {
#           const href = (a.getAttribute('href') || '').trim();
#           if (/^mailto:/i.test(href)) {
#             const m = href.replace(/^mailto:/i, '').split('?')[0];
#             if (m) emailsFromHref.push(m);
#           }
#           if (/^tel:/i.test(href)) {
#             const t = href.replace(/^tel:/i, '');
#             if (t) phones.push(t);
#           }
#         }
#       } catch (_) {}

#       // --- 3) Cloudflare-protected emails (data-cfemail) ---
#       // https://developers.cloudflare.com/fundamentals/reference/cf-email/
#       function cfDecode(cfhex) {
#         try {
#           const r = parseInt(cfhex.substr(0, 2), 16);
#           let email = '';
#           for (let n = 2; n < cfhex.length; n += 2) {
#             const charCode = parseInt(cfhex.substr(n, 2), 16) ^ r;
#             email += String.fromCharCode(charCode);
#           }
#           return email;
#         } catch (e) { return null; }
#       }
#       const cfEmails = [];
#       try {
#         for (const el of Array.from(document.querySelectorAll('[data-cfemail]'))) {
#           const hex = el.getAttribute('data-cfemail');
#           const dec = hex ? cfDecode(hex) : null;
#           if (dec) cfEmails.push(dec);
#         }
#         // Sometimes CF stores it in HTML comments or inline scripts, so scan HTML quickly
#         const html = document.documentElement ? (document.documentElement.innerHTML || '') : '';
#         const cfRe = /data-cfemail="([0-9a-fA-F]+)"/g;
#         let mm;
#         while ((mm = cfRe.exec(html)) !== null) {
#           const dec = cfDecode(mm[1]);
#           if (dec) cfEmails.push(dec);
#         }
#       } catch (_) {}

#       // --- 4) LinkedIn links ---
#       let linkedins = [];
#       try {
#         linkedins = Array.from(document.querySelectorAll('a[href*="linkedin.com"]')).map(a => a.href);
#       } catch (_) {}

#       // --- 5) Title/site name + JSON-LD (rating, reviews, address, type, telephone) ---
#       const title = safeText(document.querySelector('title'));
#       const siteName = metaContent('meta[property="og:site_name"]') || metaContent('meta[property="og:title"]') || title;

#       let ratingValue = null, reviewCount = null, address = null, schemaType = null, structuredTelephones = [];
#       try {
#         const scripts = Array.from(document.querySelectorAll('script[type="application/ld+json"]'));
#         for (const s of scripts) {
#           const txt = s.textContent || s.innerText || '';
#           if (!txt) continue;
#           try {
#             const json = JSON.parse(txt);
#             const arr = Array.isArray(json) ? json : [json];
#             for (const obj of arr) {
#               const t = obj['@type'];
#               if (!schemaType && t) schemaType = Array.isArray(t) ? t.join(',') : t;
#               if (obj.aggregateRating) {
#                 if (obj.aggregateRating.ratingValue && !ratingValue) ratingValue = obj.aggregateRating.ratingValue;
#                 if (obj.aggregateRating.reviewCount && !reviewCount) reviewCount = obj.aggregateRating.reviewCount;
#               }
#               if (obj.address && !address) {
#                 const a = obj.address;
#                 address = {
#                   city: a.addressLocality || null,
#                   region: a.addressRegion || null,
#                   country: a.addressCountry || null,
#                 };
#               }
#               if (obj.telephone) {
#                 const tel = Array.isArray(obj.telephone) ? obj.telephone : [obj.telephone];
#                 structuredTelephones.push(...tel.map(String));
#               }
#               if (obj.sameAs) {
#                 const arrSame = Array.isArray(obj.sameAs) ? obj.sameAs : [obj.sameAs];
#                 linkedins.push(...arrSame.filter(u => typeof u === 'string' && u.includes('linkedin.com')));
#               }
#             }
#           } catch (_) {}
#         }
#       } catch (_) {}

#       // Compose emails (plain + obfuscated + cloudflare + mailto)
#       const emails = uniq([].concat(plainEmails, obfus, cfEmails, emailsFromHref));

#       return {
#         pageUrl: location.href,
#         siteName,
#         title,
#         emails,
#         phones: uniq(phones),
#         linkedins: uniq(linkedins),
#         ratingValue,
#         reviewCount,
#         address,
#         schemaType: schemaType || null,
#         structuredTelephones: uniq(structuredTelephones),
#       };
#     }
#     """


# def web_scrape(urls: List[str], max_pages: int = 10) -> List[Dict[str, Any]]:
#     if not urls:
#         return []

#     start_urls = [{"url": u} for u in urls]

#     input_body = {
#         "startUrls": start_urls,
#         "maxRequestsPerCrawl": max_pages,
#         "maxConcurrency": 1,
#         "pageFunction": _default_page_function(),
#         "useChrome": True,              # PuppeteerCrawler (document is available)
#         "ignoreSslErrors": True,
#         "downloadMedia": False,
#         "downloadCss": False,
#         "downloadJavascript": False,
#         "maxRequestRetries": 1,
#         "requestHandlerTimeoutSecs": 60,
#         # no injectJQuery needed; we don’t use $
#     }

#     run = start_actor(WEB_SCRAPER_ACTOR_ID, input_body)
#     run = wait_for_run_finished(run["id"], timeout_sec=240)
#     if run.get("status") != "SUCCEEDED":
#         raise ApifyError(f"Web-scraper run failed: status={run.get('status')}")
#     ds_id = run.get("defaultDatasetId")
#     return dataset_items(ds_id, clean=True)  # will include pageFunctionResult

# def google_maps_enrich(query: str) -> Dict[str, Any]:
#     """
#     Uses Google Maps scraper and NORMALIZES fields so downstream code can rely on:
#       rating, userRatingsTotal, phone, internationalPhoneNumber, website, city, country, address
#     Works with Compass actor output (totalScore/reviewsCount, phoneUnformatted, countryCode, etc.)
#     """
#     actor_id = os.getenv("GOOGLE_MAPS_ACTOR_ID", "compass~crawler-google-places")

#     # Try newer "searchStringsArray" input; fall back to "searchString"
#     input_body = {
#         "searchStringsArray": [query],
#         "maxCrawledPlacesPerSearch": 1,
#         "language": "en",
#         "maxReviews": 0,
#         "maxImages": 0,
#     }
#     try:
#         run = start_actor(actor_id, input_body)
#     except ApifyError:
#         fallback = {
#             "searchString": query,
#             "maxCrawledPlacesPerSearch": 1,
#             "language": "en",
#             "maxReviews": 0,
#             "maxImages": 0,
#         }
#         run = start_actor(actor_id, fallback)

#     run = wait_for_run_finished(run["id"], timeout_sec=180)
#     if run.get("status") != "SUCCEEDED":
#         return {}

#     ds_id = run.get("defaultDatasetId")
#     items = dataset_items(ds_id, clean=True) or []
#     if not items:
#         return {}

#     raw = items[0]

#     # ---- Normalize to common keys used by assemble_lead_record ----
#     normalized = {
#         # name / website
#         "name": raw.get("title") or raw.get("name"),
#         "website": raw.get("website"),

#         # phones
#         "phone": raw.get("phone"),  # formatted (e.g., "+91 80 6723 2300")
#         "internationalPhoneNumber": raw.get("phoneUnformatted") or raw.get("internationalPhoneNumber"),

#         # rating + reviews (Compass uses totalScore/reviewsCount)
#         "rating": raw.get("rating") if raw.get("rating") is not None else raw.get("totalScore"),
#         "userRatingsTotal": raw.get("userRatingsTotal") if raw.get("userRatingsTotal") is not None else raw.get("reviewsCount"),

#         # location
#         "city": raw.get("city"),
#         # Compass gives ISO code; keep it if full country not present
#         "country": raw.get("country") or raw.get("countryCode"),

#         # best-effort address object
#         "address": {
#             "street": raw.get("street"),
#             "city": raw.get("city"),
#             "region": raw.get("state"),
#             "postal": raw.get("postalCode"),
#             "country": raw.get("country") or raw.get("countryCode"),
#         },

#         # keep original in case you need anything else later
#         "_raw": raw,
#     }
#     return normalized
