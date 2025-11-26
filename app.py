import csv
import os
import json
import uuid
from datetime import datetime
from io import StringIO
from typing import Any, Dict, List, Tuple, Optional
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv
from flask import (
    Flask,
    jsonify,
    render_template,
    request,
    send_from_directory,
)

# ---------------- INTERNAL IMPORTS ----------------
from person_prospect import (
    generate_company_prompt,
    query_groq,
    parse_companies,
    validate_companies,
    fetch_contacts_from_serpapi,
    parse_contacts,
)

# from custom_apify_client import google_search, web_scrape, google_maps_enrich  # Apify disabled for now
from extractors import assemble_lead_record

load_dotenv()

# ---------------- FLASK ----------------
app = Flask(__name__, static_folder="static", template_folder="templates")
EXPORTS_DIR = os.path.join(os.path.dirname(__file__), "exports")
os.makedirs(EXPORTS_DIR, exist_ok=True)
MAX_RESULTS = int(os.getenv("MAX_RESULTS", "5"))
THEIRSTACK_API_KEY = os.getenv("THEIRSTACK_API_KEY")
THEIRSTACK_ENDPOINT = os.getenv("THEIRSTACK_ENDPOINT", "https://api.theirstack.com/v1/jobs/search")
THEIRSTACK_TECH_SLUGS = [
    "jira",
    "asana",
    "monday",
    "slack",
    "microsoft-teams",
    "confluence",
    "workday",
    "power-bi",
    "microsoft-azure",
    "amazon-web-services",
]
THEIRSTACK_TECH_LABELS = {
    "jira": "Jira",
    "asana": "Asana",
    "monday": "Monday.com",
    "slack": "Slack",
    "microsoft-teams": "MS Teams",
    "confluence": "Confluence",
    "workday": "Workday",
    "power-bi": "Power BI",
    "microsoft-azure": "Azure",
    "amazon-web-services": "AWS",
}
THEIRSTACK_ROLE_FILTERS = ["CIO", "CTO", "VP", "PMO", "Engineering", "IT"]
THEIRSTACK_MAX_JOBS = 10

def _split_headquarters(raw_value):
    if not raw_value:
        return "", ""
    parts = [part.strip() for part in str(raw_value).split(",") if part.strip()]
    if len(parts) == 1:
        return "", parts[0]
    return parts[0], parts[-1]


def _write_export(prefix, payload):
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"{prefix}_{ts}_{uuid.uuid4().hex[:6]}.json"
    path = os.path.join(EXPORTS_DIR, filename)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
    return f"/download/{filename}"


def _build_company_items(valid_companies):
    items = []
    for c in valid_companies or []:
        city, country = _split_headquarters(c.get("headquarters"))
        items.append(
            {
                "company_name": c.get("company"),
                "company_size": c.get("employees"),
                "city": city,
                "country": country,
                "website": c.get("website"),
                "source": c.get("source"),
                "revenue": c.get("revenue"),
                "headquarters": c.get("headquarters"),
            }
        )
    return items


def _extract_domain(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url if "://" in url else f"http://{url}")
    host = parsed.netloc or parsed.path
    return host.split("/")[0].lower()


def _parse_theirstack_rows(resp: requests.Response) -> List[Dict[str, Any]]:
    body = resp.text.strip()
    if not body:
        return []
    try:
        payload = resp.json()
        if isinstance(payload, dict):
            for key in ("jobs", "items", "results"):
                if isinstance(payload.get(key), list):
                    return payload[key]
            inner = payload.get("data")
            if isinstance(inner, dict):
                for key in ("jobs", "items", "results"):
                    if isinstance(inner.get(key), list):
                        return inner[key]
        elif isinstance(payload, list):
            return payload
    except ValueError:
        pass

    reader = csv.DictReader(StringIO(body))
    return list(reader)


def _normalize_job(job: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(job, dict):
        return {}

    def pick(*keys):
        for k in keys:
            if job.get(k):
                return job[k]
        return ""

    return {
        "job_title": pick("job_title", "title"),
        "url": pick("url", "job_url"),
        "posted_date": pick("posted_date", "postedAt"),
        "job_location": pick("job_location", "location"),
        "job_country_code": pick("job_country_code", "country_code"),
        "employment_status": pick("employment_status"),
        "seniority": pick("seniority"),
        "is_remote": pick("is_remote", "remote"),
        "company_name": pick("company_name"),
    }


def _country_code_from_maps(maps_place: Dict[str, Any]) -> str:
    if not isinstance(maps_place, dict):
        return ""
    for key in ("countryCode", "country_code"):
        val = maps_place.get(key)
        if isinstance(val, str) and len(val.strip()) == 2:
            return val.strip().upper()
    address = maps_place.get("address")
    if isinstance(address, dict):
        for key in ("countryCode", "country_code"):
            val = address.get(key)
            if isinstance(val, str) and len(val.strip()) == 2:
                return val.strip().upper()
    return ""


def _extend_unique(target: List[str], values: List[str]):
    for val in values or []:
        if val and val not in target:
            target.append(val)


###############################################################################
# THEIRSTACK JOB SCRAPER
###############################################################################
def fetch_theirstack_jobs(
    company_name: str,
    domain: str = "",
    country_code: str = "",
    limit: int = THEIRSTACK_MAX_JOBS,
) -> Dict[str, Any]:
    if not THEIRSTACK_API_KEY:
        return {"jobs": [], "tech_stack_signals": []}

    payload = {
        "page": 0,
        "limit": limit,
        "order_by": [{"desc": True, "field": "num_jobs"}],
        "include_total_results": False,
        "blur_company_data": False,
        "posted_at_max_age_days": 30,
        "company_technology_slug_and": THEIRSTACK_TECH_SLUGS,
        "job_title_or": THEIRSTACK_ROLE_FILTERS,
    }

    if domain:
        payload["company_domain_or"] = [domain]
    if company_name:
        payload["company_name_partial_match_or"] = [company_name]
    if country_code:
        payload["company_country_code_or"] = [country_code]

    headers = {
        "Authorization": f"Bearer {THEIRSTACK_API_KEY}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    try:
        resp = requests.post(THEIRSTACK_ENDPOINT, headers=headers, json=payload, timeout=30)
        if resp.status_code != 200:
            print("[THEIRSTACK] Error", resp.status_code, resp.text[:200])
            return {"jobs": [], "tech_stack_signals": []}
        rows = _parse_theirstack_rows(resp)
        print(f"[THEIRSTACK] Raw response ({len(rows)} rows) for {company_name or domain}:")
        try:
            print(json.dumps(rows, indent=2)[:2000])
        except Exception:
            print(rows[:3])

        jobs = [_normalize_job(r) for r in rows if r]
        jobs = [j for j in jobs if j.get("job_title")]
        jobs = jobs[:limit]
        print(f"[THEIRSTACK] Normalized jobs ({len(jobs)}) -> {[j.get('job_title') for j in jobs]}")
        signals = [THEIRSTACK_TECH_LABELS.get(slug, slug.title()) for slug in THEIRSTACK_TECH_SLUGS] if jobs else []
        return {"jobs": jobs, "tech_stack_signals": signals}
    except Exception as exc:
        print("[THEIRSTACK] Request Exception:", exc)
        return {"jobs": [], "tech_stack_signals": []}


###############################################################################
# LLM ENRICHMENT - TARGETED FOR B2B SAAS
###############################################################################
def build_enrichment_prompt(company_dict: Dict[str, Any]) -> str:
    """Build a targeted prompt for B2B SaaS sales intelligence"""
    
    # Extract key information
    company_name = company_dict.get("company_name", "")
    website = company_dict.get("website", "")
    
    # Extract from summary record
    summary = company_dict.get("summary_record", {})
    industry = summary.get("Industry Segment", "")
    location = summary.get("Country / City", "")
    company_size = summary.get("Company Size", "")
    
    # Extract from jobs
    jobs = company_dict.get("jobs", [])
    job_titles = [job.get("job_title", "") for job in jobs if job.get("job_title")]
    
    prompt = f"""
You are a precise B2B GTM analyst. Use ONLY the evidence provided. If you do not see clear evidence for a field, return an empty list for that field. Never guess.

CONTEXT:
- Company: {company_name}
- Industry: {industry}
- Location: {location}
- Size: {company_size}
- Jobs (titles): {job_titles}
- Website: {website}

ALLOWED VALUES:
- Tech Stack Indicators: Jira, Asana, Monday.com, Slack, MS Teams, Confluence, Workday, Power BI, Azure, AWS.
- Buying Triggers: Recently raised funding; New CIO/VP Eng hire; Expanding engineering headcount; Active PMO hiring; Running transformation initiatives.
- Primary Pain Keywords: Delivery predictability; Engineering productivity; Digital transformation; Project visibility; Capacity planning; Resource optimization; Team collaboration; Process efficiency.

RULES:
- Only include items that are explicitly supported or strongly implied by the context (e.g., job titles hinting at engineering expansion or PMO).
- If there is no evidence for a field, return an empty array for that field.
- Output must be valid JSON ONLY, no commentary.

JSON SCHEMA (fill with evidence-based items or empty arrays):
{{
  "tech_stack_indicators": [],
  "buying_triggers": [],
  "primary_pain_keywords": []
}}
"""
    return prompt


def _coerce_json_block(text: str) -> Dict[str, Any]:
    """Extract JSON from text response"""
    if not text:
        return {}
    try:
        return json.loads(text)
    except:
        try:
            # Try to extract JSON from markdown code blocks
            if "```json" in text:
                json_str = text.split("```json")[1].split("```")[0].strip()
                return json.loads(json_str)
            elif "```" in text:
                json_str = text.split("```")[1].split("```")[0].strip()
                return json.loads(json_str)
            else:
                # Try to find JSON object in text
                start_idx = text.find('{')
                end_idx = text.rfind('}') + 1
                if start_idx != -1 and end_idx != 0:
                    json_str = text[start_idx:end_idx]
                    return json.loads(json_str)
        except:
            return {}
    return {}
def extract_enrichment_insights(company_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Targeted enrichment for B2B SaaS sales with actual LLM calls"""
    
    template = {
        "tech_stack_indicators": [],
        "buying_triggers": [], 
        "primary_pain_keywords": [],
        "message": "B2B SaaS enrichment completed",
    }
    
    # Extract key data
    tech_signals = company_dict.get("tech_stack_signals", [])
    jobs = company_dict.get("jobs", [])
    company_name = company_dict.get("company_name", "").lower()
    
    # Pre-defined B2B SaaS focus areas
    TARGET_TECH_STACK = [
        "Jira", "Asana", "Monday.com", "Slack", "MS Teams", 
        "Confluence", "Workday", "Power BI", "Azure", "AWS"
    ]
    
    TARGET_PAIN_POINTS = [
        "Delivery predictability", "Engineering productivity", "Digital transformation",
        "Project visibility", "Capacity planning", "Resource optimization",
        "Team collaboration", "Process efficiency"
    ]
    
    llm_success = False
    try:
        print(f"[ENRICHMENT] Building prompt for {company_name}...")
        prompt = build_enrichment_prompt(company_dict)
        print(f"[ENRICHMENT] Querying LLM...")
  
        llm_response = query_groq(prompt)
        
        print(f"[ENRICHMENT] LLM Raw Response: {llm_response}")
        
        if not llm_response or llm_response.strip() == "":
            print("[ENRICHMENT] Empty LLM response")
            raise ValueError("Empty LLM response")
        
        cleaned_response = llm_response
        if "```json" in cleaned_response:
            cleaned_response = cleaned_response.split("```json")[1].split("```")[0].strip()
        elif "```" in cleaned_response:
            cleaned_response = cleaned_response.split("```")[1].split("```")[0].strip()
        
        parsed = _coerce_json_block(cleaned_response)
        
        if isinstance(parsed, dict) and any(key in parsed for key in ["tech_stack_indicators", "buying_triggers", "primary_pain_keywords"]):
            llm_success = True
            print(f"[ENRICHMENT] Successfully parsed LLM response: {parsed}")
            
            if "tech_stack_indicators" in parsed and isinstance(parsed["tech_stack_indicators"], list):
                template["tech_stack_indicators"] = [
                    tool for tool in parsed["tech_stack_indicators"] 
                    if any(target.lower() in str(tool).lower() for target in TARGET_TECH_STACK)
                ]
            
            if "buying_triggers" in parsed and isinstance(parsed["buying_triggers"], list):
                template["buying_triggers"] = [
                    trigger for trigger in parsed["buying_triggers"][:3] 
                    if trigger and trigger.strip()
                ]
            
            if "primary_pain_keywords" in parsed and isinstance(parsed["primary_pain_keywords"], list):
                template["primary_pain_keywords"] = [
                    pain for pain in parsed["primary_pain_keywords"]
                    if any(target.lower() in str(pain).lower() for target in TARGET_PAIN_POINTS)
                ]
        else:
            print(f"[ENRICHMENT] Failed to parse valid JSON from LLM response")
            llm_success = False
        
    except Exception as e:
        print(f"[ENRICHMENT] LLM Error: {e}")
        llm_success = False
    
    if not llm_success:
        print("[ENRICHMENT] Using fallback logic since LLM failed")
        
        if tech_signals:
            template["tech_stack_indicators"] = tech_signals
        else:
            industry = company_dict.get("industry", "").lower()
            company_size = company_dict.get("company_size", "")
            
            if "financial" in industry or "fintech" in industry:
                template["tech_stack_indicators"] = ["Jira", "Confluence", "AWS", "Power BI", "Slack"]
            elif "consulting" in industry:
                template["tech_stack_indicators"] = ["MS Teams", "SharePoint", "Azure", "Power BI"]
            elif "tech" in industry or "software" in industry:
                template["tech_stack_indicators"] = ["Jira", "Slack", "AWS", "Confluence", "GitHub"]
            else:
                template["tech_stack_indicators"] = ["Jira", "Slack", "AWS"]
        
        triggers = []
        job_titles = [job.get("job_title", "").lower() for job in jobs]
        
        eng_keywords = ["engineer", "developer", "software", "tech"]
        if any(any(keyword in title for keyword in eng_keywords) for title in job_titles):
            triggers.append("Expanding engineering headcount")
        
        pmo_keywords = ["project", "program", "pmo", "transformation", "process"]
        if any(any(keyword in title for keyword in pmo_keywords) for title in job_titles):
            triggers.append("Active PMO hiring")
            triggers.append("Running transformation initiatives")
        
        # Check for executive roles
        exec_keywords = ["cio", "cto", "vp", "director", "head of"]
        if any(any(keyword in title for keyword in exec_keywords) for title in job_titles):
            triggers.append("New executive technology hires")
        
        if triggers:
            template["buying_triggers"] = triggers[:3]
        else:
            # Size-based fallbacks
            if "5000+" in company_size or "1000+" in company_size:
                template["buying_triggers"] = ["Digital transformation initiatives", "Enterprise scaling"]
            else:
                template["buying_triggers"] = ["Growth and expansion", "Process optimization"]
        
        # Pain points based on context
        industry = company_dict.get("industry", "").lower()
        if "financial" in industry:
            template["primary_pain_keywords"] = [
                "Regulatory compliance", "System security", "Process efficiency"
            ]
        elif "consulting" in industry:
            template["primary_pain_keywords"] = [
                "Project delivery timelines", "Resource allocation", "Client satisfaction"
            ]
        elif any("engineer" in job.get("job_title", "").lower() for job in jobs):
            template["primary_pain_keywords"] = [
                "Engineering productivity", "Delivery predictability", "Resource optimization"
            ]
        else:
            template["primary_pain_keywords"] = [
                "Delivery predictability", "Team collaboration", "Process efficiency"
            ]
    
    # Final cleanup and validation
    template["tech_stack_indicators"] = [
        tool for tool in template["tech_stack_indicators"] 
        if any(target.lower() in str(tool).lower() for target in TARGET_TECH_STACK)
    ][:5]
    
    template["primary_pain_keywords"] = [
        pain for pain in template["primary_pain_keywords"]
        if any(target.lower() in str(pain).lower() for target in TARGET_PAIN_POINTS)
    ][:4]
    
    # Update message based on source
    if llm_success:
        template["message"] = "B2B SaaS LLM enrichment completed"
    else:
        template["message"] = "B2B SaaS fallback enrichment applied"
    
    print(f"[ENRICHMENT] Final template: {template}")
    return template

def _llm_fill_company_details(context: Dict[str, Any]) -> Tuple[Dict[str, str], bool, Optional[str], bool]:
    """
    Ask the LLM to fill missing basic company fields. It must leave values empty
    when unsure to avoid hallucinations.
    """
    company = context.get("company_name", "")
    website = context.get("website", "")
    location = context.get("location", "")
    industry_hint = context.get("industry_hint", "")
    size = context.get("company_size", "")
    revenue = context.get("revenue", "")

    prompt = f"""
You are a careful B2B enrichment assistant. Given light context about a company, return only JSON.
If you are not confident about a field, return an empty string for it. Do not guess or invent.

Context:
- Company: {company}
- Website: {website}
- Location: {location}
- Industry (hint): {industry_hint}
- Company size: {size}
- Revenue: {revenue}

Return JSON with exactly these keys:
{{
  "email": "",
  "phone": "",
  "linkedin_url": "",
  "industry": "",
    "google_rating": "",
    "total_reviews": ""
}}

Rules:
- Prefer leaving a value empty over guessing.
- Phone must be in E.164 format if known, else empty.
- Google rating must be 0-5 range if known, else empty.
- Total reviews is a number or empty string.
"""
    api_key_override = context.get("groq_api_key")
    try:
        key = api_key_override or os.getenv("GROQ_API_KEY")
        if not key:
            raise EnvironmentError("GROQ_API_KEY missing; cannot call Groq.")

        print("[LLM_DETAILS] Calling Groq for enrichment fields")
        llm_resp = query_groq(prompt, api_key=key)
        parsed = _coerce_json_block(llm_resp)
        if isinstance(parsed, dict):
            out = {
                "email": str(parsed.get("email") or "").strip(),
                "phone": str(parsed.get("phone") or "").strip(),
                "linkedin_url": str(parsed.get("linkedin_url") or "").strip(),
                "industry": str(parsed.get("industry") or "").strip(),
                "google_rating": str(parsed.get("google_rating") or "").strip(),
                "total_reviews": str(parsed.get("total_reviews") or "").strip(),
            }
            filled = any(out.values())
            if not filled:
                print("[LLM_DETAILS] Groq returned empty details; will mark as fallback.")
            return out, filled, None if filled else "empty_llm_details", True
    except Exception as exc:
        print("[LLM_DETAILS] Error", exc)
    return {
        "email": "",
        "phone": "",
        "linkedin_url": "",
        "industry": "",
        "google_rating": "",
        "total_reviews": "",
    }, False, str(exc) if "exc" in locals() else "unknown_error", bool(api_key_override)


def build_llm_company_profile(
    company_name: str,
    website: str,
    google_results: List[Dict[str, Any]],
    scraped_rows: List[Dict[str, Any]],
    maps_place: Dict[str, Any],
    enrichment_record: Dict[str, Any],
    jobs: List[Dict[str, Any]],
):
    """Build structured company profile for B2B SaaS analysis"""
    
    def _safe_extract(text, max_len=1000):
        if not text:
            return ""
        text_str = str(text)
        return text_str[:max_len] if len(text_str) > max_len else text_str
    
    # Extract key company details from enrichment record
    company_size = enrichment_record.get("Company Size", "")
    industry = enrichment_record.get("Industry Segment", "")
    industry_type = enrichment_record.get("Industry Type", "")
    
    if not industry:

        company_lower = company_name.lower()
        industry_type_lower = str(industry_type).lower()
        
        travel_indicators = ["travel", "tour", "trip", "vacation", "holiday", "booking", "flight", "hotel"]
        tech_indicators = ["tech", "software", "it", "digital", "platform", "solution"]
        hospitality_indicators = ["hotel", "resort", "hospitality", "accommodation"]
        
        if any(indicator in company_lower for indicator in travel_indicators) or any(indicator in industry_type_lower for indicator in travel_indicators):
            industry = "Travel & Tourism"
        elif any(indicator in company_lower for indicator in tech_indicators) or any(indicator in industry_type_lower for indicator in tech_indicators):
            industry = "Technology"
        elif any(indicator in company_lower for indicator in hospitality_indicators) or any(indicator in industry_type_lower for indicator in hospitality_indicators):
            industry = "Hospitality"
        elif industry_type_lower and industry_type_lower != "none":
            industry = industry_type_lower.title()
        else:
            industry = "General Business"
    
    # Build scraped content summary
    scraped_content = []
    for page in scraped_rows[:3]:  # Limit to 3 most relevant pages
        scraped_content.append({
            "url": page.get("url", ""),
            "title": page.get("title", ""),
            "text_preview": _safe_extract(page.get("text"), 800)
        })
    
    return {
        "company_name": company_name,
        "website": website,
        "industry": industry,
        "company_size": company_size,
        "location": maps_place.get("formatted_address", "") if maps_place else "",
        "summary_record": {
            "Industry Segment": industry,
            "Industry Type": industry_type,
            "Company Size": company_size,
            "Country / City": enrichment_record.get("Country / City", ""),
            "Google Rating": enrichment_record.get("Google Rating", ""),
            "Total Google Reviews": enrichment_record.get("Total Google Reviews", "")
        },
        "jobs": jobs,
        "job_count": len(jobs),
        "scraped_pages": scraped_content,
        "tech_stack_signals": enrichment_record.get("tech_stack_signals", [])
    }


###############################################################################
# LEAD SEARCH
###############################################################################
@app.post("/api/leads/search")
def search_leads():
    data = request.get_json(silent=True) or {}
    return _process_lead_search(data)


@app.post("/api/companies/search")
def search_companies():
    data = request.get_json(silent=True) or {}
    return _process_lead_search(data)


def _process_lead_search(payload):
    industry = (payload.get("industry_focus") or "").strip()
    size_min = payload.get("company_size_min")
    size_max = payload.get("company_size_max")
    countries = payload.get("countries") or []

    if not industry or size_min is None or size_max is None or not countries:
        return jsonify({"ok": False, "error": "industry_focus, company_size_min, company_size_max, countries required"}), 400

    try:
        size_min_val = int(size_min)
        size_max_val = int(size_max)
    except:
        return jsonify({"ok": False, "error": "size ranges must be numeric"}), 400

    try:
        prompt = generate_company_prompt(industry, ", ".join(countries), f"{size_min_val}-{size_max_val} employees")
        groq_out = query_groq(prompt)
        parsed = parse_companies(groq_out)
        valid, rejected = validate_companies(parsed)
        items = _build_company_items(valid)

        url = _write_export("leads", {
            "generated_at": datetime.utcnow().isoformat(),
            "results": items,
            "rejected": rejected,
        })
        return jsonify({"ok": True, "items": items, "download_url": url, "rejected_count": len(rejected)})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


###############################################################################
# ENRICH COMPANY - UPDATED WITH B2B SAAS ENRICHMENT
###############################################################################
@app.post("/api/company/enrich")
def enrich_company():
    data = request.get_json(silent=True) or {}
    company_name = (data.get("company_name") or "").strip()

    if not company_name:
        return jsonify({"error": "company_name is required"}), 400

    try:
        website = str(data.get("website") or "").strip()
        city = str(data.get("city") or "").strip()
        country = str(data.get("country") or "").strip()
        headquarters = str(data.get("headquarters") or "").strip()
        company_size = str(data.get("company_size") or data.get("employees") or "").strip()
        revenue = str(data.get("revenue") or "").strip()
        linkedin_url = data.get("linkedin_url")

        if (not city or not country) and headquarters:
            hq_city, hq_country = _split_headquarters(headquarters)
            city = city or hq_city
            country = country or hq_country

        google_results = []
        scraped_rows = []
        maps_place = {}

        domain = _extract_domain(website)
        country_code = _country_code_from_maps(maps_place)
        if not country_code and country and len(country) == 2:
            country_code = country.upper()
        theirstack_data = fetch_theirstack_jobs(company_name, domain, country_code or "")
        jobs = theirstack_data.get("jobs", [])
        tech_signals = theirstack_data.get("tech_stack_signals", [])

        record = assemble_lead_record(
            company_name,
            google_results,
            scraped_rows,
            maps_place=maps_place,
            linkedin_url=linkedin_url,
        )

        record["Lead Name"] = company_name
        record["Company Name"] = company_name
        record["Company Name (linked)"] = company_name
        if website:
            record["Website URL"] = website

        location_parts = [p for p in [country, city] if p]
        if location_parts:
            record["Country / City"] = ", ".join(location_parts)
        if company_size:
            record["Company Size"] = company_size
        if revenue:
            record["Revenue"] = revenue
        if city:
            record["City"] = city
        if country:
            record["Country"] = country
        record["source"] = data.get("source") or record.get("source", "groq")

        record["jobs_count"] = len(jobs)
        record["jobs"] = jobs
        record["tech_stack_signals"] = tech_signals

        # Use LLM to fill remaining core fields conservatively
        llm_details, llm_ok, llm_err, llm_used_override = _llm_fill_company_details({
            "company_name": company_name,
            "website": website,
            "location": record.get("Country / City", ""),
            "industry_hint": record.get("Industry Segment") or record.get("Industry Type (Hotel / Resort / Service Apartment, etc.)", ""),
            "company_size": company_size,
            "revenue": revenue,
            "groq_api_key": data.get("groq_api_key"),
        })

        if not record.get("Email ID") and llm_details.get("email"):
            record["Email ID"] = llm_details["email"]
        if not record.get("Phone (if verified)") and llm_details.get("phone"):
            record["Phone (if verified)"] = llm_details["phone"]
        if not record.get("LinkedIn Profile URL") and llm_details.get("linkedin_url"):
            record["LinkedIn Profile URL"] = llm_details["linkedin_url"]
        if not record.get("Industry Segment") and llm_details.get("industry"):
            record["Industry Segment"] = llm_details["industry"]
        if not record.get("Google Rating") and llm_details.get("google_rating"):
            record["Google Rating"] = llm_details["google_rating"]
        if not record.get("Total Google Reviews") and llm_details.get("total_reviews"):
            record["Total Google Reviews"] = llm_details["total_reviews"]
        record["llm_enrichment_status"] = "llm_success" if llm_ok else "llm_fallback"
        record["callback_needed"] = not llm_ok
        record["llm_error"] = llm_err if not llm_ok else ""
        record["llm_api_key_override_used"] = llm_used_override

        # DEBUG: Check what data we have
        print(f"[DEBUG] Jobs found: {len(jobs)}")
        print(f"[DEBUG] Tech signals: {tech_signals}")
        print(f"[DEBUG] Scraped pages: {len(scraped_rows)}")
        print(f"[DEBUG] Company name: {company_name}")
        print(f"[DEBUG] Industry Type from record: {record.get('Industry Type', '')}")
        
        # Build the profile for LLM
        llm_profile = build_llm_company_profile(
            company_name,
            website,
            google_results,
            scraped_rows,
            maps_place,
            record,
            jobs,
        )
        
        # DEBUG: Check profile content
        print(f"[DEBUG] LLM Profile keys: {llm_profile.keys()}")
        print(f"[DEBUG] Summary record: {llm_profile.get('summary_record', {})}")
        print(f"[DEBUG] Detected industry: {llm_profile.get('industry', '')}")

        # Extract insights with B2B SaaS focused enrichment
        insights = extract_enrichment_insights(llm_profile)
        
        # DEBUG: Final insights
        print(f"[DEBUG] Final insights: {insights}")
        record["insights"] = insights

        print("\n============= FINAL ENRICHMENT RECORD =============")
        print(json.dumps(record, indent=2, ensure_ascii=False))
        print("===================================================\n")

        dl = _write_export("enrich", record)
        return jsonify({"ok": True, "data": record, "insights": insights, "download_url": dl})

    except Exception as e:
        print("[ENRICH][FATAL]", e)
        return jsonify({"ok": False, "error": str(e)}), 500

@app.post("/api/company/find-lead")
def find_lead():
    data = request.get_json(silent=True) or {}
    name = (data.get("company_name") or "").strip()
    if not name:
        return jsonify({"error": "company_name is required"}), 400
    try:
        serp = fetch_contacts_from_serpapi(name)
        return jsonify({"ok": True, "people": parse_contacts(serp)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.post("/api/run")
def run_single():
    d = request.get_json(silent=True) or {}
    query = (d.get("query") or "").strip()
    if not query:
        return jsonify({"ok": False, "error": "query required"}), 400
    try:
        website = str(d.get("website") or "").strip()
        google_results = [{"url": website}] if website else []
        scraped = []
        maps_place = {}

        record = assemble_lead_record(query, google_results, scraped, maps_place)
        record["Lead Name"] = query
        record["Company Name"] = query
        record["Company Name (linked)"] = query
        if website:
            record["Website URL"] = website
        dl = _write_export("lead", record)
        return jsonify({"ok": True, "json_path": dl})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/download/<path:f>")
def download_file(f):
    return send_from_directory(EXPORTS_DIR, f, as_attachment=True)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
