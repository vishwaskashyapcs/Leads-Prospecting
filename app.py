import os
import json
import uuid
from datetime import datetime

from flask import Flask, render_template, request, jsonify, send_from_directory
from dotenv import load_dotenv

# ---------------- LOAD ENV ----------------
load_dotenv()

# ---------------- INTERNAL IMPORTS ----------------
from person_prospect import (
    generate_company_prompt,
    query_groq,
    parse_companies,
    validate_companies,
    fetch_contacts_from_serpapi,
    parse_contacts,
)
from custom_apify_client import google_search, web_scrape, google_maps_enrich
from extractors import assemble_lead_record

# ---------------- FLASK ----------------
app = Flask(__name__, static_folder="static", template_folder="templates")
EXPORTS_DIR = os.path.join(os.path.dirname(__file__), "exports")
os.makedirs(EXPORTS_DIR, exist_ok=True)
MAX_RESULTS = int(os.getenv("MAX_RESULTS", "5"))


def _split_headquarters(raw_value):
    if not raw_value:
        return "", ""
    parts = [part.strip() for part in str(raw_value).split(",") if part.strip()]
    if not parts:
        return "", ""
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
    for entry in valid_companies or []:
        city, country = _split_headquarters(entry.get("headquarters"))
        items.append({
            "company_name": entry.get("company"),
            "company_size": entry.get("employees"),
            "city": city,
            "country": country,
            "website": entry.get("website"),
            "source": entry.get("source"),
            "revenue": entry.get("revenue"),
        })
    return items


def _process_lead_search(payload):
    industry = (payload.get("industry_focus") or "").strip()
    size_min = payload.get("company_size_min")
    size_max = payload.get("company_size_max")
    countries = payload.get("countries") or []

    if not industry or size_min is None or size_max is None or not countries:
        return jsonify({
            "ok": False,
            "error": "industry_focus, company_size_min, company_size_max and countries are required",
        }), 400

    try:
        size_min_val = int(size_min)
        size_max_val = int(size_max)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "company_size_min and company_size_max must be numbers"}), 400

    size_range = f"{size_min_val}-{size_max_val} employees"
    location = ", ".join(countries)

    roles = payload.get("roles")
    if isinstance(roles, list):
        role_filters = roles
    elif roles:
        role_filters = [roles]
    else:
        role_filters = []

    try:
        prompt = generate_company_prompt(industry, location, size_range)
        groq_out = query_groq(prompt)
        parsed = parse_companies(groq_out)
        valid_companies, rejected_companies = validate_companies(parsed)
        items = _build_company_items(valid_companies)

        export_payload = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "filters": {
                "industry_focus": industry,
                "company_size_min": size_min_val,
                "company_size_max": size_max_val,
                "countries": countries,
                "roles": role_filters,
            },
            "results": items,
            "rejected": rejected_companies,
        }
        download_url = _write_export("leads", export_payload)

        return jsonify({
            "ok": True,
            "total": len(items),
            "items": items,
            "download_url": download_url,
            "rejected_count": len(rejected_companies),
        })

    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


# ---------------- HOME ----------------
@app.route("/")
def index():
    return render_template("index.html")


# ==========================================================
# FIND COMPANIES (GROQ)
# ==========================================================
@app.post("/api/leads/search")
def search_leads():
    data = request.get_json(silent=True) or {}
    return _process_lead_search(data)


@app.post("/api/companies/search")
def search_companies():
    data = request.get_json(silent=True) or {}
    return _process_lead_search(data)


# ==========================================================
# ENRICH COMPANY (APIFY / SCRAPING)
# ==========================================================
@app.post("/api/company/enrich")
def enrich_company():
    data = request.get_json(silent=True) or {}
    company_name = (data.get("company_name") or "").strip()
    if not company_name:
        return jsonify({"error": "company_name is required"}), 400

    try:
        website_hint = (data.get("website") or "").strip()
        google_results = []
        site = website_hint

        if not site:
            google_results = google_search(company_name, max_results=1)
            site = google_results[0].get("url") if google_results else None
        else:
            google_results = [{"url": site}]

        maps_place = google_maps_enrich(company_name)
        scraped_rows = web_scrape([site], max_pages=10) if site else []

        enrichment = assemble_lead_record(
            company_name,
            google_results,
            scraped_rows,
            maps_place=maps_place,
            linkedin_url=None
        )

        return jsonify({"ok": True, "data": enrichment})

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/api/company/find-lead")
def find_lead():
    data = request.get_json(silent=True) or {}
    company_name = (data.get("company_name") or "").strip()
    if not company_name:
        return jsonify({"error": "company_name is required"}), 400

    try:
        serp_json = fetch_contacts_from_serpapi(company_name)
        people = parse_contacts(serp_json)
        return jsonify({"ok": True, "people": people})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post("/api/run")
def run_single_company():
    data = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()
    if not query:
        return jsonify({"ok": False, "error": "query is required"}), 400

    try:
        google_results = google_search(query, max_results=MAX_RESULTS)
        urls = []
        for result in google_results:
            url = result.get("url")
            if url and url not in urls:
                urls.append(url)
        scrape_targets = urls[:max(1, min(len(urls), MAX_RESULTS))]
        scraped_rows = web_scrape(scrape_targets, max_pages=10) if scrape_targets else []

        try:
            maps_place = google_maps_enrich(query)
        except Exception:
            maps_place = {}

        lead_record = assemble_lead_record(
            query,
            google_results,
            scraped_rows,
            maps_place=maps_place,
            linkedin_url=None
        )

        download_url = _write_export("lead", lead_record)
        return jsonify({"ok": True, "json_path": download_url})

    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500

@app.route("/download/<path:filename>")
def download_file(filename):
    return send_from_directory(EXPORTS_DIR, filename, as_attachment=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
