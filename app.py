# app.py (updated)

import os
import json
import uuid
from datetime import datetime
from typing import Dict, Any, List

from flask import Flask, render_template, request, jsonify, send_from_directory
from dotenv import load_dotenv

load_dotenv()

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
# New flow helpers (filters → Apify actor → normalize)
from custom_apify_client import call_apify_actor, mock_results, ApifyError  # ApifyError reused
from extractors import assemble_lead_record, normalize_items  # assemble used by legacy

# Legacy helpers (company query → Google → Scrape → Assemble)
# These helpers are implemented in our local wrapper `custom_apify_client.py`.
from custom_apify_client import google_search, web_scrape, google_maps_enrich

# -----------------------------------------------------------------------------
# App setup
# -----------------------------------------------------------------------------
app = Flask(__name__, static_folder="static", template_folder="templates")
app.config["SECRET_KEY"] = os.getenv("FLASK_SECRET_KEY", "change-this-secret")

EXPORTS_DIR = os.path.join(os.path.dirname(__file__), "exports")
os.makedirs(EXPORTS_DIR, exist_ok=True)

MAX_RESULTS = int(os.getenv("MAX_RESULTS", "5"))
APIFY_TOKEN = os.getenv("APIFY_TOKEN", "")
USE_MOCK = os.getenv("USE_MOCK", "false").lower() == "true"

# -----------------------------------------------------------------------------
# Pages
# -----------------------------------------------------------------------------
@app.route("/")
def index():
    """Serve the UI. The page uses /api/leads/search (new flow)."""
    return render_template("index.html")


# -----------------------------------------------------------------------------
# Health
# -----------------------------------------------------------------------------
@app.get("/api/health")
def health():
    return jsonify({"status": "ok", "use_mock": USE_MOCK})


# -----------------------------------------------------------------------------
# NEW FLOW: Filters -> Apify actor -> normalized leads -> downloadable JSON
# -----------------------------------------------------------------------------
@app.post("/api/leads/search")
def leads_search():
    """
    Body:
    {
      "industry_focus": "Hospitality & Travel",
      "company_size_min": 50,
      "company_size_max": 5000,
      "countries": ["United Kingdom","Italy","Spain","Germany","Switzerland"],
      "roles": ["CEO","COO","Head of Operations","General Manager","GM"]
    }
    """
    data: Dict[str, Any] = request.get_json(silent=True) or {}

    # Basic validation + type normalization
    required = ["industry_focus", "company_size_min", "company_size_max", "countries", "roles"]
    missing = [k for k in required if k not in data]
    if missing:
        return jsonify({"error": f"Missing fields: {', '.join(missing)}"}), 400

    try:
        # coerce types defensively
        data["industry_focus"] = str(data.get("industry_focus") or "").strip()
        data["company_size_min"] = int(data.get("company_size_min") or 1)
        data["company_size_max"] = int(data.get("company_size_max") or 10_000_000)

        # ensure arrays
        data["countries"] = [str(c).strip() for c in (data.get("countries") or []) if str(c).strip()]
        data["roles"] = [str(r).strip() for r in (data.get("roles") or []) if str(r).strip()]

        if not data["countries"]:
            return jsonify({"error": "Please select at least one country."}), 400
        if data["company_size_min"] > data["company_size_max"]:
            return jsonify({"error": "company_size_min cannot be greater than company_size_max."}), 400

        # Invoke Apify (or mock)
        if USE_MOCK:
            raw_items = mock_results(data)
        else:
            if not APIFY_TOKEN:
                return jsonify({"error": "APIFY_TOKEN not set in .env"}), 400
            raw_items = call_apify_actor(data, APIFY_TOKEN)

        # Normalize rows for UI/export
        items: List[Dict[str, Any]] = normalize_items(raw_items, data)

        # Write export file
        request_id = str(uuid.uuid4())
        out_name = f"leads_{request_id}.json"
        out_path = os.path.join(EXPORTS_DIR, out_name)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=2)

        return jsonify({
            "ok": True,
            "request_id": request_id,
            "total": len(items),
            "download_url": f"/download/{out_name}",
            "items": items
        })

    except ApifyError as e:
        # Actor input/schema errors show up here with clear message
        return jsonify({"error": f"Lead search failed: {str(e)}"}), 400
    except Exception as e:
        return jsonify({"error": f"Server error: {str(e)}"}), 500


# -----------------------------------------------------------------------------
# LEGACY FLOW: Single query -> Google -> Scrape -> Assemble (still available)
# -----------------------------------------------------------------------------
@app.post("/api/run")
def api_run():
    """
    Body: { "query": "Company or lead name" }
    Returns: { "ok": true, "json_path": "/download/<filename>" }
    """
    data = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()
    if not query:
        return jsonify({"ok": False, "error": "Query is required"}), 400

    try:
        from urllib.parse import urljoin

        # Step 1: Google Search (main)
        results = google_search(query, max_results=MAX_RESULTS)

        # Step 1b: LinkedIn discovery (via Google)
        li_results = google_search(f"{query} site:linkedin.com/company", max_results=5)
        linkedin_url = None
        for r in li_results or []:
            u = (r.get("url") or "").split("?")[0].rstrip("/")
            if "linkedin.com/company/" in u:
                if u.endswith("/posts"):
                    u = u[:-6].rstrip("/")
                linkedin_url = u
                break

        # Step 1c: Google Maps enrichment
        maps_place = google_maps_enrich(query)

        # Step 2: Build a list of likely useful URLs to scrape
        top_urls: List[str] = []
        if results:
            const_main = results[0].get("url")
            if const_main:
                top_urls.extend([
                    const_main,
                    urljoin(const_main, "/"),
                    urljoin(const_main, "/contact"),
                    urljoin(const_main, "/contact-us"),
                    urljoin(const_main, "/about"),
                    urljoin(const_main, "/about-us"),
                    urljoin(const_main, "/locations"),
                    urljoin(const_main, "/our-hotels"),
                    urljoin(const_main, "/team"),
                    urljoin(const_main, "/leadership"),
                ])
            if results[0].get("siteLinks"):
                for s in results[0]["siteLinks"]:
                    u = s.get("url")
                    if u:
                        top_urls.append(u)

        # Deduplicate while preserving order
        urls, seen = [], set()
        for u in top_urls:
            if u and u not in seen:
                urls.append(u)
                seen.add(u)

        # Step 3: Crawl & scrape the website(s)
        scraped_rows = web_scrape(urls, max_pages=10) if urls else []

        # Step 4: Normalize and merge Maps + LinkedIn data
        row = assemble_lead_record(
            query,
            results,
            scraped_rows,
            maps_place=maps_place,
            linkedin_url=linkedin_url
        )

        # Step 5: Write JSON file
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"lead_{ts}.json"
        outpath = os.path.join(EXPORTS_DIR, filename)
        with open(outpath, "w", encoding="utf-8") as f:
            json.dump(row, f, ensure_ascii=False, indent=2)

        return jsonify({"ok": True, "json_path": f"/download/{filename}"})

    except ApifyError as e:
        return jsonify({"ok": False, "error": f"Apify error: {str(e)}"}), 500
    except Exception as e:
        return jsonify({"ok": False, "error": f"Server error: {str(e)}"}), 500


# -----------------------------------------------------------------------------
# Downloads
# -----------------------------------------------------------------------------
@app.route("/download/<path:filename>")
def download_file(filename):
    return send_from_directory(EXPORTS_DIR, filename, as_attachment=True)


# -----------------------------------------------------------------------------
if __name__ == "__main__":
    # Keep 5000 for your current setup.
    app.run(host="0.0.0.0", port=5000, debug=True)