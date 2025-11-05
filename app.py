import os
import csv
import time
from datetime import datetime
from typing import List, Dict, Any
import json


from flask import Flask, render_template, request, jsonify, send_from_directory
from dotenv import load_dotenv
# app.py
from apify_client import google_search, web_scrape, ApifyError, google_maps_enrich


from apify_client import google_search, web_scrape, ApifyError
from extractors import assemble_lead_record

load_dotenv()

app = Flask(__name__, static_folder="static", template_folder="templates")
app.config["SECRET_KEY"] = os.getenv("FLASK_SECRET_KEY", "change-this-secret")

EXPORTS_DIR = os.path.join(os.path.dirname(__file__), "exports")
os.makedirs(EXPORTS_DIR, exist_ok=True)

MAX_RESULTS = int(os.getenv("MAX_RESULTS", "5"))


@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/run", methods=["POST"])
def api_run():
    """
    Body: { "query": "Company or lead name" }
    Returns: { "ok": true, "json_path": "/download/<filename>" }
    """
    from urllib.parse import urljoin
    from datetime import datetime
    import os, json

    data = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()
    if not query:
        return jsonify({"ok": False, "error": "Query is required"}), 400

    try:
        # Step 1: Google Search (main)
        results = google_search(query, max_results=MAX_RESULTS)

        # Step 1b: LinkedIn discovery (via Google)
        li_results = google_search(f"{query} site:linkedin.com/company", max_results=5)
        linkedin_url = None
        for r in li_results:
            u = (r.get("url") or "").split("?")[0].rstrip("/")
            if "linkedin.com/company/" in u:
                if u.endswith("/posts"):
                    u = u[:-6].rstrip("/")
                linkedin_url = u
                break

        # Step 1c: Google Maps enrichment
        maps_place = google_maps_enrich(query)

        # Step 2: Build a list of likely useful URLs to scrape
        top_urls = []
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

            # Add siteLinks if present
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



@app.route("/download/<path:filename>")
def download_file(filename):
    return send_from_directory(EXPORTS_DIR, filename, as_attachment=True)


if __name__ == "__main__":
    # For local dev
    app.run(host="0.0.0.0", port=5000, debug=True)
