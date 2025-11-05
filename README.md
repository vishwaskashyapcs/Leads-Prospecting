# Lead Finder

Lead Finder is a lightweight Flask application that automates first-pass research on a company or brand. It orchestrates Google search, LinkedIn discovery, and website scraping via Apify actors, normalizes the results, and saves a ready-to-use JSON snapshot for download.

## Project Highlights
- Turns a single search query into a structured company profile including website, contact info, LinkedIn URL, and Google Maps details.
- Simple web UI with progress hints while the backend gathers and cleans data.
- Exports timestamped JSON files to `exports/` for later review or ingestion into other tooling.

## Prerequisites
- Python 3.10 or newer
- `pip` for dependency installation
- An Apify account and token for the scraping actors

## Getting Started
```bash
python -m venv venv
source venv/bin/activate        # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

Create a `.env` file in the project root (the existing example shows the required keys). At minimum you need:

```
APIFY_TOKEN=...
GOOGLE_ACTOR_ID=apify~google-search-scraper
WEB_SCRAPER_ACTOR_ID=apify~web-scraper
GOOGLE_MAPS_ACTOR_ID=...
MAX_RESULTS=5
FLASK_SECRET_KEY=change-this-secret
```

## Running the App
The backend exposes a Flask server that renders the frontend and serves a single `/api/run` endpoint.

```bash
flask --app app run --debug
# or
python app.py
```

Open http://127.0.0.1:5000, submit a company name, and wait for the UI to surface a download link. Each run stores a `lead_YYYYMMDD_HHMMSS.json` file inside `exports/`, which is ignored by Git but kept locally for reference.

## How It Works
1. Google search results are fetched through the Apify Google Search actor, including company site links.
2. LinkedIn company pages and Google Maps metadata are added when available.
3. The main website and selected subpages are crawled to find emails, phone numbers, and team information.
4. `extractors.py` consolidates the raw scrape output into a standardized schema before the data is written to disk.

## Development Notes
- Frontend code lives in `static/` (styles and JavaScript). Templates are under `templates/`.
- `apify_client.py` wraps interaction with the Apify actors and centralizes error handling.
- Adjust `MAX_RESULTS` in `.env` to control how many Google links feed into the scraper.
- When iterating on data extraction, you can inspect raw scrape responses by adding temporary logging inside `assemble_lead_record` in `extractors.py`.

## Troubleshooting
- Ensure the Apify token has access to the actors listed in the environment variables.
- If a run returns `Apify error`, check the Apify dashboard for execution logs.
- Clear the `exports/` directory periodically if you generate many leads; files are not automatically pruned.
