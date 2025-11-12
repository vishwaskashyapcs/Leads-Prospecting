import os
import json
from scrapegraph_py import Client
from scrapegraph_py.exceptions import APIError

# Initialize the client using an environment variable for the API key
API_KEY = "sgai-1a175411-5324-4bd5-a416-705d992dd284"
if not API_KEY:
    print("SG_API_KEY environment variable not set. Set it and retry.")
    raise SystemExit(1)

client = Client(api_key=API_KEY)

# SearchScraper request (AI extraction mode)
try:
    response = client.searchscraper(
        user_prompt="""to extract company names.

Example query for your script:

"top SaaS companies in San Francisco Bay Area with 100-5000 employees"
"IT transformation firms in Austin Texas"
""",
        # pydantic validation requires num_results >= 3
        num_results=3,
        extraction_mode=True,
    )

except APIError as e:
    err_text = str(e)
    # Friendly message for insufficient credits (HTTP 402)
    if "Insufficient credits" in err_text or "402" in err_text:
        print("API Error: Insufficient credits on your ScrapeGraph account.")
        print("Please top up your credits at the ScrapeGraph dashboard or use a different API key.")
        # Provide a small mock response so the script can continue for testing
        response = {
            "request_id": "mock-0000",
            "status": "mock",
            "user_prompt": "(mock) to extract company names...",
            "num_results": 3,
            "result": {"saas_companies": [
                {"name": "Example Co", "location": "San Francisco Bay Area", "employee_count": 123, "industry": "SaaS"}
            ]},
            "reference_urls": [],
            "markdown_content": None,
            "error": None,
        }
    else:
        # re-raise other API errors
        raise


def print_companies_table(result):
    rows = result.get("result", {}).get("saas_companies", [])
    if not rows:
        print("No companies found.")
        return

    # Columns to show
    cols = ["name", "location", "employee_count", "industry"]
    # compute column widths
    widths = {c: max(len(str(c)), max((len(str(r.get(c, ""))) for r in rows), default=0)) for c in cols}
    # header
    header = " | ".join(c.ljust(widths[c]) for c in cols)
    sep = "-+-".join("-" * widths[c] for c in cols)
    print(header)
    print(sep)
    for r in rows:
        line = " | ".join(str(r.get(c, "N/A") if r.get(c, None) is not None else "N/A").ljust(widths[c]) for c in cols)
        print(line)

# usage
print("Result metadata:")
print("request_id:", response.get("request_id"))
print("status:", response.get("status"))
print()
print_companies_table(response)

# Also pretty-print the full response JSON if you want to inspect everything
print("\nFull response (JSON):")
print(json.dumps(response, ensure_ascii=False, indent=2))