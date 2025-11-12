## prospect_groq.py
## Uses Groq API instead of Perplexity to get company names and websites.
## Author: Ritvik Pa

import os
import requests
import re
import json
from dotenv import load_dotenv

load_dotenv()

# ✅ STEP 1: Build your structured prompt
def generate_company_prompt(industry: str, location: str, size_range: str,
                            revenue_range: str = "$500K–$50M annual revenue (growth-stage or enterprise-level spenders)") -> str:
    base_context = (
        "You are a B2B market research assistant. "
        "Find verified companies that match the given firmographic filters."
    )

    firmographics = f"""
Industry Focus:
→ {industry}

Company Size:
→ {size_range}

Revenue Range:
→ {revenue_range}

Geography:
→ {location}
"""

    instructions = (
        "Task:\n"
        "Generate a ranked list of 10–20 verified companies that best fit this profile. "
        "For each company, include:\n"
        "- Company Name\n"
        "- Website URL\n\n"
        "Exclude Fortune 50 giants and micro-startups. "
        "Use reliable, verifiable data sources like Crunchbase, LinkedIn, or company websites."
    )

    prompt = f"""{base_context}

Firmographic Filters:
{firmographics}

{instructions}
"""
    return prompt


# ✅ STEP 2: Query Groq API
def query_groq(prompt: str) -> str:
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise EnvironmentError("❌ Missing GROQ_API_KEY environment variable.")

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    # Example: using Groq’s Mixtral model (fast + large context)
    payload = {
        "model": "llama-3.1-8b-instant",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3,
        "max_tokens": 1200
    }

    response = requests.post("https://api.groq.com/openai/v1/chat/completions", json=payload, headers=headers)
    if response.status_code != 200:
        raise RuntimeError(f"API error {response.status_code}: {response.text}")

    return response.json()["choices"][0]["message"]["content"]


# ✅ STEP 3: Extract company names + websites from Groq output
def parse_companies(text: str):
    results = []
    for line in text.splitlines():
        line = line.strip()
        # Examples handled:
        # "1. Acme Corp – https://acme.com"
        # "• Acme Corp: https://acme.com"
        match = re.search(r"(?:(?:\d+\.|[-•]))\s*([A-Za-z0-9&().,\s]+?)[:–-]\s*(https?://[^\s]+)", line)
        if match:
            company = match.group(1).strip()
            website = match.group(2).strip()
            results.append({"company": company, "website": website})
    return results


if __name__ == "__main__":
    # Simulated frontend input
    industry = "Hospitality and Travel"
    location = "San Francisco Bay Area"
    size_range = "100–5000 employees"
    revenue_range = "$500K–$50M annual revenue (growth-stage and enterprise-level spenders)"

    # Step 1: Build prompt
    prompt = generate_company_prompt(industry, location, size_range, revenue_range)
    print("🧠 Generated Prompt:\n", prompt)

    # Step 2: Query Groq
    print("\n🚀 Querying Groq API...")
    raw_response = query_groq(prompt)
    print("\n📝 Raw Response:\n", raw_response)

    # Step 3: Parse results
    companies = parse_companies(raw_response)
    print("\n✅ Parsed Companies:")
    print(json.dumps(companies, indent=2))
