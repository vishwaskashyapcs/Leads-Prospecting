import os
import re
import json
import requests
from dotenv import load_dotenv

load_dotenv()

# STEP 1 — STRONGER PROMPT (with revenue constraint)
def generate_company_prompt(industry, location, size_range,
                            revenue_range="$500K–$50M annual revenue (growth-stage or enterprise-level spenders)"):
    base_context = (
        "You are a factual B2B market research assistant. "
        "You must only list verified, mid-sized companies based on public data from Crunchbase, LinkedIn, or company websites. "
        "Do NOT fabricate company names or financials."
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
        "List 10 verified companies that match ALL criteria. Each must include:\n"
        "- Company Name\n"
        "- Website URL\n"
        "- Approximate Annual Revenue (USD)\n"
        "- Headquarters (City, Country)\n"
        "- Employee Count\n"
        "- Verified Source (e.g., LinkedIn, Crunchbase)\n\n"
        "Rules:\n"
        "• Only include companies with estimated revenue between $500K–$50M.\n"
        "• Exclude any company with 50k+ employees or global Fortune 500 brands (e.g., Expedia, Airbnb).\n"
        "• If data is unavailable or uncertain, write 'Unknown'.\n"
        "• Ensure that each entry has a valid website domain.\n"
        "• Return results in Markdown list format.\n"
    )

    return f"{base_context}\n\nFirmographic Filters:\n{firmographics}\n{instructions}"


# STEP 2 — GROQ QUERY FUNCTION
def query_groq(prompt):
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise EnvironmentError("❌ Missing GROQ_API_KEY.")

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": "llama-3.1-8b-instant",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        "max_tokens": 1200
    }
    r = requests.post("https://api.groq.com/openai/v1/chat/completions", json=payload, headers=headers)
    if r.status_code != 200:
        raise RuntimeError(f"API error {r.status_code}: {r.text}")
    return r.json()["choices"][0]["message"]["content"]


# STEP 3 — IMPROVED PARSER (captures revenue, deduplicates)
def parse_companies(text):
    results, seen = [], set()
    pattern = re.compile(
        r"\*\*\s*(\d+\.?\s*)?([A-Za-z0-9&().,\s]+?)\*\*.*?"
        r"Website(?: URL)?:\s*(https?://[^\s]+).*?"
        r"(?:Revenue|Annual Revenue)[:\s$]*(~?\$?[0-9.,A-Za-z]+).*?"
        r"Employee(?:s| Count)[:\s]*([0-9.,A-Za-z+\-]+)",
        re.IGNORECASE | re.DOTALL,
    )

    for match in pattern.finditer(text):
        company = match.group(2).strip()
        website = match.group(3).strip()
        revenue = match.group(4).strip()
        employees = match.group(5).strip()

        if company not in seen:
            results.append({
                "company": company,
                "website": website,
                "revenue": revenue,
                "employees": employees
            })
            seen.add(company)

    return results


# MAIN EXECUTION
if __name__ == "__main__":
    industry = "Hospitality and Travel"
    location = "France"
    size_range = "100–5000 employees"
    revenue_range = "$500K–$50M annual revenue (growth-stage and enterprise-level spenders)"

    prompt = generate_company_prompt(industry, location, size_range, revenue_range)
    print("🧠 Generated Prompt:\n", prompt)

    print("\n🚀 Querying Groq API...")
    raw_response = query_groq(prompt)
    print("\n📝 Raw Response:\n", raw_response)

    companies = parse_companies(raw_response)
    print("\n✅ Parsed Companies:")
    print(json.dumps(companies, indent=2))
