import os
import re
import json
import requests
from dotenv import load_dotenv

load_dotenv()


def generate_company_prompt(industry, location, size_range,
                            revenue_range="$500K–$50M annual revenue (growth-stage or enterprise-level spenders)"):

    base_context = (
        "You are a factual B2B market research assistant specializing in identifying mid-market companies. "
        "You MUST follow ALL constraints exactly. Any deviation will result in rejection of your output."
    )

    firmographics = f"""
MANDATORY FIRMOGRAPHIC FILTERS (ALL must be satisfied):

Industry Focus:
→ {industry}

Company Size:
→ {size_range}

Revenue Range (STRICT):
→ {revenue_range}
→ ABSOLUTE MAXIMUM: $50 million USD annual revenue
→ ABSOLUTE MINIMUM: $500,000 USD annual revenue

Geography:
→ {location}
"""

    instructions = """
TASK:
List exactly 10 verified companies that satisfy ALL criteria above.

REQUIRED FIELDS FOR EACH COMPANY:
1. Company Name
2. Website URL
3. Approximate Annual Revenue
4. Headquarters
5. Employee Count
6. Verified Source
7. Linkedln URL (if available)

OUTPUT FORMAT:
#### 1. **[Company Name]**
* Website URL: [URL]
* Approximate Annual Revenue: $[amount]M
* Headquarters: [City], [Country]
* Employee Count: [number]
* Verified Source: [platform]
"""

    return f"{base_context}\n\n{firmographics}\n{instructions}"


def query_groq(prompt, model="llama-3.1-8b-instant", temperature=0.1, max_tokens=1500):
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise EnvironmentError("Missing GROQ_API_KEY")

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": model,
        "messages": [
            {"role": "system",
             "content": "You are a precise B2B research assistant who never invents data."},
            {"role": "user", "content": prompt}
        ],
        "temperature": temperature,
        "max_tokens": max_tokens
    }

    r = requests.post("https://api.groq.com/openai/v1/chat/completions",
                      json=payload, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]


def parse_companies(text):
    results, seen = [], set()
    blocks = re.split(r'####\s*\d+\.\s*\*\*', text)

    for block in blocks[1:]:
        try:
            name = re.search(r'^([^*\n]+)\*\*', block).group(1).strip()
            website = re.search(r'Website(?: URL)?:\s*(https?://[^\s\n]+)', block)
            website = website.group(1) if website else "Unknown"
            revenue = re.search(r'Revenue:\s*\$?([^\n]+)', block)
            revenue = revenue.group(1) if revenue else "Unknown"
            hq = re.search(r'Headquarters:\s*([^\n]+)', block)
            hq = hq.group(1) if hq else "Unknown"
            employees = re.search(r'Employee(?: Count)?:\s*([^\n]+)', block)
            employees = employees.group(1) if employees else "Unknown"
            source = re.search(r'Verified Source:\s*([^\n]+)', block)
            source = source.group(1) if source else "Unknown"

            if name not in seen:
                seen.add(name)
                results.append({
                    "company": name,
                    "website": website,
                    "revenue": revenue,
                    "headquarters": hq,
                    "employees": employees,
                    "source": source
                })
        except:
            continue

    return results


def validate_companies(companies):
    valid, rejected = [], []
    for c in companies:
        try:
            emp = int(re.sub(r"[^\d]", "", c["employees"].split("-")[0]))
            rev = float(re.sub(r"[^\d.]", "", c["revenue"]))
        except:
            rejected.append(c)
            continue

        if 100 <= emp <= 5000 and 0.5 <= rev <= 50:
            valid.append(c)
        else:
            rejected.append(c)
    return valid, rejected


ROLE_FILTERS = [
    "CIO", "CTO", "VP Engineering", "VP of Engineering",
    "Director Delivery", "Head of PMO", "Chief Transformation Officer",
    "Chief Digital Officer", "IT Director", "Technology Director"
]

def fetch_contacts_from_serpapi(company_name):
    serp_key = os.getenv("SERPAPI_KEY")
    if not serp_key:
        raise EnvironmentError("Missing SERPAPI_KEY")

    role_query = " OR ".join([f'"{r}"' for r in ROLE_FILTERS])
    q = f"\"{company_name}\" {role_query} site:linkedin.com/in"

    params = {
        "engine": "google",
        "q": q,
        "num": 10,
        "api_key": serp_key
    }

    r = requests.get("https://serpapi.com/search.json", params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def parse_contacts(serp_json):
    contacts = []
    results = serp_json.get("organic_results", [])

    for r in results:
        link = r.get("link", "")
        if "linkedin.com/in" not in link:
            continue

        title = r.get("title", "")
        name = title.split(" – ")[0].strip()
        role = title.split(" – ")[1].strip() if " – " in title else r.get("snippet", "")

        contacts.append({
            "name": name,
            "role": role,
            "linkedin": link
        })

    return contacts



if __name__ == "__main__":
    prompt = generate_company_prompt(
        industry="Technology & IT Services",
        location="France",
        size_range="100–5000 employees"
    )

    print("\n🔍 Querying Groq AI...")
    groq_output = query_groq(prompt)
    companies = parse_companies(groq_output)
    valid, rejected = validate_companies(companies)

    print(f"\n🏢 Valid Companies Found: {len(valid)}")

    # 🆕 print all companies to console
    print("\n📌 Companies retrieved from Groq:")
    for idx, c in enumerate(valid, start=1):
        print(f"{idx}. {c['company']}")

    final_output = []   # 🆕 this will store required format

    for comp in valid:
        company_name = comp["company"]
        print(f"\n🔎 Fetching contacts for: {company_name}...")

        try:
            serp_json = fetch_contacts_from_serpapi(company_name)

            # 🔥 PRINT FULL SERP OUTPUT FOR DEBUGGING
            print("\n🟦 RAW SERPAPI OUTPUT:")
            print(json.dumps(serp_json, indent=2, ensure_ascii=False))

            contacts = parse_contacts(serp_json)
            contacts = contacts[:1]

        except Exception as e:
            print(f"   ❌ Failed -> {e}")
            contacts = []

        final_output.append({
            "company": {
                "company": comp["company"],
                "website": comp["website"],
                "revenue": comp["revenue"],
                "employees": comp["employees"],
                "headquarters": comp["headquarters"],
                "verified_source": comp["source"]
            },
            "people": contacts
        })

        print(f"   ✓ Added {len(contacts)} people")


    with open("company_contacts.json", "w", encoding="utf-8") as f:
        json.dump(final_output, f, indent=2, ensure_ascii=False)

#     with open("company_contacts.json", "w", encoding="utf-8") as f:
#    # 📁 ensure output folder exists
#         output_dir = "output"
#         os.makedirs(output_dir, exist_ok=True)
#         output_file = os.path.join(output_dir, "company_contacts.json")

#         with open(output_file, "w", encoding="utf-8") as f:
#             json.dump(final_output, f, indent=2, ensure_ascii=False)

#         print(f"\n🎉 DONE — Exported → {output_file}")

    

    print("\n🎉 DONE — Exported → company_contacts.json")