import os
import re
import json
import requests
from dotenv import load_dotenv

load_dotenv()

# STEP 1 — STRENGTHENED PROMPT WITH EXPLICIT CONSTRAINTS
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
2. Website URL (must be valid and accessible)
3. Approximate Annual Revenue (in USD, must be between $500K-$50M)
4. Headquarters (City, Country)
5. Employee Count (must be within specified range)
6. Verified Source (e.g., LinkedIn, Crunchbase, company website)

STRICT EXCLUSION RULES (companies with ANY of these traits MUST be excluded):
❌ Annual revenue > $50 million USD
❌ Annual revenue < $500,000 USD
❌ Employee count > 5,000
❌ Employee count < 100
❌ Fortune 500 or Fortune 1000 companies
❌ Publicly traded multinational corporations (e.g., AccorHotels, Airbnb, Expedia, Booking.com)
❌ Large holding companies or conglomerates
❌ Companies with 50,000+ employees
❌ Subsidiaries of Fortune 500 companies (unless they operate independently with separate financials)

VALIDATION CHECKLIST (verify BEFORE including each company):
✓ Revenue is explicitly between $500K and $50M
✓ Employee count is between 100 and 5,000
✓ Company is NOT a household brand name
✓ Company is NOT publicly traded on major exchanges (unless small-cap with <$50M revenue)
✓ Source data is from a reputable platform (LinkedIn, Crunchbase, company website)

OUTPUT FORMAT (use this exact structure):
#### 1. **[Company Name]**
* Website URL: [URL]
* Approximate Annual Revenue: $[amount]M (Source: [platform])
* Headquarters: [City], [Country]
* Employee Count: [number] (Source: [platform])
* Verified Source: [platform names]

IMPORTANT NOTES:
• If you cannot find 10 companies that meet ALL criteria, return fewer companies rather than violating constraints.
• If revenue or employee data is uncertain, write "Data unavailable - excluded from results"
• Double-check each company against the exclusion rules before including it
• Prioritize smaller, lesser-known companies over recognizable brands
"""

    return f"{base_context}\n\n{firmographics}\n{instructions}"


# STEP 2 — GROQ QUERY WITH BETTER ERROR HANDLING
def query_groq(prompt, model="llama-3.1-8b-instant", temperature=0.1, max_tokens=1500):
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise EnvironmentError("❌ Missing GROQ_API_KEY in environment variables.")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": "You are a precise B2B research assistant. You strictly adhere to all constraints and never fabricate data. If you cannot verify information, you clearly state 'Data unavailable'."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        "temperature": temperature,
        "max_tokens": max_tokens
    }
    
    try:
        r = requests.post("https://api.groq.com/openai/v1/chat/completions", 
                         json=payload, headers=headers, timeout=30)
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"API request failed: {str(e)}")


# STEP 3 — ENHANCED PARSER WITH BETTER PATTERN MATCHING
def parse_companies(text):
    """
    Parses company information from markdown-formatted text.
    Handles multiple formats and variations in the response.
    """
    results, seen = [], set()
    
    # Split by company entries (looking for numbered headers)
    company_blocks = re.split(r'####\s*\d+\.\s*\*\*', text)
    
    for block in company_blocks[1:]:  # Skip first empty split
        try:
            # Extract company name (first line before **)
            company_match = re.search(r'^([^*\n]+)\*\*', block)
            if not company_match:
                continue
            company = company_match.group(1).strip()
            
            # Extract website
            website_match = re.search(r'Website(?: URL)?:\s*(https?://[^\s\n]+)', block, re.IGNORECASE)
            website = website_match.group(1).strip() if website_match else "Unknown"
            
            # Extract revenue (multiple patterns)
            revenue_patterns = [
                r'(?:Approximate\s+)?(?:Annual\s+)?Revenue:\s*\$?([\d.,]+)\s*([KMB])?(?:\s*million)?',
                r'Revenue:\s*\$?([\d.,]+)\s*([KMB])',
                r'\$?([\d.,]+)\s*([KMB])?\s*\((?:Source|Crunchbase)',
            ]
            revenue = "Unknown"
            for pattern in revenue_patterns:
                revenue_match = re.search(pattern, block, re.IGNORECASE)
                if revenue_match:
                    revenue_value = revenue_match.group(1).replace(',', '')
                    revenue_unit = revenue_match.group(2) if revenue_match.group(2) else "M"
                    
                    # Normalize to millions
                    try:
                        rev_num = float(revenue_value)
                        if revenue_unit.upper() == 'K':
                            rev_num /= 1000
                        elif revenue_unit.upper() == 'B':
                            rev_num *= 1000
                        revenue = f"${rev_num}M"
                    except ValueError:
                        revenue = f"${revenue_value}{revenue_unit}"
                    break
            
            # Extract headquarters
            hq_match = re.search(r'Headquarters:\s*([^\n*]+)', block, re.IGNORECASE)
            headquarters = hq_match.group(1).strip() if hq_match else "Unknown"
            
            # Extract employees (handle ranges and single values)
            emp_patterns = [
                r'Employee Count:\s*([\d,\-]+)(?:\s*\(Source)?',
                r'Employee(?:s)?:\s*([\d,\-]+)',
            ]
            employees = "Unknown"
            for pattern in emp_patterns:
                emp_match = re.search(pattern, block, re.IGNORECASE)
                if emp_match:
                    employees = emp_match.group(1).strip()
                    break
            
            # Extract source
            source_match = re.search(r'Verified Source:\s*([^\n]+)', block, re.IGNORECASE)
            source = source_match.group(1).strip() if source_match else "Unknown"
            
            # Add to results if not duplicate
            if company and company not in seen:
                results.append({
                    "company": company,
                    "website": website,
                    "revenue": revenue,
                    "headquarters": headquarters,
                    "employees": employees,
                    "source": source
                })
                seen.add(company)
                
        except Exception as e:
            print(f"⚠️  Warning: Failed to parse company block: {str(e)}")
            continue
    
    return results


# STEP 4 — POST-PROCESSING VALIDATION
def validate_companies(companies, min_revenue_m=0.5, max_revenue_m=50, min_employees=100, max_employees=5000):
    """
    Validates parsed companies against strict criteria.
    Returns list of valid companies and list of rejected companies with reasons.
    """
    valid = []
    rejected = []
    
    # Known large companies to exclude (Fortune 500, major brands)
    excluded_brands = {
        'accorhotels', 'accor', 'airbnb', 'booking.com', 'expedia', 'marriott',
        'hilton', 'hyatt', 'ihg', 'intercontinental', 'radisson', 'wyndham',
        'groupe barriere', 'barriere', 'pierre & vacances', 'pierre et vacances'
    }
    
    for company in companies:
        rejection_reasons = []
        
        # Check if it's a known large brand
        company_lower = company['company'].lower()
        if any(brand in company_lower for brand in excluded_brands):
            rejection_reasons.append("Known large enterprise/Fortune 500 brand")
        
        # Validate revenue
        revenue_str = company['revenue']
        try:
            # Extract numeric value from revenue string
            revenue_match = re.search(r'\$?([\d.]+)\s*([KMB])?', revenue_str)
            if revenue_match:
                revenue_value = float(revenue_match.group(1))
                revenue_unit = revenue_match.group(2) or 'M'
                
                # Convert to millions
                if revenue_unit.upper() == 'K':
                    revenue_value /= 1000
                elif revenue_unit.upper() == 'B':
                    revenue_value *= 1000
                
                if revenue_value > max_revenue_m:
                    rejection_reasons.append(f"Revenue ${revenue_value}M exceeds max ${max_revenue_m}M")
                elif revenue_value < min_revenue_m:
                    rejection_reasons.append(f"Revenue ${revenue_value}M below min ${min_revenue_m}M")
        except (ValueError, AttributeError):
            rejection_reasons.append(f"Unable to parse revenue: {revenue_str}")
        
        # Validate employee count
        employees_str = company['employees'].replace(',', '')
        try:
            # Handle ranges like "100-500"
            if '-' in employees_str:
                emp_parts = employees_str.split('-')
                emp_min = int(re.sub(r'[^\d]', '', emp_parts[0]))
                emp_max = int(re.sub(r'[^\d]', '', emp_parts[1]))
                
                if emp_max > max_employees:
                    rejection_reasons.append(f"Employee count {emp_max} exceeds max {max_employees}")
                elif emp_min < min_employees:
                    rejection_reasons.append(f"Employee count {emp_min} below min {min_employees}")
            else:
                emp_count = int(re.sub(r'[^\d]', '', employees_str))
                if emp_count > max_employees:
                    rejection_reasons.append(f"Employee count {emp_count} exceeds max {max_employees}")
                elif emp_count < min_employees:
                    rejection_reasons.append(f"Employee count {emp_count} below min {min_employees}")
        except (ValueError, IndexError):
            rejection_reasons.append(f"Unable to parse employee count: {employees_str}")
        
        # Add to appropriate list
        if rejection_reasons:
            rejected.append({
                **company,
                "rejection_reasons": rejection_reasons
            })
        else:
            valid.append(company)
    
    return valid, rejected


# MAIN EXECUTION WITH VALIDATION
if __name__ == "__main__":
    industry = "Hospitality and Travel"
    location = "France"
    size_range = "100–5000 employees"
    revenue_range = "$500K–$50M annual revenue (growth-stage and enterprise-level spenders)"

    print("=" * 80)
    print("🔍 COMPANY RESEARCH WITH STRICT VALIDATION")
    print("=" * 80)

    # Generate and display prompt
    prompt = generate_company_prompt(industry, location, size_range, revenue_range)
    print("\n🧠 Generated Prompt Preview (first 500 chars):")
    print(prompt[:500] + "...\n")

    # Query API
    print("🚀 Querying Groq API...")
    try:
        raw_response = query_groq(prompt)
        print("✅ API Response received\n")
        
        # Display raw response for debugging
        print("=" * 80)
        print("📝 RAW API RESPONSE")
        print("=" * 80)
        print(raw_response)
        print("\n" + "=" * 80 + "\n")
    except Exception as e:
        print(f"❌ API Error: {e}")
        exit(1)

    # Parse companies
    print("📊 Parsing companies from response...")
    companies = parse_companies(raw_response)
    print(f"   Found {len(companies)} companies in response")
    
    # Show what was parsed for debugging
    if companies:
        print("\n🔍 PARSED DATA (before validation):")
        print("-" * 80)
        for i, comp in enumerate(companies, 1):
            print(f"{i}. {comp['company']}")
            print(f"   Revenue: {comp['revenue']}")
            print(f"   Employees: {comp['employees']}")
            print(f"   HQ: {comp['headquarters']}")
            print(f"   Source: {comp['source']}")
        print("-" * 80 + "\n")
    else:
        print("⚠️  No companies were parsed! Check raw response above.\n")

    # Validate companies
    print("✅ Validating companies against constraints...")
    valid_companies, rejected_companies = validate_companies(companies)

    # Display results
    print("\n" + "=" * 80)
    print(f"✅ VALID COMPANIES: {len(valid_companies)}")
    print("=" * 80)
    if valid_companies:
        for i, comp in enumerate(valid_companies, 1):
            print(f"\n{i}. {comp['company']}")
            print(f"   🌐 Website: {comp['website']}")
            print(f"   💰 Revenue: {comp['revenue']}")
            print(f"   📍 Headquarters: {comp['headquarters']}")
            print(f"   👥 Employees: {comp['employees']}")
            print(f"   📊 Source: {comp['source']}")
        
        # Also save as JSON
        print("\n" + "-" * 80)
        print("JSON OUTPUT:")
        print("-" * 80)
        print(json.dumps(valid_companies, indent=2, ensure_ascii=False))
    else:
        print("❌ No companies passed validation!")

    if rejected_companies:
        print("\n" + "=" * 80)
        print(f"❌ REJECTED COMPANIES: {len(rejected_companies)}")
        print("=" * 80)
        for company in rejected_companies:
            print(f"\n🚫 {company['company']}")
            print(f"   Website: {company['website']}")
            print(f"   Revenue: {company['revenue']}")
            print(f"   Employees: {company['employees']}")
            print(f"   Rejection Reasons:")
            for reason in company['rejection_reasons']:
                print(f"      • {reason}")

    # Summary
    print("\n" + "=" * 80)
    print("📈 SUMMARY")
    print("=" * 80)
    print(f"Total companies found: {len(companies)}")
    print(f"Valid companies: {len(valid_companies)}")
    print(f"Rejected companies: {len(rejected_companies)}")
    print(f"Success rate: {len(valid_companies)/len(companies)*100:.1f}%" if companies else "N/A")
    print("=" * 80)