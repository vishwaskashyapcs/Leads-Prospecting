from custom_apify_client import ApifyClient

from dotenv import load_dotenv
import os, sys

load_dotenv()

# Initialize the ApifyClient with your Apify API token. The token is read from
# the environment variable APIFY_TOKEN (or from .env when load_dotenv() is used).
token = os.getenv("APIFY_TOKEN")
if not token:
    print("APIFY_TOKEN not found in environment. Set $env:APIFY_TOKEN or add it to .env and try again.")
    sys.exit(1)

client = ApifyClient(token)

# Prepare the Actor input
run_input = {
    "keyword": "Software Engineer",
    "maxItems": 200,
    "baseUrl": "https://www.glassdoor.com",
    "location": None,
    "includeNoSalaryJob": False,
    "companyName": None,
    "minSalary": 0,
    "maxSalary": None,
    "fromAge": None,
    "jobType": "all",
    "radius": "18",
    "industryType": "ALL",
    "domainType": "ALL",
    "employerSizes": "ALL",
    "applicationType": "ALL",
    "remoteWorkType": None,
    "seniorityType": "all",
    "minRating": "0",
    "proxy": {
        "useApifyProxy": True,
        "apifyProxyGroups": ["RESIDENTIAL"],
    },
}

# Run the Actor and wait for it to finish
run = client.actor("t2FNNV3J6mvckgV2g").call(run_input=run_input)

# Fetch and print Actor results from the run's dataset (if there are any)
for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    print(item)