import requests
import json

API_TOKEN = "YOUR_API_TOKEN_HERE" # TODO: Replace with environment variable
API_URL = f"https://api.apify.com/v2/acts/apify~google-search-scraper/run-sync-get-dataset-items?token={API_TOKEN}"

payload = {
    "focusOnPaidAds": True,
    "forceExactMatch": False,
    "includeIcons": False,
    "includeUnfilteredResults": False,
    "maxPagesPerQuery": 1,
    "maximumLeadsEnrichmentRecords": 0,
    "mobileResults": False,
    "queries": "serpstat api",
    "resultsPerPage": 100,
    "saveHtml": False,
    "saveHtmlToKeyValueStore": True
}

def run_scraper(query: str):
    payload["queries"] = query
    print(f"🔎 Running Apify scraper for query: {query}")

    resp = requests.post(API_URL, json=payload)
    resp.raise_for_status()

    data = resp.json()
    print(json.dumps(data, indent=2)[:1000])  # show first ~1000 chars

    # Simple scoring
    top_ads = [item for item in data if item.get("type") == "ad" and item.get("positionGroup") == "top"]
    bottom_ads = [item for item in data if item.get("type") == "ad" and item.get("positionGroup") == "bottom"]
    shopping_ads = [item for item in data if item.get("type") == "shopping"]

    score = 100 - (len(top_ads) * 20 + len(bottom_ads) * 10 + len(shopping_ads) * 25)
    band = (
        "Excellent" if score >= 80 else
        "Good" if score >= 60 else
        "Weak" if score >= 40 else
        "Poor"
    )

    result = {
        "query": query,
        "ads_top": len(top_ads),
        "ads_bottom": len(bottom_ads),
        "ads_shopping": len(shopping_ads),
        "score": max(score, 0),
        "band": band
    }
    return result

if __name__ == "__main__":
    result = run_scraper("serpstat api")
    print("\n--- Final SERP Analysis ---")
    print(json.dumps(result, indent=2))
