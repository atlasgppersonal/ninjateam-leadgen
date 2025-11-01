import requests
import json

# The URL of the http function emulator
url = "http://127.0.0.1:7001/fourth-outpost-470409-u3/us-central1/saveArbitrageData"

# The data to be pushed
data = {
    "data": {
        "id": "moving-services",
        "displayName": "moving-services",
        "aliases": [],
        "description": "Minimal test data",
        "confidence": 1.0,
        "avgJobAmount": 800.0,
        "suggestedAt": "2025-10-31T04:38:19.845754+00:00",
        "createdBy": "surfer_consumer",
        "lastUpdated": "2025-10-31T04:38:19.845754+00:00",
        "location": "moving-services-or",
        "arbitrageData": {
            "scored_keywords": [
                {
                    "keyword": "minimal keyword",
                    "search_volume": 10,
                    "cpc": 1.0,
                    "competition": 0.1,
                    "arbitrage_score": 100.0,
                    "velocity_score": 1.0,
                    "time_impact": 0.6,
                    "estimated_time": 10.0,
                    "estimated_velocity": 10,
                    "base_value_score": 10.0,
                    "long_term_arbitrage_score": 10.0,
                    "competition_band": 1,
                    "content_angle": "Minimal angle",
                    "monetization": "Minimal monetization",
                    "low_roi": 10.0,
                    "high_roi": 100.0,
                    "roi": 50.0,
                    "raw_data": {
                        "categories": [10003],
                        "competition": 0.1,
                        "cpc": 1.0,
                        "search_volume": 10,
                        "similar_keywords": [],
                        "overlapping_pages": None,
                        "keyword": "minimal keyword"
                    }
                }
            ],
            "customer_domain_data": {},
            "short_term_strategy": {
                "top_4_clusters": [
                    {
                        "keyword": "minimal keyword",
                        "search_volume": 10,
                        "cpc": 1.0,
                        "competition": 0.1,
                        "arbitrage_score": 100.0,
                        "velocity_score": 1.0,
                        "time_impact": 0.6,
                        "estimated_time": 10.0,
                        "estimated_velocity": 10,
                        "base_value_score": 10.0,
                        "long_term_arbitrage_score": 10.0,
                        "competition_band": 1,
                        "content_angle": "Minimal angle",
                        "monetization": "Minimal monetization",
                        "low_roi": 10.0,
                        "high_roi": 100.0,
                        "roi": 50.0,
                        "raw_data": {
                            "categories": [10003],
                            "competition": 0.1,
                            "cpc": 1.0,
                            "search_volume": 10,
                            "similar_keywords": [],
                            "overlapping_pages": None,
                            "keyword": "minimal keyword"
                        }
                    }
                ],
                "max_time_to_implement": 1.0
            }
        },
        "serviceRadiusCities": ["Minimal City"],
        "cityClusters": {
            "Minimal City": [
                {
                    "cluster_id": "Minimal City-0",
                    "keywords": ["minimal keyword"],
                    "cluster_value_score": 100.0,
                    "content_ideas": {
                        "title": "Minimal Title",
                        "content_angle": "Minimal Angle",
                        "target_audience": "Minimal Audience",
                        "key_questions": ["Minimal Question"]
                    },
                    "title": "Minimal Title"
                }
            ]
        }
    }
}

try:
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, json=data, headers=headers)
    response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
    print("Data successfully posted!")
    print("Response:", response.json())
except requests.exceptions.RequestException as e:
    print(f"Error posting data: {e}")
    if e.response:
        print(f"Response status code: {e.response.status_code}")
        print(f"Response text: {e.response.text}")
except json.JSONDecodeError:
    print("Error decoding JSON response from the server.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
