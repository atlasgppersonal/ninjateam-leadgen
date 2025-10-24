#!/usr/bin/env python3
"""
surfer_prospecting.py

Poor-man's SEO arbitrage prospecting using the Keyword Surfer API.
- Build a keyword pool by expanding seed -> similar_keywords (2 layers)
- Query /domains for competitor footprints (per keyword)
- Score keywords using real search_volume / cpc / competition
- Throttle API calls randomly between 1-3s to avoid triggering flags
- Output JSON + CSV summary for quick prospecting reports

Notes:
- No paid keys required for the shown Surfer endpoints (based on your tests).
- Adjust TARGET_POOL_SIZE if you want more/less expansion (default 50).
"""

import requests
import json
import math
import time
import random
import urllib.parse
import csv
from collections import defaultdict
from datetime import datetime
import sys
from typing import List, Dict, Any, Tuple, Set

# ---------------- CONFIG ----------------
COUNTRY = "US"
SEED_KEYWORDS = [
    "plumber orlando", "emergency plumber orlando", "24 hour plumber orlando",
    "drain cleaning orlando", "water heater repair orlando", "leak detection orlando",
    "commercial plumbing orlando", "affordable plumber orlando", "best plumber orlando",
    "local plumber orlando"
]
CUSTOMER_DOMAIN = "https://www.benjaminfranklinplumbing.com/"
TARGET_POOL_SIZE = 50
MAX_BATCH_SIZE = 6 # Max items per batch
MIN_VOLUME_FILTER = 20
OUTPUT_JSON = "surfer_prospecting_output.json"
OUTPUT_CSV = "surfer_prospecting_output.csv"
LOG_FILE = "surfer_prospecting.log"

BASE_URL = "https://db3.keywordsur.fr/api/ks"

# Per-call throttle timings (seconds)
PER_CALL_THROTTLE_MIN = 0.5
PER_CALL_THROTTLE_MAX = 1.5
THROTTLE_JITTER_PERCENTAGE = 0.25 # ±25% jitter

# Window-based pause timings (seconds)
WINDOW_PAUSE_MIN = 6.0
WINDOW_PAUSE_MAX = 10.0

# Dynamic delay when approaching window limit
# Removed as per user request.

# Query parameter length limits
MAX_QUERY_PARAM_LENGTH_SOFT = 400
MAX_QUERY_PARAM_LENGTH_HARD = 500

# Requests per window for human-like pauses
REQUESTS_PER_WINDOW = 5

# Global counter for requests within a window
_request_counter = 0

# Global list to track request timestamps for rate limiting
_request_timestamps = []

# Default headers to mimic browser behavior
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.google.com",
    "Referer": "https://www.google.com/",
    "Sec-CH-UA": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
    "Sec-CH-UA-Mobile": "?0",
    "Sec-CH-UA-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "cross-site",
    "Priority": "u=1, i"
}

# ---------------- helpers ----------------
def throttle(base_min: float = PER_CALL_THROTTLE_MIN, base_max: float = PER_CALL_THROTTLE_MAX):
    # Apply jitter to base throttle times
    min_jitter = base_min * THROTTLE_JITTER_PERCENTAGE
    max_jitter = base_max * THROTTLE_JITTER_PERCENTAGE
    
    actual_min = max(0.1, base_min - random.uniform(0, min_jitter)) # Ensure min is not negative
    actual_max = base_max + random.uniform(0, max_jitter)
    
    t = random.uniform(actual_min, actual_max)
    log_message("DEBUG", f"Throttling for {t:.2f} seconds (base: {base_min}-{base_max}, actual: {actual_min:.2f}-{actual_max:.2f}).")
    time.sleep(t)

def get_encoded_query_param_length(items: List[str]) -> int:
    """
    Returns the length of the URL-encoded JSON array representation of the items.
    This is the length of the query parameter value itself.
    """
    return len(urllib.parse.quote_plus(json.dumps(items, separators=(',', ':')), safe=''))

def safe_get(url: str, params: dict = None, timeout: int = 15, attempt: int = 1) -> Tuple[int, Any]:
    global _request_counter
    global _request_timestamps

    _request_counter += 1

    # Apply window-based pause
    if _request_counter % REQUESTS_PER_WINDOW == 0:
        pause_time = random.uniform(WINDOW_PAUSE_MIN, WINDOW_PAUSE_MAX)
        log_message("INFO", f"Requests per window limit reached ({REQUESTS_PER_WINDOW}). Pausing for {pause_time:.2f} seconds.")
        time.sleep(pause_time)
        _request_counter = 0 # Reset counter after pause
    # Removed dynamic delay as per user request.
    
    # Prepare headers for this request
    headers = DEFAULT_HEADERS.copy()
    
    # Add query-string micro-variation
    if params is None:
        params = {}

    log_message("DEBUG", f"Making GET request to: {url} (Attempt {attempt})")
    if params:
        log_message("DEBUG", f"Request parameters: {params}")
    log_message("DEBUG", f"Request headers: {headers}")

    try:
        r = requests.get(url, params=params, timeout=timeout, headers=headers)
        log_message("DEBUG", f"Received response status: {r.status_code}")
        content_type = r.headers.get("content-type", "")
        if content_type.startswith("application/json"):
            json_data = r.json()
            log_message("DEBUG", f"Response JSON (first 500 chars): {str(json_data)[:500]}")
            return r.status_code, json_data
        else:
            log_message("DEBUG", f"Response text (first 500 chars): {r.text[:500]}")
            return r.status_code, r.text
    except requests.exceptions.Timeout:
        log_message("ERROR", f"Request timed out after {timeout} seconds for URL: {url}")
        return 0, {"error": "Request timed out"}
    except requests.exceptions.RequestException as e:
        log_message("ERROR", f"Request failed for URL: {url} - {e}")
        return 0, {"error": str(e)}
    except Exception as e:
        log_message("ERROR", f"An unexpected error occurred in safe_get for URL: {url} - {e}")
        return 0, {"error": str(e)}

def urlencode_json_array(arr: List[str]) -> str:
    log_message("DEBUG", f"Encoding JSON array for URL: {arr}")
    raw = json.dumps(arr, separators=(',', ':'))
    encoded = urllib.parse.quote_plus(raw, safe='')
    log_message("DEBUG", f"Encoded array: {encoded}")
    return encoded

# ---------------- Surfer API functions ----------------
def fetch_keywords_for_batch(keywords: List[str], country: str = COUNTRY, retry_attempt: int = 0) -> Dict[str, Any]:
    log_message("INFO", f"Fetching keywords for batch (size: {len(keywords)}) for country: {country})")
    if not keywords:
        log_message("DEBUG", "No keywords provided for batch fetch. Returning empty.")
        return {}

    url = f"{BASE_URL}/keywords"
    encoded = urlencode_json_array(keywords)
    full_url = f"{url}?country={urllib.parse.quote(country)}&keywords={encoded}"
    
    status, data = safe_get(full_url)
    throttle() # Throttle after the attempt, regardless of success

    if status == 200:
        log_message("INFO", f"Successfully fetched /keywords for batch. Received {len(data)} entries.")
        return data if isinstance(data, dict) else {}
    else:
        log_message("WARN", f"/keywords batch call failed (status={status}) for batch size {len(keywords)}. Response: {data}")
        
        # Exponential backoff for 500/502 errors
        if status in [500, 502] and retry_attempt < 3: # Max 3 retry attempts for batch
            wait_time = 2 ** retry_attempt
            log_message("INFO", f"Implementing exponential backoff. Waiting {wait_time} seconds before retrying batch.")
            time.sleep(wait_time)
            return fetch_keywords_for_batch(keywords, country, retry_attempt + 1)
        elif status == 422:
            log_message("WARN", f"/keywords batch call failed with 422 (client/format issue). Not retrying batch, falling back to singles.")
        else:
            log_message("WARN", f"/keywords batch call failed (status={status}). Not retrying batch, falling back to singles.")

        log_message("INFO", "Retrying keywords as singles due to batch failure...")
        single_results = {}
        for kw in keywords:
            log_message("DEBUG", f"Retrying single keyword: '{kw}'")
            single_encoded = urlencode_json_array([kw])
            single_full_url = f"{url}?country={urllib.parse.quote(country)}&keywords={single_encoded}"
            
            single_retry_attempt = 0
            while single_retry_attempt < 3: # Max 3 retry attempts for single
                single_status, single_data = safe_get(single_full_url)
                throttle() # Throttle after each single retry attempt

                if single_status == 200:
                    if isinstance(single_data, dict) and kw in single_data:
                        single_results[kw] = single_data[kw]
                        log_message("DEBUG", f"Successfully retrieved data for single keyword: '{kw}'.")
                    else:
                        log_message("WARN", f"Single keyword '{kw}' response malformed or missing data.")
                    break # Success, break out of retry loop
                elif single_status in [500, 502]:
                    wait_time = 2 ** single_retry_attempt
                    log_message("INFO", f"Implementing exponential backoff for single keyword '{kw}'. Waiting {wait_time} seconds before retrying.")
                    time.sleep(wait_time)
                    single_retry_attempt += 1
                elif single_status == 422:
                    log_message("WARN", f"Single keyword '{kw}' fetch failed with 422 (client/format issue). Not retrying.")
                    break # Non-retryable error
                else:
                    log_message("WARN", f"Single keyword '{kw}' fetch failed (status={single_status}). No further retries for this status.")
                    break # Non-500/502 error, no further retries

            if single_retry_attempt == 3:
                log_message("ERROR", f"Single keyword '{kw}' failed after multiple exponential backoff retries.")

        log_message("INFO", f"Single retries completed. Successfully retrieved {len(single_results)} keywords.")
        return single_results

def fetch_customer_domain_data(domain: str, country: str = COUNTRY, retry_attempt: int = 0) -> Dict[str, Any]:
    """
    Call /domains for a single domain and return the response dict.
    """
    log_message("INFO", f"Fetching domain data for: {domain}")
    url = f"{BASE_URL}/domains"
    # The /domains endpoint expects a single domain in the 'domains' query parameter
    # It's not a batch endpoint for multiple domains in the same way /keywords is for keywords
    # So we pass the domain as a single-element list to urlencode_json_array
    
    # Extract only the hostname and remove 'www.' if present
    parsed_uri = urllib.parse.urlparse(domain)
    base_domain = parsed_uri.hostname
    if base_domain and base_domain.startswith("www."):
        base_domain = base_domain[4:] # Remove 'www.'
    
    encoded_domain = urlencode_json_array([base_domain])
    full_url = f"{url}?country={urllib.parse.quote(country)}&domains={encoded_domain}"
    
    status, data = safe_get(full_url)
    throttle() # Throttle after the attempt, regardless of success

    if status == 200:
        # The response for a single domain call to /domains is a dictionary where the key is the domain
        if isinstance(data, dict) and base_domain in data:
            log_message("INFO", f"Successfully fetched /domains for {base_domain}.")
            return data[base_domain]
        else:
            log_message("WARN", f"/domains response malformed or missing data for {base_domain}. Response: {data}")
            return {}
    else:
        log_message("WARN", f"/domains call failed (status={status}) for {base_domain}. Response: {data}")
        if status in [500, 502] and retry_attempt < 3:
            wait_time = 2 ** retry_attempt
            log_message("INFO", f"Implementing exponential backoff. Waiting {wait_time} seconds before retrying {base_domain}.")
            time.sleep(wait_time)
            return fetch_customer_domain_data(domain, country, retry_attempt + 1)
        else:
            log_message("ERROR", f"Failed to fetch domain data for {base_domain} after retries.")
            return {}


# ---------------- Pool builder ----------------
def build_keyword_pool(seed_keywords: List[str], target_size: int = TARGET_POOL_SIZE) -> Dict[str, Dict[str, Any]]:
    log_message("INFO", f"[START] Building keyword pool for seeds: {seed_keywords}, target size: {target_size}")
    pool_map: Dict[str, Dict[str, Any]] = {}
    all_candidates = []

    for seed in seed_keywords:
        # Step A: seed call
        log_message("INFO", f"Step A: Fetching data for seed keyword: '{seed}'")
        first_batch = [seed]
        first_resp = fetch_keywords_for_batch(first_batch)
        if not first_resp:
            log_message("ERROR", f"Failed to fetch seed keyword data for '{seed}'. Skipping this seed.")
            continue

        seed_entry = first_resp.get(seed) or {}
        if seed not in pool_map: # Avoid adding duplicates if multiple seeds are the same
            pool_map[seed] = seed_entry
            log_message("DEBUG", f"Added seed '{seed}' to pool_map. Current pool size: {len(pool_map)}")

        first_similar = []
        if seed_entry and isinstance(seed_entry.get("similar_keywords"), list):
            first_similar = [s.get("keyword") for s in seed_entry["similar_keywords"] if s.get("keyword")]
        log_message("INFO", f"Seed '{seed}' returned {len(first_similar)} similar keywords (first layer).")
        log_message("DEBUG", f"First layer similar keywords: {first_similar[:5]}...")

        for k in first_similar:
            if k not in pool_map:
                pool_map[k] = {} # Placeholder for now
                all_candidates.append(k)
    
    log_message("INFO", f"Initial candidates for second-layer expansion: {len(all_candidates)}")
    log_message("DEBUG", f"Candidates list (first 5): {all_candidates[:5]}...")

    idx = 0
    current_batch = []
    while len(pool_map) < target_size and idx < len(all_candidates):
        current_candidate = all_candidates[idx]
        
        # Check if adding the next candidate exceeds the soft limit or MAX_BATCH_SIZE
        test_batch = current_batch + [current_candidate]
        test_length = get_encoded_query_param_length(test_batch)
        
        if test_length > MAX_QUERY_PARAM_LENGTH_HARD:
            log_message("ERROR", f"Candidate '{current_candidate}' would cause query param length ({test_length}) to exceed HARD limit ({MAX_QUERY_PARAM_LENGTH_HARD}). Skipping this candidate.")
            idx += 1 # Skip this problematic candidate
            continue
        
        # Condition to send current batch: exceeds soft limit OR exceeds MAX_BATCH_SIZE
        if (test_length > MAX_QUERY_PARAM_LENGTH_SOFT or len(test_batch) > MAX_BATCH_SIZE) and len(current_batch) > 0:
            log_message("INFO", f"Current batch query param length ({get_encoded_query_param_length(current_batch)}) or size ({len(current_batch)}) would exceed SOFT limit ({MAX_QUERY_PARAM_LENGTH_SOFT}) or MAX_BATCH_SIZE ({MAX_BATCH_SIZE}). Sending current batch.")
            resp = fetch_keywords_for_batch(current_batch)
            for k in current_batch:
                if k in resp:
                    pool_map[k] = resp[k]
                    log_message("DEBUG", f"Added '{k}' payload to pool_map. Current pool size: {len(pool_map)}")
                    sim = resp[k].get("similar_keywords", []) if isinstance(resp[k], dict) else []
                    for s in sim:
                        kw = s.get("keyword")
                        if kw and kw not in pool_map:
                            pool_map[kw] = {}
                            all_candidates.append(kw) # Add to all_candidates for further expansion
                            log_message("DEBUG", f"Added new similar keyword '{kw}' to candidates.")
                else:
                    log_message("WARN", f"Keyword '{k}' not found in batch response during second-layer expansion (after soft limit/size split).")
            current_batch = [] # Reset batch
            log_message("DEBUG", f"Current pool size after batch processing: {len(pool_map)}. Total candidates: {len(all_candidates)}")
            
        current_batch.append(current_candidate)
        idx += 1
        log_message("DEBUG", f"Added '{current_candidate}' to current batch. Current batch size: {len(current_batch)}. Current query param length: {get_encoded_query_param_length(current_batch)}")

        # If we've processed all candidates or reached target size, send the last batch
        if idx == len(all_candidates) or len(pool_map) + len(current_batch) >= target_size:
            if current_batch:
                log_message("INFO", f"Sending final batch of {len(current_batch)} keywords for second-layer expansion.")
                resp = fetch_keywords_for_batch(current_batch)
                for k in current_batch:
                    if k in resp:
                        pool_map[k] = resp[k]
                        log_message("DEBUG", f"Added '{k}' payload to pool_map. Current pool size: {len(pool_map)}")
                        sim = resp[k].get("similar_keywords", []) if isinstance(resp[k], dict) else []
                        for s in sim:
                            kw = s.get("keyword")
                            if kw and kw not in pool_map:
                                pool_map[kw] = {}
                                all_candidates.append(kw) # Add to all_candidates for further expansion
                                log_message("DEBUG", f"Added new similar keyword '{kw}' to candidates.")
                else:
                    log_message("WARN", f"Keyword '{k}' not found in batch response during second-layer expansion (final batch).")
                current_batch = [] # Reset batch
                log_message("DEBUG", f"Current pool size after batch processing: {len(pool_map)}. Total candidates: {len(all_candidates)}")

    log_message("INFO", f"Finished second-layer expansion loop. Current pool size: {len(pool_map)}")

    missing = [k for k, v in pool_map.items() if not v]
    if missing:
        log_message("INFO", f"Fetching {len(missing)} missing keyword payloads in batches to fill pool.")
        current_missing_batch = []
        for kw in missing:
            test_batch = current_missing_batch + [kw]
            test_length = get_encoded_query_param_length(test_batch)
            
            if test_length > MAX_QUERY_PARAM_LENGTH_HARD:
                log_message("ERROR", f"Missing keyword '{kw}' would cause query param length ({test_length}) to exceed HARD limit ({MAX_QUERY_PARAM_LENGTH_HARD}). Skipping this missing keyword.")
                continue
            
            # Condition to send current missing batch: exceeds soft limit OR exceeds MAX_BATCH_SIZE
            if (test_length > MAX_QUERY_PARAM_LENGTH_SOFT or len(test_batch) > MAX_BATCH_SIZE) and len(current_missing_batch) > 0:
                log_message("INFO", f"Sending missing batch due to soft limit or size: {current_missing_batch}")
                resp = fetch_keywords_for_batch(current_missing_batch)
                for k in current_missing_batch:
                    if k in resp:
                        pool_map[k] = resp[k]
                        log_message("DEBUG", f"Filled missing payload for '{k}'.")
                    else:
                        log_message("WARN", f"Missing keyword '{k}' still not found after batch fetch (soft limit/size split).")
                current_missing_batch = []
            
            current_missing_batch.append(kw)
            log_message("DEBUG", f"Added '{kw}' to current missing batch. Current size: {len(current_missing_batch)}. Current query param length: {get_encoded_query_param_length(current_missing_batch)}")

        if current_missing_batch:
            log_message("INFO", f"Sending final missing batch: {current_missing_batch}")
            resp = fetch_keywords_for_batch(current_missing_batch)
            for k in current_missing_batch:
                if k in resp:
                    pool_map[k] = resp[k]
                    log_message("DEBUG", f"Filled missing payload for '{k}'.")
                else:
                    log_message("WARN", f"Missing keyword '{k}' still not found after final batch fetch.")

    log_message("INFO", f"Applying final deduplication and volume filter (MIN_VOLUME_FILTER={MIN_VOLUME_FILTER}).")
    cleaned_map = {}
    for k, payload in pool_map.items():
        vol = payload.get("search_volume") if isinstance(payload, dict) else None
        if vol is None:
            log_message("DEBUG", f"Keyword '{k}' has no search_volume. Keeping with default payload.")
            cleaned_map[k] = payload if isinstance(payload, dict) else {}
        elif vol >= MIN_VOLUME_FILTER:
            log_message("DEBUG", f"Keyword '{k}' (volume: {vol}) meets filter. Adding to cleaned_map.")
            cleaned_map[k] = payload if isinstance(payload, dict) else {}
        else:
            log_message("DEBUG", f"Keyword '{k}' (volume: {vol}) below filter. Skipping.")

    log_message("INFO", f"Built keyword pool w/ {len(cleaned_map)} items (target was {target_size}).")
    log_message("INFO", f"Exiting build_keyword_pool.")
    return cleaned_map

# ---------------- Scoring and clustering ----------------
def compute_arbitrage_score(volume: float, cpc: float, competition: float) -> float:
    """
    Arbitrage-lite score:
    - Volume (log10 scaled)
    - CPC (value potential)
    - Competition (inverse: lower comp = higher score)
    """
    vol_score = math.log10(volume) if volume > 0 else 0.0
    cpc_adjusted = cpc + 1.0 # Boosts high-value clicks
    comp_adjusted = competition + 0.1 # So that low competition keywords float to the top.
    score = (vol_score * cpc_adjusted) / comp_adjusted
    log_message("DEBUG", f"Volume score (log10): {vol_score:.2f}, Adjusted CPC: {cpc_adjusted:.2f}, Adjusted Competition: {comp_adjusted:.2f}")
    log_message("INFO", f"Arbitrage score calculated: {score:.6f}")
    return score

def classify_content_angle(competition: float) -> str:
    if competition < 0.33:
        return "Quick wins / long-tail blog"
    elif 0.33 <= competition < 0.66:
        return "Comparison / listicle"
    else:
        return "In-depth guide / landing page"

def classify_monetization(cpc: float) -> str:
    if cpc > 5.0:
        return "Service / conversion page"
    elif 1.0 <= cpc <= 5.0:
        return "Lead gen blog post"
    else:
        return "Top-of-funnel explainer"

def generate_title(keyword: str) -> str:
    # Simple heuristic for title generation
    # You can expand this with more sophisticated logic based on keyword intent
    if "emergency" in keyword.lower():
        return f"Emergency {keyword.title()}: 24/7 Fast Response"
    elif "cheap" in keyword.lower() or "affordable" in keyword.lower():
        return f"Affordable {keyword.title()}: Save on Quality Repairs"
    elif "best" in keyword.lower():
        return f"Best {keyword.title()}: Top Rated Services"
    else:
        return f"{keyword.title()} Services: Your Local Experts"


def cluster_keywords_by_overlap(keywords: List[str], min_common_words: int = 2) -> List[Dict[str, Any]]:
    log_message("INFO", f"Starting keyword clustering with {len(keywords)} keywords, min_common_words={min_common_words}.")
    clusters = []
    used = set()
    for i, kw in enumerate(keywords):
        if kw in used:
            log_message("DEBUG", f"Keyword '{kw}' already used, skipping.")
            continue
        primary = kw
        primary_words = set(primary.lower().split())
        cluster = {"primary": primary, "related": []}
        used.add(primary)
        log_message("DEBUG", f"New cluster started with primary: '{primary}'.")
        for j in range(i+1, len(keywords)):
            cand = keywords[j]
            if cand in used:
                log_message("DEBUG", f"Candidate '{cand}' already used, skipping.")
                continue
            cand_words = set(cand.lower().split())
            common_words = primary_words.intersection(cand_words)
            if len(common_words) >= min_common_words:
                cluster["related"].append(cand)
                used.add(cand)
                log_message("DEBUG", f"Added '{cand}' to cluster for '{primary}' (common words: {len(common_words)}).")
            else:
                log_message("DEBUG", f"Candidate '{cand}' does not meet common word threshold for '{primary}'.")
        clusters.append(cluster)
        log_message("DEBUG", f"Cluster for '{primary}' finalized with {len(cluster['related'])} related keywords.")
    log_message("INFO", f"Finished clustering. Generated {len(clusters)} clusters.")
    return clusters

# ---------------- Runner / main ----------------
def run_prospecting(seed_keywords: List[str] = SEED_KEYWORDS, customer_domain: str = CUSTOMER_DOMAIN, target_size: int = TARGET_POOL_SIZE) -> Dict[str, Any]:
    log_message("INFO", f"[START] Surfer prospecting run for seeds: '{seed_keywords}', target pool size: {target_size}")

    log_message("INFO", "Building keyword pool...")
    keyword_payloads = build_keyword_pool(seed_keywords, target_size=target_size)
    log_message("INFO", f"Keyword pool built with {len(keyword_payloads)} entries.")

    keywords = list(keyword_payloads.keys())
    log_message("INFO", f"Total unique keywords to evaluate: {len(keywords)}")

    log_message("INFO", f"Fetching customer domain data for: {customer_domain}...")
    customer_domain_data = fetch_customer_domain_data(customer_domain)
    customer_da_proxy = {
        "domain": customer_domain,
        "keyword_count_top10": customer_domain_data.get("keyword_count_top10", 0),
        "traffic": customer_domain_data.get("traffic", 0.0)
    }
    log_message("INFO", f"Customer DA proxy: {customer_da_proxy}")

    scored = []
    log_message("INFO", "Scoring keywords (competition-based)...")
    for k in keywords:
        payload = keyword_payloads.get(k) or {}
        vol = payload.get("search_volume") or 0
        cpc = payload.get("cpc") or 0.0
        comp = payload.get("competition") or 0.0
        log_message("DEBUG", f"Processing keyword '{k}': volume={vol}, cpc={cpc}, competition={comp}")
        if vol is None or vol < MIN_VOLUME_FILTER:
            log_message("DEBUG", f"Skipping keyword '{k}' due to low volume ({vol}) or missing data (MIN_VOLUME_FILTER={MIN_VOLUME_FILTER}).")
            continue
        score = compute_arbitrage_score(float(vol), float(cpc), float(comp))
        
        blueprint = {
            "title_idea": generate_title(k),
            "content_angle": classify_content_angle(float(comp)),
            "monetization_hint": classify_monetization(float(cpc))
        }

        scored.append({
            "keyword": k,
            "search_volume": vol,
            "cpc": cpc,
            "competition": comp,
            "arbitrage_score": round(score, 6),
            "blueprint": blueprint,
            "customer_domain_authority": customer_da_proxy
        })
        log_message("DEBUG", f"Keyword '{k}' scored: {score:.6f}")

    log_message("INFO", f"Finished scoring. {len(scored)} keywords have been scored.")
    log_message("INFO", "Sorting scored keywords by arbitrage score (descending)...")
    scored_sorted = sorted(scored, key=lambda x: x["arbitrage_score"], reverse=True)
    log_message("DEBUG", f"Top 5 scored keywords: {scored_sorted[:5]}")

    log_message("INFO", "Clustering keywords by overlap...")
    ordered_keywords = [s["keyword"] for s in scored_sorted]
    clusters = cluster_keywords_by_overlap(ordered_keywords)
    log_message("INFO", f"Generated {len(clusters)} keyword clusters.")
    log_message("DEBUG", f"First 3 clusters: {clusters[:3]}")

    output = {
        "seed_keywords": seed_keywords,
        "customer_domain": customer_domain,
        "customer_domain_authority": customer_da_proxy,
        "total_keywords_in_pool": len(keywords),
        "scored_keywords": scored_sorted,
        "clusters": clusters,
    }
    log_message("INFO", "Prospecting run summary generated.")

    log_message("INFO", f"Writing outputs to {OUTPUT_JSON} and {OUTPUT_CSV}...")
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)
    log_message("DEBUG", f"JSON output written to {OUTPUT_JSON}.")
    
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "keyword", "search_volume", "cpc", "competition", "arbitrage_score",
            "blueprint_title_idea", "blueprint_content_angle", "blueprint_monetization_hint",
            "customer_domain", "customer_da_keyword_count_top10", "customer_da_traffic"
        ])
        for r in scored_sorted:
            writer.writerow([
                r["keyword"], r["search_volume"], r["cpc"], r["competition"], r["arbitrage_score"],
                r["blueprint"]["title_idea"], r["blueprint"]["content_angle"], r["blueprint"]["monetization_hint"],
                r["customer_domain_authority"]["domain"],
                r["customer_domain_authority"]["keyword_count_top10"],
                r["customer_domain_authority"]["traffic"]
            ])
    log_message("DEBUG", f"CSV output written to {OUTPUT_CSV}.")

    log_message("DONE", f"Surfer prospecting run completed. Outputs written: {OUTPUT_JSON}, {OUTPUT_CSV}")
    return output

import sys

# Store original stdout
original_stdout = sys.stdout

def log_message(level: str, message: str):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    formatted_message = f"[{timestamp}] [{level}] {message}"
    
    # Write to log file (redirected stdout)
    print(formatted_message)
    
    # Write to original stdout (console)
    original_stdout.write(formatted_message + "\n")

if __name__ == "__main__":
    # Redirect stdout to log file
    log_file_path = LOG_FILE
    sys.stdout = open(log_file_path, 'w', encoding='utf-8')
    
    try:
        log_message("INFO", "\n--- Starting Surfer Prospecting Script ---")
        out = run_prospecting(SEED_KEYWORDS, CUSTOMER_DOMAIN, TARGET_POOL_SIZE)
        top = out["scored_keywords"][:10]
        log_message("INFO", "\n--- Top 10 Opportunities ---")
        if not top:
            log_message("INFO", "No opportunities found based on current filters and data.")
        for i, item in enumerate(top, 1):
            log_message("INFO", f"{i:02d}. {item['keyword']} | vol={item['search_volume']} | cpc={item['cpc']} | comp={item['competition']} | score={item['arbitrage_score']}")
            log_message("INFO", f"   Blueprint: Title='{item['blueprint']['title_idea']}', Angle='{item['blueprint']['content_angle']}', Monetization='{item['blueprint']['monetization_hint']}'")
            log_message("INFO", f"   Customer DA: Domain='{item['customer_domain_authority']['domain']}', KeywordsTop10={item['customer_domain_authority']['keyword_count_top10']}, Traffic={item['customer_domain_authority']['traffic']}")
        log_message("INFO", "\n--- Script Finished ---")
    finally:
        # Restore original stdout
        sys.stdout.close()
        sys.stdout = original_stdout
