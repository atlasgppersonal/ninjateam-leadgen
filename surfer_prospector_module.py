"""
surfer_prospector_module.py

Primary module for interacting with Surfer's API to:
- Fetch keyword data in batches with throttling & retries
- Expand seed keywords into a location-aware keyword pool
- Fetch domain metrics
- Provide a pool builder that filters by volume/cpc/competition

Scoring, clustering and LLM content generation utilities are intentionally
moved to scoring_utils.py to keep responsibilities separated.
"""

import json
import math
import time
import random
import urllib.parse
import csv
from collections import defaultdict
from datetime import datetime, timedelta
import sys
from typing import List, Dict, Any, Tuple, Set
import httpx  # Async HTTP client
import asyncio
import logging
import os

# Configure logging for this module
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG) # Set to DEBUG to capture all messages

# Create a file handler for this module's logs
log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "surfer_prospector_module.log")
file_handler = logging.FileHandler(log_file_path)
file_handler.setLevel(logging.DEBUG) # Log all messages to file

# Create a formatter and add it to the file handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Add the file handler to the logger
logger.addHandler(file_handler)

# Optionally, add a stream handler to also output to console (e.g., for INFO and above)
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

logger.info(f"Surfer Prospector Module logging to {log_file_path}")

# Import scoring and clustering utilities from the new module
from scoring_utils import (
    calculate_keyword_arbitrage_score,
    calculate_velocity,
    calculate_time_impact_multiplier,
    estimate_time_and_velocity,
    time_range,
    velocity_range,
    estimate_from_array,
    calculate_base_value_score,
    calculate_long_term_arbitrage_score,
    compute_cluster_value_score,
    get_competition_band,
    classify_content_angle,
    classify_monetization,
    cluster_keywords_by_overlap,
    generate_batched_content_and_titles_with_llm, # Import the new batched function
)

# Import the keyword normalization utility
from scoring_utils import _normalize_keyword

# ---------------- CONFIG (can be passed as parameters in calling code) ----------------
PER_CALL_THROTTLE_MIN = 0.5
PER_CALL_THROTTLE_MAX = 1.5
THROTTLE_JITTER_PERCENTAGE = 0.25  # ±25% jitter

WINDOW_PAUSE_MIN = 3.0
WINDOW_PAUSE_MAX = 5.0

MAX_QUERY_PARAM_LENGTH_SOFT = 400
MAX_QUERY_PARAM_LENGTH_HARD = 500
MAX_BATCH_SIZE = 50

REQUESTS_PER_WINDOW = 5
_request_counter = 0

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
    "Priority": "u=1, i",
}

BASE_URL = "https://db3.keywordsur.fr/api/ks"

# ---------------- helpers ----------------
async def throttle(base_min: float = PER_CALL_THROTTLE_MIN, base_max: float = PER_CALL_THROTTLE_MAX):
    """Applies a randomized throttle with jitter to mimic human traffic."""
    min_jitter = base_min * THROTTLE_JITTER_PERCENTAGE
    max_jitter = base_max * THROTTLE_JITTER_PERCENTAGE

    actual_min = max(0.1, base_min - random.uniform(0, min_jitter))
    actual_max = base_max + random.uniform(0, max_jitter)

    t = random.uniform(actual_min, actual_max)
    logger.debug("Throttling for %.2f seconds (actual range %.2f-%.2f)", t, actual_min, actual_max)
    await asyncio.sleep(t)


def get_encoded_query_param_length(items: List[str]) -> int:
    """
    Returns the length of the URL-encoded JSON array representation of the items.
    """
    return len(urllib.parse.quote_plus(json.dumps(items, separators=(",", ":")), safe=""))


async def safe_get(url: str, params: dict = None, timeout: int = 15, attempt: int = 1) -> Tuple[int, Any]:
    """
    Performs an async GET with throttling and basic retry/error categorization.
    Returns (status_code, response_data_or_text)
    """
    global _request_counter
    _request_counter += 1

    if REQUESTS_PER_WINDOW > 0 and _request_counter % REQUESTS_PER_WINDOW == 0:
        pause_time = random.uniform(WINDOW_PAUSE_MIN, WINDOW_PAUSE_MAX)
        logger.debug("Requests per window reached. Pausing for %.2f seconds.", pause_time)
        await asyncio.sleep(pause_time)
        _request_counter = 0

    headers = DEFAULT_HEADERS.copy()

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            r = await client.get(url, params=params, headers=headers)
        content_type = r.headers.get("content-type", "")
        if content_type.startswith("application/json"):
            try:
                json_data = r.json()
            except Exception:
                logger.exception("Failed to parse JSON response from %s", url)
                return r.status_code, r.text
            return r.status_code, json_data
        else:
            return r.status_code, r.text
    except httpx.TimeoutException:
        logger.error("Request timed out after %s seconds for URL: %s", timeout, url)
        return 0, {"error": "Request timed out"}
    except httpx.RequestError as e:
        logger.error("Request failed for URL: %s - %s", url, e)
        return 0, {"error": str(e)}
    except Exception as e:
        logger.exception("Unexpected error in safe_get for URL: %s - %s", url, e)
        return 0, {"error": str(e)}


def urlencode_json_array(arr: List[str]) -> str:
    """URL-encodes a JSON array."""
    raw = json.dumps(arr, separators=(",", ":"))
    encoded = urllib.parse.quote_plus(raw, safe="")
    return encoded


# ---------------- Surfer API functions ----------------
async def fetch_keywords_for_batch(keywords: List[str], country: str, retry_attempt: int = 0) -> Dict[str, Any]:
    """
    Fetches keyword data for a batch of keywords via Surfer's /keywords endpoint.
    On batch failure, falls back to fetching keywords one-by-one with retries.
    """
    if not keywords:
        logger.info("No keywords provided for batch fetch.")
        return {}

    url = f"{BASE_URL}/keywords"
    encoded = urlencode_json_array(keywords)
    full_url = f"{url}?country={urllib.parse.quote(country)}&keywords={encoded}"

    logger.info("Requesting keywords batch (size=%d)", len(keywords))
    status, data = await safe_get(full_url)
    await throttle()

    if status == 200:
        if isinstance(data, dict):
            logger.info("Successfully fetched /keywords batch with %d entries.", len(data))
            return data
        else:
            logger.warning("/keywords returned non-dict payload; returning empty.")
            return {}
    else:
        logger.warning("/keywords batch call failed (status=%s) for batch size %d. Retrying as singles if appropriate.", status, len(keywords))
        # Retry logic for server errors
        if status in [500, 502] and retry_attempt < 3:
            wait_time = 2 ** retry_attempt
            logger.info("Exponential backoff: sleeping %d seconds before retry (attempt %d).", wait_time, retry_attempt + 1)
            await asyncio.sleep(wait_time)
            return await fetch_keywords_for_batch(keywords, country, retry_attempt + 1)

        # Fallback: attempt single keyword fetches
        single_results: Dict[str, Any] = {}
        for kw in keywords:
            logger.info("Retrying single keyword: '%s'", kw)
            single_encoded = urlencode_json_array([kw])
            single_full_url = f"{url}?country={urllib.parse.quote(country)}&keywords={single_encoded}"
            single_retry_attempt = 0
            while single_retry_attempt < 3:
                single_status, single_data = await safe_get(single_full_url)
                await throttle()

                if single_status == 200:
                    if isinstance(single_data, dict) and kw in single_data:
                        single_results[kw] = single_data[kw]
                        logger.info("Successfully retrieved single keyword '%s'.", kw)
                    else:
                        logger.warning("Single keyword '%s' response malformed or missing. Status: %s", kw, single_status)
                    break
                elif single_status in [500, 502]:
                    wait_time = 2 ** single_retry_attempt
                    logger.info("Single retry backoff for '%s': sleeping %d seconds.", kw, wait_time)
                    await asyncio.sleep(wait_time)
                    single_retry_attempt += 1
                elif single_status == 422:
                    logger.warning("Single keyword '%s' fetch failed with 422. Not retrying.", kw)
                    break
                else:
                    logger.warning("Single keyword '%s' fetch failed (status=%s). Not retrying.", kw, single_status)
                    break

            if single_retry_attempt == 3:
                logger.error("Single keyword '%s' failed after retries.", kw)

        logger.info("Single retries completed. Successfully retrieved %d keywords.", len(single_results))
        return single_results


async def fetch_customer_domain_data(domain: str, country: str, retry_attempt: int = 0) -> Dict[str, Any]:
    """
    Fetch /domains for a single domain and return the domain data dict.
    """
    url = f"{BASE_URL}/domains"
    parsed_uri = urllib.parse.urlparse(domain)
    base_domain = parsed_uri.hostname or domain
    if base_domain.startswith("www."):
        base_domain = base_domain[4:]

    encoded_domain = urlencode_json_array([base_domain])
    full_url = f"{url}?country={urllib.parse.quote(country)}&domains={encoded_domain}"

    status, data = await safe_get(full_url)
    await throttle()

    if status == 200:
        if isinstance(data, dict) and base_domain in data:
            logger.info("Successfully fetched /domains for %s", base_domain)
            return data[base_domain]
        else:
            logger.warning("/domains response malformed or missing data for %s. Response type: %s", base_domain, type(data))
            return {}
    else:
        logger.warning("/domains call failed (status=%s) for %s", status, base_domain)
        if status in [500, 502] and retry_attempt < 3:
            wait_time = 2 ** retry_attempt
            logger.info("Exponential backoff for domain fetch: sleeping %d seconds then retrying.", wait_time)
            await asyncio.sleep(wait_time)
            return await fetch_customer_domain_data(domain, country, retry_attempt + 1)
        else:
            logger.error("Failed to fetch domain data for %s after retries.", base_domain)
            return {}


# ---------------- Pool builder ----------------
async def build_keyword_pool(seed_keywords: List[str], target_size: int, country: str, min_volume_filter: int, service_radius_cities: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Build a keyword pool by expanding seed keywords via Surfer's similar_keywords
    and applying geographical filtering and minimum volume filter.
    Returns a cleaned_map mapping keyword -> payload.
    """
    pool_map: Dict[str, Dict[str, Any]] = {}
    keywords_to_process_queue: asyncio.Queue = asyncio.Queue()
    processed_keywords: Set[str] = set()

    # Normalize service_radius_cities once
    normalized_service_radius_cities = [_normalize_keyword(city) for city in service_radius_cities if city]
    logger.debug(f"Normalized service radius cities: {normalized_service_radius_cities}")

    for seed in seed_keywords:
        normalized_seed = _normalize_keyword(seed)
        if normalized_seed not in processed_keywords:
            await keywords_to_process_queue.put(normalized_seed)
            processed_keywords.add(normalized_seed)
            pool_map[normalized_seed] = {}

    logger.info("Starting keyword pool build: target_size=%d", target_size)
    logger.debug("Seed keywords for pool build: %s", seed_keywords)

    while not keywords_to_process_queue.empty() and len(pool_map) < target_size:
        current_batch_keywords: List[str] = []
        current_batch_length = 0

        while not keywords_to_process_queue.empty() and len(current_batch_keywords) < MAX_BATCH_SIZE:
            try:
                next_keyword = keywords_to_process_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            
            # next_keyword is already normalized from the queue
            if next_keyword in processed_keywords:
                logger.debug(f"Skipping already processed keyword: '{next_keyword}'")
                continue

            temp_batch = current_batch_keywords + [next_keyword]
            temp_length = get_encoded_query_param_length(temp_batch)

            if temp_length > MAX_QUERY_PARAM_LENGTH_HARD:
                # Put it back for next batch and break to process current batch
                await keywords_to_process_queue.put(next_keyword)
                logger.warning("Keyword '%s' would exceed HARD query param limit. Processing current batch.", next_keyword)
                break

            current_batch_keywords.append(next_keyword)
            current_batch_length = temp_length

            if current_batch_length > MAX_QUERY_PARAM_LENGTH_SOFT or len(current_batch_keywords) >= MAX_BATCH_SIZE:
                break

        if not current_batch_keywords:
            logger.info("No keywords collected for current batch; exiting.")
            break

        logger.info("Fetching data for batch of %d keywords.", len(current_batch_keywords))
        resp = await fetch_keywords_for_batch(current_batch_keywords, country)
        logger.debug("Raw response for batch: %s", resp)

        for kw in current_batch_keywords:
            if kw in resp:
                payload = resp[kw]
                pool_map[kw] = payload
                logger.debug("Keyword '%s' payload before filter: %s", kw, payload)

                similar_keywords = payload.get("similar_keywords", []) if isinstance(payload, dict) else []
                for s in similar_keywords:
                    sim_kw = None
                    if isinstance(s, dict):
                        sim_kw = s.get("keyword")
                    elif isinstance(s, str):
                        sim_kw = s
                    if not sim_kw:
                        continue

                    # Normalize similar keyword for consistent comparison
                    sim_kw_normalized = _normalize_keyword(sim_kw)
                    
                    # Check geographical relevance using normalized forms
                    is_geographically_relevant = any(
                        normalized_city in sim_kw_normalized for normalized_city in normalized_service_radius_cities
                    )
                    logger.debug(f"Checking geographical relevance for '{sim_kw_normalized}' against {normalized_service_radius_cities}: {is_geographically_relevant}")

                    # Filter: any returned keywords MUST HAVE ONE OF THE CITIES in it
                    if sim_kw_normalized and is_geographically_relevant and sim_kw_normalized not in processed_keywords and len(pool_map) < target_size:
                        await keywords_to_process_queue.put(sim_kw_normalized)
                        processed_keywords.add(sim_kw_normalized)
                        pool_map[sim_kw_normalized] = {}  # placeholder to be filled later
                        logger.debug("Added similar keyword '%s' (normalized: '%s') to processing queue.", sim_kw, sim_kw_normalized)
                    elif sim_kw_normalized and not is_geographically_relevant:
                        logger.info("Skipping similar keyword '%s' (normalized: '%s') due to lack of geographical relevance.", sim_kw, sim_kw_normalized)
            else:
                logger.warning("Keyword '%s' not found in batch response. Removing from pool if present.", kw)
                if kw in pool_map:
                    del pool_map[kw]

    # Fill missing payloads
    missing_payload_keywords = [k for k, v in pool_map.items() if not v]
    if missing_payload_keywords:
        logger.info("Fetching %d keywords with missing payloads.", len(missing_payload_keywords))
        for i in range(0, len(missing_payload_keywords), MAX_BATCH_SIZE):
            batch = missing_payload_keywords[i : i + MAX_BATCH_SIZE]
            resp = await fetch_keywords_for_batch(batch, country)
            for kw in batch:
                if kw in resp:
                    payload = resp[kw]
                    pool_map[kw] = resp[kw]
                    logger.debug("Keyword '%s' payload after missing fetch: %s", kw, payload)
                else:
                    logger.warning("Keyword '%s' still missing payload after final fetch attempt. Removing from pool.", kw)
                    if kw in pool_map:
                        del pool_map[kw]

    # Apply final deduplication and volume filter
    cleaned_map: Dict[str, Dict[str, Any]] = {}
    for k, payload in pool_map.items():
        if not isinstance(payload, dict):
            continue
        vol = payload.get("search_volume")
        cpc = payload.get("cpc")
        comp = payload.get("competition")

        logger.debug("Keyword '%s' before final filter: volume=%s, cpc=%s, comp=%s", k, vol, cpc, comp)
        if (vol is not None and vol >= min_volume_filter and cpc is not None and comp is not None):
            cleaned_map[k] = payload
        else:
            logger.debug("Keyword '%s' filtered out: volume=%s (min %s), cpc=%s, comp=%s", k, vol, min_volume_filter, cpc, comp)

    logger.info("Keyword pool build complete. Final size: %d", len(cleaned_map))
    logger.debug("Final cleaned keyword pool: %s", cleaned_map)
    return cleaned_map


__all__ = [
    "throttle",
    "get_encoded_query_param_length",
    "safe_get",
    "urlencode_json_array",
    "fetch_keywords_for_batch",
    "fetch_customer_domain_data",
    "build_keyword_pool",
    "run_prospecting_async",
]

async def run_prospecting_async(
    seed_keywords: List[str],
    customer_domain: str,
    avg_job_amount: float,
    avg_conversion_rate: float,
    llm_model: Any,
    category: str,
    state: str,
    service_radius_cities: List[str],
    target_pool_size: int,
    min_volume_filter: int,
    country: str
) -> Dict[str, Any]:
    """
    Orchestrates the entire Surfer prospecting process:
    1. Builds a keyword pool based on seed keywords and geographical relevance.
    2. Fetches customer domain data.
    3. Scores keywords and calculates arbitrage opportunities.
    """
    logger.info(f"Starting run_prospecting_async for category: {category}, state: {state}")
    logger.info(f"  Seed Keywords: {seed_keywords[:5]}...")
    logger.info(f"  Customer Domain: {customer_domain}")
    logger.info(f"  Avg Job Amount: {avg_job_amount}")
    logger.info(f"  Avg Conversion Rate: {avg_conversion_rate}")
    logger.info(f"  Service Radius Cities: {service_radius_cities[:5]}...")
    logger.info(f"  Target Pool Size: {target_pool_size}")
    logger.info(f"  Min Volume Filter: {min_volume_filter}")
    logger.info(f"  Country: {country}")

    # 1. Build Keyword Pool
    keyword_pool = await build_keyword_pool(seed_keywords, target_pool_size, country, min_volume_filter, service_radius_cities)
    logger.info(f"Built keyword pool with {len(keyword_pool)} keywords.")
    logger.debug(f"Keyword pool details: {json.dumps(keyword_pool, indent=2)}")

    if not keyword_pool:
        logger.warning("Keyword pool is empty. Cannot proceed with scoring.")
        return {"error": "Empty keyword pool", "scored_keywords": []}

    # 2. Fetch Customer Domain Data
    customer_domain_data = await fetch_customer_domain_data(customer_domain, country)
    logger.info(f"Fetched customer domain data for {customer_domain}.")
    logger.debug(f"Customer domain data: {json.dumps(customer_domain_data, indent=2)}")

    # 3. Score Keywords
    scored_keywords = []
    for keyword, data in keyword_pool.items():
        if not data:
            logger.warning(f"Skipping scoring for keyword '{keyword}' due to missing data in pool.")
            continue

        # Extract necessary data for scoring
        search_volume = data.get("search_volume", 0)
        cpc = data.get("cpc", 0.0)
        competition = data.get("competition", 0.0)
        
        # Calculate arbitrage score
        arbitrage_score = calculate_keyword_arbitrage_score(
            volume=search_volume,
            cpc=cpc,
            competition=competition
        )
        logger.debug(f"Keyword '{keyword}': Arbitrage score = {arbitrage_score}")

        # Calculate velocity and time impact
        velocity_score = calculate_velocity(competition)
        logger.debug(f"Keyword '{keyword}': Velocity score = {velocity_score}")
        
        # Estimate time and velocity
        # Assuming customer_domain_data contains 'domain_authority' for 'A'
        domain_authority = customer_domain_data.get("domain_authority", 0.0)
        estimated_time, estimated_velocity = estimate_time_and_velocity(
            C=competition, P=cpc, Vol=search_volume, A=domain_authority
        )
        time_impact = calculate_time_impact_multiplier(estimated_time)
        logger.debug(f"Keyword '{keyword}': Estimated time = {estimated_time}, Estimated velocity = {estimated_velocity}, Time impact = {time_impact}")

        # Calculate base value score
        base_value_score = calculate_base_value_score(search_volume, cpc)
        logger.debug(f"Keyword '{keyword}': Base value score = {base_value_score}")

        # Calculate long-term arbitrage score
        long_term_arbitrage_score = calculate_long_term_arbitrage_score(
            base_value_score=base_value_score,
            competition=competition,
            T=estimated_time
        )
        logger.debug(f"Keyword '{keyword}': Long-term arbitrage score = {long_term_arbitrage_score}")

        # Get competition band
        comp_band = get_competition_band(competition)
        logger.debug(f"Keyword '{keyword}': Competition band = {comp_band}")

        # Classify content angle and monetization
        content_angle = classify_content_angle(competition)
        monetization = classify_monetization(cpc)
        logger.debug(f"Keyword '{keyword}': Content angle = {content_angle}, Monetization = {monetization}")

        # Calculate ROI
        LOW_CONVERSION_RATE = 0.01
        HIGH_CONVERSION_RATE = 0.03

        # Calculate low and high ROI
        low_roi = search_volume * avg_job_amount * LOW_CONVERSION_RATE
        high_roi = search_volume * avg_job_amount * HIGH_CONVERSION_RATE
        logger.debug(f"Keyword '{keyword}': Low ROI = {low_roi}, High ROI = {high_roi}")

        scored_keywords.append({
            "keyword": keyword,
            "search_volume": search_volume,
            "cpc": cpc,
            "competition": competition,
            "arbitrage_score": arbitrage_score,
            "velocity_score": velocity_score,
            "time_impact": time_impact,
            "estimated_time": estimated_time,
            "estimated_velocity": estimated_velocity,
            "base_value_score": base_value_score,
            "long_term_arbitrage_score": long_term_arbitrage_score,
            "competition_band": comp_band,
            "content_angle": content_angle,
            "monetization": monetization,
            "low_roi": low_roi, # Add low ROI
            "high_roi": high_roi, # Add high ROI
            "roi": high_roi, # Set primary ROI to high_roi as per user's implied preference for ranking
            "raw_data": data # Include raw data for completeness
        })
    
    logger.info(f"Finished scoring {len(scored_keywords)} keywords.")

    # --- Implement Top 4 Short-Term Strategy ---
    short_term_strategy = {
        "top_4_clusters": [],
        "max_time_to_implement": 0.0
    }

    if scored_keywords:
        # 1. Sort all keywords by estimated_time (least to most)
        time_sorted_keywords = sorted(scored_keywords, key=lambda x: x.get("estimated_time", float('inf')))
        
        # 2. Grab the top 4 quickest-to-implement keywords
        top_4_by_time = time_sorted_keywords[:4]

        if top_4_by_time:
            # 3. Re-sort these selected 4 keywords by their ROI (descending)
            top_4_by_roi = sorted(top_4_by_time, key=lambda x: x.get("roi", 0.0), reverse=True)
            
            # 4. Calculate max_time_to_implement
            max_time = max([kw.get("estimated_time", 0.0) for kw in top_4_by_roi])
            
            short_term_strategy["top_4_clusters"] = top_4_by_roi
            short_term_strategy["max_time_to_implement"] = max_time
            logger.info(f"Generated Top 4 Short-Term Strategy. Max time to implement: {max_time:.2f} weeks.")
            logger.debug(f"Short-term strategy details: {json.dumps(short_term_strategy, indent=2)}")
        else:
            logger.warning("No keywords available to form Top 4 Short-Term Strategy.")

    return {
        "scored_keywords": scored_keywords,
        "customer_domain_data": customer_domain_data,
        "short_term_strategy": short_term_strategy # Add the new strategy data
    }

async def generate_city_specific_clusters(
    master_keyword_pool: Dict[str, Any],
    city_keywords_map: Dict[str, List[str]],
    customer_domain: str,
    avg_job_amount: float,
    avg_conversion_rate: float,
    llm_model: Any
) -> Dict[str, Any]:
    """
    Generates city-specific keyword clusters and content ideas.
    """
    logger.info("Starting generate_city_specific_clusters.")
    city_clusters_output = {}

    for city, keywords_for_city in city_keywords_map.items():
        logger.debug(f"Processing city: {city} with {len(keywords_for_city)} keywords.")
        if not keywords_for_city:
            logger.info(f"No keywords for city: {city}. Skipping clustering.")
            continue

        # Filter master_keyword_pool to only include keywords relevant to this city
        # Ensure keywords from city_keywords_map are normalized before lookup
        city_relevant_keywords_data = {}
        for kw in keywords_for_city:
            normalized_kw = _normalize_keyword(kw)
            if normalized_kw in master_keyword_pool:
                city_relevant_keywords_data[normalized_kw] = master_keyword_pool[normalized_kw]
            else:
                logger.debug(f"Keyword '{kw}' (normalized: '{normalized_kw}') from city_keywords_map not found in master_keyword_pool.")

        if not city_relevant_keywords_data:
            logger.warning(f"No relevant keyword data found for city: {city}. Skipping clustering.")
            continue

        # Perform clustering for the city's keywords
        clusters = cluster_keywords_by_overlap(list(city_relevant_keywords_data.keys()))
        logger.debug(f"Generated {len(clusters)} clusters for city: {city}.")
        
        city_clusters_output[city] = []
        all_clusters_for_llm = [] # Collect cluster data for batched LLM call
        for i, cluster_dict in enumerate(clusters): # Iterate over the list of cluster dictionaries
            primary_keyword_for_cluster = cluster_dict["primary"]
            cluster_keywords_list = [primary_keyword_for_cluster] + cluster_dict["related"]
            logger.debug(f"City '{city}', Cluster {i}: Primary keyword '{primary_keyword_for_cluster}', Related: {cluster_dict['related']}")

            # Calculate aggregate metrics for the cluster
            aggregate_volume = 0
            total_cpc = 0.0
            total_competition = 0.0
            valid_keywords_in_cluster = 0

            for kw in cluster_keywords_list:
                kw_data = city_relevant_keywords_data.get(kw)
                if kw_data:
                    aggregate_volume += kw_data.get("search_volume", 0)
                    total_cpc += kw_data.get("cpc", 0.0)
                    total_competition += kw_data.get("competition", 0.0)
                    valid_keywords_in_cluster += 1
            
            average_cpc = total_cpc / valid_keywords_in_cluster if valid_keywords_in_cluster > 0 else 0.0
            average_competition = total_competition / valid_keywords_in_cluster if valid_keywords_in_cluster > 0 else 0.0

            # Compute cluster value score
            cluster_value_score = compute_cluster_value_score(
                aggregate_volume,
                average_cpc,
                average_competition
            )
            logger.debug(f"City '{city}', Cluster {i}: Aggregate Volume={aggregate_volume}, Avg CPC={average_cpc}, Avg Competition={average_competition}, Value Score={cluster_value_score}")

            # Prepare cluster data for LLM content generation
            cluster_data_for_llm = {
                "primary": primary_keyword_for_cluster,
                "related": cluster_dict["related"],
                "aggregate_search_volume": aggregate_volume,
                "average_cpc": average_cpc,
                "average_competition": average_competition,
                "value_score": cluster_value_score
            }

            # Store cluster data for batched LLM call
            cluster_data_for_llm["cluster_id"] = f"{city}-{i}" # Ensure unique ID for mapping results
            all_clusters_for_llm.append(cluster_data_for_llm)

            # Append a placeholder to city_clusters_output[city] for now
            # This will be filled with actual content ideas and titles after the batched LLM call
            city_clusters_output[city].append({
                "cluster_id": cluster_data_for_llm["cluster_id"],
                "keywords": cluster_keywords_list,
                "cluster_value_score": cluster_value_score,
                "content_ideas": {}, # Placeholder
                "title": "" # Placeholder
            })
        
        # Perform batched LLM call for content ideas and titles for all clusters in this city
        if all_clusters_for_llm:
            logger.info(f"Calling batched LLM for {len(all_clusters_for_llm)} clusters in city: {city}.")
            batched_llm_results = await generate_batched_content_and_titles_with_llm(
                all_clusters_for_llm,
                customer_domain,
                avg_job_amount,
                avg_conversion_rate,
                llm_model
            )
            logger.debug(f"Batched LLM results for city {city}: {json.dumps(batched_llm_results, indent=2)}")

            # Map results back to city_clusters_output
            for cluster_output_item in city_clusters_output[city]:
                cluster_id = cluster_output_item["cluster_id"]
                if cluster_id in batched_llm_results:
                    llm_generated_data = batched_llm_results[cluster_id]
                    cluster_output_item["content_ideas"] = llm_generated_data["content_ideas"]
                    cluster_output_item["title"] = llm_generated_data["title"]
                else:
                    logger.warning(f"LLM results missing for cluster_id: {cluster_id}. Filling with error placeholders.")
                    cluster_output_item["content_ideas"] = {
                        "title": f"Error: Content ideas failed for {cluster_id}",
                        "content_angle": "N/A",
                        "target_audience": "N/A",
                        "key_questions": ["LLM call error or malformed response."]
                    }
                    cluster_output_item["title"] = f"Error: Title failed for {cluster_id}"
    
    logger.info("Finished generate_city_specific_clusters.")
    return city_clusters_output

# If desired, add more helper entrypoints here that call scoring_utils functions
# e.g., a convenience wrapper to compute arbitrage for a set of keywords, etc.
