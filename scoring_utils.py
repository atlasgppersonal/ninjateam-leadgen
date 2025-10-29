"""
scoring_utils.py

Utility module containing scoring, velocity/time estimation, clustering,
title generation, content idea generation (LLM), and classification helpers
for the Surfer Prospector pipeline.

This module is intentionally self-contained and *does not* import or rely on
surfer_prospector_module to avoid circular imports.
"""

import json
import math
import logging
import re # Import re for regex operations
from typing import List, Dict, Any, Tuple

logger = logging.getLogger(__name__)

__all__ = [
    "calculate_keyword_arbitrage_score",
    "calculate_velocity",
    "calculate_time_impact_multiplier",
    "estimate_time_and_velocity",
    "time_range",
    "velocity_range",
    "estimate_from_array",
    "calculate_base_value_score",
    "calculate_long_term_arbitrage_score",
    "compute_cluster_value_score",
    "get_competition_band",
    "classify_content_angle",
    "classify_monetization",
    "cluster_keywords_by_overlap",
    "_normalize_keyword", # Add to __all__
    "generate_batched_content_and_titles_with_llm", # Add to __all__
]

def _normalize_keyword(keyword: str) -> str:
    """
    Normalizes a keyword string by lowercasing, stripping whitespace, and
    standardizing internal spacing.
    """
    if not isinstance(keyword, str):
        return ""
    # Convert to lowercase, strip leading/trailing whitespace, and replace multiple spaces with a single space
    return re.sub(r'\s+', ' ', keyword.lower().strip())


def calculate_keyword_arbitrage_score(volume: float, cpc: float, competition: float) -> float:
    """
    Computes an arbitrage score for a keyword, balancing volume, CPC, and competition.
    Prioritizes higher value (volume * CPC) for lower competition.
    """
    adjusted_competition = competition + 0.01
    value_score = volume * cpc
    score = value_score / adjusted_competition
    logger.debug("Arbitrage score: volume=%s, cpc=%s, comp=%s => %s", volume, cpc, competition, score)
    return score


def calculate_velocity(competition: float) -> float:
    """
    Calculates a velocity score based on competition. Lower competition means higher velocity.
    The formula ensures a higher score for lower competition, capped at 100.0.
    """
    return min(100.0, 1.0 / (competition**2 + 0.001))


def calculate_time_impact_multiplier(T: float) -> float:
    """
    Calculates a multiplier based on estimated time to rank (T) in weeks.
    - Boost for 4-8 weeks.
    - Neutral at 8 weeks.
    - Gradual penalty after 8 weeks, with a floor.
    """
    IDEAL_MIN_WEEKS = 4.0
    IDEAL_MAX_WEEKS = 8.0
    NEUTRAL_MULTIPLIER = 1.0
    MAX_BOOST_MULTIPLIER = 1.15  # 15% boost for 4 weeks
    PENALTY_PER_WEEK_AFTER_IDEAL = 0.02  # 2% penalty per week after 8 weeks
    MIN_MULTIPLIER_FLOOR = 0.6  # Don't penalize below 60%

    if T <= IDEAL_MIN_WEEKS:
        return MAX_BOOST_MULTIPLIER
    elif T <= IDEAL_MAX_WEEKS:
        slope = (NEUTRAL_MULTIPLIER - MAX_BOOST_MULTIPLIER) / (IDEAL_MAX_WEEKS - IDEAL_MIN_WEEKS)
        return MAX_BOOST_MULTIPLIER + slope * (T - IDEAL_MIN_WEEKS)
    else:
        penalty = (T - IDEAL_MAX_WEEKS) * PENALTY_PER_WEEK_AFTER_IDEAL
        return max(MIN_MULTIPLIER_FLOOR, NEUTRAL_MULTIPLIER - penalty)


def estimate_time_and_velocity(
    C: float, P: float, Vol: float, A: float, K: float = 20, b: float = 0.6, d: float = 0.08, s: float = 0.25
) -> Tuple[float, float]:
    """
    Estimate time to rank (T in weeks) and baseline velocity (V 1-100).
    C: Competition (0-1)
    P: CPC
    Vol: Search Volume
    A: Authority (0-1)
    K, b, d, s: tuning parameters
    """
    adjusted_C = max(0.01, C)
    T = (K * adjusted_C * (1 + b * math.log10(P + 1)) * (1 + d * math.log10(Vol + 1))) / (A + s)
    V = round(max(1, min(100, 104 - T)))
    logger.debug("estimate_time_and_velocity: C=%s P=%s Vol=%s A=%s => T=%s V=%s", C, P, Vol, A, T, V)
    return T, V


def time_range(T: float) -> Dict[str, float]:
    """
    Compute low/high range around T with dynamic margins.
    """
    margin = max(0.1, min(0.2, 0.2 - 0.02 * math.log10(T + 1)))
    low = round((1 - margin) * T, 1)
    high = round((1 + margin) * T, 1)
    return {"low": low, "high": high, "base": round(T, 1)}


def velocity_range(t_range: Dict[str, float]) -> Dict[str, float]:
    """
    Compute velocity range based on inverted time range.
    V = 104 - T (clamped 1–100).
    """
    v_low = round(max(1, min(100, 104 - t_range["high"])))
    v_high = round(max(1, min(100, 104 - t_range["low"])))
    v_base = round(max(1, min(100, 104 - t_range["base"])))
    return {"low": v_low, "high": v_high, "base": v_base}


def estimate_from_array(
    stage_inputs: List[Dict[str, float]], K: float = 20, b: float = 0.6, d: float = 0.08, s: float = 0.25
) -> Dict[str, Dict[str, Dict[str, float]]]:
    """
    Accepts an array of dicts with per-stage inputs and returns {low, mid, high} results.
    Each entry should contain keys: comp, volume, cpc, auth
    """
    stage_keys = ["low", "mid", "high"]
    results: Dict[str, Dict[str, Dict[str, float]]] = {}

    for i, entry in enumerate(stage_inputs[:3]):
        C = entry["comp"]
        P = entry["cpc"]
        Vol = entry["volume"]
        A = entry["auth"]

        T, _ = estimate_time_and_velocity(C, P, Vol, A, K, b, d, s)
        t_rng = time_range(T)
        v_rng = velocity_range(t_rng)

        results[stage_keys[i]] = {"v": v_rng, "t": t_rng}

    return results


def calculate_base_value_score(volume: float, cpc: float) -> float:
    """
    Calculates a base value score based purely on volume and CPC.
    """
    return volume * cpc


def calculate_long_term_arbitrage_score(base_value_score: float, competition: float, T: float) -> float:
    """
    Final arbitrage score for long-term value.
    """
    TIME_WEIGHT_FOR_LONG_TERM = 0.002
    adjusted_competition = competition + 0.01
    score = base_value_score / (adjusted_competition + (T * TIME_WEIGHT_FOR_LONG_TERM))
    return score


def compute_cluster_value_score(aggregate_volume: float, average_cpc: float, average_competition: float) -> float:
    """
    Computes the value score for a cluster.
    """
    capped_volume = min(aggregate_volume, 600.0)
    adjusted_competition = average_competition + 0.01
    score = (capped_volume * average_cpc) / adjusted_competition
    return score


def get_competition_band(competition: float) -> int:
    """
    Classifies competition into defined bands.
    """
    if 0.00 <= competition <= 0.10:
        return 1
    elif 0.11 <= competition <= 0.20:
        return 2
    elif 0.21 <= competition <= 0.30:
        return 3
    elif 0.31 <= competition <= 0.40:
        return 4
    else:
        return 5


def classify_content_angle(competition: float) -> str:
    """
    Classifies content angle based on competition.
    """
    if competition < 0.33:
        return "Quick wins / long-tail blog"
    elif 0.33 <= competition < 0.66:
        return "Comparison / listicle"
    else:
        return "In-depth guide / landing page"


def classify_monetization(cpc: float) -> str:
    """
    Classifies monetization strategy based on CPC.
    """
    if cpc > 5.0:
        return "Service / conversion page"
    elif 1.0 <= cpc <= 5.0:
        return "Lead gen blog post"
    else:
        return "Top-of-funnel explainer"


def cluster_keywords_by_overlap(keywords: List[str], min_common_words: int = 2) -> List[Dict[str, Any]]:
    """
    Clusters keywords by overlapping words. Returns list of clusters:
    [{"primary": primary_keyword, "related": [related_kw1, related_kw2, ...]}, ...]
    """
    clusters: List[Dict[str, Any]] = []
    used = set()

    for i, kw in enumerate(keywords):
        if kw in used:
            continue
        primary = kw
        primary_words = set(primary.lower().split())
        cluster = {"primary": primary, "related": []}
        used.add(primary)

        for j in range(i + 1, len(keywords)):
            cand = keywords[j]
            if cand in used:
                continue
            cand_words = set(cand.lower().split())
            common_words = primary_words.intersection(cand_words)
            if len(common_words) >= min_common_words:
                cluster["related"].append(cand)
                used.add(cand)

        clusters.append(cluster)

    return clusters


async def generate_batched_content_and_titles_with_llm(
    clusters_data: List[Dict[str, Any]],
    customer_domain: str,
    avg_job_amount: float,
    avg_conversion_rate: float,
    llm_model: Any,
) -> Dict[str, Dict[str, Any]]:
    """
    Generates content ideas and titles for a batch of clusters using a single LLM call.
    Returns a dictionary mapping cluster_id to its generated content ideas and title.
    """
    if not clusters_data:
        return {}

    batch_input_for_llm = []
    for i, cluster in enumerate(clusters_data):
        primary_keyword = cluster.get("primary", "")
        related_keywords = ", ".join(cluster.get("related", [])) if cluster.get("related") else "None"
        aggregate_volume = cluster.get("aggregate_search_volume", 0)
        average_cpc = cluster.get("average_cpc", 0.0)
        average_competition = cluster.get("average_competition", 0.0)
        value_score = cluster.get("value_score", 0.0)

        batch_input_for_llm.append({
            "cluster_id": cluster.get("cluster_id", f"cluster_{i}"), # Ensure cluster_id is present
            "primary_keyword": primary_keyword,
            "related_keywords": related_keywords,
            "aggregate_search_volume": aggregate_volume,
            "average_cpc": average_cpc,
            "average_competition": average_competition,
            "value_score": value_score,
            "customer_domain": customer_domain,
            "avg_job_amount": avg_job_amount,
            "avg_conversion_rate": avg_conversion_rate
        })

    system_prompt = """You are a helpful assistant that generates content ideas and titles for SEO keyword clusters.
You will receive a JSON array of objects, where each object represents a keyword cluster.
For each cluster, you need to generate:
- A compelling article title.
- Suggested content angle.
- Primary target audience.
- A list of 3-5 key questions the article should answer.

CRITICAL INSTRUCTIONS - READ CAREFULLY:
1. You MUST return ONLY a valid JSON array.
2. The response array MUST contain one object for every object in the input array.
3. Each object in your response array MUST include the original 'cluster_id'.
4. Each object MUST have the following keys: 'cluster_id', 'title', 'content_angle', 'target_audience', 'key_questions'.
5. 'key_questions' MUST be a list of 3-5 strings.
6. Maintain the exact same order as the input array.

EXPECTED JSON FORMAT (example for one cluster):
[
  {{
    "cluster_id": "cluster_123",
    "title": "Compelling Article Title for Cluster",
    "content_angle": "Suggested content angle for the cluster.",
    "target_audience": "Primary target audience for the content.",
    "key_questions": [
      "Question 1?",
      "Question 2?",
      "Question 3?"
    ]
  }}
]

Here is the batch of keyword clusters to process: {batch_input_json}

Return ONLY the JSON array. No explanations, no markdown, no additional text.
"""

    full_prompt = system_prompt.format(batch_input_json=json.dumps(batch_input_for_llm, separators=(',', ':')))

    try:
        response = await llm_model.generate_content_async(full_prompt)
        raw_response_text = getattr(response, "text", "") or str(response)
        cleaned_text = raw_response_text.strip().replace("```json", "").replace("```", "").strip()
        
        batched_llm_output = json.loads(cleaned_text)
        
        # Validate the response structure
        if not isinstance(batched_llm_output, list):
            logger.error("Batched LLM response is not a list.")
            return {}
        
        results_map = {}
        for item in batched_llm_output:
            if "cluster_id" in item and "title" in item and "content_angle" in item and "target_audience" in item and "key_questions" in item:
                results_map[item["cluster_id"]] = {
                    "content_ideas": {
                        "title": item["title"],
                        "content_angle": item["content_angle"],
                        "target_audience": item["target_audience"],
                        "key_questions": item["key_questions"]
                    },
                    "title": item["title"] # Title is also part of content_ideas, but also returned separately for convenience
                }
            else:
                logger.warning(f"Malformed item in batched LLM response: {item}")
        return results_map

    except json.JSONDecodeError as e:
        logger.error(f"LLM response not valid JSON for batched clusters. Error: {e}. Raw: {raw_response_text[:500] if 'raw_response_text' in locals() else 'No text found'}...")
        return {}
    except Exception as e:
        logger.exception(f"Error calling LLM for batched clusters: {e}")
        return {}
