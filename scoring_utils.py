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
    "generate_content_ideas_with_llm",
    "generate_title",
    "cluster_keywords_by_overlap",
    "_normalize_keyword", # Add to __all__
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


async def generate_content_ideas_with_llm(
    cluster: Dict[str, Any],
    customer_domain: str,
    avg_job_amount: float,
    avg_conversion_rate: float,
    llm_model: Any,
) -> Dict[str, Any]:
    """
    Generates content ideas (title, content angle, target audience, key questions) for a cluster using an LLM.
    Expects llm_model to have an async method generate_content_async(prompt) returning an object with .text.
    """
    primary_keyword = cluster.get("primary", "")
    related_keywords = ", ".join(cluster.get("related", [])) if cluster.get("related") else "None"
    aggregate_volume = cluster.get("aggregate_search_volume", 0)
    average_cpc = cluster.get("average_cpc", 0.0)
    average_competition = cluster.get("average_competition", 0.0)
    value_score = cluster.get("value_score", 0.0)

    system_prompt = """You are a helpful assistant that generates content ideas for SEO clusters.
Your output must be a JSON object with the following keys:
- "title": A compelling article title (string).
- "content_angle": Suggested content angle (string).
- "target_audience": Primary target audience (string).
- "key_questions": A list of 3-5 key questions the article should answer (list of strings).
"""

    user_prompt = f"""Generate content ideas for the following keyword cluster:

Primary Keyword: {primary_keyword}
Related Keywords: {related_keywords}
Aggregate Search Volume: {aggregate_volume}
Average CPC: {average_cpc}
Average Competition: {average_competition}
Value Score: {value_score}

Context:
Customer Domain: {customer_domain}
Average Job Amount: ${avg_job_amount:.2f}
Average Conversion Rate: {avg_conversion_rate:.2f}%

Consider the target audience for a business operating on '{customer_domain}' with an average job value of ${avg_job_amount:.2f}.
Focus on creating a title that is engaging and relevant to the keywords, a suitable content angle, a clear target audience, and 3-5 key questions the article should address.
"""

    full_prompt = f"<|system|>\n{system_prompt}</s>\n<|user|>\n{user_prompt}</s>\n<|assistant|>\n"

    try:
        response = await llm_model.generate_content_async(full_prompt)
        raw_response_text = getattr(response, "text", "") or str(response)
        cleaned_text = raw_response_text.strip().replace("```json", "").replace("```", "").strip()
        try:
            llm_output = json.loads(cleaned_text)
            return llm_output
        except json.JSONDecodeError:
            logger.error("LLM response not valid JSON for keyword '%s'. Raw: %s", primary_keyword, cleaned_text)
            return {
                "title": f"Error: Could not generate title for {primary_keyword}",
                "content_angle": "N/A",
                "target_audience": "N/A",
                "key_questions": ["LLM JSON parsing error."]
            }
    except Exception as e:
        logger.exception("Error calling LLM for '%s': %s", primary_keyword, e)
        return {
            "title": f"Error: LLM call failed for {primary_keyword}",
            "content_angle": "N/A",
            "target_audience": "N/A",
            "key_questions": [f"LLM call error: {e}"]
        }


async def generate_title(keyword: str, llm_model: Any) -> str:
    """
    Generates a compelling title for a keyword using an LLM.
    """
    system_prompt = """You are a creative content writer. Your task is to generate a compelling and SEO-friendly title for a given keyword.
    The title should be concise, engaging, and accurately reflect the keyword's intent.
    Return ONLY the title string, no extra text or markdown.
    """
    user_prompt = f"Generate a title for the keyword: '{keyword}'"
    full_prompt = f"<|system|>\n{system_prompt}</s>\n<|user|>\n{user_prompt}</s>\n<|assistant|>\n"

    try:
        response = await llm_model.generate_content_async(full_prompt)
        return response.text.strip()
    except Exception as e:
        logger.error(f"Error generating title for '{keyword}' with LLM: {e}")
        # Fallback to heuristic-based title generation
        if "emergency" in keyword.lower():
            return f"Emergency {keyword.title()}: 24/7 Fast Response"
        elif "cheap" in keyword.lower() or "affordable" in keyword.lower():
            return f"Affordable {keyword.title()}: Save on Quality Repairs"
        elif "best" in keyword.lower():
            return f"Best {keyword.title()}: Top Rated Services"
        else:
            return f"{keyword.title()} Services: Your Local Experts"


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
