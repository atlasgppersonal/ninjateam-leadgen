import json

def normalize(value, min_val, max_val):
    return (value - min_val) / (max_val - min_val) if max_val > min_val else 0

def calculate_opportunity(keyword, search_volume, difficulty, cpc, competition_index,
                          vol_min=0, vol_max=5000, cpc_min=0, cpc_max=5):
    # Normalize
    norm_volume = normalize(search_volume, vol_min, vol_max)
    norm_difficulty = normalize(difficulty, 0, 1)
    norm_cpc = normalize(cpc, cpc_min, cpc_max)

    # Ad likelihood
    ad_likelihood = (norm_cpc * 0.6) + (competition_index * 0.4)

    # Score
    score = (0.5 * norm_volume) - (0.3 * norm_difficulty) - (0.2 * ad_likelihood)

    # Classification
    if score >= 0.7:
        band = "Excellent"
    elif score >= 0.5:
        band = "Good"
    elif score >= 0.3:
        band = "Weak"
    else:
        band = "Poor"

    return {
        "keyword": keyword,
        "search_volume": search_volume,
        "difficulty": difficulty,
        "cpc": cpc,
        "competition_index": competition_index,
        "ad_likelihood": round(ad_likelihood, 2),
        "opportunity_score": round(score, 2),
        "band": band
    }

if __name__ == "__main__":
    test = calculate_opportunity(
        keyword="emergency plumber orlando",
        search_volume=1200,
        difficulty=0.25,
        cpc=1.2,
        competition_index=0.2
    )
    print(json.dumps(test, indent=2))
