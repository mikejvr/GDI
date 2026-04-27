#!/usr/bin/env python3
"""
recommend.py – Heuristic recommendation engine for gig drivers.
Reads the most recent gig_demand_signal shard from data/,
applies heuristics, and outputs a JSON recommendation.
Includes random driver tip from rules.json.
"""

import json
import random
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

from src.utils.io_sorted import read_shards_sorted

# ----------------------------------------------------------------------
# Load driver tips from rules.json
# ----------------------------------------------------------------------
RULES_FILE = Path("src/substrate/rules.json")

def load_random_rule() -> Optional[Dict[str, str]]:
    """Load a random rule from rules.json."""
    if not RULES_FILE.exists():
        return None
    try:
        with open(RULES_FILE, "r") as f:
            data = json.load(f)
        rules = data.get("rules", [])
        if not rules:
            return None
        return random.choice(rules)
    except Exception as e:
        print(f"⚠️ Failed to load rules: {e}")
        return None

# ----------------------------------------------------------------------
# Heuristic rules (pure functions, no external deps)
# ----------------------------------------------------------------------
def score_surge(surge_cells: List[Dict]) -> Dict[str, Any]:
    """Evaluate surge cells: max multiplier, count of high-surge zones."""
    if not surge_cells:
        return {"max_multiplier": 0, "high_surge_count": 0, "score": 0}
    multipliers = [cell.get("surge_multiplier", 1.0) for cell in surge_cells if cell.get("surge_multiplier")]
    max_mult = max(multipliers) if multipliers else 1.0
    high_count = sum(1 for m in multipliers if m >= 2.0)
    score = min(10, int(max_mult * 2) + high_count)
    return {"max_multiplier": max_mult, "high_surge_count": high_count, "score": score}

def score_events(events: List[Dict]) -> Dict[str, Any]:
    """Evaluate events: count, categories, attendance."""
    if not events:
        return {"count": 0, "score": 0, "has_big_event": False}
    category_scores = {
        "concert": 3, "sports": 3, "festival": 4, "conference": 2, "nightlife": 2,
        "music": 2, "arts & theatre": 2, "miscellaneous": 1
    }
    score = 0
    big_event = False
    for evt in events:
        cat = evt.get("category", "").lower()
        score += category_scores.get(cat, 1)
        attendance = evt.get("expected_attendance", 0)
        if attendance > 5000:
            score += 2
            big_event = True
    return {"count": len(events), "score": min(20, score), "has_big_event": big_event}

def score_weather(weather: Optional[Dict]) -> Dict[str, Any]:
    """Weather impact: rain, snow, extreme temps, wind."""
    if not weather or not isinstance(weather, dict):
        return {"score": 0, "condition": "unknown"}
    
    condition = weather.get("condition_code") or ""
    condition = condition.lower() if condition else "unknown"
    temp = weather.get("temp_c") or 20
    precip_prob = weather.get("precip_probability") or 0
    wind = weather.get("wind_speed_kph") or 0

    score = 5
    if "rain" in condition or precip_prob > 70:
        score += 3
    elif "snow" in condition:
        score += 5
    elif "clear" in condition:
        score -= 1
    if temp > 35:
        score += 2
    elif temp < 0:
        score += 3
    if wind > 30:
        score += 2
    return {"score": min(max(score, 0), 20), "condition": condition}

def time_of_day_factor() -> Dict[str, Any]:
    """Return score based on current hour (UTC)."""
    now_hour = datetime.now(timezone.utc).hour
    if 7 <= now_hour < 10:
        factor = 8
    elif 11 <= now_hour < 14:
        factor = 6
    elif 16 <= now_hour < 19:
        factor = 9
    elif 21 <= now_hour < 24:
        factor = 7
    elif 0 <= now_hour < 5:
        factor = 4
    else:
        factor = 5
    return {"hour": now_hour, "score": factor}

def recommend_apps(surge_score: int, events_score: int, weather_score: int, time_score: int) -> List[str]:
    """Select which apps to stack based on combined scores."""
    total = surge_score + events_score + weather_score + time_score
    apps = ["Uber"]
    if total > 25:
        apps.append("DoorDash")
        apps.append("Lyft")
    elif total > 18:
        apps.append("DoorDash")
    if events_score > 8:
        apps.append("Uber Eats")
    return list(set(apps))

def generate_recommendation_text(apps: List[str], expected_hourly: float, reasoning: List[str]) -> str:
    """Human-readable summary."""
    app_str = " + ".join(apps)
    return f"Recommended: Drive now, stack {app_str}. Expected ~${expected_hourly:.2f}/hr."

# ----------------------------------------------------------------------
# Main engine
# ----------------------------------------------------------------------
def load_latest_shard(data_dir: Path = Path("data")) -> Optional[Dict]:
    """Return the most recent shard (by last modified time) from data/."""
    if not data_dir.exists():
        return None
    shard_paths = list(read_shards_sorted(data_dir))
    if not shard_paths:
        return None
    latest_path = max(shard_paths, key=lambda p: p.stat().st_mtime)
    with latest_path.open("r") as f:
        shard = json.load(f)
    return shard

def compute_recommendation(shard: Dict) -> Dict[str, Any]:
    """Core heuristic logic."""
    data = shard.get("data", {})
    surge_cells = data.get("surge_cells", [])
    events = data.get("events", [])
    weather = data.get("weather")

    surge = score_surge(surge_cells)
    event_score = score_events(events)
    weather_res = score_weather(weather)
    time_res = time_of_day_factor()

    total_score = (event_score["score"] * 2) + weather_res["score"] + time_res["score"]
    expected_hourly = 12.0 + (total_score / 60) * 30
    expected_hourly = round(expected_hourly, 2)

    apps = recommend_apps(surge["score"], event_score["score"], weather_res["score"], time_res["score"])

    reasoning = []
    if surge["max_multiplier"] > 2.0:
        reasoning.append(f"Surge at {surge['max_multiplier']}x in {surge['high_surge_count']} zones.")
    elif surge["high_surge_count"] > 0:
        reasoning.append(f"{surge['high_surge_count']} zones with 2.0x+ surge.")
    if event_score["has_big_event"]:
        reasoning.append("Large event (>5000 attendees) nearby.")
    elif event_score["count"] > 0:
        reasoning.append(f"{event_score['count']} active events.")
    if weather_res["score"] > 8:
        reasoning.append(f"Weather: {weather_res['condition']} – fewer drivers expected.")
    hour = time_res["hour"]
    if hour in [7,8,9,16,17,18]:
        reasoning.append("Peak rush hour period.")

    if not reasoning:
        reasoning.append("Normal demand – base recommendation.")

    confidence = min(0.95, 0.5 + (total_score / 100))

    rec = {
        "recommendation_text": generate_recommendation_text(apps, expected_hourly, reasoning),
        "expected_hourly_min": expected_hourly,
        "confidence": round(confidence, 2),
        "actionable_tips": [
            "Avoid waiting in long restaurant drive-thrus",
            "Use the 'destination filter' to stay in surge zones",
            "Stack two apps, pause the third when you accept a trip"
        ],
        "reasoning": reasoning,
        "raw_scores": {
            "events": event_score["score"],
            "weather": weather_res["score"],
            "time": time_res["score"]
        }
    }

    # Add random driver tip if available
    rule = load_random_rule()
    if rule:
        rec["tip_of_the_day"] = {
            "title": rule["title"],
            "description": rule["description"]
        }
    else:
        rec["tip_of_the_day"] = {
            "title": "Golden Rule",
            "description": "Always know your cost per mile before you drive."
        }

    return rec

def main():
    shard = load_latest_shard()
    if not shard:
        print(json.dumps({"error": "No shards found in data/"}))
        return
    if shard.get("shard_type") != "gig_demand_signal":
        print(json.dumps({"error": f"Expected gig_demand_signal, got {shard.get('shard_type')}"}))
        return
    rec = compute_recommendation(shard)
    print(json.dumps(rec, indent=2))

if __name__ == "__main__":
    main()
