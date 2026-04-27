#!/usr/bin/env python3
"""
run_live_pipeline.py – Fetch live weather & events, run the gig demand extractor,
write a deterministic shard, and validate.
"""

import json
import hashlib
import time
from pathlib import Path
from datetime import datetime, timezone

# Your live fetchers
from src.extractors.live_events_ticketmaster import get_ticketmaster_events
from src.extractors.live_weather_nws import get_nws_forecast

from src.extractors.nexus_gig_demand import extract as extract_gig_demand

from src.utils.manifest import generate_phase1_manifest
from src.shard.shard_validator import validate_all_shards

# ----------------------------------------------------------------------
# Build payload matching what nexus_gig_demand expects
# ----------------------------------------------------------------------
def build_live_payload(weather_data, events_data):
    if isinstance(weather_data, dict) and "error" in weather_data:
        print(f"⚠️ Weather API error: {weather_data['error']}")
        weather_payload = {}
    else:
        weather_payload = {
            "timestamp": weather_data.get("timestamp"),
            "temp_c": weather_data.get("temp_c"),
            "feels_like_c": weather_data.get("temp_c"),
            "precip_probability": weather_data.get("precip_probability", 0),
            "precip_intensity_mm_hr": 0,
            "wind_speed_kph": 0,
            "wind_gust_kph": None,
            "visibility_km": None,
            "condition_code": weather_data.get("condition_code", ""),
            "alerts": []
        }

    if isinstance(events_data, dict) and "error" in events_data:
        print(f"⚠️ Events API error: {events_data['error']}")
        events_list = []
    else:
        events_list = events_data.get("events", [])
        print(f"📅 Found {len(events_list)} live events.")

    return {
        "platform": "ticketmaster_nws",
        "market": "austin_tx",
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "surge_map": [],
        "events": events_list,
        "weather": weather_payload,
        "source_meta": {
            "source": "live_fetcher",
            "fetched_at": time.time(),
            "weather_api": "nws",
            "events_api": "ticketmaster"
        }
    }

# ----------------------------------------------------------------------
# Convert raw extractor output to schema‑compliant shard
# ----------------------------------------------------------------------
def make_compliant_shard(raw_shard, asset_id):
    payload = raw_shard.get("payload", {})
    content_hash = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:16]
    shard_id = f"shard_{asset_id}_{content_hash}"
    shard_type = "gig_demand_signal"
    producer = raw_shard.get("lineage", {}).get("extractor_id", "nexus.live.pipeline.v1")
    created_at_utc = datetime.now(timezone.utc).isoformat()
    return {
        "shard_id": shard_id,
        "shard_type": shard_type,
        "producer": producer,
        "created_at_utc": created_at_utc,
        "data": payload,
        "lineage": raw_shard.get("lineage", {})
    }

# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
def run_live_pipeline():
    output_dir = Path("data")
    output_dir.mkdir(exist_ok=True)

    print("\n🌍 Fetching live data...")
    weather = get_nws_forecast()
    events = get_ticketmaster_events()

    print("DEBUG: events return structure keys:", events.keys())
    if events.get("events"):
        print("DEBUG: first event sample:", json.dumps(events["events"][0], indent=2)[:500])

    payload = build_live_payload(weather, events)

    source_id = "live_fetcher"
    asset_id = f"austin_live_{int(time.time())}"
    try:
        shards = extract_gig_demand(payload, source_id=source_id, asset_id=asset_id)
    except Exception as e:
        print(f"❌ Extractor failed: {e}")
        return

    if not shards:
        print("No shards returned.")
        return

    for raw_shard in shards:
        compliant = make_compliant_shard(raw_shard, asset_id)
        shard_path = output_dir / f"{compliant['shard_id']}.json"
        with open(shard_path, "w") as f:
            json.dump(compliant, f, sort_keys=True, indent=2)
        print(f"✅ Wrote shard: {shard_path}")

    generate_phase1_manifest(output_dir, Path("."))
    validation = validate_all_shards(output_dir)
    if validation["all_valid"]:
        print("✅ All shards valid.")
    else:
        print("❌ Validation failed:")
        for res in validation["results"]:
            if not res["valid"]:
                print(f"  - {res['path']}: {res.get('schema_error')}")

if __name__ == "__main__":
    run_live_pipeline()
