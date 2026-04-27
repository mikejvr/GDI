"""
nexus_gig_demand.py

NEXUS Extractor: Gig Demand Signals (surge + events + weather)

This extractor is designed for a scheduled scraper / API poller that
collects demand signals from gig platforms and adjacent sources:
- Surge / dynamic pricing maps
- Event calendars (sports, concerts, conferences, festivals)
- Weather forecasts and live conditions

Design notes:
- Deterministic, pure transformation: NO network calls, NO side effects.
- Input is the raw payload captured by an upstream poller/scraper.
- Output is a normalized business_dna shard focused on "gig_demand_signal".
- Optimized for real driver decision variables from long-term field experience.
"""

from typing import Any, Dict, List, Optional

from src.utils.ext_dna import (
    # Core substrate helpers (canonical, shared)
    make_extractor_spec,
    register_extractor,
    make_shard,
    make_lineage,
    normalize_timestamp,
    normalize_geo_point,
    normalize_currency,
    safe_get,
)

EXTRACTOR_ID = "nexus.gig_demand_signals.v1"


SPEC = make_extractor_spec(
    extractor_id=EXTRACTOR_ID,
    phase="phase2",
    domain="gig_demand",
    description=(
        "Normalize gig platform demand signals (surge maps, events, weather) "
        "into business_dna.gig_demand_signal shards."
    ),
    input_kind="json",
    output_kind="business_dna",
)


def _normalize_surge_cell(cell: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalize a single surge-map cell.

    Expected raw fields (examples, not strict):
    - cell_id: str
    - center: { lat: float, lon: float }
    - radius_m: float
    - surge_multiplier: float
    - min_fare: { amount: float, currency: str } (optional)
    - eta_minutes: float (optional)
    - valid_from / valid_until: timestamps (optional)
    """
    cell_id = safe_get(cell, "cell_id")
    if not cell_id:
        return None

    center_raw = safe_get(cell, "center") or {}
    center = normalize_geo_point(
        lat=safe_get(center_raw, "lat"),
        lon=safe_get(center_raw, "lon"),
    )

    if center is None:
        return None

    surge_multiplier = safe_get(cell, "surge_multiplier")
    if surge_multiplier is None:
        # If we don't have a multiplier, this cell is not useful as a demand signal.
        return None

    min_fare_raw = safe_get(cell, "min_fare") or {}
    min_fare = None
    if min_fare_raw:
        min_fare = normalize_currency(
            amount=safe_get(min_fare_raw, "amount"),
            currency=safe_get(min_fare_raw, "currency"),
        )

    valid_from = normalize_timestamp(safe_get(cell, "valid_from"))
    valid_until = normalize_timestamp(safe_get(cell, "valid_until"))

    return {
        "cell_id": cell_id,
        "center": center,
        "radius_m": safe_get(cell, "radius_m"),
        "surge_multiplier": surge_multiplier,
        "min_fare": min_fare,
        "eta_minutes": safe_get(cell, "eta_minutes"),
        "valid_from": valid_from,
        "valid_until": valid_until,
    }


def _normalize_event(evt: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalize a single event.

    High-value driver signals:
    - Start/end time (with buffer)
    - Venue location (optional – if missing we still keep the event)
    - Category (sports, concert, festival, conference, nightlife)
    - Expected attendance / capacity
    - Parking / road closure hints (if available)
    """
    event_id = safe_get(evt, "id") or safe_get(evt, "event_id")
    if not event_id:
        return None

    venue_raw = safe_get(evt, "venue") or {}
    venue_point = normalize_geo_point(
        lat=safe_get(venue_raw, "lat"),
        lon=safe_get(venue_raw, "lon"),
    )

    start_time = normalize_timestamp(safe_get(evt, "start_time"))
    end_time = normalize_timestamp(safe_get(evt, "end_time"))

    # Only require start time; location can be missing (we still want the event)
    if start_time is None:
        return None

    return {
        "event_id": event_id,
        "name": safe_get(evt, "name"),
        "category": safe_get(evt, "category"),  # e.g. "sports", "concert", "festival"
        "start_time": start_time,
        "end_time": end_time,
        "venue": {
            "name": safe_get(venue_raw, "name"),
            "location": venue_point,   # may be None
        },
        "expected_attendance": safe_get(evt, "expected_attendance"),
        "is_sold_out": safe_get(evt, "is_sold_out"),
        "notes": safe_get(evt, "notes"),
    }

def _normalize_weather(weather: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalize weather snapshot/forecast.

    High-value driver signals:
    - Precipitation probability + intensity
    - Temperature band (cold / comfortable / hot)
    - Wind speed (esp. for bikes/scooters)
    - Visibility (fog, storms)
    - Weather alerts (storms, ice, flooding)
    """
    if not weather:
        return None

    ts = normalize_timestamp(safe_get(weather, "timestamp"))
    # Weather is still useful without timestamp, but timestamp is preferred.
    return {
        "timestamp": ts,
        "temp_c": safe_get(weather, "temp_c"),
        "feels_like_c": safe_get(weather, "feels_like_c"),
        "precip_probability": safe_get(weather, "precip_probability"),
        "precip_intensity_mm_hr": safe_get(weather, "precip_intensity_mm_hr"),
        "wind_speed_kph": safe_get(weather, "wind_speed_kph"),
        "wind_gust_kph": safe_get(weather, "wind_gust_kph"),
        "visibility_km": safe_get(weather, "visibility_km"),
        "condition_code": safe_get(weather, "condition_code"),  # e.g. "rain", "snow", "storm"
        "alerts": safe_get(weather, "alerts") or [],
    }


def _build_gig_demand_payload(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map raw poller payload into a business_dna.gig_demand_signal payload.

    Expected top-level structure (example, not strict):

    {
      "platform": "uber",
      "market": "austin_tx",
      "captured_at": "...",
      "surge_map": [ { ...cell... }, ... ],
      "events": [ { ...event... }, ... ],
      "weather": { ... },
      "source_meta": { ... }
    }
    """
    platform = safe_get(raw, "platform")
    market = safe_get(raw, "market")

    captured_at = normalize_timestamp(safe_get(raw, "captured_at"))

    surge_cells_raw = safe_get(raw, "surge_map") or []
    surge_cells: List[Dict[str, Any]] = []
    for cell in surge_cells_raw:
        norm = _normalize_surge_cell(cell or {})
        if norm is not None:
            surge_cells.append(norm)

    events_raw = safe_get(raw, "events") or []
    events: List[Dict[str, Any]] = []
    for evt in events_raw:
        norm_evt = _normalize_event(evt or {})
        if norm_evt is not None:
            events.append(norm_evt)

    weather_raw = safe_get(raw, "weather") or {}
    weather = _normalize_weather(weather_raw)

    return {
        "kind": "business_dna.gig_demand_signal",
        "platform": platform,
        "market": market,
        "captured_at": captured_at,
        "surge_cells": surge_cells,
        "events": events,
        "weather": weather,
        "source_meta": safe_get(raw, "source_meta") or {},
    }


def extract(
    payload: Dict[str, Any],
    *,
    source_id: str,
    asset_id: str,
) -> List[Dict[str, Any]]:
    """
    Main extractor entrypoint.

    - `payload` is the raw JSON from the scheduled poller/scraper.
    - `source_id` identifies the upstream source (e.g. "uber_api", "doordash_scraper").
    - `asset_id` is the logical asset key in NEXUS.

    Returns a list of shards (usually 1) ready for the shard_writer.
    """
    gig_demand_payload = _build_gig_demand_payload(payload or {})

    lineage = make_lineage(
        extractor_id=EXTRACTOR_ID,
        source_id=source_id,
        asset_id=asset_id,
        notes="Gig demand signals from scheduled poller/scraper.",
    )

    shard = make_shard(
        asset_id=asset_id,
        kind=gig_demand_payload["kind"],
        payload=gig_demand_payload,
        lineage=lineage,
    )

    return [shard]


# Register with the extractor router / registry.
register_extractor(SPEC, extract)
