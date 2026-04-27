"""
ext_dna.py — Canonical deterministic helpers for all NEXUS extractors.

This module provides:
  - make_extractor_spec
  - register_extractor
  - make_shard
  - make_lineage
  - normalize_timestamp
  - normalize_geo_point
  - normalize_currency
  - safe_get

All functions are:
  - deterministic
  - pure (no I/O, no randomness)
  - stdlib-only
  - substrate-stable
"""

from __future__ import annotations
from typing import Any, Dict, Optional, Callable
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

EXTRACTOR_REGISTRY: Dict[str, Dict[str, Any]] = {}


def make_extractor_spec(
    *,
    extractor_id: str,
    phase: str,
    domain: str,
    description: str,
    input_kind: str,
    output_kind: str,
) -> Dict[str, Any]:
    """Create a deterministic extractor spec."""
    return {
        "extractor_id": extractor_id,
        "phase": phase,
        "domain": domain,
        "description": description,
        "input_kind": input_kind,
        "output_kind": output_kind,
    }


def register_extractor(spec: Dict[str, Any], fn: Callable):
    """Register extractor in global registry."""
    EXTRACTOR_REGISTRY[spec["extractor_id"]] = {
        "spec": spec,
        "fn": fn,
    }


# ---------------------------------------------------------------------------
# Safe access
# ---------------------------------------------------------------------------

def safe_get(obj: Any, key: Any, default: Any = None) -> Any:
    """Safe dict access without exceptions."""
    if isinstance(obj, dict):
        return obj.get(key, default)
    return default


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------

def normalize_timestamp(value: Any) -> Optional[str]:
    """
    Normalize timestamps into ISO8601 UTC (YYYY-MM-DDTHH:MM:SSZ).

    Accepts:
      - ISO8601 strings (naive or with offset)
      - epoch seconds
      - epoch milliseconds

    Deterministic rules:
      - Naive ISO strings are interpreted as UTC.
      - Milliseconds auto-detected when value > 1e10.
      - Microseconds stripped.
      - Always returned in UTC with trailing 'Z'.
    """
    if value is None:
        return None

    # Epoch seconds or milliseconds
    if isinstance(value, (int, float)):
        if value > 1e10:  # JS-style ms timestamps
            value = value / 1000.0
        try:
            dt = datetime.fromtimestamp(value, tz=timezone.utc)
            return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        except Exception:
            return None

    # ISO8601 string
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)  # deterministic fix
            else:
                dt = dt.astimezone(timezone.utc)
            return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        except Exception:
            return None

    return None


def normalize_geo_point(*, lat: Any, lon: Any) -> Optional[Dict[str, float]]:
    """
    Normalize a geographic point.

    Notes:
      - Callers must pass lat/lon separately.
      - Comma-separated strings like "30.2,-97.7" are NOT parsed here.
        (Extractor must split before calling.)
    """
    try:
        lat_f = float(lat)
        lon_f = float(lon)
    except Exception:
        return None

    if not (-90.0 <= lat_f <= 90.0 and -180.0 <= lon_f <= 180.0):
        return None

    return {"lat": lat_f, "lon": lon_f}


def normalize_currency(*, amount: Any, currency: Any) -> Optional[Dict[str, Any]]:
    """
    Normalize currency values.

    Deterministic rules:
      - amount → float (after stripping symbols)
      - currency → uppercase string
      - No rounding here (Phase-1 canonicalizer handles precision)
    """
    if amount is None or currency is None:
        return None

    # Strip common symbols
    if isinstance(amount, str):
        amount = (
            amount.replace("$", "")
                  .replace("€", "")
                  .replace("£", "")
                  .strip()
        )

    try:
        amt = float(amount)
    except Exception:
        return None

    return {
        "amount": amt,
        "currency": str(currency).upper(),
    }


# ---------------------------------------------------------------------------
# Lineage + Shard construction
# ---------------------------------------------------------------------------

def make_lineage(
    *,
    extractor_id: str,
    source_id: str,
    asset_id: str,
    notes: Optional[str] = None,
) -> Dict[str, Any]:
    """Deterministic lineage block."""
    return {
        "extractor_id": extractor_id,
        "source_id": source_id,
        "asset_id": asset_id,
        "notes": notes,
    }


def make_shard(
    *,
    asset_id: str,
    kind: str,
    payload: Dict[str, Any],
    lineage: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Construct a deterministic shard object.

    IMPORTANT:
      - No hashing here.
      - No timestamps here.
      - No IDs here.

    The shard_writer performs:
      - canonical JSON serialization
      - deterministic hashing
      - shard_id assignment
      - created_at injection
    """
    return {
        "asset_id": asset_id,
        "kind": kind,
        "payload": payload,
        "lineage": lineage,
    }
