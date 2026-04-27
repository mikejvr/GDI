import json
import hashlib
from pathlib import Path
from typing import Tuple, Dict, Any

from src.utils.io_sorted import read_shards_sorted

SCHEMA_PATH = Path("src/schema/phase1_schema.json")  # at repo root

def load_schema() -> Dict[str, Any]:
    with SCHEMA_PATH.open("r") as f:
        return json.load(f)

def validate_shard_schema(data: Dict[str, Any], schema: Dict[str, Any]) -> Tuple[bool, str]:
    required_keys = schema.get("required", [])
    for key in required_keys:
        if key not in data:
            return False, f"Missing required key: {key}"
    # Add more type checks if needed
    return True, "ok"

def compute_shard_hash(data: Dict[str, Any]) -> str:
    json_str = json.dumps(data, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(json_str.encode()).hexdigest()

def validate_all_shards(data_dir: Path) -> Dict[str, Any]:
    data_dir = Path(data_dir).resolve()
    schema = load_schema()
    results = []
    all_valid = True
    repo_root = data_dir.parent  # assuming data_dir is e.g., /.../nexus/data

    for shard_path in read_shards_sorted(data_dir):
        with shard_path.open("r") as f:
            data = json.load(f)
        content_hash = compute_shard_hash(data)
        schema_valid, schema_err = validate_shard_schema(data, schema)
        valid = schema_valid
        if not valid:
            all_valid = False

        # Compute relative path from repo root
        try:
            rel_path = shard_path.relative_to(repo_root)
        except ValueError:
            rel_path = shard_path  # fallback

        results.append({
            "path": str(rel_path),
            "valid": valid,
            "schema_error": schema_err if not schema_valid else None,
            "content_hash": content_hash
        })

    return {"all_valid": all_valid, "results": results}
